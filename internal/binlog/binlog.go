// Package binlog implements continuous binlog pulling from MySQL and uploading to S3.
package binlog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

const (
	// S3 key prefix for binlog files, relative to S3Prefix.
	binlogSubDir = "binlogs"
	// How often to flush/upload the current binlog segment when idle.
	flushInterval = 30 * time.Second
)

// Config holds binlog puller configuration.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string

	// MysqlbinlogBin path (defaults to "mysqlbinlog")
	MysqlbinlogBin string

	// TempDir for buffering binlog files before upload
	TempDir string

	// S3Prefix is the top-level prefix, e.g. "backups/"
	S3Prefix string

	// ServerID for the replication client (must be unique in the cluster)
	ServerID uint32

	// StartFile / StartPos: from which binlog to start reading.
	// Usually derived from the last full backup metadata.
	StartFile string
	StartPos  uint32
}

// Puller pulls binlog from MySQL and uploads segments to S3.
type Puller struct {
	cfg    Config
	store  *s3store.Client
	logger *zap.Logger
}

// New creates a new binlog Puller.
func New(cfg Config, store *s3store.Client, logger *zap.Logger) *Puller {
	if cfg.MysqlbinlogBin == "" {
		cfg.MysqlbinlogBin = "mysqlbinlog"
	}
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}
	if cfg.ServerID == 0 {
		cfg.ServerID = 99999
	}
	return &Puller{cfg: cfg, store: store, logger: logger}
}

// Run starts pulling binlog and uploading to S3. Runs until ctx is cancelled.
func (p *Puller) Run(ctx context.Context) error {
	workDir := filepath.Join(p.cfg.TempDir, "binlog-pull")
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}

	p.logger.Info("starting binlog puller",
		zap.String("host", p.cfg.Host),
		zap.String("start_file", p.cfg.StartFile),
		zap.Uint32("start_pos", p.cfg.StartPos),
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := p.pullAndUpload(ctx, workDir); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.logger.Warn("binlog pull error, retrying in 5s", zap.Error(err))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// pullAndUpload runs mysqlbinlog --read-from-remote-server and uploads each complete binlog file.
func (p *Puller) pullAndUpload(ctx context.Context, workDir string) error {
	args := []string{
		"--read-from-remote-server",
		"--raw",
		"--stop-never",
		fmt.Sprintf("--host=%s", p.cfg.Host),
		fmt.Sprintf("--port=%d", p.cfg.Port),
		fmt.Sprintf("--user=%s", p.cfg.User),
		fmt.Sprintf("--password=%s", p.cfg.Password),
		fmt.Sprintf("--connection-server-id=%d", p.cfg.ServerID),
		fmt.Sprintf("--result-file=%s/", workDir),
	}

	if p.cfg.StartFile != "" {
		args = append(args, fmt.Sprintf("--start-position=%d", p.cfg.StartPos))
		args = append(args, p.cfg.StartFile)
	}

	// We log args without password
	logArgs := make([]string, 0, len(args))
	for _, a := range args {
		if strings.HasPrefix(a, "--password=") {
			logArgs = append(logArgs, "--password=***")
		} else {
			logArgs = append(logArgs, a)
		}
	}
	p.logger.Info("starting mysqlbinlog", zap.Strings("args", logArgs))

	cmd := exec.CommandContext(ctx, p.cfg.MysqlbinlogBin, args...)
	cmd.Dir = workDir

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start mysqlbinlog: %w", err)
	}

	// Log stderr from mysqlbinlog
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			p.logger.Debug("mysqlbinlog", zap.String("stderr", scanner.Text()))
		}
	}()

	// Watch the work directory for new/completed binlog files and upload them.
	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		p.watchAndUpload(ctx, workDir)
	}()

	err = cmd.Wait()
	<-uploadDone

	// Upload any remaining files
	p.uploadNewFiles(context.Background(), workDir)

	return err
}

// watchAndUpload monitors workDir for binlog files and uploads completed ones.
func (p *Puller) watchAndUpload(ctx context.Context, workDir string) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	uploaded := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.uploadCompletedFiles(ctx, workDir, uploaded)
		}
	}
}

// uploadCompletedFiles scans workDir and uploads binlog files that appear complete
// (i.e., there's a newer file, so the current one is no longer being written to).
func (p *Puller) uploadCompletedFiles(ctx context.Context, workDir string, uploaded map[string]bool) {
	entries, err := os.ReadDir(workDir)
	if err != nil {
		p.logger.Warn("read work dir", zap.Error(err))
		return
	}

	var binlogFiles []string
	for _, e := range entries {
		if !e.IsDir() && isBinlogFile(e.Name()) {
			binlogFiles = append(binlogFiles, e.Name())
		}
	}

	// All but the last file are considered complete (mysqlbinlog has rotated to the next).
	for i, name := range binlogFiles {
		if uploaded[name] {
			continue
		}
		// Upload all but the last (currently-written) file immediately.
		// For the last file, only upload on flush timer.
		if i < len(binlogFiles)-1 || len(binlogFiles) == 1 {
			if i == len(binlogFiles)-1 {
				// It's the only file; upload a checkpoint copy
			}
			p.uploadBinlogFile(ctx, workDir, name)
			uploaded[name] = true
		}
	}
}

// uploadNewFiles uploads all binlog files in workDir (used on shutdown).
func (p *Puller) uploadNewFiles(ctx context.Context, workDir string) {
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() && isBinlogFile(e.Name()) {
			p.uploadBinlogFile(ctx, workDir, e.Name())
		}
	}
}

// uploadBinlogFile uploads a single binlog file to S3.
func (p *Puller) uploadBinlogFile(ctx context.Context, workDir, name string) {
	localPath := filepath.Join(workDir, name)
	s3Key := fmt.Sprintf("%s%s/%s", p.cfg.S3Prefix, binlogSubDir, name)

	p.logger.Info("uploading binlog", zap.String("file", name), zap.String("s3_key", s3Key))

	if err := p.store.UploadFile(ctx, s3Key, localPath); err != nil {
		p.logger.Error("upload binlog failed", zap.String("file", name), zap.Error(err))
		return
	}
	// Remove local copy after successful upload.
	os.Remove(localPath)
}

// isBinlogFile returns true if the filename looks like a MySQL binlog file.
func isBinlogFile(name string) bool {
	// binlog files are typically "binlog.000001" or "mysql-bin.000001"
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		last := parts[len(parts)-1]
		if len(last) >= 6 {
			allDigits := true
			for _, c := range last {
				if c < '0' || c > '9' {
					allDigits = false
					break
				}
			}
			return allDigits
		}
	}
	return false
}

// ListBinlogsInRange returns S3 keys for binlog files that may contain events
// between startFile/startPos and the given target time.
func ListBinlogsInRange(ctx context.Context, store *s3store.Client, s3Prefix string, startFile string) ([]string, error) {
	prefix := fmt.Sprintf("%s%s/", s3Prefix, binlogSubDir)
	objects, err := store.ListObjects(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list binlogs: %w", err)
	}

	var keys []string
	foundStart := startFile == ""
	for _, obj := range objects {
		name := filepath.Base(obj.Key)
		if !foundStart {
			if name == startFile || name >= startFile {
				foundStart = true
			} else {
				continue
			}
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// PipeReader returns an io.ReadCloser that streams multiple S3 binlog objects concatenated.
// Used by the restore process to pipe into mysqlbinlog.
func PipeReader(ctx context.Context, store *s3store.Client, keys []string) io.ReadCloser {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		for _, key := range keys {
			rc, err := store.GetObject(ctx, key)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("get binlog %s: %w", key, err))
				return
			}
			if _, err := io.Copy(pw, rc); err != nil {
				rc.Close()
				pw.CloseWithError(err)
				return
			}
			rc.Close()
		}
	}()

	return pr
}
