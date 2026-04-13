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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

const (
	binlogSubDir   = "binlogs"
	flushInterval  = 10 * time.Second // Reduced from 30s for lower latency
	uploadParallel = 4                // Parallel uploads
)

// Config holds binlog puller configuration.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string

	MysqlbinlogBin string
	TempDir        string
	S3Prefix       string
	ServerID       uint32
	StartFile      string
	StartPos       uint32

	// Compression: "none", "gzip", "zstd"
	Compression string
	// Parallel upload workers
	Parallel int
}

// Puller pulls binlog from MySQL and uploads segments to S3.
type Puller struct {
	cfg    Config
	store  *s3store.Client
	logger *zap.Logger

	// Upload queue and worker pool
	uploadQueue chan uploadTask
	uploadWg    sync.WaitGroup
}

type uploadTask struct {
	localPath string
	s3Key     string
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
	if cfg.Parallel <= 0 {
		cfg.Parallel = uploadParallel
	}
	return &Puller{
		cfg:         cfg,
		store:       store,
		logger:      logger,
		uploadQueue: make(chan uploadTask, 100),
	}
}

// Run starts pulling binlog and uploading to S3.
func (p *Puller) Run(ctx context.Context) error {
	workDir := filepath.Join(p.cfg.TempDir, "binlog-pull")
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}

	// Start upload workers
	for i := 0; i < p.cfg.Parallel; i++ {
		p.uploadWg.Add(1)
		go p.uploadWorker(ctx, i)
	}

	p.logger.Info("starting binlog puller",
		zap.String("host", p.cfg.Host),
		zap.String("start_file", p.cfg.StartFile),
		zap.Uint32("start_pos", p.cfg.StartPos),
		zap.Int("parallel", p.cfg.Parallel),
		zap.String("compression", p.cfg.Compression),
	)

	for {
		select {
		case <-ctx.Done():
			// Drain remaining uploads
			close(p.uploadQueue)
			p.uploadWg.Wait()
			return ctx.Err()
		default:
		}

		if err := p.pullAndUpload(ctx, workDir); err != nil {
			if ctx.Err() != nil {
				close(p.uploadQueue)
				p.uploadWg.Wait()
				return ctx.Err()
			}
			p.logger.Warn("binlog pull error, retrying in 5s", zap.Error(err))
			select {
			case <-ctx.Done():
				close(p.uploadQueue)
				p.uploadWg.Wait()
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// uploadWorker processes upload tasks from the queue.
func (p *Puller) uploadWorker(ctx context.Context, id int) {
	defer p.uploadWg.Done()
	for task := range p.uploadQueue {
		if err := p.uploadFile(ctx, task.localPath, task.s3Key); err != nil {
			p.logger.Error("upload failed",
				zap.Int("worker", id),
				zap.String("file", filepath.Base(task.localPath)),
				zap.Error(err),
			)
		}
	}
}

// uploadFile uploads a single file and removes it on success.
func (p *Puller) uploadFile(ctx context.Context, localPath, s3Key string) error {
	start := time.Now()
	if err := p.store.UploadFile(ctx, s3Key, localPath); err != nil {
		return err
	}
	os.Remove(localPath)
	p.logger.Info("uploaded binlog",
		zap.String("file", filepath.Base(localPath)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// pullAndUpload runs mysqlbinlog and queues files for upload.
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

	logArgs := filterPassword(args)
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

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			p.logger.Debug("mysqlbinlog", zap.String("stderr", scanner.Text()))
		}
	}()

	// Monitor and queue uploads
	uploaded := make(map[string]bool)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			cmd.Process.Kill()
			cmd.Wait()
			p.queueRemaining(workDir, uploaded)
			return ctx.Err()
		case <-ticker.C:
			p.queueCompletedFiles(workDir, uploaded)
		}
	}
}

// queueCompletedFiles scans for completed binlog files and queues them for upload.
func (p *Puller) queueCompletedFiles(workDir string, uploaded map[string]bool) {
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return
	}

	var binlogFiles []string
	for _, e := range entries {
		if !e.IsDir() && isBinlogFile(e.Name()) {
			binlogFiles = append(binlogFiles, e.Name())
		}
	}

	// All but the last file are complete
	for i, name := range binlogFiles {
		if uploaded[name] {
			continue
		}
		// Only queue files that are not currently being written
		if i < len(binlogFiles)-1 {
			localPath := filepath.Join(workDir, name)
			s3Key := fmt.Sprintf("%s%s/%s", p.cfg.S3Prefix, binlogSubDir, name)
			if p.cfg.Compression != "" && p.cfg.Compression != "none" {
				s3Key += "." + p.cfg.Compression
			}

			select {
			case p.uploadQueue <- uploadTask{localPath: localPath, s3Key: s3Key}:
				uploaded[name] = true
			default:
				p.logger.Warn("upload queue full, will retry", zap.String("file", name))
			}
		}
	}
}

// queueRemaining queues all remaining files on shutdown.
func (p *Puller) queueRemaining(workDir string, uploaded map[string]bool) {
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() && isBinlogFile(e.Name()) && !uploaded[e.Name()] {
			localPath := filepath.Join(workDir, e.Name())
			s3Key := fmt.Sprintf("%s%s/%s", p.cfg.S3Prefix, binlogSubDir, e.Name())
			if p.cfg.Compression != "" && p.cfg.Compression != "none" {
				s3Key += "." + p.cfg.Compression
			}
			p.uploadQueue <- uploadTask{localPath: localPath, s3Key: s3Key}
		}
	}
}

func isBinlogFile(name string) bool {
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

func filterPassword(args []string) []string {
	result := make([]string, 0, len(args))
	for _, a := range args {
		if strings.HasPrefix(a, "--password=") {
			result = append(result, "--password=***")
		} else {
			result = append(result, a)
		}
	}
	return result
}

// ListBinlogsInRange returns S3 keys for binlog files.
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
		// Strip compression suffix for comparison
		baseName := strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".zst")
		if !foundStart {
			if baseName == startFile || baseName >= startFile {
				foundStart = true
			} else {
				continue
			}
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// PipeReader returns an io.ReadCloser that streams multiple S3 binlog objects.
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

			// Decompress if needed
			var reader io.Reader = rc
			if strings.HasSuffix(key, ".gz") {
				// gzip decompression handled in restore module
				reader = rc
			} else if strings.HasSuffix(key, ".zst") {
				// zstd decompression handled in restore module
				reader = rc
			}

			if _, err := io.Copy(pw, reader); err != nil {
				rc.Close()
				pw.CloseWithError(err)
				return
			}
			rc.Close()
		}
	}()

	return pr
}
