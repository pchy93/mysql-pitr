// Package backup implements full backup using xtrabackup and uploads to S3.
package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

const metaFileName = "backup.meta.json"

// Config holds backup configuration.
type Config struct {
	// MySQL connection
	Host     string
	Port     int
	User     string
	Password string
	Socket   string // optional unix socket

	// xtrabackup binary path (defaults to "xtrabackup")
	XtrabackupBin string

	// Temporary directory for xtrabackup output before upload
	TempDir string

	// S3 key prefix for backups, e.g. "backups/"
	S3Prefix string
}

// Meta holds metadata stored alongside a backup in S3.
type Meta struct {
	BackupID    string    `json:"backup_id"`
	StartTime   time.Time `json:"start_time"`
	FinishTime  time.Time `json:"finish_time"`
	BinlogFile  string    `json:"binlog_file"`
	BinlogPos   uint32    `json:"binlog_pos"`
	GTIDSet     string    `json:"gtid_set,omitempty"`
	MySQLVersion string   `json:"mysql_version,omitempty"`
}

// Runner runs full backups.
type Runner struct {
	cfg    Config
	store  *s3store.Client
	logger *zap.Logger
}

// New creates a new backup Runner.
func New(cfg Config, store *s3store.Client, logger *zap.Logger) *Runner {
	if cfg.XtrabackupBin == "" {
		cfg.XtrabackupBin = "xtrabackup"
	}
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}
	return &Runner{cfg: cfg, store: store, logger: logger}
}

// Run performs a full backup and uploads it to S3.
// Returns the backup ID (used later to locate the backup on S3).
func (r *Runner) Run(ctx context.Context) (string, error) {
	backupID := fmt.Sprintf("full-%s", time.Now().UTC().Format("20060102T150405Z"))
	workDir := filepath.Join(r.cfg.TempDir, backupID)

	r.logger.Info("starting full backup", zap.String("backup_id", backupID), zap.String("work_dir", workDir))

	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return "", fmt.Errorf("create work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	startTime := time.Now().UTC()
	if err := r.runXtrabackup(ctx, workDir); err != nil {
		return "", fmt.Errorf("xtrabackup: %w", err)
	}

	meta, err := r.extractMeta(workDir, backupID, startTime)
	if err != nil {
		return "", fmt.Errorf("extract meta: %w", err)
	}

	r.logger.Info("xtrabackup finished",
		zap.String("binlog_file", meta.BinlogFile),
		zap.Uint32("binlog_pos", meta.BinlogPos),
		zap.String("gtid_set", meta.GTIDSet),
	)

	if err := r.uploadBackup(ctx, backupID, workDir, meta); err != nil {
		return "", fmt.Errorf("upload backup: %w", err)
	}

	r.logger.Info("backup uploaded successfully", zap.String("backup_id", backupID))
	return backupID, nil
}

// runXtrabackup executes xtrabackup --backup.
func (r *Runner) runXtrabackup(ctx context.Context, targetDir string) error {
	args := []string{
		"--backup",
		fmt.Sprintf("--target-dir=%s", targetDir),
		fmt.Sprintf("--host=%s", r.cfg.Host),
		fmt.Sprintf("--port=%d", r.cfg.Port),
		fmt.Sprintf("--user=%s", r.cfg.User),
	}

	if r.cfg.Socket != "" {
		args = append(args, fmt.Sprintf("--socket=%s", r.cfg.Socket))
	}

	// Pass password via environment variable to avoid shell history exposure.
	cmd := exec.CommandContext(ctx, r.cfg.XtrabackupBin, args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("XTRABACKUP_PASSWORD=%s", r.cfg.Password))
	// xtrabackup reads password from --password flag; use env workaround via stdin or flag.
	// For simplicity we append as flag (password is not logged).
	cmd.Args = append(cmd.Args, fmt.Sprintf("--password=%s", r.cfg.Password))

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	r.logger.Info("running xtrabackup", zap.Strings("args", cmd.Args[:len(cmd.Args)-1])) // omit password
	return cmd.Run()
}

// extractMeta reads xtrabackup_binlog_info and xtrabackup_info to build Meta.
func (r *Runner) extractMeta(workDir, backupID string, startTime time.Time) (*Meta, error) {
	meta := &Meta{
		BackupID:  backupID,
		StartTime: startTime,
		FinishTime: time.Now().UTC(),
	}

	// Parse xtrabackup_binlog_info: "<binlog_file>\t<pos>\t[gtid_set]"
	binlogInfo, err := os.ReadFile(filepath.Join(workDir, "xtrabackup_binlog_info"))
	if err != nil {
		return nil, fmt.Errorf("read xtrabackup_binlog_info: %w", err)
	}
	if _, err := fmt.Sscanf(string(binlogInfo), "%s\t%d", &meta.BinlogFile, &meta.BinlogPos); err != nil {
		// Try space-separated fallback
		fmt.Sscanf(string(binlogInfo), "%s %d", &meta.BinlogFile, &meta.BinlogPos)
	}

	// Try to get GTID set (third field in binlog_info or from xtrabackup_info)
	fields := splitFields(string(binlogInfo))
	if len(fields) >= 3 {
		meta.GTIDSet = fields[2]
	}

	// Read MySQL version from xtrabackup_info if available
	infoPath := filepath.Join(workDir, "xtrabackup_info")
	if data, err := os.ReadFile(infoPath); err == nil {
		meta.MySQLVersion = extractField(string(data), "mysql_version")
	}

	return meta, nil
}

// uploadBackup tars + gzips the backup directory and uploads to S3, then uploads meta.
func (r *Runner) uploadBackup(ctx context.Context, backupID, workDir string, meta *Meta) error {
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		errCh <- tarGzDir(workDir, pw)
	}()

	s3Key := fmt.Sprintf("%s%s/backup.tar.gz", r.cfg.S3Prefix, backupID)
	r.logger.Info("uploading backup archive", zap.String("s3_key", s3Key))

	if err := r.store.UploadReader(ctx, s3Key, pr, -1); err != nil {
		return fmt.Errorf("upload archive: %w", err)
	}

	if err := <-errCh; err != nil {
		return fmt.Errorf("tar backup: %w", err)
	}

	// Upload metadata JSON
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	metaKey := fmt.Sprintf("%s%s/%s", r.cfg.S3Prefix, backupID, metaFileName)
	if err := r.store.UploadReader(ctx, metaKey, jsonReader(metaBytes), int64(len(metaBytes))); err != nil {
		return fmt.Errorf("upload meta: %w", err)
	}

	return nil
}

// tarGzDir creates a tar.gz archive of dir and writes it to w.
func tarGzDir(dir string, w io.Writer) error {
	gw := gzip.NewWriter(w)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
}

// splitFields splits a string by whitespace (tab or space).
func splitFields(s string) []string {
	var fields []string
	cur := ""
	for _, c := range s {
		if c == '\t' || c == ' ' || c == '\n' {
			if cur != "" {
				fields = append(fields, cur)
				cur = ""
			}
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		fields = append(fields, cur)
	}
	return fields
}

// extractField extracts "key = value" from xtrabackup_info content.
func extractField(content, key string) string {
	for _, line := range splitLines(content) {
		var k, v string
		if n, _ := fmt.Sscanf(line, "%s = %s", &k, &v); n == 2 && k == key {
			return v
		}
	}
	return ""
}

func splitLines(s string) []string {
	var lines []string
	cur := ""
	for _, c := range s {
		if c == '\n' {
			lines = append(lines, cur)
			cur = ""
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return lines
}

// jsonReader wraps a byte slice as an io.Reader.
func jsonReader(b []byte) io.Reader {
	return &byteReader{data: b}
}

type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
