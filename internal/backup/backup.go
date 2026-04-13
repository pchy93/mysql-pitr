// Package backup implements full backup using xtrabackup and uploads to S3.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

const metaFileName = "backup.meta.json"

// Config holds backup configuration.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Socket   string

	XtrabackupBin string
	TempDir       string
	S3Prefix      string

	// Compression algorithm: "none", "gzip", "zstd" (default: "zstd")
	Compression string

	// Parallelism for compression and upload
	Parallel int
}

// Meta holds metadata stored alongside a backup in S3.
type Meta struct {
	BackupID      string    `json:"backup_id"`
	StartTime     time.Time `json:"start_time"`
	FinishTime    time.Time `json:"finish_time"`
	BinlogFile    string    `json:"binlog_file"`
	BinlogPos     uint32    `json:"binlog_pos"`
	GTIDSet       string    `json:"gtid_set,omitempty"`
	MySQLVersion  string    `json:"mysql_version,omitempty"`
	SizeBytes     int64     `json:"size_bytes,omitempty"`
	Compression   string    `json:"compression,omitempty"`
	ChecksumSHA256 string   `json:"checksum_sha256,omitempty"`
	Status        string    `json:"status,omitempty"` // "completed", "failed"
	ErrorMessage  string    `json:"error_message,omitempty"`
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
	if cfg.Compression == "" {
		cfg.Compression = "zstd"
	}
	if cfg.Parallel <= 0 {
		cfg.Parallel = 4
	}
	return &Runner{cfg: cfg, store: store, logger: logger}
}

// Run performs a full backup and uploads it to S3 using streaming (no intermediate disk storage).
func (r *Runner) Run(ctx context.Context) (string, error) {
	backupID := fmt.Sprintf("full-%s", time.Now().UTC().Format("20060102T150405Z"))
	metaDir := filepath.Join(r.cfg.TempDir, backupID+"-meta")

	r.logger.Info("starting streaming backup",
		zap.String("backup_id", backupID),
		zap.String("compression", r.cfg.Compression),
		zap.Int("parallel", r.cfg.Parallel),
	)

	if err := os.MkdirAll(metaDir, 0o750); err != nil {
		return "", fmt.Errorf("create meta dir: %w", err)
	}
	defer os.RemoveAll(metaDir)

	startTime := time.Now().UTC()

	s3Key := fmt.Sprintf("%s%s/backup.xb", r.cfg.S3Prefix, backupID)
	if r.cfg.Compression == "gzip" {
		s3Key = fmt.Sprintf("%s%s/backup.xb.gz", r.cfg.S3Prefix, backupID)
	} else if r.cfg.Compression == "zstd" {
		s3Key = fmt.Sprintf("%s%s/backup.xb.zst", r.cfg.S3Prefix, backupID)
	}

	size, checksum, err := r.runStreamingBackup(ctx, metaDir, s3Key)
	if err != nil {
		// Cleanup: delete partial upload from S3
		r.logger.Warn("backup failed, cleaning up partial upload", zap.Error(err))
		if delErr := r.store.DeleteObject(context.Background(), s3Key); delErr != nil {
			r.logger.Error("failed to delete partial upload", zap.Error(delErr))
		}

		// Mark backup as failed
		failMeta := &Meta{
			BackupID:     backupID,
			StartTime:    startTime,
			FinishTime:   time.Now().UTC(),
			Status:       "failed",
			ErrorMessage: err.Error(),
		}
		r.uploadMeta(ctx, backupID, failMeta)
		return "", fmt.Errorf("streaming backup: %w", err)
	}

	meta, err := r.extractMeta(metaDir, backupID, startTime)
	if err != nil {
		return "", fmt.Errorf("extract meta: %w", err)
	}
	meta.SizeBytes = size
	meta.Compression = r.cfg.Compression
	meta.ChecksumSHA256 = checksum
	meta.Status = "completed"

	r.logger.Info("xtrabackup finished",
		zap.String("binlog_file", meta.BinlogFile),
		zap.Uint32("binlog_pos", meta.BinlogPos),
		zap.Int64("size_bytes", size),
		zap.String("checksum_sha256", checksum),
	)

	if err := r.uploadMeta(ctx, backupID, meta); err != nil {
		return "", fmt.Errorf("upload meta: %w", err)
	}

	r.logger.Info("backup completed successfully", zap.String("backup_id", backupID))
	return backupID, nil
}

// runStreamingBackup runs xtrabackup with --stream and pipes output directly to S3.
// Returns (size, checksum, error).
func (r *Runner) runStreamingBackup(ctx context.Context, metaDir, s3Key string) (int64, string, error) {
	// Build xtrabackup command with streaming
	args := []string{
		"--backup",
		fmt.Sprintf("--stream=xbstream"),
		fmt.Sprintf("--target-dir=%s", metaDir),
		fmt.Sprintf("--host=%s", r.cfg.Host),
		fmt.Sprintf("--port=%d", r.cfg.Port),
		fmt.Sprintf("--user=%s", r.cfg.User),
		fmt.Sprintf("--parallel=%d", r.cfg.Parallel),
	}

	if r.cfg.Socket != "" {
		args = append(args, fmt.Sprintf("--socket=%s", r.cfg.Socket))
	}
	args = append(args, fmt.Sprintf("--password=%s", r.cfg.Password))

	cmd := exec.CommandContext(ctx, r.cfg.XtrabackupBin, args...)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, "", fmt.Errorf("stdout pipe: %w", err)
	}

	r.logger.Info("starting xtrabackup streaming",
		zap.String("s3_key", s3Key),
		zap.Strings("args", filterPassword(cmd.Args)),
	)

	if err := cmd.Start(); err != nil {
		return 0, "", fmt.Errorf("start xtrabackup: %w", err)
	}

	// Create streaming uploader
	uploader, err := r.store.NewStreamingUploader(ctx, s3Key)
	if err != nil {
		cmd.Process.Kill()
		return 0, "", fmt.Errorf("create uploader: %w", err)
	}

	// Pipe through compression if needed
	var reader io.Reader = stdout

	switch r.cfg.Compression {
	case "gzip":
		pr, pw := io.Pipe()
		go func() {
			gw := newGzipWriter(pw)
			if _, err := io.Copy(gw, stdout); err != nil {
				pw.CloseWithError(err)
			} else {
				gw.Close()
				pw.Close()
			}
		}()
		reader = pr
	case "zstd":
		pr, pw := io.Pipe()
		go func() {
			zw := newZstdWriter(pw)
			if _, err := io.Copy(zw, stdout); err != nil {
				pw.CloseWithError(err)
			} else {
				zw.Close()
				pw.Close()
			}
		}()
		reader = pr
	}

	// Create hash writer wrapper to compute SHA256 while uploading
	hash := sha256.New()
	hashWriter := &hashWriter{w: uploader, h: hash}

	// Copy to S3 with hash computation
	size, err := io.Copy(hashWriter, reader)
	if err != nil {
		uploader.Close()
		cmd.Process.Kill()
		return 0, "", fmt.Errorf("copy to s3: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		uploader.Close()
		return 0, "", fmt.Errorf("xtrabackup wait: %w", err)
	}

	if err := uploader.Close(); err != nil {
		return 0, "", fmt.Errorf("close uploader: %w", err)
	}

	checksum := hex.EncodeToString(hash.Sum(nil))
	return size, checksum, nil
}

// hashWriter wraps an io.Writer to compute hash while writing.
type hashWriter struct {
	w io.Writer
	h hash.Hash
}

func (hw *hashWriter) Write(p []byte) (int, error) {
	hw.h.Write(p)
	return hw.w.Write(p)
}

// extractMeta reads metadata files from the meta directory.
func (r *Runner) extractMeta(metaDir, backupID string, startTime time.Time) (*Meta, error) {
	meta := &Meta{
		BackupID:   backupID,
		StartTime:  startTime,
		FinishTime: time.Now().UTC(),
	}

	// Read xtrabackup_binlog_info
	binlogInfo, err := os.ReadFile(filepath.Join(metaDir, "xtrabackup_binlog_info"))
	if err != nil {
		return nil, fmt.Errorf("read xtrabackup_binlog_info: %w", err)
	}

	if _, err := fmt.Sscanf(string(binlogInfo), "%s\t%d", &meta.BinlogFile, &meta.BinlogPos); err != nil {
		fmt.Sscanf(string(binlogInfo), "%s %d", &meta.BinlogFile, &meta.BinlogPos)
	}

	fields := splitFields(string(binlogInfo))
	if len(fields) >= 3 {
		meta.GTIDSet = fields[2]
	}

	// Read xtrabackup_info
	infoPath := filepath.Join(metaDir, "xtrabackup_info")
	if data, err := os.ReadFile(infoPath); err == nil {
		meta.MySQLVersion = extractField(string(data), "mysql_version")
	}

	return meta, nil
}

func (r *Runner) uploadMeta(ctx context.Context, backupID string, meta *Meta) error {
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	metaKey := fmt.Sprintf("%s%s/%s", r.cfg.S3Prefix, backupID, metaFileName)
	return r.store.UploadReader(ctx, metaKey, &byteReader{data: metaBytes}, int64(len(metaBytes)))
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

func extractField(content, key string) string {
	for _, line := range strings.Split(content, "\n") {
		var k, v string
		if n, _ := fmt.Sscanf(line, "%s = %s", &k, &v); n == 2 && k == key {
			return v
		}
	}
	return ""
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
