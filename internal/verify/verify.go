// Package verify implements backup integrity verification.
package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/backup"
	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

// Config holds verify configuration.
type Config struct {
	S3Prefix   string
	TempDir    string
	VerifyData bool // Whether to verify data integrity (extract and check)
}

// Runner performs backup verification.
type Runner struct {
	cfg    Config
	store  *s3store.Client
	logger *zap.Logger
}

// New creates a new verify Runner.
func New(cfg Config, store *s3store.Client, logger *zap.Logger) *Runner {
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}
	return &Runner{cfg: cfg, store: store, logger: logger}
}

// VerifyAll verifies all backups in S3.
func (r *Runner) VerifyAll(ctx context.Context) error {
	objects, err := r.store.ListObjects(ctx, r.cfg.S3Prefix)
	if err != nil {
		return fmt.Errorf("list objects: %w", err)
	}

	var hasError bool
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, "/backup.meta.json") {
			continue
		}

		parts := strings.Split(obj.Key, "/")
		if len(parts) < 2 {
			continue
		}
		backupID := parts[len(parts)-2]

		if err := r.Verify(ctx, backupID); err != nil {
			r.logger.Error("verification failed",
				zap.String("backup_id", backupID),
				zap.Error(err),
			)
			hasError = true
		}
	}

	if hasError {
		return fmt.Errorf("one or more backups failed verification")
	}
	return nil
}

// Verify verifies a specific backup by ID.
func (r *Runner) Verify(ctx context.Context, backupID string) error {
	r.logger.Info("verifying backup", zap.String("backup_id", backupID))

	// Load metadata
	metaKey := fmt.Sprintf("%s%s/backup.meta.json", r.cfg.S3Prefix, backupID)
	rc, err := r.store.GetObject(ctx, metaKey)
	if err != nil {
		return fmt.Errorf("get meta: %w", err)
	}

	var meta backup.Meta
	if err := json.NewDecoder(rc).Decode(&meta); err != nil {
		rc.Close()
		return fmt.Errorf("decode meta: %w", err)
	}
	rc.Close()

	// Check backup status
	if meta.Status == "failed" {
		return fmt.Errorf("backup marked as failed: %s", meta.ErrorMessage)
	}

	// Determine backup file key
	s3Key := fmt.Sprintf("%s%s/backup.xb", r.cfg.S3Prefix, backupID)
	switch meta.Compression {
	case "gzip":
		s3Key = fmt.Sprintf("%s%s/backup.xb.gz", r.cfg.S3Prefix, backupID)
	case "zstd":
		s3Key = fmt.Sprintf("%s%s/backup.xb.zst", r.cfg.S3Prefix, backupID)
	}

	// Download and compute checksum
	r.logger.Info("downloading for checksum verification",
		zap.String("s3_key", s3Key),
	)

	stream, err := r.store.GetObject(ctx, s3Key)
	if err != nil {
		return fmt.Errorf("get backup: %w", err)
	}
	defer stream.Close()

	hash := sha256.New()
	size, err := io.Copy(hash, stream)
	if err != nil {
		return fmt.Errorf("read backup: %w", err)
	}

	computedChecksum := hex.EncodeToString(hash.Sum(nil))

	// Verify size
	if meta.SizeBytes > 0 && size != meta.SizeBytes {
		return fmt.Errorf("size mismatch: expected %d, got %d", meta.SizeBytes, size)
	}

	// Verify checksum
	if meta.ChecksumSHA256 != "" && computedChecksum != meta.ChecksumSHA256 {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", meta.ChecksumSHA256, computedChecksum)
	}

	r.logger.Info("checksum verification passed",
		zap.String("backup_id", backupID),
		zap.String("checksum_sha256", computedChecksum),
		zap.Int64("size_bytes", size),
	)

	// Optional: verify data integrity by extracting
	if r.cfg.VerifyData {
		if err := r.verifyDataIntegrity(ctx, &meta, s3Key); err != nil {
			return fmt.Errorf("data integrity check: %w", err)
		}
	}

	r.logger.Info("backup verification completed", zap.String("backup_id", backupID))
	return nil
}

// verifyDataIntegrity extracts the backup and verifies it can be prepared.
func (r *Runner) verifyDataIntegrity(ctx context.Context, meta *backup.Meta, s3Key string) error {
	workDir := filepath.Join(r.cfg.TempDir, fmt.Sprintf("verify-%s", meta.BackupID))
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	defer os.RemoveAll(workDir)

	extractDir := filepath.Join(workDir, "backup")
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return err
	}

	r.logger.Info("extracting backup for data verification")

	// Download and extract
	stream, err := r.store.GetObject(ctx, s3Key)
	if err != nil {
		return fmt.Errorf("get backup: %w", err)
	}
	defer stream.Close()

	var reader io.Reader = stream
	switch meta.Compression {
	case "zstd":
		zr, err := zstd.NewReader(stream)
		if err != nil {
			return fmt.Errorf("zstd reader: %w", err)
		}
		defer zr.Close()
		reader = zr
	}

	// Extract using xbstream
	cmd := exec.CommandContext(ctx, "xbstream", "-x", "-C", extractDir)
	cmd.Stdin = reader
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("xbstream extract: %w", err)
	}

	// Verify xtrabackup --prepare works
	r.logger.Info("running xtrabackup --prepare for verification")
	prepareCmd := exec.CommandContext(ctx, "xtrabackup",
		"--prepare",
		fmt.Sprintf("--target-dir=%s", extractDir),
		"--use-memory=512M",
	)
	prepareCmd.Stdout = os.Stdout
	prepareCmd.Stderr = os.Stderr

	if err := prepareCmd.Run(); err != nil {
		return fmt.Errorf("xtrabackup prepare: %w", err)
	}

	r.logger.Info("data integrity verification passed")
	return nil
}
