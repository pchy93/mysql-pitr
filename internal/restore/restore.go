// Package restore implements PITR restore from S3 full backup + binlogs.
package restore

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/backup"
	"github.com/mysql-pitr/mysql-pitr/internal/binlog"
	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

// Config holds restore configuration.
type Config struct {
	BackupID         string
	TargetTime       time.Time
	TargetGTID       string // GTID set to stop at (e.g., "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	S3Prefix         string
	DataDir          string
	TempDir          string
	TargetHost       string
	TargetPort       int
	TargetUser       string
	TargetPassword   string
	TargetSocket     string
	XtrabackupBin    string
	MysqlbinlogBin   string
	MysqlBin         string
	MysqlAdminBin    string // mysqladmin for health check
	MysqlShutdownCmd string
	MysqlStartCmd    string
	Parallel         int

	// MySQL startup timeout
	MySQLStartTimeout time.Duration

	// CopyMethod: "copy", "move", "hardlink"
	// - copy: standard xtrabackup --copy-back (safest, but slowest)
	// - move: xtrabackup --move-back (fast, but backup dir is emptied)
	// - hardlink: use hard links (fast, requires same filesystem, backup can't be deleted)
	CopyMethod string
}

// Runner performs the PITR restore.
type Runner struct {
	cfg    Config
	store  *s3store.Client
	logger *zap.Logger
}

// New creates a new restore Runner.
func New(cfg Config, store *s3store.Client, logger *zap.Logger) *Runner {
	if cfg.XtrabackupBin == "" {
		cfg.XtrabackupBin = "xtrabackup"
	}
	if cfg.MysqlbinlogBin == "" {
		cfg.MysqlbinlogBin = "mysqlbinlog"
	}
	if cfg.MysqlBin == "" {
		cfg.MysqlBin = "mysql"
	}
	if cfg.MysqlAdminBin == "" {
		cfg.MysqlAdminBin = "mysqladmin"
	}
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}
	if cfg.Parallel <= 0 {
		cfg.Parallel = 4
	}
	if cfg.CopyMethod == "" {
		cfg.CopyMethod = "copy"
	}
	if cfg.MySQLStartTimeout <= 0 {
		cfg.MySQLStartTimeout = 120 * time.Second
	}
	return &Runner{cfg: cfg, store: store, logger: logger}
}

// Run performs the full PITR restore sequence.
func (r *Runner) Run(ctx context.Context) error {
	workDir := filepath.Join(r.cfg.TempDir, fmt.Sprintf("restore-%d", time.Now().Unix()))
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	defer func() {
		r.logger.Info("cleaning up work dir", zap.String("dir", workDir))
		os.RemoveAll(workDir)
	}()

	// 1. Resolve backup metadata
	meta, err := r.resolveBackup(ctx)
	if err != nil {
		return fmt.Errorf("resolve backup: %w", err)
	}
	r.logger.Info("restoring from backup",
		zap.String("backup_id", meta.BackupID),
		zap.String("binlog_file", meta.BinlogFile),
		zap.Uint32("binlog_pos", meta.BinlogPos),
		zap.String("compression", meta.Compression),
		zap.Int64("size_bytes", meta.SizeBytes),
		zap.Time("backup_time", meta.FinishTime),
	)

	// 2. Streaming download + extract backup
	backupDir := filepath.Join(workDir, "xtrabackup")
	if err := r.streamingDownloadAndExtract(ctx, meta, backupDir); err != nil {
		return fmt.Errorf("download backup: %w", err)
	}

	// 3. xtrabackup --prepare
	if err := r.prepareBackup(ctx, backupDir); err != nil {
		return fmt.Errorf("prepare backup: %w", err)
	}

	// 4. Stop MySQL
	if err := r.runShellCmd(r.cfg.MysqlShutdownCmd); err != nil {
		return fmt.Errorf("stop mysql: %w", err)
	}

	// 5. Copy-back
	if err := r.copyBack(ctx, backupDir); err != nil {
		return fmt.Errorf("copy back: %w", err)
	}

	// 6. Start MySQL
	if err := r.runShellCmd(r.cfg.MysqlStartCmd); err != nil {
		return fmt.Errorf("start mysql: %w", err)
	}

	// Wait for MySQL to be ready
	if err := r.waitForMySQL(ctx); err != nil {
		return fmt.Errorf("wait for mysql: %w", err)
	}

	// 7. Apply binlogs
	if err := r.applyBinlogs(ctx, meta); err != nil {
		return fmt.Errorf("apply binlogs: %w", err)
	}

	r.logger.Info("PITR restore completed successfully")
	return nil
}

// resolveBackup finds the backup metadata to use for restore.
func (r *Runner) resolveBackup(ctx context.Context) (*backup.Meta, error) {
	if r.cfg.BackupID != "" {
		return r.loadMeta(ctx, r.cfg.BackupID)
	}

	prefix := r.cfg.S3Prefix
	objects, err := r.store.ListObjects(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list backups: %w", err)
	}

	var bestMeta *backup.Meta
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, "/backup.meta.json") {
			continue
		}
		parts := strings.Split(obj.Key, "/")
		if len(parts) < 2 {
			continue
		}
		backupID := parts[len(parts)-2]
		if !strings.HasPrefix(backupID, "full-") {
			continue
		}

		meta, err := r.loadMeta(ctx, backupID)
		if err != nil {
			r.logger.Warn("failed to load meta", zap.String("backup_id", backupID), zap.Error(err))
			continue
		}

		if !r.cfg.TargetTime.IsZero() && meta.FinishTime.After(r.cfg.TargetTime) {
			continue
		}

		if bestMeta == nil || meta.FinishTime.After(bestMeta.FinishTime) {
			bestMeta = meta
		}
	}

	if bestMeta == nil {
		return nil, fmt.Errorf("no suitable backup found")
	}
	return bestMeta, nil
}

func (r *Runner) loadMeta(ctx context.Context, backupID string) (*backup.Meta, error) {
	key := fmt.Sprintf("%s%s/backup.meta.json", r.cfg.S3Prefix, backupID)
	rc, err := r.store.GetObject(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get meta object %s: %w", key, err)
	}
	defer rc.Close()

	var meta backup.Meta
	if err := json.NewDecoder(rc).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decode meta: %w", err)
	}
	return &meta, nil
}

// streamingDownloadAndExtract downloads backup and extracts in a streaming fashion.
// S3 → decompressor → xbstream → disk (no intermediate file)
func (r *Runner) streamingDownloadAndExtract(ctx context.Context, meta *backup.Meta, destDir string) error {
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	// Determine S3 key based on compression
	s3Key := fmt.Sprintf("%s%s/backup.xb", r.cfg.S3Prefix, meta.BackupID)
	switch meta.Compression {
	case "gzip":
		s3Key = fmt.Sprintf("%s%s/backup.xb.gz", r.cfg.S3Prefix, meta.BackupID)
	case "zstd":
		s3Key = fmt.Sprintf("%s%s/backup.xb.zst", r.cfg.S3Prefix, meta.BackupID)
	}

	r.logger.Info("streaming download and extract",
		zap.String("s3_key", s3Key),
		zap.String("compression", meta.Compression),
	)

	rc, err := r.store.GetObject(ctx, s3Key)
	if err != nil {
		return fmt.Errorf("get backup object: %w", err)
	}
	defer rc.Close()

	// Build the pipeline: S3 stream → decompress → xbstream → files
	var reader io.Reader = rc

	// Decompression stage
	switch meta.Compression {
	case "gzip":
		gr, err := gzip.NewReader(rc)
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		defer gr.Close()
		reader = gr
	case "zstd":
		zr, err := zstd.NewReader(rc)
		if err != nil {
			return fmt.Errorf("zstd reader: %w", err)
		}
		defer zr.Close()
		reader = zr
	}

	// xbstream extraction stage
	cmd := exec.CommandContext(ctx, "xbstream", "-x", "-C", destDir)
	cmd.Stdin = reader
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("xbstream extract: %w", err)
	}

	r.logger.Info("backup extracted successfully", zap.String("dir", destDir))
	return nil
}

func (r *Runner) prepareBackup(ctx context.Context, backupDir string) error {
	r.logger.Info("preparing backup", zap.String("dir", backupDir))
	cmd := exec.CommandContext(ctx, r.cfg.XtrabackupBin,
		"--prepare",
		fmt.Sprintf("--target-dir=%s", backupDir),
		fmt.Sprintf("--use-memory=2G"),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (r *Runner) copyBack(ctx context.Context, backupDir string) error {
	r.logger.Info("restoring data files",
		zap.String("backup_dir", backupDir),
		zap.String("data_dir", r.cfg.DataDir),
		zap.String("method", r.cfg.CopyMethod),
	)

	switch r.cfg.CopyMethod {
	case "move":
		// Use xtrabackup --move-back (fast, but empties backup dir)
		cmd := exec.CommandContext(ctx, r.cfg.XtrabackupBin,
			"--move-back",
			fmt.Sprintf("--target-dir=%s", backupDir),
			fmt.Sprintf("--datadir=%s", r.cfg.DataDir),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()

	case "hardlink":
		// Use hard links - requires same filesystem
		return r.hardlinkRestore(backupDir, r.cfg.DataDir)

	default:
		// Standard copy
		cmd := exec.CommandContext(ctx, r.cfg.XtrabackupBin,
			"--copy-back",
			fmt.Sprintf("--target-dir=%s", backupDir),
			fmt.Sprintf("--datadir=%s", r.cfg.DataDir),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
}

// hardlinkRestore uses hard links to restore files (fast, same filesystem required).
func (r *Runner) hardlinkRestore(srcDir, dstDir string) error {
	// First, use xtrabackup to create the directory structure
	// Then hardlink the actual data files

	// Walk source directory and create hard links
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dstDir, relPath)

		// Check for path traversal
		if !strings.HasPrefix(dstPath, filepath.Clean(dstDir)+string(os.PathSeparator)) && dstPath != filepath.Clean(dstDir) {
			return fmt.Errorf("illegal path: %s", relPath)
		}

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// For regular files, try hard link first, fall back to copy
		if info.Mode().IsRegular() {
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
				return err
			}

			// Try reflink (copy-on-write) first, then hardlink, then regular copy
			if err := r.linkOrCopy(path, dstPath); err != nil {
				return fmt.Errorf("link/copy %s: %w", relPath, err)
			}
		}
		return nil
	})
}

// linkOrCopy tries reflink, then hardlink, then falls back to regular copy.
func (r *Runner) linkOrCopy(src, dst string) error {
	// Remove dst if exists
	os.Remove(dst)

	// Try reflink (copy-on-write, works on btrfs, xfs, apfs)
	if err := reflink(src, dst); err == nil {
		r.logger.Debug("reflinked", zap.String("file", filepath.Base(src)))
		return nil
	}

	// Try hard link
	if err := os.Link(src, dst); err == nil {
		r.logger.Debug("hardlinked", zap.String("file", filepath.Base(src)))
		return nil
	}

	// Fall back to regular copy
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// reflink attempts copy-on-write clone (Linux only, requires btrfs/xfs).
func reflink(src, dst string) error {
	// Try using cp --reflink=auto on Linux
	cmd := exec.Command("cp", "--reflink=auto", src, dst)
	return cmd.Run()
}

// applyBinlogs downloads binlog files from S3 and applies them up to TargetTime.
// Uses parallel download for multiple binlog files.
func (r *Runner) applyBinlogs(ctx context.Context, meta *backup.Meta) error {
	keys, err := binlog.ListBinlogsInRange(ctx, r.store, r.cfg.S3Prefix, meta.BinlogFile)
	if err != nil {
		return fmt.Errorf("list binlogs: %w", err)
	}

	if len(keys) == 0 {
		r.logger.Info("no binlog files found, skipping binlog apply")
		return nil
	}

	r.logger.Info("applying binlogs", zap.Int("count", len(keys)))

	// Parallel download binlogs to temp directory, then pipe to mysqlbinlog
	workDir := filepath.Join(r.cfg.TempDir, fmt.Sprintf("binlogs-%d", time.Now().Unix()))
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return err
	}
	defer os.RemoveAll(workDir)

	// Download binlogs in parallel
	type dlResult struct {
		localPath string
		err       error
	}
	dlCh := make(chan dlResult, len(keys))
	var wg sync.WaitGroup

	// Limit concurrency
	sem := make(chan struct{}, r.cfg.Parallel)

	for i, key := range keys {
		wg.Add(1)
		go func(idx int, s3Key string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			localPath := filepath.Join(workDir, filepath.Base(s3Key))
			if err := r.store.DownloadFile(ctx, s3Key, localPath); err != nil {
				dlCh <- dlResult{err: fmt.Errorf("download %s: %w", s3Key, err)}
				return
			}
			dlCh <- dlResult{localPath: localPath}
		}(i, key)
	}
	wg.Wait()
	close(dlCh)

	var localPaths []string
	for res := range dlCh {
		if res.err != nil {
			return res.err
		}
		localPaths = append(localPaths, res.localPath)
	}

	// Build mysqlbinlog command
	args := []string{}

	// GTID-based recovery takes precedence over time-based
	if r.cfg.TargetGTID != "" {
		r.logger.Info("using GTID-based recovery", zap.String("target_gtid", r.cfg.TargetGTID))
		args = append(args,
			"--skip-gtids",           // Don't execute GTIDs from binlog
			fmt.Sprintf("--stop-gtid=%s", r.cfg.TargetGTID),
		)
	} else {
		args = append(args, fmt.Sprintf("--start-position=%d", meta.BinlogPos))
		if !r.cfg.TargetTime.IsZero() {
			args = append(args, fmt.Sprintf("--stop-datetime=%s", r.cfg.TargetTime.Format("2006-01-02 15:04:05")))
		}
	}
	args = append(args, localPaths...)

	mysqlbinlogCmd := exec.CommandContext(ctx, r.cfg.MysqlbinlogBin, args...)
	mysqlbinlogCmd.Stderr = os.Stderr

	mysqlArgs := []string{
		fmt.Sprintf("--host=%s", r.cfg.TargetHost),
		fmt.Sprintf("--port=%d", r.cfg.TargetPort),
		fmt.Sprintf("--user=%s", r.cfg.TargetUser),
		fmt.Sprintf("--password=%s", r.cfg.TargetPassword),
	}
	if r.cfg.TargetSocket != "" {
		mysqlArgs = append(mysqlArgs, fmt.Sprintf("--socket=%s", r.cfg.TargetSocket))
	}
	mysqlCmd := exec.CommandContext(ctx, r.cfg.MysqlBin, mysqlArgs...)

	mysqlbinlogOut, err := mysqlbinlogCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("mysqlbinlog stdout pipe: %w", err)
	}

	mysqlCmd.Stdin = mysqlbinlogOut
	mysqlCmd.Stdout = os.Stdout
	mysqlCmd.Stderr = os.Stderr

	if err := mysqlbinlogCmd.Start(); err != nil {
		return fmt.Errorf("start mysqlbinlog: %w", err)
	}
	if err := mysqlCmd.Start(); err != nil {
		mysqlbinlogCmd.Process.Kill()
		return fmt.Errorf("start mysql: %w", err)
	}

	if err := mysqlbinlogCmd.Wait(); err != nil {
		mysqlCmd.Process.Kill()
		return fmt.Errorf("mysqlbinlog: %w", err)
	}
	if err := mysqlCmd.Wait(); err != nil {
		return fmt.Errorf("mysql: %w", err)
	}

	r.logger.Info("binlog apply completed")
	return nil
}

func (r *Runner) runShellCmd(cmdStr string) error {
	if cmdStr == "" {
		r.logger.Warn("no shell command configured, skipping")
		return nil
	}
	r.logger.Info("running shell command", zap.String("cmd", cmdStr))
	c := exec.Command("sh", "-c", cmdStr)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

// waitForMySQL waits for MySQL to become ready using mysqladmin ping.
func (r *Runner) waitForMySQL(ctx context.Context) error {
	r.logger.Info("waiting for MySQL to be ready",
		zap.Duration("timeout", r.cfg.MySQLStartTimeout),
	)

	timeout := time.NewTimer(r.cfg.MySQLStartTimeout)
	defer timeout.Stop()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for MySQL to start after %v", r.cfg.MySQLStartTimeout)
		case <-ticker.C:
			if r.isMySQLReady(ctx) {
				r.logger.Info("MySQL is ready")
				return nil
			}
		}
	}
}

// isMySQLReady checks if MySQL is ready using mysqladmin ping.
func (r *Runner) isMySQLReady(ctx context.Context) bool {
	args := []string{
		"ping",
		fmt.Sprintf("--host=%s", r.cfg.TargetHost),
		fmt.Sprintf("--port=%d", r.cfg.TargetPort),
		fmt.Sprintf("--user=%s", r.cfg.TargetUser),
		fmt.Sprintf("--password=%s", r.cfg.TargetPassword),
	}
	if r.cfg.TargetSocket != "" {
		args = append(args, fmt.Sprintf("--socket=%s", r.cfg.TargetSocket))
	}

	cmd := exec.CommandContext(ctx, r.cfg.MysqlAdminBin, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		r.logger.Debug("mysqladmin ping failed", zap.Error(err), zap.String("output", string(output)))
		return false
	}
	return strings.Contains(string(output), "alive") || strings.Contains(string(output), "mysqld is alive")
}
