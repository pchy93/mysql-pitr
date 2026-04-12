// Package restore implements PITR restore from S3 full backup + binlogs.
package restore

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
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/backup"
	"github.com/mysql-pitr/mysql-pitr/internal/binlog"
	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

// Config holds restore configuration.
type Config struct {
	// BackupID to restore from (e.g. "full-20240101T120000Z").
	// If empty, the latest full backup is used.
	BackupID string

	// TargetTime is the point-in-time to recover to.
	// If zero, restore to the latest available position.
	TargetTime time.Time

	// S3Prefix is the top-level S3 prefix, same as used during backup.
	S3Prefix string

	// DataDir is the MySQL data directory on the target host.
	DataDir string

	// TempDir for intermediate files.
	TempDir string

	// Target MySQL instance (used to apply binlog via mysql client).
	TargetHost     string
	TargetPort     int
	TargetUser     string
	TargetPassword string
	TargetSocket   string

	// XtrabackupBin path (defaults to "xtrabackup")
	XtrabackupBin string
	// MysqlbinlogBin path (defaults to "mysqlbinlog")
	MysqlbinlogBin string
	// MysqlBin path (defaults to "mysql")
	MysqlBin string

	// MysqlShutdownCmd is a shell command to stop MySQL before restoring data files.
	// e.g. "systemctl stop mysqld"
	MysqlShutdownCmd string
	// MysqlStartCmd is a shell command to start MySQL after data files are restored.
	// e.g. "systemctl start mysqld"
	MysqlStartCmd string
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
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}
	return &Runner{cfg: cfg, store: store, logger: logger}
}

// Run performs the full PITR restore sequence:
//  1. Find the most suitable full backup
//  2. Download and extract it
//  3. Run xtrabackup --prepare
//  4. Stop MySQL
//  5. Copy-back data files
//  6. Start MySQL
//  7. Apply binlogs up to TargetTime
func (r *Runner) Run(ctx context.Context) error {
	workDir := filepath.Join(r.cfg.TempDir, fmt.Sprintf("restore-%d", time.Now().Unix()))
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}
	defer func() {
		r.logger.Info("cleaning up work dir", zap.String("dir", workDir))
		os.RemoveAll(workDir)
	}()

	// 1. Resolve backup ID
	meta, err := r.resolveBackup(ctx)
	if err != nil {
		return fmt.Errorf("resolve backup: %w", err)
	}
	r.logger.Info("restoring from backup",
		zap.String("backup_id", meta.BackupID),
		zap.String("binlog_file", meta.BinlogFile),
		zap.Uint32("binlog_pos", meta.BinlogPos),
		zap.Time("backup_time", meta.FinishTime),
	)

	// 2. Download + extract full backup
	backupDir := filepath.Join(workDir, "xtrabackup")
	if err := r.downloadAndExtract(ctx, meta.BackupID, backupDir); err != nil {
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
	// Give MySQL a moment to start
	time.Sleep(5 * time.Second)

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

	// Find the latest full backup whose finish time is <= TargetTime
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

// loadMeta loads backup metadata from S3.
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

// downloadAndExtract downloads the backup tar.gz and extracts it to destDir.
func (r *Runner) downloadAndExtract(ctx context.Context, backupID, destDir string) error {
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	s3Key := fmt.Sprintf("%s%s/backup.tar.gz", r.cfg.S3Prefix, backupID)
	r.logger.Info("downloading backup", zap.String("s3_key", s3Key))

	rc, err := r.store.GetObject(ctx, s3Key)
	if err != nil {
		return fmt.Errorf("get backup object: %w", err)
	}
	defer rc.Close()

	return extractTarGz(rc, destDir)
}

// extractTarGz extracts a .tar.gz stream into destDir.
func extractTarGz(r io.Reader, destDir string) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar next: %w", err)
		}

		target := filepath.Join(destDir, hdr.Name)
		// Prevent path traversal
		if !strings.HasPrefix(target, filepath.Clean(destDir)+string(os.PathSeparator)) && target != filepath.Clean(destDir) {
			return fmt.Errorf("illegal tar path: %s", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
	}
	return nil
}

// prepareBackup runs xtrabackup --prepare on the extracted backup directory.
func (r *Runner) prepareBackup(ctx context.Context, backupDir string) error {
	r.logger.Info("preparing backup", zap.String("dir", backupDir))
	cmd := exec.CommandContext(ctx, r.cfg.XtrabackupBin,
		"--prepare",
		fmt.Sprintf("--target-dir=%s", backupDir),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// copyBack runs xtrabackup --copy-back to restore data files to MySQL data directory.
func (r *Runner) copyBack(ctx context.Context, backupDir string) error {
	r.logger.Info("copying back data files",
		zap.String("backup_dir", backupDir),
		zap.String("data_dir", r.cfg.DataDir),
	)
	cmd := exec.CommandContext(ctx, r.cfg.XtrabackupBin,
		"--copy-back",
		fmt.Sprintf("--target-dir=%s", backupDir),
		fmt.Sprintf("--datadir=%s", r.cfg.DataDir),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// applyBinlogs downloads binlog files from S3 and applies them up to TargetTime.
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

	// Build mysqlbinlog command
	args := []string{
		"--read-from-remote-binlog-dump-format=NON_BLOCKING",
		fmt.Sprintf("--start-position=%d", meta.BinlogPos),
	}
	if !r.cfg.TargetTime.IsZero() {
		args = append(args, fmt.Sprintf("--stop-datetime=%s", r.cfg.TargetTime.Format("2006-01-02 15:04:05")))
	}
	// We pipe the binlog data via stdin using "-" as the file argument
	args = append(args, "-")

	mysqlbinlogCmd := exec.CommandContext(ctx, r.cfg.MysqlbinlogBin, args...)

	// mysql client to apply the SQL
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

	// Wire: S3 binlog stream -> mysqlbinlog stdin -> mysql stdin
	binlogReader := binlog.PipeReader(ctx, r.store, keys)
	defer binlogReader.Close()

	mysqlbinlogCmd.Stdin = binlogReader
	mysqlbinlogOut, err := mysqlbinlogCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("mysqlbinlog stdout pipe: %w", err)
	}
	mysqlbinlogCmd.Stderr = os.Stderr

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

// runShellCmd runs an arbitrary shell command (used for start/stop MySQL).
func (r *Runner) runShellCmd(cmd string) error {
	if cmd == "" {
		r.logger.Warn("no shell command configured, skipping")
		return nil
	}
	r.logger.Info("running shell command", zap.String("cmd", cmd))
	c := exec.Command("sh", "-c", cmd)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}
