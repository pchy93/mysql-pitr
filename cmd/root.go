// Package cmd implements the CLI commands for mysql-pitr.
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/backup"
	"github.com/mysql-pitr/mysql-pitr/internal/binlog"
	"github.com/mysql-pitr/mysql-pitr/internal/restore"
	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

// globalFlags are shared S3 flags.
type globalFlags struct {
	s3Endpoint   string
	s3Region     string
	s3Bucket     string
	s3AccessKey  string
	s3SecretKey  string
	s3PathStyle  bool
	s3Prefix     string
}

var gf globalFlags

var rootCmd = &cobra.Command{
	Use:   "mysql-pitr",
	Short: "MySQL Point-In-Time Recovery tool",
	Long: `mysql-pitr provides full backup and point-in-time recovery for MySQL
using xtrabackup for full backups and binlog streaming for PITR.`,
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global S3 flags
	rootCmd.PersistentFlags().StringVar(&gf.s3Endpoint, "s3-endpoint", "", "S3 endpoint URL (for MinIO or other S3-compatible)")
	rootCmd.PersistentFlags().StringVar(&gf.s3Region, "s3-region", "us-east-1", "S3 region")
	rootCmd.PersistentFlags().StringVar(&gf.s3Bucket, "s3-bucket", "", "S3 bucket name")
	rootCmd.PersistentFlags().StringVar(&gf.s3AccessKey, "s3-access-key", "", "S3 access key ID (or use AWS_ACCESS_KEY_ID env)")
	rootCmd.PersistentFlags().StringVar(&gf.s3SecretKey, "s3-secret-key", "", "S3 secret access key (or use AWS_SECRET_ACCESS_KEY env)")
	rootCmd.PersistentFlags().BoolVar(&gf.s3PathStyle, "s3-path-style", false, "Use path-style S3 addressing (needed for MinIO)")
	rootCmd.PersistentFlags().StringVar(&gf.s3Prefix, "s3-prefix", "mysql-pitr/", "S3 key prefix for all objects")

	rootCmd.AddCommand(backupCmd())
	rootCmd.AddCommand(binlogCmd())
	rootCmd.AddCommand(restoreCmd())
	rootCmd.AddCommand(listCmd())
}

func newS3Client() (*s3store.Client, error) {
	if gf.s3Bucket == "" {
		return nil, fmt.Errorf("--s3-bucket is required")
	}

	// Fall back to environment variables for credentials
	accessKey := gf.s3AccessKey
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	secretKey := gf.s3SecretKey
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	return s3store.New(s3store.Config{
		Endpoint:        gf.s3Endpoint,
		Region:          gf.s3Region,
		Bucket:          gf.s3Bucket,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		ForcePathStyle:  gf.s3PathStyle,
	})
}

func newLogger(verbose bool) *zap.Logger {
	var logger *zap.Logger
	if verbose {
		logger, _ = zap.NewDevelopment()
	} else {
		logger, _ = zap.NewProduction()
	}
	return logger
}

// ── backup ────────────────────────────────────────────────────────────────────

func backupCmd() *cobra.Command {
	var (
		host          string
		port          int
		user          string
		password      string
		socket        string
		xtrabackupBin string
		tempDir       string
		compression   string
		parallel      int
		verbose       bool
	)

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Run a full backup using xtrabackup and upload to S3",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := newLogger(verbose)
			defer logger.Sync()

			store, err := newS3Client()
			if err != nil {
				return err
			}

			runner := backup.New(backup.Config{
				Host:          host,
				Port:          port,
				User:          user,
				Password:      password,
				Socket:        socket,
				XtrabackupBin: xtrabackupBin,
				TempDir:       tempDir,
				S3Prefix:      gf.s3Prefix,
				Compression:   compression,
				Parallel:      parallel,
			}, store, logger)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			backupID, err := runner.Run(ctx)
			if err != nil {
				return fmt.Errorf("backup failed: %w", err)
			}
			fmt.Printf("Backup completed: %s\n", backupID)
			return nil
		},
	}

	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "MySQL host")
	cmd.Flags().IntVar(&port, "port", 3306, "MySQL port")
	cmd.Flags().StringVar(&user, "user", "root", "MySQL user")
	cmd.Flags().StringVar(&password, "password", "", "MySQL password")
	cmd.Flags().StringVar(&socket, "socket", "", "MySQL unix socket (overrides host/port)")
	cmd.Flags().StringVar(&xtrabackupBin, "xtrabackup-bin", "xtrabackup", "Path to xtrabackup binary")
	cmd.Flags().StringVar(&tempDir, "temp-dir", os.TempDir(), "Temporary directory for metadata files")
	cmd.Flags().StringVar(&compression, "compression", "zstd", "Compression algorithm: none, gzip, zstd")
	cmd.Flags().IntVar(&parallel, "parallel", 4, "Parallel threads for backup and upload")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")

	return cmd
}

// ── binlog ────────────────────────────────────────────────────────────────────

func binlogCmd() *cobra.Command {
	var (
		host           string
		port           int
		user           string
		password       string
		mysqlbinlogBin string
		tempDir        string
		serverID       uint32
		startFile      string
		startPos       uint32
		compression    string
		parallel       int
		verbose        bool
	)

	cmd := &cobra.Command{
		Use:   "binlog",
		Short: "Continuously pull binlog from MySQL and upload to S3",
		Long: `Pulls binlog events from MySQL using mysqlbinlog --read-from-remote-server
and uploads each completed binlog file to S3. Runs until interrupted.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := newLogger(verbose)
			defer logger.Sync()

			store, err := newS3Client()
			if err != nil {
				return err
			}

			puller := binlog.New(binlog.Config{
				Host:           host,
				Port:           port,
				User:           user,
				Password:       password,
				MysqlbinlogBin: mysqlbinlogBin,
				TempDir:        tempDir,
				S3Prefix:       gf.s3Prefix,
				ServerID:       serverID,
				StartFile:      startFile,
				StartPos:       startPos,
				Compression:    compression,
				Parallel:       parallel,
			}, store, logger)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Handle SIGINT/SIGTERM gracefully
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				logger.Info("received signal, stopping binlog puller")
				cancel()
			}()

			if err := puller.Run(ctx); err != nil && err != context.Canceled {
				return fmt.Errorf("binlog puller: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "MySQL host")
	cmd.Flags().IntVar(&port, "port", 3306, "MySQL port")
	cmd.Flags().StringVar(&user, "user", "root", "MySQL user")
	cmd.Flags().StringVar(&password, "password", "", "MySQL password")
	cmd.Flags().StringVar(&mysqlbinlogBin, "mysqlbinlog-bin", "mysqlbinlog", "Path to mysqlbinlog binary")
	cmd.Flags().StringVar(&tempDir, "temp-dir", os.TempDir(), "Temporary directory for binlog files")
	cmd.Flags().Uint32Var(&serverID, "server-id", 99999, "Replication server ID (must be unique in cluster)")
	cmd.Flags().StringVar(&startFile, "start-file", "", "Binlog file to start from (e.g. binlog.000001)")
	cmd.Flags().Uint32Var(&startPos, "start-pos", 4, "Binlog position to start from")
	cmd.Flags().StringVar(&compression, "compression", "none", "Compression: none, gzip, zstd")
	cmd.Flags().IntVar(&parallel, "parallel", 4, "Parallel upload workers")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")

	return cmd
}

// ── restore ───────────────────────────────────────────────────────────────────

func restoreCmd() *cobra.Command {
	var (
		backupID         string
		targetTimeStr    string
		dataDir          string
		tempDir          string
		targetHost       string
		targetPort       int
		targetUser       string
		targetPassword   string
		targetSocket     string
		xtrabackupBin    string
		mysqlbinlogBin   string
		mysqlBin         string
		mysqlShutdown    string
		mysqlStart       string
		parallel         int
		copyMethod       string
		verbose          bool
	)

	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore MySQL to a point in time from S3 backup",
		Long: `Downloads the full backup from S3, prepares it with xtrabackup,
restores data files, then applies binlogs up to the target time.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := newLogger(verbose)
			defer logger.Sync()

			store, err := newS3Client()
			if err != nil {
				return err
			}

			var targetTime time.Time
			if targetTimeStr != "" {
				targetTime, err = time.Parse("2006-01-02T15:04:05Z", targetTimeStr)
				if err != nil {
					// Try alternate format
					targetTime, err = time.Parse("2006-01-02 15:04:05", targetTimeStr)
					if err != nil {
						return fmt.Errorf("invalid target-time format, use 2006-01-02T15:04:05Z or '2006-01-02 15:04:05': %w", err)
					}
				}
			}

			runner := restore.New(restore.Config{
				BackupID:         backupID,
				TargetTime:       targetTime,
				S3Prefix:         gf.s3Prefix,
				DataDir:          dataDir,
				TempDir:          tempDir,
				TargetHost:       targetHost,
				TargetPort:       targetPort,
				TargetUser:       targetUser,
				TargetPassword:   targetPassword,
				TargetSocket:     targetSocket,
				XtrabackupBin:    xtrabackupBin,
				MysqlbinlogBin:   mysqlbinlogBin,
				MysqlBin:         mysqlBin,
				MysqlShutdownCmd: mysqlShutdown,
				MysqlStartCmd:    mysqlStart,
				Parallel:         parallel,
				CopyMethod:       copyMethod,
			}, store, logger)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				logger.Warn("received signal during restore - restore may be incomplete")
				cancel()
			}()

			return runner.Run(ctx)
		},
	}

	cmd.Flags().StringVar(&backupID, "backup-id", "", "Backup ID to restore (default: latest before target-time)")
	cmd.Flags().StringVar(&targetTimeStr, "target-time", "", "Point-in-time to restore to (2006-01-02T15:04:05Z)")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&tempDir, "temp-dir", os.TempDir(), "Temporary directory")
	cmd.Flags().StringVar(&targetHost, "target-host", "127.0.0.1", "Target MySQL host")
	cmd.Flags().IntVar(&targetPort, "target-port", 3306, "Target MySQL port")
	cmd.Flags().StringVar(&targetUser, "target-user", "root", "Target MySQL user")
	cmd.Flags().StringVar(&targetPassword, "target-password", "", "Target MySQL password")
	cmd.Flags().StringVar(&targetSocket, "target-socket", "", "Target MySQL unix socket")
	cmd.Flags().StringVar(&xtrabackupBin, "xtrabackup-bin", "xtrabackup", "Path to xtrabackup binary")
	cmd.Flags().StringVar(&mysqlbinlogBin, "mysqlbinlog-bin", "mysqlbinlog", "Path to mysqlbinlog binary")
	cmd.Flags().StringVar(&mysqlBin, "mysql-bin", "mysql", "Path to mysql client binary")
	cmd.Flags().StringVar(&mysqlShutdown, "mysql-shutdown-cmd", "", "Command to stop MySQL (e.g. 'systemctl stop mysqld')")
	cmd.Flags().StringVar(&mysqlStart, "mysql-start-cmd", "", "Command to start MySQL (e.g. 'systemctl start mysqld')")
	cmd.Flags().IntVar(&parallel, "parallel", 4, "Parallel download threads")
	cmd.Flags().StringVar(&copyMethod, "copy-method", "copy", "Restore method: copy (safe), move (fast, empties backup), hardlink (fast, same filesystem)")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")

	return cmd
}

// ── list ──────────────────────────────────────────────────────────────────────

func listCmd() *cobra.Command {
	var (
		outputJSON bool
		verbose    bool
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available backups in S3",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := newLogger(verbose)
			defer logger.Sync()

			store, err := newS3Client()
			if err != nil {
				return err
			}

			ctx := context.Background()
			objects, err := store.ListObjects(ctx, gf.s3Prefix)
			if err != nil {
				return fmt.Errorf("list objects: %w", err)
			}

			type BackupEntry struct {
				BackupID   string    `json:"backup_id"`
				StartTime  time.Time `json:"start_time"`
				FinishTime time.Time `json:"finish_time"`
				BinlogFile string    `json:"binlog_file"`
				BinlogPos  uint32    `json:"binlog_pos"`
				GTIDSet    string    `json:"gtid_set,omitempty"`
			}

			var entries []BackupEntry
			for _, obj := range objects {
				if len(obj.Key) == 0 {
					continue
				}
				// Only process metadata files
				parts := splitKey(obj.Key, gf.s3Prefix)
				if len(parts) != 2 || parts[1] != "backup.meta.json" {
					continue
				}
				backupID := parts[0]

				rc, err := store.GetObject(ctx, obj.Key)
				if err != nil {
					logger.Warn("failed to read meta", zap.String("key", obj.Key), zap.Error(err))
					continue
				}
				var meta backup.Meta
				if err := json.NewDecoder(rc).Decode(&meta); err != nil {
					rc.Close()
					logger.Warn("failed to decode meta", zap.String("key", obj.Key), zap.Error(err))
					continue
				}
				rc.Close()

				_ = backupID
				entries = append(entries, BackupEntry{
					BackupID:   meta.BackupID,
					StartTime:  meta.StartTime,
					FinishTime: meta.FinishTime,
					BinlogFile: meta.BinlogFile,
					BinlogPos:  meta.BinlogPos,
					GTIDSet:    meta.GTIDSet,
				})
			}

			if outputJSON {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(entries)
			}

			if len(entries) == 0 {
				fmt.Println("No backups found.")
				return nil
			}

			fmt.Printf("%-30s  %-25s  %-25s  %-20s  %s\n",
				"BACKUP ID", "START TIME", "FINISH TIME", "BINLOG FILE", "POS")
			fmt.Println(repeat("-", 120))
			for _, e := range entries {
				fmt.Printf("%-30s  %-25s  %-25s  %-20s  %d\n",
					e.BackupID,
					e.StartTime.Format(time.RFC3339),
					e.FinishTime.Format(time.RFC3339),
					e.BinlogFile,
					e.BinlogPos,
				)
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&outputJSON, "json", false, "Output in JSON format")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")

	return cmd
}

// splitKey splits "prefix/backupID/file" into ["backupID", "file"] after stripping prefix.
func splitKey(key, prefix string) []string {
	trimmed := key
	if len(trimmed) > len(prefix) {
		trimmed = trimmed[len(prefix):]
	}
	var parts []string
	cur := ""
	for _, c := range trimmed {
		if c == '/' {
			if cur != "" {
				parts = append(parts, cur)
				cur = ""
			}
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		parts = append(parts, cur)
	}
	return parts
}

func repeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
