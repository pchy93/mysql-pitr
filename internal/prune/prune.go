// Package prune implements backup retention policy management.
package prune

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mysql-pitr/mysql-pitr/internal/backup"
	"github.com/mysql-pitr/mysql-pitr/internal/s3store"
)

// Config holds prune configuration.
type Config struct {
	S3Prefix      string
	RetentionDays int  // Delete backups older than this
	RetentionCount int // Keep at most N backups (0 = unlimited)
	DryRun        bool // Only show what would be deleted
}

// Runner performs backup pruning.
type Runner struct {
	cfg    Config
	store  *s3store.Client
	logger *zap.Logger
}

// BackupInfo holds info about a backup for pruning decisions.
type BackupInfo struct {
	BackupID   string
	FinishTime time.Time
	Status     string
	MetaKey    string
	DataKey    string
}

// New creates a new prune Runner.
func New(cfg Config, store *s3store.Client, logger *zap.Logger) *Runner {
	return &Runner{cfg: cfg, store: store, logger: logger}
}

// Run executes the prune operation.
func (r *Runner) Run(ctx context.Context) error {
	backups, err := r.List(ctx)
	if err != nil {
		return fmt.Errorf("list backups: %w", err)
	}

	r.logger.Info("found backups",
		zap.Int("total", len(backups)),
		zap.Int("retention_days", r.cfg.RetentionDays),
		zap.Int("retention_count", r.cfg.RetentionCount),
	)

	var toDelete []BackupInfo
	now := time.Now().UTC()
	cutoff := now.AddDate(0, 0, -r.cfg.RetentionDays)

	for i, bk := range backups {
		shouldDelete := false

		// Check retention count (keep N most recent)
		if r.cfg.RetentionCount > 0 && i >= r.cfg.RetentionCount {
			shouldDelete = true
		}

		// Check retention days
		if r.cfg.RetentionDays > 0 && bk.FinishTime.Before(cutoff) {
			shouldDelete = true
		}

		if shouldDelete {
			toDelete = append(toDelete, bk)
		}
	}

	if len(toDelete) == 0 {
		r.logger.Info("no backups to prune")
		return nil
	}

	r.logger.Info("pruning backups",
		zap.Int("to_delete", len(toDelete)),
		zap.Int("to_keep", len(backups)-len(toDelete)),
		zap.Bool("dry_run", r.cfg.DryRun),
	)

	for _, bk := range toDelete {
		r.logger.Info("backup to delete",
			zap.String("backup_id", bk.BackupID),
			zap.Time("finish_time", bk.FinishTime),
			zap.String("status", bk.Status),
		)

		if r.cfg.DryRun {
			continue
		}

		if err := r.deleteBackup(ctx, bk); err != nil {
			r.logger.Error("failed to delete backup",
				zap.String("backup_id", bk.BackupID),
				zap.Error(err),
			)
			continue
		}
		r.logger.Info("deleted backup", zap.String("backup_id", bk.BackupID))
	}

	if r.cfg.DryRun {
		r.logger.Info("dry run complete, no backups were deleted")
	}
	return nil
}

// List returns all backups sorted by finish time (newest first).
func (r *Runner) List(ctx context.Context) ([]BackupInfo, error) {
	objects, err := r.store.ListObjects(ctx, r.cfg.S3Prefix)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	var backups []BackupInfo
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

		rc, err := r.store.GetObject(ctx, obj.Key)
		if err != nil {
			continue
		}

		var meta backup.Meta
		if err := json.NewDecoder(rc).Decode(&meta); err != nil {
			rc.Close()
			continue
		}
		rc.Close()

		dataKey := fmt.Sprintf("%s%s/backup.xb", r.cfg.S3Prefix, backupID)
		switch meta.Compression {
		case "gzip":
			dataKey = fmt.Sprintf("%s%s/backup.xb.gz", r.cfg.S3Prefix, backupID)
		case "zstd":
			dataKey = fmt.Sprintf("%s%s/backup.xb.zst", r.cfg.S3Prefix, backupID)
		}

		backups = append(backups, BackupInfo{
			BackupID:   backupID,
			FinishTime: meta.FinishTime,
			Status:     meta.Status,
			MetaKey:    obj.Key,
			DataKey:    dataKey,
		})
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].FinishTime.After(backups[j].FinishTime)
	})

	return backups, nil
}

func (r *Runner) deleteBackup(ctx context.Context, bk BackupInfo) error {
	r.store.DeleteObject(ctx, bk.DataKey)
	return r.store.DeleteObject(ctx, bk.MetaKey)
}
