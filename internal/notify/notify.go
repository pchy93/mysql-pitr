// Package notify implements notification support via webhooks.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Config holds notification configuration.
type Config struct {
	WebhookURL string
	Timeout    time.Duration
}

// Event represents a notification event.
type Event struct {
	Type      string            `json:"type"`       // "backup_started", "backup_completed", "backup_failed", etc.
	Timestamp time.Time         `json:"timestamp"`
	BackupID  string            `json:"backup_id,omitempty"`
	Message   string            `json:"message"`
	Success   bool              `json:"success"`
	Error     string            `json:"error,omitempty"`
	Duration  time.Duration     `json:"duration,omitempty"`
	SizeBytes int64             `json:"size_bytes,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Notifier sends notifications.
type Notifier struct {
	cfg    Config
	logger *zap.Logger
	client *http.Client
}

// New creates a new Notifier.
func New(cfg Config, logger *zap.Logger) *Notifier {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &Notifier{
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: cfg.Timeout},
	}
}

// Send sends a notification event.
func (n *Notifier) Send(ctx context.Context, event Event) error {
	if n.cfg.WebhookURL == "" {
		n.logger.Debug("no webhook configured, skipping notification")
		return nil
	}

	event.Timestamp = time.Now().UTC()

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", n.cfg.WebhookURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "mysql-pitr/1.0")

	resp, err := n.client.Do(req)
	if err != nil {
		n.logger.Warn("webhook notification failed", zap.Error(err), zap.String("url", n.cfg.WebhookURL))
		return fmt.Errorf("send webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		n.logger.Warn("webhook returned error status",
			zap.Int("status", resp.StatusCode),
			zap.String("url", n.cfg.WebhookURL),
		)
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	n.logger.Debug("webhook notification sent",
		zap.String("type", event.Type),
		zap.String("backup_id", event.BackupID),
	)

	return nil
}

// NotifyBackupStarted sends a backup started notification.
func (n *Notifier) NotifyBackupStarted(ctx context.Context, backupID string) error {
	return n.Send(ctx, Event{
		Type:     "backup_started",
		BackupID: backupID,
		Message:  fmt.Sprintf("Backup %s started", backupID),
	})
}

// NotifyBackupCompleted sends a backup completed notification.
func (n *Notifier) NotifyBackupCompleted(ctx context.Context, backupID string, size int64, duration time.Duration) error {
	return n.Send(ctx, Event{
		Type:      "backup_completed",
		BackupID:  backupID,
		Message:   fmt.Sprintf("Backup %s completed successfully", backupID),
		Success:   true,
		SizeBytes: size,
		Duration:  duration,
	})
}

// NotifyBackupFailed sends a backup failed notification.
func (n *Notifier) NotifyBackupFailed(ctx context.Context, backupID string, errMsg string) error {
	return n.Send(ctx, Event{
		Type:     "backup_failed",
		BackupID: backupID,
		Message:  fmt.Sprintf("Backup %s failed", backupID),
		Success:  false,
		Error:    errMsg,
	})
}

// NotifyRestoreCompleted sends a restore completed notification.
func (n *Notifier) NotifyRestoreCompleted(ctx context.Context, backupID string, duration time.Duration) error {
	return n.Send(ctx, Event{
		Type:     "restore_completed",
		BackupID: backupID,
		Message:  fmt.Sprintf("Restore from %s completed successfully", backupID),
		Success:  true,
		Duration: duration,
	})
}

// NotifyRestoreFailed sends a restore failed notification.
func (n *Notifier) NotifyRestoreFailed(ctx context.Context, backupID string, errMsg string) error {
	return n.Send(ctx, Event{
		Type:     "restore_failed",
		BackupID: backupID,
		Message:  fmt.Sprintf("Restore from %s failed", backupID),
		Success:  false,
		Error:    errMsg,
	})
}
