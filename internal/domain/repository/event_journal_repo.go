package repository

import (
	"context"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

// EventJournalRepository defines the port for cold-path event journaling (`notification_event_journal`)
// and bounded `Last-Event-ID` replay buffer lookups.
type EventJournalRepository interface {
	AppendEvent(ctx context.Context, event entity.NotificationEvent) error
	FetchReplayAfter(ctx context.Context, tenantID string, lastEventID string, limit int) ([]entity.NotificationEvent, error)
}
