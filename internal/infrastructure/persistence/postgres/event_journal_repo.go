// Package postgres provides cold-path PostgreSQL 16+ persistence adapters
// for notification subscriptions, webhook targets, and cold-path event journaling.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

const (
	sqlInsertJournalEvent = `
INSERT INTO notification_event_journal (
    event_id, tenant_id, user_id, source_service, event_type,
    severity, is_terminal, payload_json, occurred_at, dispatched_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
ON CONFLICT (event_id) DO NOTHING`

	sqlFetchReplayAfter = `
SELECT event_id, tenant_id, user_id, source_service, event_type,
       severity, is_terminal, payload_json, occurred_at
FROM notification_event_journal
WHERE tenant_id = $1 AND event_id > $2
ORDER BY event_id ASC
LIMIT $3`
)

// EventJournalRepo implements repository.EventJournalRepository over cold-path PostgreSQL 16+.
type EventJournalRepo struct {
	pool *Pool
}

// NewEventJournalRepo creates a new cold-path PostgreSQL EventJournalRepo.
func NewEventJournalRepo(pool *Pool) *EventJournalRepo {
	return &EventJournalRepo{pool: pool}
}

// AppendEvent writes an event to the cold-path notification_event_journal table.
func (r *EventJournalRepo) AppendEvent(ctx context.Context, event entity.NotificationEvent) error {
	if r.pool == nil {
		return errors.New("postgres pool is nil")
	}
	payloadBytes := event.PayloadJSON
	if len(payloadBytes) == 0 {
		payloadBytes = json.RawMessage(`{}`)
	}
	err := r.pool.Exec(
		ctx,
		sqlInsertJournalEvent,
		event.EventID,
		event.TenantID,
		event.UserID,
		event.SourceService,
		event.EventType,
		string(event.Severity),
		event.IsTerminal,
		string(payloadBytes),
		event.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf("append event to journal: %w", err)
	}
	return nil
}

// FetchReplayAfter fetches events for tenant_id strictly after lastEventID from cold storage.
func (r *EventJournalRepo) FetchReplayAfter(
	ctx context.Context,
	tenantID string,
	lastEventID string,
	limit int,
) ([]entity.NotificationEvent, error) {
	if r.pool == nil {
		return nil, errors.New("postgres pool is nil")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, sqlFetchReplayAfter, tenantID, lastEventID, limit)
	if err != nil {
		return nil, fmt.Errorf("query journal replay events: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var events []entity.NotificationEvent
	for rows.Next() {
		var (
			eventID       string
			tID           string
			userID        *string
			sourceService string
			eventType     string
			rawSeverity   string
			isTerminal    bool
			rawPayload    string
			occurredAt    time.Time
		)
		if err := rows.Scan(
			&eventID,
			&tID,
			&userID,
			&sourceService,
			&eventType,
			&rawSeverity,
			&isTerminal,
			&rawPayload,
			&occurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan journal event: %w", err)
		}

		domainSev, sErr := entity.ParseSeverity(rawSeverity)
		if sErr != nil {
			domainSev = entity.SeverityInfo
		}

		uID := ""
		if userID != nil {
			uID = *userID
		}

		events = append(events, entity.NotificationEvent{
			EventID:       eventID,
			TenantID:      tID,
			UserID:        uID,
			SourceService: sourceService,
			EventType:     eventType,
			Severity:      domainSev,
			IsTerminal:    isTerminal,
			OccurredAt:    occurredAt.UTC(),
			PayloadJSON:   json.RawMessage(rawPayload),
		})
	}

	return events, rows.Err()
}

// InMemoryEventJournalRepo provides a thread-safe in-memory event journal repository for testing.
type InMemoryEventJournalRepo struct {
	mu     sync.RWMutex
	events map[string][]entity.NotificationEvent // tenant_id -> events sorted by event_id
}

// NewInMemoryEventJournalRepo creates a new in-memory journal repository.
func NewInMemoryEventJournalRepo() *InMemoryEventJournalRepo {
	return &InMemoryEventJournalRepo{
		events: make(map[string][]entity.NotificationEvent),
	}
}

// AppendEvent stores the event in the in-memory journal.
func (m *InMemoryEventJournalRepo) AppendEvent(_ context.Context, event entity.NotificationEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	list := m.events[event.TenantID]
	list = append(list, event)
	m.events[event.TenantID] = list
	return nil
}

// FetchReplayAfter returns events strictly after lastEventID up to limit.
func (m *InMemoryEventJournalRepo) FetchReplayAfter(
	_ context.Context,
	tenantID string,
	lastEventID string,
	limit int,
) ([]entity.NotificationEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.events[tenantID]
	if len(list) == 0 {
		return nil, nil
	}

	var res []entity.NotificationEvent
	for _, ev := range list {
		if lastEventID != "" && ev.EventID <= lastEventID {
			continue
		}
		res = append(res, ev)
		if limit > 0 && len(res) >= limit {
			break
		}
	}

	return res, nil
}
