package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

const (
	sqlListAllWebhookTargets = `
SELECT target_id, tenant_id, target_url, secret_ref, active, event_types, updated_at
FROM tenant_webhook_targets
WHERE active = TRUE
ORDER BY tenant_id, target_id`

	sqlListWebhookTargetsByTenant = `
SELECT target_id, tenant_id, target_url, secret_ref, active, event_types, updated_at
FROM tenant_webhook_targets
WHERE tenant_id = $1 AND active = TRUE
ORDER BY target_id`

	sqlInsertEventJournal = `
INSERT INTO notification_event_journal (
    event_id, tenant_id, user_id, source_service, event_type, severity, is_terminal, payload_json, occurred_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (event_id) DO NOTHING`

	sqlFetchEventReplay = `
SELECT event_id, tenant_id, user_id, source_service, event_type, severity, is_terminal, payload_json, occurred_at
FROM notification_event_journal
WHERE tenant_id = $1 AND event_id > $2
ORDER BY event_id ASC
LIMIT $3`
)

// WebhookRepo implements repository.WebhookTargetRepository and repository.EventJournalRepository
// over cold-path PostgreSQL 16+ (`tenant_webhook_targets` and `notification_event_journal`).
type WebhookRepo struct {
	pool *Pool
}

// NewWebhookRepo creates a cold-path PostgreSQL WebhookRepo.
func NewWebhookRepo(pool *Pool) *WebhookRepo {
	return &WebhookRepo{pool: pool}
}

// ListAllActive fetches all active customer webhook targets during pod cold-start cache warm-up.
func (r *WebhookRepo) ListAllActive(ctx context.Context) ([]entity.WebhookTarget, error) {
	if r.pool == nil {
		return nil, errors.New("postgres pool is nil")
	}
	rows, err := r.pool.Query(ctx, sqlListAllWebhookTargets)
	if err != nil {
		return nil, fmt.Errorf("query all webhook targets: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	return scanWebhookTargets(rows)
}

// ListByTenant fetches active customer webhook targets for a single tenant during
// asynchronous `tenant.config.updated` cache reload-on-invalidation.
func (r *WebhookRepo) ListByTenant(ctx context.Context, tenantID string) ([]entity.WebhookTarget, error) {
	if r.pool == nil {
		return nil, errors.New("postgres pool is nil")
	}
	if tenantID == "" {
		return nil, errors.New("tenant_id is required")
	}
	rows, err := r.pool.Query(ctx, sqlListWebhookTargetsByTenant, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query webhook targets for tenant %s: %w", tenantID, err)
	}
	defer func() {
		_ = rows.Close()
	}()
	return scanWebhookTargets(rows)
}

// AppendEvent records a notification event into the cold-path `notification_event_journal` table.
func (r *WebhookRepo) AppendEvent(ctx context.Context, event entity.NotificationEvent) error {
	if r.pool == nil {
		return errors.New("postgres pool is nil")
	}
	return r.pool.Exec(
		ctx,
		sqlInsertEventJournal,
		event.EventID,
		event.TenantID,
		event.UserID,
		event.SourceService,
		event.EventType,
		string(event.Severity),
		event.IsTerminal,
		event.PayloadJSON,
		event.OccurredAt.UTC(),
	)
}

// FetchReplayAfter queries missed events after lastEventID from the cold-path journal fallback.
func (r *WebhookRepo) FetchReplayAfter(
	ctx context.Context,
	tenantID string,
	lastEventID string,
	limit int,
) ([]entity.NotificationEvent, error) {
	if r.pool == nil {
		return nil, errors.New("postgres pool is nil")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, sqlFetchEventReplay, tenantID, lastEventID, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch replay events: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var events []entity.NotificationEvent
	for rows.Next() {
		var ev entity.NotificationEvent
		var rawSev string
		var occurredAt time.Time
		if err := rows.Scan(
			&ev.EventID,
			&ev.TenantID,
			&ev.UserID,
			&ev.SourceService,
			&ev.EventType,
			&rawSev,
			&ev.IsTerminal,
			&ev.PayloadJSON,
			&occurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan journal event: %w", err)
		}
		sev, err := entity.ParseSeverity(rawSev)
		if err != nil {
			return nil, err
		}
		ev.Severity = sev
		ev.OccurredAt = occurredAt.UTC()
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func scanWebhookTargets(rows Rows) ([]entity.WebhookTarget, error) {
	var result []entity.WebhookTarget
	for rows.Next() {
		var t entity.WebhookTarget
		var updatedAt time.Time
		if err := rows.Scan(
			&t.TargetID,
			&t.TenantID,
			&t.TargetURL,
			&t.SecretRef,
			&t.Active,
			&t.EventTypes,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan webhook target row: %w", err)
		}
		t.SignatureHeaderName = entity.DefaultWebhookSignatureHeader
		t.UpdatedAt = updatedAt.UTC()
		result = append(result, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook target rows: %w", err)
	}
	return result, nil
}
