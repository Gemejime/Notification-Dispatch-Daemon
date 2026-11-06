package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

const (
	sqlListAllSubscriptions = `
SELECT subscription_id, tenant_id, event_type_pattern, min_severity,
       sse_enabled, webhook_enabled, suppressed, rate_limit_rps, updated_at
FROM tenant_notification_subscriptions
ORDER BY tenant_id, subscription_id`

	sqlListSubscriptionsByTenant = `
SELECT subscription_id, tenant_id, event_type_pattern, min_severity,
       sse_enabled, webhook_enabled, suppressed, rate_limit_rps, updated_at
FROM tenant_notification_subscriptions
WHERE tenant_id = $1
ORDER BY subscription_id`
)

// SubscriptionRepo implements repository.SubscriptionRepository over cold-path PostgreSQL 16+.
type SubscriptionRepo struct {
	pool *Pool
}

// NewSubscriptionRepo creates a cold-path PostgreSQL SubscriptionRepo.
func NewSubscriptionRepo(pool *Pool) *SubscriptionRepo {
	return &SubscriptionRepo{pool: pool}
}

// ListAllActive fetches all tenant notification subscriptions during pod cold-start cache warm-up.
func (r *SubscriptionRepo) ListAllActive(ctx context.Context) ([]entity.Subscription, error) {
	if r.pool == nil {
		return nil, errors.New("postgres pool is nil")
	}
	rows, err := r.pool.Query(ctx, sqlListAllSubscriptions)
	if err != nil {
		return nil, fmt.Errorf("query all subscriptions: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	return scanSubscriptions(rows)
}

// ListByTenant fetches a single tenant's notification subscriptions during asynchronous
// `tenant.config.updated` cache reload-on-invalidation.
func (r *SubscriptionRepo) ListByTenant(ctx context.Context, tenantID string) ([]entity.Subscription, error) {
	if r.pool == nil {
		return nil, errors.New("postgres pool is nil")
	}
	if tenantID == "" {
		return nil, errors.New("tenant_id is required")
	}
	rows, err := r.pool.Query(ctx, sqlListSubscriptionsByTenant, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query subscriptions for tenant %s: %w", tenantID, err)
	}
	defer func() {
		_ = rows.Close()
	}()
	return scanSubscriptions(rows)
}

func scanSubscriptions(rows Rows) ([]entity.Subscription, error) {
	var result []entity.Subscription
	for rows.Next() {
		var sub entity.Subscription
		var rawSev string
		var updatedAt time.Time
		if err := rows.Scan(
			&sub.SubscriptionID,
			&sub.TenantID,
			&sub.EventTypePattern,
			&rawSev,
			&sub.SSEEnabled,
			&sub.WebhookEnabled,
			&sub.Suppressed,
			&sub.RateLimitRPS,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan subscription row: %w", err)
		}
		sev, err := entity.ParseSeverity(rawSev)
		if err != nil {
			return nil, err
		}
		sub.MinSeverity = sev
		sub.UpdatedAt = updatedAt.UTC()
		result = append(result, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate subscription rows: %w", err)
	}
	return result, nil
}
