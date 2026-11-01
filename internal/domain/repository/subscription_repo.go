// Package repository defines domain repository and cache ports for rag-notification-dispatch-daemon.
// All interfaces in this package are pure domain ports with zero external driver imports.
package repository

import (
	"context"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

// SubscriptionRepository defines the cold-path persistence port for querying authoritative
// tenant notification subscriptions from PostgreSQL 16+ (via PgBouncer).
// CRITICAL INVARIANT (ADR-004): This port is invoked ONLY on pod cold-start warm-up
// or asynchronous `tenant.config.updated` cache invalidations—never on the hot CDC path.
type SubscriptionRepository interface {
	ListAllActive(ctx context.Context) ([]entity.Subscription, error)
	ListByTenant(ctx context.Context, tenantID string) ([]entity.Subscription, error)
}

// SubscriptionCacheReader defines the hot-path read-only port for pod-local in-memory rule lookup.
type SubscriptionCacheReader interface {
	GetTenantRules(tenantID string) ([]entity.Subscription, []entity.WebhookTarget)
	IsWarmed() bool
	TenantCount() int
}

// SubscriptionCacheWriter defines the mutation port used during cold-start warm-up
// and asynchronous `tenant.config.updated` cache reload-on-invalidation.
type SubscriptionCacheWriter interface {
	ReplaceAll(subs []entity.Subscription, targets []entity.WebhookTarget)
	ReplaceTenantRules(tenantID string, subs []entity.Subscription, targets []entity.WebhookTarget)
	EvictTenant(tenantID string)
}

// SubscriptionCache combines hot-path read and cold-path reload operations for pod-local memory caching.
type SubscriptionCache interface {
	SubscriptionCacheReader
	SubscriptionCacheWriter
}
