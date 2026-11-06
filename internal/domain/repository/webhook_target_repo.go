package repository

import (
	"context"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

// WebhookTargetRepository defines the cold-path persistence port for querying customer
// webhook target metadata (`tenant_webhook_targets`) from PostgreSQL 16+ via PgBouncer.
// CRITICAL INVARIANT (ADR-004): Never queried synchronously on the hot CDC ingestion path.
type WebhookTargetRepository interface {
	ListAllActive(ctx context.Context) ([]entity.WebhookTarget, error)
	ListByTenant(ctx context.Context, tenantID string) ([]entity.WebhookTarget, error)
}
