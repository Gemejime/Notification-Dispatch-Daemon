// Package query implements application-layer query handlers for rag-notification-dispatch-daemon.
package query

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/repository"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

var (
	// ErrBackfillRateLimited is returned when a tenant's reconnect backfill requests exceed the token-bucket rate limit.
	ErrBackfillRateLimited = errors.New("backfill rate limit exceeded")
)

const (
	// DefaultBackfillRPS is the default token bucket fill rate for Last-Event-ID backfill queries.
	DefaultBackfillRPS = 50.0
	// DefaultBackfillBurst is the maximum burst capacity for backfill queries per tenant.
	DefaultBackfillBurst = 100.0
)

// ReplayStoreReader defines the outbound port for querying the bounded Redis Stream replay buffer.
type ReplayStoreReader interface {
	FetchReplayEvents(ctx context.Context, tenantID string, lastEventID string, limit int) ([]entity.NotificationEvent, error)
}

// FetchReplayEventsQuery carries input parameters for Last-Event-ID backfill.
type FetchReplayEventsQuery struct {
	TenantID    string
	UserID      string
	LastEventID string
	Limit       int
}

// FetchReplayEventsResult carries the replayed events and high watermark.
type FetchReplayEventsResult struct {
	Events                  []entity.NotificationEvent
	HighestWatermarkEventID string
}

type tenantTokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

// FetchReplayEventsHandler orchestrates rate-limited Last-Event-ID replay backfill over Redis Streams
// with fallback to PostgreSQL cold-journal (ADR-004, Runbook 2).
type FetchReplayEventsHandler struct {
	replayStore ReplayStoreReader
	journalRepo repository.EventJournalRepository
	logger      *slog.Logger
	metrics     *observability.MetricsRegistry

	rateMu  sync.Mutex
	buckets map[string]*tenantTokenBucket
	rps     float64
	burst   float64
	nowFn   func() time.Time
}

// NewFetchReplayEventsHandler creates a new FetchReplayEventsHandler.
func NewFetchReplayEventsHandler(
	replayStore ReplayStoreReader,
	journalRepo repository.EventJournalRepository,
	rps, burst float64,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *FetchReplayEventsHandler {
	if rps <= 0 {
		rps = DefaultBackfillRPS
	}
	if burst <= 0 {
		burst = DefaultBackfillBurst
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return &FetchReplayEventsHandler{
		replayStore: replayStore,
		journalRepo: journalRepo,
		logger:      logger,
		metrics:     metrics,
		buckets:     make(map[string]*tenantTokenBucket),
		rps:         rps,
		burst:       burst,
		nowFn:       func() time.Time { return time.Now().UTC() },
	}
}

// AllowTenantBackfill checks and consumes a token from the tenant's backfill bucket.
func (h *FetchReplayEventsHandler) AllowTenantBackfill(tenantID string) bool {
	h.rateMu.Lock()
	defer h.rateMu.Unlock()

	now := h.nowFn()
	tb, ok := h.buckets[tenantID]
	if !ok {
		tb = &tenantTokenBucket{
			tokens:     h.burst - 1,
			lastRefill: now,
		}
		h.buckets[tenantID] = tb
		return true
	}

	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * h.rps
	if tb.tokens > h.burst {
		tb.tokens = h.burst
	}
	tb.lastRefill = now

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}

	return false
}

// FetchReplayEvents implements sse.ReplayStreamer for direct integration into StreamHandler.
func (h *FetchReplayEventsHandler) FetchReplayEvents(
	ctx context.Context,
	tenantID string,
	lastEventID string,
	limit int,
) ([]entity.NotificationEvent, error) {
	res, err := h.Handle(ctx, FetchReplayEventsQuery{
		TenantID:    tenantID,
		LastEventID: lastEventID,
		Limit:       limit,
	})
	if err != nil {
		return nil, err
	}
	return res.Events, nil
}

// Handle executes rate-limited replay retrieval against Redis Stream and cold-path journal fallback.
func (h *FetchReplayEventsHandler) Handle(
	ctx context.Context,
	q FetchReplayEventsQuery,
) (FetchReplayEventsResult, error) {
	if q.TenantID == "" {
		return FetchReplayEventsResult{}, errors.New("tenant_id is required")
	}
	if q.Limit <= 0 {
		q.Limit = 100
	}

	// 1. Enforce per-tenant backfill rate limiting (Runbook 2, protects Redis CPU < 60%)
	if !h.AllowTenantBackfill(q.TenantID) {
		h.logger.Warn("tenant backfill query rate limited",
			slog.String("tenant_id", q.TenantID),
			slog.String("last_event_id", q.LastEventID),
		)
		return FetchReplayEventsResult{}, ErrBackfillRateLimited
	}

	var events []entity.NotificationEvent

	// 2. Query bounded Redis Stream replay buffer: rag:notify:replay:{tenant_id}
	if h.replayStore != nil {
		streamEvents, err := h.replayStore.FetchReplayEvents(ctx, q.TenantID, q.LastEventID, q.Limit)
		if err != nil {
			h.logger.Warn("redis stream replay fetch error",
				slog.String("tenant_id", q.TenantID),
				slog.String("error", err.Error()),
			)
		} else {
			events = streamEvents
		}
	}

	// 3. Fall back to PostgreSQL event journal if Redis stream has no events or lastEventID predates stream window
	if len(events) == 0 && h.journalRepo != nil && q.LastEventID != "" {
		journalEvents, jErr := h.journalRepo.FetchReplayAfter(ctx, q.TenantID, q.LastEventID, q.Limit)
		if jErr != nil {
			h.logger.Warn("cold journal replay fetch error",
				slog.String("tenant_id", q.TenantID),
				slog.String("last_event_id", q.LastEventID),
				slog.String("error", jErr.Error()),
			)
		} else {
			events = journalEvents
		}
	}

	// 4. Resolve highest watermark event ID for live ring-buffer deduplication
	var watermark string
	if len(events) > 0 {
		watermark = events[len(events)-1].EventID
	}

	return FetchReplayEventsResult{
		Events:                  events,
		HighestWatermarkEventID: watermark,
	}, nil
}
