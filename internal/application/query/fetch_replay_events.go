// Package query implements application-layer query handlers for rag-notification-dispatch-daemon.
package query

import (
	"context"
	"errors"
	"fmt"
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
	userID, _ := ctx.Value(entity.ContextKeyUserID).(string)
	res, err := h.Handle(ctx, FetchReplayEventsQuery{
		TenantID:    tenantID,
		UserID:      userID,
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
	if q.UserID == "" {
		if u, ok := ctx.Value(entity.ContextKeyUserID).(string); ok {
			q.UserID = u
		}
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
	var redisErr error
	hasRedis := h.replayStore != nil
	hasJournal := h.journalRepo != nil && q.LastEventID != ""

	// 2. Query bounded Redis Stream replay buffer: rag:notify:replay:{tenant_id}
	cursor := q.LastEventID
	if hasRedis {
		for len(events) < q.Limit {
			streamEvents, err := h.replayStore.FetchReplayEvents(ctx, q.TenantID, cursor, q.Limit)
			if err != nil {
				redisErr = err
				h.logger.Warn("redis stream replay fetch error",
					slog.String("tenant_id", q.TenantID),
					slog.String("error", err.Error()),
				)
				break
			}
			if len(streamEvents) == 0 {
				break
			}
			for _, ev := range streamEvents {
				cursor = ev.EventID
				if q.UserID == "" || ev.UserID == "" || ev.UserID == q.UserID {
					events = append(events, ev)
					if len(events) >= q.Limit {
						break
					}
				}
			}
		}
	}

	// 3. Fall back to PostgreSQL event journal if Redis stream has no events or lastEventID predates stream window
	var journalErr error
	if len(events) < q.Limit && hasJournal {
		seen := make(map[string]struct{}, len(events))
		for _, ev := range events {
			seen[ev.EventID] = struct{}{}
		}
		jCursor := q.LastEventID
		for len(events) < q.Limit {
			journalEvents, jErr := h.journalRepo.FetchReplayAfter(ctx, q.TenantID, jCursor, q.Limit)
			if jErr != nil {
				journalErr = jErr
				h.logger.Warn("cold journal replay fetch error",
					slog.String("tenant_id", q.TenantID),
					slog.String("last_event_id", jCursor),
					slog.String("error", jErr.Error()),
				)
				break
			}
			if len(journalEvents) == 0 {
				break
			}
			for _, ev := range journalEvents {
				jCursor = ev.EventID
				if _, alreadySeen := seen[ev.EventID]; alreadySeen {
					continue
				}
				if q.UserID == "" || ev.UserID == "" || ev.UserID == q.UserID {
					seen[ev.EventID] = struct{}{}
					events = append(events, ev)
					if len(events) >= q.Limit {
						break
					}
				}
			}
		}
	}

	// If events have been collected and the journal query failed, signal retry to the client
	if len(events) > 0 && journalErr != nil {
		return FetchReplayEventsResult{}, fmt.Errorf("journal replay query failed: %w", journalErr)
	}

	// If every eligible replay source failed, return error
	if len(events) == 0 {
		if hasRedis && hasJournal {
			if redisErr != nil && journalErr != nil {
				return FetchReplayEventsResult{}, fmt.Errorf("all replay sources failed: redis: %v, journal: %w", redisErr, journalErr)
			}
		} else if hasRedis && redisErr != nil {
			return FetchReplayEventsResult{}, fmt.Errorf("redis replay source failed: %w", redisErr)
		} else if hasJournal && journalErr != nil {
			return FetchReplayEventsResult{}, fmt.Errorf("journal replay source failed: %w", journalErr)
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
