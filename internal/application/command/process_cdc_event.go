// Package command implements application-layer command orchestrators for rag-notification-dispatch-daemon.
package command

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	notificationv1 "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/repository"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/router"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

const (
	// WebhookEnqueueTimeout enforces the P99 <= 15ms webhook task queue hand-off SLO.
	WebhookEnqueueTimeout = 15 * time.Millisecond
	// TargetedRoutingTimeout bounds the hot-path Redis targeted pod publish latency.
	TargetedRoutingTimeout = 10 * time.Millisecond
)

// NodeEventDispatcher defines the outbound port for targeted pod routing (`pod.{node_id}.events`, ADR-003).
type NodeEventDispatcher interface {
	DispatchSSEFrame(ctx context.Context, event entity.NotificationEvent, sseFrame []byte) error
}

// WebhookTaskEnqueuer defines the outbound port for asynchronous webhook delivery task enqueueing
// to Redis Streams / RabbitMQ (`rag-webhook-delivery-worker`, ADR-001).
type WebhookTaskEnqueuer interface {
	EnqueueWebhookTask(ctx context.Context, task *notificationv1.WebhookDeliveryTask) error
}

// ProcessCDCEventResult reports the outcome of processing a single CDC outbox event.
type ProcessCDCEventResult struct {
	EventID          string
	TenantID         string
	DuplicateSkipped bool
	Suppressed       bool
	RateLimited      bool
	SSEDispatched    bool
	WebhooksEnqueued int
	PrebuiltSSEFrame []byte
}

// ProcessCDCEventHandler orchestrates hot-path CDC event evaluation strictly against
// the pod-local in-memory SubscriptionCache (0 SQL queries on the hot path — ADR-004).
type ProcessCDCEventHandler struct {
	cache           repository.SubscriptionCacheReader
	evaluator       *router.RuleEvaluator
	filter          *router.Filter
	sseDispatcher   NodeEventDispatcher
	webhookEnqueuer WebhookTaskEnqueuer
	webhookHandler  *DispatchWebhookTaskHandler
	logger          *slog.Logger
	metrics         *observability.MetricsRegistry
	nowFn           func() time.Time
}

// NewProcessCDCEventHandler constructs a ProcessCDCEventHandler wired with in-memory cache and routing ports.
func NewProcessCDCEventHandler(
	cache repository.SubscriptionCacheReader,
	evaluator *router.RuleEvaluator,
	filter *router.Filter,
	sseDispatcher NodeEventDispatcher,
	webhookEnqueuer WebhookTaskEnqueuer,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *ProcessCDCEventHandler {
	if evaluator == nil {
		evaluator = router.NewRuleEvaluator()
	}
	if filter == nil {
		filter = router.NewFilter(router.DefaultDedupWindowTTL, router.DefaultMaxDedupEntries)
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	if metrics == nil {
		metrics = observability.NewMetricsRegistry()
	}
	return &ProcessCDCEventHandler{
		cache:           cache,
		evaluator:       evaluator,
		filter:          filter,
		sseDispatcher:   sseDispatcher,
		webhookEnqueuer: webhookEnqueuer,
		webhookHandler:  NewDispatchWebhookTaskHandler(webhookEnqueuer, logger, metrics),
		logger:          logger,
		metrics:         metrics,
		nowFn:           func() time.Time { return time.Now().UTC() },
	}
}

// Handle validates an incoming Protobuf v3 NotificationEventEnvelope, evaluates pod-local cached
// tenant rules with zero SQL reads, formats the SSE byte frame without intermediary map conversions,
// and dispatches to targeted SSE pods and the outbound webhook task queue.
func (h *ProcessCDCEventHandler) Handle(
	ctx context.Context,
	envelope *notificationv1.NotificationEventEnvelope,
) (ProcessCDCEventResult, error) {
	if err := envelope.Validate(); err != nil {
		eventType := "unknown"
		if envelope != nil && envelope.EventType != "" {
			eventType = envelope.EventType
		}
		h.metrics.IncEventsProcessed(eventType, "invalid_envelope")
		return ProcessCDCEventResult{}, fmt.Errorf("validate cdc envelope: %w", err)
	}

	domainSev, err := entity.ParseSeverity(envelope.Severity.String())
	if err != nil {
		h.metrics.IncEventsProcessed(envelope.EventType, "invalid_severity")
		return ProcessCDCEventResult{}, fmt.Errorf("parse domain severity: %w", err)
	}

	event := entity.NotificationEvent{
		EventID:       envelope.EventId,
		TenantID:      envelope.TenantId,
		UserID:        envelope.UserId,
		SourceService: envelope.SourceService,
		EventType:     envelope.EventType,
		Severity:      domainSev,
		IsTerminal:    envelope.IsTerminal,
		OccurredAt:    time.UnixMilli(envelope.OccurredAtUnixMs).UTC(),
		TraceID:       envelope.TraceId,
		PayloadJSON:   envelope.PayloadJson,
	}

	if err := event.Validate(); err != nil {
		h.metrics.IncEventsProcessed(event.EventType, "invalid_domain_event")
		return ProcessCDCEventResult{}, fmt.Errorf("validate domain event: %w", err)
	}

	now := h.nowFn()
	res := ProcessCDCEventResult{
		EventID:  event.EventID,
		TenantID: event.TenantID,
	}

	// 1. Bounded-memory (tenant_id, event_id) idempotency deduplication.
	if h.filter.CheckAndRecordDuplicate(event.TenantID, event.EventID, now) {
		res.DuplicateSkipped = true
		h.metrics.IncEventsProcessed(event.EventType, "duplicate_skipped")
		return res, nil
	}

	// 2. Pure in-memory rule lookup from pod-local SubscriptionCache (0 SQL reads — ADR-004).
	var subs []entity.Subscription
	var targets []entity.WebhookTarget
	if h.cache != nil {
		subs, targets = h.cache.GetTenantRules(event.TenantID)
	}

	decision := h.evaluator.Evaluate(event, subs, targets)
	if decision.Suppressed {
		res.Suppressed = true
		h.metrics.IncEventsProcessed(event.EventType, "suppressed")
		return res, nil
	}
	if !decision.ShouldDispatch() {
		h.metrics.IncEventsProcessed(event.EventType, "no_matching_subscription")
		return res, nil
	}

	// 3. Per-tenant burst rate limiting (terminal events always bypass rate limiting).
	if !h.filter.AllowTenantBurst(event.TenantID, decision.EffectiveRateLimitRPS, event.IsTerminal, now) {
		res.RateLimited = true
		h.metrics.IncEventsProcessed(event.EventType, "rate_limited")
		return res, nil
	}

	// 4. Zero-intermediary SSE frame serialization & targeted pod dispatch.
	if decision.DeliverSSE {
		sseFrame := event.FormatSSEFrame(entity.DefaultSSERetryMs)
		res.PrebuiltSSEFrame = sseFrame
		if h.sseDispatcher != nil {
			routeCtx, cancel := context.WithTimeout(ctx, TargetedRoutingTimeout)
			err := h.sseDispatcher.DispatchSSEFrame(routeCtx, event, sseFrame)
			cancel()
			if err != nil {
				h.metrics.IncEventsProcessed(event.EventType, "sse_dispatch_error")
				return res, fmt.Errorf("dispatch sse frame for event %s: %w", event.EventID, err)
			}
		}
		res.SSEDispatched = true
	}

	// 5. Asynchronous webhook task enqueueing (P99 <= 15ms, ADR-001).
	if decision.DeliverWebhook && len(decision.MatchedWebhookTargets) > 0 {
		whRes, err := h.webhookHandler.Handle(ctx, DispatchWebhookTaskCommand{
			Event:   event,
			Targets: decision.MatchedWebhookTargets,
		})
		if err != nil {
			return res, err
		}
		res.WebhooksEnqueued = whRes.TasksEnqueued
	}

	h.metrics.IncEventsProcessed(event.EventType, "dispatched")
	h.logger.Debug("cdc notification event processed",
		slog.String("trace_id", event.TraceID),
		slog.String("tenant_id", event.TenantID),
		slog.String("event_id", event.EventID),
		slog.String("event_type", event.EventType),
		slog.String("severity", string(event.Severity)),
		slog.Int("payload_bytes_len", len(event.PayloadJSON)),
		slog.Bool("sse_dispatched", res.SSEDispatched),
		slog.Int("webhooks_enqueued", res.WebhooksEnqueued),
	)
	return res, nil
}
