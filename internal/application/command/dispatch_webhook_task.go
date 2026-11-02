// Package command implements application-layer command orchestrators for rag-notification-dispatch-daemon.
package command

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	notificationv1 "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

// DispatchWebhookTaskCommand carries the input parameters for generating and enqueueing
// downstream webhook delivery tasks for matched tenant webhook endpoints (ADR-001).
type DispatchWebhookTaskCommand struct {
	Event   entity.NotificationEvent
	Targets []entity.WebhookTarget
}

// DispatchWebhookTaskResult summarizes the outcome of webhook task enqueueing.
type DispatchWebhookTaskResult struct {
	TasksEnqueued int
}

// DispatchWebhookTaskHandler orchestrates the generation of deterministic WebhookDeliveryTask items,
// schema validation, and enqueueing to Redis Streams / RabbitMQ with P99 <= 15ms hand-off latency (ADR-001).
// It centralizes notification_webhook_enqueued_total metric accounting so counters are incremented exactly once.
type DispatchWebhookTaskHandler struct {
	enqueuer WebhookTaskEnqueuer
	logger   *slog.Logger
	metrics  *observability.MetricsRegistry
	nowFn    func() time.Time
}

// NewDispatchWebhookTaskHandler creates a new DispatchWebhookTaskHandler.
func NewDispatchWebhookTaskHandler(
	enqueuer WebhookTaskEnqueuer,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *DispatchWebhookTaskHandler {
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	if metrics == nil {
		metrics = observability.NewMetricsRegistry()
	}
	return &DispatchWebhookTaskHandler{
		enqueuer: enqueuer,
		logger:   logger,
		metrics:  metrics,
		nowFn:    func() time.Time { return time.Now().UTC() },
	}
}

// Handle builds, validates, and enqueues WebhookDeliveryTask messages for each configured target endpoint.
// Invariant: Never executes HTTP POST requests directly (delegated to rag-webhook-delivery-worker, ADR-001).
func (h *DispatchWebhookTaskHandler) Handle(
	ctx context.Context,
	cmd DispatchWebhookTaskCommand,
) (DispatchWebhookTaskResult, error) {
	if len(cmd.Targets) == 0 {
		return DispatchWebhookTaskResult{TasksEnqueued: 0}, nil
	}

	now := h.nowFn()
	res := DispatchWebhookTaskResult{}

	for _, target := range cmd.Targets {
		taskID := entity.DeterministicTaskID(cmd.Event.TenantID, cmd.Event.EventID, target.TargetID)
		task := &notificationv1.WebhookDeliveryTask{
			TaskId:              taskID,
			EventId:             cmd.Event.EventID,
			TenantId:            cmd.Event.TenantID,
			TargetId:            target.TargetID,
			TargetUrl:           target.TargetURL,
			SecretRef:           target.SecretRef,
			SignatureHeaderName: target.EffectiveSignatureHeader(),
			TimestampUnixSec:    now.Unix(),
			EventType:           cmd.Event.EventType,
			PayloadJson:         cmd.Event.PayloadJSON,
			TraceId:             cmd.Event.TraceID,
		}

		// 1. Schema and invariant validation
		if err := task.Validate(); err != nil {
			h.metrics.IncWebhookEnqueued(cmd.Event.TenantID, "validation_error")
			h.logger.Warn("webhook task validation failed",
				slog.String("tenant_id", cmd.Event.TenantID),
				slog.String("target_id", target.TargetID),
				slog.String("error", err.Error()),
			)
			return res, fmt.Errorf("validate webhook delivery task for target %s: %w", target.TargetID, err)
		}

		// 2. Broker enqueueing with 15ms hand-off budget (ADR-001)
		if h.enqueuer != nil {
			enqCtx, cancel := context.WithTimeout(ctx, WebhookEnqueueTimeout)
			err := h.enqueuer.EnqueueWebhookTask(enqCtx, task)
			cancel()
			if err != nil {
				h.metrics.IncWebhookEnqueued(cmd.Event.TenantID, "enqueue_error")
				h.logger.Error("failed to enqueue webhook delivery task",
					slog.String("tenant_id", cmd.Event.TenantID),
					slog.String("task_id", task.TaskId),
					slog.String("target_id", target.TargetID),
					slog.String("error", err.Error()),
				)
				return res, fmt.Errorf("enqueue webhook task %s: %w", task.TaskId, err)
			}
		}

		// 3. Exactly-once Prometheus metric accounting
		res.TasksEnqueued++
		h.metrics.IncWebhookEnqueued(cmd.Event.TenantID, "enqueued")

		h.logger.Debug("webhook delivery task enqueued",
			slog.Any("task", task),
		)
	}

	return res, nil
}
