// Package hub implements high-density per-client ring buffering, connection session management,
// and targeted multi-pod routing (ADR-003).
package hub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/repository"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/redis"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

// PresenceQuerier defines the read port for resolving target node IDs for active SSE connections (ADR-003).
type PresenceQuerier interface {
	GetTargetNodes(ctx context.Context, tenantID, userID string) ([]string, error)
}

// ReplayBufferStore defines the outbound port for appending dispatched events to the bounded Redis Stream replay buffer.
type ReplayBufferStore interface {
	AppendReplayEvent(ctx context.Context, tenantID string, event entity.NotificationEvent) error
}

// PodRoutedEnvelope carries pre-formatted SSE byte frames and OpenTelemetry trace metadata
// across targeted Redis channels (`pod.{node_id}.events`) without Protobuf-to-JSON re-serialization.
type PodRoutedEnvelope struct {
	EventID          string `json:"event_id"`
	TenantID         string `json:"tenant_id"`
	UserID           string `json:"user_id,omitempty"`
	SourceService    string `json:"source_service,omitempty"`
	EventType        string `json:"event_type"`
	Severity         string `json:"severity"`
	IsTerminal       bool   `json:"is_terminal"`
	OccurredAtUnixMs int64  `json:"occurred_at_unix_ms"`
	TraceID          string `json:"trace_id,omitempty"`
	SSEFrame         []byte `json:"sse_frame"`
}

// PodChannel returns the Redis Pub/Sub channel for a specific daemon node ID.
func PodChannel(nodeID string) string {
	return fmt.Sprintf("pod.%s.events", nodeID)
}

// TargetedRouterConfig configures targeted routing for the local node.
type TargetedRouterConfig struct {
	LocalNodeID string
}

// TargetedRouter implements command.NodeEventDispatcher, routing SSE events directly to local sockets
// (when node_id == localNodeID) or over targeted Redis channels (`pod.{node_id}.events`) to peer pods (ADR-003).
type TargetedRouter struct {
	cfg         TargetedRouterConfig
	localHub    *ConnectionHub
	presence    PresenceQuerier
	replayStore ReplayBufferStore
	journalRepo repository.EventJournalRepository
	pubsub      redis.PubSubClient
	logger      *slog.Logger
	metrics     *observability.MetricsRegistry

	subMu    sync.Mutex
	sub      redis.PubSubSubscription
	cancelFn context.CancelFunc
	done     chan struct{}
}

// NewTargetedRouter creates a new TargetedRouter.
func NewTargetedRouter(
	cfg TargetedRouterConfig,
	localHub *ConnectionHub,
	presence PresenceQuerier,
	replayStore ReplayBufferStore,
	journalRepo repository.EventJournalRepository,
	pubsub redis.PubSubClient,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *TargetedRouter {
	if cfg.LocalNodeID == "" {
		cfg.LocalNodeID = "node-local-0"
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return &TargetedRouter{
		cfg:         cfg,
		localHub:    localHub,
		presence:    presence,
		replayStore: replayStore,
		journalRepo: journalRepo,
		pubsub:      pubsub,
		logger:      logger,
		metrics:     metrics,
		done:        make(chan struct{}),
	}
}

// DispatchSSEFrame delivers an event to local connections and publishes targeted wire envelopes
// to remote pods hosting matching active sessions. Implements command.NodeEventDispatcher.
func (r *TargetedRouter) DispatchSSEFrame(ctx context.Context, event entity.NotificationEvent, sseFrame []byte) error {
	// 1. Append to bounded Redis Stream replay buffer (rag:notify:replay:{tenant_id})
	if r.replayStore != nil {
		if err := r.replayStore.AppendReplayEvent(ctx, event.TenantID, event); err != nil {
			r.logger.Warn("failed to append event to redis replay buffer",
				slog.String("event_id", event.EventID),
				slog.String("tenant_id", event.TenantID),
				slog.String("error", err.Error()),
			)
		}
	}

	// 2. Cold-path journaling fallback (non-blocking / non-fatal)
	if r.journalRepo != nil {
		_ = r.journalRepo.AppendEvent(ctx, event)
	}

	// 3. Resolve target node IDs via presence directory (ADR-003)
	var targetNodes []string
	if r.presence != nil {
		nodes, err := r.presence.GetTargetNodes(ctx, event.TenantID, event.UserID)
		if err != nil {
			r.logger.Warn("failed to query presence target nodes",
				slog.String("tenant_id", event.TenantID),
				slog.String("user_id", event.UserID),
				slog.String("error", err.Error()),
			)
		} else {
			targetNodes = nodes
		}
	}

	// Always ensure local hub receives event if local connections match
	hasLocalTarget := false
	for _, n := range targetNodes {
		if n == r.cfg.LocalNodeID {
			hasLocalTarget = true
			break
		}
	}
	if !hasLocalTarget && r.localHub != nil && r.localHub.TenantActiveConnections(event.TenantID) > 0 {
		hasLocalTarget = true
	}

	// 4. Deliver directly to local ConnectionHub (zero network hop, zero re-serialization)
	if hasLocalTarget && r.localHub != nil {
		if err := r.localHub.DispatchSSEFrame(ctx, event, sseFrame); err != nil {
			r.logger.Warn("local hub dispatch error",
				slog.String("event_id", event.EventID),
				slog.String("tenant_id", event.TenantID),
				slog.String("error", err.Error()),
			)
		}
	}

	// 5. Publish pre-serialized envelope to remote pods over pod.{node_id}.events
	if r.pubsub == nil || len(targetNodes) == 0 {
		return nil
	}

	var envelopeBytes []byte
	for _, nodeID := range targetNodes {
		if nodeID == r.cfg.LocalNodeID {
			continue // Already handled locally
		}

		if envelopeBytes == nil {
			env := PodRoutedEnvelope{
				EventID:          event.EventID,
				TenantID:         event.TenantID,
				UserID:           event.UserID,
				SourceService:    event.SourceService,
				EventType:        event.EventType,
				Severity:         string(event.Severity),
				IsTerminal:       event.IsTerminal,
				OccurredAtUnixMs: event.OccurredAt.UnixMilli(),
				TraceID:          event.TraceID,
				SSEFrame:         sseFrame,
			}
			var mErr error
			envelopeBytes, mErr = json.Marshal(env)
			if mErr != nil {
				return fmt.Errorf("marshal pod routed envelope: %w", mErr)
			}
		}

		channel := PodChannel(nodeID)
		if err := r.pubsub.Publish(ctx, channel, envelopeBytes); err != nil {
			r.logger.Warn("failed to publish routed event to remote pod",
				slog.String("target_node_id", nodeID),
				slog.String("channel", channel),
				slog.String("event_id", event.EventID),
				slog.String("error", err.Error()),
			)
			continue
		}
	}

	return nil
}

// Start begins listening on pod.{localNodeID}.events for peer-routed event frames.
func (r *TargetedRouter) Start(ctx context.Context) error {
	if r.pubsub == nil {
		return errors.New("pubsub publisher is nil")
	}

	subCtx, cancel := context.WithCancel(ctx)
	r.subMu.Lock()
	r.cancelFn = cancel
	r.subMu.Unlock()

	channel := PodChannel(r.cfg.LocalNodeID)
	sub, err := r.pubsub.Subscribe(subCtx, channel)
	if err != nil {
		cancel()
		return fmt.Errorf("subscribe to pod channel %s: %w", channel, err)
	}

	r.subMu.Lock()
	r.sub = sub
	r.subMu.Unlock()

	r.logger.Info("started targeted pod routing subscriber",
		slog.String("local_node_id", r.cfg.LocalNodeID),
		slog.String("channel", channel),
	)

	go r.receiveLoop(subCtx, sub)
	return nil
}

func (r *TargetedRouter) receiveLoop(ctx context.Context, sub redis.PubSubSubscription) {
	defer close(r.done)
	msgCh := sub.Messages()

	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-msgCh:
			if !ok {
				return
			}
			var env PodRoutedEnvelope
			if err := json.Unmarshal(payload, &env); err != nil {
				r.logger.Warn("failed to unmarshal peer routed envelope", slog.String("error", err.Error()))
				continue
			}

			sev, sErr := entity.ParseSeverity(env.Severity)
			if sErr != nil {
				sev = entity.SeverityInfo
			}

			occTime := time.UnixMilli(env.OccurredAtUnixMs).UTC()
			if env.OccurredAtUnixMs == 0 {
				occTime = time.Now().UTC()
			}

			ev := entity.NotificationEvent{
				EventID:       env.EventID,
				TenantID:      env.TenantID,
				UserID:        env.UserID,
				SourceService: env.SourceService,
				EventType:     env.EventType,
				Severity:      sev,
				IsTerminal:    env.IsTerminal,
				OccurredAt:    occTime,
				TraceID:       env.TraceID,
			}

			// Push pre-formatted SSE byte frame directly to local matching client connections!
			if r.localHub != nil {
				_ = r.localHub.DispatchSSEFrame(ctx, ev, env.SSEFrame)
			}
		}
	}
}

// Close stops the peer subscriber and closes the Redis Pub/Sub subscription.
func (r *TargetedRouter) Close() error {
	r.subMu.Lock()
	if r.cancelFn != nil {
		r.cancelFn()
	}
	var err error
	if r.sub != nil {
		err = r.sub.Close()
	}
	r.subMu.Unlock()
	<-r.done
	return err
}
