// Package sse provides high-density Server-Sent Events (SSE) streaming infrastructure
// adhering to ADR-002, edge proxy bypass headers, and Slow-Loris write deadline guards.
package sse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/query"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
)

// ReplayStreamer defines the outbound port for bounded Last-Event-ID replay backfill
// (implemented via Redis in Phase 3).
type ReplayStreamer interface {
	FetchReplayEvents(ctx context.Context, tenantID string, lastEventID string, limit int) ([]entity.NotificationEvent, error)
}

// StreamHandlerConfig defines runtime tuning parameters for the SSE stream handler.
type StreamHandlerConfig struct {
	NodeID              string
	RingBufferSize      int
	SlowConsumerTimeout time.Duration
	HeartbeatInterval   time.Duration
	DefaultRetryMs      int
	ReplayMaxEvents     int
}

// StreamHandler handles multiplexed GET /v1/notifications/stream connections.
type StreamHandler struct {
	cfg               StreamHandlerConfig
	connectionHub     *hub.ConnectionHub
	registerCmd       *command.RegisterConnectionHandler
	terminateCmd      *command.TerminateConnectionHandler
	presenceRegistrar command.PresenceRegistrar
	replayStreamer    ReplayStreamer
	metrics           *observability.MetricsRegistry
	logger            *slog.Logger
}

// NewStreamHandler creates a new StreamHandler.
func NewStreamHandler(
	cfg StreamHandlerConfig,
	connectionHub *hub.ConnectionHub,
	registerCmd *command.RegisterConnectionHandler,
	terminateCmd *command.TerminateConnectionHandler,
	presenceRegistrar command.PresenceRegistrar,
	replayStreamer ReplayStreamer,
	metrics *observability.MetricsRegistry,
	logger *slog.Logger,
) *StreamHandler {
	if cfg.RingBufferSize <= 0 {
		cfg.RingBufferSize = hub.DefaultRingBufferSize
	}
	if cfg.SlowConsumerTimeout <= 0 {
		cfg.SlowConsumerTimeout = hub.DefaultSlowConsumerTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	if cfg.DefaultRetryMs <= 0 {
		cfg.DefaultRetryMs = entity.DefaultSSERetryMs
	}
	if cfg.ReplayMaxEvents <= 0 {
		cfg.ReplayMaxEvents = 100
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return &StreamHandler{
		cfg:               cfg,
		connectionHub:     connectionHub,
		registerCmd:       registerCmd,
		terminateCmd:      terminateCmd,
		presenceRegistrar: presenceRegistrar,
		replayStreamer:    replayStreamer,
		metrics:           metrics,
		logger:            logger,
	}
}

// ServeHTTP handles GET /v1/notifications/stream requests.
func (h *StreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// 1. Extract tenant_id and optional user_id from request context (set by auth middleware).
	tenantID, _ := r.Context().Value(entity.ContextKeyTenantID).(string)
	if tenantID == "" {
		tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	}
	if tenantID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
		return
	}

	userID, _ := r.Context().Value(entity.ContextKeyUserID).(string)
	if userID == "" {
		userID = strings.TrimSpace(r.URL.Query().Get("user_id"))
	}

	// 2. Parse and validate optional severity_filter query parameter.
	var severityFilter entity.Severity
	if rawSev := strings.TrimSpace(r.URL.Query().Get("severity_filter")); rawSev != "" {
		parsedSev, err := entity.ParseSeverity(rawSev)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_severity_filter"}`))
			return
		}
		severityFilter = parsedSev
	}

	// 3. Initialize Flusher with Slow-Loris write deadline guard (ADR-002).
	flusher, err := NewFlusher(w, h.cfg.NodeID, h.cfg.SlowConsumerTimeout, h.metrics)
	if err != nil {
		h.logger.Error("flusher not supported on response writer", slog.String("error", err.Error()))
		http.Error(w, `{"error":"streaming_not_supported"}`, http.StatusInternalServerError)
		return
	}

	// 4. Resolve Last-Event-ID for reconnection backfill.
	lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if lastEventID == "" {
		lastEventID = strings.TrimSpace(r.URL.Query().Get("last_event_id"))
	}

	connectionID := generateConnectionID(h.cfg.NodeID)
	now := time.Now().UTC()
	conn := entity.Connection{
		ConnectionID:   connectionID,
		TenantID:       tenantID,
		UserID:         userID,
		NodeID:         h.cfg.NodeID,
		SeverityFilter: severityFilter,
		LastEventID:    lastEventID,
		ConnectedAt:    now,
	}

	rb := hub.NewRingBuffer(
		tenantID,
		connectionID,
		h.cfg.RingBufferSize,
		h.cfg.SlowConsumerTimeout,
		h.metrics,
	)

	streamCtx, cancelStream := context.WithCancel(r.Context())
	defer cancelStream()

	// 5. Register connection in local hub and presence directory.
	regErr := h.registerCmd.Handle(streamCtx, command.RegisterConnectionCommand{
		Conn:       conn,
		RingBuffer: rb,
		Cancel:     cancelStream,
	})
	if regErr != nil {
		h.logger.Error("failed to register sse connection",
			slog.String("connection_id", connectionID),
			slog.String("tenant_id", tenantID),
			slog.String("error", regErr.Error()),
		)
		http.Error(w, `{"error":"registration_failed"}`, http.StatusInternalServerError)
		return
	}

	defer func() {
		_ = h.terminateCmd.Handle(context.Background(), command.TerminateConnectionCommand{
			ConnectionID: connectionID,
			TenantID:     tenantID,
			UserID:       userID,
		})
	}()

	// 6. Last-Event-ID replay backfill with rate limiting check (Runbook 2, ADR-004).
	var replayed []entity.NotificationEvent
	if lastEventID != "" && h.replayStreamer != nil {
		var rErr error
		replayed, rErr = h.replayStreamer.FetchReplayEvents(streamCtx, tenantID, lastEventID, h.cfg.ReplayMaxEvents)
		if rErr != nil && !errors.Is(rErr, context.Canceled) {
			if errors.Is(rErr, query.ErrBackfillRateLimited) {
				h.logger.Warn("reconnection backfill rate limited",
					slog.String("connection_id", connectionID),
					slog.String("tenant_id", tenantID),
				)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"backfill_rate_limited"}`))
				return
			}
			h.logger.Warn("failed to fetch replay events",
				slog.String("connection_id", connectionID),
				slog.String("last_event_id", lastEventID),
				slog.String("error", rErr.Error()),
			)
		}
	}

	// 7. Write ADR-002 response headers and immediate ping keepalive.
	flusher.WriteHeaders()
	if err := flusher.WritePing(); err != nil {
		h.logger.Debug("failed to write initial ping, client aborted", slog.String("connection_id", connectionID))
		return
	}

	// 8. Stream replayed frames with high-watermark tracking.
	var watermarkEventID string
	for _, ev := range replayed {
		if conn.AcceptsEvent(ev) {
			frame := ev.FormatSSEFrame(h.cfg.DefaultRetryMs)
			if wErr := flusher.WriteFrame(frame); wErr != nil {
				h.logger.Debug("client disconnected during replay backfill", slog.String("connection_id", connectionID))
				return
			}
			watermarkEventID = ev.EventID
		}
	}

	// 8. Stream event loop: handles heartbeats, live frames, drain, and slow-consumer eviction.
	heartbeatTicker := time.NewTicker(h.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-streamCtx.Done():
			h.logger.Debug("sse client connection terminated",
				slog.String("connection_id", connectionID),
				slog.String("tenant_id", tenantID),
			)
			return

		case <-heartbeatTicker.C:
			// Emit 15s : ping\n\n heartbeat to prevent edge proxy timeouts.
			if pingErr := flusher.WritePing(); pingErr != nil {
				h.logger.Debug("client ping failed; connection severed",
					slog.String("connection_id", connectionID),
					slog.String("error", pingErr.Error()),
				)
				return
			}
			if h.presenceRegistrar != nil {
				_ = h.presenceRegistrar.RefreshPresence(streamCtx, tenantID, userID, connectionID, 45*time.Second)
			}

		case drainDelay := <-rb.DrainDelay():
			// Per-Connection Staggered Graceful Draining (ADR-005): emit final retry directive frame.
			h.logger.Info("emitting sse retry directive frame before staggered drain closure",
				slog.String("connection_id", connectionID),
				slog.Duration("drain_delay", drainDelay),
			)
			_ = flusher.WriteRetryDirective(drainDelay)
			return

		case <-rb.Notify():
			// Drain all ready frames from the non-blocking ring buffer.
			for {
				entry, ok := rb.TryPop()
				if !ok {
					break
				}
				// High-watermark deduplication against backfill events.
				if watermarkEventID != "" && entry.EventID <= watermarkEventID {
					continue
				}
				if writeErr := flusher.WriteFrame(entry.Frame); writeErr != nil {
					h.logger.Debug("sse client write failed; closing socket",
						slog.String("connection_id", connectionID),
						slog.String("error", writeErr.Error()),
					)
					return
				}
			}

			// Check if connection was evicted during push or background sweep.
			if evicted, reason := rb.IsEvicted(); evicted {
				h.logger.Warn("closing evicted sse stream",
					slog.String("connection_id", connectionID),
					slog.String("tenant_id", tenantID),
					slog.String("eviction_reason", reason),
				)
				return
			}
		}
	}
}

func generateConnectionID(nodeID string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("conn_%s_%d_%s", nodeID, time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}
