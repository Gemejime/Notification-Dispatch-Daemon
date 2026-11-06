package unit_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/query"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	transporthttp "github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http/middleware"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/sse"
)

// ============================================================================
// Phase 2 Unit, Concurrency & Stream Engine Tests
// ============================================================================

func TestBoundedRingBufferSlowConsumerDropAndTerminalRetention(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	tenantID := "tnt_ring_01"
	connID := "conn_01"

	rb := hub.NewRingBuffer(tenantID, connID, hub.DefaultRingBufferSize, hub.DefaultSlowConsumerTimeout, metrics)
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

	// 1. Fill ring buffer to exactly 256 non-terminal events.
	for i := 0; i < hub.DefaultRingBufferSize; i++ {
		status := rb.Push(hub.RingBufferEntry{
			EventID:    fmt.Sprintf("evt_nt_%04d", i),
			Frame:      []byte(fmt.Sprintf("data: non-terminal-%04d\n\n", i)),
			IsTerminal: false,
		}, t0)
		if status != hub.PushSuccess {
			t.Fatalf("expected PushSuccess at index %d, got %v", i, status)
		}
	}

	if rb.Count() != hub.DefaultRingBufferSize {
		t.Fatalf("expected count 256, got %d", rb.Count())
	}
	if rb.SaturatedSince() == nil {
		t.Fatalf("expected saturated_since to be recorded at capacity 256")
	}

	// 2. Incoming non-terminal frame when saturated must be dropped without blocking.
	dropStatus := rb.Push(hub.RingBufferEntry{
		EventID:    "evt_nt_overflow",
		Frame:      []byte("data: overflow\n\n"),
		IsTerminal: false,
	}, t0)
	if dropStatus != hub.PushDroppedSaturated {
		t.Fatalf("expected PushDroppedSaturated, got %v", dropStatus)
	}
	if metrics.DroppedFramesCount(tenantID, hub.DropReasonRingBufferSaturated) != 1 {
		t.Fatalf("expected dropped_frames metric for ring_buffer_saturated to be 1")
	}

	// 3. Incoming terminal frame must preempt oldest unread non-terminal frame while preserving FIFO ordering.
	termStatus := rb.Push(hub.RingBufferEntry{
		EventID:    "evt_term_first",
		Frame:      []byte("data: terminal-first\n\n"),
		IsTerminal: true,
	}, t0)
	if termStatus != hub.PushPreemptedOldestNonTerminal {
		t.Fatalf("expected PushPreemptedOldestNonTerminal, got %v", termStatus)
	}
	if metrics.DroppedFramesCount(tenantID, hub.DropReasonTerminalPreemption) != 1 {
		t.Fatalf("expected dropped_frames metric for terminal_frame_preemption to be 1")
	}
	if rb.Count() != hub.DefaultRingBufferSize {
		t.Fatalf("expected count to remain 256 after preemption, got %d", rb.Count())
	}

	// Verify FIFO order: oldest non-terminal (index 0) was evicted, so index 1 is head, and terminal is at the tail.
	firstEntry, ok := rb.TryPop()
	if !ok || firstEntry.EventID != "evt_nt_0001" {
		t.Fatalf("expected head to be evt_nt_0001 after preemption of 0000, got %+v", firstEntry)
	}
	// Buffer count dropped to 255; saturatedSince should be cleared.
	if rb.SaturatedSince() != nil {
		t.Fatalf("expected saturated_since to be cleared when count < capacity")
	}

	// 4. Test All-256-Terminal Overflow Guard:
	// Create a smaller buffer (e.g., capacity 4) filled exclusively with terminal frames.
	rbAllTerm := hub.NewRingBuffer(tenantID, "conn_term_overflow", 4, hub.DefaultSlowConsumerTimeout, metrics)
	for i := 0; i < 4; i++ {
		rbAllTerm.Push(hub.RingBufferEntry{
			EventID:    fmt.Sprintf("evt_t_%d", i),
			Frame:      []byte(fmt.Sprintf("data: term-%d\n\n", i)),
			IsTerminal: true,
		}, t0)
	}
	if rbAllTerm.Count() != 4 {
		t.Fatalf("expected count 4")
	}

	// Pushing a 5th terminal frame when all 4 slots are already terminal triggers all_terminal_eviction.
	allTermStatus := rbAllTerm.Push(hub.RingBufferEntry{
		EventID:    "evt_t_extra",
		Frame:      []byte("data: term-extra\n\n"),
		IsTerminal: true,
	}, t0)
	if allTermStatus != hub.PushEvictedAllTerminal {
		t.Fatalf("expected PushEvictedAllTerminal, got %v", allTermStatus)
	}
	if metrics.DroppedFramesCount(tenantID, hub.DropReasonAllTerminalEviction) != 1 {
		t.Fatalf("expected dropped_frames metric for all_terminal_eviction to be 1")
	}
	if evicted, reason := rbAllTerm.IsEvicted(); !evicted || reason != hub.DropReasonAllTerminalEviction {
		t.Fatalf("expected buffer to be marked evicted with all_terminal_eviction, got (%v, %s)", evicted, reason)
	}

	// 5. Test Continuous Saturation Eviction (> 5s):
	rbSlow := hub.NewRingBuffer(tenantID, "conn_slow", 2, hub.DefaultSlowConsumerTimeout, metrics)
	rbSlow.Push(hub.RingBufferEntry{EventID: "e1", Frame: []byte("d1"), IsTerminal: false}, t0)
	rbSlow.Push(hub.RingBufferEntry{EventID: "e2", Frame: []byte("d2"), IsTerminal: false}, t0)

	// Within 5s ceiling (e.g. at t0 + 4s), not stalled:
	stalled4s, _ := rbSlow.CheckStalled(t0.Add(4 * time.Second))
	if stalled4s {
		t.Fatalf("expected not stalled at 4s")
	}

	// Past 5s ceiling (e.g. at t0 + 6s), must be stalled and marked for eviction:
	stalled6s, reason := rbSlow.CheckStalled(t0.Add(6 * time.Second))
	if !stalled6s || reason != hub.DropReasonSlowConsumerEvicted {
		t.Fatalf("expected stalled with slow_consumer_evicted at 6s, got (%v, %s)", stalled6s, reason)
	}
	if metrics.DroppedFramesCount(tenantID, hub.DropReasonSlowConsumerEvicted) != 1 {
		t.Fatalf("expected dropped_frames metric for slow_consumer_evicted to be 1")
	}
}

func TestConnectionHubLocalRoutingAndTenantIsolation(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	var logBuf bytes.Buffer
	logger := observability.NewSecuredLogger(&logBuf, slog.LevelDebug)

	h := hub.NewConnectionHub(
		"node-alpha",
		hub.DefaultSlowConsumerTimeout,
		30*time.Second,
		60*time.Second,
		metrics,
		logger,
	)

	now := time.Now().UTC()

	// Connection 1: Tenant Alpha, User 42, Severity Filter = INFO
	c1 := entity.Connection{
		ConnectionID:   "conn_alpha_u42_info",
		TenantID:       "tnt_alpha",
		UserID:         "usr_42",
		NodeID:         "node-alpha",
		SeverityFilter: entity.SeverityInfo,
		ConnectedAt:    now,
	}
	rb1 := hub.NewRingBuffer(c1.TenantID, c1.ConnectionID, 256, hub.DefaultSlowConsumerTimeout, metrics)
	_ = h.Register(c1, rb1, nil)

	// Connection 2: Tenant Alpha, User 99, Severity Filter = WARNING
	c2 := entity.Connection{
		ConnectionID:   "conn_alpha_u99_warn",
		TenantID:       "tnt_alpha",
		UserID:         "usr_99",
		NodeID:         "node-alpha",
		SeverityFilter: entity.SeverityWarning,
		ConnectedAt:    now,
	}
	rb2 := hub.NewRingBuffer(c2.TenantID, c2.ConnectionID, 256, hub.DefaultSlowConsumerTimeout, metrics)
	_ = h.Register(c2, rb2, nil)

	// Connection 3: Tenant Alpha, Anonymous (UserID = ""), Severity Filter = INFO
	c3 := entity.Connection{
		ConnectionID:   "conn_alpha_anon_info",
		TenantID:       "tnt_alpha",
		UserID:         "",
		NodeID:         "node-alpha",
		SeverityFilter: entity.SeverityInfo,
		ConnectedAt:    now,
	}
	rb3 := hub.NewRingBuffer(c3.TenantID, c3.ConnectionID, 256, hub.DefaultSlowConsumerTimeout, metrics)
	_ = h.Register(c3, rb3, nil)

	// Connection 4: Tenant Beta, User 42, Severity Filter = INFO
	c4 := entity.Connection{
		ConnectionID:   "conn_beta_u42_info",
		TenantID:       "tnt_beta",
		UserID:         "usr_42",
		NodeID:         "node-alpha",
		SeverityFilter: entity.SeverityInfo,
		ConnectedAt:    now,
	}
	rb4 := hub.NewRingBuffer(c4.TenantID, c4.ConnectionID, 256, hub.DefaultSlowConsumerTimeout, metrics)
	_ = h.Register(c4, rb4, nil)

	if h.TotalActiveConnections() != 4 {
		t.Fatalf("expected 4 total connections, got %d", h.TotalActiveConnections())
	}
	if h.TenantActiveConnections("tnt_alpha") != 3 {
		t.Fatalf("expected 3 connections in tnt_alpha, got %d", h.TenantActiveConnections("tnt_alpha"))
	}
	if h.TenantActiveConnections("tnt_beta") != 1 {
		t.Fatalf("expected 1 connection in tnt_beta, got %d", h.TenantActiveConnections("tnt_beta"))
	}

	// 1. Dispatch User-Targeted Event for usr_42 in tnt_alpha:
	// CRITICAL ISOLATION ASSERTION:
	// - c1 (u42) receives it.
	// - c2 (u99) must NOT receive it.
	// - c3 (anon) must NOT receive it (never leak user-targeted notifications to anonymous streams).
	// - c4 (beta) must NOT receive it (per-tenant isolation).
	userEvent := entity.NotificationEvent{
		EventID:     "evt_u42_01",
		TenantID:    "tnt_alpha",
		UserID:      "usr_42",
		EventType:   entity.EventTypeIngestionMilestone,
		Severity:    entity.SeverityInfo,
		OccurredAt:  now,
		PayloadJSON: []byte(`{"stage":"PARSING"}`),
	}
	frame1 := userEvent.FormatSSEFrame(5000)
	_ = h.DispatchSSEFrame(context.Background(), userEvent, frame1)

	if rb1.Count() != 1 {
		t.Fatalf("expected c1 to receive user-targeted event, count=%d", rb1.Count())
	}
	if rb2.Count() != 0 {
		t.Fatalf("ISOLATION LEAK: c2 (user 99) received user 42 event")
	}
	if rb3.Count() != 0 {
		t.Fatalf("ISOLATION LEAK: c3 (anonymous) received user 42 event")
	}
	if rb4.Count() != 0 {
		t.Fatalf("TENANT ISOLATION LEAK: c4 (tenant beta) received tenant alpha event")
	}

	// 2. Dispatch Tenant-Wide Broadcast Event (UserID = "") with Severity = INFO:
	// - c1 (u42, INFO) receives it.
	// - c2 (u99, WARN) must NOT receive it (INFO < WARN).
	// - c3 (anon, INFO) receives it.
	// - c4 (beta) must NOT receive it.
	broadcastEvent := entity.NotificationEvent{
		EventID:     "evt_broadcast_01",
		TenantID:    "tnt_alpha",
		UserID:      "",
		EventType:   entity.EventTypeIndexBlueGreenSwitched,
		Severity:    entity.SeverityInfo,
		OccurredAt:  now,
		PayloadJSON: []byte(`{"switched":true}`),
	}
	frame2 := broadcastEvent.FormatSSEFrame(5000)
	_ = h.DispatchSSEFrame(context.Background(), broadcastEvent, frame2)

	if rb1.Count() != 2 {
		t.Fatalf("expected c1 count=2, got %d", rb1.Count())
	}
	if rb2.Count() != 0 {
		t.Fatalf("SEVERITY FILTER LEAK: c2 (min severity WARN) received INFO event")
	}
	if rb3.Count() != 1 {
		t.Fatalf("expected c3 (anon, INFO) to receive broadcast event, count=%d", rb3.Count())
	}
	if rb4.Count() != 0 {
		t.Fatalf("TENANT ISOLATION LEAK: c4 received tenant alpha broadcast event")
	}

	// 3. Clean Deregistration:
	removed, ok := h.Deregister(c1.ConnectionID)
	if !ok || removed == nil || removed.ConnectionID != c1.ConnectionID {
		t.Fatalf("failed to deregister c1")
	}
	if h.TotalActiveConnections() != 3 {
		t.Fatalf("expected 3 remaining connections, got %d", h.TotalActiveConnections())
	}
}

func TestConnectionHubSlowConsumerBackgroundSweep(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	h := hub.NewConnectionHub("node-sweep", hub.DefaultSlowConsumerTimeout, 30*time.Second, 60*time.Second, metrics, nil)
	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	// Register normal connection
	cNormal := entity.Connection{ConnectionID: "conn_norm", TenantID: "tnt_sw", NodeID: "node-sweep", ConnectedAt: t0}
	rbNormal := hub.NewRingBuffer("tnt_sw", "conn_norm", 256, hub.DefaultSlowConsumerTimeout, metrics)
	_ = h.Register(cNormal, rbNormal, nil)

	// Register stalled connection (256 items, saturated at t0)
	var canceled bool
	cancelFn := func() { canceled = true }
	cStalled := entity.Connection{ConnectionID: "conn_stall", TenantID: "tnt_sw", NodeID: "node-sweep", ConnectedAt: t0}
	rbStalled := hub.NewRingBuffer("tnt_sw", "conn_stall", 2, hub.DefaultSlowConsumerTimeout, metrics)
	_ = h.Register(cStalled, rbStalled, cancelFn)

	rbStalled.Push(hub.RingBufferEntry{EventID: "e1", Frame: []byte("d1")}, t0)
	rbStalled.Push(hub.RingBufferEntry{EventID: "e2", Frame: []byte("d2")}, t0)

	// Sweep at t0 + 6s (> 5s timeout)
	evicted := h.SweepSlowConsumers(t0.Add(6 * time.Second))
	if len(evicted) != 1 || evicted[0] != "conn_stall" {
		t.Fatalf("expected conn_stall to be evicted, got %+v", evicted)
	}
	if !canceled {
		t.Fatalf("expected cancel func to be triggered on slow consumer eviction")
	}
	if h.TotalActiveConnections() != 1 {
		t.Fatalf("expected exactly 1 active connection remaining, got %d", h.TotalActiveConnections())
	}
}

func TestConnectionHubStaggeredGracefulDrainADR005(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	h := hub.NewConnectionHub("node-drain", hub.DefaultSlowConsumerTimeout, 30*time.Second, 60*time.Second, metrics, nil)

	const connCount = 10
	rbs := make([]*hub.RingBuffer, connCount)
	for i := 0; i < connCount; i++ {
		cid := fmt.Sprintf("conn_drain_%02d", i)
		conn := entity.Connection{ConnectionID: cid, TenantID: "tnt_drain", NodeID: "node-drain", ConnectedAt: time.Now().UTC()}
		rb := hub.NewRingBuffer("tnt_drain", cid, 256, hub.DefaultSlowConsumerTimeout, metrics)
		rbs[i] = rb
		_ = h.Register(conn, rb, nil)
	}

	// Trigger staggered drain with short test window [10ms, 50ms]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	doneCh := h.DrainWithJitter(ctx, 10*time.Millisecond, 50*time.Millisecond)

	// Verify each ring buffer received a drain delay directive
	delays := make(map[time.Duration]bool)
	for i, rb := range rbs {
		select {
		case delay := <-rb.DrainDelay():
			if delay < 10*time.Millisecond || delay > 50*time.Millisecond {
				t.Fatalf("connection %d drain delay %v out of [10ms, 50ms]", i, delay)
			}
			delays[delay] = true
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timeout waiting for connection %d drain signal", i)
		}
	}

	// Wait for all socket drain timers to finish
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for all connections to drain")
	}

	if h.TotalActiveConnections() != 0 {
		t.Fatalf("expected all connections to be deregistered after drain, got %d", h.TotalActiveConnections())
	}
}

func TestConnectionHubDeterministicCursorPagination(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	h := hub.NewConnectionHub("node-paged", hub.DefaultSlowConsumerTimeout, 30*time.Second, 60*time.Second, metrics, nil)

	now := time.Now().UTC()
	for i := 0; i < 10; i++ {
		cid := fmt.Sprintf("conn_%02d", i)
		conn := entity.Connection{ConnectionID: cid, TenantID: "tnt_paged", NodeID: "node-paged", ConnectedAt: now}
		rb := hub.NewRingBuffer("tnt_paged", cid, 256, hub.DefaultSlowConsumerTimeout, metrics)
		_ = h.Register(conn, rb, nil)
	}

	handler := query.NewListActiveSessionsHandler(h)
	ctx := context.Background()

	// Page 1: limit 4, cursor ""
	p1, err := handler.Handle(ctx, query.ListActiveSessionsQuery{TenantID: "tnt_paged", Limit: 4, Cursor: ""})
	if err != nil {
		t.Fatalf("page 1 failed: %v", err)
	}
	if len(p1.Sessions) != 4 || p1.ActiveConnections != 10 || p1.NextCursor != "conn_03" {
		t.Fatalf("page 1 mismatch: %+v", p1)
	}

	// Page 2: limit 4, cursor "conn_03"
	p2, err := handler.Handle(ctx, query.ListActiveSessionsQuery{TenantID: "tnt_paged", Limit: 4, Cursor: p1.NextCursor})
	if err != nil {
		t.Fatalf("page 2 failed: %v", err)
	}
	if len(p2.Sessions) != 4 || p2.Sessions[0].ConnectionID != "conn_04" || p2.NextCursor != "conn_07" {
		t.Fatalf("page 2 mismatch: %+v", p2)
	}

	// Page 3: limit 4, cursor "conn_07" -> remaining 2 items, NextCursor empty
	p3, err := handler.Handle(ctx, query.ListActiveSessionsQuery{TenantID: "tnt_paged", Limit: 4, Cursor: p2.NextCursor})
	if err != nil {
		t.Fatalf("page 3 failed: %v", err)
	}
	if len(p3.Sessions) != 2 || p3.Sessions[0].ConnectionID != "conn_08" || p3.NextCursor != "" {
		t.Fatalf("page 3 mismatch: %+v", p3)
	}
}

type flusherRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flusherRecorder) Flush() {
	f.flushed = true
}

func TestSSEFlusherProxyBypassAndWriteLatency(t *testing.T) {
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	metrics := observability.NewMetricsRegistry()

	flusher, err := sse.NewFlusher(rec, "node-flusher-test", 5*time.Second, metrics)
	if err != nil {
		t.Fatalf("NewFlusher failed: %v", err)
	}

	// 1. Headers verification (ADR-002)
	flusher.WriteHeaders()
	if rec.Header().Get("Content-Type") != "text/event-stream; charset=utf-8" {
		t.Fatalf("unexpected Content-Type: %s", rec.Header().Get("Content-Type"))
	}
	if rec.Header().Get("Cache-Control") != "no-cache, no-transform" {
		t.Fatalf("unexpected Cache-Control: %s", rec.Header().Get("Cache-Control"))
	}
	if rec.Header().Get("Connection") != "keep-alive" {
		t.Fatalf("unexpected Connection: %s", rec.Header().Get("Connection"))
	}
	if rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("unexpected X-Accel-Buffering: %s", rec.Header().Get("X-Accel-Buffering"))
	}
	if !rec.flushed {
		t.Fatalf("expected Flush() to be called on WriteHeaders")
	}

	// 2. Ping verification
	if err := flusher.WritePing(); err != nil {
		t.Fatalf("WritePing failed: %v", err)
	}
	if !strings.Contains(rec.Body.String(), ": ping\n\n") {
		t.Fatalf("expected ping in body, got: %s", rec.Body.String())
	}

	// 3. Retry directive frame verification
	if err := flusher.WriteRetryDirective(45 * time.Second); err != nil {
		t.Fatalf("WriteRetryDirective failed: %v", err)
	}
	if !strings.Contains(rec.Body.String(), "retry: 45000\n\n") {
		t.Fatalf("expected retry directive in body, got: %s", rec.Body.String())
	}

	// 4. Write latency metric verification
	prom := metrics.RenderPrometheusText()
	if !strings.Contains(prom, "notification_client_write_latency_seconds_count") {
		t.Fatalf("expected write latency histogram to be populated, got:\n%s", prom)
	}
}

func TestHTTPMiddlewareSecurityAuthAndRateLimiting(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	var logBuf bytes.Buffer
	logger := observability.NewSecuredLogger(&logBuf, slog.LevelInfo)
	h := hub.NewConnectionHub("node-mid", hub.DefaultSlowConsumerTimeout, 30*time.Second, 60*time.Second, metrics, logger)
	sessionsHandler := query.NewListActiveSessionsHandler(h)
	rateLimiter := middleware.NewReconnectRateLimiter(5, 5, 100)

	router := transporthttp.NewRouter(transporthttp.RouterConfig{
		Metrics:         metrics,
		Logger:          logger,
		SessionsHandler: sessionsHandler,
		RateLimiter:     rateLimiter,
	})

	// 1. Missing Authorization header -> 401 Unauthorized
	req1 := httptest.NewRequest(http.MethodGet, "/v1/notifications/sessions?tenant_id=tnt_01", nil)
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on missing auth, got %d", rec1.Code)
	}

	// 2. Cross-tenant access: Authed as tnt_01, querying tnt_other -> 403 Forbidden
	req2 := httptest.NewRequest(http.MethodGet, "/v1/notifications/sessions?tenant_id=tnt_other", nil)
	req2.Header.Set("Authorization", "Bearer tnt_01:usr_alice")
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusForbidden {
		t.Fatalf("expected 403 on cross-tenant access, got %d", rec2.Code)
	}

	// 3. Valid authenticated request -> 200 OK
	req3 := httptest.NewRequest(http.MethodGet, "/v1/notifications/sessions?tenant_id=tnt_01", nil)
	req3.Header.Set("Authorization", "Bearer tnt_01:usr_alice")
	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200 on authorized request, got %d", rec3.Code)
	}

	// 4. Rate limiting: Burst limit = 5. Fire 10 rapid requests.
	rateLimited := false
	for i := 0; i < 10; i++ {
		reqBurst := httptest.NewRequest(http.MethodGet, "/v1/notifications/sessions?tenant_id=tnt_rate", nil)
		reqBurst.Header.Set("Authorization", "Bearer tnt_rate")
		recBurst := httptest.NewRecorder()
		router.ServeHTTP(recBurst, reqBurst)
		if recBurst.Code == http.StatusTooManyRequests {
			rateLimited = true
			if recBurst.Header().Get("Retry-After") == "" {
				t.Fatalf("expected Retry-After header on 429")
			}
			break
		}
	}
	if !rateLimited {
		t.Fatalf("expected rate limiter to reject rapid burst with 429 Too Many Requests")
	}

	// 5. Panic Recovery: Sanitize panic and return 500 without stack leak.
	panicHandler := middleware.PanicRecoveryMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("database connection pool exhausted unexpectedly")
	}))
	reqPanic := httptest.NewRequest(http.MethodGet, "/test/panic", nil)
	recPanic := httptest.NewRecorder()
	panicHandler.ServeHTTP(recPanic, reqPanic)
	if recPanic.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on panic recovery, got %d", recPanic.Code)
	}
	if strings.Contains(recPanic.Body.String(), "database connection pool") {
		t.Fatalf("panic details leaked in HTTP response body: %s", recPanic.Body.String())
	}
}
