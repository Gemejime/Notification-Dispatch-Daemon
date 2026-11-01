package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

func TestOperationalProbesAndConfig(t *testing.T) {
	t.Setenv("SSE_RING_BUFFER_SIZE", "")
	ConfigureRuntimeMemoryBounds()
	cfg := LoadConfig()
	if cfg.HTTPPort == "" {
		t.Fatalf("expected default HTTPPort to be populated")
	}
	if cfg.SSERingBufferSize != 256 {
		t.Fatalf("expected default SSERingBufferSize=256, got %d", cfg.SSERingBufferSize)
	}

	cache := memory.NewSubscriptionCache()
	metrics := observability.NewMetricsRegistry()
	metrics.IncEventsProcessed(entity.EventTypeIngestionMilestone, "dispatched")
	var draining atomic.Bool
	var redisOK atomic.Bool
	var cdcOK atomic.Bool
	redisOK.Store(true)
	cdcOK.Store(true)

	deps := &ReadinessDependency{
		RedisConnected: redisOK.Load,
		CDCConsuming:   cdcOK.Load,
	}
	mux := NewOperationalMuxWithDependencies(cache, metrics, &draining, deps)

	// 1. /healthz should return 200 OK.
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recHealth := httptest.NewRecorder()
	mux.ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Fatalf("expected /healthz 200, got %d", recHealth.Code)
	}

	// 2. /ready before cache warm-up should return 503.
	reqReady1 := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recReady1 := httptest.NewRecorder()
	mux.ServeHTTP(recReady1, reqReady1)
	if recReady1.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected /ready 503 before warm-up, got %d", recReady1.Code)
	}

	// 3. /ready after cache warm-up should return 200.
	cache.ReplaceAll(nil, nil)
	reqReady2 := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recReady2 := httptest.NewRecorder()
	mux.ServeHTTP(recReady2, reqReady2)
	if recReady2.Code != http.StatusOK {
		t.Fatalf("expected /ready 200 after warm-up, got %d", recReady2.Code)
	}

	// 3a. /ready when Redis is disconnected should return 503 with redis_backplane="disconnected".
	redisOK.Store(false)
	reqReadyRedisDown := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recReadyRedisDown := httptest.NewRecorder()
	mux.ServeHTTP(recReadyRedisDown, reqReadyRedisDown)
	if recReadyRedisDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected /ready 503 when redis disconnected, got %d", recReadyRedisDown.Code)
	}
	if !strings.Contains(recReadyRedisDown.Body.String(), `"redis_backplane":"disconnected"`) {
		t.Fatalf("expected redis_backplane disconnected in body, got %s", recReadyRedisDown.Body.String())
	}
	redisOK.Store(true)

	// 3b. /ready when CDC consumer is stopped should return 503 with cdc_consumer="stopped".
	cdcOK.Store(false)
	reqReadyCDCDown := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recReadyCDCDown := httptest.NewRecorder()
	mux.ServeHTTP(recReadyCDCDown, reqReadyCDCDown)
	if recReadyCDCDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected /ready 503 when cdc consumer stopped, got %d", recReadyCDCDown.Code)
	}
	if !strings.Contains(recReadyCDCDown.Body.String(), `"cdc_consumer":"stopped"`) {
		t.Fatalf("expected cdc_consumer stopped in body, got %s", recReadyCDCDown.Body.String())
	}
	cdcOK.Store(true)

	// 4. /ready during SIGTERM drain should return 503.
	draining.Store(true)
	reqReady3 := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recReady3 := httptest.NewRecorder()
	mux.ServeHTTP(recReady3, reqReady3)
	if recReady3.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected /ready 503 during drain, got %d", recReady3.Code)
	}
	draining.Store(false)

	// 5. /metrics should expose Prometheus text format.
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recMetrics := httptest.NewRecorder()
	mux.ServeHTTP(recMetrics, reqMetrics)
	if recMetrics.Code != http.StatusOK {
		t.Fatalf("expected /metrics 200, got %d", recMetrics.Code)
	}
	if !strings.Contains(recMetrics.Body.String(), "notification_events_processed_total") {
		t.Fatalf("expected Prometheus output to contain notification_events_processed_total, got %s", recMetrics.Body.String())
	}

	// 6. OpenAPI 3.1 endpoints (/openapi.json, /api/docs, /docs, /redoc) should return 200 OK.
	for _, route := range []string{"/openapi.json", "/api/docs", "/docs", "/redoc"} {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected %s 200, got %d", route, rec.Code)
		}
	}

	// 7. /v1/notifications/* routes should be registered and operational.
	reqStream := httptest.NewRequest(http.MethodGet, "/v1/notifications/stream", nil)
	reqStream.Header.Set("Authorization", "Bearer valid-token")
	recStream := httptest.NewRecorder()
	mux.ServeHTTP(recStream, reqStream)
	if recStream.Code != http.StatusOK || recStream.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("expected /v1/notifications/stream 200 with X-Accel-Buffering=no, got %d", recStream.Code)
	}

	reqSessions := httptest.NewRequest(http.MethodGet, "/v1/notifications/sessions?tenant_id=tnt_01", nil)
	reqSessions.Header.Set("Authorization", "Bearer valid-token")
	recSessions := httptest.NewRecorder()
	mux.ServeHTTP(recSessions, reqSessions)
	if recSessions.Code != http.StatusOK {
		t.Fatalf("expected /v1/notifications/sessions 200, got %d", recSessions.Code)
	}

	reqInv := httptest.NewRequest(http.MethodPost, "/v1/notifications/subscriptions/invalidate", strings.NewReader(`{"tenant_id":"tnt_01","reason":"rule_change"}`))
	recInv := httptest.NewRecorder()
	mux.ServeHTTP(recInv, reqInv)
	if recInv.Code != http.StatusAccepted {
		t.Fatalf("expected /v1/notifications/subscriptions/invalidate 202, got %d", recInv.Code)
	}

	// 8. ComputeDrainDelay should stay within [min, max].
	delay := ComputeDrainDelay(30*time.Second, 60*time.Second)
	if delay < 30*time.Second || delay > 60*time.Second {
		t.Fatalf("expected drain delay in [30s, 60s], got %v", delay)
	}
}
