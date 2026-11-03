package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	transporthttp "github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http/middleware"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/sse"
)

func TestOperationalProbesAndConfig(t *testing.T) {
	t.Setenv("SSE_RING_BUFFER_SIZE", "")
	ConfigureRuntimeMemoryBounds()
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
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

	// Seed nonempty rules for tnt_01 before invalidation
	cache.ReplaceTenantRules("tnt_01", []entity.Subscription{
		{
			SubscriptionID: "sub_test_01",
			TenantID:       "tnt_01",
			SSEEnabled:     true,
		},
	}, []entity.WebhookTarget{
		{
			TargetID:  "tgt_test_01",
			TenantID:  "tnt_01",
			TargetURL: "https://webhook.internal/test",
			Active:    true,
		},
	})
	preSubs, preTargets := cache.GetTenantRules("tnt_01")
	if len(preSubs) == 0 || len(preTargets) == 0 {
		t.Fatalf("expected seeded rules for tnt_01 to be present before invalidation, got %d subs and %d targets", len(preSubs), len(preTargets))
	}

	reqInv := httptest.NewRequest(http.MethodPost, "/v1/notifications/subscriptions/invalidate", strings.NewReader(`{"tenant_id":"tnt_01","reason":"rule_change"}`))
	recInv := httptest.NewRecorder()
	mux.ServeHTTP(recInv, reqInv)
	if recInv.Code != http.StatusAccepted {
		t.Fatalf("expected /v1/notifications/subscriptions/invalidate 202, got %d", recInv.Code)
	}

	// Assert that the tenant rules reflect the invalidation (evicted from cache)
	subs, targets := cache.GetTenantRules("tnt_01")
	if len(subs) != 0 || len(targets) != 0 {
		t.Fatalf("expected tenant rules to reflect invalidation, got %d subs and %d targets", len(subs), len(targets))
	}

	// 8. Exercise production router created by transporthttp.NewRouter
	prodRouter := transporthttp.NewRouter(transporthttp.RouterConfig{
		Cache:    cache,
		Metrics:  metrics,
		Draining: &draining,
		ReadinessDeps: &transporthttp.ReadinessDependency{
			RedisConnected: redisOK.Load,
			CDCConsuming:   cdcOK.Load,
		},
		Authenticator: middleware.SimpleTokenAuthenticator{},
	})

	for _, route := range []string{"/healthz", "/ready", "/metrics", "/openapi.json", "/api/docs", "/docs", "/redoc"} {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		rec := httptest.NewRecorder()
		prodRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected prod router %s 200, got %d", route, rec.Code)
		}
	}

	// Exercise production router stream route with cancellable context and assert SSE headers
	streamHub := hub.NewConnectionHub("test-node", 5*time.Second, 30*time.Second, 60*time.Second, metrics, nil)
	streamRegCmd := command.NewRegisterConnectionHandler(streamHub, nil, 45*time.Second)
	streamTermCmd := command.NewTerminateConnectionHandler(streamHub, nil)
	testStreamHandler := sse.NewStreamHandler(
		sse.StreamHandlerConfig{
			NodeID:              "test-node",
			RingBufferSize:      256,
			SlowConsumerTimeout: 5 * time.Second,
			HeartbeatInterval:   15 * time.Second,
			DefaultRetryMs:      5000,
		},
		streamHub,
		streamRegCmd,
		streamTermCmd,
		nil,
		nil,
		metrics,
		nil,
	)

	routerWithStream := transporthttp.NewRouter(transporthttp.RouterConfig{
		Cache:         cache,
		Metrics:       metrics,
		StreamHandler: testStreamHandler,
		Authenticator: middleware.SimpleTokenAuthenticator{},
	})

	ctxStream, cancelStream := context.WithCancel(context.Background())
	reqProdStream := httptest.NewRequest(http.MethodGet, "/v1/notifications/stream", nil).WithContext(ctxStream)
	reqProdStream.Header.Set("Authorization", "Bearer valid-token")
	recProdStream := httptest.NewRecorder()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		routerWithStream.ServeHTTP(recProdStream, reqProdStream)
	}()

	time.Sleep(50 * time.Millisecond)
	cancelStream()
	<-doneCh

	if recProdStream.Header().Get("Content-Type") != "text/event-stream; charset=utf-8" ||
		recProdStream.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("expected SSE headers on production router stream, got Content-Type=%s, X-Accel-Buffering=%s",
			recProdStream.Header().Get("Content-Type"), recProdStream.Header().Get("X-Accel-Buffering"))
	}

	// 9. ComputeDrainDelay should stay within [min, max].
	delay := ComputeDrainDelay(30*time.Second, 60*time.Second)
	if delay < 30*time.Second || delay > 60*time.Second {
		t.Fatalf("expected drain delay in [30s, 60s], got %v", delay)
	}
}
