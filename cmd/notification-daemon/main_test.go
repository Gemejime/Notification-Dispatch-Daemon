package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

func TestOperationalProbesAndConfig(t *testing.T) {
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

	mux := NewOperationalMux(cache, metrics, &draining)

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

	// 4. /ready during SIGTERM drain should return 503.
	draining.Store(true)
	reqReady3 := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recReady3 := httptest.NewRecorder()
	mux.ServeHTTP(recReady3, reqReady3)
	if recReady3.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected /ready 503 during drain, got %d", recReady3.Code)
	}

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
}
