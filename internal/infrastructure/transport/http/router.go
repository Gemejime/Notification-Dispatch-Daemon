// Package http provides HTTP routing and endpoint registration for rag-notification-dispatch-daemon.
package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/query"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/redis"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http/middleware"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/sse"
)

// ReadinessDependency encapsulates liveness/readiness callbacks for Redis backplane and CDC consumer.
type ReadinessDependency struct {
	RedisConnected func() bool
	CDCConsuming   func() bool
}

// RouterConfig contains dependencies and configuration required to assemble the production HTTP mux.
type RouterConfig struct {
	Cache           *memory.SubscriptionCache
	Metrics         *observability.MetricsRegistry
	Logger          *slog.Logger
	Draining        *atomic.Bool
	ReadinessDeps   *ReadinessDependency
	StreamHandler   *sse.StreamHandler
	SessionsHandler *query.ListActiveSessionsHandler
	InvalidationSub *redis.InvalidationSubscriber
	Authenticator   middleware.Authenticator
	RateLimiter     *middleware.ReconnectRateLimiter
}

// NewRouter constructs a production HTTP handler wired with all operational probes,
// OpenAPI 3.1 documentation, SSE stream, active sessions telemetry, and cache invalidation routes.
func NewRouter(cfg RouterConfig) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	if cfg.RateLimiter == nil {
		cfg.RateLimiter = middleware.NewReconnectRateLimiter(100, 200, 10000)
	}
	if cfg.Authenticator == nil {
		cfg.Authenticator = middleware.SimpleTokenAuthenticator{}
	}

	mux := http.NewServeMux()

	// 1. Probes & Metrics (ADR-005, Prometheus, Kubernetes)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		isDraining := cfg.Draining != nil && cfg.Draining.Load()
		isWarmed := cfg.Cache != nil && cfg.Cache.IsWarmed()
		redisOK := true
		if cfg.ReadinessDeps != nil && cfg.ReadinessDeps.RedisConnected != nil {
			redisOK = cfg.ReadinessDeps.RedisConnected()
		}
		cdcOK := true
		if cfg.ReadinessDeps != nil && cfg.ReadinessDeps.CDCConsuming != nil {
			cdcOK = cfg.ReadinessDeps.CDCConsuming()
		}

		subCacheStatus := map[bool]string{true: "warmed", false: "warming"}[isWarmed]
		redisStatus := map[bool]string{true: "connected", false: "disconnected"}[redisOK]
		cdcStatus := map[bool]string{true: "consuming", false: "stopped"}[cdcOK]

		if isDraining || !isWarmed || !redisOK || !cdcOK {
			w.WriteHeader(http.StatusServiceUnavailable)
			resp, _ := json.Marshal(map[string]any{
				"status":             "not_ready",
				"subscription_cache": subCacheStatus,
				"redis_backplane":    redisStatus,
				"cdc_consumer":       cdcStatus,
				"draining":           isDraining,
			})
			_, _ = w.Write(resp)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready","subscription_cache":"warmed","redis_backplane":"connected","cdc_consumer":"consuming","draining":false}`))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		if cfg.Metrics != nil {
			_, _ = w.Write([]byte(cfg.Metrics.RenderPrometheusText()))
		}
	})

	// 2. OpenAPI 3.1 & Interactive Documentation Endpoints
	openAPIJSONHandler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(openAPI31JSONSpec))
	}
	mux.HandleFunc("/openapi.json", openAPIJSONHandler)
	mux.HandleFunc("/api/docs", openAPIJSONHandler)

	mux.HandleFunc("/docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>Swagger UI - RAG Notification Dispatch Daemon</title><link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" /></head><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>window.onload = function() { SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui" }); };</script></body></html>`))
	})

	mux.HandleFunc("/redoc", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>ReDoc - RAG Notification Dispatch Daemon</title><link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet"></head><body><redoc spec-url="/openapi.json"></redoc><script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script></body></html>`))
	})

	// 3. /v1/notifications/stream — High-Density SSE stream endpoint (ADR-002)
	authMW := middleware.AuthMiddleware(cfg.Authenticator)
	rateMW := middleware.RateLimitMiddleware(cfg.RateLimiter)
	streamHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.Draining != nil && cfg.Draining.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"pod_draining"}`))
			return
		}
		if cfg.StreamHandler != nil {
			cfg.StreamHandler.ServeHTTP(w, r)
			return
		}
		http.Error(w, `{"error":"stream_handler_not_configured"}`, http.StatusInternalServerError)
	})
	mux.Handle("/v1/notifications/stream", authMW(rateMW(middleware.TenantAuthorizationMiddleware(streamHandler))))

	// 4. /v1/notifications/sessions — Active sessions telemetry query endpoint
	sessionsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
		if tenantID == "" {
			if t, ok := r.Context().Value(entity.ContextKeyTenantID).(string); ok && t != "" {
				tenantID = t
			}
		}
		if tenantID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"tenant_id_required"}`))
			return
		}

		if cfg.SessionsHandler != nil {
			limit := 100
			if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
				if parsed, err := strconv.Atoi(rawLimit); err == nil && parsed > 0 {
					limit = parsed
				}
			}
			res, err := cfg.SessionsHandler.Handle(r.Context(), query.ListActiveSessionsQuery{
				TenantID: tenantID,
				Limit:    limit,
				Cursor:   strings.TrimSpace(r.URL.Query().Get("cursor")),
			})
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"query_failed"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(res)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp, _ := json.Marshal(map[string]any{
			"tenant_id":          tenantID,
			"active_connections": 0,
			"sessions":           []any{},
		})
		_, _ = w.Write(resp)
	})
	mux.Handle("/v1/notifications/sessions", authMW(rateMW(middleware.TenantAuthorizationMiddleware(sessionsHandler))))

	// 5. /v1/notifications/subscriptions/invalidate — Subscription cache invalidation broadcast (ADR-004)
	invalidateHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
		var req struct {
			TenantID string `json:"tenant_id"`
			Reason   string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.Reason) == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_invalidation_request"}`))
			return
		}

		if cfg.InvalidationSub != nil {
			if err := cfg.InvalidationSub.BroadcastInvalidation(r.Context(), req.TenantID, req.Reason); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"invalidation_broadcast_failed"}`))
				return
			}
		} else if cfg.Cache != nil {
			cfg.Cache.EvictTenant(req.TenantID)
			if cfg.Metrics != nil {
				cfg.Metrics.IncCacheInvalidations(req.Reason)
			}
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"invalidation_service_unavailable"}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		resp, _ := json.Marshal(map[string]any{
			"status":         "invalidated",
			"tenant_id":      req.TenantID,
			"invalidated_at": time.Now().UTC().Format(time.RFC3339),
		})
		_, _ = w.Write(resp)
	})
	mux.Handle("/v1/notifications/subscriptions/invalidate", authMW(rateMW(invalidateHandler)))

	// Wrap root with SecuredLogging, PanicRecovery, and CorrelationID
	root := middleware.CorrelationIDMiddleware(
		middleware.PanicRecoveryMiddleware(cfg.Logger)(
			middleware.SecuredLoggingMiddleware(cfg.Logger)(mux),
		),
	)

	return root
}

const openAPI31JSONSpec = `{
  "openapi": "3.1.0",
  "info": {
    "title": "RAG Notification Dispatch Daemon API",
    "version": "1.0.0"
  },
  "paths": {
    "/v1/notifications/stream": {"get": {"operationId": "streamNotifications"}},
    "/v1/notifications/sessions": {"get": {"operationId": "listActiveSessions"}},
    "/v1/notifications/subscriptions/invalidate": {"post": {"operationId": "invalidateTenantCache"}},
    "/healthz": {"get": {"operationId": "getLiveness"}},
    "/ready": {"get": {"operationId": "getReadiness"}},
    "/metrics": {"get": {"operationId": "getMetrics"}},
    "/docs": {"get": {"operationId": "getSwaggerDocs"}},
    "/redoc": {"get": {"operationId": "getRedocDocs"}},
    "/api/docs": {"get": {"operationId": "getOpenApiDocs"}},
    "/openapi.json": {"get": {"operationId": "getOpenApiJson"}}
  }
}`
