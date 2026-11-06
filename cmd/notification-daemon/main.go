// Package main is the entry point for rag-notification-dispatch-daemon.
// It configures Go 1.23+ GC/memory bounds (GOMEMLIMIT=1800MiB, GOGC=80), initializes
// Secured Logging, warms the pod-local subscription cache, starts the CDC consumer,
// exposes OpenAPI 3.1 & operational probes, and handles SIGTERM staggered connection draining.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/query"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/router"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/redis"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/config"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/persistence/postgres"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/queue/kafka"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/queue/publisher"
	transporthttp "github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http/middleware"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/sse"
)

const (
	defaultMemoryLimitBytes = 1800 * 1024 * 1024 // 1800MiB
	defaultGCPercent        = 80
	// defaultShutdownBudget matches Kubernetes terminationGracePeriodSeconds: 75 (ADR-005).
	defaultShutdownBudget = 75 * time.Second
)

// Approved environment identifiers (strictly production, staging, development; no local).
const (
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// ParseEnvironment validates that the environment string is one of the approved options:
// "production", "staging", or "development". If empty, it defaults to "development".
// Invalid environments (such as "local") return a descriptive error.
func ParseEnvironment(raw string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	switch trimmed {
	case "production", "prod":
		return EnvProduction, nil
	case "staging", "stage":
		return EnvStaging, nil
	case "development", "dev", "":
		return EnvDevelopment, nil
	default:
		return "", fmt.Errorf("invalid ENVIRONMENT %q: approved options are %q, %q, %q", raw, EnvProduction, EnvStaging, EnvDevelopment)
	}
}

// Config holds the 12-Factor environment configuration defined in .ai/doc-api.md Section 6.
type Config struct {
	Environment          string
	NodeID               string
	HTTPPort             string
	PostgresDSN          observability.SecretString
	RedisAddrs           []string
	RedisPassword        observability.SecretString
	KafkaBrokers         []string
	KafkaConsumerGroup   string
	KafkaCDCTopics       []string
	WebhookQueueStream   string
	SSERingBufferSize    int
	SSESlowConsumerLimit time.Duration
	SSEHeartbeatInterval time.Duration
	DrainJitterMin       time.Duration
	DrainJitterMax       time.Duration
	JWTPublicKeyPEM      observability.SecretString
	JWTIssuer            string
	JWTAudience          string
	LogLevel             slog.Level
}

// LoadConfig loads 12-Factor configuration from .env and environment variables with canonical defaults.
func LoadConfig() (Config, error) {
	if err := config.LoadEnv(); err != nil {
		return Config{}, fmt.Errorf("load env: %w", err)
	}
	ConfigureRuntimeMemoryBounds()

	envStr, err := ParseEnvironment(os.Getenv("ENVIRONMENT"))
	if err != nil {
		return Config{}, fmt.Errorf("parse environment: %w", err)
	}

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "node-local-0"
	}
	return Config{
		Environment:          envStr,
		NodeID:               envOrDefault("NODE_ID", hostname),
		HTTPPort:             envOrDefault("HTTP_PORT", envOrDefault("PORT", "8080")),
		PostgresDSN:          observability.NewSecretString(os.Getenv("POSTGRES_DSN")),
		RedisAddrs:           splitCSV(envOrDefault("REDIS_CLUSTER_ADDRS", envOrDefault("REDIS_ADDRS", "localhost:6379"))),
		RedisPassword:        observability.NewSecretString(os.Getenv("REDIS_PASSWORD")),
		KafkaBrokers:         splitCSV(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaConsumerGroup:   envOrDefault("KAFKA_CONSUMER_GROUP", "rag-notification-dispatch-daemon"),
		KafkaCDCTopics:       splitCSV(envOrDefault("KAFKA_CDC_TOPICS", strings.Join(kafka.DefaultCDCTopics, ","))),
		WebhookQueueStream:   envOrDefault("WEBHOOK_QUEUE_STREAM", "rag:webhook:tasks"),
		SSERingBufferSize:    envIntOrDefault("SSE_RING_BUFFER_SIZE", 256),
		SSESlowConsumerLimit: envDurationOrDefault("SSE_SLOW_CONSUMER_TIMEOUT", 5*time.Second),
		SSEHeartbeatInterval: envDurationOrDefault("SSE_HEARTBEAT_INTERVAL", 15*time.Second),
		DrainJitterMin:       envDurationOrDefault("DRAIN_JITTER_MIN", 30*time.Second),
		DrainJitterMax:       envDurationOrDefault("DRAIN_JITTER_MAX", 60*time.Second),
		JWTPublicKeyPEM:      observability.NewSecretString(os.Getenv("JWT_PUBLIC_KEY_PEM")),
		JWTIssuer:            envOrDefault("JWT_ISSUER", "rag-auth-service"),
		JWTAudience:          envOrDefault("JWT_AUDIENCE", "rag-notification-dispatch-daemon"),
		LogLevel:             observability.ParseLogLevel(envOrDefault("LOG_LEVEL", "INFO")),
	}, nil
}

// ConfigureRuntimeMemoryBounds enforces GOMEMLIMIT (default 1800MiB) and GOGC (default 80),
// dynamically applying parsed overrides from environment / .env.
func ConfigureRuntimeMemoryBounds() {
	memLimit := config.ParseMemoryLimitBytes(os.Getenv("GOMEMLIMIT"), defaultMemoryLimitBytes)
	debug.SetMemoryLimit(memLimit)

	gcPercent := config.ParseGCPercent(os.Getenv("GOGC"), defaultGCPercent)
	debug.SetGCPercent(gcPercent)
}

// ReadinessDependency encapsulates live health checks for the Redis backplane and Kafka CDC consumer.
type ReadinessDependency struct {
	RedisConnected func() bool
	CDCConsuming   func() bool
}

// NewOperationalMux builds the base HTTP mux serving operational probes, OpenAPI 3.1 endpoints,
// and the /v1/notifications/* routes with default dependency probes.
func NewOperationalMux(
	cache *memory.SubscriptionCache,
	metrics *observability.MetricsRegistry,
	draining *atomic.Bool,
) *http.ServeMux {
	return NewOperationalMuxWithDependencies(cache, metrics, draining, nil)
}

// NewOperationalMuxWithDependencies builds the HTTP mux serving operational probes (/healthz, /ready, /metrics),
// OpenAPI 3.1 endpoints (/openapi.json, /api/docs, /docs, /redoc), and /v1/notifications/* routes.
func NewOperationalMuxWithDependencies(
	cache *memory.SubscriptionCache,
	metrics *observability.MetricsRegistry,
	draining *atomic.Bool,
	deps *ReadinessDependency,
) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		isDraining := draining != nil && draining.Load()
		isWarmed := cache != nil && cache.IsWarmed()
		redisOK := true
		if deps != nil && deps.RedisConnected != nil {
			redisOK = deps.RedisConnected()
		}
		cdcOK := true
		if deps != nil && deps.CDCConsuming != nil {
			cdcOK = deps.CDCConsuming()
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
		if metrics != nil {
			_, _ = w.Write([]byte(metrics.RenderPrometheusText()))
		}
	})

	// OpenAPI 3.1 machine-readable & interactive documentation routes.
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

	// /v1/notifications/stream — Persistent SSE endpoint (ADR-002).
	mux.HandleFunc("/v1/notifications/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		if draining != nil && draining.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"pod_draining"}`))
			return
		}
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(auth, "Bearer ") || len(strings.TrimPrefix(auth, "Bearer ")) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-transform")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": ping\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	})

	// /v1/notifications/sessions — Active SSE sessions telemetry endpoint.
	mux.HandleFunc("/v1/notifications/sessions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(auth, "Bearer ") || len(strings.TrimPrefix(auth, "Bearer ")) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
		if tenantID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"tenant_id_required"}`))
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

	// /v1/notifications/subscriptions/invalidate — Pod-local cache invalidation endpoint (ADR-004).
	mux.HandleFunc("/v1/notifications/subscriptions/invalidate", func(w http.ResponseWriter, r *http.Request) {
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
		if cache != nil {
			cache.EvictTenant(req.TenantID)
		}
		if metrics != nil {
			metrics.IncCacheInvalidations(req.Reason)
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

	return mux
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

// ComputeDrainDelay calculates a randomized connection drain delay within [minJitter, maxJitter] (ADR-005).
func ComputeDrainDelay(minJitter, maxJitter time.Duration) time.Duration {
	if minJitter < 0 {
		minJitter = 0
	}
	if maxJitter <= minJitter {
		return minJitter
	}
	spread := maxJitter - minJitter
	return minJitter + time.Duration(rand.Int64N(int64(spread)+1))
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		logger := observability.NewSecuredLogger(os.Stderr, slog.LevelError)
		logger.Error("failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}
	ConfigureRuntimeMemoryBounds()
	logger := observability.NewSecuredLogger(os.Stdout, cfg.LogLevel)
	metrics := observability.NewMetricsRegistry()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pgPool, err := postgres.NewPool(postgres.PoolConfig{
		DSN:            cfg.PostgresDSN,
		MaxConns:       10,
		ConnectTimeout: 5 * time.Second,
	}, postgres.NewNoopQuerier())
	if err != nil {
		logger.Error("failed to initialize cold-path postgres pool", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		_ = pgPool.Close()
	}()

	subRepo := postgres.NewSubscriptionRepo(pgPool)
	webhookRepo := postgres.NewWebhookRepo(pgPool)
	subCache := memory.NewSubscriptionCache()

	if err := subCache.WarmUp(ctx, subRepo, webhookRepo); err != nil {
		logger.Error("failed to warm up pod-local subscription cache", slog.String("error", err.Error()))
		os.Exit(1)
	}

	eventJournalRepo := postgres.NewEventJournalRepo(pgPool)
	redisCluster := redis.NewInMemoryRedisClusterClient()
	presenceRegistry := redis.NewPresenceRegistry(
		redis.PresenceRegistryConfig{
			NodeID:       cfg.NodeID,
			PresenceTTL:  45 * time.Second,
			ReplayMaxLen: 1000,
			ReplayTTL:    3600 * time.Second,
		},
		redisCluster,
		logger,
		metrics,
	)

	taskBroker := publisher.NewInMemoryTaskQueueBroker()
	taskPublisher := publisher.NewTaskQueuePublisher(cfg.WebhookQueueStream, taskBroker, logger, metrics)

	redisClient := redis.NewInMemoryPubSubClient()
	invalidationSub := redis.NewInvalidationSubscriber(redisClient, subCache, subRepo, webhookRepo, logger, metrics)
	go func() {
		if subErr := invalidationSub.Run(ctx); subErr != nil && !errors.Is(subErr, context.Canceled) {
			logger.Error("redis invalidation subscriber terminated with error", slog.String("error", subErr.Error()))
		}
	}()

	connectionHub := hub.NewConnectionHub(
		cfg.NodeID,
		cfg.SSESlowConsumerLimit,
		cfg.DrainJitterMin,
		cfg.DrainJitterMax,
		metrics,
		logger,
	)
	connectionHub.StartSlowConsumerSweeper(ctx, 1*time.Second)

	targetedRouter := hub.NewTargetedRouter(
		hub.TargetedRouterConfig{LocalNodeID: cfg.NodeID},
		connectionHub,
		presenceRegistry,
		presenceRegistry,
		eventJournalRepo,
		redisCluster,
		logger,
		metrics,
	)
	if startErr := targetedRouter.Start(ctx); startErr != nil {
		logger.Error("failed to start targeted router subscriber", slog.String("error", startErr.Error()))
		os.Exit(1)
	}
	defer func() {
		_ = targetedRouter.Close()
	}()

	registerCmd := command.NewRegisterConnectionHandler(connectionHub, presenceRegistry, 45*time.Second)
	terminateCmd := command.NewTerminateConnectionHandler(connectionHub, presenceRegistry)

	replayHandler := query.NewFetchReplayEventsHandler(presenceRegistry, eventJournalRepo, 50, 100, logger, metrics)

	streamHandler := sse.NewStreamHandler(
		sse.StreamHandlerConfig{
			NodeID:              cfg.NodeID,
			RingBufferSize:      cfg.SSERingBufferSize,
			SlowConsumerTimeout: cfg.SSESlowConsumerLimit,
			HeartbeatInterval:   cfg.SSEHeartbeatInterval,
			DefaultRetryMs:      entity.DefaultSSERetryMs,
		},
		connectionHub,
		registerCmd,
		terminateCmd,
		presenceRegistry,
		replayHandler,
		metrics,
		logger,
	)

	sessionsHandler := query.NewListActiveSessionsHandler(connectionHub)
	rateLimiter := middleware.NewReconnectRateLimiter(100, 200, 10000)

	evaluator := router.NewRuleEvaluator()
	filter := router.NewFilter(router.DefaultDedupWindowTTL, router.DefaultMaxDedupEntries)
	cdcHandler := command.NewProcessCDCEventHandler(subCache, evaluator, filter, targetedRouter, taskPublisher, logger, metrics)

	cdcStream := kafka.NewChannelRecordStream(cfg.SSERingBufferSize)
	defer func() {
		_ = cdcStream.Close()
	}()
	cdcConsumer := kafka.NewCDCConsumer(kafka.ConsumerConfig{
		Brokers:       cfg.KafkaBrokers,
		ConsumerGroup: cfg.KafkaConsumerGroup,
		Topics:        cfg.KafkaCDCTopics,
	}, cdcStream, cdcHandler, logger, metrics)

	go func() {
		if runErr := cdcConsumer.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
			logger.Error("cdc consumer terminated with error", slog.String("error", runErr.Error()))
		}
	}()

	var draining atomic.Bool

	var authenticator middleware.Authenticator
	if cfg.JWTPublicKeyPEM.Unmasked() == "" {
		logger.Error("JWT_PUBLIC_KEY_PEM must be configured; running with fallback authenticator is disabled")
		os.Exit(1)
	}
	jwtAuth, err := middleware.NewJWTAuthenticator(cfg.JWTPublicKeyPEM.Unmasked(), cfg.JWTIssuer, cfg.JWTAudience)
	if err != nil {
		logger.Error("failed to parse jwt public key pem", slog.String("error", err.Error()))
		os.Exit(1)
	}
	authenticator = jwtAuth

	routerHandler := transporthttp.NewRouter(transporthttp.RouterConfig{
		Cache:    subCache,
		Metrics:  metrics,
		Logger:   logger,
		Draining: &draining,
		ReadinessDeps: &transporthttp.ReadinessDependency{
			RedisConnected: redisClient.IsConnected,
			CDCConsuming:   cdcConsumer.IsConsuming,
		},
		StreamHandler:   streamHandler,
		SessionsHandler: sessionsHandler,
		InvalidationSub: invalidationSub,
		Authenticator:   authenticator,
		RateLimiter:     rateLimiter,
	})

	srv := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           routerHandler,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("starting rag-notification-dispatch-daemon",
			slog.String("environment", cfg.Environment),
			slog.String("node_id", cfg.NodeID),
			slog.String("http_port", cfg.HTTPPort),
			slog.Any("postgres_dsn", cfg.PostgresDSN),
			slog.Any("redis_password", cfg.RedisPassword),
			slog.Int("default_sse_retry_ms", entity.DefaultSSERetryMs),
		)
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("http server terminated with error", slog.String("error", serveErr.Error()))
			serverErrCh <- serveErr
		}
		close(serverErrCh)
	}()

	select {
	case serveErr, ok := <-serverErrCh:
		if ok && serveErr != nil {
			stop()
			os.Exit(1)
		}
	case <-ctx.Done():
	}

	draining.Store(true)
	drainDelay := ComputeDrainDelay(cfg.DrainJitterMin, cfg.DrainJitterMax)
	logger.Info("SIGTERM received; marking /ready as 503 and initiating staggered graceful drain",
		slog.String("node_id", cfg.NodeID),
		slog.Duration("drain_jitter_min", cfg.DrainJitterMin),
		slog.Duration("drain_jitter_max", cfg.DrainJitterMax),
		slog.Duration("staggered_drain_delay", drainDelay),
	)

	shutdownBudget := defaultShutdownBudget
	if cfg.DrainJitterMax+15*time.Second > shutdownBudget {
		shutdownBudget = cfg.DrainJitterMax + 15*time.Second
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownBudget)
	defer cancel()

	// Step 1: Assign independent randomized drain delay across active sockets (ADR-005).
	drainDoneCh := connectionHub.DrainWithJitter(shutdownCtx, cfg.DrainJitterMin, cfg.DrainJitterMax)

	// Step 2: Stop accepting new connections on listener.
	listenerWaitCtx, listenerCancel := context.WithTimeout(shutdownCtx, drainDelay)
	_ = srv.Shutdown(listenerWaitCtx)
	listenerCancel()

	// Step 3: Wait for all staggered connection drains to finish or context timeout.
	select {
	case <-drainDoneCh:
		logger.Info("all active sse connections drained gracefully")
	case <-shutdownCtx.Done():
		logger.Warn("shutdown deadline reached during staggered sse drain")
	}

	_ = srv.Shutdown(shutdownCtx)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
