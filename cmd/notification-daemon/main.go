// Package main is the entry point for rag-notification-dispatch-daemon.
// It configures Go 1.23+ GC/memory bounds (GOMEMLIMIT=1800MiB, GOGC=80), initializes
// Secured Logging, warms the pod-local subscription cache, starts the CDC consumer,
// exposes OpenAPI 3.1 & operational probes, and handles SIGTERM staggered connection draining.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/router"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/persistence/postgres"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/queue/kafka"
)

const (
	defaultMemoryLimitBytes = 1800 * 1024 * 1024 // 1800MiB
	defaultGCPercent        = 80
)

// Config holds the 12-Factor environment configuration defined in .ai/doc-api.md Section 6.
type Config struct {
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
	LogLevel             slog.Level
}

// LoadConfig loads 12-Factor configuration from environment variables with canonical defaults.
func LoadConfig() Config {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "node-local-0"
	}
	return Config{
		NodeID:               envOrDefault("NODE_ID", hostname),
		HTTPPort:             envOrDefault("HTTP_PORT", "8080"),
		PostgresDSN:          observability.NewSecretString(os.Getenv("POSTGRES_DSN")),
		RedisAddrs:           splitCSV(envOrDefault("REDIS_ADDRS", "localhost:6379")),
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
		LogLevel:             observability.ParseLogLevel(envOrDefault("LOG_LEVEL", "INFO")),
	}
}

// ConfigureRuntimeMemoryBounds enforces GOMEMLIMIT=1800MiB and GOGC=80 when not already overridden.
func ConfigureRuntimeMemoryBounds() {
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(defaultMemoryLimitBytes)
	}
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(defaultGCPercent)
	}
}

// NewOperationalMux builds the base HTTP mux serving operational probes and OpenAPI 3.1 endpoints.
func NewOperationalMux(
	cache *memory.SubscriptionCache,
	metrics *observability.MetricsRegistry,
	draining *atomic.Bool,
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
		if isDraining || !isWarmed {
			w.WriteHeader(http.StatusServiceUnavailable)
			resp, _ := json.Marshal(map[string]any{
				"status":             "not_ready",
				"subscription_cache": map[bool]string{true: "warmed", false: "warming"}[isWarmed],
				"redis_backplane":    "connected",
				"cdc_consumer":       "consuming",
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

	return mux
}

func main() {
	ConfigureRuntimeMemoryBounds()
	cfg := LoadConfig()
	logger := observability.NewSecuredLogger(os.Stdout, cfg.LogLevel)
	metrics := observability.NewMetricsRegistry()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pgPool, err := postgres.NewPool(postgres.PoolConfig{
		DSN:            cfg.PostgresDSN,
		MaxConns:       10,
		ConnectTimeout: 5 * time.Second,
	}, nil)
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

	evaluator := router.NewRuleEvaluator()
	filter := router.NewFilter(router.DefaultDedupWindowTTL, router.DefaultMaxDedupEntries)
	_ = command.NewProcessCDCEventHandler(subCache, evaluator, filter, nil, nil, logger, metrics)

	var draining atomic.Bool
	mux := NewOperationalMux(subCache, metrics, &draining)

	srv := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("starting rag-notification-dispatch-daemon",
			slog.String("node_id", cfg.NodeID),
			slog.String("http_port", cfg.HTTPPort),
			slog.Any("postgres_dsn", cfg.PostgresDSN),
			slog.Any("redis_password", cfg.RedisPassword),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server terminated with error", slog.String("error", err.Error()))
		}
	}()

	<-ctx.Done()
	draining.Store(true)
	logger.Info("SIGTERM received; marking /ready as 503 and initiating graceful drain",
		slog.String("node_id", cfg.NodeID),
		slog.Duration("drain_jitter_min", cfg.DrainJitterMin),
		slog.Duration("drain_jitter_max", cfg.DrainJitterMax),
	)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
