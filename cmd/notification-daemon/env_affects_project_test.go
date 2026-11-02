package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/config"
)

// TestEnvFileDirectlyAffectsProject verifies that changes in the value of [.env]
// dynamically alter the daemon's 12-Factor Config struct, runtime memory limits,
// and GC targets.
func TestEnvFileDirectlyAffectsProject(t *testing.T) {
	tmpDir := t.TempDir()
	envPath := filepath.Join(tmpDir, ".env")

	// Phase 1: Write initial .env values
	initialEnvContent := `
NODE_ID=node-env-affected-01
HTTP_PORT=9876
LOG_LEVEL=DEBUG
SSE_RING_BUFFER_SIZE=512
SSE_SLOW_CONSUMER_TIMEOUT=8s
SSE_HEARTBEAT_INTERVAL=20s
DRAIN_JITTER_MIN=12s
DRAIN_JITTER_MAX=24s
WEBHOOK_QUEUE_STREAM=custom:webhook:stream
KAFKA_BROKERS=broker1:9092,broker2:9092
KAFKA_CONSUMER_GROUP=custom-cg
KAFKA_CDC_TOPICS=custom.cdc.one,custom.cdc.two
REDIS_ADDRS=redis-host1:6379,redis-host2:6379
POSTGRES_DSN=postgres://custom_user:custom_pass@db:5432/custom_db
GOMEMLIMIT=1500MiB
GOGC=65
`
	if err := os.WriteFile(envPath, []byte(initialEnvContent), 0600); err != nil {
		t.Fatalf("failed to write initial .env: %v", err)
	}

	// Tell LoadConfig to read from our test .env file
	t.Setenv("ENV_FILE", envPath)
	t.Setenv("DOTENV_OVERRIDE", "true")

	cfg1 := LoadConfig()

	if cfg1.NodeID != "node-env-affected-01" {
		t.Errorf("NodeID: got %q, want %q", cfg1.NodeID, "node-env-affected-01")
	}
	if cfg1.HTTPPort != "9876" {
		t.Errorf("HTTPPort: got %q, want %q", cfg1.HTTPPort, "9876")
	}
	if cfg1.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel: got %v, want %v", cfg1.LogLevel, slog.LevelDebug)
	}
	if cfg1.SSERingBufferSize != 512 {
		t.Errorf("SSERingBufferSize: got %d, want 512", cfg1.SSERingBufferSize)
	}
	if cfg1.SSESlowConsumerLimit != 8*time.Second {
		t.Errorf("SSESlowConsumerLimit: got %v, want 8s", cfg1.SSESlowConsumerLimit)
	}
	if cfg1.SSEHeartbeatInterval != 20*time.Second {
		t.Errorf("SSEHeartbeatInterval: got %v, want 20s", cfg1.SSEHeartbeatInterval)
	}
	if cfg1.DrainJitterMin != 12*time.Second {
		t.Errorf("DrainJitterMin: got %v, want 12s", cfg1.DrainJitterMin)
	}
	if cfg1.DrainJitterMax != 24*time.Second {
		t.Errorf("DrainJitterMax: got %v, want 24s", cfg1.DrainJitterMax)
	}
	if cfg1.WebhookQueueStream != "custom:webhook:stream" {
		t.Errorf("WebhookQueueStream: got %q, want %q", cfg1.WebhookQueueStream, "custom:webhook:stream")
	}
	if len(cfg1.KafkaBrokers) != 2 || cfg1.KafkaBrokers[0] != "broker1:9092" {
		t.Errorf("KafkaBrokers: got %v", cfg1.KafkaBrokers)
	}
	if cfg1.KafkaConsumerGroup != "custom-cg" {
		t.Errorf("KafkaConsumerGroup: got %q, want %q", cfg1.KafkaConsumerGroup, "custom-cg")
	}
	if len(cfg1.KafkaCDCTopics) != 2 || cfg1.KafkaCDCTopics[1] != "custom.cdc.two" {
		t.Errorf("KafkaCDCTopics: got %v", cfg1.KafkaCDCTopics)
	}
	if len(cfg1.RedisAddrs) != 2 || cfg1.RedisAddrs[0] != "redis-host1:6379" {
		t.Errorf("RedisAddrs: got %v", cfg1.RedisAddrs)
	}
	if cfg1.PostgresDSN.Unmasked() != "postgres://custom_user:custom_pass@db:5432/custom_db" {
		t.Errorf("PostgresDSN: got %q", cfg1.PostgresDSN.Unmasked())
	}

	// Verify runtime bounds parsed from .env
	expectedMemLimit := int64(1500 * 1024 * 1024)
	if parsedLimit := config.ParseMemoryLimitBytes(os.Getenv("GOMEMLIMIT"), 0); parsedLimit != expectedMemLimit {
		t.Errorf("GOMEMLIMIT: got %d, want %d", parsedLimit, expectedMemLimit)
	}
	if parsedGC := config.ParseGCPercent(os.Getenv("GOGC"), 0); parsedGC != 65 {
		t.Errorf("GOGC: got %d, want 65", parsedGC)
	}

	// Phase 2: Modify .env values and verify they IMMEDIATELY affect the project on reload
	modifiedEnvContent := `
NODE_ID=node-env-affected-02
HTTP_PORT=9999
LOG_LEVEL=ERROR
SSE_RING_BUFFER_SIZE=1024
SSE_SLOW_CONSUMER_TIMEOUT=15s
SSE_HEARTBEAT_INTERVAL=30s
GOMEMLIMIT=2GiB
GOGC=100
`
	if err := os.WriteFile(envPath, []byte(modifiedEnvContent), 0600); err != nil {
		t.Fatalf("failed to update .env: %v", err)
	}

	cfg2 := LoadConfig()

	if cfg2.NodeID != "node-env-affected-02" {
		t.Errorf("modified NodeID: got %q, want %q", cfg2.NodeID, "node-env-affected-02")
	}
	if cfg2.HTTPPort != "9999" {
		t.Errorf("modified HTTPPort: got %q, want %q", cfg2.HTTPPort, "9999")
	}
	if cfg2.LogLevel != slog.LevelError {
		t.Errorf("modified LogLevel: got %v, want %v", cfg2.LogLevel, slog.LevelError)
	}
	if cfg2.SSERingBufferSize != 1024 {
		t.Errorf("modified SSERingBufferSize: got %d, want 1024", cfg2.SSERingBufferSize)
	}
	if cfg2.SSESlowConsumerLimit != 15*time.Second {
		t.Errorf("modified SSESlowConsumerLimit: got %v, want 15s", cfg2.SSESlowConsumerLimit)
	}
	if cfg2.SSEHeartbeatInterval != 30*time.Second {
		t.Errorf("modified SSEHeartbeatInterval: got %v, want 30s", cfg2.SSEHeartbeatInterval)
	}

	expectedMemLimit2 := int64(2 * 1024 * 1024 * 1024)
	if parsedLimit := config.ParseMemoryLimitBytes(os.Getenv("GOMEMLIMIT"), 0); parsedLimit != expectedMemLimit2 {
		t.Errorf("modified GOMEMLIMIT: got %d, want %d", parsedLimit, expectedMemLimit2)
	}
	if parsedGC := config.ParseGCPercent(os.Getenv("GOGC"), 0); parsedGC != 100 {
		t.Errorf("modified GOGC: got %d, want 100", parsedGC)
	}
}
