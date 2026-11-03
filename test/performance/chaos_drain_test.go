package performance_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/query"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	transporthttp "github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/http/middleware"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/sse"
)

// TestSIGTERMGracefulDrainingReadinessAndJitter verifies ADR-005 staggered graceful draining:
//  1. On SIGTERM, /ready immediately flips to 503 Service Unavailable, while /healthz remains 200 OK.
//  2. Active SSE streams receive an SSE `retry: <ms>\n\n` directive frame with randomized jitter
//     spread across the configured window (e.g. 30s-60s in production; scaled for deterministic testing).
//  3. Sockets close in a staggered distribution rather than all terminating simultaneously.
func TestSIGTERMGracefulDrainingReadinessAndJitter(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(io.Discard, 0)
	nodeID := "node-chaos-drain-01"

	subCache := memory.NewSubscriptionCache()
	subCache.ReplaceAll(nil, nil)

	// Use scaled jitter window [80ms, 200ms] for fast deterministic socket lifecycle assertions.
	minJitter := 80 * time.Millisecond
	maxJitter := 200 * time.Millisecond

	connHub := hub.NewConnectionHub(nodeID, hub.DefaultSlowConsumerTimeout, minJitter, maxJitter, metrics, logger)
	registerCmd := command.NewRegisterConnectionHandler(connHub, nil, 45*time.Second)
	terminateCmd := command.NewTerminateConnectionHandler(connHub, nil)

	streamHandler := sse.NewStreamHandler(
		sse.StreamHandlerConfig{
			NodeID:              nodeID,
			RingBufferSize:      hub.DefaultRingBufferSize,
			SlowConsumerTimeout: hub.DefaultSlowConsumerTimeout,
			HeartbeatInterval:   10 * time.Second,
			DefaultRetryMs:      3000,
		},
		connHub,
		registerCmd,
		terminateCmd,
		nil,
		nil,
		metrics,
		logger,
	)

	var draining atomic.Bool
	router := transporthttp.NewRouter(transporthttp.RouterConfig{
		Cache:         subCache,
		Metrics:       metrics,
		Logger:        logger,
		Draining:      &draining,
		StreamHandler: streamHandler,
		Authenticator: middleware.SimpleTokenAuthenticator{},
		RateLimiter:   middleware.NewReconnectRateLimiter(10000, 20000, 10000),
	})

	server := httptest.NewServer(router)
	defer server.Close()

	// Step 1: Verify operational probes before shutdown: /ready is 200 OK, /healthz is 200 OK.
	respHealth, err := http.Get(server.URL + "/healthz")
	if err != nil || respHealth.StatusCode != http.StatusOK {
		t.Fatalf("expected /healthz 200, got %v (status %d)", err, respHealth.StatusCode)
	}
	respHealth.Body.Close()

	respReadyBefore, err := http.Get(server.URL + "/ready")
	if err != nil || respReadyBefore.StatusCode != http.StatusOK {
		t.Fatalf("expected /ready 200 before draining, got %v (status %d)", err, respReadyBefore.StatusCode)
	}
	respReadyBefore.Body.Close()

	// Step 2: Connect 50 active SSE streams to the server.
	numStreams := 50
	tenantID := "tnt_chaos_drain"
	type streamResult struct {
		retryMs       int
		closeDuration time.Duration
		err           error
	}

	results := make([]streamResult, numStreams)
	var connectWg sync.WaitGroup
	var activeStreams atomic.Int64
	streamStart := time.Now()

	connectWg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		go func(idx int) {
			defer connectWg.Done()

			req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/notifications/stream", nil)
			userID := fmt.Sprintf("usr_%03d", idx)
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s:%s", tenantID, userID))

			client := &http.Client{Timeout: 0}
			resp, rErr := client.Do(req)
			if rErr != nil {
				results[idx].err = rErr
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				results[idx].err = fmt.Errorf("unexpected status %d", resp.StatusCode)
				return
			}

			activeStreams.Add(1)

			reader := bufio.NewReader(resp.Body)
			for {
				line, readErr := reader.ReadString('\n')
				if readErr != nil {
					results[idx].closeDuration = time.Since(streamStart)
					break
				}
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "retry:") {
					parts := strings.Split(line, ":")
					if len(parts) >= 2 {
						ms, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
						results[idx].retryMs = ms
					}
				}
			}
		}(i)
	}

	// Wait for streams to connect.
	for i := 0; i < 50; i++ {
		if activeStreams.Load() >= int64(numStreams) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("established %d active streams for graceful draining test", activeStreams.Load())

	// Step 3: Trigger SIGTERM simulation: mark draining=true and start DrainWithJitter.
	draining.Store(true)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()

	drainDoneCh := connHub.DrainWithJitter(drainCtx, minJitter, maxJitter)

	// Step 4: Immediately assert /ready flips to 503 Service Unavailable with draining=true.
	respReadyAfter, err := http.Get(server.URL + "/ready")
	if err != nil {
		t.Fatalf("failed to query /ready: %v", err)
	}
	defer respReadyAfter.Body.Close()

	if respReadyAfter.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected /ready to immediately return 503 on SIGTERM, got %d", respReadyAfter.StatusCode)
	}

	var readyBody map[string]any
	if decErr := json.NewDecoder(respReadyAfter.Body).Decode(&readyBody); decErr != nil {
		t.Fatalf("failed to decode /ready response: %v", decErr)
	}
	if readyBody["draining"] != true {
		t.Fatalf("expected draining: true in /ready response, got %v", readyBody["draining"])
	}

	// /healthz MUST still remain 200 OK (daemon is alive and draining gracefully).
	respHealthAfter, err := http.Get(server.URL + "/healthz")
	if err != nil || respHealthAfter.StatusCode != http.StatusOK {
		t.Fatalf("expected /healthz 200 during draining, got status %d", respHealthAfter.StatusCode)
	}
	respHealthAfter.Body.Close()

	// Wait for draining and all streams to complete.
	select {
	case <-drainDoneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DrainWithJitter to complete")
	}

	connectWg.Wait()

	// Step 5: Verify that all streams received valid `retry:` directive frames and closed in staggered fashion.
	minClose := time.Duration(1<<62 - 1)
	maxClose := time.Duration(0)
	retryDirectivesReceived := 0

	for _, res := range results {
		if res.err != nil {
			t.Fatalf("stream encountered error: %v", res.err)
		}
		if res.retryMs > 0 {
			retryDirectivesReceived++
			// Retry duration should be within jitter window bounds.
			if time.Duration(res.retryMs)*time.Millisecond < minJitter || time.Duration(res.retryMs)*time.Millisecond > maxJitter+50*time.Millisecond {
				t.Errorf("retryMs %d outside expected jitter window [%v, %v]", res.retryMs, minJitter, maxJitter)
			}
		}
		if res.closeDuration < minClose {
			minClose = res.closeDuration
		}
		if res.closeDuration > maxClose {
			maxClose = res.closeDuration
		}
	}

	if retryDirectivesReceived != len(results) {
		t.Fatalf("expected all %d active streams to receive retry directive frames before closure, got %d", len(results), retryDirectivesReceived)
	}
	if maxClose <= minClose {
		t.Fatalf("expected staggered close-time spread to be nonzero, got minClose=%v, maxClose=%v", minClose, maxClose)
	}

	t.Logf("graceful drain verified: %d/%d received retry directives, staggered close spread across %v (min: %v, max: %v)",
		retryDirectivesReceived, numStreams, maxClose-minClose, minClose, maxClose)
}

// TestGracefulDrainReconnectionThrottlingAndRedisProtection validates Runbook 2:
// When hundreds of clients reconnect after a node restart, the token-bucket reconnect rate limiter
// protects Redis CPU (< 60%) by throttling reconnection bursts with HTTP 429 Too Many Requests (Retry-After: 5).
func TestGracefulDrainReconnectionThrottlingAndRedisProtection(t *testing.T) {
	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(io.Discard, 0)
	nodeID := "node-surviving-02"

	subCache := memory.NewSubscriptionCache()
	subCache.ReplaceAll(nil, nil)

	connHub := hub.NewConnectionHub(nodeID, hub.DefaultSlowConsumerTimeout, 30*time.Second, 60*time.Second, metrics, logger)
	registerCmd := command.NewRegisterConnectionHandler(connHub, nil, 45*time.Second)
	terminateCmd := command.NewTerminateConnectionHandler(connHub, nil)

	// Mock replay streamer that simulates Redis CPU protection under load.
	mockReplay := &mockChaosReplayStreamer{
		replayEvents: []entity.NotificationEvent{
			{EventID: "evt_backfill_01", TenantID: "tnt_burst", EventType: entity.EventTypeIngestionMilestone},
		},
	}

	streamHandler := sse.NewStreamHandler(
		sse.StreamHandlerConfig{
			NodeID:              nodeID,
			RingBufferSize:      hub.DefaultRingBufferSize,
			SlowConsumerTimeout: hub.DefaultSlowConsumerTimeout,
			HeartbeatInterval:   10 * time.Second,
			DefaultRetryMs:      3000,
		},
		connHub,
		registerCmd,
		terminateCmd,
		nil,
		mockReplay,
		metrics,
		logger,
	)

	var draining atomic.Bool
	// Strict rate limiter: 5 tokens/sec, burst capacity of 10.
	rateLimiter := middleware.NewReconnectRateLimiter(5, 10, 1000)

	router := transporthttp.NewRouter(transporthttp.RouterConfig{
		Cache:         subCache,
		Metrics:       metrics,
		Logger:        logger,
		Draining:      &draining,
		StreamHandler: streamHandler,
		Authenticator: middleware.SimpleTokenAuthenticator{},
		RateLimiter:   rateLimiter,
	})

	server := httptest.NewServer(router)
	defer server.Close()

	tenantID := "tnt_burst"
	totalReconnectingClients := 30
	var successCount atomic.Int64
	var rateLimitedCount atomic.Int64
	var retryAfterHeaderSeen atomic.Bool

	var wg sync.WaitGroup
	wg.Add(totalReconnectingClients)

	// Burst 30 reconnecting clients simultaneously with Last-Event-ID header.
	for i := 0; i < totalReconnectingClients; i++ {
		go func(idx int) {
			defer wg.Done()

			req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/notifications/stream", nil)
			userID := fmt.Sprintf("usr_%03d", idx)
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s:%s", tenantID, userID))
			req.Header.Set("Last-Event-ID", "evt_watermark_00")

			client := &http.Client{Timeout: 1 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				successCount.Add(1)
			} else if resp.StatusCode == http.StatusTooManyRequests {
				rateLimitedCount.Add(1)
				if resp.Header.Get("Retry-After") == "5" {
					retryAfterHeaderSeen.Store(true)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("reconnection burst results: %d accepted (200 OK), %d throttled (429 Too Many Requests)",
		successCount.Load(), rateLimitedCount.Load())

	if rateLimitedCount.Load() == 0 {
		t.Fatal("expected excessive reconnection burst to be throttled by token-bucket rate limiter")
	}
	if !retryAfterHeaderSeen.Load() {
		t.Fatal("expected Retry-After: 5 header on HTTP 429 response")
	}
}

type mockChaosReplayStreamer struct {
	replayEvents []entity.NotificationEvent
	rateLimited  bool
}

func (m *mockChaosReplayStreamer) FetchReplayEvents(_ context.Context, _ string, _ string, _ int) ([]entity.NotificationEvent, error) {
	if m.rateLimited {
		return nil, query.ErrBackfillRateLimited
	}
	return m.replayEvents, nil
}
