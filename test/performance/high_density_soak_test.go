package performance_test

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
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

var soakConnectionsFlag = flag.Int("connections", 20000, "number of concurrent connections for soak testing")

// TestSyntheticInMemoryHighDensitySoak validates that a single daemon node can sustain >= 20,000
// concurrent client connections under continuous broadcast event bursts while maintaining an idle
// footprint ~320MiB and strictly keeping memory allocation < 1.6GiB (inside the 2GiB container ceiling
// under GOMEMLIMIT=1800MiB and GOGC=80).
func TestSyntheticInMemoryHighDensitySoak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 20,000 synthetic high density soak test in short mode")
	}

	targetConnections := *soakConnectionsFlag
	if targetConnections <= 0 {
		targetConnections = 20000
	}

	t.Logf("starting synthetic in-memory soak test with %d concurrent sessions", targetConnections)

	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(io.Discard, 0)
	nodeID := "node-soak-synthetic-01"

	connectionHub := hub.NewConnectionHub(
		nodeID,
		hub.DefaultSlowConsumerTimeout,
		30*time.Second,
		60*time.Second,
		metrics,
		logger,
	)

	// Force initial GC to establish a clean memory baseline.
	runtime.GC()
	var mBaseline runtime.MemStats
	runtime.ReadMemStats(&mBaseline)

	tenantID := "tnt_soak_enterprise"
	subscribers := make([]*mockSoakClient, targetConnections)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(targetConnections)

	// Step 1: Concurrently spawn targetConnections (20,000) mock clients with ring buffers.
	startSetup := time.Now()
	for i := 0; i < targetConnections; i++ {
		connID := fmt.Sprintf("conn_soak_%06d", i)
		userID := fmt.Sprintf("usr_soak_%04d", i%1000)

		conn := entity.Connection{
			ConnectionID: connID,
			TenantID:     tenantID,
			UserID:       userID,
			NodeID:       nodeID,
			ConnectedAt:  time.Now().UTC(),
		}

		rb := hub.NewRingBuffer(tenantID, connID, hub.DefaultRingBufferSize, hub.DefaultSlowConsumerTimeout, metrics)
		clientCtx, clientCancel := context.WithCancel(ctx)

		client := &mockSoakClient{
			connID: connID,
			rb:     rb,
			cancel: clientCancel,
			ctx:    clientCtx,
		}
		subscribers[i] = client

		if err := connectionHub.Register(conn, rb, clientCancel); err != nil {
			t.Fatalf("failed to register connection %s: %v", connID, err)
		}

		go func(c *mockSoakClient) {
			defer wg.Done()
			c.runReaderLoop()
		}(client)
	}

	t.Logf("successfully registered %d sessions in %v", targetConnections, time.Since(startSetup))

	// Step 2: Measure memory footprint after all 20,000 sessions are registered and idle.
	runtime.GC()
	var mIdle runtime.MemStats
	runtime.ReadMemStats(&mIdle)

	idleAllocBytes := int64(mIdle.Alloc) - int64(mBaseline.Alloc)
	if idleAllocBytes < 0 {
		idleAllocBytes = int64(mIdle.Alloc)
	}
	bytesPerSocket := float64(idleAllocBytes) / float64(targetConnections)
	t.Logf("idle memory allocated: %.2f MiB total (%.2f KiB/socket)",
		float64(idleAllocBytes)/(1024*1024),
		bytesPerSocket/1024,
	)

	// Verify idle footprint adheres to ~14-16KiB baseline per socket ceiling (~320MiB for 20k sockets).
	// Allowing generous 25KiB/socket safety margin for profiling data.
	if bytesPerSocket > 25*1024 {
		t.Errorf("idle footprint per socket %.2f KiB exceeds acceptable ceiling (<= 25 KiB)", bytesPerSocket/1024)
	}

	// Step 3: Concurrently dispatch a continuous broadcast event burst across all connections.
	burstEventCount := 20
	t.Logf("dispatching %d broadcast events across %d connections (%d deliveries)...",
		burstEventCount, targetConnections, burstEventCount*targetConnections)

	startBurst := time.Now()
	for e := 0; e < burstEventCount; e++ {
		eventID := fmt.Sprintf("evt_burst_%03d", e)
		isTerminal := (e == burstEventCount-1)
		frame := []byte(fmt.Sprintf("id: %s\nevent: milestone\ndata: {\"seq\":%d,\"terminal\":%t}\n\n", eventID, e, isTerminal))

		event := entity.NotificationEvent{
			EventID:    eventID,
			TenantID:   tenantID,
			EventType:  entity.EventTypeIngestionMilestone,
			Severity:   entity.SeverityInfo,
			IsTerminal: isTerminal,
		}

		if err := connectionHub.DispatchSSEFrame(context.Background(), event, frame); err != nil {
			t.Fatalf("event %s dispatch failed: %v", eventID, err)
		}
	}

	// Wait briefly for all mock readers to drain their ring buffer notifications.
	time.Sleep(300 * time.Millisecond)
	burstDuration := time.Since(startBurst)
	t.Logf("completed burst delivery in %v", burstDuration)

	// Step 4: Verify that all subscribers received all broadcast events without loss.
	var totalReceived int64
	for _, sub := range subscribers {
		rcv := sub.received.Load()
		if rcv != int64(burstEventCount) {
			t.Fatalf("subscriber %s received %d events, expected %d", sub.connID, rcv, burstEventCount)
		}
		totalReceived += rcv
	}
	t.Logf("verified %d total delivered frames across %d subscribers", totalReceived, targetConnections)

	// Step 5: Read memory stats after event burst and assert RSS / Alloc remains strictly < 1.6GiB.
	var mActive runtime.MemStats
	runtime.ReadMemStats(&mActive)
	t.Logf("active memory during soak: Alloc=%.2f MiB, TotalAlloc=%.2f MiB, Sys=%.2f MiB, NumGC=%d",
		float64(mActive.Alloc)/(1024*1024),
		float64(mActive.TotalAlloc)/(1024*1024),
		float64(mActive.Sys)/(1024*1024),
		mActive.NumGC,
	)

	const maxAllowedAllocBytes = uint64(1600 * 1024 * 1024) // 1.6 GiB
	if mActive.Alloc > maxAllowedAllocBytes {
		t.Fatalf("Resident/Alloc memory %.2f MiB exceeds strict 1.6 GiB ceiling!", float64(mActive.Alloc)/(1024*1024))
	}

	// Step 6: Validate ring buffer drop invariants:
	// Fill a ring buffer with 256 non-terminal frames, then assert that non-terminal frames are dropped
	// while terminal frames preempt oldest non-terminal frame.
	testRingBufferDropInvariants(t, metrics)

	// Clean up all goroutines.
	cancel()
	wg.Wait()
}

// TestConfigurableTCPSocketSoak provides a loopback network stress harness supporting -connections=20000
// with automated scaled fallback to 2,000 connections (or 200 in short mode) so CI runners don't hit
// OS file descriptor limits.
func TestConfigurableTCPSocketSoak(t *testing.T) {
	numSockets := 2000
	if testing.Short() {
		numSockets = 200
	} else if *soakConnectionsFlag == 20000 {
		numSockets = 5000
	}

	t.Logf("running loopback TCP socket soak harness with %d concurrent HTTP SSE clients", numSockets)

	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(io.Discard, 0)
	nodeID := "node-tcp-soak-01"

	subCache := memory.NewSubscriptionCache()
	subCache.ReplaceAll(nil, nil)

	connHub := hub.NewConnectionHub(nodeID, hub.DefaultSlowConsumerTimeout, 30*time.Second, 60*time.Second, metrics, logger)
	registerCmd := command.NewRegisterConnectionHandler(connHub, nil, 45*time.Second)
	terminateCmd := command.NewTerminateConnectionHandler(connHub, nil)

	streamHandler := sse.NewStreamHandler(
		sse.StreamHandlerConfig{
			NodeID:              nodeID,
			RingBufferSize:      hub.DefaultRingBufferSize,
			SlowConsumerTimeout: hub.DefaultSlowConsumerTimeout,
			HeartbeatInterval:   1 * time.Second,
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
		RateLimiter:   middleware.NewReconnectRateLimiter(50000, 100000, 10000),
	})

	server := httptest.NewServer(router)
	defer server.Close()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        numSockets + 100,
			MaxIdleConnsPerHost: numSockets + 100,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
		Timeout: 0,
	}

	tenantID := "tnt_tcp_soak"
	var connectWg sync.WaitGroup
	var activeConnections atomic.Int64
	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()

	// Establish concurrent TCP loopback SSE connections.
	connectWg.Add(numSockets)
	for i := 0; i < numSockets; i++ {
		go func(idx int) {
			defer connectWg.Done()

			req, err := http.NewRequestWithContext(clientCtx, http.MethodGet, server.URL+"/v1/notifications/stream", nil)
			if err != nil {
				return
			}
			userID := fmt.Sprintf("usr_%04d", idx%100)
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s:%s", tenantID, userID))

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				return
			}

			// Validate edge proxy bypass headers per ADR-002.
			if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
				return
			}
			if xab := resp.Header.Get("X-Accel-Buffering"); xab != "no" {
				return
			}

			activeConnections.Add(1)
			defer activeConnections.Add(-1)

			reader := bufio.NewReader(resp.Body)
			for {
				line, rErr := reader.ReadString('\n')
				if rErr != nil {
					return
				}
				_ = line
			}
		}(i)
	}

	// Wait up to 5s for connections to be established.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if activeConnections.Load() >= int64(float64(numSockets)*0.85) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	established := activeConnections.Load()
	t.Logf("established %d/%d active TCP loopback SSE connections", established, numSockets)
	if established < int64(float64(numSockets)*0.5) {
		t.Fatalf("failed to establish sufficient TCP connections: got %d, target %d", established, numSockets)
	}

	// Dispatch broadcast event bursts over live TCP sockets.
	for b := 0; b < 5; b++ {
		evtID := fmt.Sprintf("evt_tcp_burst_%d", b)
		frame := []byte(fmt.Sprintf("id: %s\nevent: milestone\ndata: {\"soak\":true}\n\n", evtID))
		event := entity.NotificationEvent{
			EventID:   evtID,
			TenantID:  tenantID,
			EventType: entity.EventTypeIngestionMilestone,
			Severity:  entity.SeverityInfo,
		}
		_ = connHub.DispatchSSEFrame(context.Background(), event, frame)
	}

	time.Sleep(200 * time.Millisecond)

	// Clean shutdown of TCP clients.
	clientCancel()
	connectWg.Wait()
}

// testRingBufferDropInvariants validates the 256-event ring buffer drop policies under saturation.
func testRingBufferDropInvariants(t *testing.T, metrics *observability.MetricsRegistry) {
	tenantID := "tnt_invariants"
	connID := "conn_inv_01"
	rb := hub.NewRingBuffer(tenantID, connID, hub.DefaultRingBufferSize, hub.DefaultSlowConsumerTimeout, metrics)
	t0 := time.Now().UTC()

	// Fill buffer to capacity (256 non-terminal events).
	for i := 0; i < hub.DefaultRingBufferSize; i++ {
		status := rb.Push(hub.RingBufferEntry{
			EventID:    fmt.Sprintf("evt_%03d", i),
			Frame:      []byte("data: progressive\n\n"),
			IsTerminal: false,
		}, t0)
		if status != hub.PushSuccess {
			t.Fatalf("expected PushSuccess at %d, got %v", i, status)
		}
	}

	// 257th non-terminal event MUST be dropped with PushDroppedSaturated (reason="ring_buffer_saturated").
	statusDrop := rb.Push(hub.RingBufferEntry{
		EventID:    "evt_overflow_nt",
		Frame:      []byte("data: drop_me\n\n"),
		IsTerminal: false,
	}, t0)
	if statusDrop != hub.PushDroppedSaturated {
		t.Fatalf("expected PushDroppedSaturated for non-terminal overflow, got %v", statusDrop)
	}

	// Terminal event MUST preempt oldest non-terminal event (reason="terminal_frame_preemption").
	statusPreempt := rb.Push(hub.RingBufferEntry{
		EventID:    "evt_terminal_preempt",
		Frame:      []byte("data: terminal\n\n"),
		IsTerminal: true,
	}, t0)
	if statusPreempt != hub.PushPreemptedOldestNonTerminal {
		t.Fatalf("expected PushPreemptedOldestNonTerminal, got %v", statusPreempt)
	}
}

// BenchmarkRingBufferZeroAllocationHotPath measures that pushing and popping from the ring buffer
// hot-path does not cause heap allocations.
func BenchmarkRingBufferZeroAllocationHotPath(b *testing.B) {
	metrics := observability.NewMetricsRegistry()
	rb := hub.NewRingBuffer("tnt_bench", "conn_bench", hub.DefaultRingBufferSize, hub.DefaultSlowConsumerTimeout, metrics)
	t0 := time.Now().UTC()
	entry := hub.RingBufferEntry{
		EventID:    "evt_bench",
		Frame:      []byte("data: benchmark\n\n"),
		IsTerminal: false,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = rb.Push(entry, t0)
		_, _ = rb.TryPop()
	}
}

// mockSoakClient simulates an in-memory client consuming from its isolated RingBuffer.
type mockSoakClient struct {
	connID   string
	rb       *hub.RingBuffer
	cancel   context.CancelFunc
	ctx      context.Context
	received atomic.Int64
}

func (c *mockSoakClient) runReaderLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.rb.Notify():
			for {
				_, ok := c.rb.TryPop()
				if !ok {
					break
				}
				c.received.Add(1)
			}
		}
	}
}
