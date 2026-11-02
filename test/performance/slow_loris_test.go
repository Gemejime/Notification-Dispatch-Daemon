package performance_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
)

// TestSlowLoris2000StalledConsumersEviction verifies that injecting 2,000 slow/throttled consumers
// (with zeroed read rate, simulating zeroed TCP receive windows) does NOT block the broadcast dispatch
// loop or impact healthy subscribers. It asserts that:
// 1. Ring buffer saturation ceiling engages after > 5s of continuous saturation (reason="slow_consumer_evicted").
// 2. All 2,000 stalled consumers are evicted and deregistered cleanly.
// 3. Healthy sibling subscribers continue receiving 100% of live broadcast events with zero dropped frames.
func TestSlowLoris2000StalledConsumersEviction(t *testing.T) {
	numSlowConsumers := 2000
	numHealthyConsumers := 50

	t.Logf("starting Slow-Loris test with %d stalled consumers and %d healthy subscribers",
		numSlowConsumers, numHealthyConsumers)

	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(io.Discard, 0)
	nodeID := "node-slow-loris-01"

	// 5-second slow consumer eviction limit per specification.
	slowConsumerTimeout := 5 * time.Second

	connHub := hub.NewConnectionHub(
		nodeID,
		slowConsumerTimeout,
		30*time.Second,
		60*time.Second,
		metrics,
		logger,
	)

	tenantID := "tnt_slow_loris_enterprise"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Register 2,000 stalled consumers (they NEVER pop from their ring buffer).
	type slowConsumer struct {
		connID string
		rb     *hub.RingBuffer
		cancel context.CancelFunc
	}
	slowClients := make([]*slowConsumer, numSlowConsumers)

	for i := 0; i < numSlowConsumers; i++ {
		connID := fmt.Sprintf("conn_slow_%05d", i)
		conn := entity.Connection{
			ConnectionID: connID,
			TenantID:     tenantID,
			UserID:       fmt.Sprintf("usr_slow_%04d", i),
			NodeID:       nodeID,
			ConnectedAt:  time.Now().UTC(),
		}

		rb := hub.NewRingBuffer(tenantID, connID, hub.DefaultRingBufferSize, slowConsumerTimeout, metrics)
		clientCtx, clientCancel := context.WithCancel(ctx)
		_ = clientCtx

		sc := &slowConsumer{
			connID: connID,
			rb:     rb,
			cancel: clientCancel,
		}
		slowClients[i] = sc

		if err := connHub.Register(conn, rb, clientCancel); err != nil {
			t.Fatalf("failed to register slow connection %s: %v", connID, err)
		}
		// Do NOT start a reader loop for slow consumers: they represent stalled/zeroed TCP receive windows.
	}

	// 2. Register healthy consumers who read promptly from their ring buffers.
	type healthyConsumer struct {
		connID   string
		rb       *hub.RingBuffer
		cancel   context.CancelFunc
		ctx      context.Context
		received atomic.Int64
	}
	healthyClients := make([]*healthyConsumer, numHealthyConsumers)

	var healthyWg sync.WaitGroup
	healthyWg.Add(numHealthyConsumers)

	for i := 0; i < numHealthyConsumers; i++ {
		connID := fmt.Sprintf("conn_healthy_%03d", i)
		conn := entity.Connection{
			ConnectionID: connID,
			TenantID:     tenantID,
			UserID:       fmt.Sprintf("usr_healthy_%03d", i),
			NodeID:       nodeID,
			ConnectedAt:  time.Now().UTC(),
		}

		rb := hub.NewRingBuffer(tenantID, connID, hub.DefaultRingBufferSize, slowConsumerTimeout, metrics)
		clientCtx, clientCancel := context.WithCancel(ctx)

		hc := &healthyConsumer{
			connID: connID,
			rb:     rb,
			cancel: clientCancel,
			ctx:    clientCtx,
		}
		healthyClients[i] = hc

		if err := connHub.Register(conn, rb, clientCancel); err != nil {
			t.Fatalf("failed to register healthy connection %s: %v", connID, err)
		}

		go func(c *healthyConsumer) {
			defer healthyWg.Done()
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
		}(hc)
	}

	initialSessions, _, totalRegistered := connHub.ListActiveSessions(tenantID, 500, "")
	_ = initialSessions
	t.Logf("verified %d total active sessions registered in hub", totalRegistered)
	if totalRegistered != numSlowConsumers+numHealthyConsumers {
		t.Fatalf("expected %d sessions, got %d", numSlowConsumers+numHealthyConsumers, totalRegistered)
	}

	// 3. Dispatch initial 256 non-terminal events at t0 to saturate the ring buffers of all clients.
	t0 := time.Now().UTC()
	for i := 0; i < hub.DefaultRingBufferSize; i++ {
		evtID := fmt.Sprintf("evt_sat_%03d", i)
		frame := []byte(fmt.Sprintf("id: %s\nevent: milestone\ndata: {\"saturation_seq\":%d}\n\n", evtID, i))
		event := entity.NotificationEvent{
			EventID:    evtID,
			TenantID:   tenantID,
			EventType:  entity.EventTypeIngestionMilestone,
			Severity:   entity.SeverityInfo,
			IsTerminal: false,
		}
		_ = connHub.DispatchSSEFrame(context.Background(), event, frame)
	}

	// Assert that slow consumers are now fully saturated (count == 256).
	for _, sc := range slowClients {
		if sc.rb.Count() != hub.DefaultRingBufferSize {
			t.Fatalf("expected slow client %s ring buffer to be full (256), got %d", sc.connID, sc.rb.Count())
		}
		if sc.rb.SaturatedSince() == nil {
			t.Fatalf("expected slow client %s to have saturated_since timestamp set", sc.connID)
		}
	}

	// 4. Simulate time advance past the 5-second saturation threshold (t0 + 6 seconds).
	tPastTimeout := t0.Add(6 * time.Second)

	// Dispatch an additional event. The dispatch must proceed in non-blocking fashion without stalling on slow consumers.
	evtEvictID := "evt_post_timeout_trigger"
	frameEvict := []byte(fmt.Sprintf("id: %s\nevent: milestone\ndata: {\"trigger_evict\":true}\n\n", evtEvictID))
	eventEvict := entity.NotificationEvent{
		EventID:    evtEvictID,
		TenantID:   tenantID,
		EventType:  entity.EventTypeIngestionMilestone,
		Severity:   entity.SeverityInfo,
		IsTerminal: false,
	}

	startDispatch := time.Now()
	_ = connHub.DispatchSSEFrame(context.Background(), eventEvict, frameEvict)
	dispatchDuration := time.Since(startDispatch)

	t.Logf("dispatched post-timeout event in %v (non-blocking)", dispatchDuration)
	if dispatchDuration > 500*time.Millisecond {
		t.Errorf("broadcast dispatch loop took %v; expected non-blocking execution (< 500ms)", dispatchDuration)
	}

	// 5. Run ConnectionHub sweep to evict all stalled consumers at tPastTimeout.
	evictedIDs := connHub.SweepSlowConsumers(tPastTimeout)
	t.Logf("hub sweeper evicted %d slow consumers", len(evictedIDs))

	if len(evictedIDs) != numSlowConsumers {
		t.Fatalf("expected %d evicted slow consumers, got %d", numSlowConsumers, len(evictedIDs))
	}

	// Also verify that each slow consumer's ring buffer evaluates as evicted.
	for _, sc := range slowClients {
		evicted, reason := sc.rb.IsEvicted()
		if !evicted {
			t.Errorf("slow consumer %s was not marked as evicted", sc.connID)
		}
		if reason != hub.DropReasonSlowConsumerEvicted {
			t.Errorf("expected eviction reason %s, got %s", hub.DropReasonSlowConsumerEvicted, reason)
		}
	}

	// 6. Verify that healthy consumers received all events (256 saturation frames + 1 post-timeout frame = 257 frames).
	time.Sleep(100 * time.Millisecond)
	for _, hc := range healthyClients {
		rcv := hc.received.Load()
		if rcv < int64(hub.DefaultRingBufferSize+1) {
			t.Fatalf("healthy consumer %s received %d events, expected >= %d",
				hc.connID, rcv, hub.DefaultRingBufferSize+1)
		}
	}
	t.Logf("healthy consumers successfully processed all live frames with zero drops")

	// 7. Verify remaining active sessions in the hub: all 2,000 slow consumers are evicted,
	// only the 50 healthy consumers remain active and intact.
	_, _, remainingTotal := connHub.ListActiveSessions(tenantID, 500, "")
	t.Logf("remaining active sessions in hub: %d (healthy subscribers: %d)", remainingTotal, numHealthyConsumers)

	if remainingTotal != numHealthyConsumers {
		t.Fatalf("expected remaining active sessions to be exactly %d, got %d", numHealthyConsumers, remainingTotal)
	}

	// Clean up healthy readers.
	cancel()
	healthyWg.Wait()
}
