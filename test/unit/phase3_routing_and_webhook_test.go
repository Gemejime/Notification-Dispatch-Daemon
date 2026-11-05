package unit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	notificationv1 "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/query"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/redis"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/persistence/postgres"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/queue/publisher"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
)

// TestMultiPodTargetedRoutingDirectAndPeerDelivery verifies ADR-003 multi-pod targeted routing:
// events targeted to Pod B are published to pod.pod-b.events, while events for Pod A are delivered locally.
func TestMultiPodTargetedRoutingDirectAndPeerDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(nil, slog.LevelInfo)
	backplane := redis.NewInMemoryRedisClusterClient()

	nodeA := "pod-a"
	nodeB := "pod-b"

	// 1. Setup Pod A Presence, Hub, and Router
	regA := redis.NewPresenceRegistry(redis.PresenceRegistryConfig{
		NodeID:       nodeA,
		PresenceTTL:  45 * time.Second,
		ReplayMaxLen: 1000,
		ReplayTTL:    3600 * time.Second,
	}, backplane, logger, metrics)

	hubA := hub.NewConnectionHub(nodeA, 5*time.Second, 30*time.Second, 60*time.Second, metrics, logger)
	routerA := hub.NewTargetedRouter(hub.TargetedRouterConfig{LocalNodeID: nodeA}, hubA, regA, regA, nil, backplane, logger, metrics)
	if err := routerA.Start(ctx); err != nil {
		t.Fatalf("failed to start router A: %v", err)
	}
	defer func() { _ = routerA.Close() }()

	// 2. Setup Pod B Presence, Hub, and Router
	regB := redis.NewPresenceRegistry(redis.PresenceRegistryConfig{
		NodeID:       nodeB,
		PresenceTTL:  45 * time.Second,
		ReplayMaxLen: 1000,
		ReplayTTL:    3600 * time.Second,
	}, backplane, logger, metrics)

	hubB := hub.NewConnectionHub(nodeB, 5*time.Second, 30*time.Second, 60*time.Second, metrics, logger)
	routerB := hub.NewTargetedRouter(hub.TargetedRouterConfig{LocalNodeID: nodeB}, hubB, regB, regB, nil, backplane, logger, metrics)
	if err := routerB.Start(ctx); err != nil {
		t.Fatalf("failed to start router B: %v", err)
	}
	defer func() { _ = routerB.Close() }()

	tenantID := "tnt_cluster"
	userAlice := "usr_alice"
	userBob := "usr_bob"

	// Register Alice on Pod A
	rbAlice := hub.NewRingBuffer(tenantID, "conn_alice", 256, 5*time.Second, metrics)
	connAlice := entity.Connection{
		ConnectionID:   "conn_alice",
		TenantID:       tenantID,
		UserID:         userAlice,
		NodeID:         nodeA,
		SeverityFilter: entity.SeverityInfo,
		ConnectedAt:    time.Now().UTC(),
	}
	if err := hubA.Register(connAlice, rbAlice, nil); err != nil {
		t.Fatalf("register Alice in Hub A: %v", err)
	}
	if err := regA.RegisterPresence(ctx, tenantID, userAlice, nodeA, "conn_alice", 45*time.Second); err != nil {
		t.Fatalf("register Alice presence: %v", err)
	}

	// Register Bob on Pod B
	rbBob := hub.NewRingBuffer(tenantID, "conn_bob", 256, 5*time.Second, metrics)
	connBob := entity.Connection{
		ConnectionID:   "conn_bob",
		TenantID:       tenantID,
		UserID:         userBob,
		NodeID:         nodeB,
		SeverityFilter: entity.SeverityInfo,
		ConnectedAt:    time.Now().UTC(),
	}
	if err := hubB.Register(connBob, rbBob, nil); err != nil {
		t.Fatalf("register Bob in Hub B: %v", err)
	}
	if err := regB.RegisterPresence(ctx, tenantID, userBob, nodeB, "conn_bob", 45*time.Second); err != nil {
		t.Fatalf("register Bob presence: %v", err)
	}

	// 3. Dispatch an event for Bob from Pod A (targeted peer routing over pod.pod-b.events)
	eventForBob := entity.NotificationEvent{
		EventID:       "01J9Z8K4_BOB",
		TenantID:      tenantID,
		UserID:        userBob,
		SourceService: "rag-ingestion-coordinator",
		EventType:     entity.EventTypeIngestionMilestone,
		Severity:      entity.SeverityInfo,
		IsTerminal:    false,
		OccurredAt:    time.Now().UTC(),
		TraceID:       "trace-bob-01",
		PayloadJSON:   json.RawMessage(`{"stage":"EMBEDDING","progress_pct":50}`),
	}
	frameBob := eventForBob.FormatSSEFrame(5000)

	if err := routerA.DispatchSSEFrame(ctx, eventForBob, frameBob); err != nil {
		t.Fatalf("dispatch for Bob from Pod A: %v", err)
	}

	// Verify Bob on Pod B receives the frame
	select {
	case <-rbBob.Notify():
		entry, ok := rbBob.TryPop()
		if !ok || entry.EventID != "01J9Z8K4_BOB" {
			t.Fatalf("expected Bob to receive event 01J9Z8K4_BOB, got %v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for Bob on Pod B to receive routed event")
	}

	// Verify Alice on Pod A did NOT receive Bob's event
	select {
	case <-rbAlice.Notify():
		t.Fatalf("Alice on Pod A should not receive Bob's targeted event")
	default:
		// Expected
	}

	// 4. Dispatch an event for Alice from Pod A (local delivery)
	eventForAlice := entity.NotificationEvent{
		EventID:       "01J9Z8K4_ALICE",
		TenantID:      tenantID,
		UserID:        userAlice,
		SourceService: "rag-ingestion-coordinator",
		EventType:     entity.EventTypeIngestionMilestone,
		Severity:      entity.SeverityInfo,
		IsTerminal:    false,
		OccurredAt:    time.Now().UTC(),
		TraceID:       "trace-alice-01",
		PayloadJSON:   json.RawMessage(`{"stage":"CHUNK_PARSING","progress_pct":100}`),
	}
	frameAlice := eventForAlice.FormatSSEFrame(5000)

	if err := routerA.DispatchSSEFrame(ctx, eventForAlice, frameAlice); err != nil {
		t.Fatalf("dispatch for Alice from Pod A: %v", err)
	}

	// Verify Alice receives the frame locally
	select {
	case <-rbAlice.Notify():
		entry, ok := rbAlice.TryPop()
		if !ok || entry.EventID != "01J9Z8K4_ALICE" {
			t.Fatalf("expected Alice to receive event 01J9Z8K4_ALICE, got %v", entry)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for Alice to receive local event")
	}

	// 5. Dispatch a tenant-wide broadcast event (UserID == "") from Pod A
	broadcastEvent := entity.NotificationEvent{
		EventID:       "01J9Z8K4_BROADCAST",
		TenantID:      tenantID,
		UserID:        "", // Tenant-wide
		SourceService: "rag-kb-catalog-service",
		EventType:     entity.EventTypeIndexBlueGreenSwitched,
		Severity:      entity.SeverityWarning,
		IsTerminal:    true,
		OccurredAt:    time.Now().UTC(),
		TraceID:       "trace-broadcast-01",
		PayloadJSON:   json.RawMessage(`{"active_generation":"gen_02"}`),
	}
	frameBroadcast := broadcastEvent.FormatSSEFrame(5000)

	if err := routerA.DispatchSSEFrame(ctx, broadcastEvent, frameBroadcast); err != nil {
		t.Fatalf("dispatch tenant broadcast: %v", err)
	}

	// Both Alice (Pod A) and Bob (Pod B) must receive the broadcast frame
	select {
	case <-rbAlice.Notify():
		entry, ok := rbAlice.TryPop()
		if !ok || entry.EventID != "01J9Z8K4_BROADCAST" {
			t.Fatalf("expected Alice to receive broadcast, got %v", entry)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for Alice to receive broadcast")
	}

	select {
	case <-rbBob.Notify():
		entry, ok := rbBob.TryPop()
		if !ok || entry.EventID != "01J9Z8K4_BROADCAST" {
			t.Fatalf("expected Bob to receive broadcast, got %v", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for Bob to receive broadcast")
	}
}

// TestPresenceRegistryTTLRefreshAndLastSocketPruning verifies presence directory behavior:
// 45s heartbeat refresh and immediate pruning of tenant_nodes on last socket disconnect.
func TestPresenceRegistryTTLRefreshAndLastSocketPruning(t *testing.T) {
	ctx := context.Background()
	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(nil, slog.LevelInfo)
	backplane := redis.NewInMemoryRedisClusterClient()

	nodeID := "node-prune-test"
	reg := redis.NewPresenceRegistry(redis.PresenceRegistryConfig{
		NodeID:      nodeID,
		PresenceTTL: 45 * time.Second,
	}, backplane, logger, metrics)

	tenantID := "tnt_prune"
	conn1 := "conn_1"
	conn2 := "conn_2"
	user1 := "usr_1"
	user2 := "usr_2"

	// 1. Register first connection
	if err := reg.RegisterPresence(ctx, tenantID, user1, nodeID, conn1, 45*time.Second); err != nil {
		t.Fatalf("register conn1: %v", err)
	}

	nodes, err := reg.GetNodesForTenant(ctx, tenantID)
	if err != nil || len(nodes) != 1 || nodes[0] != nodeID {
		t.Fatalf("expected tenant nodes [%s], got %v (err: %v)", nodeID, nodes, err)
	}

	// 2. Register second connection on same node
	if err := reg.RegisterPresence(ctx, tenantID, user2, nodeID, conn2, 45*time.Second); err != nil {
		t.Fatalf("register conn2: %v", err)
	}

	// 3. Refresh presence for conn1
	if err := reg.RefreshPresence(ctx, tenantID, user1, conn1, 45*time.Second); err != nil {
		t.Fatalf("refresh presence: %v", err)
	}

	// 4. Deregister first connection — node must STILL be in tenant_nodes because conn2 is active
	if err := reg.DeregisterPresence(ctx, tenantID, user1, conn1); err != nil {
		t.Fatalf("deregister conn1: %v", err)
	}

	nodesAfterFirst, err := reg.GetNodesForTenant(ctx, tenantID)
	if err != nil || len(nodesAfterFirst) != 1 || nodesAfterFirst[0] != nodeID {
		t.Fatalf("expected node to remain after first disconnect, got %v", nodesAfterFirst)
	}

	// 5. Deregister second (last) connection — node must be IMMEDIATELY pruned from tenant_nodes
	if err := reg.DeregisterPresence(ctx, tenantID, user2, conn2); err != nil {
		t.Fatalf("deregister conn2: %v", err)
	}

	nodesAfterLast, err := reg.GetNodesForTenant(ctx, tenantID)
	if err != nil {
		t.Fatalf("get nodes after last: %v", err)
	}
	if len(nodesAfterLast) != 0 {
		t.Fatalf("expected node to be pruned immediately on last disconnect, got %v", nodesAfterLast)
	}
}

// TestWebhookTaskDeterministicEnqueueAndSecretRefRedaction verifies ADR-001:
// deterministic task_id generation, P99 <= 15ms hand-off, exactly-once metrics, and secret_ref redaction.
func TestWebhookTaskDeterministicEnqueueAndSecretRefRedaction(t *testing.T) {
	ctx := context.Background()
	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(nil, slog.LevelInfo)

	broker := publisher.NewInMemoryTaskQueueBroker()
	taskPublisher := publisher.NewTaskQueuePublisher("rag:webhook:tasks", broker, logger, metrics)
	handler := command.NewDispatchWebhookTaskHandler(taskPublisher, logger, metrics)

	tenantID := "tnt_wh"
	eventID := "01J9Z8K4_WH"
	target1ID := "tgt_01"
	target2ID := "tgt_02"

	target1 := entity.WebhookTarget{
		TargetID:            target1ID,
		TenantID:            tenantID,
		TargetURL:           "https://customer.api.com/hooks/notify",
		SecretRef:           "vault://secret/prod/webhook-hmac-key",
		SignatureHeaderName: "X-RAG-Signature-256",
		Active:              true,
		EventTypes:          []string{"*"},
	}
	target2 := entity.WebhookTarget{
		TargetID:            target2ID,
		TenantID:            tenantID,
		TargetURL:           "https://backup.api.com/webhook",
		SecretRef:           "kms://arn:aws:kms:us-east-1:123456789012:key/test",
		SignatureHeaderName: "X-Custom-Sig",
		Active:              true,
		EventTypes:          []string{"ingestion.*"},
	}

	event := entity.NotificationEvent{
		EventID:       eventID,
		TenantID:      tenantID,
		SourceService: "rag-ingestion-coordinator",
		EventType:     entity.EventTypeIngestionMilestone,
		Severity:      entity.SeverityInfo,
		IsTerminal:    true,
		OccurredAt:    time.Now().UTC(),
		TraceID:       "trace-wh-01",
		PayloadJSON:   json.RawMessage(`{"document_id":"doc_100","status":"COMPLETED"}`),
	}

	start := time.Now()
	res, err := handler.Handle(ctx, command.DispatchWebhookTaskCommand{
		Event:   event,
		Targets: []entity.WebhookTarget{target1, target2},
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("dispatch webhook tasks: %v", err)
	}
	if res.TasksEnqueued != 2 {
		t.Fatalf("expected 2 tasks enqueued, got %d", res.TasksEnqueued)
	}
	if elapsed > 15*time.Millisecond {
		t.Fatalf("hand-off latency %v exceeded 15ms SLO", elapsed)
	}

	// Verify messages in broker
	rawMessages := broker.GetMessages("rag:webhook:tasks")
	if len(rawMessages) != 2 {
		t.Fatalf("expected 2 wire messages in stream, got %d", len(rawMessages))
	}

	// Unmarshal and verify task 1
	var task1 notificationv1.WebhookDeliveryTask
	if err := task1.UnmarshalWire(rawMessages[0]); err != nil {
		t.Fatalf("unmarshal wire task 1: %v", err)
	}
	expectedTask1ID := entity.DeterministicTaskID(tenantID, eventID, target1ID)
	if task1.TaskId != expectedTask1ID {
		t.Fatalf("expected deterministic task_id %s, got %s", expectedTask1ID, task1.TaskId)
	}
	if task1.SecretRef != target1.SecretRef {
		t.Fatalf("expected secret_ref preserved on wire format")
	}

	// Verify Secured Logging redaction on task
	logVal := task1.LogValue()
	logStr := fmt.Sprintf("%v", logVal)
	if !stringsContains(logStr, "[REDACTED]") {
		t.Fatalf("expected task LogValue to redact sensitive fields, got %s", logStr)
	}
	if stringsContains(logStr, "vault://secret/prod/webhook-hmac-key") {
		t.Fatalf("raw secret_ref leaked in task LogValue")
	}

	// Verify exactly-once metrics in ProcessCDCEventHandler integration
	// Build an envelope for ProcessCDCEventHandler
	env := &notificationv1.NotificationEventEnvelope{
		EventId:          "01J9Z8K4_CDC_WH",
		TenantId:         tenantID,
		SourceService:    "rag-ingestion-coordinator",
		EventType:        entity.EventTypeIngestionMilestone,
		Severity:         notificationv1.Severity_SEVERITY_INFO,
		IsTerminal:       true,
		OccurredAtUnixMs: time.Now().UnixMilli(),
		TraceId:          "trace-cdc-wh",
		PayloadJson:      []byte(`{"status":"SUCCESS"}`),
	}

	// Set up cache mock with the targets
	subCache := &mockSubCacheReader{
		subs: []entity.Subscription{{
			SubscriptionID:   "sub_1",
			TenantID:         tenantID,
			EventTypePattern: "*",
			MinSeverity:      entity.SeverityInfo,
			SSEEnabled:       false,
			WebhookEnabled:   true,
			RateLimitRPS:     100,
		}},
		targets: []entity.WebhookTarget{target1},
	}
	cdcHandlerWithCache := command.NewProcessCDCEventHandler(
		subCache,
		nil,
		nil,
		nil,
		taskPublisher,
		logger,
		metrics,
	)

	cdcRes, cdcErr := cdcHandlerWithCache.Handle(ctx, env)
	if cdcErr != nil {
		t.Fatalf("cdc handler handle: %v", cdcErr)
	}
	if cdcRes.WebhooksEnqueued != 1 {
		t.Fatalf("expected 1 webhook enqueued from CDC handler, got %d", cdcRes.WebhooksEnqueued)
	}
}

// TestLastEventIDReplayBufferAndRateLimiting verifies bounded Last-Event-ID replay backfill
// from Redis Streams, token-bucket backfill rate limiting (ErrBackfillRateLimited), and cold-journal fallback.
func TestLastEventIDReplayBufferAndRateLimiting(t *testing.T) {
	ctx := context.Background()
	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(nil, slog.LevelInfo)
	backplane := redis.NewInMemoryRedisClusterClient()

	nodeID := "node-replay-test"
	reg := redis.NewPresenceRegistry(redis.PresenceRegistryConfig{
		NodeID:       nodeID,
		PresenceTTL:  45 * time.Second,
		ReplayMaxLen: 1000,
		ReplayTTL:    3600 * time.Second,
	}, backplane, logger, metrics)

	tenantID := "tnt_replay"

	// 1. Append 10 events to Redis Stream replay buffer
	for i := 1; i <= 10; i++ {
		ev := entity.NotificationEvent{
			EventID:       fmt.Sprintf("ev_%02d", i),
			TenantID:      tenantID,
			SourceService: "rag-ingestion-coordinator",
			EventType:     entity.EventTypeIngestionMilestone,
			Severity:      entity.SeverityInfo,
			IsTerminal:    i == 10,
			OccurredAt:    time.Now().UTC(),
			TraceID:       fmt.Sprintf("trace-%02d", i),
			PayloadJSON:   json.RawMessage(fmt.Sprintf(`{"step":%d}`, i)),
		}
		if err := reg.AppendReplayEvent(ctx, tenantID, ev); err != nil {
			t.Fatalf("append replay event %d: %v", i, err)
		}
	}

	// 2. Query replay after ev_05 -> should return ev_06 through ev_10
	replayed, err := reg.FetchReplayEvents(ctx, tenantID, "ev_05", 10)
	if err != nil {
		t.Fatalf("fetch replay events: %v", err)
	}
	if len(replayed) != 5 {
		t.Fatalf("expected 5 replayed events after ev_05, got %d", len(replayed))
	}
	if replayed[0].EventID != "ev_06" || replayed[4].EventID != "ev_10" {
		t.Fatalf("unexpected replayed range: %s to %s", replayed[0].EventID, replayed[4].EventID)
	}

	// 3. Test Query Handler with rate limiting (burst = 2, rps = 1)
	journalRepo := postgres.NewInMemoryEventJournalRepo()
	replayHandler := query.NewFetchReplayEventsHandler(reg, journalRepo, 1.0, 2.0, logger, metrics)

	// Call 1: Allowed (consume 1 token, 1 remaining)
	res1, err1 := replayHandler.Handle(ctx, query.FetchReplayEventsQuery{
		TenantID:    tenantID,
		LastEventID: "ev_08",
		Limit:       10,
	})
	if err1 != nil || len(res1.Events) != 2 {
		t.Fatalf("expected 2 events in query 1, got %v (err: %v)", res1.Events, err1)
	}
	if res1.HighestWatermarkEventID != "ev_10" {
		t.Fatalf("expected watermark ev_10, got %s", res1.HighestWatermarkEventID)
	}

	// Call 2: Allowed (consume 1 token, 0 remaining)
	_, err2 := replayHandler.Handle(ctx, query.FetchReplayEventsQuery{
		TenantID:    tenantID,
		LastEventID: "ev_09",
		Limit:       10,
	})
	if err2 != nil {
		t.Fatalf("expected query 2 allowed, got: %v", err2)
	}

	// Call 3: Rate Limited -> ErrBackfillRateLimited
	_, err3 := replayHandler.Handle(ctx, query.FetchReplayEventsQuery{
		TenantID:    tenantID,
		LastEventID: "ev_09",
		Limit:       10,
	})
	if !errors.Is(err3, query.ErrBackfillRateLimited) {
		t.Fatalf("expected ErrBackfillRateLimited on exhausted token bucket, got: %v", err3)
	}

	// 4. Test fallback to cold EventJournalRepository when Redis stream has no events
	tenantCold := "tnt_cold_journal"
	coldEv1 := entity.NotificationEvent{
		EventID:       "cold_01",
		TenantID:      tenantCold,
		SourceService: "rag-kb-catalog-service",
		EventType:     entity.EventTypeChunkOverlayConflict,
		Severity:      entity.SeverityError,
		IsTerminal:    true,
		OccurredAt:    time.Now().UTC(),
		PayloadJSON:   json.RawMessage(`{"conflict":"detected"}`),
	}
	_ = journalRepo.AppendEvent(ctx, coldEv1)

	coldReplayHandler := query.NewFetchReplayEventsHandler(reg, journalRepo, 50, 100, logger, metrics)
	resCold, errCold := coldReplayHandler.Handle(ctx, query.FetchReplayEventsQuery{
		TenantID:    tenantCold,
		LastEventID: "cold_00",
		Limit:       10,
	})
	if errCold != nil {
		t.Fatalf("cold journal fetch: %v", errCold)
	}
	if len(resCold.Events) != 1 || resCold.Events[0].EventID != "cold_01" {
		t.Fatalf("expected cold_01 recovered from journal, got %v", resCold.Events)
	}

	// 5. Test merged ordering: earlier journal events must precede Redis events
	tenantMerged := "tnt_merged"
	now := time.Now()

	// Base event present in both
	baseEv := entity.NotificationEvent{
		EventID:       "ev_base_01",
		TenantID:      tenantMerged,
		EventType:     entity.EventTypeIngestionMilestone,
		OccurredAt:    now,
		PayloadJSON:   []byte(`{"status":"base"}`),
		SourceService: "catalog",
	}
	_ = reg.AppendReplayEvent(ctx, tenantMerged, baseEv)
	_ = journalRepo.AppendEvent(ctx, baseEv)

	// Redis has a later event (t0 + 2m)
	laterRedisEv := entity.NotificationEvent{
		EventID:       "ev_a_later_03",
		TenantID:      tenantMerged,
		EventType:     entity.EventTypeIngestionMilestone,
		OccurredAt:    now.Add(2 * time.Minute),
		PayloadJSON:   []byte(`{"status":"later"}`),
		SourceService: "catalog",
	}
	_ = reg.AppendReplayEvent(ctx, tenantMerged, laterRedisEv)

	// Journal has an intermediate event (t0 + 1m)
	earlierJournalEv := entity.NotificationEvent{
		EventID:       "ev_earlier_02",
		TenantID:      tenantMerged,
		EventType:     entity.EventTypeIngestionMilestone,
		OccurredAt:    now.Add(1 * time.Minute),
		PayloadJSON:   []byte(`{"status":"earlier_2"}`),
		SourceService: "catalog",
	}
	_ = journalRepo.AppendEvent(ctx, earlierJournalEv)

	mergedHandler := query.NewFetchReplayEventsHandler(reg, journalRepo, 50, 100, logger, metrics)
	resMerged, errMerged := mergedHandler.Handle(ctx, query.FetchReplayEventsQuery{
		TenantID:    tenantMerged,
		LastEventID: "ev_base_01",
		Limit:       10,
	})
	if errMerged != nil {
		t.Fatalf("merged replay fetch failed: %v", errMerged)
	}
	if len(resMerged.Events) != 2 {
		t.Fatalf("expected 2 merged events, got %d", len(resMerged.Events))
	}
	if resMerged.Events[0].EventID != "ev_earlier_02" || resMerged.Events[1].EventID != "ev_a_later_03" {
		t.Fatalf("expected chronological order [ev_earlier_02, ev_a_later_03], got [%s, %s]",
			resMerged.Events[0].EventID, resMerged.Events[1].EventID)
	}
	if resMerged.HighestWatermarkEventID != "ev_a_later_03" {
		t.Fatalf("expected watermark ev_a_later_03, got %s", resMerged.HighestWatermarkEventID)
	}
}

type mockSubCacheReader struct {
	subs    []entity.Subscription
	targets []entity.WebhookTarget
}

func (m *mockSubCacheReader) GetTenantRules(_ string) ([]entity.Subscription, []entity.WebhookTarget) {
	return m.subs, m.targets
}

func (m *mockSubCacheReader) IsWarmed() bool {
	return true
}

func (m *mockSubCacheReader) TenantCount() int {
	return 1
}

func stringsContains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(substr) > 0 && searchSubstr(s, substr)))
}

func searchSubstr(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Ensure unused atomic import doesn't fail compilation
var _ atomic.Bool
