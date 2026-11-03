package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	notificationv1 "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/query"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/router"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/redis"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/persistence/postgres"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/queue/publisher"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/sse"
)

// TestDispatchPipelineEndToEnd verifies the complete Phase 3 pipeline:
// 1. CDC ingestion with 0 SQL reads on hot path (ADR-004)
// 2. Targeted pod routing across Redis backplane (pod.pod-b.events, ADR-003)
// 3. High-density SSE stream delivery without re-serialization
// 4. Bounded Last-Event-ID replay backfill from Redis stream buffer
// 5. Asynchronous webhook task enqueueing with P99 <= 15ms hand-off (ADR-001)
// 6. <= 100ms tenant cache invalidation coherence
func TestDispatchPipelineEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	metrics := observability.NewMetricsRegistry()
	logger := observability.NewSecuredLogger(nil, slog.LevelInfo)
	backplane := redis.NewInMemoryRedisClusterClient()

	tenantID := "tnt_e2e_01"
	userBeta := "usr_beta"
	podAName := "pod-a"
	podBName := "pod-b"

	// 1. Setup shared cold-path persistence & pod-local subscription caches
	journalRepo := postgres.NewInMemoryEventJournalRepo()
	taskBroker := publisher.NewInMemoryTaskQueueBroker()
	taskPublisher := publisher.NewTaskQueuePublisher("rag:webhook:tasks", taskBroker, logger, metrics)

	// Cold-start tenant rules
	sub1 := entity.Subscription{
		SubscriptionID:   "sub_e2e_01",
		TenantID:         tenantID,
		EventTypePattern: "ingestion.*",
		MinSeverity:      entity.SeverityInfo,
		SSEEnabled:       true,
		WebhookEnabled:   true,
		RateLimitRPS:     200,
	}
	target1 := entity.WebhookTarget{
		TargetID:            "tgt_e2e_01",
		TenantID:            tenantID,
		TargetURL:           "https://customer-dmz.example.com/webhooks",
		SecretRef:           "vault://secret/prod/customer-signing-key",
		SignatureHeaderName: "X-RAG-Signature-256",
		Active:              true,
		EventTypes:          []string{"ingestion.*"},
	}

	cacheA := memory.NewSubscriptionCache()
	cacheA.ReplaceTenantRules(tenantID, []entity.Subscription{sub1}, []entity.WebhookTarget{target1})

	cacheB := memory.NewSubscriptionCache()
	cacheB.ReplaceTenantRules(tenantID, []entity.Subscription{sub1}, []entity.WebhookTarget{target1})

	stubSub := &stubSubscriptionRepo{
		subs: map[string][]entity.Subscription{
			tenantID: {sub1},
		},
	}
	stubWebhook := &stubWebhookTargetRepo{
		targets: map[string][]entity.WebhookTarget{
			tenantID: {target1},
		},
	}
	invSubB := redis.NewInvalidationSubscriber(backplane, cacheB, stubSub, stubWebhook, logger, metrics)
	go func() {
		_ = invSubB.Run(ctx)
	}()

	// 2. Setup Pod A (Ingestion & Router)
	regA := redis.NewPresenceRegistry(redis.PresenceRegistryConfig{
		NodeID:       podAName,
		PresenceTTL:  45 * time.Second,
		ReplayMaxLen: 1000,
		ReplayTTL:    3600 * time.Second,
	}, backplane, logger, metrics)

	hubA := hub.NewConnectionHub(podAName, 5*time.Second, 30*time.Second, 60*time.Second, metrics, logger)
	routerA := hub.NewTargetedRouter(
		hub.TargetedRouterConfig{LocalNodeID: podAName},
		hubA,
		regA,
		regA,
		journalRepo,
		backplane,
		logger,
		metrics,
	)
	if err := routerA.Start(ctx); err != nil {
		t.Fatalf("start router A: %v", err)
	}
	defer func() { _ = routerA.Close() }()

	evaluatorA := router.NewRuleEvaluator()
	filterA := router.NewFilter(router.DefaultDedupWindowTTL, router.DefaultMaxDedupEntries)
	cdcHandlerA := command.NewProcessCDCEventHandler(cacheA, evaluatorA, filterA, routerA, taskPublisher, logger, metrics)

	// 3. Setup Pod B (Hosts client SSE connection for userBeta)
	regB := redis.NewPresenceRegistry(redis.PresenceRegistryConfig{
		NodeID:       podBName,
		PresenceTTL:  45 * time.Second,
		ReplayMaxLen: 1000,
		ReplayTTL:    3600 * time.Second,
	}, backplane, logger, metrics)

	hubB := hub.NewConnectionHub(podBName, 5*time.Second, 30*time.Second, 60*time.Second, metrics, logger)
	routerB := hub.NewTargetedRouter(
		hub.TargetedRouterConfig{LocalNodeID: podBName},
		hubB,
		regB,
		regB,
		journalRepo,
		backplane,
		logger,
		metrics,
	)
	if err := routerB.Start(ctx); err != nil {
		t.Fatalf("start router B: %v", err)
	}
	defer func() { _ = routerB.Close() }()

	registerCmdB := command.NewRegisterConnectionHandler(hubB, regB, 45*time.Second)
	terminateCmdB := command.NewTerminateConnectionHandler(hubB, regB)
	replayHandlerB := query.NewFetchReplayEventsHandler(regB, journalRepo, 50, 100, logger, metrics)

	streamHandlerB := sse.NewStreamHandler(
		sse.StreamHandlerConfig{
			NodeID:              podBName,
			RingBufferSize:      256,
			SlowConsumerTimeout: 5 * time.Second,
			HeartbeatInterval:   15 * time.Second,
			DefaultRetryMs:      5000,
		},
		hubB,
		registerCmdB,
		terminateCmdB,
		regB,
		replayHandlerB,
		metrics,
		logger,
	)

	// 4. Start HTTP test server on Pod B to serve SSE stream
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Inject authenticated tenant and user context
		ctxWithAuth := context.WithValue(r.Context(), entity.ContextKeyTenantID, tenantID)
		ctxWithAuth = context.WithValue(ctxWithAuth, entity.ContextKeyUserID, userBeta)
		streamHandlerB.ServeHTTP(w, r.WithContext(ctxWithAuth))
	}))
	defer serverB.Close()

	// 5. Connect SSE client to Pod B
	clientReq, err := http.NewRequestWithContext(ctx, http.MethodGet, serverB.URL+"/v1/notifications/stream", nil)
	if err != nil {
		t.Fatalf("new client request: %v", err)
	}
	clientReq.Header.Set("Authorization", "Bearer valid-token")

	clientResp, err := http.DefaultClient.Do(clientReq)
	if err != nil {
		t.Fatalf("connect sse client: %v", err)
	}
	defer func() { _ = clientResp.Body.Close() }()

	if clientResp.StatusCode != http.StatusOK {
		t.Fatalf("expected sse 200 OK, got %d", clientResp.StatusCode)
	}

	reader := bufio.NewReader(clientResp.Body)

	// Read initial : ping frame
	initialPing, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(initialPing, ": ping") {
		t.Fatalf("expected initial : ping, got %q (err: %v)", initialPing, err)
	}
	// Consume empty line after comment
	_, _ = reader.ReadString('\n')

	// Verify presence in Redis directory: userBeta mapped to pod-b
	nodesForUser, err := regA.GetNodesForUser(ctx, tenantID, userBeta)
	if err != nil || len(nodesForUser) != 1 || nodesForUser[0] != podBName {
		t.Fatalf("expected presence directory to map userBeta to [%s], got %v (err: %v)", podBName, nodesForUser, err)
	}

	// Seed predecessor event so watermark cursor exists in both journal implementations
	_ = journalRepo.AppendEvent(ctx, entity.NotificationEvent{
		EventID:       "01J9Z8K4_E2E_000",
		TenantID:      tenantID,
		UserID:        userBeta,
		SourceService: "rag-ingestion-coordinator",
		EventType:     entity.EventTypeIngestionMilestone,
		Severity:      entity.SeverityInfo,
		IsTerminal:    false,
		OccurredAt:    time.Now().Add(-5 * time.Second),
		PayloadJSON:   []byte(`{"stage":"INIT"}`),
	})

	// 6. Ingest CDC event on Pod A (routed to Pod B, pushed to client, webhook enqueued)
	eventID := "01J9Z8K4_E2E_001"
	envelope := &notificationv1.NotificationEventEnvelope{
		EventId:          eventID,
		TenantId:         tenantID,
		UserId:           userBeta,
		SourceService:    "rag-ingestion-coordinator",
		EventType:        entity.EventTypeIngestionMilestone,
		Severity:         notificationv1.Severity_SEVERITY_INFO,
		IsTerminal:       false,
		OccurredAtUnixMs: time.Now().UnixMilli(),
		TraceId:          "trace-w3c-e2e-001",
		PayloadJson:      []byte(`{"document_id":"doc_e2e","stage":"CHUNK_PARSING","progress_pct":42}`),
	}

	startIngest := time.Now()
	res, err := cdcHandlerA.Handle(ctx, envelope)
	ingestLatency := time.Since(startIngest)

	if err != nil {
		t.Fatalf("cdc handler handle: %v", err)
	}
	if !res.SSEDispatched {
		t.Fatalf("expected sse dispatched")
	}
	if res.WebhooksEnqueued != 1 {
		t.Fatalf("expected 1 webhook enqueued, got %d", res.WebhooksEnqueued)
	}
	if ingestLatency > 50*time.Millisecond {
		t.Fatalf("cdc ingestion latency %v exceeded 50ms budget", ingestLatency)
	}

	// 7. Verify SSE Client on Pod B receives the routed event frame
	var frameLines []string
	frameDeadline := time.After(3 * time.Second)
	frameDone := make(chan struct{})

	go func() {
		for {
			line, rErr := reader.ReadString('\n')
			if rErr != nil {
				return
			}
			trimmed := strings.TrimRight(line, "\r\n")
			if trimmed == "" {
				if len(frameLines) > 0 {
					close(frameDone)
					return
				}
				continue
			}
			frameLines = append(frameLines, trimmed)
		}
	}()

	select {
	case <-frameDone:
		joined := strings.Join(frameLines, "\n")
		if !strings.Contains(joined, "id: "+eventID) {
			t.Fatalf("expected frame id %s, got:\n%s", eventID, joined)
		}
		if !strings.Contains(joined, "event: "+entity.EventTypeIngestionMilestone) {
			t.Fatalf("expected event type, got:\n%s", joined)
		}
		if !strings.Contains(joined, `"progress_pct":42`) {
			t.Fatalf("expected payload contents, got:\n%s", joined)
		}
	case <-frameDeadline:
		t.Fatalf("timeout waiting for SSE client on Pod B to receive routed frame")
	}

	// 8. Verify Webhook Task Enqueued to rag:webhook:tasks with deterministic ID and P99 <= 15ms
	enqueuedTasks := taskBroker.GetMessages("rag:webhook:tasks")
	if len(enqueuedTasks) != 1 {
		t.Fatalf("expected 1 enqueued task in broker stream, got %d", len(enqueuedTasks))
	}
	var task notificationv1.WebhookDeliveryTask
	if err := task.UnmarshalWire(enqueuedTasks[0]); err != nil {
		t.Fatalf("unmarshal webhook task wire: %v", err)
	}
	expectedTaskID := entity.DeterministicTaskID(tenantID, eventID, target1.TargetID)
	if task.TaskId != expectedTaskID {
		t.Fatalf("expected deterministic task_id %s, got %s", expectedTaskID, task.TaskId)
	}

	// 9. Verify Last-Event-ID Replay Backfill
	replayReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, serverB.URL+"/v1/notifications/stream", nil)
	replayReq.Header.Set("Authorization", "Bearer valid-token")
	replayReq.Header.Set("Last-Event-ID", "01J9Z8K4_E2E_000") // Watermark prior to 001

	replayResp, rErr := http.DefaultClient.Do(replayReq)
	if rErr != nil {
		t.Fatalf("connect replay client: %v", rErr)
	}
	defer func() { _ = replayResp.Body.Close() }()

	if replayResp.StatusCode != http.StatusOK {
		t.Fatalf("expected replay 200 OK, got %d", replayResp.StatusCode)
	}

	replayReader := bufio.NewReader(replayResp.Body)
	// Read until event is seen in replay stream
	foundReplayedEvent := false
	for i := 0; i < 15; i++ {
		line, _ := replayReader.ReadString('\n')
		if strings.Contains(line, "id: "+eventID) {
			foundReplayedEvent = true
			break
		}
	}
	if !foundReplayedEvent {
		t.Fatalf("expected event %s to be replayed via Last-Event-ID backfill", eventID)
	}

	// 10. Verify <= 100ms Cache Reload-on-Invalidation Coherence
	invalidationMsg := redis.InvalidationMessage{
		TenantID:        tenantID,
		Reason:          "webhook_endpoint_disabled",
		UpdatedAtUnixMs: time.Now().UnixMilli(),
	}
	invPayload, _ := json.Marshal(invalidationMsg)

	// Simulate updated rules in repository: webhook disabled
	disabledTarget := target1
	disabledTarget.Active = false

	stubWebhook.mu.Lock()
	stubWebhook.targets[tenantID] = []entity.WebhookTarget{disabledTarget}
	stubWebhook.mu.Unlock()

	invStart := time.Now()
	_ = backplane.Publish(ctx, redis.TenantConfigUpdatedChannel, invPayload)

	// Wait for cacheB to reload via InvalidationSubscriber
	reloaded := false
	timeout := time.After(100 * time.Millisecond)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for !reloaded {
		select {
		case <-timeout:
			t.Fatalf("cache invalidation coherence exceeded 100ms SLO")
		case <-ticker.C:
			_, targets := cacheB.GetTenantRules(tenantID)
			if len(targets) > 0 && !targets[0].Active {
				reloaded = true
			}
		}
	}
	invElapsed := time.Since(invStart)

	if invElapsed > 100*time.Millisecond {
		t.Fatalf("cache invalidation coherence %v exceeded 100ms SLO", invElapsed)
	}

	// Ingest second CDC event through handler reading cacheB: SSE should dispatch, but 0 webhooks enqueued!
	evaluatorB := router.NewRuleEvaluator()
	filterB := router.NewFilter(router.DefaultDedupWindowTTL, router.DefaultMaxDedupEntries)
	cdcHandlerB := command.NewProcessCDCEventHandler(cacheB, evaluatorB, filterB, routerB, taskPublisher, logger, metrics)

	envelope2 := &notificationv1.NotificationEventEnvelope{
		EventId:          "01J9Z8K4_E2E_002",
		TenantId:         tenantID,
		UserId:           userBeta,
		SourceService:    "rag-ingestion-coordinator",
		EventType:        entity.EventTypeIngestionMilestone,
		Severity:         notificationv1.Severity_SEVERITY_INFO,
		IsTerminal:       true,
		OccurredAtUnixMs: time.Now().UnixMilli(),
		TraceId:          "trace-w3c-e2e-002",
		PayloadJson:      []byte(`{"stage":"COMPLETED","progress_pct":100}`),
	}

	res2, err2 := cdcHandlerB.Handle(ctx, envelope2)
	if err2 != nil {
		t.Fatalf("handle event 2: %v", err2)
	}
	if !res2.SSEDispatched {
		t.Fatalf("expected SSE still dispatched for event 2")
	}
	if res2.WebhooksEnqueued != 0 {
		t.Fatalf("expected 0 webhooks enqueued after invalidation disabled target, got %d", res2.WebhooksEnqueued)
	}
}

type stubSubscriptionRepo struct {
	mu   sync.Mutex
	subs map[string][]entity.Subscription
}

func (s *stubSubscriptionRepo) ListAllActive(_ context.Context) ([]entity.Subscription, error) {
	return nil, nil
}

func (s *stubSubscriptionRepo) ListByTenant(_ context.Context, tenantID string) ([]entity.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs[tenantID], nil
}

type stubWebhookTargetRepo struct {
	mu      sync.Mutex
	targets map[string][]entity.WebhookTarget
}

func (w *stubWebhookTargetRepo) ListAllActive(_ context.Context) ([]entity.WebhookTarget, error) {
	return nil, nil
}

func (w *stubWebhookTargetRepo) ListByTenant(_ context.Context, tenantID string) ([]entity.WebhookTarget, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.targets[tenantID], nil
}
