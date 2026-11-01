package unit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	notificationv1 "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/router"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/memory"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/cache/redis"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/persistence/postgres"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/queue/kafka"
)

// ============================================================================
// Test Doubles for Cold-Path SQL, Redis Pub/Sub, SSE Routing & Webhook Queue
// ============================================================================

type fakeSQLRows struct {
	rows [][]any
	idx  int
}

func (r *fakeSQLRows) Next() bool {
	if r.idx < len(r.rows) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeSQLRows) Scan(dest ...any) error {
	row := r.rows[r.idx-1]
	if len(dest) != len(row) {
		return fmt.Errorf("column count mismatch: got %d dest, have %d cols", len(dest), len(row))
	}
	for i, val := range row {
		switch d := dest[i].(type) {
		case *string:
			*d = val.(string)
		case *bool:
			*d = val.(bool)
		case *int:
			*d = val.(int)
		case *time.Time:
			*d = val.(time.Time)
		case *[]string:
			src := val.([]string)
			cp := make([]string, len(src))
			copy(cp, src)
			*d = cp
		case *[]byte:
			src := val.([]byte)
			cp := make([]byte, len(src))
			copy(cp, src)
			*d = cp
		default:
			return fmt.Errorf("unsupported scan type %T", dest[i])
		}
	}
	return nil
}

func (r *fakeSQLRows) Err() error   { return nil }
func (r *fakeSQLRows) Close() error { return nil }

type fakeColdStoreQuerier struct {
	mu      sync.RWMutex
	subs    map[string][]entity.Subscription
	targets map[string][]entity.WebhookTarget
}

func newFakeColdStoreQuerier() *fakeColdStoreQuerier {
	return &fakeColdStoreQuerier{
		subs:    make(map[string][]entity.Subscription),
		targets: make(map[string][]entity.WebhookTarget),
	}
}

func (q *fakeColdStoreQuerier) setTenantData(tenantID string, subs []entity.Subscription, targets []entity.WebhookTarget) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.subs[tenantID] = subs
	q.targets[tenantID] = targets
}

func (q *fakeColdStoreQuerier) Query(_ context.Context, sql string, args ...any) (postgres.Rows, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if strings.Contains(sql, "FROM tenant_notification_subscriptions") {
		var rows [][]any
		if len(args) == 1 {
			tenantID := args[0].(string)
			for _, s := range q.subs[tenantID] {
				rows = append(rows, []any{
					s.SubscriptionID, s.TenantID, s.EventTypePattern, string(s.MinSeverity),
					s.SSEEnabled, s.WebhookEnabled, s.Suppressed, s.RateLimitRPS, s.UpdatedAt,
				})
			}
		} else {
			for _, list := range q.subs {
				for _, s := range list {
					rows = append(rows, []any{
						s.SubscriptionID, s.TenantID, s.EventTypePattern, string(s.MinSeverity),
						s.SSEEnabled, s.WebhookEnabled, s.Suppressed, s.RateLimitRPS, s.UpdatedAt,
					})
				}
			}
		}
		return &fakeSQLRows{rows: rows}, nil
	}

	if strings.Contains(sql, "FROM tenant_webhook_targets") {
		var rows [][]any
		if len(args) == 1 {
			tenantID := args[0].(string)
			for _, t := range q.targets[tenantID] {
				rows = append(rows, []any{
					t.TargetID, t.TenantID, t.TargetURL, t.SecretRef, t.Active, t.EventTypes, t.UpdatedAt,
				})
			}
		} else {
			for _, list := range q.targets {
				for _, t := range list {
					rows = append(rows, []any{
						t.TargetID, t.TenantID, t.TargetURL, t.SecretRef, t.Active, t.EventTypes, t.UpdatedAt,
					})
				}
			}
		}
		return &fakeSQLRows{rows: rows}, nil
	}

	return &fakeSQLRows{}, nil
}

func (q *fakeColdStoreQuerier) Exec(_ context.Context, _ string, _ ...any) error { return nil }
func (q *fakeColdStoreQuerier) Ping(_ context.Context) error                     { return nil }
func (q *fakeColdStoreQuerier) Close() error                                     { return nil }

type recordingSSEDispatcher struct {
	mu     sync.Mutex
	frames [][]byte
	events []entity.NotificationEvent
}

func (d *recordingSSEDispatcher) DispatchSSEFrame(_ context.Context, event entity.NotificationEvent, sseFrame []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := make([]byte, len(sseFrame))
	copy(cp, sseFrame)
	d.frames = append(d.frames, cp)
	d.events = append(d.events, event)
	return nil
}

type recordingWebhookEnqueuer struct {
	mu    sync.Mutex
	tasks []*notificationv1.WebhookDeliveryTask
}

func (w *recordingWebhookEnqueuer) EnqueueWebhookTask(_ context.Context, task *notificationv1.WebhookDeliveryTask) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tasks = append(w.tasks, task)
	return nil
}

type inMemoryPubSub struct {
	ch chan []byte
}

func newInMemoryPubSub() *inMemoryPubSub {
	return &inMemoryPubSub{ch: make(chan []byte, 32)}
}

func (p *inMemoryPubSub) Subscribe(_ context.Context, _ string) (redis.PubSubSubscription, error) {
	return p, nil
}

func (p *inMemoryPubSub) Publish(_ context.Context, _ string, payload []byte) error {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	p.ch <- cp
	return nil
}

func (p *inMemoryPubSub) Messages() <-chan []byte { return p.ch }
func (p *inMemoryPubSub) Close() error            { return nil }

type sliceRecordStream struct {
	mu      sync.Mutex
	records []kafka.CDCRecord
	idx     int
	commits map[string]int64
}

func (s *sliceRecordStream) Fetch(ctx context.Context) (kafka.CDCRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.records) {
		return kafka.CDCRecord{}, context.Canceled
	}
	rec := s.records[s.idx]
	s.idx++
	return rec, ctx.Err()
}

func (s *sliceRecordStream) CommitOffset(_ context.Context, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commits == nil {
		s.commits = make(map[string]int64)
	}
	s.commits[fmt.Sprintf("%s:%d", topic, partition)] = offset
	return nil
}

func (s *sliceRecordStream) Close() error { return nil }

// ============================================================================
// Phase 1 Unit, Concurrency & Benchmark Tests
// ============================================================================

func TestProtobufWireRoundTripAndValidation(t *testing.T) {
	env := &notificationv1.NotificationEventEnvelope{
		EventId:          "01J9Z8K4M2N6P8Q0R2S4T6V8W0",
		TenantId:         "tnt_alpha",
		UserId:           "usr_42",
		SourceService:    entity.SourceServiceIngestionCoordinator,
		EventType:        entity.EventTypeIngestionMilestone,
		Severity:         notificationv1.Severity_SEVERITY_INFO,
		IsTerminal:       false,
		OccurredAtUnixMs: 1762000000000,
		TraceId:          "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		PayloadJson:      []byte(`{"document_id":"doc_99","stage":"CHUNK_COMPILATION","progress_pct":75}`),
	}

	if err := env.Validate(); err != nil {
		t.Fatalf("expected valid envelope, got error: %v", err)
	}

	wireBytes, err := env.MarshalWire()
	if err != nil {
		t.Fatalf("MarshalWire failed: %v", err)
	}

	var decoded notificationv1.NotificationEventEnvelope
	if err := decoded.UnmarshalWire(wireBytes); err != nil {
		t.Fatalf("UnmarshalWire failed: %v", err)
	}

	if decoded.EventId != env.EventId || decoded.TenantId != env.TenantId || decoded.TraceId != env.TraceId {
		t.Fatalf("decoded envelope mismatch: %+v vs %+v", decoded, env)
	}
	if !bytes.Equal(decoded.PayloadJson, env.PayloadJson) {
		t.Fatalf("payload_json mismatch: %s vs %s", decoded.PayloadJson, env.PayloadJson)
	}

	// Verify WebhookDeliveryTask deterministic task_id & binary wire round-trip.
	taskID := entity.DeterministicTaskID(env.TenantId, env.EventId, "trg_01")
	task := &notificationv1.WebhookDeliveryTask{
		TaskId:              taskID,
		EventId:             env.EventId,
		TenantId:            env.TenantId,
		TargetId:            "trg_01",
		TargetUrl:           "https://hooks.customer.example/rag/events",
		SecretRef:           "vault://secrets/tenants/tnt_alpha/webhook-hmac-key",
		SignatureHeaderName: entity.DefaultWebhookSignatureHeader,
		TimestampUnixSec:    1762000000,
		EventType:           env.EventType,
		PayloadJson:         env.PayloadJson,
		TraceId:             env.TraceId,
	}
	if err := task.Validate(); err != nil {
		t.Fatalf("expected valid WebhookDeliveryTask, got: %v", err)
	}

	taskWire, err := task.MarshalWire()
	if err != nil {
		t.Fatalf("task.MarshalWire failed: %v", err)
	}
	var decodedTask notificationv1.WebhookDeliveryTask
	if err := decodedTask.UnmarshalWire(taskWire); err != nil {
		t.Fatalf("task.UnmarshalWire failed: %v", err)
	}
	if decodedTask.TaskId != task.TaskId || decodedTask.SecretRef != task.SecretRef {
		t.Fatalf("decoded task mismatch: %+v", decodedTask)
	}
}

func TestZeroIntermediarySSEFrameFormatting(t *testing.T) {
	occurred := time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)
	ev := entity.NotificationEvent{
		EventID:       "01J9Z8K4M2N6P8Q0R2S4T6V8W0",
		TenantID:      "tnt_01H",
		UserID:        "usr_01",
		SourceService: entity.SourceServiceIngestionCoordinator,
		EventType:     entity.EventTypeIngestionMilestone,
		Severity:      entity.SeverityInfo,
		IsTerminal:    false,
		OccurredAt:    occurred,
		TraceID:       "trace-xyz",
		PayloadJSON:   []byte(`{"document_id":"doc_99","stage":"CHUNK_COMPILATION","progress_pct":75}`),
	}

	if err := ev.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	frame := string(ev.FormatSSEFrame(5000))
	if !strings.HasPrefix(frame, "id: 01J9Z8K4M2N6P8Q0R2S4T6V8W0\nevent: ingestion.milestone\nretry: 5000\ndata: ") {
		t.Fatalf("unexpected SSE frame header:\n%s", frame)
	}
	if !strings.HasSuffix(frame, "\n\n") {
		t.Fatalf("SSE frame must terminate with double newline")
	}

	dataLine := strings.TrimSuffix(strings.Split(frame, "data: ")[1], "\n\n")
	if !json.Valid([]byte(dataLine)) {
		t.Fatalf("SSE data line is not valid JSON: %s", dataLine)
	}

	// Verify multiline JSON payloads are compacted onto a single SSE data line.
	evMultiline := ev
	evMultiline.PayloadJSON = []byte("{\n  \"document_id\": \"doc_99\",\n  \"stage\": \"CHUNK_COMPILATION\"\n}")
	multilineFrame := string(evMultiline.FormatSSEFrame(5000))
	multilineData := strings.TrimSuffix(strings.Split(multilineFrame, "data: ")[1], "\n\n")
	if strings.Contains(multilineData, "\n") {
		t.Fatalf("expected compacted single-line SSE data JSON, got newline in: %q", multilineData)
	}
	if strings.Contains(string(evMultiline.FormatSSEDataJSON()), "\n") {
		t.Fatalf("expected FormatSSEDataJSON to compact multiline payload onto a single line")
	}
}

func TestZeroHotPathSQLReadsAndRuleEvaluation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	coldQuerier := newFakeColdStoreQuerier()
	coldQuerier.setTenantData("tnt_enterprise", []entity.Subscription{
		{
			SubscriptionID:   "sub_01",
			TenantID:         "tnt_enterprise",
			EventTypePattern: "ingestion.*",
			MinSeverity:      entity.SeverityInfo,
			SSEEnabled:       true,
			WebhookEnabled:   true,
			Suppressed:       false,
			RateLimitRPS:     5000,
			UpdatedAt:        now,
		},
	}, []entity.WebhookTarget{
		{
			TargetID:            "wh_01",
			TenantID:            "tnt_enterprise",
			TargetURL:           "https://api.enterprise.example/webhooks/rag",
			SecretRef:           "kms://projects/rag/keys/tnt_enterprise_hmac",
			SignatureHeaderName: entity.DefaultWebhookSignatureHeader,
			Active:              true,
			EventTypes:          []string{"ingestion.milestone"},
			UpdatedAt:           now,
		},
	})

	if _, err := postgres.NewPool(postgres.PoolConfig{}, nil); err == nil {
		t.Fatalf("expected NewPool with nil querier to return an error")
	}

	pool, err := postgres.NewPool(postgres.PoolConfig{
		DSN: observability.NewSecretString("postgres://user:supersecret@pgbouncer:6432/rag"),
	}, coldQuerier)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}

	subRepo := postgres.NewSubscriptionRepo(pool)
	webhookRepo := postgres.NewWebhookRepo(pool)
	cache := memory.NewSubscriptionCache()

	// Cold-start cache warm-up executes exactly 2 SQL queries (1 for subscriptions, 1 for webhook targets).
	if err := cache.WarmUp(ctx, subRepo, webhookRepo); err != nil {
		t.Fatalf("WarmUp failed: %v", err)
	}
	warmUpSQLCount := pool.QueryCount()
	if warmUpSQLCount != 2 {
		t.Fatalf("expected 2 cold-start SQL queries during WarmUp, got %d", warmUpSQLCount)
	}

	sseDispatcher := &recordingSSEDispatcher{}
	webhookEnqueuer := &recordingWebhookEnqueuer{}
	var logBuf bytes.Buffer
	logger := observability.NewSecuredLogger(&logBuf, slog.LevelInfo)
	metrics := observability.NewMetricsRegistry()

	handler := command.NewProcessCDCEventHandler(
		cache,
		router.NewRuleEvaluator(),
		router.NewFilter(5*time.Minute, 65536),
		sseDispatcher,
		webhookEnqueuer,
		logger,
		metrics,
	)

	// Concurrently process 200 CDC events on the hot path and verify 0 additional SQL queries occur.
	const numEvents = 200
	var wg sync.WaitGroup
	errCh := make(chan error, numEvents)

	for i := 0; i < numEvents; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			env := &notificationv1.NotificationEventEnvelope{
				EventId:          fmt.Sprintf("01J9Z8K4M2N6P8Q0R2S4T6%04d", idx),
				TenantId:         "tnt_enterprise",
				UserId:           "usr_1",
				SourceService:    entity.SourceServiceIngestionCoordinator,
				EventType:        entity.EventTypeIngestionMilestone,
				Severity:         notificationv1.Severity_SEVERITY_INFO,
				IsTerminal:       idx == numEvents-1,
				OccurredAtUnixMs: now.UnixMilli(),
				TraceId:          "trace-hotpath-test",
				PayloadJson:      []byte(`{"document_id":"doc_1","progress_pct":50}`),
			}
			res, hErr := handler.Handle(ctx, env)
			if hErr != nil {
				errCh <- hErr
				return
			}
			if !res.SSEDispatched || res.WebhooksEnqueued != 1 {
				errCh <- fmt.Errorf("unexpected dispatch result for event %d: %+v", idx, res)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for hErr := range errCh {
		t.Fatalf("concurrent hot-path Handle failed: %v", hErr)
	}

	// CRITICAL ADR-004 ASSERTION: Zero SQL queries executed during the 200 hot-path CDC events.
	if pool.QueryCount() != warmUpSQLCount {
		t.Fatalf("ADR-004 violation: expected 0 hot-path SQL queries (total=%d), got %d", warmUpSQLCount, pool.QueryCount())
	}
	if metrics.EventsProcessedCount(entity.EventTypeIngestionMilestone, "dispatched") != numEvents {
		t.Fatalf("expected %d dispatched events in Prometheus metrics", numEvents)
	}
	if metrics.WebhookEnqueuedCount("tnt_enterprise", "enqueued") != numEvents {
		t.Fatalf("expected %d enqueued webhooks in Prometheus metrics", numEvents)
	}
}

func TestCacheReloadOnInvalidationCoherenceUnder100ms(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now().UTC()
	coldQuerier := newFakeColdStoreQuerier()
	pool, _ := postgres.NewPool(postgres.PoolConfig{}, coldQuerier)
	subRepo := postgres.NewSubscriptionRepo(pool)
	webhookRepo := postgres.NewWebhookRepo(pool)
	cache := memory.NewSubscriptionCache()

	if err := cache.WarmUp(ctx, subRepo, webhookRepo); err != nil {
		t.Fatalf("WarmUp failed: %v", err)
	}

	pubsub := newInMemoryPubSub()
	var logBuf bytes.Buffer
	logger := observability.NewSecuredLogger(&logBuf, slog.LevelInfo)
	metrics := observability.NewMetricsRegistry()

	invalidationSub := redis.NewInvalidationSubscriber(pubsub, cache, subRepo, webhookRepo, logger, metrics)
	go func() {
		_ = invalidationSub.Run(ctx)
	}()

	// Mutate tenant rules in the cold store and publish `tenant.config.updated`.
	coldQuerier.setTenantData("tnt_dynamic", []entity.Subscription{
		{
			SubscriptionID:   "sub_dyn_1",
			TenantID:         "tnt_dynamic",
			EventTypePattern: entity.EventTypeEvaluationQualityAlert,
			MinSeverity:      entity.SeverityWarning,
			SSEEnabled:       true,
			WebhookEnabled:   false,
			Suppressed:       false,
			RateLimitRPS:     100,
			UpdatedAt:        now,
		},
	}, nil)

	start := time.Now()
	if err := invalidationSub.BroadcastInvalidation(ctx, "tnt_dynamic", "subscription_rule_updated"); err != nil {
		t.Fatalf("BroadcastInvalidation failed: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Fatalf("SLO violation: cache invalidation reload took %v (> 100ms)", elapsed)
	}

	subs, _ := cache.GetTenantRules("tnt_dynamic")
	if len(subs) != 1 || subs[0].SubscriptionID != "sub_dyn_1" {
		t.Fatalf("expected tnt_dynamic rules to be reloaded into cache, got %+v", subs)
	}
	if metrics.CacheInvalidationsCount("subscription_rule_updated") == 0 {
		t.Fatalf("expected notification_cache_invalidations_total counter to increment")
	}
}

func TestBoundedDeduplicationAndBurstRateLimiting(t *testing.T) {
	now := time.Now().UTC()
	const maxCap = 10
	filter := router.NewFilter(1*time.Minute, maxCap)

	// 1. Duplicate detection within window + unambiguous colon delimiter boundary check.
	if filter.CheckAndRecordDuplicate("tnt_1", "evt_1", now) {
		t.Fatalf("first sighting of evt_1 must not be marked duplicate")
	}
	if !filter.CheckAndRecordDuplicate("tnt_1", "evt_1", now.Add(5*time.Second)) {
		t.Fatalf("second sighting of evt_1 within TTL must be marked duplicate")
	}
	if filter.CheckAndRecordDuplicate("a:b", "c", now) {
		t.Fatalf("first sighting of (a:b, c) must not be duplicate")
	}
	if filter.CheckAndRecordDuplicate("a", "b:c", now) {
		t.Fatalf("(a, b:c) must not collide with (a:b, c)")
	}

	// 2. Bounded capacity enforcement under high volume.
	for i := 0; i < 50; i++ {
		filter.CheckAndRecordDuplicate("tnt_1", fmt.Sprintf("evt_bulk_%d", i), now)
		if filter.DedupSize() > maxCap {
			t.Fatalf("deduplication map exceeded max capacity %d: got %d", maxCap, filter.DedupSize())
		}
	}

	// 3. Token-bucket burst rate limiting + terminal state bypass.
	for i := 0; i < 5; i++ {
		if !filter.AllowTenantBurst("tnt_burst", 5, false, now) {
			t.Fatalf("expected initial burst %d to be allowed", i)
		}
	}
	if filter.AllowTenantBurst("tnt_burst", 5, false, now) {
		t.Fatalf("expected 6th non-terminal event in same instant to be rate-limited")
	}
	if !filter.AllowTenantBurst("tnt_burst", 5, true, now) {
		t.Fatalf("terminal event (is_terminal=true) must never be dropped by rate limiter")
	}

	// Verify capacity clamp applies even when elapsed == 0 and rateLimitRPS is dynamically lowered.
	if !filter.AllowTenantBurst("tnt_clamp", 10, false, now) {
		t.Fatalf("expected initial event for tnt_clamp at rate=10 to be allowed")
	}
	// Lower rate to 1 at the exact same timestamp (elapsed == 0); tokens (9) must be clamped to capacity (1), consuming 1 -> 0 remaining.
	if !filter.AllowTenantBurst("tnt_clamp", 1, false, now) {
		t.Fatalf("expected first event after rate reduction to 1 to be allowed")
	}
	if filter.AllowTenantBurst("tnt_clamp", 1, false, now) {
		t.Fatalf("expected second event at same timestamp with rate=1 to be rate-limited after capacity clamp")
	}
}

func TestCDCConsumerOffsetTrackingAndPoisonPillIsolation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	cache := memory.NewSubscriptionCache()
	cache.ReplaceTenantRules("tnt_cdc", []entity.Subscription{
		{
			SubscriptionID:   "sub_cdc",
			TenantID:         "tnt_cdc",
			EventTypePattern: "*",
			MinSeverity:      entity.SeverityInfo,
			SSEEnabled:       true,
			RateLimitRPS:     1000,
			UpdatedAt:        now,
		},
	}, nil)

	var logBuf bytes.Buffer
	logger := observability.NewSecuredLogger(&logBuf, slog.LevelInfo)
	metrics := observability.NewMetricsRegistry()
	handler := command.NewProcessCDCEventHandler(cache, nil, nil, nil, nil, logger, metrics)

	validEnv1 := &notificationv1.NotificationEventEnvelope{
		EventId:          "01J9Z8K4M2N6P8Q0R2S4T6V001",
		TenantId:         "tnt_cdc",
		SourceService:    entity.SourceServiceKBCatalog,
		EventType:        entity.EventTypeIndexBlueGreenSwitched,
		Severity:         notificationv1.Severity_SEVERITY_INFO,
		IsTerminal:       true,
		OccurredAtUnixMs: now.UnixMilli(),
		PayloadJson:      []byte(`{"index_id":"idx_blue"}`),
	}
	wire1, _ := validEnv1.MarshalWire()

	validEnv2 := &notificationv1.NotificationEventEnvelope{
		EventId:          "01J9Z8K4M2N6P8Q0R2S4T6V002",
		TenantId:         "tnt_cdc",
		SourceService:    entity.SourceServiceChunkLifecycle,
		EventType:        entity.EventTypeChunkOverlayConflict,
		Severity:         notificationv1.Severity_SEVERITY_WARNING,
		IsTerminal:       false,
		OccurredAtUnixMs: now.UnixMilli(),
		PayloadJson:      []byte(`{"chunk_id":"chk_77"}`),
	}
	wire2, _ := validEnv2.MarshalWire()

	stream := &sliceRecordStream{
		records: []kafka.CDCRecord{
			{Topic: "rag.cdc.kb-catalog", Partition: 0, Offset: 10, Value: wire1},
			{Topic: "rag.cdc.kb-catalog", Partition: 0, Offset: 11, Value: []byte{0xff, 0x00, 0xfe}}, // Poison pill wire bytes
			{Topic: "rag.cdc.kb-catalog", Partition: 0, Offset: 12, Value: wire2},
		},
	}

	consumer := kafka.NewCDCConsumer(kafka.ConsumerConfig{}, stream, handler, logger, metrics)
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer.Run returned unexpected error: %v", err)
	}

	if got := consumer.CommittedOffset("rag.cdc.kb-catalog", 0); got != 13 {
		t.Fatalf("expected committed next-offset watermark 13 after processing offsets 10..12, got %d", got)
	}
	if metrics.EventsProcessedCount("malformed", "malformed") != 1 {
		t.Fatalf("expected 1 malformed poison-pill record counted in metrics")
	}
}

func TestSecuredLoggingRedactionInvariants(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewSecuredLogger(&buf, slog.LevelDebug)

	sensitiveSecret := "whsec_live_987654321_super_secret_hmac_key"
	sensitiveJWT := "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRfaWQiOiJ0bnRfMSJ9.signature"
	sensitiveDSN := "postgres://rag_admin:TopSecretPgPass!@pgbouncer.internal:6432/rag_prod"
	sensitivePII := `{"user_email":"alice@secret-domain.example","document_text":"CONFIDENTIAL_MERGER_TERMS"}`

	ev := entity.NotificationEvent{
		EventID:       "01J9Z8K4M2N6P8Q0R2S4T6V999",
		TenantID:      "tnt_sec",
		UserID:        "usr_sec",
		SourceService: entity.SourceServiceIngestionCoordinator,
		EventType:     entity.EventTypeIngestionMilestone,
		Severity:      entity.SeverityInfo,
		OccurredAt:    time.Now().UTC(),
		PayloadJSON:   []byte(sensitivePII),
	}

	target := entity.WebhookTarget{
		TargetID:  "trg_sec",
		TenantID:  "tnt_sec",
		TargetURL: "https://customer.example/hook",
		SecretRef: sensitiveSecret,
		Active:    true,
	}

	logger.Info("testing secured logging redaction",
		slog.Any("event", ev),
		slog.Any("webhook_target", target),
		slog.String("authorization", sensitiveJWT),
		slog.String("postgres_dsn", sensitiveDSN),
		slog.String("hmac_key", sensitiveSecret),
		slog.String("payload", sensitivePII),
	)

	output := buf.String()
	for _, forbidden := range []string{
		sensitiveSecret,
		sensitiveJWT,
		"TopSecretPgPass!",
		"alice@secret-domain.example",
		"CONFIDENTIAL_MERGER_TERMS",
	} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("Secured Logging violation: raw sensitive string %q leaked in JSON log:\n%s", forbidden, output)
		}
	}
	if !strings.Contains(output, observability.RedactedPlaceholder) {
		t.Fatalf("expected %s in log output:\n%s", observability.RedactedPlaceholder, output)
	}
}

func BenchmarkDirectSSEFrameSerialization(b *testing.B) {
	ev := entity.NotificationEvent{
		EventID:       "01J9Z8K4M2N6P8Q0R2S4T6V8W0",
		TenantID:      "tnt_01H",
		UserID:        "usr_01",
		SourceService: entity.SourceServiceIngestionCoordinator,
		EventType:     entity.EventTypeIngestionMilestone,
		Severity:      entity.SeverityInfo,
		IsTerminal:    false,
		OccurredAt:    time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC),
		TraceID:       "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		PayloadJSON:   []byte(`{"document_id":"doc_99","stage":"CHUNK_COMPILATION","progress_pct":75}`),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame := ev.FormatSSEFrame(5000)
		if len(frame) == 0 {
			b.Fatal("empty frame")
		}
	}
}
