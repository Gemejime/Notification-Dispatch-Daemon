# Implementation Plan: Phase 3 — Targeted Routing & Egress Enqueueing (`rag-notification-dispatch-daemon`)

## 1. Objectives & Architectural Scope (Pressure-Tested Against `tasks/` & `.ai/`)

Implement **Phase 3: Targeted Routing & Egress Enqueueing** from `RAG Notification Dispatch Daemon Service Implementation Plan.pdf`, `Production Grade Backend Concepts.pdf`, and `.ai/progress-tracker.md`, adhering strictly to `GEMINI.MD`, `.ai/architecture.md` (ADRs 001–005), `.ai/doc-api.md`, `.ai/code-standards.md`, and `.ai/security.md`:

1. **Redis 7.2 Presence Directory (`internal/infrastructure/cache/redis/presence_registry.go`)**:
   - Maintain the lightweight user/tenant-to-node routing directory in Redis (`ADR-003`):
     - User presence key: `rag:notify:presence:{tenant_id}:{user_id}` mapping `{connection_id: node_id}` with `TTL = 45s` (refreshed by the 15s SSE heartbeat loop via the `PresenceRegistrar` port).
     - Tenant node index: `rag:notify:tenant_nodes:{tenant_id}` tracking active `node_id`s hosting connections for `tenant_id` (with `TTL = 45s`, removing `node_id` immediately when the last local connection for `tenant_id` on that node terminates).
   - Resolve target `node_id`s for targeted user notifications (`user_id != ""` -> query `rag:notify:presence:{tenant_id}:{user_id}`) or tenant-wide notifications (`user_id == ""` -> query `rag:notify:tenant_nodes:{tenant_id}`) in $\le 5\text{ms}$ on the hot path.
   - Provide a multi-pod cluster-aware backplane abstraction so multi-instance daemon tests (`Pod A` + `Pod B`) deterministically verify targeted node routing (`pod.pod-a.events`) without $O(N \times M)$ broadcast fan-out (`RAG Notification Dispatch Daemon Service Implementation Plan.pdf` Section 6).
2. **Targeted Pod Routing Engine (`internal/infrastructure/transport/hub/targeted_router.go`)**:
   - Implement `command.NodeEventDispatcher` (`ADR-003`):
     - Append dispatched SSE events to the bounded Redis replay buffer (`rag:notify:replay:{tenant_id}`, `MAXLEN ~ 1000`, `TTL = 3600s`) and optional non-blocking cold-path `EventJournalRepository` so `Last-Event-ID` reconnects can recover across rolling restarts.
     - Query `PresenceRegistry` for active recipient `node_id`s.
     - Deliver directly to local `ConnectionHub` when `node_id == localNodeID` (zero network hop, zero re-serialization).
     - Publish targeted wire envelopes (`PodRoutedEvent`) carrying the pre-serialized `sse_frame []byte` and OpenTelemetry W3C `trace_id` propagation directly to `pod.{node_id}.events` for remote pods hosting matching sessions.
   - Run a background subscriber on `pod.{local_node_id}.events` that decodes incoming peer-routed envelopes and pushes the pre-formatted `sse_frame []byte` directly into the local `ConnectionHub` without re-marshaling JSON.
3. **Asynchronous Webhook Task Enqueueing (`internal/application/command/dispatch_webhook_task.go` & `internal/infrastructure/queue/publisher/task_queue_publisher.go`)**:
   - `internal/application/command/dispatch_webhook_task.go`:
     - Construct and validate `notificationv1.WebhookDeliveryTask` messages with deterministic idempotency key `task_id = sha256(tenant_id + ":" + event_id + ":" + target_id)`, KMS/Vault `secret_ref`, `X-RAG-Signature-256` signature header metadata, `timestamp_unix_sec`, and W3C `trace_id` (`ADR-001`: zero outbound HTTP POST calls from this daemon; `P99 <= 15ms` hand-off latency).
     - Centralize `notification_webhook_enqueued_total{tenant_id, status}` metric accounting between `ProcessCDCEventHandler` and `DispatchWebhookTaskHandler` so counters are incremented **exactly once** per task attempt.
   - `internal/infrastructure/queue/publisher/task_queue_publisher.go`:
     - Implement `command.WebhookTaskEnqueuer` publishing binary Protobuf wire messages (`MarshalWire()`) to the outbound broker stream (`WEBHOOK_QUEUE_STREAM`, default `rag:webhook:tasks`) within the `15ms` timeout budget.
4. **Bounded `Last-Event-ID` Replay Buffer & Rate-Limited Backfill (`internal/application/query/fetch_replay_events.go`)**:
   - Query missed tenant events strictly after the client's `Last-Event-ID` watermark from the bounded Redis Stream replay buffer (`rag:notify:replay:{tenant_id}`, `MAXLEN = 1000`, `TTL = 3600s`) with cold-path `EventJournalRepository.FetchReplayAfter` fallback when `Last-Event-ID` predates the Redis stream window.
   - Enforce per-tenant token-bucket backfill rate limiting (`ErrBackfillRateLimited` -> HTTP `429 Too Many Requests`) to protect Redis CPU (`< 60%`) during rolling restarts (`Runbook 2`).
   - Filter replayed events through `Connection.AcceptsEvent(event)` (`tenant_id`, `user_id`, `severity_filter`), format them into SSE wire frames, and return the high-watermark `last_replayed_event_id` so `stream_handler.go` deduplicates any overlapping live ring-buffer frames.

---

## 2. Files to Create / Modify

| File Path                                                                     | Layer                      | Purpose                                                                                                                                                                                                                                                           |
| :---------------------------------------------------------------------------- | :------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `internal/infrastructure/cache/redis/presence_registry.go`                  | Infrastructure (Redis)     | User/tenant-to-node routing directory (`rag:notify:presence:{tenant_id}:{user_id}`, `rag:notify:tenant_nodes:{tenant_id}`, `TTL=45s`), heartbeat TTL refresh, last-socket node pruning, and bounded replay stream store (`rag:notify:replay:{tenant_id}`) |
| `internal/infrastructure/transport/hub/targeted_router.go`                  | Infrastructure (Transport) | Targeted pod router publishing pre-formatted `sse_frame` envelopes to `pod.{node_id}.events` and subscribing to local `pod.{local_node_id}.events` (`ADR-003`)                                                                                            |
| `internal/application/command/dispatch_webhook_task.go`                     | Application (Command)      | Builds, validates, and enqueues HMAC-metadata-packaged `WebhookDeliveryTask` items (`P99 <= 15ms`, `ADR-001`)                                                                                                                                               |
| `internal/infrastructure/queue/publisher/task_queue_publisher.go`           | Infrastructure (Queue)     | Publishes `WebhookDeliveryTask` binary Protobuf wire payloads to `rag:webhook:tasks` (Redis Streams / RabbitMQ adapter)                                                                                                                                       |
| `internal/application/query/fetch_replay_events.go`                         | Application (Query)        | Rate-limited `Last-Event-ID` reconnect backfill handler over `rag:notify:replay:{tenant_id}` with cold-journal fallback and watermark deduplication                                                                                                           |
| `internal/application/command/process_cdc_event.go`                         | Application (Command)      | Integrate `DispatchWebhookTaskHandler` and targeted `NodeEventDispatcher` without double-counting Prometheus webhook metrics                                                                                                                                  |
| `cmd/notification-daemon/main.go`                                           | Entry Point                | Wire `PresenceRegistry`, `TargetedRouter`, `TaskQueuePublisher`, `DispatchWebhookTaskHandler`, and `FetchReplayEventsHandler`                                                                                                                           |
| `test/unit/phase3_routing_and_webhook_test.go`                              | Test                       | Unit, concurrency (`-race`), multi-pod targeted routing (`Pod A` vs `Pod B`), `Last-Event-ID` replay backfill + rate limiting, and `<= 15ms` webhook enqueueing tests                                                                                   |
| `test/integration/dispatch_pipeline_integration_test.go`                    | Test (Integration)         | End-to-end CDC ingestion -> 0 SQL hot path -> targeted pod routing (`pod.pod-a.events`) -> SSE stream delivery + `Last-Event-ID` replay + `<= 100ms` cache invalidation coherence                                                                           |
| `.ai/doc-api.md`, `api/openapi/openapi.yaml`, `.ai/progress-tracker.md` | Documentation              | Continuous Doc API & OpenAPI 3.1 synchronization and Phase 3 progress tracking                                                                                                                                                                                    |

---

## 3. Pressure-Tested Failure Modes & Mitigations

1. **45s Presence TTL Expiry on Long-Lived Idle SSE Streams**:
   - `rag:notify:presence:{tenant_id}:{user_id}` and `rag:notify:tenant_nodes:{tenant_id}` carry a short `45s` TTL (`ADR-003`). The 15s SSE heartbeat loop in `stream_handler.go` invokes `PresenceRegistrar.RefreshPresence(ctx, conn)` on every `: ping\n\n` tick so active connections never silently expire from the Redis routing directory after 45 seconds.
2. **Stale Node Ghost Routing After Last Local Socket Disconnects**:
   - When the final connection for `tenant_id` on `node_a` closes, `PresenceRegistry.UnregisterPresence` removes `node_a` from `rag:notify:tenant_nodes:{tenant_id}` immediately rather than waiting 45s for TTL expiration, preventing unnecessary `pod.node_a.events` publishes.
3. **Reconnect Backfill vs. Live Ring-Buffer Race (`Last-Event-ID` Duplicate/Out-of-Order Frames)**:
   - Registering a reconnecting stream in `ConnectionHub` before running `fetch_replay_events` ensures no live event emitted during the backfill query is missed; tracking the highest replayed `EventID` watermark and skipping any queued ring-buffer frame with `EventID <= watermark` guarantees gap-free, duplicate-free monotonic delivery.
4. **Pre-Serialized SSE Frame Forwarding Across `pod.{node_id}.events`**:
   - `PodRoutedEvent` carries the pre-built `sse_frame []byte` generated once in `ProcessCDCEventHandler` so receiving pods push raw bytes directly into client `RingBuffer`s without Protobuf-to-JSON re-serialization overhead.
5. **Single-Count Prometheus Metric Invariant (`notification_webhook_enqueued_total`)**:
   - Delegating from `ProcessCDCEventHandler` to `DispatchWebhookTaskHandler` / `TaskQueuePublisher` ensures `notification_webhook_enqueued_total{tenant_id, status}` is incremented exactly once per webhook task.

---

## 4. Security & Concurrency Guardrails

- **SSRF & Egress Isolation (`ADR-001`)**: `DispatchWebhookTaskHandler` and `TaskQueuePublisher` never invoke `net/http` outbound requests; they strictly enqueue Protobuf tasks carrying `secret_ref` handles for `rag-webhook-delivery-worker`.
- **Secured Logging**: `secret_ref`, `payload_json`, and bearer tokens are automatically masked as `"[REDACTED]"` via `slog.LogValuer` and `ReplaceAttr`.
- **Zero Broadcast Amplification (`ADR-003`)**: Events are published only to `pod.{node_id}.events` for pods with active sessions for the target user/tenant.

---

## 5. Verification & Git Commit Plan

1. Run `gofmt -s -w .` and `go vet ./...`.
2. Run unit and integration test suites with the race detector:
   `GOMEMLIMIT=1800MiB GOGC=80 go test -v -race ./cmd/... ./internal/... ./test/unit/... ./test/integration/...`
3. Confirm OpenAPI 3.1 (`api/openapi/openapi.yaml`) and `.ai/doc-api.md` synchronization and Secured Logging audit.
4. Commit Phase 3 in at least two Conventional Commits (`feat:` and `tests:` / `docs:`).
