# Progress Tracker - RAG Notification Dispatch Daemon

## Current Phase

- **Phase 1: Ingestion Pipeline & Contracts** (🟢 Completed)
- **Phase 2: High-Density SSE Engine** (🟢 Completed)
- **Phase 3: Targeted Routing & Egress Enqueueing** (🟢 Completed)
- **Next Up — Phase 4: Resilience, Chaos & Production Rollout** (⏳ Ready to start)

## Current Goal

- Complete Phase 3 (Redis 7.2 presence directory `rag:notify:presence:{tenant_id}:{user_id}` and tenant node index with 45s TTL and immediate last-socket pruning, targeted pod router `pod.{node_id}.events` carrying pre-serialized `sse_frame` envelopes without $O(N \times M)$ fan-out, asynchronous HMAC-SHA256 metadata webhook task enqueueing with $P99 \le 15\text{ms}$ budget, bounded Redis Stream replay buffer `rag:notify:replay:{tenant_id}` with cold-journal fallback, and token-bucket reconnect rate limiting returning HTTP 429) and prepare for Phase 4 (100,000 socket cluster soak testing, SIGTERM chaos verification, Helm charts, and distroless containerization).

---

## Calibrated 4-Phase Roadmap & Milestones

| Phase | Duration | Core Domain Focus | Deliverables & Milestone Exit Criteria | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1: Ingestion Pipeline & Contracts** | Weeks 1–2 | Event Consumption & Schemas | • Repository governance: `.gitignore` (excluding `tasks/`), `.github/workflows/ci.yml` (`lint-and-typecheck`, `security-and-logging-audit`, `openapi-and-doc-api-sync`, `test-suites`), `.ai/`, `.ai.md`, and `GEMINI.MD`.<br>• Protobuf v3 contracts (`api/proto/v1/notification_event.proto`, `webhook_task.proto`, `contracts.pb.go`) for internal Kafka outbox events and webhook delivery tasks; clean JSON formatting pipeline for SSE client writes.<br>• Kafka consumer group setup (`internal/infrastructure/queue/kafka/cdc_consumer.go`) with offset tracking.<br>• In-memory local tenant subscription cache (`internal/infrastructure/cache/memory/subscription_cache.go`) with asynchronous `tenant.config.updated` invalidation listeners (`<= 100ms`). | 🟢 Completed |
| **Phase 2: High-Density SSE Engine** | Weeks 3–4 | Streaming Infrastructure | • Non-blocking SSE stream handler (`internal/infrastructure/transport/sse/stream_handler.go`) with bounded per-client channels (buffer size: 256 events in `ring_buffer.go`).<br>• Edge proxy compatibility: immediate `http.Flusher` comment frames (`: ping\n\n`) every 15s (`flusher.go`), `X-Accel-Buffering: no` enforcement, and Slow-Loris write deadline protection.<br>• Slow consumer drop policies dropping non-terminal progress frames at capacity (256) while retaining terminal transitions, and evicting connections stalled for `> 5s` (`connection_hub.go`).<br>• ADR-005 per-connection staggered graceful draining with randomized jitter [30s, 60s] and `retry: <ms>\n\n` directive frame emission. | 🟢 Completed |
| **Phase 3: Targeted Routing & Egress Enqueueing** | Weeks 5–6 | Targeted Node Routing & Queues | • Targeted pod routing channels on Redis (`pod.{node_id}.events` via `targeted_router.go` and `presence_registry.go`) replacing broadcast Pub/Sub (ADR-003).<br>• Webhook routing evaluator (`rule_evaluator.go`, `dispatch_webhook_task.go`) and HMAC-SHA256 task enqueueing to Redis Streams / RabbitMQ (`P99 <= 15ms`, ADR-001).<br>• Bounded `Last-Event-ID` replay buffer in Redis (`fetch_replay_events.go`) with token-bucket rate-limited backfill queries and cold-journal fallback (`event_journal_repo.go`). | 🟢 Completed |
| **Phase 4: Resilience, Chaos & Production Rollout** | Weeks 7–8 | Load Tuning & Rollout | • Soak testing with 100,000 concurrent persistent connections across 5 pods (`~20,000` per pod, RSS `<= 1.6GiB`) under continuous broadcast bursts.<br>• Chaos testing verifying graceful draining on `SIGTERM` with client reconnection jitter spread across 30–60s (`terminationGracePeriodSeconds: 75`) without thundering herd spikes or Redis CPU `> 60%`.<br>• Production Helm cutover (`deployments/helm/`) and multi-stage distroless Dockerfile (`gcr.io/distroless/static:nonroot`, `GOMEMLIMIT=1800MiB`, `GOGC=80`). | ⏳ Planned |

---

## Completed Units

- **Phase 3 Implementation — Targeted Routing & Egress Enqueueing**:
  - **Redis 7.2 Presence Directory & Bounded Replay Store (`internal/infrastructure/cache/redis/presence_registry.go`)**:
    - Implemented `command.PresenceRegistrar` (`RegisterPresence`, `DeregisterPresence`, `RefreshPresence`).
    - Hot-path targeted node queries: `rag:notify:presence:{tenant_id}:{user_id}` and `rag:notify:tenant_nodes:{tenant_id}` resolved in $\le 5\text{ms}$.
    - 45s heartbeat refresh on 15s SSE keepalive pings; immediate node pruning from `tenant_nodes` on last local socket disconnect to prevent stale ghost routing.
    - Bounded Redis Stream replay buffer (`rag:notify:replay:{tenant_id}`, `MAXLEN ~ 1000`, `TTL = 3600s`).
    - High-performance thread-safe multi-node Redis cluster simulator (`InMemoryRedisClusterClient`) for deterministic multi-instance tests.
  - **Targeted Pod Routing Engine (`internal/infrastructure/transport/hub/targeted_router.go`)**:
    - Implemented `command.NodeEventDispatcher` (`ADR-003`): delivers directly to local `ConnectionHub` when `node_id == localNodeID` (zero network hop, zero re-serialization), and publishes pre-formatted `sse_frame` wire envelopes (`PodRoutedEnvelope`) with OpenTelemetry W3C `trace_id` directly to `pod.{node_id}.events`.
    - Background subscriber on `pod.{local_node_id}.events` receiving peer-routed envelopes and pushing pre-formatted frames directly into matching client ring buffers.
  - **Asynchronous Webhook Task Enqueueing (`internal/application/command/dispatch_webhook_task.go` & `internal/infrastructure/queue/publisher/task_queue_publisher.go`)**:
    - Built deterministic task generator (`task_id = sha256(tenant_id + ":" + event_id + ":" + target_id)`) with `secret_ref` and `payload_json` Secured Logging redaction (`ADR-001`).
    - Hand-off latency within $P99 \le 15\text{ms}$ budget; Protobuf v3 binary wire format serialization (`MarshalWire()`).
    - Centralized `notification_webhook_enqueued_total{tenant_id, status}` metric accounting with exactly-once increment per task attempt.
  - **Bounded `Last-Event-ID` Replay Buffer & Rate-Limited Backfill (`internal/application/query/fetch_replay_events.go`)**:
    - Query missed tenant events strictly after client's watermark from `rag:notify:replay:{tenant_id}` with cold-path `EventJournalRepository` fallback (`event_journal_repo.go`).
    - Per-tenant token-bucket backfill rate limiting returning `ErrBackfillRateLimited` -> HTTP `429 Too Many Requests` with `Retry-After: 5` to protect Redis CPU ($< 60\%$) during rolling restarts (`Runbook 2`).
  - **Full Unit & End-to-End Integration Test Suites (`test/unit/`, `test/integration/`)**:
    - `test/unit/phase3_routing_and_webhook_test.go`: verified multi-pod targeted routing (`Pod A` vs `Pod B`), TTL refresh and immediate last-socket pruning, deterministic webhook task enqueueing, and rate-limited replay backfill.
    - `test/integration/dispatch_pipeline_integration_test.go`: end-to-end verification of CDC ingestion -> 0 SQL hot path -> targeted pod routing (`pod.pod-b.events`) -> SSE client stream delivery + `Last-Event-ID` replay + $\le 100\text{ms}$ cache invalidation coherence.

- **Phase 2 Implementation — High-Density SSE Engine**:
  - **Bounded Per-Client Ring Buffer (`internal/infrastructure/transport/hub/ring_buffer.go`)**:
    - Compact 256-event ring buffer layout (~2KiB baseline heap footprint).
    - Non-blocking $O(1)$ push with channel-1 wakeups; drops intermediate non-terminal frames at capacity (`reason="ring_buffer_saturated"`).
    - Terminal frame preemption: evicts oldest unread non-terminal frame while preserving FIFO ordering (`reason="terminal_frame_preemption"`).
    - All-256-terminal overflow guard: flags connection for immediate eviction on arrival of 257th terminal frame (`reason="all_terminal_eviction"`).
    - Continuous saturation ceiling: marks and evicts slow consumers continuously saturated for $> 5\text{s}$ (`reason="slow_consumer_evicted"`).
  - **Local Node Connection Hub (`internal/infrastructure/transport/hub/connection_hub.go`)**:
    - Pod-local session registries indexed by `tenant_id`, `user_id`, and `connection_id`.
    - Strict per-tenant and user-targeted (`event.UserID != ""`) push isolation with optional `severity_filter` comparison.
    - Implemented `command.NodeEventDispatcher` (`DispatchSSEFrame`) wired to `ProcessCDCEventHandler`.
    - Background slow-consumer sweeper evicting idle-traffic saturated buffers.
    - ADR-005 per-connection staggered graceful draining assigning independent jitter delays in `[30s, 60s]` and emitting `retry: <ms>\n\n` frames.
    - Deterministic cursor-based pagination for `GET /v1/notifications/sessions`.
  - **Edge Proxy Flusher & Slow-Loris Guard (`internal/infrastructure/transport/sse/flusher.go`)**:
    - Enforced `X-Accel-Buffering: no`, `Content-Type: text/event-stream; charset=utf-8`, and `Cache-Control: no-cache`.
    - Immediate and periodic 15s `: ping\n\n` heartbeat comment frames via `http.Flusher`.
    - `http.ResponseController.SetWriteDeadline` bounding write stalls against zeroed TCP receive windows.
    - Histogram telemetry for `notification_client_write_latency_seconds{node_id}`.
  - **Multiplexed Stream Handler (`internal/infrastructure/transport/sse/stream_handler.go`)**:
    - Serves `GET /v1/notifications/stream` with high-watermark deduplicated backfill port, 15s keepalive loop, and graceful drain listener.
  - **Application Commands & Queries (`internal/application/`)**:
    - Implemented `command/register_connection.go`, `command/terminate_connection.go`, and `query/list_active_sessions.go`.
  - **HTTP Security Middleware & Unified Router (`internal/infrastructure/transport/http/`)**:
    - Implemented `middleware/middleware.go` (correlation ID, sanitized panic recovery, secured logging, bearer token auth, tenant isolation 403, and token-bucket reconnect rate limiting 429).
    - Implemented `router.go` assembling probes, docs, streams, sessions, and cache invalidation routes.
  - **Daemon Entry Point & Unit Test Suite (`cmd/`, `test/unit/`)**:
    - Wired `main.go` with `ConnectionHub`, `StreamHandler`, `Router`, and staggered `SIGTERM` draining.
    - Implemented `test/unit/phase2_sse_engine_test.go` verifying ring buffer drop/preemption/eviction policies, hub isolation, slow-consumer sweeping, staggered draining, cursor pagination, flusher headers, and security middleware.

---

## Completed Units

- **Phase 1 Implementation — Ingestion Pipeline & Contracts**:
  - **Protobuf v3 & OpenAPI 3.1 Contracts (`api/`)**:
    - Created `api/proto/v1/notification_event.proto`, `api/proto/v1/webhook_task.proto`, and `api/proto/v1/contracts.pb.go` with binary/JSON wire marshalers, schema validation, and `slog.LogValuer` redaction.
    - Materialized `api/openapi/openapi.yaml` synchronized 1:1 with `.ai/doc-api.md`.
  - **Hexagonal Domain Layer (`internal/domain/`)**:
    - Implemented `entity/notification.go` (with zero-intermediary `FormatSSEFrame` & `FormatSSEDataJSON`), `entity/subscription.go`, `entity/webhook_target.go` (with deterministic SHA-256 `task_id`), `entity/connection.go`, `router/rule_evaluator.go`, `router/filter.go` (bounded TTL deduplication & token-bucket burst limiter), and repository ports (`subscription_repo.go`, `webhook_target_repo.go`, `event_journal_repo.go`).
  - **Pod-Local Subscription Cache, Reload-on-Invalidation & Cold-Path PostgreSQL (`internal/infrastructure/`)**:
    - Implemented `cache/memory/subscription_cache.go` guaranteeing 0 SQL reads on the hot CDC ingestion path (ADR-004).
    - Implemented `cache/redis/invalidation_sub.go` listening on `tenant.config.updated` with `<= 100ms` reload-on-invalidation from cold-path PostgreSQL (`persistence/postgres/pool.go`, `subscription_repo.go`, `webhook_repo.go`).
  - **CDC Consumer Group, Command Orchestrator & Observability**:
    - Implemented `application/command/process_cdc_event.go`, `queue/kafka/cdc_consumer.go` (offset watermark tracking & poison-pill isolation), `observability/logging.go` (Secured Logging `ReplaceAttr` redaction & 6 canonical Prometheus metrics), `cmd/notification-daemon/main.go`, and `test/unit/phase1_ingestion_test.go`.
- **Repository Governance & CI/CD Foundation**:
  - **Git Ignore Configuration (`.gitignore`)**:
    - Excluded `tasks/` specification directory, Go binaries/build artifacts, test profiles, `.env` secrets, and private keys.
  - **GitHub Actions CI Workflow (`.github/workflows/ci.yml`)**:
    - Configured automated jobs for `lint-and-typecheck` (Go 1.23+, `gofmt -s`, `go vet`, `golangci-lint`), `security-and-logging-audit` (`.gitignore` verification, unstructured logging check, raw secret/token redaction audit, `govulncheck`), `openapi-and-doc-api-sync` (OpenAPI 3.1 & `.ai/doc-api.md` contract synchronization), `test-suites` (`go test -v -race`), and `architecture-guardrails`.
  - **AI Context & Architecture Alignment (`.ai/`, `.ai.md`, `GEMINI.MD`)**:
    - Realigned `.ai.md` and all 8 `.ai/` documents (`project-overview.md`, `architecture.md`, `doc-api.md`, `code-standards.md`, `security.md`, `ai-workflow-rules.md`, `agents.md`, `progress-tracker.md`) to **`rag-notification-dispatch-daemon`** based on `RAG Notification Dispatch Daemon Service Implementation Plan.pdf`, `Production Grade Backend Concepts.pdf`, `Disaggregated Hybrid Model Architecture.pdf`, `Go and Python Microservices Architecture.pdf`, `My Naming Convention.pdf`, and `Production-Grade Multimodal RAG Architectural Specification.pdf`.
    - Created `GEMINI.MD` referencing `.ai/agents.md` and the full `.ai/` directory, enforcing Continuous OpenAPI 3.1 / Doc API updates, Secured Logging, and Git Commit Standards.
    - Pressure-tested `GEMINI.MD` and `.ai/` against all 6 `tasks/` specification blueprints, adding the `notification_event_journal` schema, 12-Factor environment variable matrix, OpenTelemetry W3C trace propagation, and KMS/Vault `secret_ref` HMAC-SHA256 signing hand-off clarity to `rag-webhook-delivery-worker`.

---

## Architecture Decision Records (ADRs)

- **ADR-001: Separation of Dispatch and Egress Delivery**: The daemon strictly manages event ingestion, SSE streaming, and webhook task enqueueing. Outbound HTTP requests are delegated to `rag-webhook-delivery-worker` to isolate network I/O and prevent SSRF.
- **ADR-002: SSE Unification for Milestone Streaming**: Pipeline notifications standardize 100% on Server-Sent Events. Complex framed WebSockets are removed from this daemon to eliminate protocol bloat and leverage standard HTTP connection infrastructure.
- **ADR-003: Targeted Node Routing Over Global Redis Fan-Out**: Replaces blind Redis Pub/Sub broadcast with a user/node directory and targeted pod channels (`pod.{node_id}.events`) to eliminate $O(N \times M)$ message duplication.
- **ADR-004: In-Memory Subscription Caching**: All tenant routing configurations are stored in pod-local read memory with asynchronous event-driven invalidation, decoupling CDC processing latency from PostgreSQL connection limits.
- **ADR-005: Staggered Connection Draining**: On `SIGTERM`, pods reject new connections and gracefully close active SSE streams using randomized retry jitter over a 30–60 second window to prevent thundering herd spikes.

---

## Operational Runbooks

### Runbook 1: Memory Pressure and OOM Alerting
- Inspect `go_memstats_alloc_bytes` and container memory working set against the `2GiB` limit (alert fires at `> 1.8GiB` for `> 1m`).
- Verify whether `GOMEMLIMIT=1800MiB` and `GOGC=80` are enforcing timely GC sweeps.
- Check `notification_dropped_frames_total`; if frame dropping is low but memory is high, lower the active connection threshold per pod in the HPA target (`<= 15,000` sockets).

### Runbook 2: Diagnosing Redis Replay Buffer Contention
- Monitor Redis CPU utilization during rolling deployments.
- If `fetch_replay_events` causes Redis latency spikes or drives CPU past 60%, verify that the edge API Gateway and HTTP middleware are enforcing reconnect rate limits and that clients are applying randomized backoff jitter (30–60s) on reconnection.

### Runbook 3: Edge Proxy Stream Stalling
- If clients report dropped connections every 60 seconds, inspect gateway logs (ALB/Envoy/Cloudflare).
- Verify that `X-Accel-Buffering: no` is present on `/v1/notifications/stream` response headers and verify that the 15-second heartbeat ping task (`: ping\n\n`) is actively calling `http.Flusher.Flush()` to push bytes to the network layer.
