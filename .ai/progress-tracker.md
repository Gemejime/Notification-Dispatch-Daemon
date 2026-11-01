# Progress Tracker - RAG Notification Dispatch Daemon

## Current Phase

- **Phase 1: Governance, CI/CD, OpenAPI 3.1 Doc API & Context Alignment** (🟢 Completed)
- **Next Up — Phase 1 Implementation: Ingestion Pipeline & Contracts** (⏳ Ready to start)

## Current Goal

- Establish repository governance (`.gitignore`, GitHub Actions CI pipeline), align all `.ai/` context files and `.ai.md` with `RAG Notification Dispatch Daemon Service Implementation Plan.pdf` and `Production Grade Backend Concepts.pdf`, create `GEMINI.MD`, and enforce Secured Logging, continuous OpenAPI 3.1 Doc API updates, and Git Commit Standards.

---

## Calibrated 4-Phase Roadmap & Milestones

| Phase | Duration | Core Domain Focus | Deliverables & Milestone Exit Criteria | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1: Ingestion Pipeline & Contracts** | Weeks 1–2 | Event Consumption & Schemas | • Repository governance: `.gitignore` (excluding `tasks/`), `.github/workflows/ci.yml` (`lint-and-typecheck`, `security-and-logging-audit`, `openapi-and-doc-api-sync`, `test-suites`), `.ai/`, `.ai.md`, and `GEMINI.MD`.<br>• Protobuf v3 contracts (`api/proto/v1/notification_event.proto`, `webhook_task.proto`) for internal Kafka outbox events and webhook delivery tasks; clean JSON formatting pipeline for SSE client writes.<br>• Kafka consumer group setup (`internal/infrastructure/queue/kafka/cdc_consumer.go`) with offset tracking.<br>• In-memory local tenant subscription cache (`internal/infrastructure/cache/memory/subscription_cache.go`) with asynchronous `tenant.config.updated` invalidation listeners (`<= 100ms`). | 🟡 In Progress (Governance & CI Completed) |
| **Phase 2: High-Density SSE Engine** | Weeks 3–4 | Streaming Infrastructure | • Non-blocking SSE stream handler (`internal/infrastructure/transport/sse/stream_handler.go`) using epoll/netpoll or bounded per-client channels (buffer size: 256 events in `ring_buffer.go`).<br>• Edge proxy compatibility: immediate `http.Flusher` comment frames (`: ping\n\n`) every 15s (`flusher.go`) and `X-Accel-Buffering: no` enforcement.<br>• Slow consumer drop policies dropping non-terminal progress frames at capacity (256) while retaining terminal transitions, and evicting connections stalled for `> 5s`. | ⏳ Planned |
| **Phase 3: Targeted Routing & Egress Enqueueing** | Weeks 5–6 | Targeted Node Routing & Queues | • Targeted pod routing channels on Redis (`pod.{node_id}.events` via `targeted_router.go` and `presence_registry.go`) replacing broadcast Pub/Sub (ADR-003).<br>• Webhook routing evaluator (`rule_evaluator.go`, `dispatch_webhook_task.go`) and HMAC-SHA256 task enqueueing to Redis Streams / RabbitMQ (`P99 <= 15ms`).<br>• Bounded `Last-Event-ID` replay buffer in Redis (`fetch_replay_events.go`) with rate-limited backfill queries. | ⏳ Planned |
| **Phase 4: Resilience, Chaos & Production Rollout** | Weeks 7–8 | Load Tuning & Rollout | • Soak testing with 100,000 concurrent persistent connections across 5 pods (`~20,000` per pod, RSS `<= 1.6GiB`) under continuous broadcast bursts.<br>• Chaos testing verifying graceful draining on `SIGTERM` with client reconnection jitter spread across 30–60s (`terminationGracePeriodSeconds: 75`) without thundering herd spikes or Redis CPU `> 60%`.<br>• Production Helm cutover (`deployments/helm/`) and multi-stage distroless Dockerfile (`gcr.io/distroless/static:nonroot`, `GOMEMLIMIT=1800MiB`, `GOGC=80`). | ⏳ Planned |

---

## Completed Units

- **Repository Governance & CI/CD Foundation**:
  - **Git Ignore Configuration (`.gitignore`)**:
    - Excluded `tasks/` specification directory, Go binaries/build artifacts, test profiles, `.env` secrets, and private keys.
  - **GitHub Actions CI Workflow (`.github/workflows/ci.yml`)**:
    - Configured automated jobs for `lint-and-typecheck` (Go 1.23+, `gofmt -s`, `go vet`, `golangci-lint`), `security-and-logging-audit` (`.gitignore` verification, unstructured logging check, raw secret/token redaction audit, `govulncheck`), `openapi-and-doc-api-sync` (OpenAPI 3.1 & `.ai/doc-api.md` contract synchronization), `test-suites` (`go test -v -race`), and `architecture-guardrails`.
  - **AI Context & Architecture Alignment (`.ai/`, `.ai.md`, `GEMINI.MD`)**:
    - Realigned `.ai.md` and all 8 `.ai/` documents (`project-overview.md`, `architecture.md`, `doc-api.md`, `code-standards.md`, `security.md`, `ai-workflow-rules.md`, `agents.md`, `progress-tracker.md`) to **`rag-notification-dispatch-daemon`** based on `RAG Notification Dispatch Daemon Service Implementation Plan.pdf` and the 20 concepts across 5 architectural layers in `Production Grade Backend Concepts.pdf`.
    - Created `GEMINI.MD` referencing `.ai/agents.md` and the full `.ai/` directory, enforcing Continuous OpenAPI 3.1 / Doc API updates, Secured Logging, and Git Commit Standards.

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
