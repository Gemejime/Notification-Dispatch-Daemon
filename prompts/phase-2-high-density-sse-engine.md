# Implementation Plan: Phase 2 — High-Density SSE Engine (`rag-notification-dispatch-daemon`)

## 1. Objectives & Architectural Scope (Pressure-Tested Against `tasks/` & `.ai/`)

Implement **Phase 2: High-Density SSE Engine** from `RAG Notification Dispatch Daemon Service Implementation Plan.pdf`, `Production Grade Backend Concepts.pdf`, and `.ai/progress-tracker.md`, adhering strictly to `GEMINI.MD`, `.ai/architecture.md` (5-Layer Production-Grade Backend Architecture, ADRs 001–005), `.ai/doc-api.md`, `.ai/code-standards.md`, and `.ai/security.md`:

1. **Bounded Per-Client Ring Buffer (`internal/infrastructure/transport/hub/ring_buffer.go`)**:
   - Isolate every SSE client stream behind a non-blocking ring buffer of capacity **256 events** (`SSE_RING_BUFFER_SIZE=256`, $\approx 2\text{KiB}$ baseline footprint inside the $\approx 14\text{–}16\text{KiB}$ per-socket budget).
   - **Slow-Consumer Drop & Terminal Retention Policy**:
     - When buffered items `< 256`, enqueue in $O(1)$ without blocking the caller and signal a non-blocking `size=1` wakeup channel (`notifyCh chan struct{}`).
     - When saturated at `256` items:
       - Record `SaturatedSince` timestamp on first saturation and clear `SaturatedSince` whenever buffered items drop below `256`.
       - **Non-Terminal Frame (`is_terminal == false`)**: Drop incoming non-terminal progress frame and increment `notification_dropped_frames_total{tenant_id, reason="ring_buffer_saturated"}`.
       - **Terminal Frame (`is_terminal == true`)**: Evict the oldest unread non-terminal frame (`is_terminal == false`) in the ring buffer while preserving FIFO ordering of remaining frames to make room for the terminal transition, and increment `notification_dropped_frames_total{tenant_id, reason="terminal_frame_preemption"}`.
       - **All-256-Terminal Overflow Guard**: If all 256 buffered frames are already terminal (`is_terminal == true`) and another terminal frame arrives, immediately mark the buffer for connection eviction (`reason="all_terminal_eviction"`) and increment `notification_dropped_frames_total{tenant_id, reason="all_terminal_eviction"}` so the client reconnects with `Last-Event-ID`, preserving the 256-event bound and non-blocking writes.
       - **Continuous Saturation Eviction (`> 5s`)**: If `SaturatedSince` exceeds `SSE_SLOW_CONSUMER_TIMEOUT` (default `5s`), evict the connection (`reason="slow_consumer_evicted"`), enforced both on `Push` and via periodic background sweep so idle-traffic stalled buffers are evicted after `5s` even if no further events arrive.
2. **Local Node Session Manager (`internal/infrastructure/transport/hub/connection_hub.go`)**:
   - Maintain thread-safe pod-local active socket registries indexed by `tenant_id`, `user_id`, and `connection_id` (supporting $\ge 20,000$ connections per pod / $\ge 100,000$ cluster-wide within `GOMEMLIMIT=1800MiB` and `GOGC=80`, steady-state RSS $\le 1.6\text{GiB}$).
   - Update `notification_active_sse_connections{tenant_id, node_id}` gauge on registration and termination.
   - Evaluate `Connection.AcceptsEvent(event)` (strict per-tenant boundary, strict `user_id` isolation when `event.UserID != ""` so user-targeted notifications never leak to anonymous/other-user tenant streams, and optional `severity_filter` rank comparison) and push frames into matching client `RingBuffer` instances.
   - Implement `command.NodeEventDispatcher` (`DispatchSSEFrame`) for local single-node delivery in Phase 2 (seamlessly wrapped by `TargetedRouter` in Phase 3).
   - Periodically sweep and evict slow consumers whose ring buffers remain continuously saturated for `> 5s` (`SSE_SLOW_CONSUMER_TIMEOUT`).
   - Implement **Per-Connection Staggered Graceful Draining on SIGTERM (`ADR-005`)**: assign each active SSE stream its own independent randomized drain delay across the `[DRAIN_JITTER_MIN, DRAIN_JITTER_MAX]` (`30s–60s`) window (`terminationGracePeriodSeconds: 75`), flushing a final SSE `retry: <jitter_ms>\n\n` directive frame before closing each socket to prevent 20,000-client thundering herd reconnection spikes.
   - Support deterministic cursor-based pagination (`ListActiveSessions`) for `GET /v1/notifications/sessions`.
3. **Edge Proxy Bypass Flusher & Slow-Loris Write Deadline Guard (`internal/infrastructure/transport/sse/flusher.go`)**:
   - Enforce `ADR-002` response headers on `/v1/notifications/stream`:
     - `Content-Type: text/event-stream; charset=utf-8`
     - `Cache-Control: no-cache, no-transform`
     - `Connection: keep-alive`
     - `X-Accel-Buffering: no`
   - Emit `: ping\n\n` keepalive comment frames immediately on stream establishment and every `15s` (`SSE_HEARTBEAT_INTERVAL`) via `http.Flusher.Flush()` to prevent Envoy/ALB/Cloudflare 60s idle proxy timeouts (`Runbook 3`).
   - Enforce per-write socket deadlines via `http.NewResponseController(w).SetWriteDeadline(...)` (bounded by `SSE_SLOW_CONSUMER_TIMEOUT`) so a Slow-Loris client with a zeroed TCP receive window cannot block a stream goroutine indefinitely inside `w.Write()` or `Flush()`.
   - Record per-socket write latency in `notification_client_write_latency_seconds{node_id}` (target $P99 \le 10\text{ms}$; alert threshold $P99 > 50\text{ms}$ over $3\text{m}$).
4. **Non-Blocking SSE Stream Handler (`internal/infrastructure/transport/sse/stream_handler.go`)**:
   - Serve `GET /v1/notifications/stream` with authentication context validation, `severity_filter` query validation (`INFO`, `WARNING`, `ERROR`, `CRITICAL`), pluggable `Last-Event-ID` reconnect backfill port with high-watermark deduplication against live ring-buffer frames, `15s` keepalive heartbeats (with optional presence TTL refresh hook), slow-consumer eviction checks, and per-stream `SIGTERM` staggered drain handling.
5. **Application Connection Lifecycle Commands & Queries (`internal/application/`)**:
   - `internal/application/command/register_connection.go`: Orchestrate new SSE session registration across local `ConnectionHub` and a decoupled `PresenceRegistrar` port (nil-safe in Phase 2; backed by Redis `PresenceRegistry` in Phase 3).
   - `internal/application/command/terminate_connection.go`: Cleanly remove disconnected/evicted sessions from local `ConnectionHub` and the `PresenceRegistrar` port.
   - `internal/application/query/list_active_sessions.go`: Paginated query (`limit` 1..500, default 100, `cursor`) returning `ActiveSessionsResponse`.
6. **HTTP Router & Security Middleware (`internal/infrastructure/transport/http/`)**:
   - `internal/infrastructure/transport/http/middleware/middleware.go`: Correlation/Request ID injection, Bearer token / JWT authentication & tenant/user claim extraction, per-tenant authorization enforcement (`401 Unauthorized` / `403 Forbidden`), reconnect token-bucket rate limiting (`429 Too Many Requests`), sanitized panic recovery, and Secured Logging (`log/slog` with secret/token redaction).
   - `internal/infrastructure/transport/http/router.go`: Production HTTP mux wiring `/v1/notifications/stream`, `/v1/notifications/sessions`, `/v1/notifications/subscriptions/invalidate` (wired to `InvalidationSubscriber.BroadcastInvalidation` for `<= 100ms` cold-store reload & pub/sub broadcast), `/healthz`, `/ready`, `/metrics`, `/docs`, `/redoc`, `/api/docs`, and `/openapi.json`.

---

## 2. Files to Create / Modify

| File Path | Layer | Purpose |
| :--- | :--- | :--- |
| `internal/domain/entity/connection.go` | Domain | Harden `Connection.AcceptsEvent` user-isolation check so user-targeted events (`event.UserID != ""`) never leak to connections with mismatched or empty `UserID` |
| `internal/infrastructure/transport/hub/ring_buffer.go` | Infrastructure (Transport) | 256-event non-blocking per-client ring buffer with compact $\approx 2\text{KiB}$ memory layout, terminal-frame preemption, all-256-terminal overflow guard, and `> 5s` slow-consumer eviction |
| `internal/infrastructure/transport/hub/connection_hub.go` | Infrastructure (Transport) | Local node session registry, fan-out to local sockets, periodic slow-consumer sweep, cursor pagination, and per-connection staggered `SIGTERM` drain |
| `internal/infrastructure/transport/sse/flusher.go` | Infrastructure (Transport) | `http.Flusher` wrapper enforcing `X-Accel-Buffering: no`, `http.ResponseController` write deadlines, `: ping\n\n` heartbeats, and write latency telemetry |
| `internal/infrastructure/transport/sse/stream_handler.go` | Infrastructure (Transport) | Multiplexed `GET /v1/notifications/stream` HTTP handler with watermark-deduplicated backfill port, ring buffer loop, and 15s keepalive pings |
| `internal/application/command/register_connection.go` | Application (Command) | Registers new SSE client connections in local `ConnectionHub` and `PresenceRegistrar` port |
| `internal/application/command/terminate_connection.go` | Application (Command) | Cleans up terminated/evicted SSE connections in local `ConnectionHub` and `PresenceRegistrar` port |
| `internal/application/query/list_active_sessions.go` | Application (Query) | Cursor-paginated active session telemetry query handler |
| `internal/infrastructure/transport/http/middleware/middleware.go` | Infrastructure (HTTP) | AuthN/AuthZ tenant isolation, reconnect rate limiter, correlation ID, panic recovery, and Secured Logging middleware |
| `internal/infrastructure/transport/http/router.go` | Infrastructure (HTTP) | Unified REST, SSE, cache invalidation broadcast, operational probe, and OpenAPI 3.1 HTTP router |
| `cmd/notification-daemon/main.go` | Entry Point | Wire `ConnectionHub`, `SSEStreamHandler`, `Router`, and per-connection staggered socket draining on `SIGTERM` |
| `test/unit/phase2_sse_engine_test.go` | Test | Unit, concurrency (`-race`), ring buffer drop/terminal-retention, slow-consumer `> 5s` sweep/eviction, per-connection staggered `SIGTERM` drain, and SSE streaming tests |
| `.ai/doc-api.md`, `api/openapi/openapi.yaml`, `.ai/progress-tracker.md` | Documentation | Continuous Doc API & OpenAPI 3.1 synchronization and Phase 2 progress tracking |

---

## 3. Pressure-Tested Failure Modes & Mitigations

1. **Cross-Phase Dependency Decoupling (`PresenceRegistrar` & `ReplayStreamer` Ports)**:
   - `register_connection.go`, `terminate_connection.go`, and `stream_handler.go` interact with Redis presence and `Last-Event-ID` replay backfill, whose concrete adapters (`presence_registry.go` and `fetch_replay_events.go`) are implemented in Phase 3. Defining clean application ports (`PresenceRegistrar`, `ReplayStreamer`) with nil-safe fallback in Phase 2 guarantees Phase 2 compiles and passes all tests independently while allowing zero-friction Phase 3 wiring.
2. **Per-Socket Memory Ceiling (`~2KiB` Ring Buffer inside `GOMEMLIMIT=1800MiB`)**:
   - Allocating heavy structs or per-frame channels for 256 slots across 20,000 sockets would consume cientos of megabytes of idle heap. `RingBuffer` uses a compact circular slice of `{EventID string, Frame []byte, IsTerminal bool}` and a single `chan struct{}` (`cap=1`) wakeup signal so 20,000 idle connections stay within the $\approx 320\text{MiB}$ pod baseline (`RAG Notification Dispatch Daemon Service Implementation Plan.pdf` Section 4).
3. **Idle-Traffic & Zero-TCP-Window Slow-Loris Eviction (`> 5s`)**:
   - If a client's 256-event buffer saturates and no further events arrive for that tenant, checking `SaturatedSince` only inside `Push()` would fail to evict the stalled connection. `ConnectionHub` / `stream_handler.go` runs a periodic sweep ticker to evict connections saturated for `> 5s`, and `flusher.go` sets per-write deadlines via `http.ResponseController.SetWriteDeadline` so stalled TCP sockets cannot hang `w.Write()` / `Flush()`.
4. **Per-Connection Staggered `SIGTERM` Drain vs. Single-Timestamp Thundering Herd (`ADR-005`)**:
   - Waiting a single pod-wide random delay and then closing all 20,000 connections simultaneously would still trigger a 20,000-client thundering herd at that instant. `ConnectionHub.DrainWithJitter` assigns each active connection its own randomized delay in `[30s, 60s]` and emits a final `retry: <ms>\n\n` SSE frame before closing each stream, while `cmd/notification-daemon/main.go` flips `/ready` to `503` immediately and waits for the staggered drain before `srv.Shutdown`.
5. **User-Targeted Event Isolation (`Connection.AcceptsEvent`)**:
   - When `event.UserID != ""`, `Connection.AcceptsEvent` requires `c.UserID == event.UserID` so a user-scoped notification is never delivered to a connection with an empty or different `UserID` within the same tenant.

---

## 4. Security & Concurrency Guardrails

- **Secured Logging**: All HTTP middleware, SSE stream lifecycle logs, and connection eviction logs use `log/slog` `JSONHandler` with `ReplaceAttr` redaction. Raw `Authorization` headers, Bearer JWTs, and notification payload text are never logged.
- **Per-Tenant & Per-User Push Isolation**: A client authenticated for `tenant_a` can never receive SSE frames for `tenant_b` or query `/v1/notifications/sessions?tenant_id=tenant_b` (`403 Forbidden`), and user-targeted frames (`user_id != ""`) are isolated strictly to matching `user_id` streams.
- **Non-Blocking Hot Path**: `RingBuffer.Push` never blocks; slow TCP readers cannot stall `ConnectionHub` or `CDCConsumer`.

---

## 5. Verification & Git Commit Plan

1. Run `gofmt -s -w .` and `go vet ./...`.
2. Run unit and concurrency tests with the race detector:
   `GOMEMLIMIT=1800MiB GOGC=80 go test -v -race ./cmd/... ./internal/... ./test/unit/...`
3. Verify OpenAPI 3.1 & `.ai/doc-api.md` synchronization and Secured Logging redaction audit.
4. Commit Phase 2 in at least two Conventional Commits (`feat:` and `tests:` / `docs:`).

