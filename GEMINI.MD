# GEMINI.MD - Master AI Engineering Directive (`rag-notification-dispatch-daemon`)

This file is the authoritative system directive for AI coding agents working on the **`rag-notification-dispatch-daemon`** (Notification Dispatch Daemon) microservice.

## 1. Mandatory Context & Persona References

Before planning, creating, or modifying any code, configuration, or documentation in this repository, you **MUST** read and adhere to [`.ai/agents.md`](.ai/agents.md), [`.ai.md`](.ai.md), and the entire [`.ai/`](.ai/) directory:

1. **[`.ai/agents.md`](.ai/agents.md)** — Agent persona (**Principal Distributed Systems & Go Backend Engineer**), system role, operational invariants, and step-by-step execution protocol.
2. **[`.ai/project-overview.md`](.ai/project-overview.md)** — Service mission, decoupled CDC outbox ingestion, high-density multiplexed SSE engine, asynchronous webhook task enqueueing, scope/exclusions, and SLIs/SLOs ($P99 \le 50\text{ms}$ dispatch, $\ge 100,000$ cluster connections, $P99 \le 15\text{ms}$ webhook queue hand-off, $99.99\%$ availability).
3. **[`.ai/architecture.md`](.ai/architecture.md)** — 5-Layer Production-Grade Backend Architecture (API & Routing, Data Persistence & State, Architecture & Scale, Security & Identity, Observability & Quality), Hexagonal Go 1.23+ folder structure (`cmd/`, `api/`, `internal/domain`, `internal/application`, `internal/infrastructure`, `deployments/`, `test/`), and ADRs (ADR-001 through ADR-005).
4. **[`.ai/doc-api.md`](.ai/doc-api.md)** — Complete OpenAPI 3.1 specification (`/v1/notifications/stream`, `/v1/notifications/sessions`, `/v1/notifications/subscriptions/invalidate`, `/docs`, `/redoc`, `/api/docs`, `/openapi.json`, `/healthz`, `/ready`, `/metrics`), Protobuf v3 contracts (`notification_event.proto`, `webhook_task.proto`), Redis 7.2 backplane schemas, PostgreSQL 16+ cold-path & journal schemas (`tenant_notification_subscriptions`, `tenant_webhook_targets`, `notification_event_journal`), Prometheus metrics/alerts, and 12-Factor environment variable contracts.
5. **[`.ai/code-standards.md`](.ai/code-standards.md)** — Go 1.23+ engineering standards, `GOMEMLIMIT=1800MiB` & `GOGC=80`, 256-event ring buffer drop policies, 15s SSE `: ping\n\n` flusher rules, `X-Accel-Buffering: no`, and race-free concurrency requirements.
6. **[`.ai/security.md`](.ai/security.md)** — Threat model, per-tenant push isolation, SSRF delegation to `rag-webhook-delivery-worker`, HMAC-SHA256 webhook signing metadata, distroless non-root containerization (`gcr.io/distroless/static:nonroot`), and **Secured Logging** redaction rules.
7. **[`.ai/ai-workflow-rules.md`](.ai/ai-workflow-rules.md)** — Spec-driven workflow rules, human-in-the-loop checkpoints, and pre-commit verification gates.
8. **[`.ai/progress-tracker.md`](.ai/progress-tracker.md)** — 4-phase implementation roadmap, completed work units, ADRs, and operational runbooks.

---

## 2. Continuous Doc API & OpenAPI 3.1 Synchronization Invariant

- **Zero-Drift Requirement**: The **"Doc API"** ([`.ai/doc-api.md`](.ai/doc-api.md)) and the **OpenAPI 3.1 specification** (`api/openapi/openapi.yaml` and HTTP endpoints `/docs`, `/redoc`, `/api/docs`, `/openapi.json`) **MUST ALWAYS BE UPDATED** whenever code is created or modified—**no matter how little the created or modified code is**, if need be.
- Any addition or change to HTTP/SSE routes, headers, query parameters, response schemas, Protobuf v3 messages, Redis keys, PostgreSQL tables, Prometheus metrics, or environment variables requires an immediate, synchronous update to [`.ai/doc-api.md`](.ai/doc-api.md) and `api/openapi/openapi.yaml` in the same work unit.

---

## 3. Secured Logging Invariant

- **Structured JSON Output Only**: All application, domain, and infrastructure logs must use Go's structured `log/slog` `JSONHandler`. Unstructured `fmt.Print*` or `log.Print*` statements are strictly prohibited.
- **Strict Redaction Before Emission**:
  - **NEVER** log raw JWTs, channel authorization bearer tokens, webhook HMAC-SHA256 signing secrets, PostgreSQL/PgBouncer credentials, Redis passwords, or sensitive PII/document text inside notification payloads.
  - Enforce automatic redaction via `slog.LogValuer` and `ReplaceAttr` hooks so sensitive fields (`authorization`, `token`, `secret`, `password`, `hmac_key`) always serialize as `"[REDACTED]"`.
  - Emit only safe operational metadata and correlation identifiers (`environment`, `trace_id`, `tenant_id`, `user_id`, `node_id`, `event_id`, `connection_id`, `event_type`, `severity`, `payload_bytes_len`, `write_latency_ms`).
- **Sanitized Client Errors**: Never leak internal stack traces, SQL queries, or Redis topology details in HTTP/SSE error responses.

---

## 4. Core Architectural & Runtime Guardrails

- **Runtime & Memory Ceiling**: Go 1.23+ with `GOMEMLIMIT=1800MiB` and `GOGC=80` inside `2GiB` pod memory limits (`1GiB` request) and `500m`/`2000m` CPU.
- **Zero Hot-Path SQL Reads (ADR-004)**: CDC event routing (`process_cdc_event.go`) evaluates exclusively against the pod-local in-memory subscription cache (`subscription_cache.go`), invalidated asynchronously within $\le 100\text{ms}$ via `tenant.config.updated`.
- **Targeted Pod Routing (ADR-003)**: Route events to recipient pods over Redis `pod.{node_id}.events` via `presence_registry.go` rather than global Pub/Sub broadcast.
- **Bounded Per-Client Ring Buffers**: 256-event non-blocking ring buffer (`ring_buffer.go`) per SSE stream; drop intermediate non-terminal frames (`is_terminal == false`) when full, retain terminal transitions (`is_terminal == true`)—or evict the connection immediately and rely on `Last-Event-ID` recovery if all 256 buffered frames are terminal and another terminal frame arrives—and evict stalled consumers after $> 5\text{s}$ of continuous saturation.
- **Edge Proxy Bypass (ADR-002)**: Enforce `X-Accel-Buffering: no`, `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control: no-cache`, and immediate `http.Flusher` `: ping\n\n` heartbeats every 15s.
- **Staggered Graceful Draining (ADR-005)**: On `SIGTERM`, flip `/ready` to `503`, emit SSE `retry:` directives, and spread socket closures across a 30–60s randomized jitter window (`terminationGracePeriodSeconds: 75`).
- **SSRF & Egress Isolation (ADR-001)**: Never execute outbound webhook HTTP POST requests in this daemon; enqueue signed tasks (`P99 <= 15ms`) for `rag-webhook-delivery-worker`.

---

## Git Commit Standards
- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
