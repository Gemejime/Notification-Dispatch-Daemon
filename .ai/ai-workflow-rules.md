# AI Workflow Rules - RAG Notification Dispatch Daemon

## Development Approach

Build **`rag-notification-dispatch-daemon`** incrementally following a strict spec-driven workflow:

- The specification documents in `tasks/` (specifically `RAG Notification Dispatch Daemon Service Implementation Plan.pdf` and `Production Grade Backend Concepts.pdf`) and context files in [`.ai/`](.) define all system invariants, hexagonal package boundaries, SLIs/SLOs, and operational guardrails.
- Adhere strictly to the hexagonal Go 1.23+ architecture (`internal/domain`, `internal/application`, `internal/infrastructure`, `cmd/notification-daemon`) and architectural decision records (ADR-001 through ADR-005) documented in [`.ai/architecture.md`](architecture.md).

---

## Human-in-the-Loop Rules

- **Plan First**: Propose a clear implementation plan in `prompts/<task-name>.md` before making major architectural changes, and ask for clarification if requirements are ambiguous.
- **Destructive Action Guards**: Always request human confirmation before executing destructive operations, dropping tables/streams, or altering git history.
- **Demonstrate & Verify**: Provide clear verification steps, unit/race test output, and OpenAPI sync confirmation after completing each work unit.

---

## Non-Negotiable Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: [`.ai/doc-api.md`](doc-api.md), `api/openapi/openapi.yaml`, and OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`) **MUST** be updated on **every code creation or modification**, no matter how small the change is.
2. **Secured Logging**: All logs emitted by the daemon must be structured JSON (`log/slog`). Strictly redact JWT/channel bearer tokens, webhook HMAC-SHA256 signing secrets, database/Redis credentials, and sensitive notification payload bodies before emission.
3. **Separation of Dispatch and Egress Delivery (ADR-001)**: The daemon manages CDC event ingestion, SSE streaming, and webhook task enqueueing (`P99 <= 15ms`). Outbound HTTP POST execution and SSRF filtering are strictly delegated to `rag-webhook-delivery-worker`.
4. **SSE Unification & Edge Proxy Guardrails (ADR-002)**: Serve persistent streams exclusively via SSE (`text/event-stream; charset=utf-8`) with `X-Accel-Buffering: no`, `Cache-Control: no-cache`, and immediate `http.Flusher` `: ping\n\n` frames every 15s.
5. **Targeted Node Routing Over Global Redis Fan-Out (ADR-003)**: Route events to specific pods (`pod.{node_id}.events`) via the Redis presence directory rather than blind global Pub/Sub broadcast.
6. **In-Memory Subscription Caching & Zero Hot-Path SQL (ADR-004)**: Evaluate routing rules and suppressions strictly via pod-local read memory (`subscription_cache.go`) invalidated within `<= 100ms` via `tenant.config.updated`. Never query PostgreSQL/PgBouncer on the hot CDC ingestion path.
7. **Bounded Ring Buffers & Slow Consumer Eviction**: Isolate every SSE connection behind a 256-event ring buffer (`ring_buffer.go`) that drops unread non-terminal progress frames on saturation while retaining terminal transitions, and evicts streams saturated for `> 5s`.
8. **Staggered Draining on SIGTERM (ADR-005)**: Reject new connections (`/ready` returns `503`), emit SSE `retry:` directives, and stagger active socket closures across a 30–60s randomized jitter window (`terminationGracePeriodSeconds: 75`).
9. **Pod Memory & GC Ceiling**: Run Go 1.23+ with `GOMEMLIMIT=1800MiB` and `GOGC=80` inside `2GiB` pod memory limits to sustain `>= 20,000` concurrent connections per pod (`>= 100,000` cluster-wide) without OOM kills.
10. **Git Commit Standards**:
    - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
    - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
      - `feat:` for new features
      - `fix:` for bug fixes
      - `refactor:` for code refactoring
      - `docs:` for documentation updates
      - `tests:` for tests addition and modification
      - `chore:` for maintenance tasks and environment setup

---

## Pre-Commit Verification Checklist

Before committing code, submitting changes, or marking a task complete:

1. **Formatting & Linting**: Run `gofmt -s -w .`, `go vet ./...`, and `golangci-lint run`.
2. **Automated Tests & Race Detector**: Run `go test -v -race ./internal/... ./test/unit/...`.
3. **Secured Logging Audit**: Verify zero unstructured `fmt.Print*` / `log.Print*` calls and zero raw secret/token/PII emissions in `log/slog`.
4. **Doc API & OpenAPI Sync**: Confirm [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` reflect any created or modified endpoints, schemas, headers, Protobuf messages, or configuration variables.
5. **Progress Tracker**: Update [`.ai/progress-tracker.md`](progress-tracker.md) with completed items.
