# Code Standards - RAG Notification Dispatch Daemon

## General Engineering Principles

1. **Hexagonal / Clean Layer Isolation**:
   - `internal/domain/` (`entity`, `router`, `repository` ports) must contain pure Go business logic with **zero** imports of external I/O drivers (no `pgx`, `go-redis`, `kafka-go`, or `net/http`).
   - `internal/application/` (`command`, `query`) orchestrates domain logic through interface ports.
   - `internal/infrastructure/` implements `cache`, `persistence`, `queue`, and `transport` adapters.
2. **Zero SQL Reads on Hot Path (ADR-004)**:
   - CDC event evaluation (`process_cdc_event.go`) must read exclusively from the pod-local in-memory subscription cache (`subscription_cache.go`). Synchronous PostgreSQL queries during event ingestion are strictly forbidden.
3. **Zero Intermediary Serialization Overhead**:
   - SSE client event payloads must serialize directly to JSON byte buffers (`[]byte`) without unnecessary Protobuf-to-struct-to-JSON re-marshaling on the per-socket write path.
4. **Root-Cause Concurrency Governance**:
   - Never block the CDC ingestion consumer or node router on a slow client socket. Every SSE connection must write to its own isolated 256-event ring buffer (`ring_buffer.go`) using non-blocking `select` semantics.

---

## Go 1.23+ & Code Quality Standards

- **Runtime Target**: Go 1.23+ configured with `GOMEMLIMIT=1800MiB` and `GOGC=80`.
- **Formatting & Linting**:
  - Format all code with `gofmt -s -w .` and `goimports`.
  - Pass `go vet ./...` and `golangci-lint run` with zero warnings or errors.
- **Race Detection**:
  - All unit and integration tests must pass with the Go race detector enabled: `go test -race ./...`.
- **Context Propagation & Timeouts**:
  - Pass `context.Context` as the first parameter of every application command, query, and infrastructure method.
  - Enforce explicit deadlines on Redis operations ($\le 5\text{ms}$ on hot path), webhook queue publishing ($\le 15\text{ms}$), and graceful shutdown draining ($30\text{–}60\text{s}$ jitter within $75\text{s}$ pod termination grace period).
- **Error Handling**:
  - Wrap errors with context using `fmt.Errorf("...: %w", err)`.
  - Never expose raw internal errors or stack traces to HTTP/SSE clients; sanitize responses at the transport boundary.

---

## Concurrency, Ring Buffer & Connection Safety Rules

1. **Memory Footprint Protection**:
   - Maintain per-socket footprint at $\approx 14\text{–}16\text{KiB}$ ($8\text{KiB}$ kernel TCP buffers, $4\text{KiB}$ goroutine stack, $2\text{KiB}$ ring buffer) so $20,000$ idle connections consume $\approx 320\text{MiB}$ and peak active bursts stay safely below $1.6\text{GiB}$ RSS ($2\text{GiB}$ container limit).
2. **Slow Consumer Mitigation (`ring_buffer.go`)**:
   - Isolate every client stream behind a bounded channel/ring buffer of capacity **256 events**.
   - When the buffer reaches 256 items, drop unread non-terminal progress frames (`is_terminal == false`) and increment `notification_dropped_frames_total`, while preserving terminal state transitions (`is_terminal == true`).
   - If a client's buffer remains saturated continuously for $> 5\text{ seconds}$, terminate and evict the connection cleanly to protect pod memory.
3. **Edge Proxy Guardrails (`stream_handler.go` & `flusher.go`)**:
   - Immediately write headers `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control: no-cache, no-transform`, `Connection: keep-alive`, and `X-Accel-Buffering: no`.
   - Emit `: ping\n\n` keepalive comment frames every **15 seconds** and immediately call `http.Flusher.Flush()` to prevent Envoy/ALB/Cloudflare idle 60s timeouts.
4. **Staggered Draining on SIGTERM (ADR-005)**:
   - On `SIGTERM`, flip readiness to `503`, emit SSE `retry:` directives, and stagger active socket closures using randomized jitter over a **30–60 second** window.

---

## Secured Logging Invariants

- **Structured JSON Logging (`log/slog`)**:
  - All logs must be emitted as single-line machine-parseable JSON via Go's standard `log/slog` `JSONHandler`.
  - Unstructured `fmt.Println`, `fmt.Printf`, or `log.Printf` calls are forbidden in application/domain/infrastructure packages.
- **Mandatory Correlation Fields**:
  - Include `trace_id`, `tenant_id`, `node_id`, `event_id`, and `connection_id` (where applicable) on log records.
- **Strict Redaction (Secured Logging)**:
  - **NEVER** log raw JWT/Bearer tokens, channel authorization tokens, webhook HMAC-SHA256 signing secrets, PostgreSQL/Redis passwords, or raw user/document PII inside notification payloads.
  - Implement custom `slog.LogValuer` or `ReplaceAttr` redaction hooks that mask sensitive keys (`authorization`, `token`, `secret`, `password`, `hmac_key`) with `"[REDACTED]"` before serialization.

---

## Doc API & OpenAPI Mandatory Maintenance

- **Continuous Synchronization**: Whenever any endpoint, query parameter, SSE frame schema, Protobuf message, Redis key pattern, or configuration variable is created or modified, [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` **MUST** be updated in the exact same step, no matter how small the code modification is.

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
