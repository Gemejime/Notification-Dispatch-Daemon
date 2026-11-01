# Security Context - RAG Notification Dispatch Daemon

## Threat Model & Trust Boundaries

1. **Strict Separation of Dispatch and External HTTP Egress (ADR-001)**:
   - The `rag-notification-dispatch-daemon` operates inside the internal control plane and **NEVER** executes outbound HTTP POST webhook requests directly to customer-supplied URLs.
   - All external webhook HTTP delivery, retry backoff, and Server-Side Request Forgery (SSRF) firewalling are strictly delegated to `rag-webhook-delivery-worker` running in an isolated egress DMZ.
2. **Per-Tenant Push Isolation (AuthN & AuthZ)**:
   - Every SSE connection to `/v1/notifications/stream` must authenticate via a short-lived signed JWT or channel bearer token.
   - The authenticated `tenant_id` and `user_id` claims govern which Redis presence keys (`rag:notify:presence:{tenant_id}:{user_id}`) and in-memory subscription rules apply. Cross-tenant event routing or replay buffer inspection (`Last-Event-ID`) is strictly blocked.
3. **Slow-Loris & Connection Exhaustion Defense**:
   - Malicious or stalled clients attempting to exhaust pod memory via throttled TCP receive windows are isolated behind bounded 256-event ring buffers (`ring_buffer.go`) and forcibly evicted if buffer saturation persists for $> 5\text{ seconds}$.
   - Reconnect and `Last-Event-ID` replay backfill queries (`fetch_replay_events.go`) are governed by token-bucket rate limiters to prevent Redis CPU exhaustion during reconnection storms.
4. **Cryptographic Webhook Task Signing & Secret Isolation**:
   - Outbound webhook tasks enqueued to Redis Streams / RabbitMQ (`dispatch_webhook_task.go`) carry KMS/Vault secret handles (`secret_ref`), deterministic idempotency keys (`task_id = sha256(tenant_id + event_id + target_id)`), and signing metadata (`X-RAG-Signature-256`) so `rag-webhook-delivery-worker` in the egress DMZ can compute HMAC-SHA256 signatures over `<timestamp>.<payload_json>` without ever exposing plaintext signing keys in daemon logs or public APIs.
5. **Distroless Non-Root Container Hardening**:
   - Packaged using `gcr.io/distroless/static:nonroot` (`runAsNonRoot: true`, UID `65532`, `readOnlyRootFilesystem: true`, dropped Linux capabilities `ALL`, `ulimit -n 65535`).

---

## Secured Logging Invariants

1. **Zero Secret or Token Leakage**:
   - Channel bearer tokens, JWTs, webhook HMAC-SHA256 secrets, PostgreSQL/PgBouncer credentials, and Redis passwords must **NEVER** be emitted to stdout, stderr, or telemetry spans.
2. **Confidential Payload Redaction**:
   - CDC notification payloads may reference sensitive document titles, user emails, or query snippets. Structured JSON logs (`log/slog`) must log only metadata identifiers (`event_id`, `tenant_id`, `user_id`, `connection_id`, `event_type`, `severity`, `payload_bytes_len`, `write_latency_ms`) and redact raw payload bodies.
3. **Sanitized Error Responses**:
   - HTTP and SSE error responses must return uniform, sanitized error structures without leaking Go stack traces, internal SQL/Redis key structures, or pod IP addresses.
4. **Automated Redaction Enforcement**:
   - The `log/slog` logger initialization must configure `ReplaceAttr` and `slog.LogValuer` implementations so that any struct representing credentials, tokens, or `WebhookTarget` secrets automatically renders as `"[REDACTED]"`.

---

## Secrets & Configuration Management

- Follow 12-Factor App methodology: all environment-specific configuration and credentials are injected via environment variables or Kubernetes Secret mounts.
- `.env` files, private keys (`*.pem`, `*.key`), and `tasks/` reference blueprints are strictly excluded via `.gitignore`.
- Commit only `.env.example` templates with placeholder values.

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
