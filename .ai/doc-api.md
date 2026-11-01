# Microservice Documented API ("Doc API") - RAG Notification Dispatch Daemon

> **MANDATORY INVARIANT**: This document specifies the complete HTTP/SSE REST contracts, OpenAPI 3.1 specifications, Protobuf v3 contracts, Redis backplane schemas, and PostgreSQL cold-path schemas for **`rag-notification-dispatch-daemon`**.
>
> **API ACCESSIBILITY REQUIREMENT**: The OpenAPI 3.1 specification MUST be accessible via HTTP endpoints hosted on the daemon's HTTP server (default port `8080`) and synchronized with `api/openapi/openapi.yaml`:
> - `GET /docs` — Interactive OpenAPI 3.1 Documentation via **Swagger UI**.
> - `GET /redoc` — Interactive OpenAPI 3.1 Documentation via **ReDoc**.
> - `GET /openapi.json` & `GET /api/docs` — Machine-readable **OpenAPI 3.1 JSON/YAML Specification**.
>
> **CONTINUOUS DOC API UPDATE INVARIANT**: This file ([`.ai/doc-api.md`](doc-api.md)) and the OpenAPI 3.1 specification (`api/openapi/openapi.yaml`) **MUST** be updated on **every code creation or modification**, no matter how small the change is.

---

## 1. HTTP, SSE & Operational Endpoints Overview

| Endpoint | Method | Content-Type | Auth / Scope | Description |
| :--- | :--- | :--- | :--- | :--- |
| `/v1/notifications/stream` | `GET` | `text/event-stream; charset=utf-8` | Bearer Token (`tenant_id`, `user_id`) | Multiplexed unidirectional SSE push channel with `X-Accel-Buffering: no`, 15s `: ping\n\n` heartbeats, and `Last-Event-ID` replay backfill |
| `/v1/notifications/sessions` | `GET` | `application/json` | Bearer Token (Admin / Tenant Scope) | Query active SSE session counts and targeted node presence (`list_active_sessions.go`) |
| `/v1/notifications/subscriptions/invalidate` | `POST` | `application/json` | Internal mTLS / Service Token | Trigger immediate pod-local cache invalidation for a `tenant_id` and broadcast `tenant.config.updated` |
| `/docs` | `GET` | `text/html` | Internal / Public Doc | Interactive Swagger UI powered by OpenAPI 3.1 |
| `/redoc` | `GET` | `text/html` | Internal / Public Doc | Interactive ReDoc UI documentation |
| `/api/docs` | `GET` | `application/json` | Internal / Public Doc | Raw OpenAPI 3.1 JSON specification schema |
| `/openapi.json` | `GET` | `application/json` | Internal / Public Doc | Standard machine-readable OpenAPI 3.1 JSON specification |
| `/healthz` | `GET` | `application/json` | Public Probe | Liveness probe returning `{"status": "healthy"}` |
| `/ready` | `GET` | `application/json` | Public Probe | Readiness probe verifying subscription cache warm-up, Redis cluster reachability, and Kafka/Redis CDC consumer status (returns `503` during `SIGTERM` drain) |
| `/metrics` | `GET` | `text/plain; version=0.0.4` | Prometheus Scraper | Prometheus metrics scrape endpoint |

---

## 2. OpenAPI 3.1 Specification (`api/openapi/openapi.yaml` & `/api/docs`)

```yaml
openapi: 3.1.0
info:
  title: RAG Notification Dispatch Daemon API
  description: >-
    High-density Server-Sent Events (SSE) streaming, targeted Redis pod routing,
    in-memory subscription rule evaluation, and asynchronous webhook task enqueueing
    microservice (rag-notification-dispatch-daemon).
  version: 1.0.0
servers:
  - url: http://localhost:8080
    description: Local / Internal Pod HTTP & SSE Server
security:
  - bearerAuth: []
paths:
  /v1/notifications/stream:
    get:
      summary: Establish Persistent Server-Sent Events (SSE) Notification Stream
      operationId: streamNotifications
      tags:
        - SSE Streaming
      parameters:
        - name: Last-Event-ID
          in: header
          required: false
          description: >-
            Standard SSE watermark event ID for rate-limited reconnect backfill from Redis replay buffer.
          schema:
            type: string
            example: "01J9Z8K4M2N6P8Q0R2S4T6V8W0"
        - name: severity_filter
          in: query
          required: false
          description: Optional minimum severity filter (INFO, WARNING, ERROR, CRITICAL).
          schema:
            type: string
            enum: [INFO, WARNING, ERROR, CRITICAL]
      responses:
        "200":
          description: >-
            Persistent SSE stream opened. Emits immediate `: ping\n\n` every 15s and JSON-formatted
            domain notification frames. Sets `X-Accel-Buffering: no`.
          headers:
            Content-Type:
              schema:
                type: string
                example: "text/event-stream; charset=utf-8"
            Cache-Control:
              schema:
                type: string
                example: "no-cache, no-transform"
            Connection:
              schema:
                type: string
                example: "keep-alive"
            X-Accel-Buffering:
              schema:
                type: string
                example: "no"
          content:
            text/event-stream:
              schema:
                type: string
                example: |
                  : ping

                  id: 01J9Z8K4M2N6P8Q0R2S4T6V8W0
                  event: ingestion.milestone
                  retry: 5000
                  data: {"event_id":"01J9Z8K4M2N6P8Q0R2S4T6V8W0","tenant_id":"tnt_01H","event_type":"ingestion.milestone","severity":"INFO","is_terminal":false,"occurred_at":"2026-11-01T08:00:00Z","payload":{"document_id":"doc_99","stage":"CHUNK_COMPILATION","progress_pct":75}}
        "401":
          description: Missing or invalid channel authorization bearer token
        "429":
          description: Reconnect or backfill rate limit exceeded
        "503":
          description: Pod is draining connections (SIGTERM active); client should reconnect with jitter
  /v1/notifications/sessions:
    get:
      summary: List Active SSE Sessions and Node Presence Telemetry
      operationId: listActiveSessions
      tags:
        - Administration & Presence
      parameters:
        - name: tenant_id
          in: query
          required: true
          schema:
            type: string
        - name: limit
          in: query
          required: false
          schema:
            type: integer
            minimum: 1
            maximum: 500
            default: 100
        - name: cursor
          in: query
          required: false
          schema:
            type: string
      responses:
        "200":
          description: Paginated active session telemetry for the tenant
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ActiveSessionsResponse"
        "401":
          description: Unauthorized
        "403":
          description: Cross-tenant access forbidden
  /v1/notifications/subscriptions/invalidate:
    post:
      summary: Invalidate Pod-Local In-Memory Tenant Subscription Cache
      operationId: invalidateTenantCache
      tags:
        - Administration & Cache
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/CacheInvalidationRequest"
      responses:
        "202":
          description: Cache invalidation accepted and broadcast on tenant.config.updated
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/CacheInvalidationResponse"
  /healthz:
    get:
      summary: Liveness Probe
      operationId: getLiveness
      tags:
        - Probes & Observability
      security: []
      responses:
        "200":
          description: Go runtime and HTTP listener are alive
          content:
            application/json:
              schema:
                type: object
                required: [status]
                properties:
                  status:
                    type: string
                    example: "healthy"
  /ready:
    get:
      summary: Readiness Probe
      operationId: getReadiness
      tags:
        - Probes & Observability
      security: []
      responses:
        "200":
          description: Pod-local cache warmed, Redis cluster connected, CDC consumer active, not draining
          content:
            application/json:
              schema:
                type: object
                required: [status, subscription_cache, redis_backplane, cdc_consumer, draining]
                properties:
                  status:
                    type: string
                    example: "ready"
                  subscription_cache:
                    type: string
                    example: "warmed"
                  redis_backplane:
                    type: string
                    example: "connected"
                  cdc_consumer:
                    type: string
                    example: "consuming"
                  draining:
                    type: boolean
                    example: false
        "503":
          description: Pod is initializing, dependency unreachable, or SIGTERM graceful draining in progress
  /metrics:
    get:
      summary: Prometheus Metrics Scrape Endpoint
      operationId: getMetrics
      tags:
        - Probes & Observability
      security: []
      responses:
        "200":
          description: Prometheus text exposition format
          content:
            text/plain:
              schema:
                type: string
  /docs:
    get:
      summary: Interactive Swagger UI Documentation
      operationId: getSwaggerDocs
      tags:
        - Documentation
      security: []
      responses:
        "200":
          description: HTML Swagger UI
  /redoc:
    get:
      summary: Interactive ReDoc UI Documentation
      operationId: getRedocDocs
      tags:
        - Documentation
      security: []
      responses:
        "200":
          description: HTML ReDoc UI
  /api/docs:
    get:
      summary: Machine-Readable OpenAPI 3.1 JSON Specification
      operationId: getOpenApiDocs
      tags:
        - Documentation
      security: []
      responses:
        "200":
          description: OpenAPI 3.1 JSON schema
  /openapi.json:
    get:
      summary: Standard Machine-Readable OpenAPI 3.1 JSON Specification
      operationId: getOpenApiJson
      tags:
        - Documentation
      security: []
      responses:
        "200":
          description: OpenAPI 3.1 JSON schema
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
  schemas:
    SSENotificationFrame:
      type: object
      required:
        - event_id
        - tenant_id
        - event_type
        - severity
        - is_terminal
        - occurred_at
        - payload
      properties:
        event_id:
          type: string
          description: Monotonic ULID/UUID watermark used for Last-Event-ID replay
        tenant_id:
          type: string
        user_id:
          type: string
        event_type:
          type: string
          enum:
            - ingestion.milestone
            - index.blue_green_switched
            - chunk.overlay_conflict
            - evaluation.quality_alert
        severity:
          type: string
          enum: [INFO, WARNING, ERROR, CRITICAL]
        is_terminal:
          type: boolean
          description: >-
            If true (e.g. COMPLETED/FAILED), ring buffer retains this frame by evicting an unread non-terminal frame during slow-consumer saturation; if all 256 buffered frames are already terminal, the connection is evicted immediately and relies on Last-Event-ID recovery to preserve the 256-event bound and non-blocking writes.
        occurred_at:
          type: string
          format: date-time
        payload:
          type: object
          additionalProperties: true
    ActiveSessionsResponse:
      type: object
      required: [tenant_id, active_connections, sessions]
      properties:
        tenant_id:
          type: string
        active_connections:
          type: integer
        next_cursor:
          type: string
        sessions:
          type: array
          items:
            type: object
            required: [connection_id, user_id, node_id, connected_at, buffered_events]
            properties:
              connection_id:
                type: string
              user_id:
                type: string
              node_id:
                type: string
              connected_at:
                type: string
                format: date-time
              buffered_events:
                type: integer
                maximum: 256
    CacheInvalidationRequest:
      type: object
      required: [tenant_id, reason]
      properties:
        tenant_id:
          type: string
        reason:
          type: string
          example: "subscription_rule_updated"
    CacheInvalidationResponse:
      type: object
      required: [status, tenant_id, invalidated_at]
      properties:
        status:
          type: string
          example: "invalidated"
        tenant_id:
          type: string
        invalidated_at:
          type: string
          format: date-time
```

---

## 3. Protobuf v3 Contracts (`api/proto/v1/`)

### A. CDC Outbox Event Envelope (`api/proto/v1/notification_event.proto`)
Consumed from Kafka 3.7+ / Redis 7.2 Streams outbox topics produced by `rag-kb-catalog-service`, `rag-ingestion-coordinator`, and `rag-chunk-lifecycle-service`:

```protobuf
syntax = "proto3";

package rag.notification.v1;

option go_package = "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1;notificationv1";

enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_INFO = 1;
  SEVERITY_WARNING = 2;
  SEVERITY_ERROR = 3;
  SEVERITY_CRITICAL = 4;
}

message NotificationEventEnvelope {
  string event_id = 1;           // ULID watermark (maps to SSE id: field)
  string tenant_id = 2;          // Mandatory tenant isolation key
  string user_id = 3;            // Optional targeted user recipient
  string source_service = 4;     // e.g. "rag-ingestion-coordinator"
  string event_type = 5;         // e.g. "ingestion.milestone", "evaluation.quality_alert"
  Severity severity = 6;
  bool is_terminal = 7;          // True for terminal state transitions (retained on buffer saturation)
  int64 occurred_at_unix_ms = 8;
  string trace_id = 9;           // OpenTelemetry W3C trace correlation ID
  bytes payload_json = 10;       // Pre-validated JSON object bytes for zero-conversion SSE write path
}
```

### B. Downstream Webhook Delivery Task (`api/proto/v1/webhook_task.proto`)
Enqueued into Redis Streams / RabbitMQ (`rag:webhook:tasks`) for consumption by `rag-webhook-delivery-worker` (ADR-001: the dispatch daemon resolves the tenant's `secret_ref` reference and routing metadata in under $\le 15\text{ms}$, while `rag-webhook-delivery-worker` in the egress DMZ resolves the KMS/Vault secret, generates the `X-RAG-Signature-256` HMAC-SHA256 signature over `<timestamp>.<payload_json>`, executes HTTP POST with exponential backoff jitter, and enforces SSRF firewalling):

```protobuf
syntax = "proto3";

package rag.notification.v1;

option go_package = "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1;notificationv1";

message WebhookDeliveryTask {
  string task_id = 1;            // Deterministic idempotency key: sha256(tenant_id + event_id + target_id)
  string event_id = 2;           // Originating CDC outbox event_id
  string tenant_id = 3;          // Tenant isolation boundary
  string target_id = 4;          // Configured webhook target UUID
  string target_url = 5;         // Customer destination URL (validated/called by rag-webhook-delivery-worker)
  string secret_ref = 6;         // KMS/Vault reference for HMAC-SHA256 signing key (never raw secret in logs)
  string signature_header_name = 7; // Target HTTP signature header name (default: "X-RAG-Signature-256")
  int64 timestamp_unix_sec = 8;  // Task creation epoch timestamp for replay protection
  string event_type = 9;         // Domain event type (e.g. "ingestion.milestone")
  bytes payload_json = 10;       // Pre-serialized JSON payload bytes
  string trace_id = 11;          // OpenTelemetry W3C traceparent correlation ID
}
```

---

## 4. Redis 7.2 Cluster Backplane & PostgreSQL 16+ Cold Schemas

### A. Redis Targeted Routing & Replay Keys
1. **Presence Directory (`internal/infrastructure/cache/redis/presence_registry.go`)**:
   - Key: `rag:notify:presence:{tenant_id}:{user_id}` -> Hash / Set of `{connection_id: node_id}` with `TTL = 45s` (refreshed by 15s heartbeat loop).
   - Tenant-wide broadcast index: `rag:notify:tenant_nodes:{tenant_id}` -> Set of active `node_id`s hosting connections for `tenant_id`.
2. **Targeted Pod Routing Channel (`internal/infrastructure/transport/hub/targeted_router.go`)**:
   - Pub/Sub Channel: `pod.{node_id}.events` (ADR-003: prevents $O(N \times M)$ cluster-wide broadcast fan-out).
3. **Cache Invalidation Channel (`internal/infrastructure/cache/redis/invalidation_sub.go`)**:
   - Pub/Sub Channel: `tenant.config.updated` carrying JSON `{"tenant_id": "...", "updated_at_unix_ms": 1762000000000}`. All pods evict/reload local memory cache within $\le 100\text{ms}$.
4. **Bounded `Last-Event-ID` Replay Buffer (`internal/application/query/fetch_replay_events.go`)**:
   - Key: `rag:notify:replay:{tenant_id}` (Redis Stream `MAXLEN ~ 1000` with `TTL = 3600s`), queried with rate-limited `XRANGE` on client reconnect.

### B. PostgreSQL 16+ Cold-Path Configuration & Journal Tables (`internal/infrastructure/persistence/postgres/`)
> **CRITICAL INVARIANT (ADR-004)**: `tenant_notification_subscriptions` and `tenant_webhook_targets` are queried **ONLY** on cold start or asynchronous `tenant.config.updated` cache invalidation via PgBouncer. **Zero synchronous SQL reads** are permitted on the hot CDC ingestion path. `notification_event_journal` (`event_journal_repo.go`) is written asynchronously off the critical path or used as a cold fallback for audit/journal continuity.

```sql
CREATE TABLE IF NOT EXISTS tenant_notification_subscriptions (
    subscription_id UUID PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    event_type_pattern VARCHAR(128) NOT NULL,
    min_severity VARCHAR(16) NOT NULL DEFAULT 'INFO',
    sse_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    webhook_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    suppressed BOOLEAN NOT NULL DEFAULT FALSE,
    rate_limit_rps INTEGER NOT NULL DEFAULT 100,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_tenant ON tenant_notification_subscriptions(tenant_id);

CREATE TABLE IF NOT EXISTS tenant_webhook_targets (
    target_id UUID PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    target_url TEXT NOT NULL,
    secret_ref VARCHAR(256) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    event_types TEXT[] NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_webhook_targets_tenant ON tenant_webhook_targets(tenant_id);

CREATE TABLE IF NOT EXISTS notification_event_journal (
    event_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64),
    source_service VARCHAR(64) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    is_terminal BOOLEAN NOT NULL DEFAULT FALSE,
    payload_json JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    dispatched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_event_journal_tenant_occurred ON notification_event_journal(tenant_id, occurred_at DESC);
```

---

## 5. Prometheus Observability & Alerting Matrix

| Metric Identifier | Type | Labels | SLO / Alert Threshold |
| :--- | :--- | :--- | :--- |
| `notification_active_sse_connections` | Gauge | `tenant_id`, `node_id` | HPA scales at $\le 15,000$ active sockets per pod ($\approx 20,000$ max ceiling) |
| `notification_events_processed_total` | Counter | `event_type`, `status` | Alert if Kafka/Redis consumer lag $> 5,000$ records for $> 2\text{m}$ |
| `notification_client_write_latency_seconds` | Histogram | `node_id` | Target $P99 \le 10\text{ms}$; Alert if $P99 > 50\text{ms}$ over $3\text{m}$ window |
| `notification_dropped_frames_total` | Counter | `tenant_id`, `reason` | Alert if dropped frames $> 1\%$ of total emitted frames across an active tenant |
| `notification_webhook_enqueued_total` | Counter | `tenant_id`, `status` | Target hand-off latency $P99 \le 15\text{ms}$ |
| `notification_cache_invalidations_total` | Counter | `reason` | Coherence propagation $\le 100\text{ms}$ |

---

## 6. 12-Factor Environment Configuration Matrix

| Environment Variable | Default | Required | Description |
| :--- | :--- | :--- | :--- |
| `GOMEMLIMIT` | `1800MiB` | Yes | Go runtime soft memory limit inside `2GiB` pod ceiling |
| `GOGC` | `80` | Yes | Go garbage collector target percentage to prevent OOM spikes |
| `NODE_ID` | Pod hostname | Yes | Unique daemon instance identifier for `pod.{node_id}.events` targeted routing |
| `HTTP_PORT` | `8080` | Yes | HTTP/2 SSE & REST listening port |
| `POSTGRES_DSN` | — | Yes (Secret) | PgBouncer PostgreSQL 16+ connection string (strictly cold-path only; redacted in logs) |
| `REDIS_ADDRS` | — | Yes | Comma-separated Redis 7.2 Cluster nodes |
| `REDIS_PASSWORD` | — | No (Secret) | Redis authentication secret (strictly redacted in logs) |
| `KAFKA_BROKERS` | — | Yes | Comma-separated Apache Kafka 3.7+ bootstrap brokers |
| `KAFKA_CONSUMER_GROUP` | `rag-notification-dispatch-daemon` | Yes | Consumer group ID for CDC outbox topics |
| `KAFKA_CDC_TOPICS` | `rag.cdc.kb-catalog,rag.cdc.ingestion,rag.cdc.chunk-lifecycle` | Yes | Upstream CDC outbox topics |
| `WEBHOOK_QUEUE_STREAM` | `rag:webhook:tasks` | Yes | Redis Stream / RabbitMQ routing key for `rag-webhook-delivery-worker` |
| `SSE_RING_BUFFER_SIZE` | `256` | Yes | Per-client non-blocking ring buffer capacity |
| `SSE_SLOW_CONSUMER_TIMEOUT` | `5s` | Yes | Continuous buffer saturation duration before client eviction |
| `SSE_HEARTBEAT_INTERVAL` | `15s` | Yes | Interval for `: ping\n\n` keepalive frames flushed via `http.Flusher` |
| `DRAIN_JITTER_MIN` | `30s` | Yes | Minimum SIGTERM staggered drain window (`terminationGracePeriodSeconds: 75`) |
| `DRAIN_JITTER_MAX` | `60s` | Yes | Maximum SIGTERM staggered drain window (`terminationGracePeriodSeconds: 75`) |
| `JWT_PUBLIC_KEY_PEM` | — | Yes (Secret) | Public key / JWKS reference for verifying SSE channel bearer tokens |
| `LOG_LEVEL` | `INFO` | No | `slog` JSON logging threshold (`DEBUG`, `INFO`, `WARN`, `ERROR`) |
