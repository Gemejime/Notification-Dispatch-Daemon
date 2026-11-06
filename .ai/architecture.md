# Architecture Context - RAG Notification Dispatch Daemon

## System Stack & Specifications

| Layer | Technology | Role & Operational Characteristics |
| :--- | :--- | :--- |
| **Language Runtime** | Go 1.23+ | High-concurrency network I/O, epoll/netpoll streaming, low-allocation goroutines |
| **GC & Memory Governor** | `GOMEMLIMIT=1800MiB`, `GOGC=80` | Constrains GC cycles inside 2GiB container limit; ~14–16KiB per socket (~320MiB at 20k idle sockets) |
| **Event Broker Ingress** | Apache Kafka 3.7+ / Redis 7.2 Streams | Consumes CDC transactional outbox mutations with offset tracking |
| **Targeted Routing Backplane** | Redis 7.2 Cluster | User/tenant-to-node presence directory + targeted pod channels (`pod.{node_id}.events`) & `Last-Event-ID` replay buffer |
| **Config Store & Local Cache** | PostgreSQL 16+ (PgBouncer) + Pod-Local Memory Cache | Authoritative cold store mirrored into thread-safe pod-local read cache; invalidated via `tenant.config.updated` |
| **Egress Task Broker** | Redis Streams / RabbitMQ | Enqueues signed webhook delivery tasks (`webhook_task.proto`) for `rag-webhook-delivery-worker` |
| **Transport & Streaming** | HTTP/2 SSE (`text/event-stream`) + REST + Protobuf v3 | Multiplexed SSE with `X-Accel-Buffering: no`, 15s `: ping\n\n` heartbeats, 256-event ring buffers |
| **Doc API & Probes** | OpenAPI 3.1 (`api/openapi/openapi.yaml`) | Interactive Swagger/ReDoc (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`), `/healthz`, `/ready`, `/metrics` |
| **Container & Orchestration** | `gcr.io/distroless/static:nonroot` + Kubernetes HPA/PDB | `ulimit -n 65535`, min 3 replicas across AZs, `minAvailable: 2`, 75s termination grace period |

---

## Production-Grade 5-Layer Backend Architecture

Aligned with `Production Grade Backend Concepts.pdf` and the Hexagonal / Clean Architecture structure in `RAG Notification Dispatch Daemon Service Implementation Plan.pdf`:

### Layer 1: API & Routing (`internal/infrastructure/transport/`, `cmd/notification-daemon/`)
1. **Entry Points (`cmd/notification-daemon/main.go`)**:
   - Initializes Go 1.23+ runtime (`GOMEMLIMIT=1800MiB`, `GOGC=80`), validates environment configuration, sets up PgBouncer PostgreSQL pool (cold path), connects to Redis Cluster (presence directory, replay buffer, invalidation pub/sub), warms up the pod-local in-memory subscription cache, starts the Kafka/Redis CDC consumer group, and launches the HTTP/SSE server.
   - Registers OS signal hooks (`SIGTERM`, `SIGINT`) for staggered connection draining across a 30–60s window within the 75s Kubernetes `terminationGracePeriodSeconds`.
2. **Endpoints & Routing (`internal/infrastructure/transport/http/router.go` & `sse/stream_handler.go`)**:
   - `GET /v1/notifications/stream`: Authenticated unidirectional SSE push stream (`text/event-stream; charset=utf-8`), supporting `Last-Event-ID` backfill replay.
   - `GET /v1/notifications/sessions`: Administrative/diagnostic query returning active tenant connection counts and node presence metadata (`list_active_sessions.go`).
   - `POST /v1/notifications/subscriptions/invalidate`: Internal/administrative cache invalidation trigger.
   - `GET /docs`, `GET /redoc`, `GET /api/docs`, `GET /openapi.json`: OpenAPI 3.1 live documentation endpoints synchronized with `api/openapi/openapi.yaml`.
   - `GET /healthz`: Liveness probe verifying process and runtime loop responsiveness.
   - `GET /ready`: Readiness probe verifying local cache warm-up, Redis Cluster reachability, and CDC consumer health (returns `503 Service Unavailable` immediately upon `SIGTERM` to halt new ingress connections during draining).
   - `GET /metrics`: Prometheus metrics endpoint.
3. **Middleware (`internal/infrastructure/transport/http/middleware/`)**:
   - Request/Correlation ID assignment (ULID/UUIDv4), tenant context extraction, JWT/channel bearer token authentication, structured JSON access logging (`log/slog`), panic recovery with sanitized stack traces, and reconnect rate-limiting middleware.
4. **Input Validation & Sanitization**:
   - Strict schema validation for Protobuf v3 CDC event envelopes (`notification_event.proto`), REST query parameters, `Last-Event-ID` watermarks, and subscription filter rules before invoking application commands or queries.
5. **Rate Limiting & Throttling (`internal/domain/router/filter.go` & HTTP Middleware)**:
   - Token-bucket rate limiting on `/v1/notifications/stream` reconnections and `fetch_replay_events` queries to prevent Redis CPU spikes (>60%) during rolling restarts.
   - Per-tenant event deduplication and notification burst throttling in the domain router.
6. **Idempotency**:
   - Deterministic `event_id` watermarks on every CDC outbox event and SSE frame (`id: <event_id>\n`).
   - Reconnecting clients supplying `Last-Event-ID` receive exact missed frames without duplicate webhook enqueueing (keyed by idempotent `(tenant_id, event_id, target_id)` delivery task IDs).

### Layer 2: Data Persistence & State (`internal/infrastructure/cache/`, `internal/infrastructure/persistence/`)
7. **Database Migrations & Schema Evolution**:
   - Version-controlled SQL migrations for authoritative tenant notification preferences (`subscriptions`) and webhook target metadata (`webhook_targets`) in PostgreSQL 16+.
8. **Database Transactions (ACID) & Outbox Pattern**:
   - Upstream services commit business state and outbox events atomically. The daemon consumes committed CDC streams and never executes synchronous SQL transactions on the hot dispatch path.
9. **Connection Pooling (`internal/infrastructure/persistence/postgres/pool.go`)**:
   - Managed `pgxpool` through PgBouncer strictly for cold-start cache hydration and cold-path configuration refreshes, protecting PostgreSQL from connection exhaustion.
10. **Multi-Tier Caching & Presence State (`internal/infrastructure/cache/`)**:
    - **Pod-Local In-Memory Subscription Cache (`memory/subscription_cache.go`)**: Thread-safe read-only in-memory cache (synchronized map / Ristretto) storing tenant routing rules and suppression filters. Invalidated within $\le 100\text{ms}$ via Redis Pub/Sub channel `tenant.config.updated` (`redis/invalidation_sub.go`).
    - **Targeted Node Presence Directory (`redis/presence_registry.go`)**: Lightweight user/tenant-to-node routing registry in Redis 7.2 with short keepalive TTLs.
    - **Bounded Replay Buffer**: Redis bounded stream/ring per tenant storing recent events for rate-limited `Last-Event-ID` backfill queries (`fetch_replay_events.go`).
11. **Pagination & Query Limits**:
    - Strict upper bounds on `Last-Event-ID` replay backfill batch sizes and active session administrative queries (`list_active_sessions.go`) to prevent OOM spikes.
12. **Resource Identification Strategies**:
    - High-entropy UUIDv4/ULIDs for `event_id`, `connection_id`, `subscription_id`, and `webhook_task_id`, paired with strict tenant boundary verification on every operation.

### Layer 3: Architecture & Scale (`internal/application/`, `internal/infrastructure/queue/`, `transport/hub/`)
13. **Asynchronous Task Queues & Stream Workers**:
    - **Ingress (`queue/kafka/cdc_consumer.go`)**: Consumer group ingesting CDC outbox events with deterministic offset tracking.
    - **Targeted Pod Routing (`transport/hub/targeted_router.go`)**: Resolves recipient `node_id` via Redis directory and publishes directly to `pod.{node_id}.events`, eliminating $O(N \times M)$ global Pub/Sub broadcast amplification (ADR-003).
    - **Egress (`queue/publisher/task_queue_publisher.go`)**: Asynchronously publishes validated `WebhookTask` messages to Redis Streams / RabbitMQ for `rag-webhook-delivery-worker` (ADR-001).
14. **High Availability, Redundancy & Connection Governance**:
    - **Per-Connection Bounded Ring Buffer (`transport/hub/ring_buffer.go`)**: 256-event non-blocking channel/ring buffer per client stream. When saturated, unread intermediate non-terminal progress ticks are dropped (`notification_dropped_frames_total`) while terminal state transitions (`COMPLETED`, `FAILED`) are retained. Connections saturated for $> 5\text{s}$ are cleanly evicted.
    - **Edge Proxy Bypass (`transport/sse/flusher.go`)**: Emits `X-Accel-Buffering: no`, `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control: no-cache`, and flushes `: ping\n\n` every 15s via `http.Flusher`.
    - **Multi-AZ Deployment**: Minimum 3 replicas with pod anti-affinity, `PodDisruptionBudget` (`minAvailable: 2`), and HPA scaling on active TCP socket count ($\le 15,000$/pod) and CPU (65%).
15. **Creational Design Patterns**:
    - Dependency injection wired in `cmd/notification-daemon/main.go` instantiating singletons for the local `ConnectionHub`, `SubscriptionCache`, `PgBouncerPool`, `RedisClient`, and `PrometheusRegistry`.

### Layer 4: Security & Identity (`internal/infrastructure/transport/http/middleware/`)
16. **Authentication vs. Authorization (AuthN vs. AuthZ)**:
    - **AuthN**: Validates short-lived JWT / signed channel authorization tokens on `/v1/notifications/stream` handshake.
    - **AuthZ**: Enforces strict per-tenant push isolation (`tenant_id` extracted from verified token claims must match subscription/stream namespace; cross-tenant channel subscription is impossible).
17. **Configuration & Secrets Management**:
    - 12-Factor App configuration loaded from environment variables.
    - Webhook HMAC-SHA256 signing secrets are referenced via secure secret identifiers (`secret_ref`) or vaulted memory; never hardcoded or logged.
18. **Data Protection & SSRF Isolation**:
    - TLS 1.3 in transit; mTLS / VPC isolation for internal Kafka, Redis, and PostgreSQL traffic.
    - Outbound webhook HTTP execution is physically and logically isolated into `rag-webhook-delivery-worker` in the egress DMZ to eliminate SSRF attack vectors inside the dispatch daemon (ADR-001).

### Layer 5: Observability & Quality
19. **Observability (Secured Structured Logging, Metrics & Tracing)**:
    - **Secured Logging (`log/slog`)**: Machine-parseable JSON logs with correlation IDs (`trace_id`, `tenant_id`, `event_id`, `connection_id`, `node_id`). Strictly redacts bearer tokens, HMAC secrets, database passwords, and sensitive payload text.
    - **Distributed Tracing (OpenTelemetry W3C `traceparent`)**: Extracts `trace_id` from incoming CDC outbox envelopes (`notification_event.proto`), propagates span context across targeted Redis node routing (`pod.{node_id}.events`) and outbound `WebhookDeliveryTask` messages (`webhook_task.proto`), and exports spans asynchronously to `rag-trace-collector-worker` without blocking the serving path.
    - **Prometheus Metrics & Production Alert Thresholds**:
      - `notification_active_sse_connections` (Gauge, partitioned by `tenant_id`, `node_id`; HPA scales at $\le 15,000$/pod)
      - `notification_events_processed_total` (Counter; alert if CDC consumer lag $> 5,000$ records for $> 2\text{m}$)
      - `notification_client_write_latency_seconds` (Histogram, target $P99 \le 10\text{ms}$; alert if $P99 > 50\text{ms}$ over $3\text{m}$)
      - `notification_dropped_frames_total` (Counter, slow consumer drops; alert if $> 1\%$ of emitted frames across an active tenant)
      - `notification_webhook_enqueued_total` (Counter; target hand-off $P99 \le 15\text{ms}$)
      - `notification_cache_invalidations_total` (Counter; target propagation $\le 100\text{ms}$)
      - **Near-OOM Saturation Alert**: Fires if container memory working set exceeds $1.8\text{GiB}$ for $> 1\text{m}$.
20. **Health Checks & Graceful Connection Draining**:
    - `/healthz` and `/ready` probes govern load balancer traffic routing.
    - **Staggered Connection Draining on SIGTERM (ADR-005)**: On pod termination, the service marks `/ready` as `503`, stops accepting new connections, emits an SSE `retry:` directive header/frame with randomized jitter, and spreads socket disconnections across a 30–60 second `preStop` window (`terminationGracePeriodSeconds: 75`) to prevent thundering herd reconnection spikes.

---

## Hexagonal Folder Structure

```
rag-notification-dispatch-daemon/
├── cmd/
│   └── notification-daemon/
│       └── main.go                      # Dependency wiring, cache warm-up, graceful drain hooks
├── api/
│   ├── proto/v1/
│   │   ├── notification_event.proto     # CDC outbox event envelope definitions
│   │   ├── webhook_task.proto           # Downstream delivery task contracts
│   │   └── contracts.pb.go              # Deterministic Protobuf/JSON wire bindings & validators
│   └── openapi/
│       └── openapi.yaml                 # Public push subscription & probe OpenAPI 3.1 spec
├── internal/
│   ├── domain/                          # Core business logic (isolated from framework I/O)
│   │   ├── entity/
│   │   │   ├── connection.go            # Client session aggregate (user, tenant, stream ID)
│   │   │   ├── notification.go          # Domain event model and severity classification
│   │   │   ├── subscription.go          # Tenant routing preferences and filters
│   │   │   └── webhook_target.go        # External URL metadata and HMAC secret references
│   │   ├── router/
│   │   │   ├── rule_evaluator.go        # In-memory rule matching logic
│   │   │   └── filter.go                # Deduplication and rate-limiting evaluation
│   │   └── repository/                  # Domain interface ports
│   │       ├── subscription_repo.go
│   │       ├── webhook_target_repo.go
│   │       └── event_journal_repo.go
│   ├── application/                     # Orchestration layer
│   │   ├── command/
│   │   │   ├── process_cdc_event.go     # Ingestion pipeline routing via in-memory cache
│   │   │   ├── register_connection.go   # Tracks new SSE sessions and node presence
│   │   │   ├── terminate_connection.go  # Cleans local and distributed registries
│   │   │   └── dispatch_webhook_task.go # Signs and enqueues external delivery tasks
│   │   └── query/
│   │       ├── list_active_sessions.go  # Presence and connection telemetry
│   │       └── fetch_replay_events.go   # Rate-limited reconnect backfill handler
│   └── infrastructure/                  # Adapters and external drivers
│       ├── observability/
│       │   └── logging.go               # Secured slog JSON logger (ReplaceAttr redaction) & Prometheus metrics
│       ├── cache/
│       │   ├── memory/
│       │   │   └── subscription_cache.go # Local read-only subscription cache
│       │   └── redis/
│       │       ├── presence_registry.go # User-to-node routing directory
│       │       └── invalidation_sub.go  # Subscribes to tenant.config.updated
│       ├── persistence/
│       │   └── postgres/
│       │       ├── pool.go              # PgBouncer connection pool setup
│       │       ├── subscription_repo.go # Cold-path configuration queries
│       │       └── webhook_repo.go      # Cold-path webhook target queries
│       ├── queue/
│       │   ├── kafka/
│       │   │   └── cdc_consumer.go      # Consumer group ingestion engine
│       │   └── publisher/
│       │       └── task_queue_publisher.go # Pushes tasks to webhook delivery queue
│       └── transport/
│           ├── hub/
│           │   ├── connection_hub.go    # Local node session manager
│           │   ├── targeted_router.go   # Routes events directly to specific pod channels
│           │   └── ring_buffer.go       # Non-blocking per-connection buffer (256 cap)
│           ├── sse/
│           │   ├── stream_handler.go    # Text/event-stream HTTP endpoint with proxy bypass
│           │   └── flusher.go           # Immediate chunk byte flusher with keepalive pings
│           └── http/
│               ├── router.go            # REST administrative & OpenAPI 3.1 endpoints
│               └── middleware/          # Tenant context, auth, secured logging, rate limiters
├── deployments/
│   ├── helm/                            # Kubernetes deployment charts (HPA, PDB, resources)
│   └── docker/
│       └── Dockerfile                   # Multi-stage distroless build (gcr.io/distroless/static:nonroot)
├── test/
│   ├── unit/                            # Pure routing, JSON speed, and ring buffer drop tests
│   ├── integration/                     # Testcontainers suites (Postgres + Redis + Kafka)
│   └── performance/                     # 100k concurrent client streaming & chaos benchmarks
├── Makefile
├── go.mod
└── go.sum
```

---

## Architecture Decision Records (ADRs)

- **ADR-001: Separation of Dispatch and Egress Delivery**: The daemon strictly manages event ingestion, SSE streaming, and webhook task enqueueing. Outbound HTTP requests are delegated to `rag-webhook-delivery-worker` to isolate network I/O and prevent SSRF.
- **ADR-002: SSE Unification for Milestone Streaming**: Pipeline notifications standardize 100% on Server-Sent Events (`text/event-stream`). Complex framed WebSockets are removed from this daemon to eliminate protocol bloat and leverage standard HTTP/2 connection infrastructure.
- **ADR-003: Targeted Node Routing Over Global Redis Fan-Out**: Replaces blind Redis Pub/Sub broadcast with a user/node directory and targeted pod channels (`pod.{node_id}.events`) to eliminate $O(N \times M)$ message duplication.
- **ADR-004: In-Memory Subscription Caching**: All tenant routing configurations are stored in pod-local read memory with asynchronous event-driven invalidation (`tenant.config.updated`), decoupling CDC processing latency from PostgreSQL connection limits.
- **ADR-005: Staggered Connection Draining**: On `SIGTERM`, pods reject new connections and gracefully close active SSE streams using randomized retry jitter over a 30–60 second window to prevent thundering herd spikes.
