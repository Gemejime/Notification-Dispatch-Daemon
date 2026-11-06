# RAG Notification Dispatch Daemon (`rag-notification-dispatch-daemon`)

[![Go Version](https://img.shields.io/badge/go-1.23%2B-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg)](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/LICENSE)
[![Architecture](https://img.shields.io/badge/architecture-hexagonal-orange.svg)](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/ARCHITECTURE.md)
[![OpenAPI](https://img.shields.io/badge/OpenAPI-3.1-blueviolet.svg)](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/api/openapi/openapi.yaml)
[![SLO Availability](https://img.shields.io/badge/SLO%20Availability-99.99%25-success.svg)](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/project-overview.md)
[![Concurrent Sockets](https://img.shields.io/badge/capacity-%E2%89%A5100k%20sockets-purple.svg)](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/project-overview.md)

The **`rag-notification-dispatch-daemon`** is a high-density, ultra-low-latency Go 1.23+ microservice operating within the vertical feature control plane of the Multimodal RAG platform.

It is engineered for **multiplexed Server-Sent Events (SSE) streaming** (`text/event-stream; charset=utf-8`) and **decoupled Change Data Capture (CDC) outbox ingestion**. By pairing targeted pod routing over Redis 7.2 Cluster with pod-local in-memory subscription rule caching, the daemon achieves sub-50ms notification delivery to browser clients and sub-15ms webhook task queue hand-offs—all with **zero synchronous database reads on the hot path**.

---

## Architecture Overview

```
[ Upstream Domain Services: rag-kb-catalog-service / rag-ingestion-coordinator / rag-chunk-lifecycle-service ]
                                            │
                                            ▼ (Transactional Outbox / CDC Stream: Kafka 3.7+ / Redis 7.2 Streams)
                             [ rag-notification-dispatch-daemon ] (Go 1.23+, GOMEMLIMIT=1800MiB, GOGC=80)
                                            │
              ┌─────────────────────────────┼─────────────────────────────┐
              ▼                             ▼                             ▼
   Local Read-Only Cache         Targeted Pod Routing          Egress Webhook Task Queue
 (In-Memory Rule Evaluator)    (Redis: pod.{node_id}.events)   (Redis Streams / RabbitMQ)
    [0 SQL Reads on Hot Path]       [Bounded 256-Event Ring]       [HMAC-SHA256 Metadata]
              │                             │                             │
              ▼                             ▼                             ▼
    PostgreSQL 16+ (Cold)       Active Client SSE Streams     [ rag-webhook-delivery-worker ]
  + tenant.config.updated      (GET /v1/notifications/stream)    (Python Egress DMZ Pool)
```

```mermaid
flowchart TD
    subgraph Upstream ["Upstream Domain Outboxes"]
        KB["rag-kb-catalog-service"]
        IC["rag-ingestion-coordinator"]
        CLS["rag-chunk-lifecycle-service"]
    end

    subgraph Broker ["CDC Ingestion Broker"]
        Kafka["Apache Kafka 3.7+ / Redis Streams\ntopic: cdc.notifications.events"]
    end

    subgraph Daemon ["rag-notification-dispatch-daemon Pod (Hexagonal Architecture)"]
        Consumer["CDC Consumer Group\n(cdc_consumer.go)"]
        Cache["In-Memory Subscription Cache\n(subscription_cache.go)\n[0 SQL Reads on Hot Path]"]
        Router["Targeted Pod Router\n(targeted_router.go)"]
        Hub["Connection Hub\n(connection_hub.go)"]
        Ring["256-Event Ring Buffer\n(ring_buffer.go)\n[Terminal Preservation]"]
        SSE["SSE Flusher\n(flusher.go)\n15s : ping\\n\\n"]
    end

    subgraph StorageBackplane ["State & Persistence Backplane"]
        RedisCluster["Dedicated Redis 7.2 Cluster\n- Presence Registry\n- pod.{node_id}.events\n- Last-Event-ID Replay"]
        Postgres["PostgreSQL 16+ (PgBouncer)\nCold Hydration & Configs"]
    end

    subgraph EgressWorkers ["Asynchronous Delivery"]
        Worker["rag-webhook-delivery-worker\n(Python Egress DMZ Pool)"]
        Clients["Active Browser / UI Clients\n(GET /v1/notifications/stream)"]
    end

    KB -->|Outbox Commit| Kafka
    IC -->|Outbox Commit| Kafka
    CLS -->|Outbox Commit| Kafka

    Kafka -->|Consume| Consumer
    Consumer --> Cache
    Cache --> Router

    Router -->|Local Session| Hub
    Router -->|Remote Node| RedisCluster
    RedisCluster -->|pod.{node_id}.events| Hub

    Hub --> Ring
    Ring --> SSE
    SSE -->|text/event-stream| Clients

    Router -->|Enqueue WebhookTask| Worker
    Postgres -.->|Cold Start Warmup| Cache
    RedisCluster -.->|tenant.config.updated <=100ms| Cache
```

---

## Architectural Decision Records (ADRs)

| ADR | Title | Core Decision & Operational Rationale |
| :--- | :--- | :--- |
| **[ADR-001](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/architecture.md#adr-001-asynchronous-webhook-enqueueing--ssrf-isolation)** | **SSRF & Egress Isolation** | The daemon **never** executes external HTTP POST requests. It packages tenant webhook tasks with HMAC-SHA256 signing metadata and enqueues them into Redis Streams / RabbitMQ for [`rag-webhook-delivery-worker`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/internal/infrastructure/queue/publisher/task_queue_publisher.go) in an egress DMZ, fully isolating against SSRF vectors. |
| **[ADR-002](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/architecture.md#adr-002-sse-over-websockets-for-unidirectional-push)** | **SSE Over WebSockets** | Push notifications are strictly unidirectional (ingestion milestones, vector pipeline progress). The daemon exposes HTTP/2 Server-Sent Events (`text/event-stream; charset=utf-8`) with `X-Accel-Buffering: no`, `Cache-Control: no-cache`, and 15s keepalive comment frames (`: ping\n\n`), reducing connection memory footprint to ~14–16KiB per socket. |
| **[ADR-003](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/architecture.md#adr-003-targeted-pod-routing-via-redis-cluster)** | **Targeted Pod Routing** | Instead of global Pub/Sub broadcast storms ($O(N \times M)$ message duplication), daemon pods register active socket sessions in a Redis 7.2 Cluster presence directory and publish routed frames directly to node-specific channels (`pod.{node_id}.events`). |
| **[ADR-004](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/architecture.md#adr-004-zero-hot-path-sql-reads--subscription-rule-caching)** | **Zero Hot-Path SQL Reads** | All subscription routing rules and tenant notification suppressions evaluate exclusively against thread-safe, pod-local in-memory caches. Cold caches warm from PostgreSQL 16+ on boot; updates invalidate across all cluster pods asynchronously via `tenant.config.updated` within $\le 100\text{ms}$. |
| **[ADR-005](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/architecture.md#adr-005-staggered-graceful-draining-on-sigterm)** | **Staggered Graceful Draining** | On `SIGTERM`, `/ready` flips to `503` immediately to stop ingress traffic. Connected SSE sockets receive a randomized reconnect retry header (`retry: <ms>\n\n`) spread across a 30–60s jitter window within the 75s Kubernetes `terminationGracePeriodSeconds` to prevent thundering herd reconnection storms. |

---

## Quantitative Performance & SLOs

- **Dispatch Latency**: $P99 \le 50\text{ms}$ from CDC event ingestion to client socket write ($P99 \le 10\text{ms}$ socket write latency).
- **Webhook Enqueue Latency**: $P99 \le 15\text{ms}$ to evaluate routing rules and push signed tasks to queues.
- **Connection Capacity**: $\ge 100,000$ concurrent idle push connections across the daemon cluster ($\approx 20,000$ per pod; HPA threshold $\le 15,000$ sockets/pod).
- **Runtime Memory Governor**: Go 1.23+ runtime configured with `GOMEMLIMIT=1800MiB` and `GOGC=80` inside `2GiB` container memory limits (`1GiB` request) to eliminate OOMKills under heavy GC allocation bursts.
- **Ring Buffer Drop Policy**: 256-event non-blocking ring buffer per connection ([`ring_buffer.go`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/internal/infrastructure/transport/hub/ring_buffer.go)). When saturated, intermediate non-terminal progress events (`is_terminal == false`) are dropped while terminal state transitions (`COMPLETED`, `FAILED`) are strictly preserved. Saturated slow consumers are evicted after $> 5\text{s}$.
- **Service Availability**: $99.99\%$ availability with zero dropped terminal events during rolling deployments.

---

## API Surface & Documentation

| Method | Path | Auth | Purpose |
| :--- | :--- | :--- | :--- |
| `GET` | `/v1/notifications/stream` | Bearer JWT / Channel Token | Unidirectional SSE push channel. Supports `Last-Event-ID` recovery replay. |
| `GET` | `/v1/notifications/sessions` | Bearer JWT (Admin / Tenant) | Active connection counts and node presence telemetry for a tenant. |
| `POST` | `/v1/notifications/subscriptions/invalidate` | Bearer JWT (Admin) | Asynchronously broadcasts `tenant.config.updated` cache invalidations. |
| `GET` | `/docs` | Public | Interactive Swagger UI live API documentation. |
| `GET` | `/redoc` | Public | Interactive ReDoc live API documentation. |
| `GET` | `/openapi.json`, `/api/docs` | Public | Full OpenAPI 3.1 specification. |
| `GET` | `/healthz` | Public | Kubernetes Liveness probe (verifies process loop). |
| `GET` | `/ready` | Public | Kubernetes Readiness probe (verifies cache, Redis, and CDC health; returns `503` during SIGTERM drain). |
| `GET` | `/metrics` | Public | Prometheus operational telemetry metrics. |

Full OpenAPI 3.1 specification is available in [`api/openapi/openapi.yaml`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/api/openapi/openapi.yaml) and documented in detail in [`.ai/doc-api.md`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/.ai/doc-api.md).

---

## Hexagonal Codebase Structure

```
.
├── ARCHITECTURE.md                       # Comprehensive 5-Layer backend architecture & ADRs
├── .ai/                                  # Authoritative system specs, architecture, & doc-api
│   ├── architecture.md                   # 5-Layer Backend Architecture & ADR-001..ADR-005
│   ├── doc-api.md                        # Complete API contracts, schemas, Redis keys, & Prometheus metrics
│   └── project-overview.md               # Mission, SLIs/SLOs, and operational boundaries
├── api/
│   ├── openapi/openapi.yaml              # OpenAPI 3.1 live specification
│   └── proto/v1/                         # Protobuf v3 contracts (notification_event, webhook_task)
├── cmd/
│   └── notification-daemon/              # Application entrypoint & dependency injection
│       └── main.go                       # Runtime initialization, signal trapping, and server startup
├── internal/
│   ├── domain/                           # Pure domain logic (entities, values, rules, zero external deps)
│   │   ├── entity/                       # NotificationEvent, WebhookTask, Subscription, Session
│   │   └── router/                       # Routing rules, severity classifications, and burst throttlers
│   ├── application/                      # Use case orchestration (CQRS patterns)
│   │   ├── command/                      # ProcessCDCEvent, DispatchWebhookTask
│   │   └── query/                        # FetchReplayEvents (Last-Event-ID), ListActiveSessions
│   └── infrastructure/                   # External adapters & transport protocols
│       ├── cache/                        # Memory SubscriptionCache, Redis PresenceRegistry, InvalidationSub
│       ├── config/                       # 12-Factor environment variable configuration loader
│       ├── observability/                # Secured Logger (log/slog), Prometheus metrics registry
│       ├── persistence/postgres/         # PgBouncer connection pool & cold-path hydration
│       ├── queue/                        # Kafka/Redis CDC consumer & webhook task publisher
│       └── transport/                    # HTTP/2 SSE transport, ConnectionHub, RingBuffer, Middleware
├── deployments/
│   ├── argocd/                           # Declarative GitOps ArgoCD application manifests
│   ├── docker/                           # Dockerfile (distroless:nonroot) & Compose dev stacks
│   ├── helm/                             # Production Kubernetes Helm v3 charts & Bitnami Redis Cluster
│   ├── redis.md                          # Dedicated Redis 7.2 Cluster architecture & runbooks
│   └── SECRET_MANAGEMENT.md              # Zero-leakage secret management & rotation guide
├── scripts/
│   ├── deploy_redis_cluster.sh / .ps1    # Automated Bitnami Redis Cluster deployment scripts
│   └── sync_env_to_deployments.py        # Strict zero-drift environment & Helm values synchronizer
└── test/
    ├── unit/                             # Unit test suites across domain, application, and infrastructure
    └── performance/                      # Soak tests (100k sockets), chaos drain tests, & Slow-Loris tests
```

---

## Local Development Quickstart

### Prerequisites

- **Go**: 1.23 or newer
- **Docker & Docker Compose**: 24+
- **Make**: GNU Make

### 1. Launch Local Infrastructure

Spin up a standalone Redis 7.2 development instance with AOF persistence and authentication enabled:

```bash
docker compose -f deployments/docker/docker-compose.redis.yml up -d
```

### 2. Configure Environment

Copy the development environment template:

```bash
cp .env.example .env
```

Ensure sensitive variables match your local setup:
- `REDIS_CLUSTER_ADDRS=127.0.0.1:6379`
- `REDIS_PASSWORD=devpassword123`
- `KAFKA_BROKERS=127.0.0.1:9092`
- `PORT=8080`

### 3. Build & Run the Daemon

```bash
# Build the binary with Go runtime memory limits
make build

# Run the daemon
./bin/notification-daemon
```

Or run directly:

```bash
go run ./cmd/notification-daemon
```

### 4. Verify Endpoints

```bash
# Liveness probe
curl -i http://localhost:8080/healthz

# Readiness probe
curl -i http://localhost:8080/ready

# Prometheus metrics
curl -s http://localhost:8080/metrics

# Connect to SSE stream
curl -N -H "Authorization: Bearer <valid_jwt>" http://localhost:8080/v1/notifications/stream
```

---

## Testing & Quality Assurance

The codebase enforces strict unit, integration, and performance benchmarks:

```bash
# Run unit tests with race detection and coverage reporting
make test

# Run micro-benchmarks
make bench

# Run chaos and graceful draining tests
make test-performance

# Run high-density soak test (20,000 synthetic connections under heap limits)
make soak-test

# Lint Go code
make vet
```

---

## Kubernetes & Production Deployment

### 1. Dedicated Bitnami Redis 7.2 Cluster

The daemon requires a dedicated, in-cluster 6-node Bitnami Redis 7.2 Cluster (3 Primaries + 3 Replicas) for sub-millisecond presence tracking and targeted routing:

```bash
# Linux / macOS
bash scripts/deploy_redis_cluster.sh

# Windows PowerShell
powershell -ExecutionPolicy Bypass -File scripts/deploy_redis_cluster.ps1
```

For topology details, failover procedures, and headless DNS routing, refer to [`deployments/redis.md`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/deployments/redis.md).

### 2. Microservice Helm Deployment

Deploy the daemon using Helm v3:

```bash
helm upgrade --install rag-notification-dispatch-daemon \
  deployments/helm/rag-notification-dispatch-daemon \
  -n rag-system \
  -f deployments/helm/rag-notification-dispatch-daemon/values.yaml
```

### 3. GitOps with ArgoCD

Automated declarative synchronization is configured in [`deployments/argocd/`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/deployments/argocd/):

```bash
kubectl apply -f deployments/argocd/redis-cluster-application.yaml
kubectl apply -f deployments/argo/application.yaml
```

---

## Security & Observability

- **Secured Logging Invariant**: Structured JSON logging powered by Go's `log/slog`. All bearer tokens, HMAC secrets, database credentials, and raw notification payloads are automatically intercepted and serialized as `"[REDACTED]"`.
- **Tenant Push Isolation**: Handshake tokens bind each SSE client to a specific `tenant_id`. Cross-tenant channel subscription or message interception is impossible at the domain router level.
- **Zero-Drift Configuration**: All production configuration parameters in `.env.prod` are validated against Helm charts via [`scripts/sync_env_to_deployments.py`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/scripts/sync_env_to_deployments.py).
- **Secret Management**: Follows External Secrets Operator (ESO) and HashiCorp Vault workflows detailed in [`deployments/SECRET_MANAGEMENT.md`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/deployments/SECRET_MANAGEMENT.md).

---

## License

This project is licensed under the Apache 2.0 License. See the [`LICENSE`](file:///c:/Users/ANIK/Desktop/Gemejime/Notification%20Dispatch%20Daemon/LICENSE) file for details.

