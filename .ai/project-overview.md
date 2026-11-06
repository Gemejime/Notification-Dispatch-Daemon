# RAG Notification Dispatch Daemon - Project Overview

## Overview

The **`rag-notification-dispatch-daemon`** (Notification Dispatch Daemon) is a high-density, low-latency Go (Golang 1.23+) microservice operating within the vertical feature control plane of the Multimodal RAG platform.

It provides **multiplexed Server-Sent Events (SSE) streaming** (`text/event-stream`) and **decoupled Change Data Capture (CDC) outbox ingestion** for real-time notification dispatching. By utilizing targeted pod routing over Redis (`pod.{node_id}.events`) and pod-local in-memory subscription rule caching, it ensures sub-50ms event delivery to active browser sessions and sub-15ms asynchronous webhook task enqueueing without incurring synchronous database read overhead on the critical path.

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

---

## Strategic Objectives

1. **Decoupled Outbox Ingestion**: Consume internal domain events emitted via Change Data Capture (CDC) and transactional outbox streams without introducing synchronous latency or database locks into retrieval or ingestion execution paths.
2. **Multiplexed SSE Push Engine**: Manage persistent unidirectional client push channels over Server-Sent Events (SSE), routing ingestion milestones, re-indexing triggers, and evaluation alerts to active browser sessions (dropping complex framed WebSockets on this service to halve protocol overhead — ADR-002).
3. **Asynchronous Webhook Enqueueing**: Evaluate tenant routing rules and notification subscriptions via local in-memory caches, translating internal event envelopes into validated, HMAC-SHA256 metadata-packaged webhook delivery tasks queued for downstream execution by `rag-webhook-delivery-worker` (ADR-001).
4. **Strict Security & Tenant Isolation**: Enforce per-tenant push segregation, secure channel authorization tokens, Secured Logging redaction invariants, and strict boundary separation between event dispatching and external HTTP delivery.
5. **Resilient Ephemeral Connection Handling**: Provide graceful connection lifecycle governance, bounded write ring buffers (256 events) per client connection with slow-consumer drop policies, and reconnect recovery via standard `Last-Event-ID` watermarks.
6. **Continuous Documentation & Contract Synchronization**: Ensure OpenAPI 3.1 specifications (`api/openapi/openapi.yaml`, `/docs`, `/redoc`, `/api/docs`, `/openapi.json`), Protobuf v3 contracts, and [`.ai/doc-api.md`](doc-api.md) are continuously updated on every code creation or modification, no matter how small.

---

## Quantitative Success Criteria (SLIs & SLOs)

- **End-to-End Dispatch Latency**: $P99 \le 50\text{ms}$ from CDC event ingestion to SSE client write buffer under steady-state load (with per-socket write duration $P99 \le 10\text{ms}$).
- **High Concurrent Connection Density**: Maintain $\ge 100,000$ concurrent idle push connections across the daemon cluster ($\approx 20,000$ active connections per pod node; HPA target $\le 15,000$ connections per pod).
- **Pod Resource Ceiling**:
  - Memory requests: `1GiB`, limits: `2GiB` (steady-state RSS $\le 1.6\text{GiB}$ under 20k sockets; configured with `GOMEMLIMIT=1800MiB` and `GOGC=80` to eliminate OOMKills under GC spikes).
  - CPU requests: `500m`, limits: `2000m` (HPA target 65% utilization).
- **Webhook Queue Hand-off Latency**: $P99 \le 15\text{ms}$ to evaluate cached tenant subscription rules and enqueue payloads into Redis Streams / RabbitMQ delivery queues.
- **Cache Invalidation Coherence**: All daemon pods invalidate their local in-memory tenant subscription cache within $\le 100\text{ms}$ of a `tenant.config.updated` Pub/Sub broadcast.
- **Availability & Staggered Draining**: $99.99\%$ service availability with zero dropped terminal events during rolling restarts, guaranteed via graceful connection draining (30–60s randomized retry jitter over a 75s `terminationGracePeriodSeconds` window) and rate-limited `Last-Event-ID` backfills.

---

## Scope & Exclusions

### In-Scope
- **CDC & Outbox Stream Ingestion**: Subscribing to internal domain topic streams (Apache Kafka 3.7+ or Redis 7.2 Streams) sourced from transactional outbox commits across `rag-kb-catalog-service`, `rag-ingestion-coordinator`, and `rag-chunk-lifecycle-service`.
- **Unidirectional SSE Push Management**: Managing authenticated SSE (`text/event-stream; charset=utf-8`) endpoints terminating at the daemon boundary (`/v1/notifications/stream`), setting explicit proxy-bypass buffering headers (`X-Accel-Buffering: no`, `Cache-Control: no-cache`) and 15-second keepalive comment frames (`: ping\n\n`).
- **Local In-Memory Subscription Routing**: Evaluating tenant-level preferences, severity classifications, deduplication, and notification suppressions via a read-only in-memory cache (synchronized concurrent map / Ristretto) warmed from PostgreSQL 16+ on cold start and invalidated asynchronously via `tenant.config.updated` events (zero SQL queries on the hot ingestion path).
- **Targeted Connection Node Registry**: Managing local in-memory active socket registries coordinated via a lightweight user/tenant session directory in Redis 7.2 Cluster and targeted node routing channels (`pod.{node_id}.events`) to prevent $O(N \times M)$ broadcast fan-out amplification.
- **Bounded Per-Client Ring Buffers & Slow Consumer Eviction**: Isolating every client stream behind a non-blocking 256-event ring buffer that drops intermediate non-terminal progress frames on saturation while preserving terminal state transitions, and evicts connections saturated for $> 5\text{ seconds}$.
- **Egress Task Enqueueing**: Packaging outgoing webhook tasks with cryptographic signing metadata references (HMAC-SHA256) and pushing to Redis Streams / RabbitMQ queues consumed by `rag-webhook-delivery-worker`.
- **OpenAPI 3.1 & Operational Probes**: Exposing interactive documentation (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`), REST subscription/session endpoints, `/healthz`, `/ready`, and `/metrics`.

### Exclusions (Out-of-Scope)
- **Direct External HTTP Egress**: Outbound webhook HTTP POST execution, retry backoff jitter, and SSRF firewalling are strictly delegated to horizontal Python workers in the egress DMZ (`rag-webhook-delivery-worker` — ADR-001).
- **Bidirectional WebSockets**: Complex full-duplex socket framing is excluded; unidirectional pipeline milestones are strictly served via SSE, while chat ingress is managed by the API Gateway (ADR-002).
- **Synchronous Database Reads on Ingestion**: Hitting PgBouncer/PostgreSQL per ingested CDC event for routing or preference checks is strictly forbidden on the hot path (ADR-004).
- **Direct Ingestion Binary Processing**: Parsing files, computing OCR, or generating embeddings (handled by `rag-document-ai-worker` and `rag-inference-serving`).
- **Long-Term Event Archival**: Permanent audit logs and HIPAA/SOC2 compliance storage are delegated to `rag-telemetry-audit-daemon`.
