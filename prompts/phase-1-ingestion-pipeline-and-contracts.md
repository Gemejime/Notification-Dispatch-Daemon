# Implementation Plan: Phase 1 — Ingestion Pipeline & Contracts (`rag-notification-dispatch-daemon`)

## 1. Objectives & Architectural Scope (Pressure-Tested Against `tasks/` & `.ai/`)

Implement **Phase 1: Ingestion Pipeline & Contracts** from the `RAG Notification Dispatch Daemon Service Implementation Plan` and `.ai/progress-tracker.md`, adhering strictly to `GEMINI.MD`, `.ai/architecture.md` (5-Layer Production Backend & Hexagonal Go 1.23+ Architecture, ADRs 001–005), `.ai/doc-api.md`, `.ai/code-standards.md`, and `.ai/security.md`:

1. **Protobuf v3 & OpenAPI 3.1 Contracts (`api/`)**:
   - Create `api/proto/v1/notification_event.proto` and `api/proto/v1/webhook_task.proto` alongside their Go contract bindings (`api/proto/v1/contracts.pb.go`).
   - Materialize `api/openapi/openapi.yaml` synchronized 1:1 with `.ai/doc-api.md` so CI job `openapi-and-doc-api-sync` validates all required paths (`/v1/notifications/stream`, `/v1/notifications/sessions`, `/v1/notifications/subscriptions/invalidate`, `/healthz`, `/ready`, `/metrics`, `/docs`, `/redoc`, `/api/docs`, `/openapi.json`).
   - Provide a **zero-intermediary JSON formatting pipeline** for SSE client writes (`id: <event_id>\nevent: <event_type>\nretry: <ms>\ndata: <json>\n\n`) that splices pre-validated `payload_json` bytes directly into a reusable byte buffer without Protobuf-to-Map-to-JSON re-marshaling overhead on the write path (`P99 <= 10ms` socket write / `P99 <= 50ms` end-to-end dispatch).
2. **Core Hexagonal Domain Layer (`internal/domain/`)**:
   - Pure Go 1.23+ business logic with **zero external I/O driver imports** (no `pgx`, `go-redis`, `kafka-go`, or `net/http`).
   - **Entities**:
     - `NotificationEvent` & `Severity` (`notification.go`): Validates canonical upstream services (`rag-kb-catalog-service`, `rag-ingestion-coordinator`, `rag-chunk-lifecycle-service`), event types (`ingestion.milestone`, `index.blue_green_switched`, `chunk.overlay_conflict`, `evaluation.quality_alert`), ULID/UUIDv4 `event_id` watermarks, `is_terminal` flags, W3C `trace_id`, direct SSE byte formatting, and redacted `LogValue()`.
     - `Subscription` (`subscription.go`): Tenant routing preferences, wildcard/exact event pattern matching, `min_severity` threshold, `sse_enabled`, `webhook_enabled`, `suppressed` flags, and `rate_limit_rps`.
     - `WebhookTarget` (`webhook_target.go`): Customer target metadata (`target_id`, `tenant_id`, `target_url`, KMS/Vault `secret_ref`, `signature_header_name`, `active`, `event_types`), deterministic idempotency key generator `task_id = sha256(tenant_id + ":" + event_id + ":" + target_id)`, and redacted `LogValue()` masking `secret_ref`.
     - `Connection` (`connection.go`): Client session aggregate (`connection_id`, `tenant_id`, `user_id`, `node_id`, `severity_filter`, `last_event_id`, `connected_at`, `buffered_events` max 256, `saturated_since` for `> 5s` slow-consumer eviction) with redacted `LogValue()`.
   - **Router**:
     - `RuleEvaluator` (`rule_evaluator.go`): Pure in-memory evaluation of tenant subscriptions and active webhook targets without database I/O.
     - `Filter` (`filter.go`): Bounded-memory `(tenant_id, event_id)` deduplication window with TTL eviction (preventing unbounded map growth under `GOMEMLIMIT=1800MiB`) and per-tenant token-bucket burst rate limiter (`rate_limit_rps`).
   - **Repository Ports**:
     - `SubscriptionRepository` (`subscription_repo.go`), `WebhookTargetRepository` (`webhook_target_repo.go`), and `EventJournalRepository` (`event_journal_repo.go`).
3. **In-Memory Subscription Cache & Asynchronous Reload-on-Invalidation (ADR-004)**:
   - Implement thread-safe pod-local `SubscriptionCache` (`internal/infrastructure/cache/memory/subscription_cache.go`) guaranteeing **zero synchronous SQL reads on the hot CDC ingestion path**.
   - Implement **Reload-on-Invalidation** in `InvalidationSubscriber` (`internal/infrastructure/cache/redis/invalidation_sub.go`) listening on `tenant.config.updated`: when a tenant config mutation arrives, asynchronously fetch the updated tenant subscriptions and webhook targets from the cold-path PostgreSQL repositories and atomically swap the tenant's cache entry within `<= 100ms` (evicting if deleted), preventing false cache misses on subsequent hot-path CDC events.
   - Implement cold-path PostgreSQL 16+ / PgBouncer adapters (`internal/infrastructure/persistence/postgres/pool.go`, `subscription_repo.go`, `webhook_repo.go`) used strictly during pod cold-start warm-up (`WarmUp(ctx)`) and asynchronous invalidation reloads.
4. **CDC / Outbox Consumer Group & Hot-Path Event Processor**:
   - Implement `ProcessCDCEventHandler` (`internal/application/command/process_cdc_event.go`) evaluating ingested events strictly against `SubscriptionCache`, propagating OpenTelemetry W3C `trace_id`, pre-serializing SSE JSON frames once, and invoking downstream targeted routing (`NodeEventDispatcher` port) and webhook task enqueueing (`WebhookTaskEnqueuer` port, `P99 <= 15ms`) plus optional non-blocking cold event journaling.
   - Implement `CDCConsumer` (`internal/infrastructure/queue/kafka/cdc_consumer.go`) supporting Apache Kafka 3.7+ / stream broker message consumption across `rag.cdc.kb-catalog`, `rag.cdc.ingestion`, and `rag.cdc.chunk-lifecycle` with deterministic post-processing offset watermarks, poison-pill isolation (preventing corrupt payloads from stalling consumer lag past the 5,000-record alert threshold), context-aware shutdown, and Prometheus telemetry.
5. **Secured Logging (`log/slog`), Prometheus Metrics & Bootstrap Entry Point (`cmd/`)**:
   - Enforce structured JSON logging with automatic redaction (`ReplaceAttr` + `slog.LogValuer`) masking `authorization`, `token`, `bearer_token`, `secret`, `hmac_secret`, `signing_secret`, `secret_ref`, `password`, `dsn`, and sensitive `payload` / `payload_json` bodies as `"[REDACTED]"`.
   - Register the 6 canonical Prometheus metrics from the implementation plan (`notification_active_sse_connections`, `notification_events_processed_total`, `notification_client_write_latency_seconds`, `notification_dropped_frames_total`, `notification_webhook_enqueued_total`, `notification_cache_invalidations_total`).
   - Provide `cmd/notification-daemon/main.go` and `Makefile` so `./cmd/... ./internal/... ./test/unit/...` builds cleanly and passes all 5 GitHub Actions CI jobs (`lint-and-typecheck`, `security-and-logging-audit`, `openapi-and-doc-api-sync`, `test-suites`, `architecture-guardrails`).

---

## 2. Files to Create / Modify

| File Path | Layer | Purpose |
| :--- | :--- | :--- |
| `go.mod` | Root | Go 1.23+ module definition (`github.com/gemejime/rag-notification-dispatch-daemon`) |
| `Makefile` | Root | Build, lint, test (`-race`), OpenAPI sync, and benchmark targets aligned with `GOMEMLIMIT=1800MiB GOGC=80` |
| `cmd/notification-daemon/main.go` | Entry Point (Layer 1) | Runtime bootstrap (`GOMEMLIMIT=1800MiB`, `GOGC=80`), 12-Factor config loader, cache warm-up, CDC consumer wiring, and `SIGTERM` drain signal hook |
| `api/openapi/openapi.yaml` | API Contract | OpenAPI 3.1.0 specification synchronized 1:1 with `.ai/doc-api.md` |
| `api/proto/v1/notification_event.proto` | API Contract | Protobuf v3 CDC outbox event envelope schema (`NotificationEventEnvelope`, `Severity`) |
| `api/proto/v1/webhook_task.proto` | API Contract | Protobuf v3 downstream webhook task schema (`WebhookDeliveryTask`) |
| `api/proto/v1/contracts.pb.go` | API Contract | Go contract types, deterministic binary/JSON wire marshalers, and schema validators |
| `internal/domain/entity/notification.go` | Domain | `NotificationEvent`, `Severity`, zero-intermediary SSE JSON byte formatter, redacted `LogValue()` |
| `internal/domain/entity/subscription.go` | Domain | `Subscription` tenant routing preference and filter entity |
| `internal/domain/entity/webhook_target.go` | Domain | `WebhookTarget` entity, deterministic SHA-256 `task_id` builder, redacted `LogValue()` |
| `internal/domain/entity/connection.go` | Domain | `Connection` SSE client session aggregate (256-buffer watermarks, slow-consumer saturation timer) |
| `internal/domain/router/rule_evaluator.go` | Domain | Pure in-memory rule matching evaluator (event pattern, severity, suppression, SSE vs. webhook channels) |
| `internal/domain/router/filter.go` | Domain | Bounded TTL deduplication window and per-tenant token-bucket rate limiter |
| `internal/domain/repository/subscription_repo.go` | Domain Port | Cold-store & in-memory cache interfaces for tenant subscriptions |
| `internal/domain/repository/webhook_target_repo.go` | Domain Port | Cold-store & in-memory cache interfaces for tenant webhook targets |
| `internal/domain/repository/event_journal_repo.go` | Domain Port | Cold-path `notification_event_journal` & `Last-Event-ID` replay buffer interfaces |
| `internal/infrastructure/observability/logging.go` | Infrastructure | Secured `log/slog` JSON logger with `ReplaceAttr` secret/token/PII redaction and Prometheus metrics registry |
| `internal/infrastructure/cache/memory/subscription_cache.go` | Infrastructure | Thread-safe pod-local subscription & webhook target cache (0 SQL on hot path, atomic tenant swap) |
| `internal/infrastructure/cache/redis/invalidation_sub.go` | Infrastructure | `tenant.config.updated` Pub/Sub invalidation listener with async cold-store reload (`<= 100ms` coherence) |
| `internal/infrastructure/persistence/postgres/pool.go` | Infrastructure | Cold-path PgBouncer connection pool setup, health check, and redacted DSN config |
| `internal/infrastructure/persistence/postgres/subscription_repo.go` | Infrastructure | Cold-path SQL repository for `tenant_notification_subscriptions` |
| `internal/infrastructure/persistence/postgres/webhook_repo.go` | Infrastructure | Cold-path SQL repository for `tenant_webhook_targets` and `notification_event_journal` |
| `internal/application/command/process_cdc_event.go` | Application | Hot-path CDC ingestion command orchestrator using in-memory cache, direct SSE frame pre-serialization, and trace propagation |
| `internal/infrastructure/queue/kafka/cdc_consumer.go` | Infrastructure | Kafka / CDC consumer group ingestion engine with deterministic post-process offset tracking and poison-pill safety |
| `test/unit/phase1_ingestion_test.go` | Test | Unit, benchmark, and `-race` tests for rule evaluation (0 SQL), direct JSON serialization speed, cache reload-on-invalidation (`<= 100ms`), offset tracking, deduplication/rate-limiting, and log redaction |
| `.ai/architecture.md`, `.ai/doc-api.md`, `.ai/progress-tracker.md` | Documentation | Continuous Doc API synchronization & Phase 1 progress tracking |

---

## 3. Pressure-Tested Failure Modes & Mitigations

1. **CI Pipeline `./cmd/...` Test Execution Trap**:
   - `.github/workflows/ci.yml` executes `go test -v -race -coverprofile=coverage.txt ./cmd/... ./internal/... ./test/unit/...` whenever `go.mod` exists. Including `cmd/notification-daemon/main.go` in Phase 1 prevents `go test ./cmd/...` from failing on a missing `./cmd` package tree.
2. **Cache Invalidation Evict-Without-Reload Blackhole (ADR-004)**:
   - Because `ProcessCDCEventHandler` never queries PostgreSQL on a cache miss on the hot path, simply deleting a tenant key from `SubscriptionCache` upon receiving `tenant.config.updated` would cause all subsequent CDC events for that tenant to miss the cache and drop notifications. `InvalidationSubscriber` therefore reloads the tenant's latest rules from the cold-path PostgreSQL repository inside the background invalidation handler and atomically updates `SubscriptionCache` within `<= 100ms`.
3. **Zero Intermediary Serialization Overhead**:
   - `NotificationEventEnvelope.payload_json` arrives as pre-validated JSON bytes. `FormatSSEFrame()` constructs the SSE `data:` JSON envelope by writing header fields and raw `payload_json` bytes directly into a contiguous `[]byte` buffer—avoiding `json.Unmarshal` into `map[string]any` on the hot path.
4. **Unbounded Deduplication State Under `GOMEMLIMIT=1800MiB`**:
   - `Filter` in `internal/domain/router/filter.go` bounds its deduplication map capacity and evicts expired entries so high-throughput CDC streams cannot exhaust the `2GiB` pod memory ceiling.
5. **Poison-Pill CDC Event Partition Wedge**:
   - Malformed Protobuf messages or invalid event envelopes are logged with redacted metadata, counted in `notification_events_processed_total{status="malformed"}`, and safely skipped with offset advancement so a single bad record cannot stall consumer lag past 5,000 messages.

---

## 4. Verification & Git Commit Plan

1. Format and static-check all packages: `gofmt -s -w .` and `go vet ./...`.
2. Run unit tests and JSON serialization benchmarks with the race detector:
   `GOMEMLIMIT=1800MiB GOGC=80 go test -v -race ./cmd/... ./internal/... ./test/unit/...`
3. Verify CI checks locally (unstructured log check, secret redaction audit, `api/openapi/openapi.yaml` and `.ai/doc-api.md` sync).
4. Create **at least two Conventional Commits** (`feat:` for contracts, domain, cache, and CDC ingestion pipeline; `tests:` / `docs:` for unit/benchmark test suites and Doc API / progress tracker synchronization).
