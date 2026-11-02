# Implementation Plan: Phase 4 — Resilience, Chaos & Production Rollout (`rag-notification-dispatch-daemon`)

## 1. Objectives & Architectural Scope (Pressure-Tested Against `tasks/` & `.ai/`)

Implement **Phase 4: Resilience, Chaos & Production Rollout** from `RAG Notification Dispatch Daemon Service Implementation Plan.pdf`, `Production Grade Backend Concepts.pdf`, `Go and Python Microservices Architecture.pdf`, and `.ai/progress-tracker.md`, adhering strictly to `GEMINI.MD`, `.ai/architecture.md` (ADRs 001–005), `.ai/doc-api.md`, `.ai/code-standards.md`, and `.ai/security.md`:

1. **High-Density Memory Soak & Benchmarking (`test/performance/high_density_soak_test.go`)**:
   - Validate high concurrent connection density: sustain $\ge 20,000$ active client connections per pod node ($\ge 100,000$ cluster-wide) under continuous broadcast event bursts.
   - Enforce memory footprint ceiling: sockets allocate $\sim 14\text{--}16\text{KiB}$ baseline ($8\text{KiB}$ kernel TCP buffers, $4\text{KiB}$ goroutine stacks, $2\text{KiB}$ ring buffer); idle footprint $\sim 320\text{MiB}$ for $20,000$ sockets; Resident Set Size (RSS) strictly remains $< 1.6\text{GiB}$ inside the $2\text{GiB}$ container limit under `GOMEMLIMIT=1800MiB` and `GOGC=80` without triggering OOMKills.
   - Implement dual-mode soak architecture to protect CI runner file descriptor ceilings:
     - Synthetic In-Memory Soak: 20,000 goroutines coupled with in-memory non-blocking mock stream writers profiling exact memory allocation, GC sweep frequency, and verifying that RSS stays bounded $< 1.6\text{GiB}$.
     - Configurable TCP Socket Soak: Loopback network stress harness supporting `-connections=20000` with automated scaled fallback to 2,000 connections under standard `go test -short`.
   - Benchmark ring buffer throughput, memory allocations per event dispatch ($0$ allocation hot-path routing), and verify non-terminal progress frame drop invariants (`reason="ring_buffer_saturated"`) while terminal state transitions are strictly retained.

2. **Chaos Testing & Graceful Drain Verification (`test/performance/chaos_drain_test.go` & `test/performance/slow_loris_test.go`)**:
   - **Staggered Graceful Draining Chaos (`ADR-005`)**:
     - Simulate `SIGTERM` on a daemon node holding high-density active streams.
     - Assert `/ready` flips immediately to `503 Service Unavailable` so ingress traffic stops routing to the terminating pod.
     - Verify emission of SSE `retry: <ms>\n\n` directive frames with randomized reconnection jitter spread across a $[30\text{s}, 60\text{s}]$ window (`terminationGracePeriodSeconds: 75`).
     - Assert sockets close gracefully in staggered fashion without abrupt drops or thundering herd reconnection spikes against surviving pods.
     - Validate `Last-Event-ID` reconnection backfill against remaining cluster pods with Redis CPU remaining $< 60\%$.
   - **Slow-Loris & Stalled Consumer Chaos (`test/performance/slow_loris_test.go`)**:
     - Inject **2,000 slow/throttled consumers** with zeroed TCP receive windows (strictly adhering to spec §6); verify that buffer saturation ceiling engages after $> 5\text{s}$ (`reason="slow_consumer_evicted"`) and closes the connection cleanly without blocking the broadcast loop or impacting healthy sibling subscribers.

3. **Multi-Stage Distroless Containerization (`deployments/docker/Dockerfile`)**:
   - Multi-stage build leveraging `golang:1.23-bookworm` with Go build flags: `-trimpath -ldflags="-s -w"`.
   - Runtime base: `gcr.io/distroless/static:nonroot` (unprivileged `nonroot:nonroot`, UID:GID `65532:65532`).
   - Zero shell, zero package manager, and minimal attack surface in the serving container.
   - Hardened environment variable defaults: `GOMEMLIMIT=1800MiB`, `GOGC=80`, `PORT=8080`.
   - Pod lifecycle invariant: Because distroless contains no shell (`/bin/sh` does not exist), container termination must NOT rely on shell-based `preStop` hooks (e.g. `sleep 5`). Termination and immediate `/ready` 503 flipping are handled strictly inside Go's runtime signal listener.

4. **Production Kubernetes Helm Chart (`deployments/helm/rag-notification-dispatch-daemon/`) & GitOps Strategy**:
   - **Separation of Concerns & GitOps Delivery**:
     - This repository packages the authoritative, versioned Helm v3 chart and executes CI quality/verification gates.
     - ArgoCD natively consumes `deployments/helm/rag-notification-dispatch-daemon/` without circular dependencies.
     - Declarative reference ArgoCD Application manifest (`deployments/argo/application.yaml`) provided with automated sync, prune, self-healing, and namespace creation.
   - **Production-Ready Helm Chart Manifests**:
     - `deployment.yaml`:
       - Minimum 3 replicas with pod anti-affinity across failure zones (`topologyKey: topology.kubernetes.io/zone`) and nodes (`kubernetes.io/hostname`).
       - Node affinity targeting memory-optimized, compute-light Kubernetes node groups (`workload: memory-optimized-io`, per `Go and Python Microservices Architecture.pdf`).
       - Zero-downtime rolling update strategy: `maxSurge: 25%`, `maxUnavailable: 0` to preserve connection capacity during rollouts.
       - Resource requests: CPU `500m`, Memory `1GiB`; Resource limits: CPU `2000m`, Memory `2GiB`.
       - Pod lifecycle: `terminationGracePeriodSeconds: 75` to allow full $60\text{s}$ connection draining.
       - SecurityContext: `runAsNonRoot: true`, `runAsUser: 65532`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities: drop: ["ALL"]`.
       - Liveness and readiness probes pointing to `/healthz` and `/ready`.
     - `pdb.yaml`: `PodDisruptionBudget` configured with `minAvailable: 2`.
     - `hpa.yaml`: Kubernetes `autoscaling/v2` `HorizontalPodAutoscaler` targeting:
       - CPU utilization at $65\%$ (`type: Resource`).
       - Socket density $\le 15,000$ active connections per pod (`type: Pods`, metric: `notification_active_sse_connections`).
     - `servicemonitor.yaml`: Prometheus Operator ServiceMonitor scraping `/metrics` every $15\text{s}$.
     - `prometheusrule.yaml`: Prometheus Operator alert rules defining the 4 canonical production alerts:
       1. *High Client Write Latency*: P99 write latency $> 50\text{ms}$ over 3 minutes.
       2. *CDC Ingestion Lag*: Topic consumer lag $> 5,000$ records for $> 2\text{ minutes}$.
       3. *Elevated Drop Frame Rate*: Dropped frame rate $> 1\%$ of emitted frames over 5 minutes.
       4. *Near-OOM Memory Saturation*: Working set $> 1.8\text{GiB}$ for $> 1\text{ minute}$.
     - `service.yaml`: ClusterIP service exposing port 8080 for HTTP/SSE and metric scraping.
     - `configmap.yaml` & `secret.yaml`: 12-factor configuration templates with external KMS/Vault secret reference placeholders.

5. **Operational Verification & Documentation Synchronization**:
   - Update `Makefile` with targets for `docker-build`, `test-performance`, `helm-lint`, and `soak-test`.
   - Synchronize `api/openapi/openapi.yaml`, `.ai/doc-api.md`, and `.ai/progress-tracker.md` to reflect Phase 4 completion and production readiness.

---

## 2. Files to Create / Modify

| File Path | Layer | Purpose |
| :--- | :--- | :--- |
| `deployments/docker/Dockerfile` | Deployment (Docker) | Multi-stage distroless container image (`gcr.io/distroless/static:nonroot`, `GOMEMLIMIT=1800MiB`, `GOGC=80`) |
| `deployments/helm/rag-notification-dispatch-daemon/Chart.yaml` | Deployment (Helm) | Helm chart metadata (v2, appVersion 1.0.0) |
| `deployments/helm/rag-notification-dispatch-daemon/values.yaml` | Deployment (Helm) | Production default values (resources, replicas=3, PDB minAvailable=2, HPA, anti-affinity, probe timings, node affinity) |
| `deployments/helm/rag-notification-dispatch-daemon/templates/deployment.yaml` | Deployment (Helm) | Kubernetes Deployment with 75s grace period, `maxUnavailable: 0`, distroless non-root security context, env vars, probes |
| `deployments/helm/rag-notification-dispatch-daemon/templates/service.yaml` | Deployment (Helm) | Kubernetes Service definition (port 8080) |
| `deployments/helm/rag-notification-dispatch-daemon/templates/pdb.yaml` | Deployment (Helm) | PodDisruptionBudget with minAvailable: 2 |
| `deployments/helm/rag-notification-dispatch-daemon/templates/hpa.yaml` | Deployment (Helm) | HorizontalPodAutoscaler (CPU 65%, `notification_active_sse_connections` <= 15,000) |
| `deployments/helm/rag-notification-dispatch-daemon/templates/servicemonitor.yaml` | Deployment (Helm) | Prometheus Operator ServiceMonitor for `/metrics` (15s scrape interval) |
| `deployments/helm/rag-notification-dispatch-daemon/templates/prometheusrule.yaml` | Deployment (Helm) | Prometheus Operator alert rules (P99 write latency, CDC lag, frame drop %, near-OOM) |
| `deployments/helm/rag-notification-dispatch-daemon/templates/configmap.yaml` | Deployment (Helm) | ConfigMap with 12-factor runtime configuration |
| `deployments/helm/rag-notification-dispatch-daemon/templates/secret.yaml` | Deployment (Helm) | Secret template with external secret store integration references |
| `deployments/argo/application.yaml` | Deployment (ArgoCD) | Reference ArgoCD Application manifest with automated sync, prune, selfHeal, and namespace creation |
| `test/performance/high_density_soak_test.go` | Test (Performance) | 20k-per-pod concurrent SSE streaming soak test, RSS memory bound validation (<1.6GiB), throughput benchmarks |
| `test/performance/chaos_drain_test.go` | Test (Performance) | SIGTERM graceful draining chaos test, readiness probe 503 check, 30–60s jitter verification, reconnect recovery |
| `test/performance/slow_loris_test.go` | Test (Performance) | 2,000-client Slow-Loris throttled TCP client eviction after >5s saturation |
| `Makefile` | Build & Automation | Add targets for `test-performance`, `docker-build`, `helm-lint`, `soak-test` |
| `.ai/doc-api.md`, `api/openapi/openapi.yaml`, `.ai/progress-tracker.md` | Documentation | Continuous Doc API & OpenAPI 3.1 synchronization, Phase 4 roadmap updates |

---

## 3. Failure Modes & Production Guardrails

1. **Memory Ceiling & GC Pressure Under Concurrency**:
   - `GOMEMLIMIT=1800MiB` and `GOGC=80` guarantee Go runtime triggers aggressive GC before container reaches the 2GiB hard cgroup limit. Soak tests explicitly monitor `runtime.ReadMemStats` allocations during 20,000 connection bursts.
2. **Thundering Herd Suppression During Rolling Restarts**:
   - On `SIGTERM`, clients are assigned a randomized retry duration between 30,000ms and 60,000ms via `retry: <ms>\n\n`. When connections close, reconnection bursts are evenly distributed across surviving pods, preventing Redis CPU spikes past 60%.
3. **Slow-Loris and Stalled Network Desync**:
   - 2,000 throttled client connections that do not read events within 5s are forcefully terminated by `RingBuffer` and `ConnectionHub` sweeps, preventing slow clients from consuming kernel socket buffers or pod memory.
4. **Non-Root & Shell-Free Distroless Security**:
   - Distroless container runs as UID 65532 (`nonroot`) with `readOnlyRootFilesystem: true` and all Linux capabilities dropped (`drop: ["ALL"]`). Termination lifecycle is handled purely in Go runtime signal channels, avoiding broken shell `preStop` exec dependencies.
5. **GitOps Zero-Downtime Rolling Restarts**:
   - ArgoCD syncs enforce `maxUnavailable: 0` and `maxSurge: 25%` in `deployment.yaml`. Replacement pods must pass `/ready` before Kubernetes terminates old instances holding active push streams.

---

## 4. Pre-Commit Verification & Git Commit Plan

1. Verify formatting: `gofmt -s -w .` and `go vet ./...`.
2. Execute all unit, integration, and performance test suites:
   `go test -v ./internal/... ./test/unit/... ./test/integration/... ./test/performance/... ./cmd/...`
3. Verify Helm templates syntax (`helm lint`) and Dockerfile structure.
4. Confirm zero drift between `.ai/doc-api.md`, `api/openapi/openapi.yaml`, and `.ai/progress-tracker.md`.
5. Execute git commits adhering strictly to Conventional Commits:
   - Commit 1: `feat: implement phase 4 production helm charts, distroless dockerfile, and build targets`
   - Commit 2: `tests: and docs: add high-density soak, chaos draining, and slow-loris test suites with production roadmap updates`
