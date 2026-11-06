# Dedicated Self-Hosted Bitnami Redis 7.2 Cluster Architecture

## 1. Executive Summary & Cluster Mission

The **`rag-notification-dispatch-daemon`** requires an ultra-low-latency, horizontally scalable distributed state and event backplane to achieve its core service level objectives:
- **$P99 \le 50\text{ms}$** End-to-end notification dispatch latency.
- **$\ge 100,000$** Cluster-wide concurrent SSE connection multiplexing.
- **$\le 100\text{ms}$** Asynchronous subscription cache invalidation and presence discovery across all pods.

To meet these invariants without the network overhead and cross-tenant noisy-neighbor variance of shared cloud PaaS offerings, the daemon utilizes a **dedicated in-cluster Bitnami Redis 7.2 Cluster** deployed directly inside the `rag-system` Kubernetes namespace.

---

## 2. Cluster Topology & Low-Latency Architecture

### 2.1 6-Node Sharded Topology (3 Primaries + 3 Secondaries)

The Redis cluster is provisioned with 6 stateful pods partitioned into 3 primary shards and 3 secondary replicas (`cluster.init: true`, `cluster.nodes: 6`, `cluster.replicas: 1`):

```
                   ┌────────────────────────────────────────┐
                   │    Kubernetes Client Pods              │
                   │ (rag-notification-dispatch-daemon)    │
                   └──────────────────┬─────────────────────┘
                                      │ In-Cluster Headless DNS
            ┌─────────────────────────┼─────────────────────────┐
            ▼                         ▼                         ▼
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│ Primary Shard 0       │ │ Primary Shard 1       │ │ Primary Shard 2       │
│ Slots 0 - 5460        │ │ Slots 5461 - 10922    │ │ Slots 10923 - 16383   │
│ Pod: ...-redis-0      │ │ Pod: ...-redis-1      │ │ Pod: ...-redis-2      │
└───────────┬───────────┘ └───────────┬───────────┘ └───────────┬───────────┘
            │ Replication             │ Replication             │ Replication
            ▼                         ▼                         ▼
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│ Secondary Replica 0   │ │ Secondary Replica 1   │ │ Secondary Replica 2   │
│ Pod: ...-redis-3      │ │ Pod: ...-redis-4      │ │ Pod: ...-redis-5      │
└───────────────────────┘ └───────────────────────┘ └───────────────────────┘
```

### 2.2 Why Dedicated In-Cluster Redis Achieves Sub-Millisecond ($P99 < 1\text{ms}$) Latency

1. **Zero External Network Hops**: Operating within the same Kubernetes VPC/overlay network eliminates cloud NAT gateways, AWS Transit Gateways, or VPC peering transit latency ($\Delta \approx 2\text{--}5\text{ms}$ eliminated).
2. **Direct Hash Slot Routing**: The daemon's Redis client caches the 16,384 cluster hash slots locally and connects directly to the owning primary shard via headless DNS rather than proxying through an intermediate load balancer.
3. **Noisy-NeighborPush Isolation**: Isolates targeted pod event queues (`pod.{node_id}.events`) and replay streams (`rag:notify:replay:{tenant_id}`) from other enterprise microservices, guaranteeing deterministic I/O performance.
4. **Soft Anti-Affinity**: Pods are scheduled across different Kubernetes worker nodes (`podAntiAffinityPreset: soft`) to ensure node failure survival without scheduling deadlock on smaller development clusters.

---

## 3. Headless DNS Naming & Service Routing Format

Bitnami Redis Cluster utilizes a Kubernetes Headless Service (`...-headless`) to publish direct pod endpoints. The canonical internal addresses for `rag-notification-dispatch-daemon` in namespace `rag-system` are:

```
# Primary Seed Nodes
rag-notification-dispatch-daemon-redis-0.rag-notification-dispatch-daemon-redis-headless.rag-system.svc.cluster.local:6379
rag-notification-dispatch-daemon-redis-1.rag-notification-dispatch-daemon-redis-headless.rag-system.svc.cluster.local:6379
rag-notification-dispatch-daemon-redis-2.rag-notification-dispatch-daemon-redis-headless.rag-system.svc.cluster.local:6379

# Secondary Replica Nodes
rag-notification-dispatch-daemon-redis-3.rag-notification-dispatch-daemon-redis-headless.rag-system.svc.cluster.local:6379
rag-notification-dispatch-daemon-redis-4.rag-notification-dispatch-daemon-redis-headless.rag-system.svc.cluster.local:6379
rag-notification-dispatch-daemon-redis-5.rag-notification-dispatch-daemon-redis-headless.rag-system.svc.cluster.local:6379
```

---

## 4. Local Development via Docker Compose

For local workstation development, a standalone Redis 7.2 container matching production engine parameters is provided at `deployments/docker/docker-compose.redis.yml`:

```bash
# 1. Start local Redis 7.2 container with AOF persistence & password auth
docker compose -f deployments/docker/docker-compose.redis.yml up -d

# 2. Verify container health status
docker compose -f deployments/docker/docker-compose.redis.yml ps

# 3. Connect via redis-cli
docker exec -it rag-notification-dispatch-daemon-redis-local redis-cli -a dev_redis_secret ping
# Expected output: PONG

# 4. Tear down container
docker compose -f deployments/docker/docker-compose.redis.yml down
```

---

## 5. Kubernetes & GitOps ArgoCD Deployment

### 5.1 One-Click Deployment Scripts

Automated deployment scripts are provided in both Bash and PowerShell:

**Bash (Linux / macOS / CI):**
```bash
chmod +x scripts/deploy_redis_cluster.sh
./scripts/deploy_redis_cluster.sh
```

**PowerShell (Windows / CI):**
```powershell
./scripts/deploy_redis_cluster.ps1
```

The script performs:
1. Verification of `helm` and `kubectl` dependencies.
2. Namespace creation (`rag-system`).
3. Shared secret validation (`rag-notification-dispatch-daemon-secrets`).
4. `helm upgrade --install` with `deployments/helm/redis-cluster/values.yaml`.
5. Rollout wait until all 6 StatefulSet pods reach `Ready`.
6. Health and slot verification (`redis-cli cluster info` -> `cluster_state:ok`, `cluster_slots_assigned:16384`).

### 5.2 Declarative GitOps via ArgoCD

To manage the Redis cluster continuously via ArgoCD, apply the application manifest:

```bash
kubectl apply -f deployments/argocd/redis-cluster-application.yaml
```

ArgoCD continuously reconciles the Bitnami chart against `https://kubernetes.default.svc` with automated drift healing (`selfHeal: true`) and pruning (`prune: true`).

---

## 6. Day-2 Operational Runbooks

### Runbook 1: Cluster Health & Topology Inspection

```bash
# Connect to pod 0 and query cluster state
kubectl exec -it rag-notification-dispatch-daemon-redis-0 -n rag-system -- bash -c '
REDIS_PASSWORD=$(cat /opt/bitnami/redis/etc/redis.conf | grep "^requirepass" | awk "{print \$2}" | tr -d "\"")
redis-cli -c -a "$REDIS_PASSWORD" cluster info
'

# Inspect node role assignments and hash slot distribution
kubectl exec -it rag-notification-dispatch-daemon-redis-0 -n rag-system -- bash -c '
REDIS_PASSWORD=$(cat /opt/bitnami/redis/etc/redis.conf | grep "^requirepass" | awk "{print \$2}" | tr -d "\"")
redis-cli -c -a "$REDIS_PASSWORD" cluster nodes
'
```

### Runbook 2: Real-Time Latency Verification ($P99 < 1\text{ms}$)

Run the built-in latency diagnostic tools against any pod:

```bash
# 1. Continuous latency monitor (press Ctrl+C to abort)
kubectl exec -it rag-notification-dispatch-daemon-redis-0 -n rag-system -- bash -c '
REDIS_PASSWORD=$(cat /opt/bitnami/redis/etc/redis.conf | grep "^requirepass" | awk "{print \$2}" | tr -d "\"")
redis-cli -a "$REDIS_PASSWORD" --latency
'

# 2. Intrinsic system hardware latency test (measures kernel/hypervisor jitter)
kubectl exec -it rag-notification-dispatch-daemon-redis-0 -n rag-system -- redis-cli --intrinsic-latency 5
```

### Runbook 3: Prometheus Metrics & Monitoring

Every cluster pod runs an embedded Prometheus exporter sidecar on port `9121`:

```bash
# Verify metrics scrape output from within the cluster
kubectl exec -it rag-notification-dispatch-daemon-redis-0 -c metrics -n rag-system -- curl -s http://localhost:9121/metrics | grep -E "redis_up|redis_connected_clients|redis_commands_total"
```

Standard Prometheus annotations enable automatic scraping by Prometheus Operator:
```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "9121"
prometheus.io/path: "/metrics"
```
Key operational metrics to alert on:
- `redis_up == 0`: Pod is down.
- `redis_cluster_state != 1`: Cluster degraded or unassigned slots.
- `redis_connected_clients > 10000`: Client connection spike.
