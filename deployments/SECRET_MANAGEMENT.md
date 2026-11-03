# Secret Management & CI/CD Injection Architecture

## 1. Executive Summary & Core Invariants

The **`rag-notification-dispatch-daemon`** is a high-density, low-latency ($P99 \le 50\text{ms}$) Go microservice responsible for real-time notification dispatch, change-data-capture (CDC) outbox event consumption, multiplexed Server-Sent Events (SSE) streaming, and asynchronous webhook task enqueueing.

To maintain a zero-trust security posture and comply with SOC 2, ISO 27001, and the authoritative architectural directives defined in `GEMINI.MD`, this repository enforces three non-negotiable security invariants:

1. **Zero Secrets in Git**: Plaintext passwords, authentication bearer tokens, signing keys, and connection strings containing embedded credentials must **never** be committed to Git or written into `deployments/helm/values.yaml` or ConfigMaps.
2. **Decoupled Configuration & Credential Planes**:
   - **Non-sensitive runtime configuration** (`ENVIRONMENT`, `HTTP_PORT`, `PORT`, `LOG_LEVEL`, `KAFKA_BROKERS`, `SSE_RING_BUFFER_SIZE`, `GOMEMLIMIT`, `GOGC`) is driven by `.env.prod` as the single source of truth and rendered into Kubernetes `ConfigMap` resources.
   - **Sensitive credentials** (`REDIS_PASSWORD`, `POSTGRES_PASSWORD`, `POSTGRES_DSN`, `CHANNEL_AUTH_SECRET`, `JWT_PUBLIC_KEY_PEM`) are strictly isolated within Kubernetes `Secret` resources (`rag-notification-dispatch-daemon-secrets`) or cloud secret managers.
3. **Automated Drift & Secret Filtering Gates**: Git pre-commit hooks and GitHub Actions CI pipelines validate zero configuration drift while automatically redacting all keys matching `PASSWORD`, `SECRET`, `TOKEN`, or `KEY` before updating deployment manifests.

---

## 2. Secrets Inventory & Classification Matrix

| Secret Identifier | Classification | Description | Ingestion Target | Rotation Frequency |
| :--- | :--- | :--- | :--- | :--- |
| `REDIS_PASSWORD` | **Critical** | Password/token for authenticated Redis 7.2 Cluster backplane (`pod.{node_id}.events`, presence registry, replay buffers). | K8s Secret (`rag-notification-dispatch-daemon-secrets`) | 90 days / On-demand |
| `POSTGRES_PASSWORD` | **Critical** | Password for PgBouncer / PostgreSQL 16+ cold-path storage (`tenant_notification_subscriptions`, `notification_event_journal`). | K8s Secret (`rag-notification-dispatch-daemon-secrets`) | 90 days / On-demand |
| `POSTGRES_DSN` | **Critical** | Full connection string with embedded credentials (`postgres://user:pass@host:port/db?sslmode=verify-full`). | K8s Secret (`rag-notification-dispatch-daemon-secrets`) | 90 days / On-demand |
| `CHANNEL_AUTH_SECRET` | **Critical** | Shared secret / HMAC signing key for channel token authentication and webhook metadata verification. | K8s Secret (`rag-notification-dispatch-daemon-secrets`) | 90 days / On-demand |
| `JWT_PUBLIC_KEY_PEM` | **High** | RSA/ECDSA public key PEM string for verifying SSE client bearer tokens at `/v1/notifications/stream`. | K8s Secret (`rag-notification-dispatch-daemon-secrets`) | 180 days / On-demand |
| `KUBECONFIG` | **Critical** | Cluster administrative service account credentials granting deployment permissions to namespace `rag-system`. | GitHub Actions Environment Secret | 180 days |
| `GHCR_PAT` | **High** | GitHub Personal Access Token or Robot account with `write:packages` permission for container registry. | GitHub Actions Repository Secret | 90 days |

---

## 3. Secret Injection Architecture

```
                          ┌────────────────────────┐
                          │   Developer Machine    │
                          │   Local .env (Ignored) │
                          └───────────┬────────────┘
                                      │ git push
                                      ▼
                        ┌────────────────────────────┐
                        │   GitHub Actions CI/CD     │
                        │ ┌────────────────────────┐ │
                        │ │ Drift Check & Masking  │ │
                        │ └───────────┬────────────┘ │
                        │             │ Injects GitHub Secrets
                        └─────────────┼──────────────┘
                                      │
            ┌─────────────────────────┴─────────────────────────┐
            ▼                                                   ▼
┌───────────────────────────────┐               ┌───────────────────────────────┐
│     Kubernetes ConfigMap      │               │       Kubernetes Secret       │
│ rag-notification-daemon-config│               │ rag-notification-daemon-secret│
├───────────────────────────────┤               ├───────────────────────────────┤
│ PORT: "8080"                  │               │ REDIS_PASSWORD: "secret-token"│
│ GRPC_PORT: "50051"            │               │ POSTGRES_PASSWORD: "pg-pass"  │
│ LOG_LEVEL: "INFO"             │               │ CHANNEL_AUTH_SECRET: "hmac-k" │
│ SSE_RING_BUFFER_SIZE: "256"   │               │ JWT_PUBLIC_KEY_PEM: "-----..."│
│ KAFKA_BROKERS: "cluster:9092" │               └───────────────┬───────────────┘
└───────────────┬───────────────┘                               │
                │                                               │
                └───────────────────────┬───────────────────────┘
                                        │ envFrom (Secret overrides ConfigMap)
                                        ▼
                        ┌───────────────────────────────┐
                        │       Pod Container           │
                        │ (rag-notification-dispatch)  │
                        └───────────────────────────────┘
```

---

## 4. CI/CD Secret Injection via GitHub Actions

Deployment automation is governed by [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml).

### Step-by-Step Injection Pipeline

1. **Pre-Deployment Drift Enforcement**:
   - The workflow executes `python scripts/sync_env_to_deployments.py --check-only` on the runner.
   - Any uncommitted drift between `.env.prod` and `deployments/helm/values.yaml` aborts the pipeline before any secrets are accessed.

2. **Log Masking Guard**:
   - All environment secrets are explicitly registered with GitHub Actions log masking:
     ```bash
     echo "::add-mask::$REDIS_PASSWORD"
     echo "::add-mask::$POSTGRES_PASSWORD"
     echo "::add-mask::$CHANNEL_AUTH_SECRET"
     echo "::add-mask::$JWT_PUBLIC_KEY_PEM"
     ```
   - Prevents accidental credential leakage into build summaries, debug output, or execution error traces.

3. **Dynamic Kubernetes Secret Provisioning**:
   - Injects credentials directly into the target namespace (`rag-system`):
     ```bash
     kubectl create secret generic rag-notification-dispatch-daemon-secrets \
       --namespace rag-system \
       --from-literal=REDIS_PASSWORD="${REDIS_PASSWORD}" \
       --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
       --from-literal=CHANNEL_AUTH_SECRET="${CHANNEL_AUTH_SECRET}" \
       --from-literal=JWT_PUBLIC_KEY_PEM="${JWT_PUBLIC_KEY_PEM}" \
       --dry-run=client -o yaml | \
     kubectl annotate --local -f - "meta.helm.sh/release-name=rag-notification-dispatch-daemon" "meta.helm.sh/release-namespace=rag-system" -o yaml | \
     kubectl label --local -f - "app.kubernetes.io/managed-by=Helm" -o yaml | \
     kubectl apply -f -
     ```

4. **Helm Release Deployment with Dynamic Overrides**:
   - Deploys the release, dynamically referencing the managed secret:
     ```bash
     helm upgrade --install rag-notification-dispatch-daemon deployments/helm/rag-notification-dispatch-daemon \
       --namespace rag-system \
       --create-namespace \
       --wait \
       --timeout 5m0s
     ```

5. **Automated Rolling Restart Verification**:
   - The checksum annotation `checksum/config` in `deployment.yaml` guarantees that pods automatically restart when configuration changes.
   - The pipeline monitors rollout progress:
     ```bash
     kubectl rollout status deployment/rag-notification-dispatch-daemon -n rag-system --timeout=120s
     ```

---

## 5. Production Secret Providers & External Secrets Operator

For enterprises utilizing dedicated KMS / Vault solutions (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault), this daemon supports native integration via the **External Secrets Operator (ESO)**:

### 1. Enable External Secret in `values.yaml`

```yaml
secret:
  create: false
  externalSecret:
    enabled: true
    name: "rag-notification-dispatch-daemon-secrets"
```

### 2. ExternalSecret CRD Manifest

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: rag-notification-dispatch-daemon-secrets
  namespace: rag-system
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: rag-notification-dispatch-daemon-secrets
    creationPolicy: Owner
  data:
    - secretKey: REDIS_PASSWORD
      remoteRef:
        key: secret/data/rag/redis
        property: password
    - secretKey: POSTGRES_PASSWORD
      remoteRef:
        key: secret/data/rag/postgres
        property: password
    - secretKey: CHANNEL_AUTH_SECRET
      remoteRef:
        key: secret/data/rag/auth
        property: channel_secret
    - secretKey: JWT_PUBLIC_KEY_PEM
      remoteRef:
        key: secret/data/rag/jwt
        property: public_key
```

---

## 6. Zero-Downtime Secret Rotation Runbook

### A. Redis Cluster Password Rotation
1. Update Redis cluster to accept both old and new credentials (dual-auth mode).
2. Update `REDIS_PASSWORD` in GitHub Actions Secrets or Vault.
3. Trigger the CD pipeline (`gh workflow run deploy.yml -f environment=production`).
4. Daemon pods perform staggered graceful draining (ADR-005: 30–60s randomized jitter window) and reconnect with the new credentials.
5. Decommission the old password in Redis Cluster.

### B. PostgreSQL PgBouncer Password Rotation
1. Create new user password in PostgreSQL / PgBouncer auth file.
2. Update `POSTGRES_PASSWORD` in GitHub Secrets / Vault.
3. Trigger CD pipeline rollout.
4. Revoke the old password in PostgreSQL after all daemon pods have restarted.

### C. JWT / Channel Authentication Secret Rotation
1. Auth service issues JWTs signed with Key Pair B while continuing to accept Key Pair A.
2. Update `JWT_PUBLIC_KEY_PEM` / `CHANNEL_AUTH_SECRET` in secret store.
3. Deploy updated secret. Daemon validates against new key pair immediately upon pod restart.
4. Decommission Key Pair A after all short-lived client tokens expire.

---

## 7. Emergency Incident Response & Credential Revocation

In the event of a suspected credential leak:

1. **Immediate Revocation**: Change the credential directly in the upstream datastore (Redis, Postgres, Auth Service).
2. **Secret Store Update**: Update the secret in GitHub Actions Secrets and cloud KMS/Vault.
3. **Trigger Immediate Rollout**:
   ```bash
   kubectl rollout restart deployment/rag-notification-dispatch-daemon -n rag-system
   ```
4. **Audit Logs Verification**: Review `log/slog` structured output in OpenTelemetry / CloudWatch to ensure no leaked plaintext secrets appear in query strings or exception messages.

