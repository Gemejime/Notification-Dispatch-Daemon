#!/usr/bin/env bash
# ==============================================================================
# Bitnami Redis 7.2 Cluster Automated Deployment & Verification Script
# Microservice: rag-notification-dispatch-daemon
# Namespace: rag-system
# Release: rag-notification-dispatch-daemon-redis
# ==============================================================================
set -euo pipefail

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SERVICE_NAME="rag-notification-dispatch-daemon"
RELEASE_NAME="${SERVICE_NAME}-redis"
NAMESPACE="rag-system"
VALUES_FILE="deployments/helm/redis-cluster/values.yaml"
SECRET_NAME="${SERVICE_NAME}-secrets"

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERROR]${NC} $1"; }

# 1. Verify helm and kubectl prerequisites
log_info "Verifying CLI dependencies (helm and kubectl)..."
if ! command -v helm &> /dev/null; then
  log_err "'helm' CLI is required but not installed in PATH. Please install Helm 3.8+."
  exit 1
fi

if ! command -v kubectl &> /dev/null; then
  log_err "'kubectl' CLI is required but not installed in PATH. Please install kubectl."
  exit 1
fi
log_ok "CLI prerequisites satisfied."

# 2. Ensure target namespace exists
log_info "Ensuring namespace '${NAMESPACE}' exists..."
if ! kubectl get namespace "${NAMESPACE}" &> /dev/null; then
  log_info "Creating namespace '${NAMESPACE}'..."
  kubectl create namespace "${NAMESPACE}"
  log_ok "Namespace '${NAMESPACE}' created."
else
  log_ok "Namespace '${NAMESPACE}' is present."
fi

# 3. Add or update Bitnami Helm repository
log_info "Ensuring Bitnami Helm repository is configured..."
if ! helm repo list | grep -q "bitnami"; then
  helm repo add bitnami https://charts.bitnami.com/bitnami
fi
helm repo update bitnami
log_ok "Bitnami repository is ready."

# 4. Check for existing secret containing REDIS_PASSWORD
log_info "Validating shared secret '${SECRET_NAME}' in namespace '${NAMESPACE}'..."
if ! kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" &> /dev/null; then
  log_warn "Secret '${SECRET_NAME}' not found. Generating temporary bootstrap secret..."
  BOOTSTRAP_PASSWORD=$(openssl rand -hex 16 2>/dev/null || date +%s | sha256sum | head -c 32)
  kubectl create secret generic "${SECRET_NAME}" \
    --namespace="${NAMESPACE}" \
    --from-literal=REDIS_PASSWORD="${BOOTSTRAP_PASSWORD}"
  log_ok "Bootstrap secret '${SECRET_NAME}' created with generated REDIS_PASSWORD."
else
  log_ok "Secret '${SECRET_NAME}' is present in namespace '${NAMESPACE}'."
fi

# 5. Execute Helm Upgrade/Install
log_info "Deploying Redis Cluster release '${RELEASE_NAME}' using '${VALUES_FILE}'..."
helm upgrade --install "${RELEASE_NAME}" bitnami/redis-cluster \
  --namespace "${NAMESPACE}" \
  --values "${VALUES_FILE}" \
  --wait \
  --timeout 10m0s
log_ok "Helm upgrade/install completed successfully."

# 6. Wait for StatefulSet rollout to complete
log_info "Waiting for StatefulSet '${RELEASE_NAME}' pods to reach Ready state..."
kubectl rollout status statefulset/"${RELEASE_NAME}" -n "${NAMESPACE}" --timeout=300s
log_ok "All StatefulSet replica pods are Ready."

# 7. Execute redis-cli cluster health verification
log_info "Inspecting Redis cluster health and slot allocation..."
POD_NAME="${RELEASE_NAME}-0"
REDIS_PASSWORD=$(kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.REDIS_PASSWORD}' | base64 --decode)

CLUSTER_INFO=$(kubectl exec -i -n "${NAMESPACE}" "${POD_NAME}" -- redis-cli -c -a "${REDIS_PASSWORD}" cluster info 2>/dev/null || true)

if echo "${CLUSTER_INFO}" | grep -q "cluster_state:ok"; then
  log_ok "Redis Cluster state: OK"
else
  log_err "Redis Cluster state is NOT ok:\n${CLUSTER_INFO}"
  exit 1
fi

ASSIGNED_SLOTS=$(echo "${CLUSTER_INFO}" | grep "cluster_slots_assigned" | tr -d '\r' | cut -d':' -f2)
if [ "${ASSIGNED_SLOTS}" = "16384" ]; then
  log_ok "All 16,384 cluster hash slots successfully assigned."
else
  log_err "Unexpected assigned hash slots count: ${ASSIGNED_SLOTS} (expected 16384)."
  exit 1
fi

log_ok "Redis Cluster deployment verified successfully! Endpoints:"
for i in {0..2}; do
  echo "  - ${RELEASE_NAME}-${i}.${RELEASE_NAME}-headless.${NAMESPACE}.svc.cluster.local:6379"
done
