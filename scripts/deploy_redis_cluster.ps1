<#
.SYNOPSIS
    Automated Deployment & Health Verification Script for Bitnami Redis 7.2 Cluster.
.DESCRIPTION
    Deploys a dedicated 6-node Bitnami Redis 7.2 Cluster for rag-notification-dispatch-daemon
    in the rag-system namespace, waits for StatefulSet readiness, and verifies 16,384 cluster slots.
#>
[CmdletBinding()]
param(
    [string]$ServiceName = "rag-notification-dispatch-daemon",
    [string]$Namespace = "rag-system",
    [string]$ValuesFile = "deployments/helm/redis-cluster/values.yaml"
)

$ErrorActionPreference = "Stop"

$ReleaseName = "$ServiceName-redis"
$SecretName = "$ServiceName-secrets"

function Write-Info { param([string]$Message) Write-Host "[INFO] $Message" -ForegroundColor Cyan }
function Write-Ok   { param([string]$Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Warn { param([string]$Message) Write-Host "[WARN] $Message" -ForegroundColor Yellow }
function Write-Err  { param([string]$Message) Write-Host "[ERROR] $Message" -ForegroundColor Red }

# 1. Verify helm and kubectl prerequisites
Write-Info "Verifying CLI tools (helm and kubectl)..."
if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
    Write-Err "'helm' executable was not found in PATH. Please install Helm 3.8+."
    exit 1
}
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Err "'kubectl' executable was not found in PATH. Please install kubectl."
    exit 1
}
Write-Ok "CLI prerequisites satisfied."

# 2. Ensure target namespace exists
Write-Info "Ensuring namespace '$Namespace' exists..."
$nsCheck = kubectl get namespace $Namespace 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Info "Creating namespace '$Namespace'..."
    kubectl create namespace $Namespace | Out-Null
    Write-Ok "Namespace '$Namespace' created."
} else {
    Write-Ok "Namespace '$Namespace' is present."
}

# 3. Add or update Bitnami repository
Write-Info "Ensuring Bitnami Helm repo is registered..."
$repoList = helm repo list 2>&1
if ($repoList -notmatch "bitnami") {
    helm repo add bitnami https://charts.bitnami.com/bitnami | Out-Null
}
helm repo update bitnami | Out-Null
Write-Ok "Bitnami Helm repo updated."

# 4. Check for shared secret
Write-Info "Checking for shared secret '$SecretName' in namespace '$Namespace'..."
$secretCheck = kubectl get secret $SecretName -n $Namespace 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Warn "Secret '$SecretName' not found. Generating temporary bootstrap secret..."
    $randomPass = [System.Guid]::NewGuid().ToString("N")
    kubectl create secret generic $SecretName -n $Namespace --from-literal=REDIS_PASSWORD=$randomPass | Out-Null
    Write-Ok "Bootstrap secret '$SecretName' generated."
} else {
    Write-Ok "Secret '$SecretName' is present."
}

# 5. Deploy Helm Chart
Write-Info "Executing Helm upgrade --install for '$ReleaseName'..."
helm upgrade --install $ReleaseName bitnami/redis-cluster `
    --namespace $Namespace `
    --values $ValuesFile `
    --wait `
    --timeout 10m0s
if ($LASTEXITCODE -ne 0) {
    Write-Err "Helm installation failed."
    exit 1
}
Write-Ok "Helm upgrade/install completed successfully."

# 6. Wait for StatefulSet rollout
Write-Info "Waiting for StatefulSet '$ReleaseName' to become Ready..."
kubectl rollout status statefulset/$ReleaseName -n $Namespace --timeout=300s
if ($LASTEXITCODE -ne 0) {
    Write-Err "StatefulSet failed to reach Ready status in time."
    exit 1
}
Write-Ok "StatefulSet pods are all Ready."

# 7. Verify cluster info & slot assignment
Write-Info "Verifying Redis Cluster slot allocation..."
$b64Pass = kubectl get secret $SecretName -n $Namespace -o jsonpath="{.data.REDIS_PASSWORD}"
$redisPass = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($b64Pass))

$podName = "$ReleaseName-0"
$clusterInfo = kubectl exec -i -n $Namespace $podName -- redis-cli -c -a $redisPass cluster info 2>$null

if ($clusterInfo -match "cluster_state:ok") {
    Write-Ok "Redis Cluster state: OK"
} else {
    Write-Err "Redis Cluster state is NOT ok:`n$clusterInfo"
    exit 1
}

if ($clusterInfo -match "cluster_slots_assigned:16384") {
    Write-Ok "All 16,384 cluster hash slots successfully assigned."
} else {
    Write-Err "Not all cluster hash slots are assigned:`n$clusterInfo"
    exit 1
}

Write-Ok "Redis Cluster deployment verified successfully! Pod endpoints:"
for ($i = 0; $i -lt 3; $i++) {
    Write-Host "  - $ReleaseName-$i.$ReleaseName-headless.$Namespace.svc.cluster.local:6379" -ForegroundColor Gray
}
