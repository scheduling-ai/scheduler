#!/usr/bin/env bash
# Bootstrap the observed cluster (cluster #2) and wire it up to the
# scheduler-plane cluster (cluster #1).
#
# Idempotent.  Safe to re-run after `terraform apply` recreates either
# cluster, after Kueue is bumped, or after the SA token is rotated.
#
# Steps:
#   1. Fetch kubeconfig for cluster #2.
#   2. Apply observer RBAC + Kueue install + queue config there.
#   3. Mint a long-lived token for the scheduler-observer ServiceAccount.
#   4. Build a kubeconfig pointing at cluster #2's public master with
#      that token, and write it as the `observed-cluster-kubeconfig`
#      Secret in cluster #1's `scheduler-system` namespace.
#
# After this runs, `scripts/deploy.sh` will roll out the
# `k8s-bridge-observed` Deployment (it currently no-ops when the Secret
# is missing).
#
# Prereqs:
#   - terraform apply must have created the `scheduler-observed` cluster.
#   - gcloud + kubectl + jq + gke-gcloud-auth-plugin on PATH.
#   - You're authenticated to the GCP project (gcloud auth login).
set -euo pipefail
cd "$(dirname "$0")/.."

KUEUE_VERSION="${KUEUE_VERSION:-v0.17.2}"

# ----------------------------------------------------------------- inputs ---
PROJECT_ID="$(cd infra && terraform output -raw project_id)"
ZONE="$(cd infra && terraform output -raw zone)"
SCHED_CLUSTER="$(cd infra && terraform output -raw cluster_name)"
OBS_CLUSTER="$(cd infra && terraform output -raw observed_cluster_name)"
OBS_ENDPOINT="$(cd infra && terraform output -raw observed_cluster_endpoint)"
OBS_CA="$(cd infra && terraform output -raw observed_cluster_ca_certificate)"

echo "==> Fetching kubectl credentials for both clusters"
gcloud container clusters get-credentials "${SCHED_CLUSTER}" \
  --zone "${ZONE}" --project "${PROJECT_ID}" >/dev/null
SCHED_CTX="gke_${PROJECT_ID}_${ZONE}_${SCHED_CLUSTER}"

gcloud container clusters get-credentials "${OBS_CLUSTER}" \
  --zone "${ZONE}" --project "${PROJECT_ID}" >/dev/null
OBS_CTX="gke_${PROJECT_ID}_${ZONE}_${OBS_CLUSTER}"

# ----------------------------------------------------- observed cluster ---
echo "==> Applying observer RBAC to ${OBS_CLUSTER}"
kubectl --context="${OBS_CTX}" apply -f infra/k8s/observed-cluster/observer-rbac.yaml

echo "==> Installing Kueue ${KUEUE_VERSION} on ${OBS_CLUSTER}"
kubectl --context="${OBS_CTX}" apply --server-side -f \
  "https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml"
kubectl --context="${OBS_CTX}" -n kueue-system rollout status \
  deploy/kueue-controller-manager --timeout=3m

echo "==> Applying Kueue queue config"
kubectl --context="${OBS_CTX}" apply -f infra/k8s/observed-cluster/kueue/queues.yaml

# Mint a long-lived token for the observer SA via a backing Secret.
# Modern k8s (>=1.24) doesn't auto-issue SA token Secrets; we create one
# explicitly and let the token controller populate `data.token`.
echo "==> Provisioning scheduler-observer token Secret on ${OBS_CLUSTER}"
kubectl --context="${OBS_CTX}" apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: scheduler-observer-token
  namespace: scheduler-observer
  annotations:
    kubernetes.io/service-account.name: scheduler-observer
type: kubernetes.io/service-account-token
EOF

# Wait for the controller to populate the token field.
TOKEN=""
for _ in $(seq 1 30); do
  TOKEN_B64="$(kubectl --context="${OBS_CTX}" -n scheduler-observer \
    get secret scheduler-observer-token -o jsonpath='{.data.token}' 2>/dev/null || true)"
  if [[ -n "${TOKEN_B64}" ]]; then
    TOKEN="$(printf '%s' "${TOKEN_B64}" | base64 -d)"
    break
  fi
  sleep 1
done
if [[ -z "${TOKEN}" ]]; then
  echo "ERROR: token never populated in scheduler-observer-token" >&2
  exit 1
fi

# ------------------------------------------ kubeconfig + Secret on cluster #1 ---
echo "==> Writing observed-cluster-kubeconfig Secret to ${SCHED_CLUSTER}"
KUBECONFIG_TMP="$(mktemp)"
trap 'rm -f "${KUBECONFIG_TMP}"' EXIT
cat > "${KUBECONFIG_TMP}" <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: observed
    cluster:
      server: https://${OBS_ENDPOINT}
      certificate-authority-data: ${OBS_CA}
users:
  - name: scheduler-observer
    user:
      token: ${TOKEN}
contexts:
  - name: observed
    context:
      cluster: observed
      user: scheduler-observer
      namespace: default
current-context: observed
EOF

kubectl --context="${SCHED_CTX}" -n scheduler-system \
  create secret generic observed-cluster-kubeconfig \
  --from-file=config="${KUBECONFIG_TMP}" \
  --dry-run=client -o yaml \
| kubectl --context="${SCHED_CTX}" apply -f -

echo "==> Done. Run scripts/deploy.sh to roll out k8s-bridge-observed."
