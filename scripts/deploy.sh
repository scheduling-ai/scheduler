#!/usr/bin/env bash
# Deploy the scheduler plane to the GKE cluster defined in infra/.
#
# Idempotent: safe to re-run. Each invocation builds the current checkout,
# pushes a new tagged image to Artifact Registry, substitutes the tag into
# the manifests, and rolls out the three Deployments.
#
# Each deploy also wipes managed Jobs in the data plane — this is a demo
# cluster and we'd rather start clean than try to recover in-flight work
# across bridge restarts. If you ever need cross-restart durability,
# revisit this and add real recovery in the bridge.
#
# Prereqs (run once on a fresh machine):
#   cd infra && ./setup.sh                        # creates the cluster + RBAC
#   gcloud auth configure-docker europe-west4-docker.pkg.dev
#
# Uncommitted changes are deployed as `<sha>-dirty-<unix-ts>` so the tag is
# unique and the rollout always pulls a fresh image.
set -euo pipefail
cd "$(dirname "$0")/.."

# ------------------------------------------------------------------ tag -----
SHA="$(git rev-parse --short HEAD)"
if ! git diff --quiet || ! git diff --cached --quiet; then
  TAG="${SHA}-dirty-$(date +%s)"
  echo "==> Working tree is dirty; tagging as ${TAG}"
else
  TAG="${SHA}"
fi

# --------------------------------------------------------------- kubectx ----
PROJECT_ID="$(cd infra && terraform output -raw project_id 2>/dev/null || grep -E '^project_id' infra/terraform.tfvars | cut -d'"' -f2)"
ZONE="$(cd infra && terraform output -raw zone 2>/dev/null || echo europe-west4-a)"
CLUSTER_NAME="$(cd infra && terraform output -raw cluster_name)"

echo "==> Fetching kubectl credentials for ${CLUSTER_NAME}"
gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --zone "${ZONE}" --project "${PROJECT_ID}" >/dev/null

# ----------------------------------------------------------------- image ----
REPO="$(cd infra && terraform output -raw image_repo)"
IMAGE="${REPO}/scheduler:${TAG}"

echo "==> Building ${IMAGE}"
docker build -t "${IMAGE}" .

echo "==> Pushing ${IMAGE}"
docker push "${IMAGE}"

# --------------------------------------------------------------- apply -----
echo "==> Applying data-plane manifests (namespace + RBAC)"
kubectl apply -f infra/k8s/data-plane/

# Wipe managed Jobs from the data plane before the new bridge takes over.
# This is a demo cluster — we treat each redeploy as a fresh start rather
# than trying to recover in-flight workloads across restarts (the bridge's
# pending_node_assignments map is in-memory and TTL-bounded, so anything
# not freshly placed becomes an orphan that the next bridge can't easily
# re-bind). Background cascade so we don't block on pod GC.
echo "==> Resetting data-plane Jobs (managed-by=${MANAGED_BY:-custom-scheduler})"
kubectl -n default delete jobs \
  -l "scheduler.example.com/managed-by=${MANAGED_BY:-custom-scheduler}" \
  --cascade=background --wait=false --ignore-not-found

echo "==> Applying scheduler-plane manifests"
for f in infra/k8s/scheduler-plane/*.yaml; do
  # Use sed so we don't depend on envsubst being installed.
  sed "s|\${IMAGE}|${IMAGE}|g" "${f}" | kubectl apply -f -
done

echo "==> Waiting for rollouts"
kubectl -n scheduler-system rollout status deploy/k8s-bridge --timeout=3m
kubectl -n scheduler-system rollout status deploy/load-generator --timeout=3m
kubectl -n scheduler-system rollout status deploy/scheduler-ui --timeout=3m
if kubectl -n scheduler-system get secret pomerium-zero >/dev/null 2>&1; then
  kubectl -n scheduler-system rollout status deploy/pomerium --timeout=3m
else
  echo "    (skipping pomerium rollout — secret pomerium-zero not found)"
fi

echo "==> Done. State:"
kubectl -n scheduler-system get deploy,svc
