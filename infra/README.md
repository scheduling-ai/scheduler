# Test cluster infrastructure

GKE cluster with fake GPU resources for testing the scheduler against
real k8s APIs without real GPUs.

## What it creates

- 1 zonal GKE Standard cluster in `europe-west4-a` (free tier: $0 control plane)
- 32 spot `e2-micro` nodes with 15GB pd-standard disks
- Cloud NAT for outbound traffic (private nodes have no external IPs)
- DaemonSet that patches every node with fake extended resources:
  - `example.com/H200`: 8 per node (matches a3-ultragpu-8g)
  - `example.com/H100`: 8 per node (matches a3-highgpu-8g)
  - `example.com/A100`: 16 per node (matches a2-megagpu-16g)
  - `example.com/L40S`: 4 per node (matches g4-standard-48)
- Scheduler SA with RBAC (read nodes/pods/jobs, manage workloads, impersonate users)

## Cost

~$83/month while running. $0 when destroyed.

| Component         | Monthly |
|-------------------|---------|
| Control plane     | $0      |
| 32x e2-micro spot | ~$58    |
| 32x 15GB HDD      | ~$23    |
| Cloud NAT gateway | ~$1     |

## Key decisions

**e2-micro spot** ($1.8/node/mo): Cheapest GKE-compatible machine type.
System pods use 90% of RAM but fake workloads need almost nothing
(`cpu: 1m, memory: 0.5Mi` per sleep pod works fine). Spot preemption
doubles as free chaos testing.

**Private nodes + Cloud NAT**: Avoids `IN_USE_ADDRESSES` quota (default: 8).
Adds ~$1/mo. Nodes pull images via NAT.

**No logging/monitoring agents**: e2-micro has only 1GB RAM. System pods
leave ~60MB free. GKE agents don't fit. Trade-off: `kubectl logs` and
`kubectl exec` don't work via the API server. Use `kubectl debug` or
port-forward instead.

**All chip types on every node**: Unrealistic (real nodes have one GPU type)
but lets us test multi-accelerator scheduling without separate node pools.
The scheduler still sees correct per-node chip counts.

**Patch loop every 10s**: k8s has no declarative way to add extended resources
without a device plugin. The DaemonSet patches `/status/capacity` via the
API. A proper device plugin would be cleaner but is much more code for the
same result.

## Usage

```sh
# Prerequisites: gcloud auth login, terraform, kubectl, gke-gcloud-auth-plugin

# First time / rebuild
cd infra && ./setup.sh

# Tear down (stops billing)
cd infra && terraform destroy

# Scale nodes
cd infra && terraform apply -var="node_count=16"
```
