# Test cluster infrastructure

GKE cluster with fake GPU resources for testing the scheduler against
real k8s APIs without real GPUs.

## What it creates

- 1 zonal GKE Standard cluster in `europe-west4-a` (free tier: $0 control plane)
- 4 spot node pools, 8 `e2-micro` nodes each (32 nodes total), one pool per chip type
- Cloud NAT for outbound traffic (private nodes have no external IPs)
- Scheduler SA with RBAC (read nodes/pods/jobs, manage workloads, impersonate users)

Each pool sets two node labels at registration:
- `accelerator`: `H200` | `H100` | `A100` | `L40S`
- `scheduler.example.com/chips`: per-node chip count for that type

| Pool  | Nodes | Chip type | Chips/node | Pool chips |
|-------|-------|-----------|------------|------------|
| h200  | 8     | H200      | 8          | 64         |
| h100  | 8     | H100      | 8          | 64         |
| a100  | 8     | A100      | 16         | 128        |
| l40s  | 8     | L40S      | 4          | 32         |
| Total | 32    |           |            | 288        |

Each node simulates a single chip type, matching real GCP GPU node topology
(a3-ultragpu-8g, a3-highgpu-8g, a2-megagpu-16g, g4-standard-48).

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

**Labels, not extended resources**: k8s has no declarative way to advertise
extended resources without a device plugin, and a DaemonSet patching
`/status/capacity` every 10s races kubelet's own status writes and leaves
a cold-start window after spot preemption. Since our scheduler binds pods
directly (bypassing kube-scheduler), we don't need k8s-side enforcement —
the scheduler reads chip type/count from node labels set declaratively in
the pool config. Run the scheduler with `--chip-count-label
scheduler.example.com/chips`.

**One chip type per pool**: A homogeneous node matches real GPU hardware
(no node runs mixed chip types) and fits the scheduler's data model
(single `chip_type` + single `chips` per node). Earlier multi-type setup
needed a patcher DaemonSet and never mapped cleanly to the solver schema.

## Usage

```sh
# Prerequisites: gcloud auth login, terraform, kubectl, gke-gcloud-auth-plugin

# First time / rebuild
cd infra && ./setup.sh

# Tear down (stops billing)
cd infra && terraform destroy

# Scale (applies to every pool)
cd infra && terraform apply -var="nodes_per_pool=4"
```
