# **Multi-Cluster GPU Job Scheduler**

We are building an open-source, optimization-based scheduler for AI organizations operating GPU fleets across multiple Kubernetes clusters. The scheduler places jobs across clusters in real time, handling gang scheduling, priorities, job suspension, quota guarantees, cross-cluster dependencies, resource borrowing, and fair sharing.

This is intended as a replacement for existing "Kubernetes-native" schedulers (e.g., [kueue](https://github.com/kubernetes-sigs/kueue)) and aims to improve particularly around observability and robustness. We're targeting teams that have outgrown these tools but aren't large enough to justify a fully custom internal scheduler.

The primary focus of this project is improved **observability and robustness of scheduling decisions**, pursued through two design choices: a **UX-first** approach that makes every decision inspectable, and **end-to-end control** of placement formulated as a **single optimisation problem** rather than a pipeline of independent heuristics.

The rest of this document defines key concepts, walks through a concrete example, and lists the requirements of the scheduler. The appendix describes the interface layer our scheduler must speak with and offers an example high-level systems view.

## **Definitions**

**Cluster.** The interface layer of our scheduler. A cluster is a set of nodes in one region, managed via Kubernetes. See Appendix A for more details.

**Region.** A cloud provider's geographic zone (e.g., `us-central1-b`). Low intra-region latency.

**Node.** A machine with fixed resources (accelerators, RAM, vCPUs). Belongs to exactly one cluster.

**Pod.** Smallest schedulable unit. Declares hardware requirements; runs on exactly one node.

**Job.** A group of pods that can only be scheduled and start running as a unit.

**Deployment**. A job variant that begins with zero running pods and scales pod count dynamically. An external autoscaler observes usage metrics and issues scale-up/down requests for the pod count of the deployment.

**Suspension.** A running job can be set to suspended state to free resources for another one.

**Priority.** A property of a job: low, medium, or high. Higher-priority jobs can suspend lower-priority ones.

**Quota.** A partition of cluster resources with a guaranteed floor. Quotas can borrow unused capacity from other quotas. Quota guarantees override priorities. Every job must belong to exactly one quota.

## **Example**

**Cluster Setup:** A single cluster: 100× `a3-ultragpu-8g` nodes in `us-central1-b`, each with 8 H200s. Cluster total: 800 GPUs. Two quotas: quota-a (70%, 560 GPUs) and quota-b (30%, 240 GPUs).

**Workloads** User A runs a medium-priority training job on quota-a: 60 pods × 8 GPUs \= 480 GPUs, all running simultaneously for gradient synchronization. User B runs 300 high-priority single-GPU inference jobs on quota-b, borrowing 60 GPUs beyond its 240 guarantee. A separate eval job on a different A100 cluster reads training checkpoints via GCS. The training and eval jobs are co-scheduled and co-suspended — if one is evicted, the other must be too.

**Conflict** User B's jobs are high-priority and exceed quota-b's guarantee, so they borrow from quota-a. If more jobs arrive in quota-b, the scheduler must suspend some of User A's pods. But User A's job is gang-scheduled — suspending even one pod effectively kills the entire training run, freeing all 480 GPUs at once, far more than needed. Simultaneously, the eval job on the other cluster must also be suspended. The scheduler must reason about these all-or-nothing cascades across clusters, while minimizing wasted GPU-hours and converging to a stable allocation without thrashing.

Additionally, User C runs a medium-priority serving deployment on quota-b with an initial replica count of zero. As inference traffic arrives, the autoscaler requests scale-ups from the scheduler, which places individual pods subject to quota-b's available capacity. When traffic subsides, the autoscaler requests scale-downs and the scheduler releases pods. If quota-b is overcommitted, the scheduler may deny scale-up requests or reclaim deployment pods before suspending gang-scheduled jobs, since deployment pods can be removed individually without all-or-nothing cascades.

## **Requirements**

**Scheduling latency.** Jobs decisions must be reached in \<10 seconds after placement and \<1 second for special fast-track jobs.

**Bin packing.** Minimize fragmentation. Pod shapes are full-node or single-GPU, which may simplify the bin-packing subproblem — though how this interacts with the broader optimization is an open question..

**Multi-choice accelerators.** Pods can be eligible for multiple accelerator types (e.g., H200 or H100) with optional preference weights.

**Cross-cluster gang scheduling.** Atomic scheduling of pods spanning multiple clusters. Co-scheduling and co-suspension constraints must be expressible.

**Autoscaled deployments.** The scheduler must accept incremental replica-count change requests from an external autoscaler (like [Keda](https://github.com/kedacore/keda)). Scale-up requests are subject to the same quota, priority, and capacity constraints as regular job placement. Scale-down releases are immediate. The scheduler must not treat a deployment's current replica set as gang-scheduled — individual pods can be reclaimed without suspending the entire deployment.

**Hardware fault tolerance.** When a node fails, affected pods must be quickly rescheduled — especially critical for gang-scheduled training jobs, where all pods block on gradient synchronization and one missing pod stalls the entire job. On-call teams typically cordon bad nodes; the scheduler, or the cluster itself, must react to the resulting state change and re-place pods fast. Note: pod-to-node reassignment is often delegated to the cluster's own scheduler, which is a reason to avoid pinning pods to specific nodes in placement decisions.

**Utilization.** Lend idle resources across quotas.

**Fairness.** Fair sharing within quotas. Fair borrowing across quotas. Borrowed resources reclaimed first.

**Minimize thrashing.** Suspending and then resuming a Job has high costs, equivalent to wasted \~1h of the Job’s work, and includes other risks. It should thus be minimized.

**Reclamation ordering** When resources must be freed, the scheduler should generally prefer reclaiming deployment pods (which can be individually removed and restored by the autoscaler) over suspending gang-scheduled jobs (which incur all-or-nothing restart costs). Within deployments, most-recently-added pods should be reclaimed first.

**Observability.** Explain infeasibility. Show queue positions and estimated start times.

**Robustness.** The scheduler must maintain \>99.9% availability. No user-submitted job — malformed, adversarial, or otherwise — may crash or wedge the scheduler. To achieve this, the scheduler is free to occasionally reject jobs from the queue and mark them appropriately.

---

## **Appendix A: Cluster Interface Boundary**

Each cluster runs Kubernetes, an orchestration system that manages the lifecycle of workloads on that cluster's nodes and includes a basic scheduler of its own. Among other things, Kubernetes assigns pods to nodes, enforces resource limits, restarts failed containers, evicts pods under memory or disk pressure, and runs health checks that can kill unresponsive pods. The multi-cluster scheduler sits above it, deciding which jobs run on which clusters and issuing submit/suspend directives. It does not replace Kubernetes — it must coexist with it.

This means two systems (our scheduler and kubernetes) might be making decisions about the same resources. Kubernetes may evict a pod the multi-cluster scheduler intended to keep, restart a container the scheduler was about to suspend, or place a replacement pod in a way that conflicts with the global plan. The interface must be designed so that these systems do not work against each other. Some considerations towards these are listed below.

**Pod-to-node placement.** The local Kubernetes scheduler can be bypassed, replaced, or constrained. The multi-cluster scheduler could delegate placement to Kubernetes (simpler, but must prevent it from making independent preemption decisions) or replace the local scheduler entirely. This is an open design fork.

**Stale state.** Cluster state is observed, not pushed. The multi-cluster scheduler always operates on slightly stale data. Decisions must be safe under stale reads; commands must be idempotent and state versioned.

**Bad hardware loops.** Degraded nodes cause immediate job failures. Without intervention, replacements may land on the same bad node. The scheduler must detect repeated rapid failures and back off. Ownership of automatic node exclusion is an open question.

## **Appendix B: An Example Systems View/Architecture**

A centralized scheduler can read from a replicated cluster-and-jobs record, compute placement, and issue submit/suspend commands to each cluster. User requests can be written to the record; the scheduler can pick them up each cycle. Both the scheduler and the record can be replicated for fault tolerance and easier redeployment.