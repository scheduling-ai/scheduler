# 20 Challenging Questions Followed by Answers

---

**1. Why not just contribute multi-cluster support to Kueue?**

Kueue's multi-cluster story (MultiKueue) is an AdmissionCheck that creates mirror Workloads on all candidate clusters and waits for one to admit locally — it's a "race and pick the winner" dispatch, not a central optimizer. There is no single scheduling cycle that sees total capacity across clusters. Adding that would require replacing Kueue's core scheduling loop (`pkg/scheduler/scheduler.go`) which takes a per-ClusterQueue snapshot. That is a fundamental architectural change, not a feature addition. The more pragmatic answer: Kueue has 70k+ lines of Go across 40+ controllers, a CRD ecosystem, and a release cadence tied to SIG Scheduling. The cost of upstreaming a central multi-cluster optimizer exceeds the cost of building one.

*Further reading:* [Kueue MultiKueue design](https://kueue.sigs.k8s.io/docs/concepts/multikueue/) · `pkg/controller/admissionchecks/multikueue/workload.go` in the Kueue repo.

---

**2. You call the Binding API directly. Doesn't that bypass kube-scheduler's feasibility checks — topology, resource fits, PDB, affinity?**

Yes. The Binding API (`POST .../pods/{name}/binding`) is a low-level operation that skips the kube-scheduler's filtering and scoring pipeline entirely. We do our own capacity check (allocatable minus managed pod GPU usage), but we do not check: inter-pod affinity/anti-affinity, PodDisruptionBudgets, volume topology constraints, or memory/CPU headroom from non-GPU pods. The bet is that GPU scheduling for AI workloads is dominated by a single scarce resource (accelerators) and full-node or single-GPU pod shapes. If that assumption holds, the bypassed checks are vacuous. If users submit heterogeneous workloads with complex affinity rules, this breaks.

*Trace:* `binder.rs:1756-1785` (Binding API call), `binder.rs:594-624` (capacity computation). Compare Kueue's approach: `podset.Merge` injects `nodeSelector` and lets kube-scheduler handle binding (`pkg/util/admissioncheck/podset/podset.go`).

---

**3. What happens if someone does `kubectl delete job <name>` while the scheduler has the workload in its store?**

For a Job: the k8s API deletes the Job and its pods. The job reflector detects the absence. On the next binder cycle, `rebuild_cluster_state` sees the job is gone from the cluster. If it was in the `pending_node_assignments` map, the entry is cleared within one cycle (not waiting for TTL). The workload remains in the in-memory store as "queued" — the solver will try to re-place it next cycle, creating a new Job. This is actually a reasonable self-healing behavior for Jobs. For standalone Pods that were suspended (deleted from cluster, held in store): there is no cluster-side object to delete. The store entry survives and the pod will be recreated on unsuspension. There is no mechanism for a user to cancel a suspended Pod-type workload except through the scheduler's `DELETE /jobs/{name}` API.

*Trace:* `binder.rs:952-989` (pending entry cleanup on job disappearance), `api.rs:165-173` (DELETE endpoint).

---

**4. You have two solvers: a MILP and a heuristic. What does each give up?**

The heuristic (`solver.py`) runs a priority-ordered greedy admission followed by best-fit-decreasing node packing. This is O(P·C·N) per cycle and is the default solver. The MILP (`milp_solver.py`) jointly optimizes across all workloads with a lexicographic four-stage objective (quota coverage → priority → thrash reduction → tie-breaks) and is opt-in via `--solver milp`. The MILP finds placements the heuristic misses — e.g., deferring a medium-priority job to make room for two small high-priority jobs that together use less capacity. The heuristic can also thrash: admit A which preempts B, next cycle B's capacity is freed so C gets admitted preempting A, etc. The MILP's thrash-reduction stage mitigates this. `test_scenario_oscillating_preemption` in the test suite documents the heuristic's oscillation as known behavior. The interface (`SolverRequest`/`ScheduleResult`) is solver-agnostic — swapping the solver requires no Rust changes. There is no automatic fallback between solvers on failure; a failed solve skips the cycle.

*Trace:* `solver.py:124-132` (heuristic two-phase pipeline), `milp_solver.py:480-517` (lexicographic staged solves), `test_scenarios.py:267` (oscillation test).

---

**5. How do you handle the case where a node fails mid-cycle — after the solver places a workload there but before the Binding API call?**

The solver runs on a snapshot. If a node fails between snapshot and apply, the Binding API call will fail (node not ready or not found). The binder logs the error, the workload stays in the store, and the next cycle's reflectors will show the node as NotReady or gone. The solver will exclude it and re-place the workload elsewhere. The failure window is one cycle (~5s). During that window the workload is in limbo — placed in the store but not on any cluster. The generation counter prevents a stale removal. This is safe but the user sees a 5-10s delay for what feels like a placement failure.

*Trace:* `binder.rs:1867-1897` (parallel binding tasks, individual failures logged), `binder.rs:1372-1394` (generation check prevents stale removal).

---

**6. Kueue persists its state in etcd via the Workload CRD. You use in-memory state. What's the actual recovery story?**

On restart: Jobs and their pods exist on clusters and are rediscovered by reflectors. The binder reconstructs their state (running, which nodes, which cluster). Suspended Jobs (`spec.suspend: true`) are also on clusters and reconstructed. Queued workloads (never placed) are lost — they existed only in memory. Suspended standalone Pods are lost — they were deleted from the cluster on suspension and held only in the store. A durable store is explicitly deferred. The minimal fix is writing the store to a file on mutation and replaying on startup. The harder problem is consistency: if the binder crashes mid-apply, the file may not reflect the partial state.

*Compare:* Kueue's Workload CRD (`apis/kueue/v1beta1/workload_types.go`) is the single source of truth. Admission, eviction, and queue position are all persisted. Controller restart is a no-op.

---

**7. You spawn a new Python process every cycle. At 5-second intervals that's 17k process spawns per day. Is this sustainable?**

Each spawn is `uv run --no-sync python -m scheduler <solver_name>`. The `--no-sync` flag skips environment resolution, but there's still process creation, Python interpreter startup, and module import overhead. On a warm system this is ~200-400ms. At 5s intervals this is 4-8% overhead. The real concern is tail latency: if the OS is under memory pressure or the Python environment is cold (e.g., after a deployment), startup can spike to seconds. A persistent subprocess with a request/response protocol over stdin would eliminate this entirely. The solver interface is already JSON-over-stdin/stdout, so the change is mechanical.

*Trace:* `solver.rs:25-112` (spawn logic), `solver.rs:16` (60s timeout constant).

---

**8. What prevents two solver cycles from producing conflicting decisions — e.g., cycle N places workload A on node-1, cycle N+1 (before reflectors confirm) places workload B on node-1 too?**

The `pending_node_assignments` map. When cycle N places A on node-1, the binder records `{A: {node-1: 1 replica}}` with a timestamp. Cycle N+1's `build_cluster_state` injects this as already-occupied capacity before calling the solver. The solver sees node-1 as having one fewer GPU and will not over-place. The entry is cleared only when the reflector confirms `spec.nodeName` is set on A's pod (binding complete), or after 30s TTL. This is the core gap-closure mechanism, and it is unit-tested.

*Trace:* `binder.rs:377-378` (pending map structure), `binder.rs:617-624` (injection into solver request).

---

**9. Gang scheduling across clusters — how does atomicity work when placements hit different k8s API servers?**

Both solvers decide gang placement atomically: the heuristic (`solver.py:546-577`) rolls back all placements if any member fails; the MILP (`milp_solver.py:400-410`) encodes gang sets as equality constraints. The binder then applies assignments to potentially different clusters in parallel via `JoinSet`. If one cluster's API call fails and another succeeds, we have a partial gang placement. The successful placements become running workloads; the failed ones stay in the store. Next cycle the solver sees a partial gang (some running, some queued) and must decide what to do — the current solver does not have explicit logic for this case. It will attempt to place the remaining members, which may or may not succeed depending on capacity. There is no rollback of the successful placements. This is a genuine gap: cross-cluster atomicity requires 2PC or saga-style compensation, neither of which exists.

*Trace:* `solver.py:546-577` (heuristic atomic rollback), `milp_solver.py:400-410` (MILP gang set constraints), `binder.rs:1301-1366` (parallel apply via JoinSet, no compensation on partial failure).

---

**10. How does your quota model compare to Kueue's Cohort borrowing?**

Both allow borrowing unused capacity. Kueue's model: ClusterQueues in a Cohort share a pool. A CQ can borrow up to `borrowingLimit` from the cohort's unused nominal quota. Lending is capped by `lendingLimit`. Borrowers can be preempted to reclaim capacity. Our model: quotas have per-cluster-per-chip guarantees. Unused guarantees form an "unguaranteed pool" per cluster. Any quota can borrow from this pool. Cross-quota preemption is restricted to borrowers — you cannot preempt a pod within its guarantee from a different quota. The key difference: Kueue's cohort is a flexible hierarchy (cohorts can nest via parent references). Ours is flat — all quotas implicitly share one pool per cluster. Kueue also has `FlavorFungibility` (try next flavor before borrowing), `FairSharing` weights, and `AdmissionFairSharing` (historical-usage-weighted admission). We have none of these.

*Trace:* `solver.py:227-247` (borrowing logic), Kueue: `apis/kueue/v1beta1/clusterqueue_types.go` (Cohort, borrowingLimit, lendingLimit).

---

**11. What k8s objects does this create that an operator would need to know about?**

Jobs (`batch/v1`) with label `scheduler.example.com/managed-by=custom-scheduler` and `schedulerName: custom-scheduler` in the pod template. Pods (`v1`) with the same label and `spec.nodeName` set directly (KEDA path) or bound via the Binding API (Job path). Binding subresources (`POST .../pods/{name}/binding`). No CRDs, no ConfigMaps, no Services, no ServiceAccounts beyond the binder's own. The binder itself runs as a single process — there is no Deployment manifest, no Helm chart, no operator. For a demo, it's `cargo build && ./k8s-bridge serve`. For production, you'd need to write the deployment infrastructure.

*Trace:* `binder.rs:1650-1754` (object creation), `binder.rs:1756-1785` (Binding API).

---

**12. The pod reflector watches all pods. On a 10k-pod cluster, what's the memory and API-server impact?**

Each Pod object is ~2-5 KB in the kube-rs reflector store. At 10k pods: 20-50 MB memory, plus the initial list response and ongoing watch stream. The API server cost is one watch connection per cluster with no field/label selector, meaning every pod create/update/delete event is streamed. This is the same pattern as `kubectl get pods --watch` on all namespaces. It works on small clusters but is a known scalability concern for the k8s API server at scale. The fix is straightforward: add a label selector to the pod reflector (same as the job reflector already does) and only watch pods on tainted nodes or with our label.

*Trace:* `binder.rs:156-240` (reflector setup — note job reflector has label selector, pod reflector does not).

---

**13. What's the actual scheduling latency budget? Walk through the critical path.**

Cycle tick (~5s configurable) + reflector snapshot (in-memory, ~0ms) + solver request serialization (~1ms) + Python process spawn (~200-400ms) + solver execution (depends on workload count; heuristic is O(P·C·N), <100ms for 100 pods; MILP depends on problem structure, tested up to ~1000 pods on 10k-GPU infra) + response deserialization (~1ms) + diff computation (~0ms) + k8s API calls (create/patch/bind, 50-200ms per call, parallelized) + reflector confirmation (~1 cycle). End-to-end from submission to pod running: one full cycle (5s) waiting for the next tick, plus the cycle itself (~1-2s for heuristic, variable for MILP), plus kubelet pod startup. Realistic: 6-10s for the heuristic solver. With a MILP solver taking 10s: 15-20s. A sub-1s fast-track path has been considered but is not implemented.

---

**14. How do you handle priority inversion — a low-priority job holding resources that block a high-priority gang?**

Both solvers handle this. The heuristic's preemption logic (`solver.py:580-751`) finds victims when admitting a high-priority pod: lowest-priority borrowers first, then lowest-priority within-quota pods (same quota only). Gang members are preempted atomically — if any member of a gang is a victim, all members are suspended. The MILP handles this via its lexicographic objective: priority stages run from highest to lowest, so higher-priority work is maximized first, naturally suspending lower-priority pods when capacity is needed. The gap: if a high-priority gang arrives but the low-priority job is within its guarantee and from a different quota, it cannot be preempted. The high-priority gang queues indefinitely. This is by design (guarantees are inviolable across quotas) but it can surprise users.

*Trace:* `solver.py:636-651` (cross-quota preemption restricted to borrowers), `solver.py:667-685` (victim selection by priority), `milp_solver.py:454-465` (priority stages in MILP objective).

---

**15. Kueue has 40+ integration adapters (PyTorchJob, RayJob, JobSet, etc.). You support batch/v1 Job and v1 Pod. How do you handle training frameworks?**

We don't, directly. PyTorchJob creates multiple replica sets (master, workers) as a single unit. Our API accepts raw Job or Pod JSON — the user (or a thin adapter) must decompose a PyTorchJob into its constituent pods and submit them as a gang set with the `scheduler.example.com/gang-set` annotation. The solver will ensure atomic placement. This works but pushes framework-specific logic to the client. Kueue's approach is the opposite: a reconciler per framework that watches the CRD, creates a Workload, and syncs suspend state. Building even the PyTorchJob adapter is non-trivial — Kueue's is ~500 lines of Go with edge cases around replica failures and partial completions.

*Compare:* `pkg/controller/jobs/kubeflow/jobs/pytorchjob/` in Kueue repo.

---

**16. What's your story for observability? How does an operator debug "why is my job queued?"**

`GET /status/{name}` returns: phase (Queued/Assigning/Running/Suspended), queue_position, eviction_risk (Safe/AtRisk/Evicting), and cluster assignment. The replay UI (`uv run scheduler-sim`) can load a recorded session and step through solver decisions frame by frame — this is the primary debugging tool for "why did the solver make this decision." There are no Prometheus metrics, no structured events, no audit log. The binder uses `tracing` for structured logging but there is no log aggregation or dashboard. Kueue emits Kubernetes Events on Workloads (admission, eviction, preemption with reason) and exposes Prometheus metrics (queue depth, admission latency, preemption counts). We have none of that.

*Trace:* `job_store.rs:104-118` (status response), `api.rs:175-193` (status endpoints).

---

**17. If I'm running Kueue today, what's the migration path?**

There isn't one yet. Kueue workloads use the Workload CRD and ClusterQueue/LocalQueue for quota. Our system uses HTTP API submission with priority/quota annotations. A coexistence path: partition GPU nodes by taint — some nodes managed by Kueue, some by us. Migrate workloads gradually. The node partitioning is clean because both systems use taints for isolation. The harder problem is quota: guarantees must be split between the two systems during migration. The eventual goal is to replace Kueue entirely, at which point users submit to our API (or a kubectl plugin that wraps it) instead of creating Jobs with `kueue.x-k8s.io/queue-name` labels.

---

**18. The binder runs as a single process with no HA. What's the blast radius of a crash?**

Running workloads continue running — they are on k8s clusters with their own controllers. Suspended Jobs stay suspended (`spec.suspend: true` is persisted in etcd). The blast radius is: (a) queued workloads lost, (b) no new admissions until restart, (c) no preemption enforcement, (d) suspended standalone Pods lost permanently. Restart recovery takes: Rust binary startup + reflector initial list (proportional to cluster size, typically 1-5s) + one solver cycle. Total: ~10-15s. There is no leader election, no hot standby, no WAL. Kueue runs as a Deployment with leader election (`LeaderElectionConfiguration` in its config); a crashed replica is replaced by k8s and resumes from etcd state.

*Trace:* `main.rs:139-212` (single process, no HA setup).

---

**19. You mentioned quotas are passed via `--quotas` flag from a JSON file. Who is the source of truth for quota definitions, and what happens when they change?**

The JSON file is read once at startup (`main.rs`). Quota changes require a binder restart. There is no dynamic quota API, no k8s CRD for quotas, no watch mechanism. Kueue's ClusterQueue is a live k8s object — an admin can `kubectl edit clusterqueue` and the change takes effect on the next scheduling cycle. For a demo, static quotas are fine. For production, quotas need to be a watched resource (either a CRD or a ConfigMap with a file watch).

*Trace:* `solver_types.rs:47-51` (quota schema), `main.rs` (loaded once at startup and passed to every solver invocation).

---

**20. What's the fundamental bet this project is making, and where does it fail?**

The bet: a centralized, stateless solver that sees all clusters simultaneously produces better scheduling decisions than Kueue's per-cluster event-driven admission, especially for gang scheduling and cross-cluster bin packing. The solver is stateless — it receives a full snapshot and returns a full schedule, making it easy to test, replay, and swap. The cost: we reimplemented the Kueue lifecycle (suspend, preempt, quota) without the years of production hardening. The bet fails if: (a) the scheduling problem is not actually hard enough to justify a central optimizer — if most workloads are single-node single-GPU, Kueue's per-cluster greedy admission is sufficient; (b) the latency cost of a central solver (5-15s per cycle) is unacceptable for interactive/serving workloads; (c) the surface area of k8s edge cases (PDBs, volume affinity, DaemonSet interference, Job immutability constraints) overwhelms the small team before the core value proposition can be demonstrated. The replay UI and the test suite are the main defenses against (c) — they make it possible to reproduce and fix edge cases quickly rather than discovering them in production.

---

## Further reading

| Topic | Resource |
|-------|----------|
| Kueue architecture | [kueue.sigs.k8s.io/docs/concepts](https://kueue.sigs.k8s.io/docs/concepts/) |
| MultiKueue design | [kueue.sigs.k8s.io/docs/concepts/multikueue](https://kueue.sigs.k8s.io/docs/concepts/multikueue/) |
| k8s Job suspend semantics | KEP-3329 (Job suspend/resume), KEP-2214 (indexed Jobs) |
| Binding API | `k8s.io/api/core/v1.Binding` — used by kube-scheduler, documented in scheduler framework |
| Kueue preemption | `pkg/scheduler/preemption/` in the Kueue repo |
| Kueue pod template mutation | `pkg/util/admissioncheck/podset/podset.go` — `Merge` and `RestorePodSpec` |
| Gang scheduling in Kueue | JobSet integration: `pkg/controller/jobs/jobset/` |
| Scheduler framework (k8s) | [kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework](https://github.com/kubernetes/kubernetes/tree/master/pkg/scheduler) |
| Shipyard (multi-cluster platform) | [shipyard.build](https://shipyard.build) |
| Simon Willison on LLM-assisted prototyping | [til.simonwillison.net](https://til.simonwillison.net) |
