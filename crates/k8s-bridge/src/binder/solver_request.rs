//! Build the [`SolverRequest`] each cycle.
//!
//! Single builder serves both modes — solver mode (the bridge actively
//! schedules) and observe-only mode (the bridge passively reflects an
//! externally-managed cluster).  The two modes diverge in three places,
//! all parameterized:
//!
//!   * `placement_shadow: Option<&mut PlacementShadow>` — `Some` only
//!     when we're the scheduler.  Drives shadow read/write/cleanup,
//!     gates the "keep nodes occupied during pod termination" subcase,
//!     and gates warnings about workloads that bypass our binder.
//!   * `store_workloads: Option<&HashMap<...>>` — `Some` only when we
//!     own a workload store.  Adds queued workloads to the snapshot
//!     and to gang-set membership.
//!   * `pod_predicate: impl Fn(&Pod) -> bool` — solver mode passes
//!     `is_managed_by_us`; observe mode passes `|_| true`.
//!
//! The remaining behaviors (namespace exclusion, chip-type fallback
//! from node labels, dropping replicas on unknown nodes) are
//! always-on but no-op when the relevant config is empty / inputs are
//! well-formed — so solver mode is unchanged in practice.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::batch::v1::Job as K8sJob;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{ResourceExt, runtime::reflector};
use tracing::warn;

use crate::job_store::{self, ManagedObject, Workload};
use crate::solver_types::{
    ClusterState as SolverCluster, Node as SolverNode, Phase, Pod as SolverPod, PodKind,
    PodReplicaStatus as SolverReplicaStatus, SolverRequest,
};

use super::extract::{
    extract_job_metadata, extract_pod_metadata, extract_workload_metadata, is_known_quota,
};
use super::{BinderConfig, ClusterRuntime, PlacementShadow};

/// Predicate matching standalone Pods we own (solver mode).  Observe
/// mode passes `|_| true` to surface every Pod.
pub(super) fn is_managed_pod(config: &BinderConfig) -> impl Fn(&Pod) -> bool + Copy + '_ {
    |pod: &Pod| {
        pod.labels()
            .get(&config.managed_by_label)
            .map(|v| v == &config.managed_by_value)
            .unwrap_or(false)
    }
}

/// Aggregate per-cluster state into a single [`SolverRequest`].
///
/// `placement_shadow` and `store_workloads` are `Some` in solver mode
/// and `None` in observe mode.  See module docs for the shape of the
/// divergence.
pub(super) fn build_request_multi(
    runtimes: &[ClusterRuntime],
    config: &BinderConfig,
    store_workloads: Option<&HashMap<String, Workload>>,
    mut placement_shadow: Option<&mut PlacementShadow>,
    pod_predicate: impl Fn(&Pod) -> bool + Copy,
) -> SolverRequest {
    let mut cluster_states: Vec<SolverCluster> = Vec::with_capacity(runtimes.len());
    let mut pods: HashMap<String, SolverPod> = HashMap::new();

    for rt in runtimes {
        let node_r = rt.node_reader();
        let pod_r = rt.pod_reader();
        let job_r = rt.job_reader();
        let (cluster, cluster_pods) = build_cluster_state(
            &node_r,
            &pod_r,
            &job_r,
            &rt.name,
            config,
            placement_shadow.as_deref_mut(),
            pod_predicate,
        );
        cluster_states.push(cluster);

        for (name, pod) in cluster_pods {
            pods.entry(name).or_insert(pod);
        }
    }

    // Inject just-placed workloads from the shadow that aren't yet
    // visible in any reflector.  Closes the 1-cycle gap between store
    // removal (apply_assignments_multi removes the workload from the
    // store as soon as the k8s create succeeds) and the job reflector
    // confirming the new Job — without this the solver sees the
    // assigned nodes as free and could double-book them.
    //
    // Observe mode has no shadow; this block is a no-op.
    if let Some(shadow) = placement_shadow.as_deref() {
        for (name, (_, _, solver_pod)) in shadow.iter() {
            if pods.contains_key(name) {
                continue;
            }
            if let Some(pod) = solver_pod {
                pods.entry(name.clone()).or_insert_with(|| pod.clone());
            }
        }
    }

    // Store-submitted workloads: not yet placed on any cluster, or
    // suspended Pods pinned to a cluster.  Observe mode has no store;
    // this block is a no-op.
    if let Some(store) = store_workloads {
        for (wl_name, workload) in store {
            if pods.contains_key(wl_name) {
                continue;
            }
            // Skip workloads in backoff — they failed placement too
            // many times and will be retried when cluster state changes.
            if workload.consecutive_failures >= job_store::BACKOFF_THRESHOLD {
                continue;
            }

            let (chips, chip_type, priority, quota, parallelism) =
                extract_workload_metadata(&workload.managed, config);

            if !is_known_quota(config, &quota) {
                warn!(
                    workload = %wl_name,
                    quota = %quota,
                    "skipping store workload with unknown quota"
                );
                continue;
            }

            // Store-side workloads are always queued (suspended
            // workloads stay on their cluster and reach the solver via
            // reflector state).
            let statuses_by_replica: Vec<SolverReplicaStatus> = (0..parallelism)
                .map(|_| SolverReplicaStatus {
                    phase: Phase::Running,
                    node: None,
                })
                .collect();

            pods.insert(
                wl_name.clone(),
                SolverPod {
                    chips_per_replica: chips,
                    chip_type,
                    priority,
                    quota,
                    cluster: None,
                    statuses_by_replica,
                    kind: PodKind::Job,
                },
            );
        }
    }

    let gang_sets = build_gang_sets(runtimes, config, store_workloads, &pods, pod_predicate);

    SolverRequest {
        clusters: cluster_states,
        pods,
        gang_sets,
        quotas: config.quotas.clone(),
        // 30s is the solver's default budget; observe mode never runs
        // a solver so this field is informational only.
        time_limit: 30.0,
    }
}

/// Build the [`SolverCluster`] (topology) and per-pod state for one
/// cluster.  Solver and observe modes share this function — see
/// [`build_request_multi`] for the divergence parameters.
pub(super) fn build_cluster_state(
    node_store: &reflector::Store<Node>,
    pod_store: &reflector::Store<Pod>,
    job_store: &reflector::Store<K8sJob>,
    cluster_name: &str,
    config: &BinderConfig,
    mut placement_shadow: Option<&mut PlacementShadow>,
    pod_predicate: impl Fn(&Pod) -> bool,
) -> (SolverCluster, HashMap<String, SolverPod>) {
    let solver_nodes: Vec<SolverNode> = get_candidate_nodes(node_store, config)
        .iter()
        .map(|node| SolverNode {
            name: node.name_any(),
            chip_type: node
                .labels()
                .get(&config.chip_label)
                .cloned()
                .unwrap_or_default(),
            chips: node_chip_capacity(node, config),
        })
        .collect();

    // Index nodes by name for chip_type fallback when a workload
    // doesn't carry the chip-type label (typical for non-managed
    // workloads — Kueue jobs only have flavor info on the assigned Node).
    let node_chip_type: HashMap<String, String> = solver_nodes
        .iter()
        .map(|n| (n.name.clone(), n.chip_type.clone()))
        .collect();

    let excluded_ns: HashSet<&str> = config
        .excluded_namespaces
        .iter()
        .map(String::as_str)
        .collect();
    let in_excluded_ns =
        |ns: Option<&str>| -> bool { ns.map(|n| excluded_ns.contains(n)).unwrap_or(false) };

    // Are we in solver mode?  Cached so we don't repeatedly call
    // `placement_shadow.is_some()` and to keep call sites readable.
    let is_managing = placement_shadow.is_some();

    let mut solver_pods: HashMap<String, SolverPod> = HashMap::new();

    // --- Jobs on the cluster ---
    for job in job_store.state() {
        if in_excluded_ns(job.metadata.namespace.as_deref()) {
            continue;
        }

        let job_name = job
            .labels()
            .get(&config.job_name_label)
            .cloned()
            .unwrap_or_else(|| job.name_any());

        // Skip terminal Jobs entirely.  Without this, every
        // completed/failed Job that hasn't been GC'd by k8s would be
        // padded with phantom "Running/None" replicas (see
        // build_replica_statuses_from_job_pods) and reach the
        // solver/snapshot as zombie queued work.
        if is_job_finished(&job) {
            if let Some(s) = placement_shadow.as_deref_mut() {
                s.remove(&job_name);
            }
            continue;
        }

        let (chips, mut chip_type, priority, quota, parallelism) =
            extract_job_metadata(&job, config);

        // Quota enforcement is solver-side: observe mode surfaces the
        // workload regardless so the UI shows what's actually there.
        // (When `config.quotas` is empty, `is_known_quota` returns true
        // for everything — a no-op for solver too if quotas aren't
        // configured.)
        if is_managing && !is_known_quota(config, &quota) {
            warn!(
                workload = %job_name,
                quota = %quota,
                "skipping reflector-discovered Job with unknown quota"
            );
            if let Some(s) = placement_shadow.as_deref_mut() {
                s.remove(&job_name);
            }
            continue;
        }

        // Footgun for kubectl-applied Jobs that bypass our binder by
        // setting the wrong (or missing) schedulerName.  Only relevant
        // when we're the scheduler — observe mode expects
        // kube-scheduler to bind these pods.
        if is_managing {
            let template_scheduler = job
                .spec
                .as_ref()
                .and_then(|s| s.template.spec.as_ref())
                .and_then(|p| p.scheduler_name.as_deref());
            if template_scheduler != Some(config.scheduler_name.as_str()) {
                warn!(
                    workload = %job_name,
                    expected = %config.scheduler_name,
                    actual = ?template_scheduler,
                    "managed Job's pod template has wrong schedulerName; kube-scheduler will bind its pods, bypassing this scheduler"
                );
            }
        }

        let is_suspended = job.spec.as_ref().and_then(|s| s.suspend).unwrap_or(false);
        let pods_present = pods_exist_for_job(pod_store, &job);

        // Build statuses_by_replica.  Sources, in priority:
        //   1. Bound child pods (always preferred when present).
        //   2. Placement shadow — solver mode only; reconstructs
        //      in-flight placements between store-remove and
        //      reflector-confirm.
        //   3. Empty pending — solver re-schedules; observe shows
        //      as queued.
        //
        // Suspended Jobs: always emit Phase::Suspended for every
        // replica, regardless of whether the child Pods are still in
        // the reflector (Terminating).  Otherwise
        // build_replica_statuses_from_job_pods would derive
        // Phase::Failed for those pods (kubelet sets pod.status.phase
        // to Failed when SIGKILL'd by suspend), the Python solver
        // would classify the workload as `passthrough_pods` instead
        // of `suspended_pods`, and it would never get re-admitted
        // when capacity frees.  Capacity is briefly over-counted
        // until the terminating pods are gone — acceptable, since
        // the solver still sees the workload as a re-admission
        // candidate and queues it if chips aren't free yet.
        let statuses_by_replica: Vec<SolverReplicaStatus> = if is_suspended {
            if let Some(s) = placement_shadow.as_deref_mut() {
                s.remove(&job_name);
            }
            (0..parallelism)
                .map(|_| SolverReplicaStatus {
                    phase: Phase::Suspended,
                    node: None,
                })
                .collect()
        } else if pods_present {
            if let Some(s) = placement_shadow.as_deref_mut() {
                s.remove(&job_name);
            }
            build_replica_statuses_from_job_pods(pod_store, &job, parallelism)
        } else if let Some(shadow) = placement_shadow.as_deref()
            && let Some((node_counts, _, _)) = shadow.get(&job_name)
        {
            // Placement decision in flight; reconstruct from the
            // shadow so the assigned nodes appear occupied this cycle.
            let mut s: Vec<SolverReplicaStatus> = node_counts
                .iter()
                .flat_map(|(node, &count)| {
                    (0..count).map(move |_| SolverReplicaStatus {
                        phase: Phase::Running,
                        node: Some(node.clone()),
                    })
                })
                .collect();
            while (s.len() as u32) < parallelism {
                s.push(SolverReplicaStatus {
                    phase: Phase::Running,
                    node: None,
                });
            }
            s
        } else {
            // No pods, no shadow.  Emit pending replicas: solver
            // mode treats this as "needs scheduling" (cluster=None
            // below); observe mode renders it as queued.
            (0..parallelism)
                .map(|_| SolverReplicaStatus {
                    phase: Phase::Running,
                    node: None,
                })
                .collect()
        };

        // chip_type fallback from the assigned node's label.  No-op
        // when the workload already carries the label (always true
        // for solver-managed workloads).
        if chip_type.is_empty() {
            for status in &statuses_by_replica {
                if let Some(node) = status.node.as_deref()
                    && let Some(t) = node_chip_type.get(node)
                    && !t.is_empty()
                {
                    chip_type = t.clone();
                    break;
                }
            }
        }

        // Drop replicas that landed on nodes outside the candidate
        // set — observe mode only.  Solver mode keeps them: an
        // orphaned managed pod still needs tracking so we can re-bind
        // or re-schedule it; dropping it would silently leak the
        // workload from our view.
        let statuses_by_replica: Vec<SolverReplicaStatus> = if is_managing {
            statuses_by_replica
        } else {
            let filtered: Vec<_> = statuses_by_replica
                .into_iter()
                .filter(|r| {
                    r.node
                        .as_deref()
                        .is_none_or(|n| node_chip_type.contains_key(n))
                })
                .collect();
            if filtered.is_empty() {
                continue;
            }
            filtered
        };

        // Cluster pinning: pin if any replica is placed on a known
        // node OR the job is suspended (solver mode treats suspended
        // workloads as cluster-pinned for re-admission decisions).
        // Otherwise leave None — solver re-schedules; observe shows
        // the workload as queued without cluster scoping.
        let cluster = if statuses_by_replica
            .iter()
            .any(|r| r.node.is_some() && r.phase != Phase::Suspended)
            || is_suspended
        {
            Some(cluster_name.to_string())
        } else {
            None
        };

        solver_pods.insert(
            job_name,
            SolverPod {
                chips_per_replica: chips,
                chip_type,
                priority,
                quota,
                cluster,
                statuses_by_replica,
                kind: PodKind::Job,
            },
        );
    }

    // --- Standalone Pods on the cluster ---
    //
    // `pod_predicate` filters: solver mode requires our managed-by
    // label; observe mode accepts every pod.  Pods owned by a Job are
    // skipped — they're accounted for under the Job above.
    //
    // Per the model.py contract, each k8s Pod becomes its own
    // single-replica SolverPod — independently schedulable and
    // reclaimable, no gang-scheduling.  This means the SolverPod key
    // must be the k8s pod name, not the `job-name` label: a
    // Deployment's ReplicaSet stamps the same `job-name` onto every
    // replica's pod template (see deployment_driver.py), so keying by
    // label would collapse N replicas into one entry and silently
    // drop the rest from the solver's view.
    for pod in pod_store.state() {
        if in_excluded_ns(pod.metadata.namespace.as_deref()) {
            continue;
        }
        if !pod_predicate(&pod) {
            continue;
        }

        let owned_by_job = pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.kind == "Job"))
            .unwrap_or(false);
        if owned_by_job {
            continue;
        }

        let pod_name = pod.name_any();

        // Defensive: skip if a Job has already claimed this name.
        if solver_pods.contains_key(&pod_name) {
            continue;
        }

        let (chips, mut chip_type, priority, quota, _) = extract_pod_metadata(&pod, config);

        if is_managing && !is_known_quota(config, &quota) {
            warn!(
                workload = %pod_name,
                quota = %quota,
                "skipping reflector-discovered Pod with unknown quota"
            );
            continue;
        }

        let phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("Unknown");
        if phase == "Succeeded" || phase == "Failed" {
            continue;
        }

        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());

        // Drop pods landed on nodes outside the candidate set —
        // observe mode only (same reasoning as the per-Job branch:
        // solver mode needs to keep tracking orphaned managed pods).
        if !is_managing
            && let Some(node) = node_name.as_deref()
            && !node_chip_type.contains_key(node)
        {
            continue;
        }

        if chip_type.is_empty()
            && let Some(node) = node_name.as_deref()
            && let Some(t) = node_chip_type.get(node)
        {
            chip_type = t.clone();
        }

        // Cluster pinning: only set once the Pod has actually landed
        // on a node.  A Pending Pod (Deployment-spawned, not yet
        // bound) needs the solver's admission step to consider it —
        // and both solvers gate admission on `pod.cluster is None`,
        // so leaving cluster=Some on a Pending Pod silently drops it
        // from placement.
        let cluster = node_name.as_ref().map(|_| cluster_name.to_string());

        solver_pods.insert(
            pod_name,
            SolverPod {
                chips_per_replica: chips,
                chip_type,
                priority,
                quota,
                cluster,
                statuses_by_replica: vec![SolverReplicaStatus {
                    phase: Phase::Running,
                    node: node_name,
                }],
                kind: PodKind::Deployment,
            },
        );
    }

    // --- Placement-shadow cleanup (solver mode only) ---
    //
    // Drop entries for jobs that no longer exist on this cluster.
    // When a job is deleted externally (e.g. operator action) its
    // entry is never touched by the loop above because the job has
    // vanished from the reflector.  Without this cleanup the entry
    // would linger for the full PENDING_TTL, causing the injection
    // loop in build_request_multi to keep inserting a phantom running
    // pod and blocking placement on those nodes.
    //
    // Only entries targeting *this* cluster are cleaned up; entries
    // for other clusters are left intact.
    if let Some(shadow) = placement_shadow {
        let known_on_cluster: HashSet<String> = job_store
            .state()
            .into_iter()
            .map(|job| {
                job.labels()
                    .get(&config.job_name_label)
                    .cloned()
                    .unwrap_or_else(|| job.name_any())
            })
            .collect();
        shadow.retain(|name, (_, _, solver_pod)| {
            let targets_this_cluster =
                solver_pod.as_ref().and_then(|p| p.cluster.as_deref()) == Some(cluster_name);
            if !targets_this_cluster {
                return true;
            }
            if known_on_cluster.contains(name) {
                return true;
            }
            if solver_pods.contains_key(name) {
                return true;
            }
            false
        });
    }

    let cluster = SolverCluster {
        name: cluster_name.to_string(),
        nodes: solver_nodes,
    };

    (cluster, solver_pods)
}

/// Returns true if the Job is in a terminal state (Complete or Failed).
///
/// Checks (in order): the Complete/Failed condition with status=True,
/// `completionTime`, then the count-based fallbacks `succeeded >= completions`
/// and `failed > backoffLimit`.  The fallbacks matter because the Job
/// controller can lag for many seconds (sometimes indefinitely if pods are
/// reaped before it observes them) before writing the condition or
/// completionTime — without them, finished Jobs leak into the orphan-recovery
/// branch and overwhelm the solver.
fn is_job_finished(job: &K8sJob) -> bool {
    let Some(status) = job.status.as_ref() else {
        return false;
    };
    if status.completion_time.is_some() {
        return true;
    }
    if let Some(conditions) = status.conditions.as_ref() {
        for c in conditions {
            if (c.type_ == "Complete" || c.type_ == "Failed") && c.status == "True" {
                return true;
            }
        }
    }
    let spec = job.spec.as_ref();
    let completions = spec.and_then(|s| s.completions).unwrap_or(1);
    if status.succeeded.unwrap_or(0) >= completions {
        return true;
    }
    let backoff_limit = spec.and_then(|s| s.backoff_limit).unwrap_or(6);
    if status.failed.unwrap_or(0) > backoff_limit {
        return true;
    }
    false
}

/// Returns true if at least one pod owned by `job` is present in the pod
/// store and has already been bound to a node (`spec.nodeName` is set).
///
/// Unbound Pending pods (waiting for the Binding API call) are intentionally
/// excluded: `placement_shadow` remains authoritative until the
/// reflector confirms a bound pod, ensuring the solver does not see those
/// nodes as free between placement and binding confirmation.
fn pods_exist_for_job(pod_store: &reflector::Store<Pod>, job: &K8sJob) -> bool {
    let job_uid = job.metadata.uid.as_deref().unwrap_or("");
    pod_store.state().iter().any(|pod| {
        let is_child = pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid && r.kind == "Job"))
            .unwrap_or(false);
        if !is_child {
            return false;
        }
        pod.spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .is_some()
    })
}

/// Build per-replica statuses by examining child pods of a running Job.
fn build_replica_statuses_from_job_pods(
    pod_store: &reflector::Store<Pod>,
    job: &K8sJob,
    parallelism: u32,
) -> Vec<SolverReplicaStatus> {
    let job_uid = job.metadata.uid.as_deref().unwrap_or("");

    let mut statuses: Vec<SolverReplicaStatus> = Vec::new();
    for pod in pod_store.state() {
        let is_child = pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid && r.kind == "Job"))
            .unwrap_or(false);

        if !is_child {
            continue;
        }

        let phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("Unknown");

        if phase == "Succeeded" {
            continue;
        }

        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());

        let solver_phase = match phase {
            "Running" => Phase::Running,
            "Failed" => Phase::Failed,
            _ => Phase::Running,
        };

        statuses.push(SolverReplicaStatus {
            phase: solver_phase,
            node: if solver_phase == Phase::Running {
                node_name
            } else {
                None
            },
        });
    }

    while (statuses.len() as u32) < parallelism {
        statuses.push(SolverReplicaStatus {
            phase: Phase::Running,
            node: None,
        });
    }

    statuses
}

/// Build gang sets from gang-set annotations on observed workloads
/// and (in solver mode) workloads still in the in-memory store.
///
/// Workloads sharing the same `gang-set` annotation value form a gang.
fn build_gang_sets(
    runtimes: &[ClusterRuntime],
    config: &BinderConfig,
    store_workloads: Option<&HashMap<String, Workload>>,
    known_pods: &HashMap<String, SolverPod>,
    pod_predicate: impl Fn(&Pod) -> bool,
) -> Vec<Vec<String>> {
    let mut annotation_groups: HashMap<String, Vec<String>> = HashMap::new();

    for rt in runtimes {
        for job in rt.job_reader().state() {
            let job_name = job
                .labels()
                .get(&config.job_name_label)
                .cloned()
                .unwrap_or_else(|| job.name_any());

            if !known_pods.contains_key(&job_name) {
                continue;
            }
            if let Some(gang_id) = job.annotations().get(&config.gang_set_annotation) {
                annotation_groups
                    .entry(gang_id.clone())
                    .or_default()
                    .push(job_name);
            }
        }

        for pod in rt.pod_reader().state() {
            if !pod_predicate(&pod) {
                continue;
            }
            let owned_by_job = pod
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| r.kind == "Job"))
                .unwrap_or(false);
            if owned_by_job {
                continue;
            }

            // Match the SolverPod key used in build_cluster_state:
            // each managed standalone Pod is its own SolverPod, keyed
            // by k8s pod name.
            let pod_name = pod.name_any();
            if !known_pods.contains_key(&pod_name) {
                continue;
            }

            if let Some(gang_id) = pod.annotations().get(&config.gang_set_annotation) {
                annotation_groups
                    .entry(gang_id.clone())
                    .or_default()
                    .push(pod_name);
            }
        }
    }

    if let Some(store) = store_workloads {
        for (wl_name, workload) in store {
            if !known_pods.contains_key(wl_name) {
                continue;
            }

            let gang_id = match &workload.managed {
                ManagedObject::Job(job) => {
                    job.annotations().get(&config.gang_set_annotation).cloned()
                }
                ManagedObject::Pod(pod) => {
                    pod.annotations().get(&config.gang_set_annotation).cloned()
                }
            };

            if let Some(gang_id) = gang_id {
                annotation_groups
                    .entry(gang_id)
                    .or_default()
                    .push(wl_name.clone());
            }
        }
    }

    let mut gang_sets: Vec<Vec<String>> = annotation_groups
        .into_values()
        .filter(|members| members.len() > 1)
        .collect();

    for set in &mut gang_sets {
        set.sort();
        set.dedup();
    }

    gang_sets
}

/// Read per-node chip count. If `chip_count_label` is set, the count comes
/// from that node label (used by test clusters without a device plugin);
/// otherwise it comes from `status.allocatable`/`status.capacity` for the
/// configured extended resource.
pub(super) fn node_chip_capacity(node: &Node, config: &BinderConfig) -> u32 {
    if let Some(label) = config.chip_count_label.as_deref() {
        return node
            .labels()
            .get(label)
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
    }
    node.status
        .as_ref()
        .and_then(|s| {
            s.allocatable
                .as_ref()
                .and_then(|a| a.get(&config.chip_resource))
                .or_else(|| {
                    s.capacity
                        .as_ref()
                        .and_then(|c| c.get(&config.chip_resource))
                })
        })
        .and_then(|q| q.0.parse::<u32>().ok())
        .unwrap_or(0)
}

/// Return Ready, non-cordoned nodes that match the bridge's filter.
///
/// Solver mode (`config.require_taint == true`) returns only nodes that
/// carry our scheduler taint — the GPU pool reserved for us.  Observe-only
/// mode (`require_taint == false`) returns every Ready node, since the UI
/// is showing whatever workloads happen to be on the cluster, not what
/// we're allowed to schedule onto.
pub(super) fn get_candidate_nodes(
    store: &reflector::Store<Node>,
    config: &BinderConfig,
) -> Vec<Arc<Node>> {
    store
        .state()
        .into_iter()
        .filter(|node| {
            if config.require_taint {
                let taints = node
                    .spec
                    .as_ref()
                    .and_then(|s| s.taints.as_ref())
                    .map(|t| t.as_slice())
                    .unwrap_or_default();
                let has_taint = taints.iter().any(|t| {
                    t.key == config.taint_key
                        && t.value.as_deref() == Some(&config.taint_value)
                        && t.effect == "NoSchedule"
                });
                if !has_taint {
                    return false;
                }
            }

            // Skip cordoned nodes (on-call team marks bad hardware this way).
            if node
                .spec
                .as_ref()
                .and_then(|s| s.unschedulable)
                .unwrap_or(false)
            {
                return false;
            }

            // Skip nodes that aren't Ready.
            node.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conditions| {
                    conditions
                        .iter()
                        .any(|c| c.type_ == "Ready" && c.status == "True")
                })
                .unwrap_or(false)
        })
        .collect()
}
