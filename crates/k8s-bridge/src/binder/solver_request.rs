//! Build the [`SolverRequest`] each cycle.
//!
//! Aggregates state from three sources:
//!   1. Per-cluster reflectors (Jobs, Pods, Nodes) → `build_cluster_state`.
//!   2. The placement shadow — recently-applied placements not yet visible
//!      in the reflector — re-injected so the solver doesn't double-book
//!      nodes during the gap.
//!   3. The workload store — pending/suspended workloads not yet on any
//!      cluster.
//!
//! Also exposes a few small helpers shared with tests: `build_cluster_state`,
//! `node_chip_capacity`, `get_candidate_nodes`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::batch::v1::Job as K8sJob;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{ResourceExt, runtime::reflector};
use tracing::warn;

use crate::job_store::{self, ManagedObject, Workload};
use crate::solver_types::{
    ClusterState as SolverCluster, Node as SolverNode, Phase, Pod as SolverPod,
    PodReplicaStatus as SolverReplicaStatus, SolverRequest,
};

use super::extract::{
    extract_job_metadata, extract_pod_metadata, extract_workload_metadata, is_known_quota,
};
use super::{BinderConfig, ClusterRuntime, PlacementShadow};

/// Build a [`SolverRequest`] aggregating state from all clusters and the
/// workload store.
pub(super) fn build_solver_request_multi(
    runtimes: &[ClusterRuntime],
    config: &BinderConfig,
    store_workloads: &HashMap<String, Workload>,
    placement_shadow: &mut PlacementShadow,
) -> SolverRequest {
    let mut cluster_states: Vec<SolverCluster> = Vec::with_capacity(runtimes.len());
    let mut pods: HashMap<String, SolverPod> = HashMap::new();

    for rt in runtimes {
        let node_r = rt.node_reader();
        let pod_r = rt.pod_reader();
        let job_r = rt.job_reader();
        let (cluster, cluster_pods) =
            build_cluster_state(&node_r, &pod_r, &job_r, &rt.name, config, placement_shadow);
        cluster_states.push(cluster);

        for (name, pod) in cluster_pods {
            pods.entry(name).or_insert(pod);
        }
    }

    // Inject just-placed workloads that have a pending entry with a SolverPod
    // snapshot but are not yet visible in any cluster reflector.  This closes
    // the 1-cycle gap between store removal (apply_assignments_multi removes
    // the workload from the store as soon as the k8s create succeeds) and the
    // job reflector confirming the new Job.  Without this, the solver would
    // see the assigned nodes as free and could place another workload there.
    for (name, (_, _, solver_pod)) in placement_shadow.iter() {
        if pods.contains_key(name) {
            continue;
        }
        if let Some(pod) = solver_pod {
            pods.entry(name.clone()).or_insert_with(|| pod.clone());
        }
    }

    // Store-submitted workloads: not yet placed on any cluster, or suspended
    // Pods pinned to a cluster.
    for (wl_name, workload) in store_workloads {
        if pods.contains_key(wl_name) {
            continue;
        }

        // Skip workloads in backoff — they failed placement too many times
        // and will be retried when cluster state changes.
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

        // Store-side workloads are always queued (suspended workloads stay
        // on their cluster and reach the solver via reflector state).
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
            },
        );
    }

    let gang_sets = build_gang_sets(runtimes, config, store_workloads, &pods);

    SolverRequest {
        clusters: cluster_states,
        pods,
        gang_sets,
        quotas: config.quotas.clone(),
        time_limit: 30.0,
    }
}

/// Build the [`SolverCluster`] (topology) and solver pods for a single cluster.
///
/// Job reflector provides job-level state (suspended, parallelism).
/// Pod reflector provides per-replica node assignments and managed standalone
/// Pods.
pub(super) fn build_cluster_state(
    node_store: &reflector::Store<Node>,
    pod_store: &reflector::Store<Pod>,
    job_store: &reflector::Store<K8sJob>,
    cluster_name: &str,
    config: &BinderConfig,
    placement_shadow: &mut PlacementShadow,
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

    let mut solver_pods: HashMap<String, SolverPod> = HashMap::new();

    // --- Jobs on the cluster ---
    for job in job_store.state() {
        let job_name = match job.labels().get(&config.job_name_label) {
            Some(name) => name.clone(),
            None => job.name_any(),
        };

        // Skip terminal Jobs entirely. Without this, every completed/failed
        // Job that hasn't been GC'd by k8s would be padded with phantom
        // "Running/None" replicas (see build_replica_statuses_from_job_pods)
        // and reach the solver/snapshot as zombie queued work.
        if is_job_finished(&job) {
            placement_shadow.remove(&job_name);
            continue;
        }

        let (chips, chip_type, priority, quota, parallelism) = extract_job_metadata(&job, config);

        if !is_known_quota(config, &quota) {
            warn!(
                workload = %job_name,
                quota = %quota,
                "skipping reflector-discovered Job with unknown quota"
            );
            placement_shadow.remove(&job_name);
            continue;
        }

        // Footgun for kubectl-applied Jobs: if the pod template's
        // schedulerName isn't ours, kube-scheduler will bind the pods and
        // bypass our binder entirely. Warn so the operator notices.
        // Mirrors the per-tick warn pattern of the unknown-quota check
        // above; stops as soon as the manifest is fixed.
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

        let is_suspended = job.spec.as_ref().and_then(|s| s.suspend).unwrap_or(false);

        // Three placement states for an active Job's pods, in priority order:
        //   1. Pods bound to nodes (reflector-confirmed) — use their nodes.
        //   2. Pending-nodes entry — placement decision in flight.
        //   3. Neither — orphaned. Pods missed their binding window (bridge
        //      restart, expired PENDING_TTL, etc.) and the solver has lost
        //      track. Emit with cluster=None so the solver re-schedules.
        let mut orphan = false;
        let statuses_by_replica = if is_suspended {
            // `spec.suspend=true` has been patched, but pods may still be
            // terminating (graceful shutdown window, up to 30 s by default).
            // Keep their nodes occupied in the solver's view until the pod
            // store confirms they are gone — otherwise the solver may
            // double-book those nodes.
            if pods_exist_for_job(pod_store, &job) {
                build_replica_statuses_from_job_pods(pod_store, &job, parallelism)
            } else {
                placement_shadow.remove(&job_name);
                (0..parallelism)
                    .map(|_| SolverReplicaStatus {
                        phase: Phase::Suspended,
                        node: None,
                    })
                    .collect()
            }
        } else if pods_exist_for_job(pod_store, &job) {
            placement_shadow.remove(&job_name);
            build_replica_statuses_from_job_pods(pod_store, &job, parallelism)
        } else if let Some((node_counts, _, _)) = placement_shadow.get(&job_name) {
            // Pods not yet visible but we know where they were placed.
            // Reconstruct statuses from the recorded node assignments so
            // those nodes appear occupied to the solver this cycle.
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
            // Orphan: no bound pods, no in-flight placement.  Emit as
            // fresh-pending so the solver picks new nodes; the next cycle's
            // bind_pending_pods will bind the existing Pending pods to them.
            orphan = true;
            (0..parallelism)
                .map(|_| SolverReplicaStatus {
                    phase: Phase::Running,
                    node: None,
                })
                .collect()
        };

        solver_pods.insert(
            job_name,
            SolverPod {
                chips_per_replica: chips,
                chip_type,
                priority,
                quota,
                cluster: if orphan {
                    None
                } else {
                    Some(cluster_name.to_string())
                },
                statuses_by_replica,
            },
        );
    }

    // --- Managed standalone Pods on the cluster ---
    for pod in pod_store.state() {
        let is_managed = pod
            .labels()
            .get(&config.managed_by_label)
            .map(|v| v == &config.managed_by_value)
            .unwrap_or(false);
        if !is_managed {
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

        // Per the model.py contract, each k8s Pod becomes its own
        // single-replica SolverPod — independently schedulable and
        // reclaimable, no gang-scheduling.  This means the SolverPod key
        // must be the k8s pod name, not the `job-name` label: a
        // Deployment's ReplicaSet stamps the same `job-name` onto every
        // replica's pod template (see deployment_driver.py), so keying
        // by label would collapse N replicas into one entry and silently
        // drop the rest from the solver's view.
        //
        // Aside: each replica adds one entry to the SolverRequest. Wire
        // and solve cost scale with replicas × candidate-nodes, which is
        // the same regardless of grouping — a 50-replica Deployment is
        // 50 placement decisions either way.
        let pod_name = pod.name_any();

        // Defensive: skip if a Job has already claimed this name.  This
        // is rare (would require a managed Pod whose name collides with
        // a Job's `job-name` label value) but cheap to check.
        if solver_pods.contains_key(&pod_name) {
            continue;
        }

        let (chips, chip_type, priority, quota, _) = extract_pod_metadata(&pod, config);

        if !is_known_quota(config, &quota) {
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
        let solver_phase = Phase::Running;

        // Cluster pinning: only set `cluster` once the Pod has actually
        // landed on a node.  A Pending Pod (Deployment-spawned, not yet
        // bound) needs the solver's admission step to consider it — and
        // both the heuristic and MILP solvers gate admission on
        // `pod.cluster is None`, so leaving cluster=Some on a Pending
        // Pod silently drops it from placement.
        //
        // v0 assumption: this is safe because the bridge currently runs
        // against one cluster.  Multi-cluster needs a real cluster-pinned
        // pending category in the solver — the Pod can only be bound
        // back to the cluster it lives on, regardless of solver choice.
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
                    phase: solver_phase,
                    node: node_name,
                }],
            },
        );
    }

    // Clear pending entries for jobs that no longer exist on this cluster.
    // When a job is deleted externally (e.g. by test cleanup or operator
    // action) its pending entry is never touched by the job-processing loop
    // above because the job has vanished from the reflector.  Without this
    // cleanup the entry would linger for the full 30 s TTL, causing the
    // injection loop in build_solver_request_multi to keep inserting a
    // phantom running pod and blocking placement on those nodes.
    //
    // Only entries targeting *this* cluster are cleaned up here; entries for
    // other clusters are left intact.
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
    placement_shadow.retain(|name, (_, _, solver_pod)| {
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

/// Build gang sets from workload annotations.
///
/// Workloads sharing the same `gang-set` annotation value form a gang set.
fn build_gang_sets(
    runtimes: &[ClusterRuntime],
    config: &BinderConfig,
    store_workloads: &HashMap<String, Workload>,
    known_pods: &HashMap<String, SolverPod>,
) -> Vec<Vec<String>> {
    let mut annotation_groups: HashMap<String, Vec<String>> = HashMap::new();

    for rt in runtimes {
        for job in rt.job_reader().state() {
            let job_name = match job.labels().get(&config.job_name_label) {
                Some(name) => name.clone(),
                None => job.name_any(),
            };

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
            let is_managed = pod
                .labels()
                .get(&config.managed_by_label)
                .map(|v| v == &config.managed_by_value)
                .unwrap_or(false);
            if !is_managed {
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

            // Match the SolverPod key used in build_cluster_state above:
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

    for (wl_name, workload) in store_workloads {
        if !known_pods.contains_key(wl_name) {
            continue;
        }

        let gang_id = match &workload.managed {
            ManagedObject::Job(job) => job.annotations().get(&config.gang_set_annotation).cloned(),
            ManagedObject::Pod(pod) => pod.annotations().get(&config.gang_set_annotation).cloned(),
        };

        if let Some(gang_id) = gang_id {
            annotation_groups
                .entry(gang_id)
                .or_default()
                .push(wl_name.clone());
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

/// Return nodes that have our taint.
pub(super) fn get_candidate_nodes(
    store: &reflector::Store<Node>,
    config: &BinderConfig,
) -> Vec<Arc<Node>> {
    store
        .state()
        .into_iter()
        .filter(|node| {
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
