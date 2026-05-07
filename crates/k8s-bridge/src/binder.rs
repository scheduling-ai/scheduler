//! Workload binder: watches cluster state, calls the Python solver, and manages
//! workloads on target clusters.
//!
//! Supports two workload types following Kueue's abstraction:
//! - **batch/v1 Job**: lifecycle managed via `spec.suspend` toggle.
//! - **v1 Pod**: lifecycle managed via create/delete (following Kueue's
//!   preemption model — Pods are deleted, not re-gated).
//!
//! Supports multi-cluster operation: each cluster gets its own kube client and
//! reflectors. State from all clusters is aggregated into a single
//! [`SolverRequest`], and solver assignments are routed back to the correct
//! cluster's API server.
//!
//! Lifecycle:
//! - Workloads are submitted to the central store via the HTTP API.
//! - On first placement: workload is created on the target cluster.
//!   - Jobs: `spec.suspend=false` + `spec.schedulerName` set so pods go Pending
//!     waiting for our binder to bind them via the k8s Binding API.
//!   - Pods: `spec.nodeName` set directly at creation (bypasses scheduler).
//! - Binding: each cycle, Pending pods with our schedulerName are bound to
//!   their target nodes via `POST /api/v1/namespaces/{ns}/pods/{name}/binding`.
//!   This replaces the k8s scheduler entirely — no nodeAffinity, no taints.
//! - On suspension:
//!   - Jobs: `spec.suspend` is patched to `true` (k8s deletes pods atomically).
//!   - Pods: deleted from cluster, re-enter store as `Suspended(cluster)`.
//! - On unsuspension:
//!   - Jobs: `spec.suspend` set to `false`; pods go Pending and are bound
//!     to the solver's new node assignments by the next binding pass.
//!   - Pods: created on pinned cluster with `spec.nodeName` set directly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use k8s_openapi::api::batch::v1::Job as K8sJob;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Client, ResourceExt,
    api::Api,
    config::{KubeConfigOptions, Kubeconfig},
    runtime::{reflector, watcher},
};
use tracing::{info, warn};

#[cfg(test)]
use crate::job_store::ManagedObject;
use crate::job_store::{SchedulerState, SchedulerStateInner, Workload, WorkloadStore};

use crate::snapshot::{self, SnapshotState};
use crate::solver;
#[cfg(test)]
use crate::solver_types::{ClusterState as SolverCluster, Node as SolverNode, ScheduleResult};
use crate::solver_types::{
    Phase, Pod as SolverPod, PodReplicaStatus as SolverReplicaStatus, SolverRequest,
};

/// Configuration for the binder, replacing hardcoded constants.
#[derive(Clone)]
pub struct BinderConfig {
    /// Name used in `spec.schedulerName` on Job pod templates so the k8s
    /// scheduler ignores them; our binder binds them via the Binding API.
    pub scheduler_name: String,
    /// Taint key/value identifying GPU nodes managed by this scheduler.
    /// The admin sets this taint to prevent the k8s scheduler from placing
    /// other workloads on those nodes; we use it to filter candidate nodes.
    pub taint_key: String,
    pub taint_value: String,
    pub chip_label: String,
    pub chip_resource: String,
    /// If set, read per-node chip count from this node label instead of
    /// the extended resource in `status.allocatable`/`status.capacity`.
    /// Used by test clusters that advertise chips via labels rather than
    /// a device plugin.
    pub chip_count_label: Option<String>,
    /// If set, read the per-replica chip count from this annotation on
    /// the Job/Pod as a fallback when `resources.requests[chip_resource]`
    /// is missing or zero. Used by demo clusters without a GPU device
    /// plugin: the workload can't request `nvidia.com/gpu` (kubelet would
    /// reject the pod) but the scheduler still needs to know how many
    /// chips each replica needs.
    pub chips_annotation: Option<String>,
    pub job_name_label: String,
    pub priority_annotation: String,
    pub quota_annotation: String,
    pub gang_set_annotation: String,
    pub managed_by_label: String,
    pub managed_by_value: String,
    pub solver_interval: Duration,
    pub quotas: Vec<crate::solver_types::Quota>,
    /// Python solver to use (e.g. "heuristic").
    pub solver_name: String,
}

impl Default for BinderConfig {
    fn default() -> Self {
        Self {
            scheduler_name: "custom-scheduler".into(),
            taint_key: "scheduler".into(),
            taint_value: "custom".into(),
            chip_label: "accelerator".into(),
            chip_resource: "nvidia.com/gpu".into(),
            chip_count_label: None,
            chips_annotation: None,
            job_name_label: "scheduler.example.com/job-name".into(),
            priority_annotation: "scheduler.example.com/priority".into(),
            quota_annotation: "scheduler.example.com/quota".into(),
            gang_set_annotation: "scheduler.example.com/gang-set".into(),
            managed_by_label: "scheduler.example.com/managed-by".into(),
            managed_by_value: "custom-scheduler".into(),
            solver_interval: Duration::from_secs(5),
            quotas: vec![],
            solver_name: "heuristic".into(),
        }
    }
}

/// Swappable reflector store — allows the retry loop to replace the
/// underlying store when a reflector stream is restarted.
type SharedStore<T> = Arc<std::sync::RwLock<reflector::Store<T>>>;

/// In-memory shadow of placements the binder has applied to k8s but the
/// reflector has not yet observed.  Bridges the gap between two
/// eventually-consistent sources of truth: the workload store (removed as
/// soon as the k8s create succeeds) and the cluster reflector (catches up a
/// cycle or two later).
///
/// Without this shadow the solver would, for one cycle, see the assigned
/// nodes as free and could over-commit them.
///
/// Each entry carries: per-node replica counts (used by `bind_pending_pods`
/// to know which Pods to bind to which nodes), an insertion timestamp (TTL
/// safety net), and an optional `SolverPod` snapshot (re-injected into the
/// solver request to keep the workload accounted for during the gap).
///
/// Lifecycle:
///
/// - **Inserted**: in the binder loop after a successful k8s create
///   (`apply_assignments_multi` returns) — see the post-apply block in
///   `run_loop`.
/// - **Removed** (any one of):
///   1. Reflector confirms the Job and we account for its Pods properly
///      (multiple branches in `build_cluster_state`).
///   2. The Job has vanished from the cluster reflector for long enough
///      that the targeted-cluster cleanup at the end of
///      `build_cluster_state` drops it.
///   3. TTL expiry (`PENDING_TTL`) — last-resort safety net for entries
///      neither confirmed nor explicitly cleaned.
///
/// State is in-memory only.  On bridge restart the map is empty and the
/// reflector's initial list-watch repopulates ground truth before the
/// first solver cycle runs.
pub(super) type PlacementShadow =
    HashMap<String, (HashMap<String, u32>, std::time::Instant, Option<SolverPod>)>;

/// Per-cluster runtime state: kube client and reflector readers.
pub(super) struct ClusterRuntime {
    pub(super) name: String,
    client: Client,
    node_store: SharedStore<Node>,
    pod_store: SharedStore<Pod>,
    job_store: SharedStore<K8sJob>,
    /// Set to false if a reflector stream dies. The binder pauses
    /// scheduling for this cluster until reflectors are healthy.
    nodes_healthy: Arc<AtomicBool>,
    pods_healthy: Arc<AtomicBool>,
    jobs_healthy: Arc<AtomicBool>,
}

impl ClusterRuntime {
    pub(super) fn node_reader(&self) -> reflector::Store<Node> {
        self.node_store.read().unwrap().clone()
    }

    pub(super) fn pod_reader(&self) -> reflector::Store<Pod> {
        self.pod_store.read().unwrap().clone()
    }

    pub(super) fn job_reader(&self) -> reflector::Store<K8sJob> {
        self.job_store.read().unwrap().clone()
    }
}

/// Create a kube [`Client`] for a specific kubeconfig context.
async fn client_for_context(context: Option<&str>) -> Result<Client> {
    let kubeconfig = Kubeconfig::read().context("failed to read kubeconfig")?;
    let options = KubeConfigOptions {
        context: context.map(String::from),
        ..Default::default()
    };
    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &options)
        .await
        .context("failed to build kube config")?;
    Client::try_from(config).context("failed to create kube client")
}

/// Initialise reflectors for a single cluster, returning the runtime handle.
async fn init_cluster(
    name: String,
    context: Option<&str>,
    config: &BinderConfig,
) -> Result<ClusterRuntime> {
    let client = client_for_context(context).await?;

    // Start unhealthy: the binder skips scheduling cycles until each
    // reflector emits an `InitDone` event (i.e. its initial LIST is
    // complete and the kube client has actually round-tripped to the
    // cluster API server).  Without this gate the binder loop runs
    // before the kube client is warm — the very first non-watch RPC
    // (`jobs_api.create`) was observed taking up to 4m43s on a freshly
    // created kind cluster (CircleCI pipeline #42), stalling the whole
    // binder loop and timing out e2e tests.
    let nodes_healthy = Arc::new(AtomicBool::new(false));
    let pods_healthy = Arc::new(AtomicBool::new(false));
    let jobs_healthy = Arc::new(AtomicBool::new(false));

    // Node reflector with auto-recovery.
    let node_writer = reflector::store::Writer::default();
    let node_shared: SharedStore<Node> = Arc::new(std::sync::RwLock::new(node_writer.as_reader()));
    {
        let client = client.clone();
        let shared = Arc::clone(&node_shared);
        let flag = Arc::clone(&nodes_healthy);
        let cluster_name = name.clone();
        tokio::spawn(run_reflector_with_retry(
            cluster_name,
            "node",
            flag,
            shared,
            node_writer,
            move || {
                let api: Api<Node> = Api::all(client.clone());
                watcher(api, Default::default())
            },
        ));
    }

    // Pod reflector with auto-recovery.
    let pod_writer = reflector::store::Writer::default();
    let pod_shared: SharedStore<Pod> = Arc::new(std::sync::RwLock::new(pod_writer.as_reader()));
    {
        let client = client.clone();
        let shared = Arc::clone(&pod_shared);
        let flag = Arc::clone(&pods_healthy);
        let cluster_name = name.clone();
        tokio::spawn(run_reflector_with_retry(
            cluster_name,
            "pod",
            flag,
            shared,
            pod_writer,
            move || {
                let api: Api<Pod> = Api::all(client.clone());
                watcher(api, Default::default())
            },
        ));
    }

    // Job reflector with auto-recovery.
    let job_writer = reflector::store::Writer::default();
    let job_shared: SharedStore<K8sJob> = Arc::new(std::sync::RwLock::new(job_writer.as_reader()));
    {
        let client = client.clone();
        let shared = Arc::clone(&job_shared);
        let flag = Arc::clone(&jobs_healthy);
        let cluster_name = name.clone();
        let label_selector = format!("{}={}", config.managed_by_label, config.managed_by_value);
        tokio::spawn(run_reflector_with_retry(
            cluster_name,
            "job",
            flag,
            shared,
            job_writer,
            move || {
                let api: Api<K8sJob> = Api::all(client.clone());
                let wc = watcher::Config::default().labels(&label_selector);
                watcher(api, wc)
            },
        ));
    }

    info!(cluster = %name, "cluster reflectors started");
    Ok(ClusterRuntime {
        name,
        client,
        node_store: node_shared,
        pod_store: pod_shared,
        job_store: job_shared,
        nodes_healthy,
        pods_healthy,
        jobs_healthy,
    })
}

/// Drive a reflector stream with automatic recovery. The kube-rs watcher
/// handles reconnection internally (re-list on 410 Gone, etc.), so most
/// errors are transient. When the stream truly ends (e.g. RBAC
/// misconfiguration), mark the cluster unhealthy, wait with exponential
/// backoff, then create a fresh reflector and try again.
async fn run_reflector_with_retry<K, W, F>(
    cluster: String,
    resource: &'static str,
    healthy: Arc<AtomicBool>,
    shared_store: SharedStore<K>,
    initial_writer: reflector::store::Writer<K>,
    make_watcher: F,
) where
    K: kube::Resource + Clone + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
    W: futures::Stream<Item = std::result::Result<watcher::Event<K>, watcher::Error>>,
    F: Fn() -> W + Send + 'static,
{
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    let mut backoff = Duration::from_secs(1);

    // First run uses the pre-built writer (reader already installed).
    // Healthy is gated on InitDone — the watcher emits this once the
    // initial LIST has been replayed into the store, which is also when
    // we can be confident the kube client has actually reached the API
    // server.  Until then the binder skips scheduling cycles.
    let stream = reflector::reflector(initial_writer, make_watcher());
    let mut stream = std::pin::pin!(stream);
    let mut saw_init_done = false;
    while let Some(item) = stream.next().await {
        backoff = Duration::from_secs(1);
        match &item {
            Ok(watcher::Event::InitDone) if !saw_init_done => {
                saw_init_done = true;
                healthy.store(true, Ordering::Release);
                info!(
                    cluster = %cluster,
                    resource,
                    "reflector ready (initial list complete)"
                );
            }
            Err(e) => warn!(
                cluster = %cluster,
                resource,
                "reflector transient error (watcher will re-list): {e}"
            ),
            _ => {}
        }
    }

    // Retry loop for subsequent attempts.
    loop {
        healthy.store(false, Ordering::Release);
        warn!(
            cluster = %cluster,
            resource,
            backoff_secs = backoff.as_secs(),
            "reflector stream ended, will retry"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);

        // Create a fresh writer+reader and swap into shared state.
        let writer = reflector::store::Writer::default();
        *shared_store.write().unwrap() = writer.as_reader();

        let stream = reflector::reflector(writer, make_watcher());
        let mut stream = std::pin::pin!(stream);

        // Wait for InitDone before marking healthy: the re-listed store
        // is empty until the LIST replays, so any earlier "Apply" event
        // would have us scheduling against a partial view.
        let mut saw_init_done = false;
        while let Some(item) = stream.next().await {
            match &item {
                Ok(watcher::Event::InitDone) if !saw_init_done => {
                    saw_init_done = true;
                    healthy.store(true, Ordering::Release);
                    info!(
                        cluster = %cluster,
                        resource,
                        "reflector recovered"
                    );
                    backoff = Duration::from_secs(1);
                }
                Err(e) => warn!(
                    cluster = %cluster,
                    resource,
                    "reflector transient error (watcher will re-list): {e}"
                ),
                _ => {}
            }
        }
    }
}

/// A cluster to connect to, specified as a name and an optional kubeconfig
/// context.
pub struct ClusterSpec {
    pub name: String,
    pub context: Option<String>,
}

/// Run the binder loop: reflect nodes/pods/jobs, call solver, manage workloads.
pub async fn run(
    dry_run: bool,
    clusters: &[ClusterSpec],
    config: &BinderConfig,
    store: Option<WorkloadStore>,
    scheduler_state: Option<SchedulerState>,
    snapshot_state: Option<SnapshotState>,
    record_path: Option<std::path::PathBuf>,
) -> Result<()> {
    anyhow::ensure!(
        !clusters.is_empty(),
        "at least one cluster must be specified"
    );

    let mut runtimes: Vec<ClusterRuntime> = Vec::with_capacity(clusters.len());
    for spec in clusters {
        let rt = init_cluster(spec.name.clone(), spec.context.as_deref(), config).await?;
        runtimes.push(rt);
    }

    info!(
        managed_by = config.managed_by_value,
        dry_run,
        clusters = runtimes.len(),
        has_workload_store = store.is_some(),
        "solver binder started"
    );

    let clients: HashMap<String, Client> = runtimes
        .iter()
        .map(|rt| (rt.name.clone(), rt.client.clone()))
        .collect();

    let mut interval = tokio::time::interval(config.solver_interval);
    let mut prev_cluster_workloads: HashSet<String> = HashSet::new();
    // Jobs placed or unsuspended in a recent cycle whose pods may not yet be
    // visible in reflectors.  Entries are cleared by build_cluster_state as
    // soon as the reflector confirms the pods, or expired after a TTL if the
    // pods never appear (e.g. job externally deleted after placement).
    //
    // The Option<SolverPod> carries a snapshot of the pod as-placed (chip
    // info, cluster, node assignments).  It is Some for fresh assignments
    // (where the store entry has been removed before the reflector confirms
    // the Job) and None for unsuspensions (the Job is already in the
    // reflector so build_cluster_state handles it directly).  The snapshot
    // is used in build_solver_request_multi to inject the pod into the
    // solver request during the gap cycle.
    const PENDING_TTL: Duration = Duration::from_secs(30);
    let mut placement_shadow: PlacementShadow = HashMap::new();
    let mut seq: u64 = 0;
    let mut last_tick_at: Option<std::time::Instant> = None;
    let mut was_unhealthy = false;
    loop {
        interval.tick().await;
        let cycle_start = std::time::Instant::now();
        let tick_gap_ms = last_tick_at
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0);
        last_tick_at = Some(cycle_start);

        // Expire pending entries whose pods never appeared (e.g. job was
        // deleted externally between placement and reflector confirmation).
        placement_shadow.retain(|_, (_, inserted, _)| inserted.elapsed() < PENDING_TTL);

        // Pause if any reflector has died.
        let unhealthy: Vec<&str> = runtimes
            .iter()
            .filter(|rt| {
                !rt.nodes_healthy.load(Ordering::Acquire)
                    || !rt.pods_healthy.load(Ordering::Acquire)
                    || !rt.jobs_healthy.load(Ordering::Acquire)
            })
            .map(|rt| rt.name.as_str())
            .collect();
        if !unhealthy.is_empty() {
            warn!(
                clusters = ?unhealthy,
                tick_gap_ms,
                "reflectors unhealthy, skipping scheduling cycle"
            );
            was_unhealthy = true;
            continue;
        }
        if was_unhealthy {
            info!(
                tick_gap_ms,
                "reflectors recovered, resuming scheduling cycles"
            );
            was_unhealthy = false;
        }

        // Collect workload names currently on clusters (from reflectors).
        let mut current_cluster_workloads: HashSet<String> = HashSet::new();
        for rt in &runtimes {
            for job in rt.job_reader().state() {
                if let Some(name) = job
                    .labels()
                    .get(&config.managed_by_label)
                    .filter(|v| *v == &config.managed_by_value)
                    .and_then(|_| job.labels().get(&config.job_name_label))
                {
                    current_cluster_workloads.insert(name.clone());
                }
            }
            for pod in rt.pod_reader().state() {
                if let Some(name) = pod
                    .labels()
                    .get(&config.managed_by_label)
                    .filter(|v| *v == &config.managed_by_value)
                    .and_then(|_| pod.labels().get(&config.job_name_label))
                {
                    current_cluster_workloads.insert(name.clone());
                }
            }
        }

        // If a workload disappeared from cluster reflectors, capacity was
        // freed. Reset backoff counters so previously-excluded workloads
        // get another chance.
        if prev_cluster_workloads
            .iter()
            .any(|name| !current_cluster_workloads.contains(name))
            && let Some(ref s) = store
        {
            if let Err(e) = s.reset_all_failures().await {
                warn!("failed to reset backoff counters: {e}");
            } else {
                info!("cluster capacity changed, reset backoff counters");
            }
        }
        prev_cluster_workloads = current_cluster_workloads;

        let store_snapshot: HashMap<String, Workload> = match &store {
            Some(s) => s.snapshot().await,
            None => HashMap::new(),
        };

        // Check for work: store workloads or managed objects on clusters.
        let has_cluster_jobs = runtimes
            .iter()
            .any(|rt| !rt.job_reader().state().is_empty());
        let has_cluster_pods = runtimes.iter().any(|rt| {
            rt.pod_reader().state().iter().any(|pod| {
                pod.labels()
                    .get(&config.managed_by_label)
                    .map(|v| v == &config.managed_by_value)
                    .unwrap_or(false)
            })
        });
        if !has_cluster_jobs && !has_cluster_pods && store_snapshot.is_empty() {
            // No work this cycle — still emit a snapshot so the UI sees
            // cluster capacity and heartbeats. Mirrors `loop_runner`'s
            // "empty" frame (py-scheduler/scheduler/loop_runner.py:455).
            seq += 1;
            if let Some(ref snap) = snapshot_state {
                let request = build_solver_request_multi(
                    &runtimes,
                    config,
                    &store_snapshot,
                    &mut placement_shadow,
                );
                let frame = snapshot::build_frame(seq, &config.solver_name, &request, "empty", 0);
                *snap.lock().await = Some(frame);
            }
            info!(
                seq,
                tick_gap_ms,
                cycle_ms = cycle_start.elapsed().as_millis() as u64,
                "idle cycle (no work)"
            );
            continue;
        }

        // Bind any Pending pods that appeared since the last cycle.  This
        // must happen before building the solver request so that nodes being
        // bound this cycle are already occupied in the solver's view (via
        // placement_shadow — cleared once the reflector confirms).
        bind_pending_pods(&runtimes, &clients, config, &placement_shadow).await;

        info!(
            store_workloads = store_snapshot.len(),
            "building solver request"
        );
        let request =
            build_solver_request_multi(&runtimes, config, &store_snapshot, &mut placement_shadow);

        let request_pods = request.pods.len();
        let request_gangs = request.gang_sets.len();
        let request_clusters = request.clusters.len();

        seq += 1;
        let solve_started = std::time::Instant::now();
        let solve_outcome =
            solver::call_solver(&request, record_path.as_deref(), &config.solver_name).await;
        let duration_ms = solve_started.elapsed().as_millis() as u64;

        let solve_status: &str = match &solve_outcome {
            Ok(r) => r.solver_status.as_str(),
            Err(_) => "error",
        };
        info!(
            seq,
            tick_gap_ms,
            store_workloads = store_snapshot.len(),
            pods = request_pods,
            gangs = request_gangs,
            clusters = request_clusters,
            solve_ms = duration_ms,
            status = solve_status,
            "solver call complete"
        );

        if let Some(ref snap) = snapshot_state {
            let solver_status = match &solve_outcome {
                Ok(r) => r.solver_status.clone(),
                Err(_) => "error".to_string(),
            };
            let frame = snapshot::build_frame(
                seq,
                &config.solver_name,
                &request,
                &solver_status,
                duration_ms,
            );
            *snap.lock().await = Some(frame);
        }

        match solve_outcome {
            Ok(result) => {
                let diff = diff_schedule(&request, &result);

                if let Some(ref sched) = scheduler_state {
                    update_scheduler_state(sched, &diff, &request).await;
                }

                let ctx = ApplyContext {
                    clients: clients.clone(),
                    dry_run,
                    config: config.clone(),
                    store_snapshot: store_snapshot.clone(),
                    store: store.clone(),
                    cluster_job_readers: runtimes
                        .iter()
                        .map(|rt| (rt.name.clone(), rt.job_reader()))
                        .collect(),
                    cluster_pod_readers: runtimes
                        .iter()
                        .map(|rt| (rt.name.clone(), rt.pod_reader()))
                        .collect(),
                };
                let (suspend_count, unsuspend_count, assign_count) =
                    (diff.suspend.len(), diff.unsuspend.len(), diff.assign.len());
                let apply_started = std::time::Instant::now();
                apply_assignments_multi(&diff, &ctx).await;
                let apply_ms = apply_started.elapsed().as_millis() as u64;
                info!(
                    seq,
                    assigned = assign_count,
                    suspended = suspend_count,
                    unsuspended = unsuspend_count,
                    apply_ms,
                    cycle_ms = cycle_start.elapsed().as_millis() as u64,
                    "apply cycle complete"
                );

                // Record placements so the next cycle's solver sees the nodes
                // as occupied even before the pod reflector catches up.
                //
                // For new assignments we also store a SolverPod snapshot so
                // that build_solver_request_multi can inject the pod into the
                // solver request during the 1-cycle gap between store removal
                // (apply_assignments_multi removes the workload from the store
                // immediately) and job-reflector confirmation.  Without this,
                // the solver would not see the just-placed workload as
                // occupying any node and could over-commit capacity.
                let now = std::time::Instant::now();
                for (name, (cluster, node_counts)) in &diff.assign {
                    let solver_pod = store_snapshot.get(name).map(|wl| {
                        let (chips, chip_type, priority, quota, _) =
                            extract_workload_metadata(&wl.managed, config);
                        let statuses = node_counts
                            .iter()
                            .flat_map(|(node, &count)| {
                                (0..count).map(move |_| SolverReplicaStatus {
                                    phase: Phase::Running,
                                    node: Some(node.clone()),
                                })
                            })
                            .collect();
                        SolverPod {
                            chips_per_replica: chips,
                            chip_type,
                            priority,
                            quota,
                            cluster: Some(cluster.clone()),
                            statuses_by_replica: statuses,
                        }
                    });
                    placement_shadow.insert(name.clone(), (node_counts.clone(), now, solver_pod));
                }
                for (name, node_counts) in &diff.unsuspend {
                    // Unsuspend entries: job is already in the reflector, so
                    // build_cluster_state handles capacity directly.  No pod
                    // snapshot needed.
                    placement_shadow.insert(name.clone(), (node_counts.clone(), now, None));
                }

                // Update backoff counters for store workloads: increment
                // for those still queued, reset for those that were placed.
                if let Some(ref s) = store {
                    for name in &diff.queue_order {
                        if let Some(wl) = s.get(name).await {
                            let next = wl.consecutive_failures.saturating_add(1);
                            if let Err(e) = s.set_failures(name, next).await {
                                warn!(workload = %name, "failed to bump backoff: {e}");
                            }
                        }
                    }
                    // Reset counters for placed/unsuspended workloads (they
                    // succeeded — if they re-enter the store later they
                    // start fresh).
                    for name in diff.assign.keys().chain(diff.unsuspend.keys()) {
                        if let Err(e) = s.set_failures(name, 0).await {
                            warn!(workload = %name, "failed to reset backoff: {e}");
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    seq,
                    cycle_ms = cycle_start.elapsed().as_millis() as u64,
                    "solver call failed: {e}"
                );
                // Solver failures stall the whole cycle — every workload in
                // the request is delayed.  Surface them in Sentry so we
                // notice without grepping logs.  The error chain from
                // `anyhow` includes the Python traceback (see
                // `solver::call_solver`), which is what we actually want.
                let err: &(dyn std::error::Error + 'static) = e.as_ref();
                sentry::capture_error(err);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

mod extract;
use extract::extract_workload_metadata;

mod solver_request;
#[cfg(test)]
use solver_request::build_cluster_state;
use solver_request::build_solver_request_multi;

mod diff;
use diff::{ScheduleDiff, diff_schedule};

mod apply;
use apply::{ApplyContext, apply_assignments_multi};

mod bind;
use bind::bind_pending_pods;

/// Rebuild the [`SchedulerStateInner`] from the solver diff and request,
/// then publish it to the shared state.
async fn update_scheduler_state(
    state: &SchedulerState,
    diff: &ScheduleDiff,
    request: &SolverRequest,
) {
    let evicting: HashSet<String> = diff.suspend.iter().cloned().collect();

    // Running pods: those with a cluster assigned and running replicas.
    let mut running: HashMap<String, String> = HashMap::new();
    for (name, pod) in &request.pods {
        if let Some(ref cluster) = pod.cluster
            && pod
                .statuses_by_replica
                .iter()
                .any(|r| r.phase == Phase::Running)
        {
            running
                .entry(name.clone())
                .or_insert_with(|| cluster.clone());
        }
    }

    // Assigning: pods the solver just placed (new assignments + unsuspensions).
    let mut assigning: HashMap<String, String> = diff
        .assign
        .iter()
        .map(|(name, (cluster, _))| (name.clone(), cluster.clone()))
        .collect();

    // Suspended pods — now keyed by name → cluster.
    let mut suspended: HashMap<String, String> = request
        .pods
        .iter()
        .filter(|(_, pod)| {
            pod.statuses_by_replica
                .iter()
                .any(|r| r.phase == Phase::Suspended)
        })
        .map(|(name, pod)| (name.clone(), pod.cluster.clone().unwrap_or_default()))
        .collect();

    // Apply solver decisions: pods the solver just suspended move from
    // running → suspended; pods just unsuspended move from suspended →
    // assigning.  Without this, the status API would lag one cycle behind
    // the solver's decisions.
    for name in &diff.suspend {
        if let Some(cluster) = running.remove(name) {
            suspended.insert(name.clone(), cluster);
        }
    }
    for name in diff.unsuspend.keys() {
        if let Some(cluster) = suspended.remove(name) {
            assigning.insert(name.clone(), cluster);
        }
    }

    // Pod priorities.
    let job_priorities: HashMap<String, i32> = request
        .pods
        .iter()
        .map(|(name, pod)| (name.clone(), pod.priority))
        .collect();

    let max_queued_priority = diff
        .queue_order
        .iter()
        .filter_map(|name| job_priorities.get(name))
        .copied()
        .max()
        .unwrap_or(i32::MIN);

    let inner = SchedulerStateInner {
        queue_order: diff.queue_order.clone(),
        evicting,
        running,
        assigning,
        suspended,
        max_queued_priority,
        job_priorities,
    };

    *state.lock().await = inner;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job_store;
    use indexmap::IndexMap;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn pod(cluster: Option<&str>, statuses: Vec<(Phase, Option<&str>)>) -> SolverPod {
        SolverPod {
            chips_per_replica: 8,
            chip_type: "H100".into(),
            priority: 5,
            quota: "default".into(),
            cluster: cluster.map(String::from),
            statuses_by_replica: statuses
                .into_iter()
                .map(|(phase, node)| SolverReplicaStatus {
                    phase,
                    node: node.map(String::from),
                })
                .collect(),
        }
    }

    fn request_with(pods: Vec<(&str, SolverPod)>) -> SolverRequest {
        SolverRequest {
            clusters: vec![SolverCluster {
                name: "cluster-a".into(),
                nodes: vec![SolverNode {
                    name: "node-0".into(),
                    chip_type: "H100".into(),
                    chips: 8,
                }],
            }],
            pods: pods.into_iter().map(|(n, p)| (n.to_string(), p)).collect(),
            gang_sets: vec![],
            quotas: vec![],
            time_limit: 10.0,
        }
    }

    fn result_with(pods: Vec<(&str, SolverPod)>) -> ScheduleResult {
        ScheduleResult {
            pods: pods
                .into_iter()
                .map(|(n, p)| (n.to_string(), p))
                .collect::<IndexMap<_, _>>(),
            solver_status: "optimal".into(),
        }
    }

    // -----------------------------------------------------------------------
    // diff_schedule tests
    // -----------------------------------------------------------------------

    /// BUG #3: A queued pod (cluster=None, Phase::Running, node=None) that the
    /// solver marks as Suspended must NOT appear in `suspend` — there is
    /// nothing on any cluster to suspend. It should remain in queue_order.
    #[test]
    fn queued_pod_not_placed_is_not_suspended() {
        let req = request_with(vec![("queued", pod(None, vec![(Phase::Running, None)]))]);
        let res = result_with(vec![("queued", pod(None, vec![(Phase::Suspended, None)]))]);

        let diff = diff_schedule(&req, &res);

        assert!(
            !diff.suspend.contains(&"queued".to_string()),
            "queued pod with no cluster must not be suspended"
        );
    }

    /// BUG #4: A 4-replica pod where the solver places 3 replicas but leaves
    /// 1 unassigned must still produce an assignment for the 3 placed
    /// replicas. The `still_pending` check must not swallow the placement.
    #[test]
    fn partial_placement_produces_assignments() {
        let req = request_with(vec![(
            "multi",
            pod(
                None,
                vec![
                    (Phase::Running, None),
                    (Phase::Running, None),
                    (Phase::Running, None),
                    (Phase::Running, None),
                ],
            ),
        )]);
        let res = result_with(vec![(
            "multi",
            SolverPod {
                cluster: Some("cluster-a".into()),
                statuses_by_replica: vec![
                    SolverReplicaStatus {
                        phase: Phase::Running,
                        node: Some("node-0".into()),
                    },
                    SolverReplicaStatus {
                        phase: Phase::Running,
                        node: Some("node-0".into()),
                    },
                    SolverReplicaStatus {
                        phase: Phase::Running,
                        node: Some("node-1".into()),
                    },
                    SolverReplicaStatus {
                        phase: Phase::Running,
                        node: None,
                    },
                ],
                ..pod(Some("cluster-a"), vec![])
            },
        )]);

        let diff = diff_schedule(&req, &res);

        assert!(
            diff.assign.contains_key("multi"),
            "partially placed pod must appear in assign, not be silently dropped"
        );
        let (cluster, nodes) = &diff.assign["multi"];
        assert_eq!(cluster, "cluster-a");
        let total_placed: u32 = nodes.values().sum();
        assert_eq!(total_placed, 3, "3 replicas were placed");
    }

    /// Sanity: running pod on a node → solver suspends → must appear in suspend.
    #[test]
    fn running_to_suspended_produces_suspension() {
        let req = request_with(vec![(
            "victim",
            pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
        )]);
        let res = result_with(vec![(
            "victim",
            pod(Some("cluster-a"), vec![(Phase::Suspended, None)]),
        )]);

        let diff = diff_schedule(&req, &res);
        assert!(diff.suspend.contains(&"victim".to_string()));
    }

    /// Sanity: suspended pod → solver assigns node → must appear in unsuspend.
    #[test]
    fn suspended_to_placed_produces_unsuspension() {
        let req = request_with(vec![(
            "paused",
            pod(Some("cluster-a"), vec![(Phase::Suspended, None)]),
        )]);
        let res = result_with(vec![(
            "paused",
            pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
        )]);

        let diff = diff_schedule(&req, &res);
        assert!(diff.unsuspend.contains_key("paused"));
        assert!(
            !diff.assign.contains_key("paused"),
            "was suspended, not queued"
        );
    }

    /// Sanity: queued pod → solver assigns cluster + node → must appear in assign.
    #[test]
    fn queued_pod_placed_produces_assignment() {
        let req = request_with(vec![("new", pod(None, vec![(Phase::Running, None)]))]);
        let res = result_with(vec![(
            "new",
            pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
        )]);

        let diff = diff_schedule(&req, &res);
        assert!(diff.assign.contains_key("new"));
        assert_eq!(diff.assign["new"].0, "cluster-a");
    }

    /// Sanity: pod already running on node-0 → solver keeps it → no action.
    #[test]
    fn no_op_produces_empty_diff() {
        let req = request_with(vec![(
            "stable",
            pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
        )]);
        let res = result_with(vec![(
            "stable",
            pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
        )]);

        let diff = diff_schedule(&req, &res);
        assert!(diff.assign.is_empty());
        assert!(diff.suspend.is_empty());
        assert!(diff.unsuspend.is_empty());
        assert!(diff.queue_order.is_empty());
    }

    /// Invariant: no pod name appears in more than one action set.
    #[test]
    fn mutual_exclusivity() {
        // Build a scenario with multiple pods in different states.
        let req = request_with(vec![
            (
                "running",
                pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
            ),
            (
                "to_suspend",
                pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
            ),
            (
                "to_unsuspend",
                pod(Some("cluster-a"), vec![(Phase::Suspended, None)]),
            ),
            ("to_assign", pod(None, vec![(Phase::Running, None)])),
            ("still_queued", pod(None, vec![(Phase::Running, None)])),
        ]);
        let res = result_with(vec![
            (
                "running",
                pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
            ),
            (
                "to_suspend",
                pod(Some("cluster-a"), vec![(Phase::Suspended, None)]),
            ),
            (
                "to_unsuspend",
                pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
            ),
            (
                "to_assign",
                pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))]),
            ),
            ("still_queued", pod(None, vec![(Phase::Running, None)])),
        ]);

        let diff = diff_schedule(&req, &res);

        let all_names: Vec<&str> = diff
            .assign
            .keys()
            .map(|s| s.as_str())
            .chain(diff.suspend.iter().map(|s| s.as_str()))
            .chain(diff.unsuspend.keys().map(|s| s.as_str()))
            .chain(diff.queue_order.iter().map(|s| s.as_str()))
            .collect();
        let unique: std::collections::HashSet<&str> = all_names.iter().copied().collect();
        assert_eq!(
            all_names.len(),
            unique.len(),
            "pod appears in multiple action sets: {all_names:?}"
        );
    }

    // -----------------------------------------------------------------------
    // update_scheduler_state tests
    // -----------------------------------------------------------------------

    /// A pod just suspended by the solver must immediately appear as
    /// Suspended in the status API, not remain in Running.
    #[tokio::test]
    async fn scheduler_state_reflects_suspensions() {
        let state = job_store::new_scheduler_state();

        let req = request_with(vec![
            (
                "victim",
                SolverPod {
                    priority: 3,
                    ..pod(Some("cluster-a"), vec![(Phase::Running, Some("node-0"))])
                },
            ),
            (
                "newcomer",
                SolverPod {
                    priority: 10,
                    ..pod(None, vec![(Phase::Running, None)])
                },
            ),
        ]);

        let diff = ScheduleDiff {
            assign: HashMap::new(),
            suspend: vec!["victim".into()],
            unsuspend: HashMap::new(),
            queue_order: vec!["newcomer".into()],
        };

        update_scheduler_state(&state, &diff, &req).await;
        let inner = state.lock().await;

        // Victim must be moved from running to suspended immediately.
        assert!(
            !inner.running.contains_key("victim"),
            "just-suspended pod must not remain in running"
        );
        assert!(
            inner.suspended.contains_key("victim"),
            "just-suspended pod must appear in suspended"
        );

        // Check that the status API shows phase=Suspended.
        let statuses = inner.job_statuses();
        let victim_status = statuses.iter().find(|s| s.name == "victim").unwrap();
        assert_eq!(
            victim_status.phase,
            job_store::JobPhase::Suspended,
            "victim must show phase Suspended"
        );
    }

    /// A pod just unsuspended by the solver must immediately appear as
    /// Assigning, not remain in Suspended.
    #[tokio::test]
    async fn scheduler_state_reflects_unsuspensions() {
        let state = job_store::new_scheduler_state();

        let req = request_with(vec![(
            "resuming",
            SolverPod {
                priority: 5,
                ..pod(Some("cluster-a"), vec![(Phase::Suspended, None)])
            },
        )]);

        let mut unsuspend_nodes = HashMap::new();
        unsuspend_nodes.insert("node-0".to_string(), 1u32);
        let diff = ScheduleDiff {
            assign: HashMap::new(),
            suspend: vec![],
            unsuspend: HashMap::from([("resuming".into(), unsuspend_nodes)]),
            queue_order: vec![],
        };

        update_scheduler_state(&state, &diff, &req).await;
        let inner = state.lock().await;

        assert!(
            !inner.suspended.contains_key("resuming"),
            "just-unsuspended pod must not remain in suspended"
        );
        assert!(
            inner.assigning.contains_key("resuming"),
            "just-unsuspended pod must appear in assigning"
        );

        let statuses = inner.job_statuses();
        let status = statuses.iter().find(|s| s.name == "resuming").unwrap();
        assert_eq!(
            status.phase,
            job_store::JobPhase::Assigning,
            "resuming pod must show phase Assigning"
        );
    }

    // -----------------------------------------------------------------------
    // build_cluster_state — pending-ops gap tests
    // -----------------------------------------------------------------------
    //
    // Two races are covered:
    //
    // Gap 1 (suspend): spec.suspend=true is patched but pods are still
    //   terminating.  Without the fix the solver would see the nodes as free
    //   and could double-book them before the pods are actually gone.
    //
    // Gap 2 (placement): a job was just created/unsuspended but its pods
    //   have not yet appeared in the pod reflector.  Without the fix the
    //   solver sees the replicas as unplaced and re-assigns those nodes,
    //   corrupting capacity accounting for the cycle.

    use k8s_openapi::api::batch::v1::JobSpec;
    use k8s_openapi::api::core::v1::{PodSpec, PodStatus, PodTemplateSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

    fn make_job_store(jobs: Vec<K8sJob>) -> reflector::Store<K8sJob> {
        let mut writer: reflector::store::Writer<K8sJob> = reflector::store::Writer::default();
        for j in jobs {
            writer.apply_watcher_event(&watcher::Event::Apply(j));
        }
        writer.as_reader()
    }

    fn make_pod_store(pods: Vec<Pod>) -> reflector::Store<Pod> {
        let mut writer: reflector::store::Writer<Pod> = reflector::store::Writer::default();
        for p in pods {
            writer.apply_watcher_event(&watcher::Event::Apply(p));
        }
        writer.as_reader()
    }

    fn empty_node_store() -> reflector::Store<Node> {
        reflector::store::Writer::<Node>::default().as_reader()
    }

    /// A minimal K8sJob with the labels build_cluster_state expects, and
    /// the right schedulerName on the pod template (so build_cluster_state
    /// doesn't emit the bypass-warning during tests).
    fn test_job(uid: &str, suspend: bool, parallelism: i32, config: &BinderConfig) -> K8sJob {
        K8sJob {
            metadata: ObjectMeta {
                name: Some("k8s-job".to_string()),
                uid: Some(uid.to_string()),
                labels: Some(
                    [
                        (config.job_name_label.clone(), "wl-1".to_string()),
                        (
                            config.managed_by_label.clone(),
                            config.managed_by_value.clone(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            spec: Some(JobSpec {
                suspend: Some(suspend),
                parallelism: Some(parallelism),
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        scheduler_name: Some(config.scheduler_name.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A pod owned by `job_uid` with no node assignment (unbound / Pending).
    fn test_pod_unbound(uid: &str, job_uid: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(uid.to_string()),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "k8s-job".to_string(),
                    uid: job_uid.to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: None, // not yet bound
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Pending".to_string()),
                ..Default::default()
            }),
        }
    }

    /// A pod owned by `job_uid`, bound to `node`.
    fn test_pod(uid: &str, job_uid: &str, node: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(uid.to_string()),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "k8s-job".to_string(),
                    uid: job_uid.to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some(node.to_string()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                ..Default::default()
            }),
        }
    }

    /// Gap 1: spec.suspend=true is set, but the pod is still in the pod store
    /// (terminating). Nodes must remain occupied.
    #[test]
    fn suspended_job_with_terminating_pod_keeps_node_occupied() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", true, 1, &config);
        let pod = test_pod("pod-0", "uid-1", "node-042");

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![pod]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        let statuses = &pods["wl-1"].statuses_by_replica;
        assert_eq!(statuses.len(), 1);
        assert_eq!(
            statuses[0].phase,
            Phase::Running,
            "terminating pod must still appear Running so its node stays occupied"
        );
        assert_eq!(statuses[0].node.as_deref(), Some("node-042"));
    }

    /// Gap 1 (resolved): pod is fully gone, job should report Suspended and
    /// any stale pending-node entry must be cleaned up.
    #[test]
    fn suspended_job_with_no_pods_reports_suspended_and_clears_pending() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", true, 2, &config);

        let mut pending = [(
            "wl-1".to_string(),
            (
                [("node-042".to_string(), 1u32)]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                std::time::Instant::now(),
                None,
            ),
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![]), // no pods
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut pending,
        );

        assert!(
            pods["wl-1"]
                .statuses_by_replica
                .iter()
                .all(|s| s.phase == Phase::Suspended && s.node.is_none()),
            "fully terminated job must show Suspended/None"
        );
        assert!(
            !pending.contains_key("wl-1"),
            "stale pending entry must be removed once pods are confirmed gone"
        );
    }

    /// Gap 2: job was just placed/unsuspended, pods not yet in reflector.
    /// The pending-nodes map must be used to keep those nodes occupied.
    #[test]
    fn placement_shadow_used_before_pods_appear_in_reflector() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", false, 1, &config);

        let mut pending = [(
            "wl-1".to_string(),
            (
                [("node-042".to_string(), 1u32)]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                std::time::Instant::now(),
                None,
            ),
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![]), // pods not yet visible
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut pending,
        );

        assert_eq!(
            pods["wl-1"].statuses_by_replica[0].node.as_deref(),
            Some("node-042"),
            "pending node assignment must keep the node occupied before reflector catches up"
        );
        // Entry stays until pods are confirmed.
        assert!(pending.contains_key("wl-1"));
    }

    /// Gap 2 (resolved): pods have appeared in the reflector.
    /// The actual pod's node is used and the pending entry is cleared.
    #[test]
    fn placement_shadow_cleared_once_pods_confirmed_in_reflector() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", false, 1, &config);
        let pod = test_pod("pod-0", "uid-1", "node-042");

        // Stale pending entry pointing at the wrong node — must be overridden.
        let mut pending = [(
            "wl-1".to_string(),
            (
                [("node-stale".to_string(), 1u32)]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                std::time::Instant::now(),
                None,
            ),
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![pod]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut pending,
        );

        assert_eq!(
            pods["wl-1"].statuses_by_replica[0].node.as_deref(),
            Some("node-042"),
            "actual pod node must win over stale pending entry"
        );
        assert!(
            !pending.contains_key("wl-1"),
            "confirmed entry must be removed from pending map"
        );
    }

    /// Gap 1 (label-fallback isolation): a suspended job with no pods of its
    /// own must report Suspended even when another job's pods (carrying the
    /// same managed-by label) are present in the pod store.
    ///
    /// Before the fix, `pods_exist_for_job` matched any pod with the managed-by
    /// label regardless of job name — so job-A would stay "Running" as long as
    /// job-B's pods existed.
    #[test]
    fn suspended_job_not_confused_by_other_jobs_pods() {
        let config = BinderConfig::default();

        // job-A: suspended, uid="uid-A", no pods.
        let job_a = K8sJob {
            metadata: ObjectMeta {
                name: Some("k8s-job-a".to_string()),
                uid: Some("uid-A".to_string()),
                labels: Some(
                    [
                        (config.job_name_label.clone(), "wl-A".to_string()),
                        (
                            config.managed_by_label.clone(),
                            config.managed_by_value.clone(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            spec: Some(JobSpec {
                suspend: Some(true),
                parallelism: Some(1),
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        scheduler_name: Some(config.scheduler_name.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        // job-B: running, uid="uid-B", has a pod on node-099.
        let job_b = K8sJob {
            metadata: ObjectMeta {
                name: Some("k8s-job-b".to_string()),
                uid: Some("uid-B".to_string()),
                labels: Some(
                    [
                        (config.job_name_label.clone(), "wl-B".to_string()),
                        (
                            config.managed_by_label.clone(),
                            config.managed_by_value.clone(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            spec: Some(JobSpec {
                suspend: Some(false),
                parallelism: Some(1),
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        scheduler_name: Some(config.scheduler_name.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        // Pod belongs to job-B (uid-B, name "k8s-job-b").
        let pod_b = Pod {
            metadata: ObjectMeta {
                name: Some("pod-b-0".to_string()),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "k8s-job-b".to_string(),
                    uid: "uid-B".to_string(),
                    ..Default::default()
                }]),
                labels: Some(
                    [(
                        config.managed_by_label.clone(),
                        config.managed_by_value.clone(),
                    )]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some("node-099".to_string()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some("Running".to_string()),
                ..Default::default()
            }),
        };

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![pod_b]),
            &make_job_store(vec![job_a, job_b]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        // job-A must report Suspended — it has no pods of its own.
        assert!(
            pods["wl-A"]
                .statuses_by_replica
                .iter()
                .all(|s| s.phase == Phase::Suspended && s.node.is_none()),
            "suspended job-A must report Suspended even though job-B's pods are in the store"
        );

        // job-B must still report Running on node-099.
        assert_eq!(
            pods["wl-B"].statuses_by_replica[0].node.as_deref(),
            Some("node-099"),
            "job-B must still report its pod's node"
        );
    }

    /// Multi-replica Deployment: every replica's pod template carries the
    /// same `job-name` label (the ReplicaSet stamps it).  Each k8s Pod must
    /// still appear as its own SolverPod, keyed by k8s pod name — otherwise
    /// the solver under-counts demand and treats the "missing" replicas'
    /// nodes as free.
    #[test]
    fn deployment_replicas_each_become_their_own_solver_pod() {
        let config = BinderConfig::default();

        // Three managed standalone Pods (no Job owner_reference) all
        // sharing the same `job-name` label, simulating a Deployment
        // scaled to 3 replicas.
        let make_replica = |name: &str, node: Option<&str>| Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(
                    [
                        (config.job_name_label.clone(), "serve-flagship".to_string()),
                        (
                            config.managed_by_label.clone(),
                            config.managed_by_value.clone(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
                annotations: Some(
                    [
                        (config.priority_annotation.clone(), "80".to_string()),
                        (config.quota_annotation.clone(), "default".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: node.map(str::to_string),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(if node.is_some() { "Running" } else { "Pending" }.to_string()),
                ..Default::default()
            }),
        };

        let pods_in_store = vec![
            make_replica("serve-flagship-abc-aa1", Some("node-1")),
            make_replica("serve-flagship-abc-bb2", Some("node-2")),
            make_replica("serve-flagship-abc-cc3", None),
        ];

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(pods_in_store),
            &make_job_store(vec![]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        assert_eq!(
            pods.len(),
            3,
            "each Deployment replica must produce its own SolverPod (got {})",
            pods.len()
        );
        assert!(pods.contains_key("serve-flagship-abc-aa1"));
        assert!(pods.contains_key("serve-flagship-abc-bb2"));
        assert!(pods.contains_key("serve-flagship-abc-cc3"));

        // Bound replicas carry their node; the unbound one has cluster=None
        // so the solver's admission step picks a node.
        assert_eq!(
            pods["serve-flagship-abc-aa1"].statuses_by_replica[0]
                .node
                .as_deref(),
            Some("node-1")
        );
        assert_eq!(
            pods["serve-flagship-abc-bb2"].statuses_by_replica[0]
                .node
                .as_deref(),
            Some("node-2")
        );
        assert!(pods["serve-flagship-abc-cc3"].cluster.is_none());
    }

    /// Binding API — unbound pod does not clear pending entry.
    ///
    /// With the Binding API, pods are created without a node and bound via a
    /// separate API call.  Between Job creation and the binding call, the pod
    /// exists in the reflector but has no `spec.nodeName`.  The pending-node
    /// entry must stay active during this window so the solver keeps those
    /// nodes occupied.
    #[test]
    fn unbound_pending_pod_does_not_clear_pending_entry() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", false, 1, &config);
        let pod = test_pod_unbound("pod-0", "uid-1"); // owned by job, not yet bound

        let mut pending = [(
            "wl-1".to_string(),
            (
                [("node-042".to_string(), 1u32)]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                std::time::Instant::now(),
                None,
            ),
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![pod]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut pending,
        );

        // Pending entry must still be used to mark node-042 as occupied.
        assert_eq!(
            pods["wl-1"].statuses_by_replica[0].node.as_deref(),
            Some("node-042"),
            "pending node must stay occupied while pod awaits binding"
        );
        assert!(
            pending.contains_key("wl-1"),
            "pending entry must NOT be cleared by an unbound pod"
        );
    }

    /// Binding API — bound pod clears pending entry.
    ///
    /// Once the binding call succeeds the pod appears in the reflector with
    /// `spec.nodeName` set.  At that point `pods_exist_for_job` returns true
    /// and `build_cluster_state` must clear the pending entry and use the
    /// pod's actual node.
    #[test]
    fn bound_pod_clears_pending_entry() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", false, 1, &config);
        let pod = test_pod("pod-0", "uid-1", "node-042"); // fully bound

        let mut pending = [(
            "wl-1".to_string(),
            (
                [("node-stale".to_string(), 1u32)]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                std::time::Instant::now(),
                None,
            ),
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![pod]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut pending,
        );

        assert_eq!(
            pods["wl-1"].statuses_by_replica[0].node.as_deref(),
            Some("node-042"),
            "bound pod's actual node must be used"
        );
        assert!(
            !pending.contains_key("wl-1"),
            "pending entry must be cleared once pod is confirmed bound"
        );
    }

    // -----------------------------------------------------------------------
    // build_cluster_state — Job-finished filtering and orphan recovery
    // -----------------------------------------------------------------------

    use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use k8s_openapi::jiff::Timestamp;

    fn finished_job(uid: &str, condition_type: &str, config: &BinderConfig) -> K8sJob {
        let mut job = test_job(uid, false, 1, config);
        job.status = Some(JobStatus {
            completion_time: Some(Time(Timestamp::now())),
            conditions: Some(vec![JobCondition {
                type_: condition_type.to_string(),
                status: "True".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        });
        job
    }

    /// Helper that sets a unique k8s name and workload label on a test Job so
    /// multiple Jobs can co-exist in the reflector store (which keys by name).
    fn relabel(job: &mut K8sJob, k8s_name: &str, wl_name: &str, config: &BinderConfig) {
        job.metadata.name = Some(k8s_name.to_string());
        job.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(config.job_name_label.clone(), wl_name.to_string());
    }

    /// A finished Job must not appear in the solver request at all — otherwise
    /// every completed Job in etcd accumulates as zombie queued work, bloating
    /// the snapshot and the UI payload.
    #[test]
    fn finished_jobs_are_filtered_out() {
        let config = BinderConfig::default();
        let mut active = test_job("uid-active", false, 1, &config);
        relabel(&mut active, "k8s-active", "wl-active", &config);
        let mut completed = finished_job("uid-done", "Complete", &config);
        relabel(&mut completed, "k8s-done", "wl-done", &config);
        let mut failed = finished_job("uid-fail", "Failed", &config);
        relabel(&mut failed, "k8s-fail", "wl-fail", &config);

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![]),
            &make_job_store(vec![active, completed, failed]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        assert!(pods.contains_key("wl-active"), "active Job must remain");
        assert!(
            !pods.contains_key("wl-done"),
            "Complete Job must be filtered out"
        );
        assert!(
            !pods.contains_key("wl-fail"),
            "Failed Job must be filtered out"
        );
    }

    /// The Job controller often lags writing `completionTime` and the Complete
    /// condition: a Job can sit at `succeeded == completions` (or
    /// `failed > backoffLimit`) for many seconds with neither field populated,
    /// especially when pods are reaped before the controller observes them.
    /// `is_job_finished` must catch those via the count-based fallback —
    /// otherwise finished Jobs leak into the orphan-recovery branch.
    #[test]
    fn finished_via_count_fallback_is_filtered() {
        let config = BinderConfig::default();

        // Logically complete (succeeded == completions) but no completionTime
        // and no conditions yet.
        let mut succeeded = test_job("uid-s", false, 1, &config);
        relabel(&mut succeeded, "k8s-s", "wl-s", &config);
        succeeded.spec.as_mut().unwrap().completions = Some(1);
        succeeded.status = Some(JobStatus {
            succeeded: Some(1),
            ..Default::default()
        });

        // Failures past backoffLimit.
        let mut exhausted = test_job("uid-f", false, 1, &config);
        relabel(&mut exhausted, "k8s-f", "wl-f", &config);
        exhausted.spec.as_mut().unwrap().backoff_limit = Some(0);
        exhausted.status = Some(JobStatus {
            failed: Some(1),
            ..Default::default()
        });

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![]),
            &make_job_store(vec![succeeded, exhausted]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        assert!(
            !pods.contains_key("wl-s"),
            "Job with succeeded >= completions must be filtered even without completionTime"
        );
        assert!(
            !pods.contains_key("wl-f"),
            "Job with failed > backoffLimit must be filtered even without completionTime"
        );
    }

    /// A finished Job must also clear any stale pending-nodes entry — otherwise
    /// the entry would linger until the 30s TTL expires, and the gap-cycle
    /// injection in build_solver_request_multi would keep emitting a phantom
    /// running pod.
    #[test]
    fn finished_job_clears_pending_entry() {
        let config = BinderConfig::default();
        let job = finished_job("uid-1", "Complete", &config);

        let mut pending = [(
            "wl-1".to_string(),
            (
                [("node-042".to_string(), 1u32)]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                std::time::Instant::now(),
                None,
            ),
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut pending,
        );

        assert!(!pods.contains_key("wl-1"));
        assert!(
            !pending.contains_key("wl-1"),
            "stale pending entry for finished Job must be cleared"
        );
    }

    /// Orphan recovery: an active Job whose pods exist as Pending without any
    /// node assigned, and with no pending-nodes entry, must be re-emitted with
    /// cluster=None so the solver re-schedules it.  Without this, the pod is
    /// stuck forever (the bridge has lost its placement decision and the
    /// solver classifies the pod as passthrough).
    #[test]
    fn orphan_active_job_emitted_as_unscheduled() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", false, 1, &config);
        let pod = test_pod_unbound("pod-0", "uid-1");

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![pod]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        let solver_pod = &pods["wl-1"];
        assert!(
            solver_pod.cluster.is_none(),
            "orphan Job must be emitted with cluster=None so the solver re-schedules it; \
             got cluster={:?}",
            solver_pod.cluster
        );
        assert_eq!(solver_pod.statuses_by_replica.len(), 1);
        assert_eq!(solver_pod.statuses_by_replica[0].phase, Phase::Running);
        assert!(solver_pod.statuses_by_replica[0].node.is_none());
    }

    /// A normal active Job whose pods are bound to nodes must continue to be
    /// emitted with the cluster set — the orphan branch must not fire when
    /// pods are healthy.
    #[test]
    fn healthy_active_job_keeps_cluster_set() {
        let config = BinderConfig::default();
        let job = test_job("uid-1", false, 1, &config);
        let pod = test_pod("pod-0", "uid-1", "node-042");

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![pod]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        assert_eq!(
            pods["wl-1"].cluster.as_deref(),
            Some("cluster-a"),
            "active Job with bound pods must keep its cluster"
        );
    }

    // -----------------------------------------------------------------------
    // Backoff tests
    // -----------------------------------------------------------------------

    /// Backoff resets when cluster capacity changes. A workload that hits the
    /// backoff threshold is re-included after a reset.
    #[test]
    fn backoff_resets_on_capacity_change() {
        let mut wl = Workload {
            managed: ManagedObject::Pod(Box::default()),
            generation: 0,
            consecutive_failures: 0,
        };

        // Simulate 3 solver cycles where workload stays queued.
        for _ in 0..job_store::BACKOFF_THRESHOLD {
            wl.consecutive_failures = wl.consecutive_failures.saturating_add(1);
        }

        assert!(
            wl.consecutive_failures >= job_store::BACKOFF_THRESHOLD,
            "workload should be in backoff"
        );

        // Simulate capacity change: reset backoff counter (as the main loop
        // does when a workload disappears from cluster reflectors).
        wl.consecutive_failures = 0;

        assert!(
            wl.consecutive_failures < job_store::BACKOFF_THRESHOLD,
            "workload should no longer be in backoff after reset"
        );
    }

    /// End-to-end: a kubectl-applied managed Job with `spec.suspend=true` is
    /// observed by the Job reflector, surfaces in the SolverRequest as
    /// `Phase::Suspended` pinned to its origin cluster, and — when the
    /// solver places it — the diff routes it to `unsuspend` (patch existing
    /// Job) rather than `assign` (create on cluster).
    ///
    /// The distinction matters: `assign` would call `create_k8s_job`,
    /// which 409s because the Job already exists. `unsuspend` patches
    /// `spec.suspend=false` on the live object, which is what we want.
    /// This is the path users get when they `kubectl apply -f job.yaml`
    /// instead of POSTing to the bridge's HTTP API.
    #[test]
    fn kubectl_applied_suspended_job_routes_to_unsuspend_not_assign() {
        let config = BinderConfig::default();

        // What a user's manifest must contain: managed-by + job-name labels,
        // suspend=true. (Quota annotation is optional given default config
        // has no known-quota allowlist.)
        let job = test_job("uid-applied", true, 1, &config);

        // Read side: the reflector path produces the SolverRequest pod.
        let mut shadow = HashMap::new();
        let (cluster, request_pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut shadow,
        );

        let request_pod = request_pods
            .get("wl-1")
            .expect("kubectl-applied Job must surface in the SolverRequest");
        assert_eq!(
            request_pod.cluster.as_deref(),
            Some("cluster-a"),
            "Job must be pinned to the cluster it was applied to"
        );
        assert!(
            request_pod
                .statuses_by_replica
                .iter()
                .all(|s| s.phase == Phase::Suspended),
            "suspended Job must reach the solver as Phase::Suspended"
        );

        // Solver places the Job on node-0.
        let request = SolverRequest {
            clusters: vec![cluster],
            pods: request_pods,
            gang_sets: vec![],
            quotas: vec![],
            time_limit: 10.0,
        };
        let placed_pod = SolverPod {
            cluster: Some("cluster-a".into()),
            statuses_by_replica: vec![SolverReplicaStatus {
                phase: Phase::Running,
                node: Some("node-0".into()),
            }],
            ..request.pods["wl-1"].clone()
        };
        let result = result_with(vec![("wl-1", placed_pod)]);

        let diff = diff_schedule(&request, &result);

        assert!(
            diff.unsuspend.contains_key("wl-1"),
            "kubectl-applied Job must be routed to unsuspend (patch existing)"
        );
        assert!(
            !diff.assign.contains_key("wl-1"),
            "Job already lives on the cluster — must not be re-created via assign"
        );
    }

    /// A managed Job whose pod template has the *wrong* schedulerName means
    /// kube-scheduler is binding its pods, bypassing the bridge.  We can't
    /// fix that for the operator, but we must not silently swallow the Job
    /// either: it still has to surface in the SolverRequest so capacity
    /// accounting stays correct.  (The warning itself is logged via the
    /// `tracing` crate; we don't assert on log output.)
    #[test]
    fn job_with_wrong_scheduler_name_still_appears_in_solver_request() {
        let config = BinderConfig::default();
        let mut job = test_job("uid-bypass", true, 1, &config);
        // Strip the schedulerName the helper sets, so this Job looks
        // like one a user shipped without our schedulerName.
        if let Some(spec) = job.spec.as_mut()
            && let Some(pod_spec) = spec.template.spec.as_mut()
        {
            pod_spec.scheduler_name = None;
        }

        let (_cl, pods) = build_cluster_state(
            &empty_node_store(),
            &make_pod_store(vec![]),
            &make_job_store(vec![job]),
            "cluster-a",
            &config,
            &mut HashMap::new(),
        );

        assert!(
            pods.contains_key("wl-1"),
            "Job must still surface in the SolverRequest so its capacity \
             usage is accounted for, even when kube-scheduler is binding \
             its pods"
        );
    }
}
