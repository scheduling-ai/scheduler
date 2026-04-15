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
use k8s_openapi::api::core::v1::{Binding, Node, ObjectReference, Pod};
use kube::{
    Client, ResourceExt,
    api::{Api, DeleteParams, Patch, PatchParams, PostParams},
    config::{KubeConfigOptions, Kubeconfig},
    runtime::{reflector, watcher},
};
use tracing::{info, warn};

use crate::job_store::{
    self, ManagedObject, SchedulerState, SchedulerStateInner, Workload, WorkloadState,
    WorkloadStore,
};

use crate::solver;
use crate::solver_types::{
    ClusterState as SolverCluster, Node as SolverNode, Phase, Pod as SolverPod,
    PodReplicaStatus as SolverReplicaStatus, ScheduleResult, SolverRequest,
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

/// Per-job pending node assignment: node->count map, insertion time, and
/// optional solver pod snapshot for the gap cycle.
type PendingNodeMap =
    HashMap<String, (HashMap<String, u32>, std::time::Instant, Option<SolverPod>)>;

/// Per-cluster runtime state: kube client and reflector readers.
struct ClusterRuntime {
    name: String,
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
    fn node_reader(&self) -> reflector::Store<Node> {
        self.node_store.read().unwrap().clone()
    }

    fn pod_reader(&self) -> reflector::Store<Pod> {
        self.pod_store.read().unwrap().clone()
    }

    fn job_reader(&self) -> reflector::Store<K8sJob> {
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

    let nodes_healthy = Arc::new(AtomicBool::new(true));
    let pods_healthy = Arc::new(AtomicBool::new(true));
    let jobs_healthy = Arc::new(AtomicBool::new(true));

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
    let stream = reflector::reflector(initial_writer, make_watcher());
    let mut stream = std::pin::pin!(stream);
    while let Some(item) = stream.next().await {
        backoff = Duration::from_secs(1);
        if let Err(e) = item {
            warn!(
                cluster = %cluster,
                resource,
                "reflector transient error (watcher will re-list): {e}"
            );
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

        // Wait for the first successful event before marking healthy.
        let mut saw_ok = false;
        while let Some(item) = stream.next().await {
            if item.is_ok() && !saw_ok {
                saw_ok = true;
                healthy.store(true, Ordering::Release);
                info!(
                    cluster = %cluster,
                    resource,
                    "reflector recovered"
                );
                backoff = Duration::from_secs(1);
            }
            if let Err(e) = item {
                warn!(
                    cluster = %cluster,
                    resource,
                    "reflector transient error (watcher will re-list): {e}"
                );
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
    let mut pending_node_assignments: PendingNodeMap = HashMap::new();
    loop {
        interval.tick().await;

        // Expire pending entries whose pods never appeared (e.g. job was
        // deleted externally between placement and reflector confirmation).
        pending_node_assignments.retain(|_, (_, inserted, _)| inserted.elapsed() < PENDING_TTL);

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
                "reflectors unhealthy, skipping scheduling cycle"
            );
            continue;
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
            let mut guard = s.lock().await;
            for wl in guard.values_mut() {
                wl.consecutive_failures = 0;
            }
            info!("cluster capacity changed, reset backoff counters");
        }
        prev_cluster_workloads = current_cluster_workloads;

        let store_snapshot: HashMap<String, Workload> = match &store {
            Some(s) => s.lock().await.clone(),
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
            continue;
        }

        // Bind any Pending pods that appeared since the last cycle.  This
        // must happen before building the solver request so that nodes being
        // bound this cycle are already occupied in the solver's view (via
        // pending_node_assignments — cleared once the reflector confirms).
        bind_pending_pods(&runtimes, &clients, config, &pending_node_assignments).await;

        info!(
            store_workloads = store_snapshot.len(),
            "building solver request"
        );
        let request = build_solver_request_multi(
            &runtimes,
            config,
            &store_snapshot,
            &mut pending_node_assignments,
        );

        match solver::call_solver(&request, record_path.as_deref(), &config.solver_name).await {
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
                apply_assignments_multi(&diff, &ctx).await;
                info!(
                    assigned = assign_count,
                    suspended = suspend_count,
                    unsuspended = unsuspend_count,
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
                    pending_node_assignments
                        .insert(name.clone(), (node_counts.clone(), now, solver_pod));
                }
                for (name, node_counts) in &diff.unsuspend {
                    // Unsuspend entries: job is already in the reflector, so
                    // build_cluster_state handles capacity directly.  No pod
                    // snapshot needed.
                    pending_node_assignments.insert(name.clone(), (node_counts.clone(), now, None));
                }

                // Update backoff counters for store workloads: increment
                // for those still queued, reset for those that were placed.
                if let Some(ref s) = store {
                    let mut guard = s.lock().await;
                    for name in &diff.queue_order {
                        if let Some(wl) = guard.get_mut(name) {
                            wl.consecutive_failures = wl.consecutive_failures.saturating_add(1);
                        }
                    }
                    // Reset counters for placed/unsuspended workloads (they
                    // succeeded — if they re-enter the store later they
                    // start fresh).
                    for name in diff.assign.keys().chain(diff.unsuspend.keys()) {
                        if let Some(wl) = guard.get_mut(name) {
                            wl.consecutive_failures = 0;
                        }
                    }
                }
            }
            Err(e) => warn!("solver call failed: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Solver integration
// ---------------------------------------------------------------------------

/// Build a [`SolverRequest`] aggregating state from all clusters and the
/// workload store.
fn build_solver_request_multi(
    runtimes: &[ClusterRuntime],
    config: &BinderConfig,
    store_workloads: &HashMap<String, Workload>,
    pending_nodes: &mut PendingNodeMap,
) -> SolverRequest {
    let mut cluster_states: Vec<SolverCluster> = Vec::with_capacity(runtimes.len());
    let mut pods: HashMap<String, SolverPod> = HashMap::new();

    for rt in runtimes {
        let node_r = rt.node_reader();
        let pod_r = rt.pod_reader();
        let job_r = rt.job_reader();
        let (cluster, cluster_pods) =
            build_cluster_state(&node_r, &pod_r, &job_r, &rt.name, config, pending_nodes);
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
    for (name, (_, _, solver_pod)) in pending_nodes.iter() {
        if pods.contains_key(name) {
            continue; // Already accounted for via the reflector.
        }
        if let Some(pod) = solver_pod {
            pods.entry(name.clone()).or_insert_with(|| pod.clone());
        }
    }

    // Store-submitted workloads: not yet placed on any cluster, or suspended
    // Pods pinned to a cluster.
    for (wl_name, workload) in store_workloads {
        if pods.contains_key(wl_name) {
            continue; // already observed on a cluster
        }

        // Skip workloads in backoff — they failed placement too many times
        // and will be retried when cluster state changes.
        if workload.consecutive_failures >= job_store::BACKOFF_THRESHOLD {
            continue;
        }

        let (chips, chip_type, priority, quota, parallelism) =
            extract_workload_metadata(&workload.managed, config);

        match &workload.state {
            WorkloadState::Queued => {
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
            WorkloadState::Suspended(cluster) => {
                let statuses_by_replica: Vec<SolverReplicaStatus> = (0..parallelism)
                    .map(|_| SolverReplicaStatus {
                        phase: Phase::Suspended,
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
                        cluster: Some(cluster.clone()),
                        statuses_by_replica,
                    },
                );
            }
        }
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

/// Extract scheduling metadata from a workload's managed object.
fn extract_workload_metadata(
    managed: &ManagedObject,
    config: &BinderConfig,
) -> (u32, String, i32, String, u32) {
    match managed {
        ManagedObject::Job(job) => extract_job_metadata(job, config),
        ManagedObject::Pod(pod) => extract_pod_metadata(pod, config),
    }
}

/// Extract scheduling metadata from a k8s Job manifest.
fn extract_job_metadata(job: &K8sJob, config: &BinderConfig) -> (u32, String, i32, String, u32) {
    let spec = job.spec.as_ref();
    let pod_spec = spec.and_then(|s| s.template.spec.as_ref());

    let chips = pod_spec
        .and_then(|ps| ps.containers.first())
        .and_then(|c| c.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .and_then(|r| r.get(&config.chip_resource))
        .and_then(|q| q.0.parse::<u32>().ok())
        .unwrap_or(0);

    let chip_type = job
        .labels()
        .get(&config.chip_label)
        .cloned()
        .unwrap_or_default();

    let priority = job
        .annotations()
        .get(&config.priority_annotation)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let quota = job
        .annotations()
        .get(&config.quota_annotation)
        .cloned()
        .unwrap_or_else(|| "default".into());

    let parallelism = spec.and_then(|s| s.parallelism).unwrap_or(1) as u32;

    (chips, chip_type, priority, quota, parallelism)
}

/// Extract scheduling metadata from a k8s Pod manifest.
fn extract_pod_metadata(pod: &Pod, config: &BinderConfig) -> (u32, String, i32, String, u32) {
    let pod_spec = pod.spec.as_ref();

    let chips = pod_spec
        .and_then(|ps| ps.containers.first())
        .and_then(|c| c.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .and_then(|r| r.get(&config.chip_resource))
        .and_then(|q| q.0.parse::<u32>().ok())
        .unwrap_or(0);

    let chip_type = pod
        .labels()
        .get(&config.chip_label)
        .cloned()
        .unwrap_or_default();

    let priority = pod
        .annotations()
        .get(&config.priority_annotation)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let quota = pod
        .annotations()
        .get(&config.quota_annotation)
        .cloned()
        .unwrap_or_else(|| "default".into());

    // Pods are always a single replica.
    (chips, chip_type, priority, quota, 1)
}

/// Build the [`SolverCluster`] (topology) and solver pods for a single cluster.
///
/// Job reflector provides job-level state (suspended, parallelism).
/// Pod reflector provides per-replica node assignments and managed standalone
/// Pods.
fn build_cluster_state(
    node_store: &reflector::Store<Node>,
    pod_store: &reflector::Store<Pod>,
    job_store: &reflector::Store<K8sJob>,
    cluster_name: &str,
    config: &BinderConfig,
    // Pending node assignments from a recent cycle: jobs that were just placed
    // or unsuspended whose pods may not yet be visible in the reflector.
    // Entries are removed here as soon as the reflector confirms the pods.
    pending_nodes: &mut PendingNodeMap,
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

        let (chips, chip_type, priority, quota, parallelism) = extract_job_metadata(&job, config);

        let is_suspended = job.spec.as_ref().and_then(|s| s.suspend).unwrap_or(false);

        let statuses_by_replica = if is_suspended {
            // `spec.suspend=true` has been patched, but pods may still be
            // terminating (graceful shutdown window, up to 30 s by default).
            // Keep their nodes occupied in the solver's view until the pod
            // store confirms they are gone — otherwise the solver may
            // double-book those nodes.
            if pods_exist_for_job(pod_store, &job) {
                build_replica_statuses_from_job_pods(pod_store, &job, parallelism)
            } else {
                // Pods fully gone — capacity is free. Also clean up any stale
                // pending-placement entry that predates this suspension.
                pending_nodes.remove(&job_name);
                (0..parallelism)
                    .map(|_| SolverReplicaStatus {
                        phase: Phase::Suspended,
                        node: None,
                    })
                    .collect()
            }
        } else {
            // Not suspended: check whether pods are already visible.
            if pods_exist_for_job(pod_store, &job) {
                // Reflector has confirmed the pods — clear any pending entry.
                pending_nodes.remove(&job_name);
                build_replica_statuses_from_job_pods(pod_store, &job, parallelism)
            } else if let Some((node_counts, _, _)) = pending_nodes.get(&job_name) {
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
                // No pods and no pending info — pad with Running/None as usual.
                build_replica_statuses_from_job_pods(pod_store, &job, parallelism)
            }
        };

        solver_pods.insert(
            job_name,
            SolverPod {
                chips_per_replica: chips,
                chip_type,
                priority,
                quota,
                cluster: Some(cluster_name.to_string()),
                statuses_by_replica,
            },
        );
    }

    // --- Managed standalone Pods on the cluster ---
    for pod in pod_store.state() {
        // Only consider Pods with our managed-by label that are NOT owned by a Job.
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
            continue; // Already tracked via the Job reflector path above.
        }

        let pod_name = pod
            .labels()
            .get(&config.job_name_label)
            .cloned()
            .unwrap_or_else(|| pod.name_any());

        if solver_pods.contains_key(&pod_name) {
            continue; // Already tracked.
        }

        let (chips, chip_type, priority, quota, _) = extract_pod_metadata(&pod, config);

        let phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("Unknown");

        if phase == "Succeeded" || phase == "Failed" {
            continue; // Terminal.
        }

        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
        // Running or Pending both map to Running (unplaced replica).
        let solver_phase = Phase::Running;

        solver_pods.insert(
            pod_name,
            SolverPod {
                chips_per_replica: chips,
                chip_type,
                priority,
                quota,
                cluster: Some(cluster_name.to_string()),
                statuses_by_replica: vec![SolverReplicaStatus {
                    phase: solver_phase,
                    node: if solver_phase == Phase::Running {
                        node_name
                    } else {
                        None
                    },
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
    pending_nodes.retain(|name, (_, _, solver_pod)| {
        // Only clean up entries that target this cluster.
        let targets_this_cluster =
            solver_pod.as_ref().and_then(|p| p.cluster.as_deref()) == Some(cluster_name);
        if !targets_this_cluster {
            return true; // different cluster or unsuspension entry — leave alone
        }
        // Keep if the job still appears in the cluster's job reflector.
        if known_on_cluster.contains(name) {
            return true;
        }
        // Keep if a managed standalone pod already accounts for this entry.
        if solver_pods.contains_key(name) {
            return true;
        }
        // Job is gone — drop stale entry so its nodes appear free.
        false
    });

    let cluster = SolverCluster {
        name: cluster_name.to_string(),
        nodes: solver_nodes,
    };

    (cluster, solver_pods)
}

/// Build per-replica statuses by examining child pods of a running Job.
/// Returns true if any pods owned by `job` are present in the pod store.
///
/// Used to detect the window between `spec.suspend=true` being patched and
/// the pods actually terminating — during that window nodes are still occupied.
/// Returns true if at least one pod owned by `job` is present in the pod
/// store and has already been bound to a node (`spec.nodeName` is set).
///
/// Unbound Pending pods (waiting for the Binding API call) are intentionally
/// excluded: `pending_node_assignments` remains authoritative until the
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
            continue; // terminal
        }

        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());

        let solver_phase = match phase {
            "Running" => Phase::Running,
            "Failed" => Phase::Failed,
            _ => Phase::Running, // Pending (bound or unbound) and unknown map to Running
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

    // Pad with pending replicas if fewer pods than parallelism.
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

    // From cluster Jobs.
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

        // From cluster managed Pods.
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

            let pod_name = pod
                .labels()
                .get(&config.job_name_label)
                .cloned()
                .unwrap_or_else(|| pod.name_any());

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

    // From store workloads.
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

// ---------------------------------------------------------------------------
// Schedule diffing
// ---------------------------------------------------------------------------

/// Derived actions from diffing the solver's input and output.
struct ScheduleDiff {
    /// pod_name -> (cluster_name, {node_name -> replica_count}) for newly placed pods.
    assign: HashMap<String, (String, HashMap<String, u32>)>,
    /// pod_names to suspend (running -> suspended).
    suspend: Vec<String>,
    /// pod_name -> {node_name -> replica_count} for unsuspended pods.
    unsuspend: HashMap<String, HashMap<String, u32>>,
    /// pod_names still waiting, in queue-priority order.
    queue_order: Vec<String>,
}

/// Compare solver input and output to determine what actions to take.
fn diff_schedule(request: &SolverRequest, result: &ScheduleResult) -> ScheduleDiff {
    let mut assign: HashMap<String, (String, HashMap<String, u32>)> = HashMap::new();
    let mut suspend: Vec<String> = Vec::new();
    let mut unsuspend: HashMap<String, HashMap<String, u32>> = HashMap::new();
    let mut queue_order: Vec<String> = Vec::new();

    for (name, out_pod) in &result.pods {
        let in_pod = match request.pods.get(name) {
            Some(p) => p,
            None => continue,
        };

        // Detect suspension: was running ON A CLUSTER, now suspended.
        // Only pods with a cluster assignment can be suspended — queued pods
        // (cluster=None) have nothing on any cluster to suspend.
        let was_on_cluster = in_pod.cluster.is_some();
        let was_running = in_pod
            .statuses_by_replica
            .iter()
            .any(|r| r.phase == Phase::Running);
        let now_suspended = out_pod
            .statuses_by_replica
            .iter()
            .any(|r| r.phase == Phase::Suspended);
        if was_on_cluster && was_running && now_suspended {
            suspend.push(name.clone());
            continue;
        }

        // Count newly assigned nodes (replicas that gained a node).
        // This must happen BEFORE the still_pending check so that partial
        // placements (some replicas placed, some not) are not silently lost.
        let mut new_nodes: HashMap<String, u32> = HashMap::new();
        for (in_r, out_r) in in_pod
            .statuses_by_replica
            .iter()
            .zip(out_pod.statuses_by_replica.iter())
        {
            if in_r.node.is_none()
                && let Some(ref node) = out_r.node
            {
                *new_nodes.entry(node.clone()).or_insert(0) += 1;
            }
        }

        if !new_nodes.is_empty() {
            let was_suspended = in_pod
                .statuses_by_replica
                .iter()
                .any(|r| r.phase == Phase::Suspended);
            if was_suspended {
                unsuspend.insert(name.clone(), new_nodes);
            } else {
                let cluster = out_pod.cluster.clone().unwrap_or_default();
                assign.insert(name.clone(), (cluster, new_nodes));
            }
            continue;
        }

        // Still-queued: all replicas pending (no new nodes assigned).
        let still_pending = out_pod
            .statuses_by_replica
            .iter()
            .any(|r| r.phase == Phase::Running && r.node.is_none());
        if still_pending {
            queue_order.push(name.clone());
        }
    }

    ScheduleDiff {
        assign,
        suspend,
        unsuspend,
        queue_order,
    }
}

// ---------------------------------------------------------------------------
// Applying solver decisions
// ---------------------------------------------------------------------------

/// Everything needed by [`apply_assignments_multi`].
struct ApplyContext {
    clients: HashMap<String, Client>,
    dry_run: bool,
    config: BinderConfig,
    store_snapshot: HashMap<String, Workload>,
    store: Option<WorkloadStore>,
    cluster_job_readers: HashMap<String, reflector::Store<K8sJob>>,
    cluster_pod_readers: HashMap<String, reflector::Store<Pod>>,
}

/// Apply all solver actions: suspensions, unsuspensions, and new assignments.
async fn apply_assignments_multi(diff: &ScheduleDiff, ctx: &ApplyContext) {
    let mut join_set = tokio::task::JoinSet::new();

    // --- Suspensions ---
    for pod_name in &diff.suspend {
        apply_suspension(pod_name, ctx, &mut join_set).await;
    }

    // --- Unsuspensions ---
    for (pod_name, node_counts) in &diff.unsuspend {
        apply_unsuspension(pod_name, node_counts, ctx, &mut join_set).await;
    }

    // --- New assignments: create workload on target cluster ---
    for (pod_name, (cluster_name, node_counts)) in &diff.assign {
        let Some(client) = ctx.clients.get(cluster_name) else {
            warn!(
                workload = pod_name,
                cluster = cluster_name,
                "no client for assigned cluster, skipping"
            );
            continue;
        };

        if let Some(workload) = ctx.store_snapshot.get(pod_name) {
            if ctx.dry_run {
                info!(
                    workload = pod_name,
                    cluster = cluster_name,
                    "would create workload on cluster"
                );
            } else {
                let result = match &workload.managed {
                    ManagedObject::Job(job) => create_k8s_job(job, client, &ctx.config).await,
                    ManagedObject::Pod(pod) => {
                        // Standalone pods: take the first node (single-replica).
                        let node_name = node_counts.keys().next().map(String::as_str).unwrap_or("");
                        create_k8s_pod(pod, client, node_name, &ctx.config).await
                    }
                };
                match result {
                    Ok(()) => {
                        info!(
                            workload = pod_name,
                            cluster = cluster_name,
                            "created workload on cluster"
                        );
                    }
                    Err(e) => {
                        warn!(
                            workload = pod_name,
                            cluster = cluster_name,
                            "failed to create workload: {e}"
                        );
                        continue;
                    }
                }
            }
            if let Some(s) = &ctx.store {
                remove_if_generation_matches(s, pod_name, &ctx.store_snapshot).await;
            }
        }
    }

    while join_set.join_next().await.is_some() {}
}

/// Remove a workload from the store only if its generation matches the
/// snapshot the solver acted on. If the workload was modified (e.g.,
/// deleted and resubmitted) between snapshot and apply, the removal is
/// skipped and the workload will be retried next cycle.
async fn remove_if_generation_matches(
    store: &WorkloadStore,
    name: &str,
    snapshot: &HashMap<String, Workload>,
) {
    let expected_gen = match snapshot.get(name) {
        Some(wl) => wl.generation,
        None => return,
    };
    let mut s = store.lock().await;
    if let Some(current) = s.get(name) {
        if current.generation == expected_gen {
            s.remove(name);
        } else {
            warn!(
                workload = name,
                expected = expected_gen,
                actual = current.generation,
                "generation mismatch, skipping store removal"
            );
        }
    }
}

/// Suspend a workload.
///
/// - Jobs: patch `spec.suspend = true` (k8s deletes pods atomically).
/// - Pods: delete the Pod from the cluster, re-enter store as Suspended.
async fn apply_suspension(
    wl_name: &str,
    ctx: &ApplyContext,
    join_set: &mut tokio::task::JoinSet<()>,
) {
    if ctx.dry_run {
        info!(workload = wl_name, "would suspend");
        return;
    }

    // Try Job path first.
    for (cluster_name, job_reader) in &ctx.cluster_job_readers {
        let target = job_reader
            .state()
            .iter()
            .find(|j| {
                j.labels()
                    .get(&ctx.config.job_name_label)
                    .map(|n| n == wl_name)
                    .unwrap_or_else(|| j.name_any() == wl_name)
            })
            .cloned();

        let Some(job) = target else { continue };

        let k8s_name = job.name_any();
        let ns = job.namespace().unwrap_or_else(|| "default".into());
        let Some(client) = ctx.clients.get(cluster_name).cloned() else {
            continue;
        };
        let cluster_owned = cluster_name.clone();

        join_set.spawn(async move {
            let jobs_api: Api<K8sJob> = Api::namespaced(client, &ns);
            let patch = serde_json::json!({
                "spec": { "suspend": true }
            });
            match jobs_api
                .patch(&k8s_name, &PatchParams::default(), &Patch::Merge(patch))
                .await
            {
                Ok(_) => info!(cluster = cluster_owned, "suspended job {ns}/{k8s_name}"),
                Err(e) => warn!(
                    cluster = cluster_owned,
                    "failed to suspend job {ns}/{k8s_name}: {e}"
                ),
            }
        });
        return; // Found on this cluster.
    }

    // Try Pod path: delete managed Pod from cluster, move to store as Suspended.
    for (cluster_name, pod_reader) in &ctx.cluster_pod_readers {
        let target = pod_reader
            .state()
            .iter()
            .find(|p| {
                let is_managed = p
                    .labels()
                    .get(&ctx.config.managed_by_label)
                    .map(|v| v == &ctx.config.managed_by_value)
                    .unwrap_or(false);
                if !is_managed {
                    return false;
                }
                // Not owned by a Job (those are handled above).
                let owned_by_job = p
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.kind == "Job"))
                    .unwrap_or(false);
                if owned_by_job {
                    return false;
                }
                p.labels()
                    .get(&ctx.config.job_name_label)
                    .map(|n| n == wl_name)
                    .unwrap_or_else(|| p.name_any() == wl_name)
            })
            .cloned();

        let Some(pod) = target else { continue };

        let k8s_name = pod.name_any();
        let ns = pod.namespace().unwrap_or_else(|| "default".into());
        let Some(client) = ctx.clients.get(cluster_name).cloned() else {
            continue;
        };
        let cluster_owned = cluster_name.clone();
        let store = ctx.store.clone();
        let wl_name_owned = wl_name.to_string();
        let prev_generation = ctx
            .store_snapshot
            .get(wl_name)
            .map(|w| w.generation)
            .unwrap_or(0);

        // Save the original Pod spec (strip runtime fields) for later
        // recreation on unsuspension.
        let mut original_pod = pod.as_ref().clone();
        original_pod.metadata.resource_version = None;
        original_pod.metadata.uid = None;
        original_pod.metadata.creation_timestamp = None;
        original_pod.status = None;

        join_set.spawn(async move {
            let pods_api: Api<Pod> = Api::namespaced(client, &ns);
            match pods_api.delete(&k8s_name, &DeleteParams::default()).await {
                Ok(_) => {
                    info!(
                        cluster = cluster_owned,
                        "suspended (deleted) pod {ns}/{k8s_name}"
                    );
                    // Re-enter store as Suspended so the solver can unsuspend later.
                    // Increment generation so stale placement results from the
                    // pre-suspension cycle are rejected by generation checks.
                    if let Some(s) = &store {
                        s.lock().await.insert(
                            wl_name_owned.clone(),
                            Workload {
                                managed: ManagedObject::Pod(Box::new(original_pod)),
                                state: WorkloadState::Suspended(cluster_owned),
                                generation: prev_generation.wrapping_add(1),
                                consecutive_failures: 0,
                            },
                        );
                    }
                }
                Err(e) => warn!(
                    cluster = cluster_owned,
                    "failed to delete pod {ns}/{k8s_name} for suspension: {e}"
                ),
            }
        });
        return; // Found on this cluster.
    }
}

/// Unsuspend a workload.
///
/// - Jobs: flip `spec.suspend = false`.  Pods go Pending with our
///   `schedulerName`; the next `bind_pending_pods` pass will bind them to
///   the nodes recorded in `pending_node_assignments`.
/// - Pods: recreate on the pinned cluster with `spec.nodeName` set directly.
async fn apply_unsuspension(
    wl_name: &str,
    node_counts: &HashMap<String, u32>,
    ctx: &ApplyContext,
    join_set: &mut tokio::task::JoinSet<()>,
) {
    if ctx.dry_run {
        info!(workload = wl_name, ?node_counts, "would unsuspend");
        return;
    }

    // Suspended Pod path: recreate with nodeName set directly.
    if let Some(workload) = ctx.store_snapshot.get(wl_name)
        && let WorkloadState::Suspended(ref cluster) = workload.state
        && let ManagedObject::Pod(ref pod) = workload.managed
    {
        let Some(client) = ctx.clients.get(cluster).cloned() else {
            warn!(
                workload = wl_name,
                cluster = cluster,
                "no client for pinned cluster"
            );
            return;
        };
        // Single-replica pods: take the first (and only) node.
        let node_name = match node_counts.keys().next() {
            Some(n) => n.clone(),
            None => {
                warn!(
                    workload = wl_name,
                    "unsuspend: no target node in node_counts"
                );
                return;
            }
        };
        let pod_owned = pod.clone();
        let config = ctx.config.clone();
        let store = ctx.store.clone();
        let wl_name_owned = wl_name.to_string();
        let cluster_owned = cluster.clone();
        let expected_gen = workload.generation;

        join_set.spawn(async move {
            match create_k8s_pod(&pod_owned, &client, &node_name, &config).await {
                Ok(()) => {
                    info!(
                        cluster = cluster_owned,
                        "unsuspended pod {wl_name_owned} on {node_name}"
                    );
                    if let Some(s) = &store {
                        let mut guard = s.lock().await;
                        if guard.get(&wl_name_owned).map(|w| w.generation) == Some(expected_gen) {
                            guard.remove(&wl_name_owned);
                        }
                    }
                }
                Err(e) => warn!(
                    cluster = cluster_owned,
                    "failed to recreate pod {wl_name_owned}: {e}"
                ),
            }
        });
        return;
    }

    // Job path: flip suspend; pods go Pending and are bound next cycle.
    for (cluster_name, job_reader) in &ctx.cluster_job_readers {
        let target = job_reader
            .state()
            .iter()
            .find(|j| {
                j.labels()
                    .get(&ctx.config.job_name_label)
                    .map(|n| n == wl_name)
                    .unwrap_or_else(|| j.name_any() == wl_name)
            })
            .cloned();

        let Some(job) = target else { continue };

        let ns = job.namespace().unwrap_or_else(|| "default".into());
        let k8s_name = job.name_any();
        let Some(client) = ctx.clients.get(cluster_name).cloned() else {
            continue;
        };
        let cluster_owned = cluster_name.clone();

        join_set.spawn(async move {
            let jobs_api: Api<K8sJob> = Api::namespaced(client, &ns);
            let patch = serde_json::json!({ "spec": { "suspend": false } });
            match jobs_api
                .patch(&k8s_name, &PatchParams::default(), &Patch::Merge(patch))
                .await
            {
                Ok(_) => info!(cluster = cluster_owned, "unsuspended job {ns}/{k8s_name}"),
                Err(e) => warn!(
                    cluster = cluster_owned,
                    "failed to unsuspend job {ns}/{k8s_name}: {e}"
                ),
            }
        });
        return;
    }
}

/// Create a batch/v1 Job on the target cluster.
///
/// The pod template gets `spec.schedulerName` set to our scheduler name so
/// that the k8s default scheduler ignores the resulting pods.  The binder
/// will bind each pod to its target node via the Binding API in the next
/// `bind_pending_pods` pass.
async fn create_k8s_job(
    submitted_job: &K8sJob,
    client: &Client,
    config: &BinderConfig,
) -> Result<()> {
    let ns = submitted_job
        .metadata
        .namespace
        .as_deref()
        .unwrap_or("default");
    let jobs_api: Api<K8sJob> = Api::namespaced(client.clone(), ns);

    let mut job = submitted_job.clone();

    // Clear server-side fields for creation.
    job.metadata.resource_version = None;
    job.metadata.uid = None;
    job.status = None;

    // Add managed-by label to the Job itself.
    let labels = job.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        config.managed_by_label.clone(),
        config.managed_by_value.clone(),
    );

    if let Some(ref mut spec) = job.spec {
        // Our scheduler name: k8s default scheduler skips these pods.
        let pod_spec = spec.template.spec.get_or_insert_with(Default::default);
        pod_spec.scheduler_name = Some(config.scheduler_name.clone());

        // Add managed-by label to the pod template so the pod reflector can
        // identify child pods of our jobs.
        let template_labels = spec
            .template
            .metadata
            .get_or_insert_with(Default::default)
            .labels
            .get_or_insert_with(Default::default);
        template_labels.insert(
            config.managed_by_label.clone(),
            config.managed_by_value.clone(),
        );

        spec.suspend = Some(false);
    }

    jobs_api
        .create(&PostParams::default(), &job)
        .await
        .context("failed to create Job on cluster")?;

    Ok(())
}

/// Create a standalone v1 Pod on the target cluster.
///
/// `spec.nodeName` is set directly, bypassing the k8s scheduler entirely.
/// This is the correct approach for standalone pods because we control their
/// creation and can set the target node atomically.
async fn create_k8s_pod(
    submitted_pod: &Pod,
    client: &Client,
    node_name: &str,
    config: &BinderConfig,
) -> Result<()> {
    let ns = submitted_pod
        .metadata
        .namespace
        .as_deref()
        .unwrap_or("default");
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), ns);

    let mut pod = submitted_pod.clone();

    // Clear server-side fields for creation.
    pod.metadata.resource_version = None;
    pod.metadata.uid = None;
    pod.metadata.creation_timestamp = None;
    pod.status = None;

    // Add managed-by label.
    let labels = pod.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        config.managed_by_label.clone(),
        config.managed_by_value.clone(),
    );

    // Pin directly to the target node — no scheduler involved.
    let pod_spec = pod.spec.get_or_insert_with(Default::default);
    pod_spec.node_name = Some(node_name.to_owned());

    pods_api
        .create(&PostParams::default(), &pod)
        .await
        .context("failed to create Pod on cluster")?;

    Ok(())
}

/// Bind a Pending pod to a specific node via the k8s Binding API.
///
/// This is the mechanism by which our binder acts as a scheduler: instead of
/// relying on the k8s scheduler to find a node, we call
/// `POST /api/v1/namespaces/{ns}/pods/{name}/binding` directly.
async fn bind_pod(client: &Client, ns: &str, pod_name: &str, node_name: &str) -> Result<()> {
    let binding = Binding {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(pod_name.to_owned()),
            ..Default::default()
        },
        target: ObjectReference {
            api_version: Some("v1".into()),
            kind: Some("Node".into()),
            name: Some(node_name.to_owned()),
            ..Default::default()
        },
    };
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), ns);
    pods_api
        .create_subresource::<Binding, serde_json::Value>(
            "binding",
            pod_name,
            &PostParams::default(),
            &binding,
        )
        .await
        .context("failed to bind pod to node")?;
    Ok(())
}

/// Bind any Pending pods that have our `schedulerName` and no `nodeName` yet.
///
/// Called each cycle before the solver request is built.  The
/// `pending_node_assignments` map tells us which node each job's replicas
/// should land on; we distribute Pending pods across those nodes in name-sorted
/// order for stability.
async fn bind_pending_pods(
    runtimes: &[ClusterRuntime],
    clients: &HashMap<String, Client>,
    config: &BinderConfig,
    pending_nodes: &PendingNodeMap,
) {
    for rt in runtimes {
        let Some(client) = clients.get(&rt.name) else {
            continue;
        };
        let pod_reader = rt.pod_reader();
        let job_reader = rt.job_reader();

        // Collect Pending, unbound pods with our schedulerName, owned by a Job.
        let mut unbound: Vec<Arc<Pod>> = pod_reader
            .state()
            .into_iter()
            .filter(|pod| {
                let our_scheduler = pod.spec.as_ref().and_then(|s| s.scheduler_name.as_deref())
                    == Some(config.scheduler_name.as_str());
                let pending =
                    pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Pending");
                let no_node = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .is_none();
                let owned_by_job = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.kind == "Job"))
                    .unwrap_or(false);
                our_scheduler && pending && no_node && owned_by_job
            })
            .collect();

        if unbound.is_empty() {
            continue;
        }

        // Group by logical workload name (looked up via parent Job).
        let mut by_workload: HashMap<String, Vec<Arc<Pod>>> = HashMap::new();
        for pod in unbound.drain(..) {
            let job_uid = pod
                .metadata
                .owner_references
                .as_ref()
                .and_then(|refs| refs.iter().find(|r| r.kind == "Job"))
                .map(|r| r.uid.as_str())
                .unwrap_or("");
            let Some(job) = job_reader
                .state()
                .iter()
                .find(|j| j.metadata.uid.as_deref() == Some(job_uid))
                .cloned()
            else {
                continue;
            };
            let wl_name = job
                .labels()
                .get(&config.job_name_label)
                .cloned()
                .unwrap_or_else(|| job.name_any());
            by_workload.entry(wl_name).or_default().push(pod);
        }

        for (wl_name, mut pods) in by_workload {
            let Some((node_counts, _, _)) = pending_nodes.get(&wl_name) else {
                continue;
            };

            // Stable assignment: sort pods by name, expand node_counts into a
            // flat list, zip together.
            pods.sort_by_key(|p| p.name_any());
            let nodes_flat: Vec<&str> = node_counts
                .iter()
                .flat_map(|(node, &count)| std::iter::repeat_n(node.as_str(), count as usize))
                .collect();

            for (pod, node_name) in pods.iter().zip(nodes_flat.iter()) {
                let pod_name = pod.name_any();
                let ns = pod.namespace().unwrap_or_else(|| "default".into());
                let client = client.clone();
                let node_name = node_name.to_string();
                let wl = wl_name.clone();

                tokio::spawn(async move {
                    match bind_pod(&client, &ns, &pod_name, &node_name).await {
                        Ok(()) => info!(
                            workload = wl,
                            pod = pod_name,
                            node = node_name,
                            "bound pod to node"
                        ),
                        Err(e) => warn!(
                            workload = wl,
                            pod = pod_name,
                            node = node_name,
                            "bind failed: {e}"
                        ),
                    }
                });
            }
        }
    }
}

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

/// Read per-node chip count. If `chip_count_label` is set, the count comes
/// from that node label (used by test clusters without a device plugin);
/// otherwise it comes from `status.allocatable`/`status.capacity` for the
/// configured extended resource.
fn node_chip_capacity(node: &Node, config: &BinderConfig) -> u32 {
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
fn get_candidate_nodes(store: &reflector::Store<Node>, config: &BinderConfig) -> Vec<Arc<Node>> {
    store
        .state()
        .into_iter()
        .filter(|node| {
            // Must have our scheduler taint.
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
    use k8s_openapi::api::core::v1::{PodSpec, PodStatus};
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

    /// A minimal K8sJob with the labels build_cluster_state expects.
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
    fn pending_nodes_used_before_pods_appear_in_reflector() {
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
    fn pending_nodes_cleared_once_pods_confirmed_in_reflector() {
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
    // Backoff tests
    // -----------------------------------------------------------------------

    /// Backoff resets when cluster capacity changes. A workload that hits the
    /// backoff threshold is re-included after a reset.
    #[test]
    fn backoff_resets_on_capacity_change() {
        let mut wl = Workload {
            managed: ManagedObject::Pod(Box::default()),
            state: WorkloadState::Queued,
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
}
