//! In-memory workload store and scheduler state.
//!
//! Workloads are submitted via the HTTP API as native Kubernetes manifests
//! (batch/v1 Job or v1 Pod). They are held in the central store until the
//! solver places them on a cluster.
//!
//! Design: follows Kueue's internal Workload abstraction — each submitted
//! manifest is wrapped in a [`Workload`] with a [`ManagedObject`] enum.
//! Unlike Kueue, workloads are NOT stored on any k8s cluster until first
//! placement, enabling multi-cluster optimisation, backpressure on the API
//! server, and early rejection without cluster-side effects. Once placed,
//! cluster assignment is sticky (no cross-cluster migration).
//!
//! The [`SchedulerState`] is updated after each solver call so the API can
//! expose queue positions and eviction risk.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Workload abstraction
// ---------------------------------------------------------------------------

/// The underlying Kubernetes object managed by the scheduler.
#[derive(Debug, Clone)]
pub enum ManagedObject {
    Job(Box<Job>),
    Pod(Box<Pod>),
}

/// Lifecycle state of a workload in the store.
#[derive(Debug, Clone)]
pub enum WorkloadState {
    /// Not yet placed on any cluster. Solver has full cluster freedom.
    Queued,
    /// Was running, now suspended. Pinned to this cluster.
    ///
    /// Jobs stay on the cluster as suspended k8s objects and are tracked via
    /// reflectors — they do NOT re-enter the store in this state.
    /// Pods are deleted from the cluster on suspension (following Kueue's
    /// approach) and re-enter the store in this state so the binder can
    /// recreate them on unsuspension.
    Suspended(String),
}

/// A workload: a Kubernetes manifest + its scheduling lifecycle state.
#[derive(Debug, Clone)]
pub struct Workload {
    pub managed: ManagedObject,
    pub state: WorkloadState,
    /// Monotonically increasing generation counter. Incremented on every
    /// mutation (state change, resubmission). The binder snapshots this
    /// value before calling the solver and checks it before removing the
    /// workload after placement — if the generation has changed, the
    /// removal is rejected and the workload is retried next cycle.
    pub generation: u64,
    /// Number of consecutive solver cycles where this workload was not
    /// placed. After [`BACKOFF_THRESHOLD`] failures the workload is
    /// excluded from the solver request until cluster state changes
    /// (tracked via [`WorkloadStore::reset_backoffs`]).
    pub consecutive_failures: u32,
}

/// Number of consecutive placement failures before a workload is
/// excluded from solver requests.
pub const BACKOFF_THRESHOLD: u32 = 3;

// ---------------------------------------------------------------------------
// Workload store
// ---------------------------------------------------------------------------

/// Shared, cloneable handle to the workload store.
///
/// Key: workload name (from `metadata.name` on the submitted manifest).
/// Value: the workload with its manifest and lifecycle state.
pub type WorkloadStore = Arc<Mutex<HashMap<String, Workload>>>;

pub fn new_store() -> WorkloadStore {
    Arc::new(Mutex::new(HashMap::new()))
}

// ---------------------------------------------------------------------------
// Scheduler state (updated by the binder after each solver call)
// ---------------------------------------------------------------------------

/// Eviction risk level for a running job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionRisk {
    /// Job is being actively evicted (in the solver's `suspend` list).
    Evicting,
    /// Job is running but has lower priority than a queued job — it may be
    /// preempted if resources are needed.
    AtRisk,
    /// No eviction signal from the solver.
    Safe,
}

/// Per-job status as seen by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatus {
    pub name: String,
    pub phase: JobPhase,
    /// 0-based queue position (only meaningful when `phase == Queued`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<usize>,
    /// Eviction risk (only meaningful when `phase == Running`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eviction_risk: Option<EvictionRisk>,
    /// Cluster the job is assigned/running on (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobPhase {
    Queued,
    Assigning,
    Running,
    Suspended,
}

/// Snapshot of the scheduler's view, rebuilt after each solver call.
#[derive(Debug, Clone, Default)]
pub struct SchedulerStateInner {
    /// Ordered queue — index is the queue position.
    pub queue_order: Vec<String>,
    /// Jobs being evicted right now.
    pub evicting: HashSet<String>,
    /// Jobs currently running, keyed by name → cluster.
    pub running: HashMap<String, String>,
    /// Jobs being assigned this cycle, keyed by name → cluster.
    pub assigning: HashMap<String, String>,
    /// Jobs suspended, keyed by name → cluster.
    pub suspended: HashMap<String, String>,
    /// Max priority among queued jobs (used to compute at-risk).
    pub max_queued_priority: i32,
    /// job_name → priority (for all known jobs).
    pub job_priorities: HashMap<String, i32>,
}

pub type SchedulerState = Arc<Mutex<SchedulerStateInner>>;

pub fn new_scheduler_state() -> SchedulerState {
    Arc::new(Mutex::new(SchedulerStateInner::default()))
}

impl SchedulerStateInner {
    /// Build a list of per-job statuses for the API.
    pub fn job_statuses(&self) -> Vec<JobStatus> {
        let mut out = Vec::new();

        // Queued jobs.
        for (pos, name) in self.queue_order.iter().enumerate() {
            out.push(JobStatus {
                name: name.clone(),
                phase: JobPhase::Queued,
                queue_position: Some(pos),
                eviction_risk: None,
                cluster: None,
            });
        }

        // Assigning jobs (placed this cycle, Job being created on cluster).
        for (name, cluster) in &self.assigning {
            out.push(JobStatus {
                name: name.clone(),
                phase: JobPhase::Assigning,
                queue_position: None,
                eviction_risk: None,
                cluster: Some(cluster.clone()),
            });
        }

        // Running jobs.
        for (name, cluster) in &self.running {
            let risk = if self.evicting.contains(name) {
                EvictionRisk::Evicting
            } else {
                let prio = self.job_priorities.get(name).copied().unwrap_or(0);
                if prio < self.max_queued_priority {
                    EvictionRisk::AtRisk
                } else {
                    EvictionRisk::Safe
                }
            };
            out.push(JobStatus {
                name: name.clone(),
                phase: JobPhase::Running,
                queue_position: None,
                eviction_risk: Some(risk),
                cluster: Some(cluster.clone()),
            });
        }

        // Suspended jobs.
        for (name, cluster) in &self.suspended {
            out.push(JobStatus {
                name: name.clone(),
                phase: JobPhase::Suspended,
                queue_position: None,
                eviction_risk: None,
                cluster: Some(cluster.clone()),
            });
        }

        out
    }
}
