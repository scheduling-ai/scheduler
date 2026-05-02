//! Workload store and scheduler state.
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
//! Persistence: the in-memory map is the working set; every mutation is
//! also written through to a [`Persistence`] backend so the store survives
//! a scheduler restart.  Production wires [`crate::persistence::PgStore`];
//! tests use [`MemoryPersistence`] which has no on-disk footprint.
//!
//! The [`SchedulerState`] is updated after each solver call so the API can
//! expose queue positions and eviction risk.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Workload abstraction
// ---------------------------------------------------------------------------

/// The underlying Kubernetes object managed by the scheduler.
///
/// Serialised as `{"Job": {...}}` or `{"Pod": {...}}` (default external
/// tagging) — no collision with the inner `kind` field on Job/Pod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManagedObject {
    Job(Box<Job>),
    Pod(Box<Pod>),
}

/// Lifecycle state of a workload in the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// excluded from the solver request until cluster state changes.
    pub consecutive_failures: u32,
}

/// Number of consecutive placement failures before a workload is
/// excluded from solver requests.
pub const BACKOFF_THRESHOLD: u32 = 3;

// ---------------------------------------------------------------------------
// Persistence backend
// ---------------------------------------------------------------------------

/// Durable store for workloads.  All mutations on [`WorkloadStore`] are
/// written through to one of these so the in-memory map can be rebuilt
/// after a scheduler restart.
#[async_trait]
pub trait Persistence: Send + Sync + 'static {
    /// Fetch every workload row.  Called once at startup to populate the
    /// in-memory map.  Order is unspecified.
    async fn load_all(&self) -> Result<Vec<(String, Workload)>>;

    /// Insert or update a workload.  Idempotent.
    async fn upsert(&self, name: &str, workload: &Workload) -> Result<()>;

    /// Remove a workload by name.  Removing a non-existent row is not
    /// an error.
    async fn remove(&self, name: &str) -> Result<()>;
}

/// In-memory [`Persistence`] for unit tests.  No durability — drops on
/// restart.  Production uses the Postgres backend.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct MemoryPersistence {
    inner: Mutex<HashMap<String, Workload>>,
}

#[cfg(test)]
#[async_trait]
impl Persistence for MemoryPersistence {
    async fn load_all(&self) -> Result<Vec<(String, Workload)>> {
        Ok(self
            .inner
            .lock()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn upsert(&self, name: &str, workload: &Workload) -> Result<()> {
        self.inner
            .lock()
            .await
            .insert(name.to_string(), workload.clone());
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.lock().await.remove(name);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Workload store
// ---------------------------------------------------------------------------

/// Shared, cloneable handle to the workload store.
///
/// Key: workload name (from `metadata.name` on the submitted manifest).
/// Value: the workload with its manifest and lifecycle state.
///
/// All mutations write through to a [`Persistence`] backend.  If the
/// persistence write fails, the in-memory map is *not* updated and the
/// caller sees the error — keeping memory and DB in sync at the cost of
/// surfacing transient DB outages to the API.
#[derive(Clone)]
pub struct WorkloadStore {
    inner: Arc<Mutex<HashMap<String, Workload>>>,
    persistence: Arc<dyn Persistence>,
}

impl WorkloadStore {
    /// Construct a store, loading any persisted rows into the in-memory map.
    pub async fn new(persistence: Arc<dyn Persistence>) -> Result<Self> {
        let rows = persistence
            .load_all()
            .await
            .context("loading workloads from persistence")?;
        let count = rows.len();
        let mut map = HashMap::with_capacity(count);
        for (name, workload) in rows {
            map.insert(name, workload);
        }
        if count > 0 {
            tracing::info!(count, "restored workloads from persistence");
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(map)),
            persistence,
        })
    }

    /// Snapshot of the entire store (clones every value).  Read-only;
    /// no DB I/O.  Used by the binder once per cycle.
    pub async fn snapshot(&self) -> HashMap<String, Workload> {
        self.inner.lock().await.clone()
    }

    pub async fn get(&self, name: &str) -> Option<Workload> {
        self.inner.lock().await.get(name).cloned()
    }

    #[cfg(test)]
    pub async fn contains(&self, name: &str) -> bool {
        self.inner.lock().await.contains_key(name)
    }

    pub async fn keys(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }

    /// Insert a brand-new workload.  Returns `true` if inserted, `false` if
    /// the name was already present (caller should treat as conflict).
    pub async fn insert_new(&self, name: String, workload: Workload) -> Result<bool> {
        let mut guard = self.inner.lock().await;
        if guard.contains_key(&name) {
            return Ok(false);
        }
        self.persistence.upsert(&name, &workload).await?;
        guard.insert(name, workload);
        Ok(true)
    }

    /// Insert or replace (used by re-entry on Pod suspension, where the
    /// caller has already constructed the new Workload state with an
    /// incremented generation).
    pub async fn upsert(&self, name: String, workload: Workload) -> Result<()> {
        let mut guard = self.inner.lock().await;
        self.persistence.upsert(&name, &workload).await?;
        guard.insert(name, workload);
        Ok(())
    }

    /// Remove unconditionally.  Returns `true` if a row was removed.
    pub async fn remove(&self, name: &str) -> Result<bool> {
        let mut guard = self.inner.lock().await;
        if !guard.contains_key(name) {
            return Ok(false);
        }
        self.persistence.remove(name).await?;
        guard.remove(name);
        Ok(true)
    }

    /// Remove only if the workload's current generation matches `expected`.
    /// Returns `true` if removed, `false` if generation mismatched.
    pub async fn remove_if_generation_matches(
        &self,
        name: &str,
        expected: u64,
    ) -> Result<RemoveOutcome> {
        let mut guard = self.inner.lock().await;
        let actual = match guard.get(name) {
            Some(w) => w.generation,
            None => return Ok(RemoveOutcome::NotPresent),
        };
        if actual != expected {
            return Ok(RemoveOutcome::GenerationMismatch { actual });
        }
        self.persistence.remove(name).await?;
        guard.remove(name);
        Ok(RemoveOutcome::Removed)
    }

    /// Set `consecutive_failures` for a single workload.  Used by the
    /// binder's per-cycle backoff bookkeeping.
    pub async fn set_failures(&self, name: &str, failures: u32) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let Some(wl) = guard.get_mut(name) else {
            return Ok(());
        };
        if wl.consecutive_failures == failures {
            return Ok(());
        }
        wl.consecutive_failures = failures;
        let snap = wl.clone();
        self.persistence.upsert(name, &snap).await?;
        Ok(())
    }

    /// Reset `consecutive_failures = 0` on every workload.  Called when
    /// cluster capacity frees up so previously-excluded workloads get
    /// another chance.
    pub async fn reset_all_failures(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let names: Vec<String> = guard
            .iter()
            .filter(|(_, w)| w.consecutive_failures != 0)
            .map(|(k, _)| k.clone())
            .collect();
        for name in &names {
            if let Some(wl) = guard.get_mut(name) {
                wl.consecutive_failures = 0;
                let snap = wl.clone();
                self.persistence.upsert(name, &snap).await?;
            }
        }
        Ok(())
    }
}

/// Outcome of [`WorkloadStore::remove_if_generation_matches`].
#[derive(Debug, Clone, Copy)]
pub enum RemoveOutcome {
    Removed,
    NotPresent,
    GenerationMismatch { actual: u64 },
}

/// Convenience for callers that don't care about durability: build a store
/// backed by [`MemoryPersistence`].  Used by tests.
#[cfg(test)]
pub async fn new_memory_store() -> WorkloadStore {
    WorkloadStore::new(Arc::new(MemoryPersistence::default()))
        .await
        .expect("memory persistence cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::batch::v1::Job;

    fn job(name: &str) -> Workload {
        let job = Job {
            metadata: kube::core::ObjectMeta {
                name: Some(name.into()),
                ..Default::default()
            },
            ..Default::default()
        };
        Workload {
            managed: ManagedObject::Job(Box::new(job)),
            state: WorkloadState::Queued,
            generation: 0,
            consecutive_failures: 0,
        }
    }

    #[tokio::test]
    async fn insert_then_remove_roundtrips_through_persistence() {
        let persistence = Arc::new(MemoryPersistence::default());
        let store = WorkloadStore::new(persistence.clone()).await.unwrap();

        assert!(store.insert_new("a".into(), job("a")).await.unwrap());
        assert!(!store.insert_new("a".into(), job("a")).await.unwrap()); // conflict
        assert_eq!(persistence.load_all().await.unwrap().len(), 1);

        assert!(store.remove("a").await.unwrap());
        assert_eq!(persistence.load_all().await.unwrap().len(), 0);
        assert!(!store.remove("a").await.unwrap());
    }

    #[tokio::test]
    async fn restart_repopulates_from_persistence() {
        let persistence: Arc<dyn Persistence> = Arc::new(MemoryPersistence::default());
        let store = WorkloadStore::new(persistence.clone()).await.unwrap();
        store
            .insert_new("queued".into(), job("queued"))
            .await
            .unwrap();
        drop(store);

        // Same persistence handle, fresh store: state must be loaded back.
        let store2 = WorkloadStore::new(persistence).await.unwrap();
        assert!(store2.contains("queued").await);
        assert_eq!(store2.keys().await.len(), 1);
    }

    #[tokio::test]
    async fn remove_if_generation_matches_respects_generation() {
        let store = new_memory_store().await;
        let mut wl = job("g");
        wl.generation = 7;
        store.insert_new("g".into(), wl).await.unwrap();

        match store.remove_if_generation_matches("g", 6).await.unwrap() {
            RemoveOutcome::GenerationMismatch { actual: 7 } => {}
            other => panic!("unexpected: {other:?}"),
        }
        assert!(store.contains("g").await);

        match store.remove_if_generation_matches("g", 7).await.unwrap() {
            RemoveOutcome::Removed => {}
            other => panic!("unexpected: {other:?}"),
        }
        assert!(!store.contains("g").await);
    }

    #[tokio::test]
    async fn set_failures_persists() {
        let persistence = Arc::new(MemoryPersistence::default());
        let store = WorkloadStore::new(persistence.clone()).await.unwrap();
        store.insert_new("f".into(), job("f")).await.unwrap();
        store.set_failures("f", 3).await.unwrap();
        let back = persistence.load_all().await.unwrap();
        assert_eq!(back[0].1.consecutive_failures, 3);

        store.reset_all_failures().await.unwrap();
        let back = persistence.load_all().await.unwrap();
        assert_eq!(back[0].1.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn workload_json_roundtrip() {
        let wl = job("x");
        let s = serde_json::to_string(&wl).unwrap();
        let back: Workload = serde_json::from_str(&s).unwrap();
        assert!(matches!(back.state, WorkloadState::Queued));
        assert!(matches!(back.managed, ManagedObject::Job(_)));
    }
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
