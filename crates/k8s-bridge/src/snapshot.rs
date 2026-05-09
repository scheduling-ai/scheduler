//! Per-tick UI snapshot.
//!
//! After each solver cycle the binder builds a [`Frame`] describing the input
//! state the solver was handed plus the resulting solve stats. The HTTP API
//! serves the latest one at `GET /snapshot` for the UI to poll.
//!
//! The schema matches `loop_runner.write_snapshot` in
//! `py-scheduler/scheduler/loop_runner.py` so the Svelte UI's existing live
//! mode (`state.svelte.ts::bootstrapLive`) does not need to change.
//!
//! Summary fields mirror `compute_summary` there: `total_capacity` is the sum
//! of chips across all nodes, `used_capacity` is the sum occupied by fully
//! running pods, `running_jobs`/`queued_jobs` count pods by whether every
//! replica has a node assigned.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::solver_types::{ClusterState, Phase, Pod, Quota, SolverRequest};

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub job_count: usize,
    pub running_jobs: usize,
    pub queued_jobs: usize,
    pub total_capacity: u32,
    pub used_capacity: u32,
    pub utilization_percent: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Frame {
    pub seq: u64,
    pub timestamp: String,
    pub scheduler: String,
    pub tick: u64,
    /// Solver outcome string (e.g. "ok/optimal").  Absent in observe-only
    /// mode where no solver runs — the UI hides the badge when missing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub solver_status: Option<String>,
    /// Wall-clock solve duration.  Absent in observe-only mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub solver_duration_ms: Option<u64>,
    pub clusters: Vec<ClusterState>,
    pub pods: HashMap<String, Pod>,
    pub gang_sets: Vec<Vec<String>>,
    pub quotas: Vec<Quota>,
    pub summary: Summary,
    pub failed_nodes: Vec<String>,
    pub nodes: Vec<String>,
}

pub type SnapshotState = Arc<Mutex<Option<Frame>>>;

pub fn new_snapshot_state() -> SnapshotState {
    Arc::new(Mutex::new(None))
}

/// Build a Frame from the solver request (input pods + clusters + quotas) and
/// the solve outcome.  Pass `None` for `solver_status`/`solver_duration_ms`
/// in observe-only mode where no solver runs.
pub fn build_frame(
    seq: u64,
    scheduler: &str,
    request: &SolverRequest,
    solver_status: Option<&str>,
    solver_duration_ms: Option<u64>,
) -> Frame {
    let nodes: Vec<String> = request
        .clusters
        .iter()
        .flat_map(|c| c.nodes.iter().map(|n| n.name.clone()))
        .collect();

    let summary = compute_summary(&request.clusters, &request.pods);

    Frame {
        seq,
        timestamp: OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        scheduler: scheduler.to_string(),
        tick: seq,
        solver_status: solver_status.map(str::to_string),
        solver_duration_ms,
        clusters: request.clusters.clone(),
        pods: request.pods.clone(),
        gang_sets: request.gang_sets.clone(),
        quotas: request.quotas.clone(),
        summary,
        failed_nodes: Vec::new(),
        nodes,
    }
}

fn compute_summary(clusters: &[ClusterState], pods: &HashMap<String, Pod>) -> Summary {
    let total_capacity: u32 = clusters
        .iter()
        .flat_map(|c| c.nodes.iter())
        .map(|n| n.chips)
        .sum();

    let mut running = 0usize;
    let mut queued = 0usize;
    let mut used_capacity = 0u32;

    for pod in pods.values() {
        if pod_fully_running(pod) {
            running += 1;
            used_capacity += pod.chips_per_replica * (pod.statuses_by_replica.len() as u32);
        } else {
            queued += 1;
        }
    }

    let utilization_percent = if total_capacity > 0 {
        let raw = (used_capacity as f64) / (total_capacity as f64) * 100.0;
        (raw * 100.0).round() / 100.0
    } else {
        0.0
    };

    Summary {
        job_count: pods.len(),
        running_jobs: running,
        queued_jobs: queued,
        total_capacity,
        used_capacity,
        utilization_percent,
    }
}

fn pod_fully_running(pod: &Pod) -> bool {
    !pod.statuses_by_replica.is_empty()
        && pod
            .statuses_by_replica
            .iter()
            .all(|s| s.phase == Phase::Running && s.node.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solver_types::{Node, PodReplicaStatus};

    fn running_replica(node: &str) -> PodReplicaStatus {
        PodReplicaStatus {
            phase: Phase::Running,
            node: Some(node.into()),
        }
    }
    fn queued_replica() -> PodReplicaStatus {
        PodReplicaStatus {
            phase: Phase::Running,
            node: None,
        }
    }

    #[test]
    fn summary_counts_running_and_queued() {
        let clusters = vec![ClusterState {
            name: "c".into(),
            nodes: vec![
                Node {
                    name: "n1".into(),
                    chip_type: "H200".into(),
                    chips: 8,
                },
                Node {
                    name: "n2".into(),
                    chip_type: "H200".into(),
                    chips: 8,
                },
            ],
        }];
        let mut pods = HashMap::new();
        pods.insert(
            "run".into(),
            Pod {
                chips_per_replica: 8,
                chip_type: "H200".into(),
                priority: 1,
                quota: "q".into(),
                cluster: Some("c".into()),
                statuses_by_replica: vec![running_replica("n1")],
            },
        );
        pods.insert(
            "wait".into(),
            Pod {
                chips_per_replica: 8,
                chip_type: "H200".into(),
                priority: 1,
                quota: "q".into(),
                cluster: None,
                statuses_by_replica: vec![queued_replica()],
            },
        );

        let s = compute_summary(&clusters, &pods);
        assert_eq!(s.job_count, 2);
        assert_eq!(s.running_jobs, 1);
        assert_eq!(s.queued_jobs, 1);
        assert_eq!(s.total_capacity, 16);
        assert_eq!(s.used_capacity, 8);
        assert_eq!(s.utilization_percent, 50.0);
    }

    #[test]
    fn frame_json_has_ui_fields() {
        let req = SolverRequest {
            clusters: vec![],
            pods: HashMap::new(),
            gang_sets: vec![],
            quotas: vec![],
            time_limit: 30.0,
        };
        let frame = build_frame(42, "milp", &req, Some("optimal"), Some(123));
        let v: serde_json::Value = serde_json::to_value(&frame).unwrap();
        for key in [
            "seq",
            "timestamp",
            "scheduler",
            "tick",
            "solver_status",
            "solver_duration_ms",
            "clusters",
            "pods",
            "gang_sets",
            "quotas",
            "summary",
            "failed_nodes",
            "nodes",
        ] {
            assert!(v.get(key).is_some(), "missing field: {key}");
        }
        assert_eq!(v["seq"], 42);
        assert_eq!(v["scheduler"], "milp");
        assert_eq!(v["solver_status"], "optimal");
    }

    #[test]
    fn frame_omits_solver_fields_in_observe_mode() {
        let req = SolverRequest {
            clusters: vec![],
            pods: HashMap::new(),
            gang_sets: vec![],
            quotas: vec![],
            time_limit: 30.0,
        };
        let frame = build_frame(7, "observed", &req, None, None);
        let v: serde_json::Value = serde_json::to_value(&frame).unwrap();
        assert!(
            v.get("solver_status").is_none(),
            "solver_status should be omitted"
        );
        assert!(
            v.get("solver_duration_ms").is_none(),
            "solver_duration_ms should be omitted"
        );
        assert_eq!(v["scheduler"], "observed");
    }
}
