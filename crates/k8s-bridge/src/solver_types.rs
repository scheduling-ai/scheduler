//! Data types mirroring `py-scheduler/scheduler/model.py`.
//!
//! These structs are the JSON contract between the Rust k8s-bridge and the
//! Python solver. The Python `model.py` is the source of truth; these must
//! stay in sync (validated by the round-trip integration test).

use std::collections::HashMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub name: String,
    pub chip_type: String,
    pub chips: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Running,
    Failed,
    Suspended,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PodReplicaStatus {
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Pod {
    pub chips_per_replica: u32,
    pub chip_type: String,
    pub priority: i32,
    pub quota: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    pub statuses_by_replica: Vec<PodReplicaStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Quota {
    pub name: String,
    /// cluster_name -> {chip_type -> guaranteed chip count}
    pub guarantees: HashMap<String, HashMap<String, u32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterState {
    pub name: String,
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SolverRequest {
    pub clusters: Vec<ClusterState>,
    pub pods: HashMap<String, Pod>,
    #[serde(default)]
    pub gang_sets: Vec<Vec<String>>,
    pub quotas: Vec<Quota>,
    #[serde(default = "default_time_limit")]
    pub time_limit: f64,
}

fn default_time_limit() -> f64 {
    30.0
}

/// Solver output: updated pods reflecting all scheduling decisions.
///
/// Each pod carries the desired state after the solver runs — new node
/// assignments, phase changes, or no change. The map is ordered: pods
/// still waiting for resources appear last, in queue-priority order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleResult {
    pub pods: IndexMap<String, Pod>,
    pub solver_status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_solver_request_roundtrip() {
        let request = SolverRequest {
            clusters: vec![ClusterState {
                name: "us-east".into(),
                nodes: vec![Node {
                    name: "node-0".into(),
                    chip_type: "H200".into(),
                    chips: 8,
                }],
            }],
            pods: HashMap::from([
                (
                    "queued-pod".into(),
                    Pod {
                        chips_per_replica: 8,
                        chip_type: "H200".into(),
                        priority: 3,
                        quota: "training".into(),
                        cluster: None,
                        statuses_by_replica: vec![
                            PodReplicaStatus {
                                phase: Phase::Running,
                                node: None,
                            },
                            PodReplicaStatus {
                                phase: Phase::Running,
                                node: None,
                            },
                        ],
                    },
                ),
                (
                    "running-pod".into(),
                    Pod {
                        chips_per_replica: 8,
                        chip_type: "H200".into(),
                        priority: 2,
                        quota: "training".into(),
                        cluster: Some("us-east".into()),
                        statuses_by_replica: vec![PodReplicaStatus {
                            phase: Phase::Running,
                            node: Some("node-0".into()),
                        }],
                    },
                ),
                (
                    "suspended-pod".into(),
                    Pod {
                        chips_per_replica: 1,
                        chip_type: "H100".into(),
                        priority: 1,
                        quota: "inference".into(),
                        cluster: Some("us-east".into()),
                        statuses_by_replica: vec![
                            PodReplicaStatus {
                                phase: Phase::Suspended,
                                node: None,
                            };
                            4
                        ],
                    },
                ),
                (
                    "completed-pod".into(),
                    Pod {
                        chips_per_replica: 8,
                        chip_type: "H200".into(),
                        priority: 2,
                        quota: "training".into(),
                        cluster: Some("us-east".into()),
                        statuses_by_replica: vec![PodReplicaStatus {
                            phase: Phase::Completed,
                            node: Some("node-0".into()),
                        }],
                    },
                ),
            ]),
            gang_sets: vec![vec!["queued-pod".into()]],
            quotas: vec![Quota {
                name: "training".into(),
                guarantees: HashMap::from([(
                    "us-east".into(),
                    HashMap::from([("H200".into(), 100)]),
                )]),
            }],
            time_limit: 10.0,
        };

        let json = serde_json::to_string(&request).unwrap();
        let parsed: SolverRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request, parsed);
    }

    /// Cross-language roundtrip: Rust -> JSON -> Python deserialize+reserialize -> JSON -> Rust.
    /// Requires Python + the py-scheduler package to be importable via `uv run`.
    #[test]
    fn test_solver_request_cross_language_roundtrip() {
        let request = SolverRequest {
            clusters: vec![ClusterState {
                name: "us-east".into(),
                nodes: vec![Node {
                    name: "node-0".into(),
                    chip_type: "H200".into(),
                    chips: 8,
                }],
            }],
            pods: HashMap::from([(
                "queued-pod".into(),
                Pod {
                    chips_per_replica: 8,
                    chip_type: "H200".into(),
                    priority: 3,
                    quota: "training".into(),
                    cluster: None,
                    statuses_by_replica: vec![
                        PodReplicaStatus {
                            phase: Phase::Running,
                            node: None,
                        },
                        PodReplicaStatus {
                            phase: Phase::Running,
                            node: None,
                        },
                    ],
                },
            )]),
            gang_sets: vec![],
            quotas: vec![Quota {
                name: "training".into(),
                guarantees: HashMap::from([(
                    "us-east".into(),
                    HashMap::from([("H200".into(), 100)]),
                )]),
            }],
            time_limit: 10.0,
        };

        let json_in = serde_json::to_string(&request).unwrap();

        let python_code = concat!(
            "import sys, json; ",
            "from scheduler.model import solver_request_from_json; ",
            "from dataclasses import asdict; ",
            "req = solver_request_from_json(sys.stdin.read()); ",
            "json.dump(asdict(req), sys.stdout)",
        );

        let repo_root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
        let result = std::process::Command::new("uv")
            .args(["run", "python", "-c", python_code])
            .current_dir(repo_root)
            .env("PYTHONPATH", format!("{repo_root}/py-scheduler"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .as_mut()
                    .unwrap()
                    .write_all(json_in.as_bytes())
                    .unwrap();
                child.wait_with_output()
            });

        let output = match result {
            Ok(o) => o,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("skipping cross-language test: uv not found");
                return;
            }
            Err(e) => panic!("failed to run Python roundtrip: {e}"),
        };

        assert!(
            output.status.success(),
            "Python roundtrip failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let json_out = String::from_utf8(output.stdout).unwrap();
        let parsed: SolverRequest =
            serde_json::from_str(&json_out).expect("Rust failed to parse Python output");
        assert_eq!(request, parsed);
    }

    /// Cross-language roundtrip for ScheduleResult: Rust -> JSON -> Python -> JSON -> Rust.
    #[test]
    fn test_schedule_result_cross_language_roundtrip() {
        let result = ScheduleResult {
            pods: IndexMap::from([
                (
                    "pod-a".into(),
                    Pod {
                        chips_per_replica: 8,
                        chip_type: "H200".into(),
                        priority: 2,
                        quota: "training".into(),
                        cluster: Some("us-east".into()),
                        statuses_by_replica: vec![PodReplicaStatus {
                            phase: Phase::Running,
                            node: Some("node-0".into()),
                        }],
                    },
                ),
                (
                    "pod-b".into(),
                    Pod {
                        chips_per_replica: 1,
                        chip_type: "H100".into(),
                        priority: 1,
                        quota: "inference".into(),
                        cluster: None,
                        statuses_by_replica: vec![PodReplicaStatus {
                            phase: Phase::Running,
                            node: None,
                        }],
                    },
                ),
            ]),
            solver_status: "heuristic".into(),
        };

        let json_in = serde_json::to_string(&result).unwrap();

        let python_code = concat!(
            "import sys, json; ",
            "from scheduler.model import ScheduleResult, Pod, PodReplicaStatus, Phase; ",
            "from dataclasses import asdict; ",
            "d = json.loads(sys.stdin.read()); ",
            "pods = {k: Pod(",
            "chips_per_replica=v['chips_per_replica'], ",
            "chip_type=v['chip_type'], ",
            "priority=v['priority'], ",
            "quota=v['quota'], ",
            "cluster=v.get('cluster'), ",
            "statuses_by_replica=[PodReplicaStatus(Phase(r['phase']), r.get('node')) ",
            "for r in v['statuses_by_replica']]",
            ") for k, v in d['pods'].items()}; ",
            "r = ScheduleResult(pods=pods, solver_status=d['solver_status']); ",
            "json.dump(asdict(r), sys.stdout)",
        );

        let repo_root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
        let spawn_result = std::process::Command::new("uv")
            .args(["run", "python", "-c", python_code])
            .current_dir(repo_root)
            .env("PYTHONPATH", format!("{repo_root}/py-scheduler"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .as_mut()
                    .unwrap()
                    .write_all(json_in.as_bytes())
                    .unwrap();
                child.wait_with_output()
            });

        let output = match spawn_result {
            Ok(o) => o,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("skipping cross-language test: uv not found");
                return;
            }
            Err(e) => panic!("failed to run Python roundtrip: {e}"),
        };

        assert!(
            output.status.success(),
            "Python roundtrip failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let json_out = String::from_utf8(output.stdout).unwrap();
        let parsed: ScheduleResult =
            serde_json::from_str(&json_out).expect("Rust failed to parse Python output");
        assert_eq!(result, parsed);
    }

    #[test]
    fn test_schedule_result_roundtrip() {
        let result = ScheduleResult {
            pods: IndexMap::from([
                (
                    "pod-a".into(),
                    Pod {
                        chips_per_replica: 8,
                        chip_type: "H200".into(),
                        priority: 2,
                        quota: "training".into(),
                        cluster: Some("us-east".into()),
                        statuses_by_replica: vec![PodReplicaStatus {
                            phase: Phase::Running,
                            node: Some("node-0".into()),
                        }],
                    },
                ),
                (
                    "pod-b".into(),
                    Pod {
                        chips_per_replica: 1,
                        chip_type: "H100".into(),
                        priority: 1,
                        quota: "inference".into(),
                        cluster: None,
                        statuses_by_replica: vec![PodReplicaStatus {
                            phase: Phase::Running,
                            node: None,
                        }],
                    },
                ),
            ]),
            solver_status: "heuristic".into(),
        };

        let json = serde_json::to_string(&result).unwrap();
        let parsed: ScheduleResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, parsed);
    }
}
