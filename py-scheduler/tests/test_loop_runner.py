from __future__ import annotations

import json
import random
from pathlib import Path
from tempfile import TemporaryDirectory

from scheduler.loop_runner import (
    GeneratorConfig,
    advance_runtimes,
    compute_summary,
    gang_sets_from,
    generate_cycle,
    project_clusters,
    read_config,
    write_snapshot,
)
from scheduler.model import ClusterState, Node, Phase, Pod, PodReplicaStatus, Quota, ScheduleResult


def _solve_all(clusters, pods, gang_sets, quotas, **kwargs):
    """Place every pod on the first node."""
    node_name = clusters[0].nodes[0].name
    cluster_name = clusters[0].name
    result = {}
    for name, pod in pods.items():
        result[name] = Pod(
            pod.chips_per_replica,
            pod.chip_type,
            pod.priority,
            pod.quota,
            cluster_name,
            [PodReplicaStatus(Phase.RUNNING, node_name) for _ in pod.statuses_by_replica],
        )
    return ScheduleResult(result, "ok")


CLUSTERS = [ClusterState("test", [Node("node-a", "H100", 8)])]
QUOTAS = [Quota("training", {"test": {"H100": 8}})]


def test_write_snapshot():
    with TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        pods = {"job-1": Pod(4, "H100", 50, "training", None, [PodReplicaStatus(Phase.RUNNING)])}

        write_snapshot(
            state_dir, "heuristic", 1, pods, "ok", 42, CLUSTERS, [], QUOTAS, 1, set(), ["node-a"]
        )
        latest = json.loads((state_dir / "latest-heuristic.json").read_text())
        assert latest["scheduler"] == "heuristic"
        assert latest["tick"] == 1
        assert latest["solver_status"] == "ok"
        assert latest["nodes"] == ["node-a"]


def test_advance_runtimes_and_completion():
    pods: dict[str, Pod] = {
        "job-1": Pod(4, "H100", 50, "training", None, [PodReplicaStatus(Phase.RUNNING)]),
    }
    runtimes = {"job-1": 10.0}
    gangs: dict[str, str] = {}

    solved = _solve_all(CLUSTERS, dict(pods), [], QUOTAS)
    advance_runtimes(pods, runtimes, gangs, solved.pods, 5.0)
    assert "job-1" in pods
    assert runtimes["job-1"] == 5.0

    advance_runtimes(pods, runtimes, gangs, solved.pods, 5.0)
    assert "job-1" not in pods
    assert "job-1" not in runtimes


def test_generate_cycle_produces_jobs():
    config = GeneratorConfig(seed=42, arrival_rate=5.0, burst_factor=1.0)
    rng = random.Random(config.seed)
    pods: dict[str, Pod] = {}
    runtimes: dict[str, float] = {}
    gangs: dict[str, str] = {}
    failed: set[str] = set()

    generate_cycle(rng, config, pods, runtimes, gangs, failed, ["node-a", "node-b"], dt=1.0)
    assert len(pods) > 0
    assert len(runtimes) == len(pods)


def test_generate_cycle_deterministic():
    config = GeneratorConfig(seed=11, arrival_rate=2.5, burst_factor=1.5)

    left_pods: dict[str, Pod] = {}
    right_pods: dict[str, Pod] = {}

    generate_cycle(random.Random(11), config, left_pods, {}, {}, set(), ["node-a"], dt=1.0)
    generate_cycle(random.Random(11), config, right_pods, {}, {}, set(), ["node-a"], dt=1.0)
    assert len(left_pods) == len(right_pods)


def test_chips_per_replica_respects_chip_type():
    """Each chip type's chips_per_replica must come from its own distribution
    so a single replica never exceeds the per-node chip count of that pool."""
    config = GeneratorConfig(
        seed=1,
        arrival_rate=200.0,
        burst_factor=1.0,
        chips_weights={
            "A100": {16: 1.0},  # only 16-chip
            "L40S": {1: 1.0},  # only 1-chip
        },
        chip_weights={"A100": 1.0, "L40S": 1.0},
    )
    pods: dict[str, Pod] = {}
    generate_cycle(random.Random(1), config, pods, {}, {}, set(), ["node-a"], dt=1.0)
    assert len(pods) > 50, "need a decent sample to exercise both chip types"
    for pod in pods.values():
        if pod.chip_type == "A100":
            assert pod.chips_per_replica == 16
        elif pod.chip_type == "L40S":
            assert pod.chips_per_replica == 1
        else:
            raise AssertionError(f"unexpected chip type {pod.chip_type}")


def test_chips_weights_missing_chip_type_falls_back_to_one():
    """If a chip type has no chips_weights entry, default to single-chip
    replicas — always feasible, easy to spot in the UI."""
    config = GeneratorConfig(
        seed=2,
        arrival_rate=50.0,
        burst_factor=1.0,
        chips_weights={"A100": {8: 1.0}},  # no entry for H100
        chip_weights={"H100": 1.0},  # only ever pick H100
    )
    pods: dict[str, Pod] = {}
    generate_cycle(random.Random(2), config, pods, {}, {}, set(), ["node-a"], dt=1.0)
    assert pods, "expected at least one job"
    for pod in pods.values():
        assert pod.chip_type == "H100"
        assert pod.chips_per_replica == 1


def test_generator_config_round_trips_through_json():
    """Persisted configs come back through JSON, which stringifies all keys —
    from_dict must restore the nested int keys so chips_weights stays usable."""
    original = GeneratorConfig()
    serialised = json.loads(json.dumps(original.to_dict()))
    restored = GeneratorConfig.from_dict(serialised)
    assert restored.chips_weights == original.chips_weights
    # And spot-check: A100 16-chip option survived.
    assert restored.chips_weights["A100"][16] == original.chips_weights["A100"][16]


def test_read_config_from_disk():
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "config.json"
        path.write_text(json.dumps({"running": False, "seed": 99}))
        config = read_config(path)
        assert config.running is False
        assert config.seed == 99


def test_read_config_missing():
    config = read_config(Path("/nonexistent/config.json"))
    assert config.running is True
    assert config.seed == 7


def test_project_clusters_removes_failed():
    clusters = [ClusterState("test", [Node("a", "H100", 8), Node("b", "H100", 8)])]
    projected = project_clusters(clusters, {"a"})
    assert [n.name for n in projected[0].nodes] == ["b"]


def test_gang_sets_from():
    pods = {"j1": Pod(4, "H100", 50, "q", None, []), "j2": Pod(4, "H100", 50, "q", None, [])}
    gangs = {"j1": "g1", "j2": "g1"}
    assert gang_sets_from(pods, gangs) == [["j1", "j2"]]


def test_compute_summary():
    clusters = [ClusterState("test", [Node("a", "H100", 8)])]
    solved = {"j1": Pod(4, "H100", 50, "q", "test", [PodReplicaStatus(Phase.RUNNING, "a")])}
    summary = compute_summary(clusters, solved, 1)
    assert summary["running_jobs"] == 1
    assert summary["used_capacity"] == 4
    assert summary["utilization_percent"] == 50.0


def test_empty_snapshot():
    with TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        write_snapshot(
            state_dir, "heuristic", 1, {}, "empty", 0, CLUSTERS, [], QUOTAS, 0, set(), ["node-a"]
        )
        latest = json.loads((state_dir / "latest-heuristic.json").read_text())
        assert latest["solver_status"] == "empty"
        assert latest["pods"] == {}


def test_snapshot_writes_input_pods_not_solved():
    """Snapshot must contain the solver INPUT (pre-solve pods), not the output."""
    with TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        # Input: pod is pending with no node assignment
        input_pods = {
            "job-1": Pod(4, "H100", 50, "training", None, [PodReplicaStatus(Phase.RUNNING)])
        }
        write_snapshot(
            state_dir,
            "heuristic",
            1,
            input_pods,
            "ok",
            100,
            CLUSTERS,
            [],
            QUOTAS,
            1,
            set(),
            ["node-a"],
        )
        latest = json.loads((state_dir / "latest-heuristic.json").read_text())
        pod_data = latest["pods"]["job-1"]
        # The snapshot should have the input state (no node), not solved state
        assert pod_data["statuses_by_replica"][0]["node"] is None
        # But solver stats should still be present
        assert latest["solver_status"] == "ok"
        assert latest["solver_duration_ms"] == 100


def test_solver_feedback_propagates_to_next_tick():
    """After solving, result pods are fed back into s.pods so the next tick sees them."""
    # Simulate two ticks of the loop
    pods: dict[str, Pod] = {
        "job-1": Pod(4, "H100", 50, "training", None, [PodReplicaStatus(Phase.RUNNING)])
    }
    runtimes = {"job-1": 30.0}
    gangs: dict[str, str] = {}

    # Tick 1: solver places job-1 on node-a
    result = _solve_all(CLUSTERS, dict(pods), [], QUOTAS)
    advance_runtimes(pods, runtimes, gangs, result.pods, 5.0)

    # Feed solver assignments back (as the loop runner does)
    for name, pod in result.pods.items():
        if name in pods:
            pods[name] = pod

    # After feedback, job-1 should have a node assignment
    assert pods["job-1"].statuses_by_replica[0].node == "node-a"
    assert pods["job-1"].statuses_by_replica[0].phase == Phase.RUNNING

    # Tick 2: add a new pod — it should be pending while job-1 is placed
    pods["job-2"] = Pod(2, "H100", 40, "training", None, [PodReplicaStatus(Phase.RUNNING)])
    assert pods["job-1"].statuses_by_replica[0].node == "node-a"  # still placed
    assert pods["job-2"].statuses_by_replica[0].node is None  # new, unplaced
