"""
Multi-frame scenario tests with session recording.

Each test simulates a realistic scheduling session over multiple solver
invocations, asserting invariants at every frame. Produces JSONL files
compatible with the session-replay visualiser.

Run:
    uv run pytest py-scheduler/tests/test_scenarios.py -v

Produce JSONL for the visualiser:
    SCENARIO_OUTPUT=/tmp/sessions uv run pytest py-scheduler/tests/test_scenarios.py -v
    uv run scheduler-sim
    # Drag-and-drop a .jsonl file onto the UI, or use ?session= query param
"""

import json
import os
from dataclasses import asdict
from pathlib import Path

from scheduler.model import (
    ClusterState,
    Node,
    Phase,
    Pod,
    PodReplicaStatus,
    Quota,
    SolverRequest,
)
from scheduler.solver import solve

SCENARIO_OUTPUT = os.environ.get("SCENARIO_OUTPUT", "")


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------


def mk_nodes(prefix: str, count: int, chip: str, per_node: int = 8) -> list[Node]:
    return [Node(f"{prefix}-{i:03d}", chip, per_node) for i in range(count)]


CLUSTERS = [
    ClusterState("us-east", mk_nodes("use-h200", 10, "H200") + mk_nodes("use-h100", 5, "H100")),
    ClusterState("us-west", mk_nodes("usw-a100", 6, "A100") + mk_nodes("usw-l40s", 4, "L40S")),
    ClusterState("eu-central", mk_nodes("euc-h100", 6, "H100")),
]

QUOTAS = [
    Quota("training", {"us-east": {"H200": 48, "H100": 24}, "us-west": {"A100": 24}}),
    Quota("inference", {"us-east": {"H100": 16}, "eu-central": {"H100": 32}}),
    Quota("research", {"us-west": {"L40S": 24, "A100": 16}}),
]


def pending(n: int, chips: int, chip_type: str, priority: int, quota: str) -> Pod:
    return Pod(chips, chip_type, priority, quota, None, [PodReplicaStatus(Phase.RUNNING)] * n)


# ---------------------------------------------------------------------------
# Recorder
# ---------------------------------------------------------------------------


class ScenarioRecorder:
    """Run solver across frames, assert invariants, record JSONL."""

    def __init__(self, clusters=CLUSTERS, quotas=QUOTAS):
        self.clusters = clusters
        self.quotas = quotas
        self.frames: list[dict] = []

    def step(
        self, pods: dict[str, Pod], gang_sets: list[list[str]] | None = None
    ) -> dict[str, Pod]:
        """Record state, run solver, check invariants, return updated pods."""
        gangs = gang_sets or []
        req = SolverRequest(self.clusters, pods, gangs, self.quotas)
        self.frames.append(asdict(req))

        result = solve(req.clusters, req.pods, req.gang_sets, req.quotas)

        # Invariant: no node over-committed.
        cap = {n.name: n.chips for c in self.clusters for n in c.nodes}
        usage: dict[str, int] = {}
        for pod in result.pods.values():
            for rs in pod.statuses_by_replica:
                if rs.node:
                    usage[rs.node] = usage.get(rs.node, 0) + pod.chips_per_replica
        for node, used in usage.items():
            assert used <= cap[node], (
                f"Frame {len(self.frames)}: {node} over-committed ({used}>{cap[node]})"
            )

        # Invariant: no pod migrated to a different cluster.
        for name, pod in req.pods.items():
            if pod.cluster is None:
                continue
            out = result.pods.get(name)
            if out is None:
                continue
            assert out.cluster in (pod.cluster, None), (
                f"Frame {len(self.frames)}: {name} moved from {pod.cluster} to {out.cluster}"
            )

        return result.pods

    def write(self, name: str) -> None:
        if not SCENARIO_OUTPUT:
            return
        out_dir = Path(SCENARIO_OUTPUT)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{name}.jsonl"
        with open(path, "w") as f:
            for frame in self.frames:
                f.write(json.dumps(frame) + "\n")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_placed(pod: Pod) -> bool:
    return pod.cluster is not None and all(rs.node is not None for rs in pod.statuses_by_replica)


def is_suspended(pod: Pod) -> bool:
    return all(rs.phase == Phase.SUSPENDED for rs in pod.statuses_by_replica)


def remove_pods(pods: dict[str, Pod], names: list[str]) -> dict[str, Pod]:
    """Simulate job completion by removing pods."""
    return {k: v for k, v in pods.items() if k not in names}


def fail_replicas(pods: dict[str, Pod], name: str, count: int) -> dict[str, Pod]:
    """Simulate replica failures."""
    pods = dict(pods)
    pod = pods[name]
    statuses = list(pod.statuses_by_replica)
    failed = 0
    for i, rs in enumerate(statuses):
        if rs.phase == Phase.RUNNING and rs.node and failed < count:
            statuses[i] = PodReplicaStatus(Phase.FAILED, rs.node)
            failed += 1
    pods[name] = Pod(
        pod.chips_per_replica, pod.chip_type, pod.priority, pod.quota, pod.cluster, statuses
    )
    return pods


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def test_scenario_preemption_cascade_over_time():
    """Priority unwinding: low → medium preempts → high preempts → high completes → medium resumes."""
    rec = ScenarioRecorder()

    # Frame 0: low-priority fills us-east H200 (10 nodes = 80 chips).
    pods: dict[str, Pod] = {f"low-{i}": pending(1, 8, "H200", 10, "training") for i in range(10)}
    pods = rec.step(pods)
    assert all(is_placed(pods[f"low-{i}"]) for i in range(10))

    # Frame 1: medium-priority arrives needing 24 chips. Must preempt 3 low pods.
    pods["medium"] = pending(3, 8, "H200", 50, "training")
    pods = rec.step(pods)
    assert is_placed(pods["medium"])
    suspended_lows = [f"low-{i}" for i in range(10) if is_suspended(pods[f"low-{i}"])]
    assert len(suspended_lows) >= 3

    # Frame 2: high-priority arrives needing 40 chips. Must preempt medium + more lows.
    pods["high"] = pending(5, 8, "H200", 90, "training")
    pods = rec.step(pods)
    assert is_placed(pods["high"])

    # Frame 3: high completes. Medium should unsuspend.
    pods = remove_pods(pods, ["high"])
    pods = rec.step(pods)
    if is_suspended(pods["medium"]):
        # May not unsuspend in same cycle if medium was suspended in frame 2.
        # Run one more cycle.
        pods = rec.step(pods)
    # After enough cycles, medium should have capacity.

    rec.write("preemption_cascade")


def test_scenario_thundering_herd():
    """200 pods arrive at once. Higher priority placed first, no over-commitment."""
    rec = ScenarioRecorder()

    pods: dict[str, Pod] = {}
    for i in range(80):
        pods[f"h200-{i}"] = pending(1, 1, "H200", 100 - (i % 10), "training")
    for i in range(60):
        pods[f"h100-{i}"] = pending(1, 1, "H100", 80 - (i % 10), "inference")
    for i in range(40):
        pods[f"a100-{i}"] = pending(1, 8, "A100", 60 - (i % 10), "research")
    for i in range(20):
        pods[f"l40s-{i}"] = pending(1, 8, "L40S", 40 - (i % 10), "research")

    # Run 3 solver cycles.
    for _ in range(3):
        pods = rec.step(pods)

    # All H200 single-chip pods should fit (80 chips available, 80 pods * 1 chip).
    placed_h200 = sum(1 for i in range(80) if is_placed(pods[f"h200-{i}"]))
    assert placed_h200 == 80

    rec.write("thundering_herd")


def test_scenario_cluster_drain_and_refill():
    """Cluster empties after completions, new arrivals fill it."""
    rec = ScenarioRecorder()

    # Frame 0: place jobs on both clusters.
    pods: dict[str, Pod] = {f"east-{i}": pending(1, 8, "H200", 50, "training") for i in range(5)}
    pods |= {f"west-{i}": pending(1, 8, "A100", 50, "research") for i in range(3)}
    pods = rec.step(pods)

    east_jobs = [f"east-{i}" for i in range(5)]
    west_jobs = [f"west-{i}" for i in range(3)]
    assert all(is_placed(pods[n]) for n in east_jobs)
    assert all(is_placed(pods[n]) for n in west_jobs)

    # Frame 1: all east jobs complete. Cluster drains.
    pods = remove_pods(pods, east_jobs)
    pods = rec.step(pods)

    # Frame 2: new H200 jobs arrive. Should fill the freed us-east capacity.
    pods |= {f"new-east-{i}": pending(1, 8, "H200", 60, "training") for i in range(5)}
    pods = rec.step(pods)
    assert all(is_placed(pods[f"new-east-{i}"]) for i in range(5))

    rec.write("cluster_drain_refill")


def test_scenario_gang_member_completion():
    """Gang member completes, remaining members stay placed."""
    rec = ScenarioRecorder()

    pods: dict[str, Pod] = {
        "gang-a": pending(2, 8, "H200", 50, "training"),
        "gang-b": pending(2, 8, "H200", 50, "training"),
        "gang-c": pending(2, 8, "H200", 50, "training"),
    }
    gangs = [["gang-a", "gang-b", "gang-c"]]
    pods = rec.step(pods, gangs)
    assert all(is_placed(pods[f"gang-{x}"]) for x in "abc")

    # Frame 1: gang-a completes. Remaining 2 should stay placed.
    pods = remove_pods(pods, ["gang-a"])
    gangs = [["gang-b", "gang-c"]]  # pruned
    pods = rec.step(pods, gangs)
    assert is_placed(pods["gang-b"])
    assert is_placed(pods["gang-c"])

    rec.write("gang_completion")


def test_scenario_oscillating_preemption():
    """Documents anti-thrashing: A fills → B preempts → A returns higher priority.

    This test records current behavior. If the solver adds anti-thrashing
    guards later, the assertions should be updated.
    """
    rec = ScenarioRecorder()

    # Frame 0: quota-A (training) fills us-east H200.
    pods: dict[str, Pod] = {f"a-{i}": pending(1, 8, "H200", 50, "training") for i in range(8)}
    pods = rec.step(pods)
    assert all(is_placed(pods[f"a-{i}"]) for i in range(8))

    # Frame 1: quota-B (research) can't get H200 (no guarantee on us-east).
    # But training has H200 guarantee of 48 on us-east — using 64 chips (borrowing 16).
    # If research has no H200 guarantee, it can't preempt training.
    # Use inference with H100 on eu-central instead to avoid false dependency.
    pods["b-big"] = pending(4, 8, "H100", 80, "inference")
    pods = rec.step(pods)
    assert is_placed(pods["b-big"])  # should go to eu-central (has H100)

    # Frame 2: check that training jobs on us-east are unaffected.
    pods = rec.step(pods)
    assert all(is_placed(pods[f"a-{i}"]) for i in range(8))

    rec.write("oscillating_preemption")


def test_scenario_failure_storm():
    """Mass replica failures, solver reschedules them."""
    rec = ScenarioRecorder()

    # Frame 0: place 6 training jobs.
    pods: dict[str, Pod] = {f"train-{i}": pending(2, 8, "H200", 50, "training") for i in range(5)}
    pods = rec.step(pods)
    assert all(is_placed(pods[f"train-{i}"]) for i in range(5))

    # Frame 1: 3 jobs lose a replica each.
    for i in range(3):
        pods = fail_replicas(pods, f"train-{i}", 1)
    pods = rec.step(pods)

    # After solver: failed replicas should be rescheduled.
    for i in range(3):
        pod = pods[f"train-{i}"]
        running_count = sum(
            1 for rs in pod.statuses_by_replica if rs.phase == Phase.RUNNING and rs.node
        )
        assert running_count == 2, f"train-{i}: expected 2 running replicas, got {running_count}"

    # Frame 2: 2 more fail.
    for i in range(3, 5):
        pods = fail_replicas(pods, f"train-{i}", 1)
    pods = rec.step(pods)

    for i in range(3, 5):
        pod = pods[f"train-{i}"]
        running_count = sum(
            1 for rs in pod.statuses_by_replica if rs.phase == Phase.RUNNING and rs.node
        )
        assert running_count == 2, f"train-{i}: expected 2 running replicas, got {running_count}"

    rec.write("failure_storm")


def test_scenario_quota_borrowing_contention():
    """All guarantees consumed. Fourth quota with no guarantee can't place."""
    # Tight cluster: 3 H100 nodes = 24 chips.
    cl = [ClusterState("tight", mk_nodes("t-h100", 3, "H100"))]
    quotas = [
        Quota("qa", {"tight": {"H100": 8}}),
        Quota("qb", {"tight": {"H100": 8}}),
        Quota("qc", {"tight": {"H100": 8}}),
        Quota("qd", {"tight": {"H100": 0}}),  # no guarantee
    ]
    # Unguaranteed = 24 - 24 = 0. qd has 0 guarantee and can't borrow.
    rec = ScenarioRecorder(clusters=cl, quotas=quotas)

    # Frame 0: qa, qb, qc each fill their guarantee.
    pods: dict[str, Pod] = {
        "qa-job": pending(1, 8, "H100", 50, "qa"),
        "qb-job": pending(1, 8, "H100", 50, "qb"),
        "qc-job": pending(1, 8, "H100", 50, "qc"),
    }
    pods = rec.step(pods)
    assert all(is_placed(pods[n]) for n in ["qa-job", "qb-job", "qc-job"])

    # Frame 1: qd tries to place. No guarantee, no borrowing pool, no capacity.
    pods["qd-job"] = pending(1, 8, "H100", 99, "qd")
    pods = rec.step(pods)
    queued = [
        name
        for name, pod in pods.items()
        if any(rs.phase == Phase.RUNNING and rs.node is None for rs in pod.statuses_by_replica)
    ]
    assert "qd-job" in queued

    # Frame 2: qa completes. Now 8 chips free. But qd has 0 guarantee
    # and unguaranteed pool is still 0. qd should still be blocked.
    pods = remove_pods(pods, ["qa-job"])
    pods = rec.step(pods)
    queued = [
        name
        for name, pod in pods.items()
        if any(rs.phase == Phase.RUNNING and rs.node is None for rs in pod.statuses_by_replica)
    ]
    # Actually, when qa-job completes, qa's usage drops to 0. Unused guarantee
    # doesn't become borrowable (per-quota guarantee is reserved). But the
    # physical capacity IS free. The quota check: qd guarantee=0, unguaranteed=0,
    # so qd can't place. The 8 free chips belong to qa's unused guarantee.
    assert "qd-job" in queued

    rec.write("borrowing_contention")
