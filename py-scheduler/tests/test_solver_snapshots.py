"""
Snapshot tests for the MILP solver on fixed single-frame inputs.

Re-run with ``UPDATE_SNAPSHOTS=1`` to regenerate snapshot files after a
deliberate scoring/encoding change. Snapshots live under
``py-scheduler/tests/__snapshots__/test_solver_snapshots/``.

Mock solver is excluded — these are encoding/scoring regression tests
for the MILP path specifically.

Notes on coverage relative to the original snapshot tests (deleted in
the MILP port, see ``scheduler-private-old`` commit ``63460e7``):

* ``test_large_mixed_workload`` is intentionally not ported. Its value
  came from a 10-chip-type, 5-cluster fixture (TPUv5, P100, V100, T4,
  A10G, L4, ...). Reviving it means reviving that fixture; the
  multi-frame ``test_scenarios.py`` cases already cover heterogeneous
  workloads. Add it back if/when the wider fixture earns its keep.

* ``test_queue_order_under_h100_pressure`` is weaker than the original.
  The old fixture left enough free room on a single cluster for both
  the small *and* the big queued jobs, so the test exercised the
  same-priority tiebreaker (solver prefers more pods placed → smalls
  win, bigs queue). On this tighter fixture the solver is willing to
  suspend same-priority incumbents to fit a big, so the test sidesteps
  by making the bigs too large for any single cluster's H100 inventory
  — they queue because they cannot fit, not because of a tiebreaker
  decision. Catches encoding regressions; does not catch tiebreaker
  changes. A dedicated test on a fixture sized to expose the
  tiebreaker would be the way to restore that coverage.
"""

from __future__ import annotations

from dataclasses import asdict

from scheduler.milp_solver import solve
from scheduler.model import ClusterState, Node, Phase, Pod, PodReplicaStatus, Quota


# ---------------------------------------------------------------------------
# Fixed infrastructure
# ---------------------------------------------------------------------------

# 3 clusters, ~600 GPUs total. Sized so MILP solves comfortably under 1s
# while still leaving room for the workloads each test exercises.
#
# us-east:    40 H200 + 15 H100              =  320 + 120 =  440
# us-west:    15 H100 + 8  L40S              =  120 +  64 =  184
# eu-central: 8  H100 + 8  A100              =   64 +  64 =  128
# total: 752 GPUs across 94 nodes.


def _nodes(prefix: str, count: int, chip: str, per_node: int = 8) -> list[Node]:
    return [Node(f"{prefix}-{i:04d}", chip, per_node) for i in range(count)]


CLUSTERS: list[ClusterState] = [
    ClusterState(
        "us-east",
        _nodes("use-h200", 40, "H200") + _nodes("use-h100", 15, "H100"),
    ),
    ClusterState(
        "us-west",
        _nodes("usw-h100", 15, "H100") + _nodes("usw-l40s", 8, "L40S"),
    ),
    ClusterState(
        "eu-central",
        _nodes("euc-h100", 8, "H100") + _nodes("euc-a100", 8, "A100"),
    ),
]

QUOTAS: list[Quota] = [
    Quota(
        "training-large",
        {"us-east": {"H200": 240}},
    ),
    Quota(
        "platform-ci",
        {"us-east": {"H200": 40, "H100": 40}},
    ),
    Quota(
        "research-vision",
        {
            "us-east": {"H100": 80},
            "us-west": {"H100": 80},
            "eu-central": {"H100": 32, "A100": 32},
        },
    ),
    Quota(
        "research-rl",
        {"us-west": {"H100": 40}, "eu-central": {"H100": 32}},
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pending(replicas: int, chips: int, chip_type: str, priority: int, quota: str) -> Pod:
    return Pod(
        chips,
        chip_type,
        priority,
        quota,
        None,
        [PodReplicaStatus(Phase.RUNNING)] * replicas,
    )


def _running(
    cluster: str, nodes: list[str], chips: int, chip_type: str, priority: int, quota: str
) -> Pod:
    return Pod(
        chips,
        chip_type,
        priority,
        quota,
        cluster,
        [PodReplicaStatus(Phase.RUNNING, node) for node in nodes],
    )


def _suspended(
    cluster: str, replicas: int, chips: int, chip_type: str, priority: int, quota: str
) -> Pod:
    return Pod(
        chips,
        chip_type,
        priority,
        quota,
        cluster,
        [PodReplicaStatus(Phase.SUSPENDED)] * replicas,
    )


def _is_pending(pod: Pod) -> bool:
    return pod.cluster is None and any(
        rs.node is None and rs.phase == Phase.RUNNING for rs in pod.statuses_by_replica
    )


def _is_placed(pod: Pod) -> bool:
    return pod.cluster is not None and all(
        rs.phase == Phase.RUNNING and rs.node is not None for rs in pod.statuses_by_replica
    )


def _is_suspended(pod: Pod) -> bool:
    return all(rs.phase == Phase.SUSPENDED for rs in pod.statuses_by_replica)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_unsuspend_resumes_on_bound_cluster(json_snapshot):
    """Suspended pod resumes on the cluster it was bound to, not migrated."""
    pods = {
        "urgent-research": _suspended("us-west", 8, 8, "H100", 5, "research-vision"),
    }

    result = solve(CLUSTERS, pods, [], QUOTAS)

    out = result.pods["urgent-research"]
    assert _is_placed(out)
    assert out.cluster == "us-west"
    assert all(
        rs.node is not None and rs.node.startswith("usw-h100") for rs in out.statuses_by_replica
    )

    json_snapshot.assert_match(asdict(result))


def test_gang_contention_with_small_jobs(json_snapshot):
    """Larger challenger preempts incumbent; small high-priority jobs slip through."""
    pods = {
        "big-train": _running(
            "us-east",
            [f"use-h200-{i:04d}" for i in range(30)],
            8,
            "H200",
            2,
            "training-large",
        ),
        "challenger": _pending(35, 8, "H200", 3, "training-large"),
    }
    for i in range(20):
        pods[f"small-{i}"] = _pending(1, 1, "H200", 5, "platform-ci")

    result = solve(CLUSTERS, pods, [], QUOTAS)

    assert _is_suspended(result.pods["big-train"])
    assert _is_placed(result.pods["challenger"])
    placed_smalls = [
        n for n in result.pods if n.startswith("small-") and _is_placed(result.pods[n])
    ]
    assert len(placed_smalls) == 20

    json_snapshot.assert_match(asdict(result))


def test_queue_order_under_h100_pressure(json_snapshot):
    """Small and mid jobs slot into free capacity; bigs that exceed any single
    cluster's H100 inventory must queue."""
    pods = {
        "east-busy": _running(
            "us-east",
            [f"use-h100-{i:04d}" for i in range(12)],
            8,
            "H100",
            5,
            "research-vision",
        ),
        "west-busy": _running(
            "us-west",
            [f"usw-h100-{i:04d}" for i in range(12)],
            8,
            "H100",
            5,
            "research-vision",
        ),
        "eu-busy": _running(
            "eu-central",
            [f"euc-h100-{i:04d}" for i in range(5)],
            8,
            "H100",
            5,
            "research-vision",
        ),
        "p5-small": _pending(1, 8, "H100", 5, "platform-ci"),
        # p5-big / p4-big each need 128 chips on a single cluster; no cluster has
        # that much H100 (us-east/us-west = 120, eu-central = 64), so they must
        # queue regardless of preemption decisions.
        "p5-big": _pending(16, 8, "H100", 5, "research-vision"),
        "p4-small": _pending(1, 8, "H100", 4, "research-vision"),
        "p4-big": _pending(16, 8, "H100", 4, "research-vision"),
        "p3-mid": _pending(3, 8, "H100", 3, "research-rl"),
    }

    result = solve(CLUSTERS, pods, [], QUOTAS)

    placed = {n for n in result.pods if _is_placed(result.pods[n])}
    pending_ = {n for n in result.pods if _is_pending(result.pods[n])}

    assert {"p5-small", "p4-small", "p3-mid"} <= placed
    assert {"p5-big", "p4-big"} <= pending_

    json_snapshot.assert_match(asdict(result))
