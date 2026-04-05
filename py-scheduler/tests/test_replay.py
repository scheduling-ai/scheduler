"""
Session replay tests.

Loads a recorded session (JSONL file produced by the Rust binder with
``--record <path>``) and replays every solver request through the Python
solver, checking feasibility on each frame.

Usage:
    SCHEDULER_SESSION=session.jsonl uv run pytest tests/test_replay.py
"""

import os

import pytest

from scheduler.model import SolverRequest, load_session

SESSION_PATH = os.environ.get("SCHEDULER_SESSION")


@pytest.fixture(scope="module")
def session() -> list[SolverRequest]:
    if not SESSION_PATH:
        pytest.skip("set SCHEDULER_SESSION to a recorded session file")
    return list(load_session(SESSION_PATH))


def assert_feasible(result, clusters):
    node_capacity = {n.name: n.chips for c in clusters for n in c.nodes}
    usage: dict[str, int] = {}
    for pod in result.pods.values():
        for rs in pod.statuses_by_replica:
            if rs.node:
                usage[rs.node] = usage.get(rs.node, 0) + pod.chips_per_replica
    for node, used in usage.items():
        capacity = node_capacity.get(node, 0)
        assert used <= capacity, f"{node}: used {used} > capacity {capacity}"


def test_replay_feasibility(session, solver_fn):
    for i, request in enumerate(session):
        result = solver_fn(
            request.clusters,
            request.pods,
            request.gang_sets,
            request.quotas,
            time_limit=request.time_limit,
        )
        assert_feasible(result, request.clusters)


def test_replay_cluster_binding(session, solver_fn):
    """Pods already bound to a cluster must not move to a different cluster."""
    for i, request in enumerate(session):
        result = solver_fn(
            request.clusters,
            request.pods,
            request.gang_sets,
            request.quotas,
            time_limit=request.time_limit,
        )
        for name, pod in request.pods.items():
            if pod.cluster is None:
                continue
            result_pod = result.pods.get(name)
            if result_pod is None:
                continue
            assert result_pod.cluster in (pod.cluster, None), (
                f"frame {i}: pod {name!r} moved from cluster {pod.cluster!r} "
                f"to {result_pod.cluster!r}"
            )


def test_replay_idempotent(session, solver_fn):
    """Running the solver twice on the same input produces the same result."""
    for i, request in enumerate(session):
        r1 = solver_fn(
            request.clusters,
            request.pods,
            request.gang_sets,
            request.quotas,
            time_limit=request.time_limit,
        )
        r2 = solver_fn(
            request.clusters,
            request.pods,
            request.gang_sets,
            request.quotas,
            time_limit=request.time_limit,
        )
        assert r1 == r2, f"frame {i}: solver is not idempotent"
