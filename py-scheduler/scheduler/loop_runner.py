"""Loop runner: tick-based solver loop with fake workload generation.

Reads generator config from ``config.json`` on the shared state directory,
runs the solver each tick, and writes ``latest-{solver}.json``.  No HTTP
server, no event queue — just a loop.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter, sleep

import sentry_sdk

from scheduler.generator import GeneratorConfig, generate_cycle, read_config
from scheduler.model import (
    ClusterState,
    Node,
    Phase,
    Pod,
    Quota,
    ScheduleResult,
    SolverRequest,
)
from scheduler.solvers import SOLVERS

__all__ = [
    "GeneratorConfig",
    "generate_cycle",
    "read_config",
]

log = logging.getLogger(__name__)

STATE_DIR = Path(os.environ.get("LOOP_RUNNER_STATE_DIR", "/data/live-state"))


# ---------------------------------------------------------------------------
# Default cluster / quota fixtures
# ---------------------------------------------------------------------------


def _mk_nodes(prefix: str, count: int, chip: str, per_node: int = 8) -> list[Node]:
    return [Node(f"{prefix}-{i:03d}", chip, per_node) for i in range(count)]


DEFAULT_CLUSTERS = [
    ClusterState(
        "us-east",
        _mk_nodes("use-h200", 10, "H200") + _mk_nodes("use-h100", 5, "H100"),
    ),
    ClusterState(
        "us-west",
        _mk_nodes("usw-a100", 6, "A100") + _mk_nodes("usw-l40s", 4, "L40S"),
    ),
    ClusterState("eu-central", _mk_nodes("euc-h100", 6, "H100")),
]

DEFAULT_QUOTAS = [
    Quota(
        "training-quota",
        {"us-east": {"H200": 48, "H100": 24}, "us-west": {"A100": 24}},
    ),
    Quota("inference-quota", {"us-east": {"H100": 16}, "eu-central": {"H100": 32}}),
    Quota("research-quota", {"us-west": {"L40S": 24, "A100": 16}}),
]

# ---------------------------------------------------------------------------
# Solver tick helpers
# ---------------------------------------------------------------------------


def _fully_running(pod: Pod) -> bool:
    return (
        bool(pod.statuses_by_replica)
        and pod.cluster is not None
        and all(s.phase == Phase.RUNNING and s.node is not None for s in pod.statuses_by_replica)
    )


def gang_sets_from(pods: dict[str, Pod], gangs: dict[str, str]) -> list[list[str]]:
    groups: dict[str, list[str]] = {}
    for job_id, gang_id in gangs.items():
        if job_id in pods:
            groups.setdefault(gang_id, []).append(job_id)
    return [sorted(g) for g in groups.values() if len(g) > 1]


def project_clusters(clusters: list[ClusterState], failed_nodes: set[str]) -> list[ClusterState]:
    return [
        ClusterState(c.name, [n for n in c.nodes if n.name not in failed_nodes]) for c in clusters
    ]


def advance_runtimes(
    pods: dict[str, Pod],
    runtimes: dict[str, float],
    gangs: dict[str, str],
    solved_pods: dict[str, Pod],
    dt: float,
) -> None:
    """Tick down runtimes and remove completed jobs. Mutates in place."""
    completed: list[str] = []
    for job_id in pods:
        solved = solved_pods.get(job_id)
        if solved is None or job_id not in runtimes:
            continue
        if _fully_running(solved):
            runtimes[job_id] = max(0.0, runtimes[job_id] - dt)
            if runtimes[job_id] <= 0:
                completed.append(job_id)
    for jid in completed:
        pods.pop(jid, None)
        runtimes.pop(jid, None)
        gangs.pop(jid, None)


def compute_summary(
    clusters: list[ClusterState], solved_pods: dict[str, Pod], total_jobs: int
) -> dict:
    total_capacity = sum(n.chips for c in clusters for n in c.nodes)
    total_used = 0
    running = 0
    queued = 0
    for pod in solved_pods.values():
        if _fully_running(pod):
            running += 1
            total_used += pod.chips_per_replica * len(pod.statuses_by_replica)
        else:
            queued += 1
    return {
        "job_count": total_jobs,
        "running_jobs": running,
        "queued_jobs": queued,
        "total_capacity": total_capacity,
        "used_capacity": total_used,
        "utilization_percent": round(total_used / total_capacity * 100, 2)
        if total_capacity
        else 0.0,
    }


def write_snapshot(
    state_dir: Path,
    scheduler: str,
    tick: int,
    input_pods: dict[str, Pod],
    solver_status: str,
    duration_ms: int,
    clusters: list[ClusterState],
    gang_sets: list[list[str]],
    quotas: list[Quota],
    total_jobs: int,
    failed_nodes: set[str],
    node_names: list[str],
) -> None:
    """Write a snapshot showing the solver *input* state plus solve stats."""
    snapshot = {
        "seq": tick,
        "timestamp": datetime.now(UTC).isoformat(),
        "scheduler": scheduler,
        "tick": tick,
        "solver_status": solver_status,
        "solver_duration_ms": duration_ms,
        "clusters": [asdict(c) for c in clusters],
        "pods": {name: asdict(pod) for name, pod in input_pods.items()},
        "gang_sets": gang_sets,
        "quotas": [asdict(q) for q in quotas],
        "summary": compute_summary(clusters, input_pods, total_jobs),
        "failed_nodes": sorted(failed_nodes),
        "nodes": node_names,
    }
    path = state_dir / f"latest-{scheduler}.json"
    path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Timeout case capture
# ---------------------------------------------------------------------------

BAD_SOLVE_CAP = 200


def maybe_save_bad_solve(
    state_dir: Path,
    scheduler: str,
    solver_status: str,
    duration_ms: int,
    request: SolverRequest,
) -> None:
    """Save and report solver inputs for any non-optimal solve.

    Deduplication: the content hash is embedded in the filename, so
    identical requests are never saved twice — even across restarts.
    Reports to Sentry with the request JSON attached.
    """
    # Only flag non-optimal results (e.g. "ok/maxTimeLimit").
    # The heuristic solver always returns "heuristic", and empty ticks
    # return "empty" — neither is actionable.
    if "optimal" in solver_status or solver_status in ("empty", "heuristic"):
        return
    blob = json.dumps(asdict(request), sort_keys=True).encode()
    h = hashlib.sha256(blob).hexdigest()[:16]
    case_dir = state_dir / "bad-solves"
    case_dir.mkdir(exist_ok=True)
    if list(case_dir.glob(f"*_{h}.json")):
        return
    for old in sorted(case_dir.glob("*.json"))[: -BAD_SOLVE_CAP + 1]:
        old.unlink(missing_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    path = case_dir / f"{ts}_{scheduler}_{h}.json"
    request_json = json.dumps(asdict(request), indent=2)
    path.write_text(request_json)
    log.warning("saved bad solve: %s (status=%s, %dms)", path.name, solver_status, duration_ms)

    sentry_sdk.set_context(
        "bad_solve",
        {"scheduler": scheduler, "solver_status": solver_status, "duration_ms": duration_ms},
    )
    sentry_sdk.add_attachment(
        bytes=request_json.encode(),
        filename=f"solver-request-{h}.json",
        content_type="application/json",
    )
    sentry_sdk.capture_message(
        f"Non-optimal solve: {scheduler} {solver_status} ({duration_ms}ms)",
        level="warning",
        fingerprint=["bad-solve", scheduler, h],
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


@dataclass
class _LoopState:
    """Mutable state bag for the tick loop — avoids deep nesting."""

    state_dir: Path
    config_path: Path
    clusters: list[ClusterState]
    quotas: list[Quota]
    solver_map: dict[str, Callable[..., ScheduleResult]]
    node_names: list[str]
    tick_interval: float

    pods: dict[str, Pod] = field(default_factory=dict)
    runtimes: dict[str, float] = field(default_factory=dict)
    gangs: dict[str, str] = field(default_factory=dict)
    failed_nodes: set[str] = field(default_factory=set)
    tick: int = 0
    config: GeneratorConfig = field(default_factory=GeneratorConfig)
    rng: random.Random = field(default_factory=lambda: random.Random(7))
    config_mtime: float = 0.0


def _maybe_reload_config(s: _LoopState) -> None:
    try:
        mtime = s.config_path.stat().st_mtime
    except FileNotFoundError:
        mtime = 0.0
    if mtime == s.config_mtime:
        return
    s.config_mtime = mtime
    s.config = read_config(s.config_path)
    s.rng = random.Random(s.config.seed)
    log.info("reloaded generator config")


def _tick_once(s: _LoopState) -> None:
    _maybe_reload_config(s)

    if s.config.running:
        generate_cycle(
            s.rng,
            s.config,
            s.pods,
            s.runtimes,
            s.gangs,
            s.failed_nodes,
            s.node_names,
            s.config.loop_interval_seconds,
        )

    s.tick += 1
    projected = project_clusters(s.clusters, s.failed_nodes)
    gang = gang_sets_from(s.pods, s.gangs)

    for name, solve_fn in s.solver_map.items():
        if not s.pods:
            write_snapshot(
                s.state_dir,
                name,
                s.tick,
                {},
                "empty",
                0,
                projected,
                gang,
                s.quotas,
                0,
                s.failed_nodes,
                s.node_names,
            )
            continue

        request = SolverRequest(projected, s.pods, gang, s.quotas)
        input_pods = dict(s.pods)
        started = perf_counter()
        result = solve_fn(projected, dict(s.pods), gang, s.quotas, time_limit=request.time_limit)
        duration_ms = round((perf_counter() - started) * 1000)
        if name != "heuristic":
            attrs = {"solver": name}
            sentry_sdk.metrics.distribution(
                "solver.duration_ms", duration_ms, unit="millisecond", attributes=attrs
            )
            sentry_sdk.metrics.gauge("solver.pod_count", len(s.pods), attributes=attrs)

        status = str(getattr(result, "solver_status", "ok"))
        maybe_save_bad_solve(s.state_dir, name, status, duration_ms, request)

        advance_runtimes(s.pods, s.runtimes, s.gangs, result.pods, s.tick_interval)

        # Feed solver assignments back so the next tick's input reflects them.
        for pod_name, pod in result.pods.items():
            if pod_name in s.pods:
                s.pods[pod_name] = pod

        write_snapshot(
            s.state_dir,
            name,
            s.tick,
            input_pods,
            status,
            duration_ms,
            projected,
            gang,
            s.quotas,
            len(s.pods),
            s.failed_nodes,
            s.node_names,
        )


def run_forever(
    *,
    state_dir: Path,
    clusters: list[ClusterState],
    quotas: list[Quota],
    tick_interval: float = 1.0,
    solvers: dict[str, Callable[..., ScheduleResult]] | None = None,
) -> None:
    """Run the solver tick loop until interrupted."""
    state_dir.mkdir(parents=True, exist_ok=True)
    config = read_config(state_dir / "config.json")
    s = _LoopState(
        state_dir=state_dir,
        config_path=state_dir / "config.json",
        clusters=clusters,
        quotas=quotas,
        solver_map=dict(solvers or SOLVERS),
        node_names=[n.name for c in clusters for n in c.nodes],
        tick_interval=tick_interval,
        config=config,
        rng=random.Random(config.seed),
        config_mtime=(state_dir / "config.json").stat().st_mtime
        if (state_dir / "config.json").exists()
        else 0.0,
    )

    log.info("loop runner started (tick=%.1fs, state=%s)", tick_interval, state_dir)
    try:
        while True:
            try:
                _tick_once(s)
            except Exception:
                log.exception("tick failed")
            sleep(tick_interval)
    except KeyboardInterrupt:
        log.info("shutting down")


def main() -> None:
    """Entry point for the loop runner."""
    import scheduler.observability  # noqa: F401 — initialise logging/sentry

    tick = float(os.environ.get("TICK_SECONDS", "1.0"))
    run_forever(
        state_dir=STATE_DIR, clusters=DEFAULT_CLUSTERS, quotas=DEFAULT_QUOTAS, tick_interval=tick
    )
