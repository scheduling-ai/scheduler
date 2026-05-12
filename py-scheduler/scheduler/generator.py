"""Synthetic workload generator shared between the simulator (``loop_runner``)
and the cluster-driving load generator (``load_generator``).

Two entry points:

- :func:`generate_cycle` — the simulator loop needs a complete world view
  (failing replicas, failing nodes, gang linkage) and mutates in place.
- :func:`generate_submissions` — the cluster loader only needs the *new*
  jobs this tick; it submits them to ``k8s-bridge`` and then forgets about
  them (k8s owns the lifecycle after that).
"""

from __future__ import annotations

import json
import logging
import math
import random
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from scheduler.model import Phase, Pod, PodReplicaStatus

log = logging.getLogger(__name__)


@dataclass
class GeneratorConfig:
    seed: int = 7
    # Tuned against the demo cluster (8 nodes × varying chips per pool):
    # at avg 4.6 chips per replica and 25s runtime, 1.5 jobs/sec leaves
    # H100/H200 pools at ~85% utilisation so bursts produce visible
    # pending in the UI without saturating the rest of the cluster.
    arrival_rate: float = 1.5
    burst_factor: float = 1.4
    quota_weights: dict[str, float] = field(
        default_factory=lambda: {
            "inference-quota": 1.0,
            "research-quota": 1.0,
            "training-quota": 1.0,
        }
    )
    chip_weights: dict[str, float] = field(
        default_factory=lambda: {"A100": 1.0, "H100": 1.0, "H200": 1.0, "L40S": 0.7}
    )
    # Chips-per-replica is keyed by chip type because realistic workload mixes
    # depend on the SKU. A single replica must fit on one node, so the keys
    # for each chip type stay <= the per-node chip count of that pool. The
    # weights track empirical cloud usage (see docs/notes): H100/H200 ship
    # almost exclusively as 8-GPU HGX baseboards, A100 has a broad SKU range
    # 1..16, L40S/L4 are PCIe single-card by default.
    chips_weights: dict[str, dict[int, float]] = field(
        default_factory=lambda: {
            "A100": {1: 0.30, 2: 0.15, 4: 0.15, 8: 0.35, 16: 0.05},
            "H100": {1: 0.20, 2: 0.10, 4: 0.10, 8: 0.60},
            "H200": {1: 0.20, 2: 0.10, 4: 0.10, 8: 0.60},
            "L40S": {1: 0.75, 2: 0.10, 4: 0.10},
        }
    )
    priority_min: int = 30
    priority_max: int = 99
    replica_min: int = 1
    replica_max: int = 2
    runtime_min: float = 12.0
    runtime_max: float = 40.0
    gang_frequency: float = 0.08
    replica_failure_rate: float = 0.03
    node_failure_rate: float = 0.005
    node_recovery_rate: float = 0.03
    loop_interval_seconds: float = 5.0
    # Cap on the sine-wave amplitude for each autoscaled Deployment driven
    # by `deployment_driver`.  0 disables the driver entirely.  Surfaced
    # in the UI's traffic-generator panel so a viewer can crank inference
    # contention up or quiet it down without touching code.
    deployment_max_replicas: int = 2
    running: bool = True

    @classmethod
    def from_dict(cls, data: dict) -> GeneratorConfig:
        """Build from a JSON-parsed dict, ignoring unknown keys.

        ``chips_weights`` survives a JSON round-trip with stringified int keys;
        coerce them back so the rest of the code can index by ``int``.
        """
        known = {f.name for f in cls.__dataclass_fields__.values()}
        if "chips_weights" in data and isinstance(data["chips_weights"], dict):
            data = dict(data)
            data["chips_weights"] = {
                str(chip_type): {int(k): float(v) for k, v in inner.items()}
                for chip_type, inner in data["chips_weights"].items()
            }
        return cls(**{k: v for k, v in data.items() if k in known})

    def to_dict(self) -> dict:
        result = asdict(self)
        result["chips_weights"] = {
            chip_type: {str(k): v for k, v in inner.items()}
            for chip_type, inner in result["chips_weights"].items()
        }
        return result


def read_config(path: Path) -> GeneratorConfig:
    if not path.exists():
        return GeneratorConfig()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return GeneratorConfig.from_dict(data)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        log.warning("bad config file %s: %s", path, exc)
        return GeneratorConfig()


def _choose[T](rng: random.Random, weights: dict[T, float]) -> T:
    items = list(weights.items())
    total = sum(w for _, w in items)
    target = rng.random() * total
    acc = 0.0
    for item, weight in items:
        acc += weight
        if target <= acc:
            return item
    return items[-1][0]


def _unique_id(prefix: str = "job") -> str:
    now = datetime.now(UTC)
    return f"{prefix}-{now.strftime('%m%d-%H%M%S')}-{now.microsecond:06d}"


def _make_job(rng: random.Random, cfg: GeneratorConfig) -> tuple[str, Pod, float]:
    """Returns (job_id, pod, runtime_seconds)."""
    runtime = round(rng.uniform(cfg.runtime_min, cfg.runtime_max), 2)
    replicas = rng.randint(cfg.replica_min, cfg.replica_max)
    job_id = _unique_id()
    chip_type = _choose(rng, cfg.chip_weights)
    # Chips-per-replica is per chip type so we never emit a replica that
    # exceeds a node's chip count (e.g. 8 chips on an L40S 4-chip node).
    # If a chip type is missing from the dict, fall back to single-chip —
    # always feasible, easy to spot in the UI.
    chip_dist = cfg.chips_weights.get(chip_type) or {1: 1.0}
    pod = Pod(
        chips_per_replica=_choose(rng, chip_dist),
        chip_type=chip_type,
        priority=rng.randint(cfg.priority_min, cfg.priority_max),
        quota=_choose(rng, cfg.quota_weights),
        cluster=None,
        statuses_by_replica=[PodReplicaStatus(Phase.RUNNING) for _ in range(replicas)],
    )
    return job_id, pod, runtime


@dataclass(frozen=True)
class NewSubmission:
    """A freshly generated job ready to be submitted somewhere.

    ``gang_id`` is set when this job was drawn as part of a multi-job gang
    set this tick — downstream code groups submissions by that id.
    """

    job_id: str
    pod: Pod
    runtime_seconds: float
    gang_id: str | None


def generate_submissions(
    rng: random.Random, cfg: GeneratorConfig, dt: float
) -> Iterator[NewSubmission]:
    """Yield the new jobs drawn from a Poisson arrival with burst factor.

    Does not touch any shared state — safe to drive a real cluster with.
    The loop runner's :func:`generate_cycle` wraps this to also apply
    simulator-only effects (replica failures, node failures).
    """
    multiplier = 1.0 + (max(cfg.burst_factor, 1.0) - 1.0) * rng.random()
    expected = max(0.0, cfg.arrival_rate * dt * multiplier)
    count = math.floor(expected)
    if rng.random() < expected - count:
        count += 1

    remaining = count
    while remaining > 0:
        if remaining >= 2 and rng.random() < cfg.gang_frequency:
            gang_id = _unique_id("gang")
            gang_size = min(remaining, 2 + int(rng.random() < 0.35))
            for _ in range(gang_size):
                job_id, pod, rt = _make_job(rng, cfg)
                yield NewSubmission(job_id, pod, rt, gang_id)
            remaining -= gang_size
            continue
        job_id, pod, rt = _make_job(rng, cfg)
        yield NewSubmission(job_id, pod, rt, None)
        remaining -= 1


def generate_cycle(
    rng: random.Random,
    cfg: GeneratorConfig,
    pods: dict[str, Pod],
    runtimes: dict[str, float],
    gangs: dict[str, str],
    failed_nodes: set[str],
    node_names: list[str],
    dt: float,
) -> None:
    """Mutate *pods*, *runtimes*, *gangs*, and *failed_nodes* in place.

    Used by the offline simulator (``loop_runner``); the real-cluster
    loader uses :func:`generate_submissions` instead.
    """
    for sub in generate_submissions(rng, cfg, dt):
        pods[sub.job_id] = sub.pod
        runtimes[sub.job_id] = sub.runtime_seconds
        if sub.gang_id is not None:
            gangs[sub.job_id] = sub.gang_id

    job_ids = list(pods.keys())
    if job_ids and rng.random() < cfg.replica_failure_rate * dt:
        target = rng.choice(job_ids)
        pod = pods[target]
        fail_count = 1 if rng.random() < 0.7 else 2
        new_count = max(0, len(pod.statuses_by_replica) - fail_count)
        if new_count <= 0:
            del pods[target]
            runtimes.pop(target, None)
            gangs.pop(target, None)
        else:
            pods[target] = Pod(
                pod.chips_per_replica,
                pod.chip_type,
                pod.priority,
                pod.quota,
                pod.cluster,
                pod.statuses_by_replica[:new_count],
            )

    healthy = [n for n in node_names if n not in failed_nodes]
    if healthy and rng.random() < cfg.node_failure_rate * dt:
        failed = rng.choice(healthy)
        failed_nodes.add(failed)
        for job_id in list(pods.keys()):
            pod = pods[job_id]
            survivors = [rs for rs in pod.statuses_by_replica if rs.node != failed]
            if len(survivors) == len(pod.statuses_by_replica):
                continue
            if not survivors:
                del pods[job_id]
                runtimes.pop(job_id, None)
                gangs.pop(job_id, None)
            else:
                pods[job_id] = Pod(
                    pod.chips_per_replica,
                    pod.chip_type,
                    pod.priority,
                    pod.quota,
                    pod.cluster,
                    survivors,
                )

    if failed_nodes and rng.random() < cfg.node_recovery_rate * dt:
        failed_nodes.discard(rng.choice(sorted(failed_nodes)))
