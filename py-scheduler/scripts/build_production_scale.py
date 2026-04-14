"""Generate scheduler/scenarios/production_scale.jsonl.

A canned 8-frame trace at the scale called for in docs/scaling_requirements.md:
15 unbalanced clusters (~20k GPUs), ~30 quotas, ~12k inference pods, ~25
gang-scheduled training jobs (mix of small / medium / large, some
cross-cluster).

The frames walk through a clear narrative the UI can demonstrate:

    0. Steady state.
    1. Three nodes in us-east-1 fail; affected replicas -> phase=failed.
    2. Failed replicas re-placed elsewhere; the dead nodes stay empty.
    3. A large new training job (cross-cluster, 288 H200) arrives queued.
    4. Solver suspends a low-priority training job and reclaims a batch of
       inference pods to make room; the new job is placed.
    5. Stable while the large job runs.
    6. Large job completes (-> phase=completed); suspended training resumes
       and the reclaimed inference pods come back online.
    7. Steady state restored; completed pods removed.

Run with::

    uv run python py-scheduler/scripts/build_production_scale.py

Writes ``py-scheduler/scheduler/scenarios/production_scale.jsonl``.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

OUTPUT = (
    Path(__file__).resolve().parent.parent / "scheduler" / "scenarios" / "production_scale.jsonl"
)

RNG_SEED = 20260414
START = datetime(2026, 4, 14, 10, 0, 0, tzinfo=UTC)
FRAME_INTERVAL = timedelta(seconds=30)

CHIPS_PER_NODE = 8


# ---------------------------------------------------------------------------
# Topology: 15 clusters, unbalanced sizes, mix of H200 / H100 / A100 / L40S
# ---------------------------------------------------------------------------

# (cluster_name, list of (chip_type, node_count))
CLUSTER_SHAPES: list[tuple[str, list[tuple[str, int]]]] = [
    ("us-east-1", [("H200", 500)]),  # 4000 H200 - mega
    ("us-west-2", [("H100", 400)]),  # 3200 H100 - mega
    ("eu-west-1", [("H200", 250)]),  # 2000 H200 - mid
    ("us-central-1", [("H100", 200)]),  # 1600 H100 - mid
    ("eu-central-1", [("A100", 150)]),  # 1200 A100 - mid
    ("ap-northeast-1", [("H200", 125)]),  # 1000 H200 - mid
    ("ap-east-1", [("H100", 100)]),  # 800 H100
    ("ap-south-1", [("A100", 100)]),  # 800 A100
    ("us-east-2", [("H100", 100)]),  # 800 H100
    ("us-west-1", [("A100", 75)]),  # 600 A100
    ("eu-west-2", [("L40S", 75)]),  # 600 L40S
    ("sa-east-1", [("A100", 60)]),  # 480 A100
    ("ca-central-1", [("L40S", 60)]),  # 480 L40S
    ("me-south-1", [("L40S", 50)]),  # 400 L40S
    ("af-south-1", [("H100", 50)]),  # 400 H100
]


# ---------------------------------------------------------------------------
# Quotas: 30 names, with per-cluster guarantees that sum to < cluster capacity
# ---------------------------------------------------------------------------

# (quota_name, list of (cluster, chip_type, guaranteed_chips))
QUOTA_GUARANTEES: list[tuple[str, list[tuple[str, str, int]]]] = [
    # Foundation / pretraining (heavy H200 in mega clusters)
    ("pretrain-foundation", [("us-east-1", "H200", 1600), ("eu-west-1", "H200", 600)]),
    ("pretrain-multimodal", [("us-east-1", "H200", 600), ("ap-northeast-1", "H200", 400)]),
    # Fine-tuning
    ("finetune-rlhf", [("us-east-1", "H200", 200), ("us-west-2", "H100", 300)]),
    ("finetune-instruct", [("us-west-2", "H100", 400)]),
    # Training other modalities
    ("train-vision", [("eu-central-1", "A100", 400), ("eu-west-1", "H200", 200)]),
    ("train-speech", [("us-central-1", "H100", 200), ("eu-central-1", "A100", 200)]),
    ("train-code", [("us-east-1", "H200", 300), ("us-west-2", "H100", 300)]),
    ("train-alignment", [("us-east-1", "H200", 200)]),
    # Online serving
    ("serve-search", [("us-west-2", "H100", 400), ("eu-west-1", "H200", 200)]),
    ("serve-recsys", [("us-west-2", "H100", 350), ("ap-east-1", "H100", 200)]),
    ("serve-ads", [("us-east-2", "H100", 250), ("eu-west-1", "H200", 100)]),
    ("serve-chat", [("us-west-2", "H100", 250), ("ap-northeast-1", "H200", 200)]),
    ("serve-embeddings", [("us-central-1", "H100", 200), ("eu-west-1", "H200", 100)]),
    ("serve-translate", [("eu-west-1", "H200", 150), ("eu-central-1", "A100", 100)]),
    ("serve-async", [("us-east-2", "H100", 150), ("ap-south-1", "A100", 150)]),
    ("serve-realtime", [("us-west-2", "H100", 200), ("us-east-1", "H200", 100)]),
    # Batch inference
    ("batch-vision", [("eu-west-2", "L40S", 200), ("ca-central-1", "L40S", 150)]),
    ("batch-speech", [("me-south-1", "L40S", 150), ("eu-west-2", "L40S", 100)]),
    ("batch-summary", [("af-south-1", "H100", 150), ("us-west-1", "A100", 100)]),
    ("batch-eval", [("ap-south-1", "A100", 200), ("sa-east-1", "A100", 150)]),
    # Research
    ("research-interp", [("us-west-1", "A100", 100), ("eu-central-1", "A100", 100)]),
    ("research-evals", [("us-west-1", "A100", 150)]),
    ("research-redteam", [("eu-central-1", "A100", 100)]),
    ("research-theory", [("sa-east-1", "A100", 100)]),
    ("research-scaling", [("us-east-1", "H200", 200)]),
    # Platform & ops
    ("mlops-platform", [("us-east-1", "H200", 100), ("us-west-2", "H100", 100)]),
    ("oncall-debug", [("us-east-1", "H200", 50), ("eu-west-1", "H200", 50)]),
    ("capacity-test", [("af-south-1", "H100", 100), ("ca-central-1", "L40S", 100)]),
    # External partners
    ("partner-acme", [("us-west-2", "H100", 200)]),
    ("partner-globex", [("ap-northeast-1", "H200", 150)]),
]


# ---------------------------------------------------------------------------
# Workload composition
# ---------------------------------------------------------------------------

# Inference deployments: (deployment_prefix, quota, chip_type, cluster, pod_count, priority)
INFERENCE_DEPLOYMENTS: list[tuple[str, str, str, str, int, int]] = [
    ("serve-search-prod", "serve-search", "H100", "us-west-2", 1800, 25),
    ("serve-search-eu", "serve-search", "H200", "eu-west-1", 800, 25),
    ("serve-recsys-prod", "serve-recsys", "H100", "us-west-2", 1500, 22),
    ("serve-recsys-asia", "serve-recsys", "H100", "ap-east-1", 700, 22),
    ("serve-ads-prod", "serve-ads", "H100", "us-east-2", 900, 28),
    ("serve-ads-eu", "serve-ads", "H200", "eu-west-1", 350, 28),
    ("serve-chat-prod", "serve-chat", "H100", "us-west-2", 1100, 30),
    ("serve-chat-asia", "serve-chat", "H200", "ap-northeast-1", 700, 30),
    ("serve-embeddings", "serve-embeddings", "H100", "us-central-1", 600, 18),
    ("serve-embeddings-eu", "serve-embeddings", "H200", "eu-west-1", 300, 18),
    ("serve-translate", "serve-translate", "H200", "eu-west-1", 400, 16),
    ("serve-translate-de", "serve-translate", "A100", "eu-central-1", 250, 16),
    ("serve-async", "serve-async", "H100", "us-east-2", 450, 14),
    ("serve-async-asia", "serve-async", "A100", "ap-south-1", 350, 14),
    ("serve-realtime", "serve-realtime", "H100", "us-west-2", 600, 32),
    ("batch-vision-eu", "batch-vision", "L40S", "eu-west-2", 450, 12),
    ("batch-vision-ca", "batch-vision", "L40S", "ca-central-1", 300, 12),
    ("batch-speech-me", "batch-speech", "L40S", "me-south-1", 300, 11),
    ("batch-summary-af", "batch-summary", "H100", "af-south-1", 250, 11),
    ("batch-eval-india", "batch-eval", "A100", "ap-south-1", 400, 10),
    ("batch-eval-sa", "batch-eval", "A100", "sa-east-1", 250, 10),
    ("mlops-platform", "mlops-platform", "H100", "us-west-2", 120, 35),
    ("mlops-platform-east", "mlops-platform", "H200", "us-east-1", 80, 35),
    ("oncall-debug", "oncall-debug", "H200", "us-east-1", 40, 40),
]

# Training jobs: each is a gang of >=2 single-Pod gang members.
# (job_id, [(role, cluster, chip_type, replicas, chips_per_replica)], priority, quota)
TRAINING_JOBS: list[tuple[str, list[tuple[str, str, str, int, int]], int, str]] = [
    # ── Large (256+ GPUs), cross-cluster ──
    (
        "pretrain-foundation-llama5",
        [("workers", "us-east-1", "H200", 64, 8), ("eval", "eu-west-1", "H200", 4, 8)],
        85,
        "pretrain-foundation",
    ),
    (
        "pretrain-foundation-gpt-omega",
        [("workers", "us-east-1", "H200", 32, 8), ("eval", "us-west-2", "H100", 4, 8)],
        80,
        "pretrain-foundation",
    ),
    (
        "pretrain-multimodal-vision-xl",
        [("workers", "eu-west-1", "H200", 24, 8), ("eval", "ap-northeast-1", "H200", 2, 8)],
        78,
        "pretrain-multimodal",
    ),
    # ── Medium (64-256 GPUs) ──
    (
        "finetune-rlhf-llama4-prod",
        [("workers", "us-east-1", "H200", 16, 8), ("eval", "us-east-1", "H200", 1, 8)],
        72,
        "finetune-rlhf",
    ),
    (
        "finetune-instruct-claude35",
        [("workers", "us-west-2", "H100", 12, 8), ("eval", "us-west-2", "H100", 1, 8)],
        70,
        "finetune-instruct",
    ),
    (
        "train-code-deepseek-v3",
        [("workers", "us-east-1", "H200", 16, 8), ("eval", "us-west-2", "H100", 1, 8)],
        68,
        "train-code",
    ),
    (
        "train-vision-yolo-v10",
        [("workers", "eu-central-1", "A100", 12, 8), ("eval", "eu-central-1", "A100", 1, 8)],
        65,
        "train-vision",
    ),
    (
        "train-speech-whisper-large",
        [("workers", "us-central-1", "H100", 10, 8), ("eval", "eu-central-1", "A100", 1, 8)],
        63,
        "train-speech",
    ),
    (
        "train-alignment-rlhf-v7",
        [("workers", "us-east-1", "H200", 12, 8), ("eval", "us-east-1", "H200", 1, 8)],
        70,
        "train-alignment",
    ),
    (
        "finetune-rlhf-mistral-72b",
        [("workers", "eu-west-1", "H200", 8, 8), ("eval", "eu-west-1", "H200", 1, 8)],
        66,
        "finetune-rlhf",
    ),
    # ── Small (8-64 GPUs) ──
    (
        "train-vision-experimental",  # the one we suspend later
        [("workers", "eu-central-1", "A100", 6, 8), ("eval", "eu-central-1", "A100", 1, 8)],
        45,
        "train-vision",
    ),
    (
        "train-code-completion-v2",
        [("workers", "us-west-2", "H100", 4, 8), ("eval", "us-west-2", "H100", 1, 8)],
        58,
        "train-code",
    ),
    (
        "research-interp-probe-circuits",
        [("workers", "us-west-1", "A100", 4, 8), ("eval", "us-west-1", "A100", 1, 8)],
        50,
        "research-interp",
    ),
    (
        "research-interp-probe-attention",
        [("workers", "eu-central-1", "A100", 2, 8), ("eval", "eu-central-1", "A100", 1, 8)],
        50,
        "research-interp",
    ),
    (
        "research-evals-baseline-v4",
        [("workers", "us-west-1", "A100", 3, 8), ("eval", "us-west-1", "A100", 1, 8)],
        52,
        "research-evals",
    ),
    (
        "research-redteam-jailbreak-3",
        [("workers", "eu-central-1", "A100", 2, 8), ("eval", "eu-central-1", "A100", 1, 8)],
        55,
        "research-redteam",
    ),
    (
        "research-theory-mech-interp",
        [("workers", "sa-east-1", "A100", 2, 8), ("eval", "sa-east-1", "A100", 1, 8)],
        48,
        "research-theory",
    ),
    (
        "research-scaling-laws-2026",
        [("workers", "us-east-1", "H200", 4, 8), ("eval", "us-east-1", "H200", 1, 8)],
        62,
        "research-scaling",
    ),
    (
        "capacity-test-bench-q2",
        [("workers", "af-south-1", "H100", 4, 8), ("eval", "af-south-1", "H100", 1, 8)],
        40,
        "capacity-test",
    ),
    (
        "partner-acme-finetune",
        [("workers", "us-west-2", "H100", 6, 8), ("eval", "us-west-2", "H100", 1, 8)],
        60,
        "partner-acme",
    ),
    (
        "partner-globex-train",
        [("workers", "ap-northeast-1", "H200", 8, 8), ("eval", "ap-northeast-1", "H200", 1, 8)],
        62,
        "partner-globex",
    ),
]

# The large training job that arrives mid-trace (frame 3+) and forces reclaim.
ARRIVAL_JOB: tuple[str, list[tuple[str, str, str, int, int]], int, str] = (
    "pretrain-foundation-llama5-next",
    [("workers", "us-east-1", "H200", 32, 8), ("eval", "eu-west-1", "H200", 4, 8)],
    90,
    "pretrain-foundation",
)

# Nodes that fail in frame 1: a rack failure spanning training (low indices)
# and inference (higher indices) so the failure has visible blast radius.
FAILED_NODES = [
    "us-east-1-node-0010",  # training (1 replica)
    "us-east-1-node-0011",  # training (1 replica)
    "us-east-1-node-0150",  # oncall-debug inference (8 replicas)
    "us-east-1-node-0155",  # mlops-platform-east inference (8 replicas)
    "us-east-1-node-0160",  # mlops-platform-east inference (8 replicas)
]


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


@dataclass
class Replica:
    phase: str = "running"
    node: str | None = None


@dataclass
class PodRec:
    name: str
    chips_per_replica: int
    chip_type: str
    priority: int
    quota: str
    cluster_hint: str | None  # cluster the workload belongs to (placement target)
    statuses: list[Replica]


@dataclass
class State:
    rng: random.Random
    nodes: list[tuple[str, str, str]]  # (cluster, node_name, chip_type)
    free: dict[str, int] = field(default_factory=dict)  # node_name -> chips free
    node_cluster: dict[str, str] = field(default_factory=dict)
    node_chip: dict[str, str] = field(default_factory=dict)
    nodes_by_cluster_chip: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    pods: dict[str, PodRec] = field(default_factory=dict)
    gang_sets: list[list[str]] = field(default_factory=list)
    failed_nodes: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        for cluster, name, chip in self.nodes:
            self.free[name] = CHIPS_PER_NODE
            self.node_cluster[name] = cluster
            self.node_chip[name] = chip
            self.nodes_by_cluster_chip.setdefault((cluster, chip), []).append(name)

    # ── Placement helpers ──

    def free_in(self, cluster: str, chip_type: str) -> int:
        return sum(
            self.free[n]
            for n in self.nodes_by_cluster_chip.get((cluster, chip_type), [])
            if n not in self.failed_nodes
        )

    def place_one_chip(self, cluster: str, chip_type: str) -> str | None:
        nodes = self.nodes_by_cluster_chip.get((cluster, chip_type), [])
        for name in nodes:
            if name in self.failed_nodes:
                continue
            if self.free[name] >= 1:
                self.free[name] -= 1
                return name
        return None

    def place_full_node(self, cluster: str, chip_type: str) -> str | None:
        nodes = self.nodes_by_cluster_chip.get((cluster, chip_type), [])
        for name in nodes:
            if name in self.failed_nodes:
                continue
            if self.free[name] == CHIPS_PER_NODE:
                self.free[name] = 0
                return name
        return None

    def release(self, node: str, chips: int) -> None:
        if node in self.free:
            self.free[node] = min(CHIPS_PER_NODE, self.free[node] + chips)

    # ── Pod operations ──

    def add_inference_deployment(
        self, prefix: str, quota: str, chip_type: str, cluster: str, count: int, priority: int
    ) -> None:
        for i in range(1, count + 1):
            name = f"{prefix}-{i:04d}"
            self.pods[name] = PodRec(
                name=name,
                chips_per_replica=1,
                chip_type=chip_type,
                priority=priority,
                quota=quota,
                cluster_hint=cluster,
                statuses=[Replica()],
            )

    def add_training_job(
        self,
        job_id: str,
        members: list[tuple[str, str, str, int, int]],
        priority: int,
        quota: str,
    ) -> list[str]:
        names: list[str] = []
        for role, cluster, chip_type, replicas, cpr in members:
            name = f"{job_id}-{role}"
            self.pods[name] = PodRec(
                name=name,
                chips_per_replica=cpr,
                chip_type=chip_type,
                priority=priority,
                quota=quota,
                cluster_hint=cluster,
                statuses=[Replica() for _ in range(replicas)],
            )
            names.append(name)
        if len(names) > 1:
            self.gang_sets.append(sorted(names))
        return names

    def place_pod(self, pod: PodRec) -> bool:
        """Place all unplaced replicas of pod. Returns True if all placed."""
        cluster = pod.cluster_hint
        if cluster is None:
            return False
        for r in pod.statuses:
            if r.node is not None:
                continue
            if pod.chips_per_replica == CHIPS_PER_NODE:
                node = self.place_full_node(cluster, pod.chip_type)
            else:
                node = self.place_one_chip(cluster, pod.chip_type)
            if node is None:
                return False
            r.node = node
            r.phase = "running"
        return True

    def unplace_pod(self, pod: PodRec, new_phase: str) -> None:
        """Free all replicas of pod and set them to new_phase, node=None."""
        for r in pod.statuses:
            if r.node:
                self.release(r.node, pod.chips_per_replica)
            r.node = None
            r.phase = new_phase

    def fail_node(self, node: str) -> list[str]:
        """Move all replicas on a node to phase=failed, node=None. Returns affected pod names."""
        self.failed_nodes.add(node)
        affected: list[str] = []
        for pod in self.pods.values():
            for r in pod.statuses:
                if r.node == node:
                    r.node = None
                    r.phase = "failed"
                    affected.append(pod.name)
        # node's chips are gone (the node is unhealthy, not free)
        self.free[node] = 0
        return sorted(set(affected))

    def replace_failed(self, pod_names: list[str]) -> None:
        """Re-place any replica that is in failed state, moving it elsewhere."""
        for name in pod_names:
            pod = self.pods.get(name)
            if pod is None:
                continue
            for r in pod.statuses:
                if r.phase != "failed":
                    continue
                # Try to place on a healthy node, same chip_type, same cluster first,
                # then any other cluster with that chip type.
                clusters_to_try = [pod.cluster_hint] + [
                    c for c, _ in CLUSTER_SHAPES if c != pod.cluster_hint
                ]
                placed = False
                for cluster in clusters_to_try:
                    if cluster is None:
                        continue
                    if pod.chips_per_replica == CHIPS_PER_NODE:
                        node = self.place_full_node(cluster, pod.chip_type)
                    else:
                        node = self.place_one_chip(cluster, pod.chip_type)
                    if node is not None:
                        r.node = node
                        r.phase = "running"
                        placed = True
                        break
                if not placed:
                    # Leave it queued (running, no node).
                    r.phase = "running"

    # ── Snapshots ──

    def snapshot(self, seq: int, ts: datetime, reason: str) -> dict:
        clusters_out = []
        nodes_by_cluster: dict[str, list[dict]] = {}
        for cluster, name, chip in self.nodes:
            nodes_by_cluster.setdefault(cluster, []).append(
                {"name": name, "chips": CHIPS_PER_NODE, "chip_type": chip}
            )
        for cluster_name, _ in CLUSTER_SHAPES:
            clusters_out.append({"name": cluster_name, "nodes": nodes_by_cluster[cluster_name]})

        pods_out: dict[str, dict] = {}
        running_jobs = 0
        queued_replicas = 0
        used_chips = 0
        total_chips = sum(CHIPS_PER_NODE for _ in self.nodes)
        for pod in self.pods.values():
            statuses = []
            placed = False
            for r in pod.statuses:
                entry: dict = {"phase": r.phase}
                if r.node is not None:
                    entry["node"] = r.node
                    placed = True
                    used_chips += pod.chips_per_replica
                else:
                    if r.phase in ("running", "failed"):
                        queued_replicas += 1
                statuses.append(entry)
            if placed:
                running_jobs += 1
            pods_out[pod.name] = {
                "priority": pod.priority,
                "quota": pod.quota,
                "chip_type": pod.chip_type,
                "chips_per_replica": pod.chips_per_replica,
                "statuses_by_replica": statuses,
            }

        quotas_out = []
        for qname, items in QUOTA_GUARANTEES:
            guarantees: dict[str, dict[str, int]] = {}
            for cluster, chip_type, chips in items:
                guarantees.setdefault(cluster, {})[chip_type] = chips
            quotas_out.append({"name": qname, "guarantees": guarantees})

        utilization = round(100 * used_chips / total_chips, 2) if total_chips else 0.0

        return {
            "seq": seq,
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reason": reason,
            "solver_status": "optimal",
            "solver_duration_ms": int(self.rng.uniform(800, 1800)),
            "summary": {
                "running_jobs": running_jobs,
                "queued_jobs": queued_replicas,
                "utilization_percent": utilization,
            },
            "clusters": clusters_out,
            "quotas": quotas_out,
            "pods": pods_out,
            "gang_sets": [list(g) for g in self.gang_sets],
        }


# ---------------------------------------------------------------------------
# Build the trace
# ---------------------------------------------------------------------------


def build_nodes() -> list[tuple[str, str, str]]:
    nodes: list[tuple[str, str, str]] = []
    for cluster, parts in CLUSTER_SHAPES:
        idx = 0
        for chip, count in parts:
            for _ in range(count):
                nodes.append((cluster, f"{cluster}-node-{idx:04d}", chip))
                idx += 1
    return nodes


def build_initial_state(rng: random.Random) -> State:
    state = State(rng=rng, nodes=build_nodes())

    # Inference deployments.
    for prefix, quota, chip, cluster, count, pri in INFERENCE_DEPLOYMENTS:
        state.add_inference_deployment(prefix, quota, chip, cluster, count, pri)

    # Training jobs.
    for job_id, members, pri, quota in TRAINING_JOBS:
        state.add_training_job(job_id, members, pri, quota)

    # Place training first (high priority), then inference.
    train_pods = sorted(
        (p for p in state.pods.values() if p.chips_per_replica == CHIPS_PER_NODE),
        key=lambda p: -p.priority,
    )
    for p in train_pods:
        state.place_pod(p)

    inf_pods = sorted(
        (p for p in state.pods.values() if p.chips_per_replica == 1),
        key=lambda p: -p.priority,
    )
    for p in inf_pods:
        state.place_pod(p)
        # Stop placing once a pool is heavily used; remaining pods stay queued
        # (matches a realistic steady state where some inference is waiting).

    return state


def reclaim_inference(state: State, count: int) -> list[str]:
    """Unplace `count` lowest-priority placed inference replicas. Returns names."""
    candidates = sorted(
        (p for p in state.pods.values() if p.chips_per_replica == 1),
        key=lambda p: p.priority,
    )
    reclaimed: list[str] = []
    for pod in candidates:
        for r in pod.statuses:
            if r.node is not None:
                state.release(r.node, 1)
                r.node = None
                r.phase = "running"
                reclaimed.append(pod.name)
                if len(reclaimed) >= count:
                    return reclaimed
    return reclaimed


def restore_inference(state: State, names: list[str]) -> None:
    for name in names:
        pod = state.pods.get(name)
        if pod is None:
            continue
        state.place_pod(pod)


def main() -> None:
    rng = random.Random(RNG_SEED)
    state = build_initial_state(rng)

    frames: list[dict] = []

    # Frame 0: steady state.
    frames.append(state.snapshot(seq=1, ts=START, reason="steady_state"))

    # Frame 1: three nodes fail.
    affected: list[str] = []
    for n in FAILED_NODES:
        affected.extend(state.fail_node(n))
    affected = sorted(set(affected))
    frames.append(state.snapshot(seq=2, ts=START + FRAME_INTERVAL, reason="node_failure"))

    # Frame 2: failed replicas re-placed elsewhere.
    state.replace_failed(affected)
    frames.append(state.snapshot(seq=3, ts=START + 2 * FRAME_INTERVAL, reason="recovery"))

    # Frame 3: large training job arrives; queued.
    arrival_names = state.add_training_job(*ARRIVAL_JOB)
    frames.append(state.snapshot(seq=4, ts=START + 3 * FRAME_INTERVAL, reason="large_job_queued"))

    # Frame 4: solver suspends a low-pri training job, reclaims inference, places arrival.
    suspend_names = [
        "train-vision-experimental-workers",
        "train-vision-experimental-eval",
    ]
    for name in suspend_names:
        pod = state.pods.get(name)
        if pod is not None:
            state.unplace_pod(pod, "suspended")
    reclaimed = reclaim_inference(state, count=192)
    for name in arrival_names:
        pod = state.pods.get(name)
        if pod is not None:
            state.place_pod(pod)
    frames.append(state.snapshot(seq=5, ts=START + 4 * FRAME_INTERVAL, reason="reclaim_and_place"))

    # Frame 5: stable while large job runs.
    frames.append(state.snapshot(seq=6, ts=START + 5 * FRAME_INTERVAL, reason="running"))

    # Frame 6: large job completes; suspended training resumes; inference comes back.
    for name in arrival_names:
        pod = state.pods.get(name)
        if pod is not None:
            for r in pod.statuses:
                if r.node:
                    state.release(r.node, pod.chips_per_replica)
                r.node = None
                r.phase = "completed"
    for name in suspend_names:
        pod = state.pods.get(name)
        if pod is not None:
            state.place_pod(pod)
    restore_inference(state, reclaimed)
    frames.append(
        state.snapshot(seq=7, ts=START + 6 * FRAME_INTERVAL, reason="completion_and_resume")
    )

    # Frame 7: completed pods removed; steady state restored.
    for name in arrival_names:
        state.pods.pop(name, None)
    state.gang_sets = [g for g in state.gang_sets if not any(n in arrival_names for n in g)]
    frames.append(state.snapshot(seq=8, ts=START + 7 * FRAME_INTERVAL, reason="steady_state"))

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w") as f:
        for frame in frames:
            f.write(json.dumps(frame, separators=(",", ":")))
            f.write("\n")

    size_mb = OUTPUT.stat().st_size / 1024 / 1024
    print(f"wrote {OUTPUT.name}: {len(frames)} frames, {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
