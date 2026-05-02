"""Generate scheduler/scenarios/production_scale.jsonl.

A canned multi-frame trace at the scale described in
docs/private/scaling_requirements.md.  The fixture is illustrative — cluster
sizes, quotas, and workload mix are example values for a research-leaning lab
fleet at this order of magnitude, not measurements of any specific
organisation.

Topology:
- 3 training fabrics (mega + mid + tertiary), dense H200/H100.
- 6 inference regions, mix of H100/A100/L40S, sized for latency-to-users.

Workloads:
- Pretraining: 1–2 active jobs, the largest may span two fabrics as a
  workers+workers gang (a single parallelism group split across fabrics
  because no single fabric fits it).
- Posttraining (RLHF/DPO/SFT/distillation): O(10) active jobs, intra-cluster.
- Evals: standalone (non-gang) Jobs, small.  They consume checkpoints; they
  are not gang siblings of training pods.
- Research long tail: O(50–150) small experimental jobs across a fixed
  user pool; preemptable.
- Inference: 4 deployment classes per region (flagship, mini, embedding,
  batch), replica counts skewed heavy on flagship.
- Partners: small bounded slice, anonymised.

Frame narrative (8 frames, 1 frame ~= 1 hour of wall clock — see
``frame_interval_seconds`` on each frame):

    0. Steady state (~90% utilisation on train-fabric-1).
    1. Rack failure on train-fabric-1: 8 contiguous nodes (one rack, shared
       PSU and ToR) go down; affected pretraining replicas -> failed.
    2. Failed replicas re-placed within the fabric's slack.
    3. A new posttraining job (rlhf, 32 nodes / 256 H200) arrives queued.
    4. Solver suspends lowest-priority eligible pods on fabric-1 (research
       first, then a low-priority posttraining job if research alone isn't
       enough), admits the arrival.
    5. Stable while the arrival runs.
    6. Arrival completes; suspended pods resume.
    7. Steady state restored; rack repaired (failed nodes back as healthy).

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

RNG_SEED = 20260501
START = datetime(2026, 5, 1, 8, 0, 0, tzinfo=UTC)
FRAME_INTERVAL = timedelta(hours=1)
FRAME_INTERVAL_SECONDS = int(FRAME_INTERVAL.total_seconds())

CHIPS_PER_NODE = 8

# ---------------------------------------------------------------------------
# Topology: 3 training fabrics + 6 inference regions
# ---------------------------------------------------------------------------
#
# Training fabrics carry pretraining/posttraining/research (dense H200/H100).
# Inference regions carry serving deployments (mix of chip generations,
# including L40S for batch/cheap inference).
#
# (cluster_name, list of (chip_type, node_count))
CLUSTER_SHAPES: list[tuple[str, list[tuple[str, int]]]] = [
    # Training fabrics
    ("train-fabric-1", [("H200", 500)]),  # 4000 H200 — primary mega
    ("train-fabric-2", [("H200", 375)]),  # 3000 H200 — secondary
    ("train-fabric-3", [("H100", 250)]),  # 2000 H100 — older generation
    # Inference regions (6, varied sizes; H100/A100/L40S only — no H200 here)
    ("inf-region-1", [("H100", 150)]),  # 1200 H100
    ("inf-region-2", [("H100", 125)]),  # 1000 H100
    ("inf-region-3", [("A100", 100)]),  #  800 A100
    ("inf-region-4", [("A100", 75)]),  #  600 A100
    ("inf-region-5", [("L40S", 75)]),  #  600 L40S
    ("inf-region-6", [("L40S", 50)]),  #  400 L40S
]
# Total: 13_600 GPUs, mostly on training fabrics.
# Doc envelope is "order of 20k"; this is on that order.

TRAINING_FABRICS = ["train-fabric-1", "train-fabric-2", "train-fabric-3"]
INFERENCE_REGIONS = [
    "inf-region-1",
    "inf-region-2",
    "inf-region-3",
    "inf-region-4",
    "inf-region-5",
    "inf-region-6",
]


# ---------------------------------------------------------------------------
# Quotas: 10 team/cost-center scoped quotas
# ---------------------------------------------------------------------------
#
# Guarantees are illustrative round numbers. Sum-per-cluster < capacity
# (asserted at build time).
#
# (quota_name, list of (cluster, chip_type, guaranteed_chips))
QUOTA_GUARANTEES: list[tuple[str, list[tuple[str, str, int]]]] = [
    (
        "pretraining",
        [
            ("train-fabric-1", "H200", 2400),
            ("train-fabric-2", "H200", 2000),
            ("train-fabric-3", "H100", 800),
        ],
    ),
    (
        "posttraining",
        [
            ("train-fabric-1", "H200", 600),
            ("train-fabric-2", "H200", 400),
            ("train-fabric-3", "H100", 400),
        ],
    ),
    (
        "safety-evals",
        [
            ("train-fabric-1", "H200", 200),
            ("train-fabric-3", "H100", 200),
        ],
    ),
    (
        "interp",
        [
            ("train-fabric-3", "H100", 200),
        ],
    ),
    (
        "research",
        [
            ("train-fabric-1", "H200", 200),
            ("train-fabric-2", "H200", 200),
            ("train-fabric-3", "H100", 200),
        ],
    ),
    (
        "serving",
        [
            ("inf-region-1", "H100", 800),
            ("inf-region-2", "H100", 600),
            ("inf-region-3", "A100", 400),
            ("inf-region-4", "A100", 300),
        ],
    ),
    (
        "serving-batch",
        [
            ("inf-region-5", "L40S", 400),
            ("inf-region-6", "L40S", 300),
        ],
    ),
    (
        "partners",
        [
            ("train-fabric-2", "H200", 100),
        ],
    ),
    (
        "infra",
        [
            ("train-fabric-3", "H100", 100),
        ],
    ),
    (
        "oncall",
        [
            ("train-fabric-1", "H200", 16),  # one node-equivalent reserved hot-spare
        ],
    ),
]


# ---------------------------------------------------------------------------
# Workload generators
# ---------------------------------------------------------------------------

# Priorities: inference < research < posttraining ~ evals < pretraining.
PRIORITY = {
    "infra": 5,
    "research_low": 20,
    "inference_batch": 25,
    "research_high": 35,
    "inference_serving": 40,
    "partners": 50,
    "posttraining": 65,
    "safety_evals": 72,  # above posttraining — eval results gate releases
    "pretraining": 85,
    "oncall": 90,  # reserved hot-spare, never displaced
}

# A fixed pool of pseudonymous research users.  Used for the long tail.
RESEARCH_USERS = [f"user{i:02d}" for i in range(1, 21)]

# Slug fragments for research jobs (deliberately generic, no model names).
RESEARCH_SLUGS = [
    "attn-scan",
    "tokenizer-r2",
    "lr-sweep",
    "dropout-r3",
    "rope-ablation",
    "init-study",
    "grad-clip",
    "mup-test",
    "moe-ablation",
    "depth-sweep",
    "bs-scan",
    "warmup-r2",
    "ctx-len",
    "mha-vs-gqa",
    "ssm-probe",
]

# Eval suites (non-gang Jobs, standalone).
EVAL_SUITES = [
    "mmlu",
    "coding",
    "math-bench",
    "redteam",
    "factuality",
    "safety-suite",
]


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
    cluster_hint: str | None
    statuses: list[Replica]


@dataclass
class State:
    rng: random.Random
    nodes: list[tuple[str, str, str]]  # (cluster, node_name, chip_type)
    free: dict[str, int] = field(default_factory=dict)
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
        for name in self.nodes_by_cluster_chip.get((cluster, chip_type), []):
            if name in self.failed_nodes:
                continue
            if self.free[name] >= 1:
                self.free[name] -= 1
                return name
        return None

    def place_full_node(self, cluster: str, chip_type: str) -> str | None:
        for name in self.nodes_by_cluster_chip.get((cluster, chip_type), []):
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

    def add_pod(self, pod: PodRec) -> None:
        self.pods[pod.name] = pod

    def add_gang(self, member_names: list[str]) -> None:
        if len(member_names) > 1:
            self.gang_sets.append(sorted(member_names))

    def place_pod(self, pod: PodRec) -> bool:
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
        for r in pod.statuses:
            if r.node:
                self.release(r.node, pod.chips_per_replica)
            r.node = None
            r.phase = new_phase

    def fail_node(self, node: str) -> list[str]:
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

    def repair_nodes(self, names: list[str]) -> None:
        for name in names:
            if name in self.failed_nodes:
                self.failed_nodes.remove(name)
                self.free[name] = CHIPS_PER_NODE

    def replace_failed(self, pod_names: list[str]) -> None:
        for name in pod_names:
            pod = self.pods.get(name)
            if pod is None:
                continue
            for r in pod.statuses:
                if r.phase != "failed":
                    continue
                # Try same cluster first, then any other cluster with that chip type.
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
                    # Leave queued (running, no node).
                    r.phase = "running"

    # ── Snapshot ──

    def snapshot(self, seq: int, ts: datetime, reason: str) -> dict:
        nodes_by_cluster: dict[str, list[dict]] = {}
        for cluster, name, chip in self.nodes:
            nodes_by_cluster.setdefault(cluster, []).append(
                {"name": name, "chips": CHIPS_PER_NODE, "chip_type": chip}
            )
        clusters_out = [
            {"name": cname, "nodes": nodes_by_cluster[cname]} for cname, _ in CLUSTER_SHAPES
        ]

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
            "frame_interval_seconds": FRAME_INTERVAL_SECONDS,
            "reason": reason,
            "solver_status": "synthetic",
            "solver_duration_ms": None,
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
# Build
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


def assert_quota_invariant() -> None:
    """∑ guarantees per (cluster, chip_type) < capacity for that pair."""
    capacity: dict[tuple[str, str], int] = {}
    for cluster, parts in CLUSTER_SHAPES:
        for chip, count in parts:
            capacity[(cluster, chip)] = count * CHIPS_PER_NODE
    used: dict[tuple[str, str], int] = {}
    for _, items in QUOTA_GUARANTEES:
        for cluster, chip, chips in items:
            used[(cluster, chip)] = used.get((cluster, chip), 0) + chips
    for key, total in used.items():
        cap = capacity.get(key, 0)
        if total >= cap:
            raise AssertionError(
                f"quota guarantees on {key} sum to {total}, must be < cluster capacity {cap}"
            )


def add_pretraining(state: State) -> tuple[list[str], list[str]]:
    """Two pretraining jobs.  The first is the largest single job in the
    fixture (1024 H200) on train-fabric-1.  The second is a cross-fabric
    workers+workers gang split across fabric-1 and fabric-2.
    """
    pri = PRIORITY["pretraining"]

    # Job A: large single-fabric pretrain on fabric-1.  Sized to occupy most
    # of the fabric so the arrival in frame 4 forces real preemption.
    a_workers = PodRec(
        name="pretrain-base-2026q2-workers",
        chips_per_replica=CHIPS_PER_NODE,
        chip_type="H200",
        priority=pri,
        quota="pretraining",
        cluster_hint="train-fabric-1",
        statuses=[Replica() for _ in range(320)],  # 320 nodes × 8 = 2560 H200
    )
    state.add_pod(a_workers)

    # Job B: cross-fabric pretrain (workers split across fabric-1 + fabric-2).
    # This is the *exception* — only used when one fabric can't fit the
    # parallelism group.  Both shards are at the same priority and quota.
    b_shard_a = PodRec(
        name="pretrain-mid-2026q2-shard-a",
        chips_per_replica=CHIPS_PER_NODE,
        chip_type="H200",
        priority=pri - 5,
        quota="pretraining",
        cluster_hint="train-fabric-1",
        statuses=[Replica() for _ in range(64)],  # 64 nodes × 8 = 512 H200
    )
    b_shard_b = PodRec(
        name="pretrain-mid-2026q2-shard-b",
        chips_per_replica=CHIPS_PER_NODE,
        chip_type="H200",
        priority=pri - 5,
        quota="pretraining",
        cluster_hint="train-fabric-2",
        statuses=[Replica() for _ in range(256)],  # 256 nodes × 8 = 2048 H200
    )
    state.add_pod(b_shard_a)
    state.add_pod(b_shard_b)
    state.add_gang([b_shard_a.name, b_shard_b.name])

    return [a_workers.name], [b_shard_a.name, b_shard_b.name]


def add_posttraining(state: State) -> list[str]:
    """About 12 posttraining jobs across rlhf/dpo/sft/distill, intra-cluster.

    Sizes vary 64–512 GPUs.  Each is a single Pod (no gang) — the posttraining
    pipelines here are framed as one parallel training group per job.
    """
    pri = PRIORITY["posttraining"]
    specs = [
        # (slug, chip_type, fabric, replicas)
        ("rlhf-2026-04-22", "H200", "train-fabric-1", 32),  # 256 GPU
        ("rlhf-2026-04-25", "H200", "train-fabric-2", 24),  # 192 GPU
        ("dpo-r3", "H200", "train-fabric-1", 16),  # 128 GPU
        ("dpo-r4-helpful", "H200", "train-fabric-2", 16),  # 128 GPU
        ("sft-tools-r7", "H100", "train-fabric-3", 32),  # 256 GPU
        ("sft-tools-r8", "H100", "train-fabric-3", 16),  # 128 GPU
        ("distill-mini-r4", "H200", "train-fabric-1", 12),  # 96 GPU
        ("distill-mini-r5", "H200", "train-fabric-2", 8),  # 64 GPU
        ("rlhf-harmless-r2", "H100", "train-fabric-3", 24),  # 192 GPU
        ("dpo-tone-r1", "H200", "train-fabric-1", 8),  # 64 GPU
        ("sft-cookbook-r2", "H100", "train-fabric-3", 12),  # 96 GPU
        ("rlhf-tools-r5", "H200", "train-fabric-2", 12),  # 96 GPU
    ]
    names: list[str] = []
    for slug, chip, fabric, reps in specs:
        name = f"posttrain-{slug}"
        state.add_pod(
            PodRec(
                name=name,
                chips_per_replica=CHIPS_PER_NODE,
                chip_type=chip,
                priority=pri + state.rng.randint(-3, 3),
                quota="posttraining",
                cluster_hint=fabric,
                statuses=[Replica() for _ in range(reps)],
            )
        )
        names.append(name)
    return names


def add_evals(state: State) -> list[str]:
    """About 18 standalone eval Jobs, small (8–32 GPUs each).  Not gang
    siblings of training pods — they run independently against checkpoints.
    """
    pri = PRIORITY["safety_evals"]
    names: list[str] = []
    rng = state.rng
    suites = list(EVAL_SUITES)
    for r in range(1, 19):
        suite = suites[r % len(suites)]
        # Bias evals to fabric-3 (H100, the eval-friendly older fabric).
        fabric = rng.choices(
            ["train-fabric-3", "train-fabric-1"],
            weights=[3, 1],
        )[0]
        chip = "H100" if fabric == "train-fabric-3" else "H200"
        reps = rng.choice([1, 1, 2, 2, 4])  # mostly 8–16 GPU, occasionally 32
        quota = "safety-evals" if "redteam" in suite or "safety" in suite else "safety-evals"
        name = f"eval-{suite}-r{r:02d}"
        state.add_pod(
            PodRec(
                name=name,
                chips_per_replica=CHIPS_PER_NODE,
                chip_type=chip,
                priority=pri + rng.randint(-2, 2),
                quota=quota,
                cluster_hint=fabric,
                statuses=[Replica() for _ in range(reps)],
            )
        )
        names.append(name)
    return names


def add_research_long_tail(state: State) -> list[str]:
    """O(80) small research jobs, sampled from a fixed user pool.

    Sizes: 1–32 GPUs, power-law (most are 1–4 GPU, a few are 16–32).
    Quotas: ``research`` for most, ``interp`` for a subset.
    """
    rng = state.rng
    names: list[str] = []
    n = 90
    # Skew users: a few power users own more jobs.
    user_weights = [4 if i < 5 else 2 if i < 12 else 1 for i in range(len(RESEARCH_USERS))]
    for i in range(1, n + 1):
        user = rng.choices(RESEARCH_USERS, weights=user_weights, k=1)[0]
        slug = rng.choice(RESEARCH_SLUGS)
        # Power-law-ish size: 70% small (1–4 chips), 25% mid (8 chips), 5% big (16–32).
        roll = rng.random()
        if roll < 0.70:
            chips_per = 1
            reps = rng.randint(1, 4)
        elif roll < 0.95:
            chips_per = CHIPS_PER_NODE
            reps = 1
        else:
            chips_per = CHIPS_PER_NODE
            reps = rng.choice([2, 4])
        # Most research goes to fabric-3 (H100, cheaper); some to fabric-1.
        # Interp jobs are biased to fabric-3.
        is_interp = rng.random() < 0.20
        quota = "interp" if is_interp else "research"
        if is_interp:
            fabric = "train-fabric-3"
            chip = "H100"
        else:
            fabric = rng.choices(
                ["train-fabric-3", "train-fabric-1", "train-fabric-2"],
                weights=[6, 2, 2],
            )[0]
            chip = "H100" if fabric == "train-fabric-3" else "H200"
        name = f"exp-{user}-{slug}-r{i:03d}"
        pri = PRIORITY["research_high"] if rng.random() < 0.25 else PRIORITY["research_low"]
        state.add_pod(
            PodRec(
                name=name,
                chips_per_replica=chips_per,
                chip_type=chip,
                priority=pri,
                quota=quota,
                cluster_hint=fabric,
                statuses=[Replica() for _ in range(reps)],
            )
        )
        names.append(name)
    return names


def add_partners(state: State) -> list[str]:
    """A small bounded slice of partner workloads, anonymised."""
    pri = PRIORITY["partners"]
    names: list[str] = []
    specs = [
        ("partner-001-finetune", "H200", "train-fabric-2", 4),  # 32 GPU
        ("partner-002-finetune", "H200", "train-fabric-2", 8),  # 64 GPU
    ]
    for name, chip, fabric, reps in specs:
        state.add_pod(
            PodRec(
                name=name,
                chips_per_replica=CHIPS_PER_NODE,
                chip_type=chip,
                priority=pri,
                quota="partners",
                cluster_hint=fabric,
                statuses=[Replica() for _ in range(reps)],
            )
        )
        names.append(name)
    return names


def add_oncall_hot_spare(state: State) -> list[str]:
    """A tiny standing reserve so oncall always has a hot spare."""
    pri = PRIORITY["oncall"]
    name = "oncall-hot-spare"
    state.add_pod(
        PodRec(
            name=name,
            chips_per_replica=CHIPS_PER_NODE,
            chip_type="H200",
            priority=pri,
            quota="oncall",
            cluster_hint="train-fabric-1",
            statuses=[Replica()],  # 1 node
        )
    )
    return [name]


# Inference deployments: 4 classes per region, replica counts skewed heavy
# on flagship.  Total ~22k inference replicas across all regions.
#
# Region chip type is determined by the cluster's chip mix (each region is
# single-chip-type in this fixture).  Batch deployments are L40S-only.
INFERENCE_PROFILE: dict[str, dict[str, int]] = {
    # region -> {deployment_class: replica_count}
    "inf-region-1": {"flagship": 5500, "mini": 2200, "embedding": 1500},  # H100
    "inf-region-2": {"flagship": 4500, "mini": 1800, "embedding": 1200},  # H100
    "inf-region-3": {"flagship": 3000, "mini": 1200, "embedding": 800},  # A100
    "inf-region-4": {"flagship": 2200, "mini": 900, "embedding": 600},  # A100
    "inf-region-5": {"batch": 4500},  # L40S
    "inf-region-6": {"batch": 3000},  # L40S
}


def add_inference(state: State) -> list[str]:
    names: list[str] = []
    region_chip = {
        cluster: parts[0][0] for cluster, parts in CLUSTER_SHAPES if cluster.startswith("inf-")
    }
    for region, classes in INFERENCE_PROFILE.items():
        chip = region_chip[region]
        for klass, count in classes.items():
            quota = "serving-batch" if klass == "batch" else "serving"
            pri = PRIORITY["inference_batch"] if klass == "batch" else PRIORITY["inference_serving"]
            for i in range(1, count + 1):
                name = f"serve-{klass}-{region}-{i:04d}"
                state.add_pod(
                    PodRec(
                        name=name,
                        chips_per_replica=1,
                        chip_type=chip,
                        priority=pri,
                        quota=quota,
                        cluster_hint=region,
                        statuses=[Replica()],
                    )
                )
                names.append(name)
    return names


def initial_placement(state: State) -> None:
    """Place training-shaped pods (full-node) first, then inference."""
    train_pods = [p for p in state.pods.values() if p.chips_per_replica == CHIPS_PER_NODE]
    train_pods.sort(key=lambda p: -p.priority)
    for p in train_pods:
        state.place_pod(p)

    inf_pods = [p for p in state.pods.values() if p.chips_per_replica == 1]
    inf_pods.sort(key=lambda p: -p.priority)
    for p in inf_pods:
        state.place_pod(p)
        # Pods that don't fit stay queued (running, no node).


def count_full_free_nodes(state: State, fabric: str, chip_type: str) -> int:
    return sum(
        1
        for n in state.nodes_by_cluster_chip.get((fabric, chip_type), [])
        if n not in state.failed_nodes and state.free[n] == CHIPS_PER_NODE
    )


# Quotas eligible for preemption when admitting a higher-priority arrival.
# Excludes pretraining and oncall (always preserved).
PREEMPTIBLE_QUOTAS = ("research", "interp", "partners", "safety-evals", "posttraining")


def reclaim_for_full_nodes(
    state: State, want_full_nodes: int, fabric: str, chip_type: str
) -> list[str]:
    """Suspend lowest-priority eligible pods on (fabric, chip_type) until at
    least ``want_full_nodes`` fully-empty nodes are available.  Suspends
    pods one at a time in priority order, recomputing fully-empty count
    each step so we don't over-preempt.

    Returns the names of suspended pods, in suspension order.
    """
    suspended: list[str] = []
    while count_full_free_nodes(state, fabric, chip_type) < want_full_nodes:
        candidates = [
            p
            for p in state.pods.values()
            if p.cluster_hint == fabric
            and p.chip_type == chip_type
            and p.quota in PREEMPTIBLE_QUOTAS
            and any(r.node is not None for r in p.statuses)
        ]
        if not candidates:
            break
        candidates.sort(key=lambda p: (p.priority, p.name))
        victim = candidates[0]
        state.unplace_pod(victim, "suspended")
        suspended.append(victim.name)
    return suspended


def restore_pods(state: State, names: list[str]) -> None:
    for name in names:
        pod = state.pods.get(name)
        if pod is None:
            continue
        # Reset suspended -> running (no node yet) and try to place.
        for r in pod.statuses:
            if r.phase == "suspended":
                r.phase = "running"
                r.node = None
        state.place_pod(pod)


def main() -> None:
    rng = random.Random(RNG_SEED)
    assert_quota_invariant()

    state = State(rng=rng, nodes=build_nodes())

    # Build the workload taxonomy.
    add_oncall_hot_spare(state)
    add_pretraining(state)
    add_posttraining(state)
    add_evals(state)
    research_names = add_research_long_tail(state)
    add_partners(state)
    add_inference(state)

    initial_placement(state)

    frames: list[dict] = []

    # Frame 0: steady state.
    frames.append(state.snapshot(seq=1, ts=START, reason="steady_state"))

    # Frame 1: rack failure on train-fabric-1 (8 contiguous nodes — one
    # 8-GPU rack sharing a PSU and ToR).
    rack_nodes = [f"train-fabric-1-node-{i:04d}" for i in range(64, 72)]
    affected: list[str] = []
    for n in rack_nodes:
        affected.extend(state.fail_node(n))
    affected = sorted(set(affected))
    frames.append(state.snapshot(seq=2, ts=START + FRAME_INTERVAL, reason="rack_failure"))

    # Frame 2: failed replicas re-placed within fabric-1's slack.  If slack
    # is tight, some research jobs get suspended to make room.
    state.replace_failed(affected)
    frames.append(state.snapshot(seq=3, ts=START + 2 * FRAME_INTERVAL, reason="recovery"))

    # Frame 3: a new posttraining job arrives queued (256 GPU rlhf on
    # fabric-1, H200).
    arrival = PodRec(
        name="posttrain-rlhf-2026-05-01-incident",
        chips_per_replica=CHIPS_PER_NODE,
        chip_type="H200",
        priority=PRIORITY["posttraining"] + 5,  # slightly elevated to force admit
        quota="posttraining",
        cluster_hint="train-fabric-1",
        statuses=[Replica() for _ in range(32)],  # 256 H200
    )
    state.add_pod(arrival)
    frames.append(
        state.snapshot(seq=4, ts=START + 3 * FRAME_INTERVAL, reason="posttraining_queued")
    )

    # Frame 4: solver suspends lowest-priority eligible pods on fabric-1
    # H200 to free room, then admits the arrival.  Eligibility excludes
    # pretraining and oncall.  In practice this pulls in research first,
    # then the lowest-priority posttraining pods if research alone isn't
    # enough.
    suspended_research = reclaim_for_full_nodes(state, 32, "train-fabric-1", "H200")
    state.place_pod(arrival)
    frames.append(state.snapshot(seq=5, ts=START + 4 * FRAME_INTERVAL, reason="reclaim_and_admit"))

    # Frame 5: stable while the arrival runs.
    frames.append(state.snapshot(seq=6, ts=START + 5 * FRAME_INTERVAL, reason="running"))

    # Frame 6: arrival completes; suspended research resumes.
    for r in arrival.statuses:
        if r.node:
            state.release(r.node, arrival.chips_per_replica)
        r.node = None
        r.phase = "completed"
    restore_pods(state, suspended_research)
    frames.append(
        state.snapshot(seq=7, ts=START + 6 * FRAME_INTERVAL, reason="completion_and_resume")
    )

    # Frame 7: rack repaired; arrival pruned; steady state restored.
    state.repair_nodes(rack_nodes)
    state.pods.pop(arrival.name, None)
    frames.append(state.snapshot(seq=8, ts=START + 7 * FRAME_INTERVAL, reason="steady_state"))

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w") as f:
        for frame in frames:
            f.write(json.dumps(frame, separators=(",", ":")))
            f.write("\n")

    size_mb = OUTPUT.stat().st_size / 1024 / 1024
    print(f"wrote {OUTPUT.name}: {len(frames)} frames, {size_mb:.1f} MB")
    print(f"  pods: {len(state.pods)}")
    print(f"  gang_sets: {len(state.gang_sets)}")

    # Suppress unused-name warnings (these are diagnostic handles).
    _ = research_names


if __name__ == "__main__":
    main()
