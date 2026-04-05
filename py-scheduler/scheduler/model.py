"""
Data model for multi-cluster GPU scheduling.

The scheduler is stateless: it receives the current world state, makes
placement decisions, and returns. It has no persistent memory between
invocations.

The scheduler operates at the bin-packing level: it sees individual nodes
per cluster and decides replica-to-node assignments.

A Pod is the scheduling unit: a set of identical replicas that run on a
single cluster. A Pod always gang-schedules its own replicas (all placed
or none). Gang sets link *multiple* Pods for cross-pod atomic scheduling
(e.g. cross-cluster training jobs) — the all-or-nothing semantics come
from gang_sets, not from the replica count. All pods in a gang set must
share the same priority and quota; the solver validates this at entry.

Deployments are modelled as individual single-replica Pods (one per k8s
pod), each independently schedulable and reclaimable. No separate
Deployment type exists — the distinction lives entirely in how the
binder constructs Pods and whether they share a gang set.

Key invariant: once a Pod starts running on a cluster, it cannot move
to a different cluster. Suspended pods remember their cluster binding.

Pod lifecycle (from the scheduler's perspective):
    pending -> running -> completed/failed
                 |  ^  ^
                 v  |  |
             suspended |
                       |
           (replica dies, restart policy applies -> failed replica -> re-placed)

Terminal pods (completed or permanently failed) are a Kubernetes concern,
not a scheduler concern. The binder filters them out before building the
solver request. Gang sets are likewise pruned of any removed pods. In
general, solver inputs are kept minimal: only pods that need a scheduling
decision appear.

The scheduler derives what needs action from replica statuses — no
separate queue is needed.

All types are JSON-serializable. The Python model.py is the source of
truth; the Rust solver_types.rs must stay in sync.
"""

import json
from dataclasses import dataclass
from enum import StrEnum


@dataclass(frozen=True)
class Node:
    name: str
    chip_type: str
    chips: int  # total GPUs/chips on this node (e.g. 8)


class Phase(StrEnum):
    # NOTE: These phases do not map 1:1 to Kubernetes pod phases. They are
    # an abstraction that exposes only the distinctions the solver needs to
    # make decisions. For example, a k8s "Pending" pod that is already bound
    # to a node is indistinguishable from "Running" here: both are occupying
    # capacity. The binder is responsible for translating k8s state into
    # these solver-facing phases, including two transient windows:
    #
    #  - Terminating: spec.suspend=true patched but pods not yet gone.
    #    The binder reports these as RUNNING (with their node) so the solver
    #    keeps the nodes occupied until k8s confirms deletion.
    #
    #  - Pending placement: job created/unsuspended but pods not yet visible
    #    in the reflector.  The binder injects the known node assignments from
    #    the previous cycle so the solver does not re-assign those nodes.
    #
    # No new Phase value is needed — both cases are correctly represented as
    # RUNNING with a node assignment.
    RUNNING = "running"
    FAILED = "failed"
    SUSPENDED = "suspended"
    COMPLETED = "completed"


@dataclass(frozen=True)
class PodReplicaStatus:
    phase: Phase
    node: str | None = None  # assigned node, None if unplaced


@dataclass(frozen=True)
class Pod:
    """Scheduling unit: a set of identical replicas on a single cluster.

    Each replica requires ``chips_per_replica`` chips of ``chip_type``.
    ``cluster`` is None until the pod is first placed; once set, the pod
    is bound to that cluster for its lifetime.

    The binder constructs these from k8s state, grouping k8s pods by their
    ``job-name`` label. How different k8s workloads map to Pods:

    * **Jobs** — one Pod per Job, one replica per k8s pod. All replicas
      are gang-scheduled (placed together or not at all).
    * **Deployments** — one Pod *per k8s pod* (single replica each),
      independently schedulable and reclaimable. The external autoscaler
      scales by creating or removing these single-replica Pods. Because
      they are not gang-scheduled, the solver can reclaim any individual
      deployment pod without all-or-nothing cascades.
    * **Cross-cluster training** — separate Pods (one per cluster) linked
      via ``gang_sets`` on the SolverRequest for atomic co-scheduling.

    Replica lifecycle is tracked via ``PodReplicaStatus.phase``. When a
    replica fails and the restart policy allows, it appears as
    ``phase=failed`` and the solver re-places it on a suitable node.
    When a Pod's k8s Job completes or its restart policy is exhausted
    the binder removes it from the solver input entirely.
    """

    chips_per_replica: int
    chip_type: str
    priority: int  # higher = more important
    quota: str
    cluster: str | None  # assigned cluster, None if not yet placed
    statuses_by_replica: list[PodReplicaStatus]


@dataclass(frozen=True)
class Quota:
    name: str
    # cluster_name -> {chip_type -> guaranteed chip count}
    guarantees: dict[str, dict[str, int]]


@dataclass(frozen=True)
class ClusterState:
    name: str
    nodes: list[Node]


@dataclass(frozen=True)
class ScheduleResult:
    """Solver output: updated pods reflecting all scheduling decisions.

    Each pod carries the desired state after the solver runs — new node
    assignments, phase changes (e.g. suspended), or no change. The dict
    is ordered: pods still waiting for resources (pending replicas with
    no node) must appear in queue-priority order.

    The binder diffs this against its input to determine actions: bind
    new replicas to nodes, suspend or unsuspend pods, etc.
    """

    pods: dict[str, Pod]
    solver_status: str


@dataclass(frozen=True)
class SolverRequest:
    clusters: list[ClusterState]
    pods: dict[str, Pod]
    gang_sets: list[list[str]]
    quotas: list[Quota]
    time_limit: float = 30.0


def load_session(path: str):
    """Yield SolverRequest objects from a JSONL session file.

    Each line in the file is one JSON-serialized SolverRequest, as written
    by the Rust binder when run with ``--record <path>``.
    """
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield solver_request_from_json(line)


def solver_request_from_json(s: str) -> SolverRequest:
    """Deserialize a SolverRequest from a JSON string."""
    d = json.loads(s)
    return SolverRequest(
        clusters=[
            ClusterState(
                name=c["name"],
                nodes=[Node(**n) for n in c["nodes"]],
            )
            for c in d["clusters"]
        ],
        pods={
            k: Pod(
                chips_per_replica=v["chips_per_replica"],
                chip_type=v["chip_type"],
                priority=v["priority"],
                quota=v["quota"],
                cluster=v.get("cluster"),
                statuses_by_replica=[
                    PodReplicaStatus(phase=Phase(r["phase"]), node=r.get("node"))
                    for r in v["statuses_by_replica"]
                ],
            )
            for k, v in d["pods"].items()
        },
        gang_sets=d.get("gang_sets", []),
        quotas=[Quota(**q) for q in d["quotas"]],
        time_limit=d.get("time_limit", 30.0),
    )
