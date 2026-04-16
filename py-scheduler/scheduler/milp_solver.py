"""
MILP scheduler built with Pyomo and HiGHS.

The model chooses which pending pods to start, which running pods to
keep, and which suspended pods to unsuspend, with explicit replica-to-node
placements. Running pods can only stay on their current nodes, so active
suspension is modeled as an all-or-nothing keep decision.

This solver accepts the same interface as the mock heuristic solver
(see solver.py) and returns a ScheduleResult with updated pod statuses.
"""

from collections import defaultdict
from dataclasses import dataclass, replace
from time import monotonic
from typing import Sequence

import pyomo.environ as pyo
from pyomo.opt import SolverResults

from scheduler.model import (
    ClusterState,
    Node,
    Phase,
    Pod,
    PodReplicaStatus,
    Quota,
    ScheduleResult,
)

DEFAULT_SOLVER = "highs"


@dataclass(frozen=True)
class LexicographicScore:
    """Solver-internal score tracking for lexicographic MILP stages."""

    quota: float
    priority: dict[int, float]
    thrash: float
    tie_break: dict[str, float]


def solve(
    clusters: Sequence[ClusterState],
    pods: dict[str, Pod],
    gang_sets: Sequence[Sequence[str]],
    quotas: Sequence[Quota],
    *,
    solver: str | None = None,
    presolve: bool = False,
    time_limit: float = 30.0,
    verbose: bool = False,
) -> ScheduleResult:
    clusters = list(clusters)

    # Validate quotas.
    quota_names = {quota.name for quota in quotas}
    for name, pod in pods.items():
        if pod.quota not in quota_names:
            raise ValueError(f"pod {name!r} references unknown quota {pod.quota!r}")

    # Categorize pods into pending, running, and suspended.
    pending_pods: dict[str, Pod] = {}
    running_pods: dict[str, tuple[Pod, str, dict[str, int]]] = {}
    suspended_pods: dict[str, tuple[Pod, str]] = {}
    passthrough_pods: dict[str, Pod] = {}

    for name, pod in pods.items():
        if pod.cluster is None and any(
            rs.phase == Phase.RUNNING and rs.node is None for rs in pod.statuses_by_replica
        ):
            pending_pods[name] = pod
        elif pod.cluster and any(rs.phase == Phase.SUSPENDED for rs in pod.statuses_by_replica):
            suspended_pods[name] = (pod, pod.cluster)
        elif pod.cluster and any(
            rs.phase == Phase.RUNNING and rs.node is not None for rs in pod.statuses_by_replica
        ):
            placement: dict[str, int] = {}
            for rs in pod.statuses_by_replica:
                if rs.phase == Phase.RUNNING and rs.node:
                    placement[rs.node] = placement.get(rs.node, 0) + 1
            running_pods[name] = (pod, pod.cluster, placement)
        else:
            passthrough_pods[name] = pod

    if not pending_pods and not suspended_pods:
        return ScheduleResult(pods=dict(pods), solver_status="empty")

    # Compute chips consumed by passthrough pods (failed, completed, etc.)
    # that still occupy nodes. These are not part of the optimisation but
    # reduce available capacity.
    passthrough_usage: dict[str, int] = defaultdict(int)
    for pod in passthrough_pods.values():
        for rs in pod.statuses_by_replica:
            if rs.node:
                passthrough_usage[rs.node] += pod.chips_per_replica

    quota_lookup = {quota.name: quota for quota in quotas}
    guarantee_by_quota_pool = _build_quota_guarantees(quotas)
    node_capacity = _build_node_capacity(clusters)
    node_free = _build_node_free(clusters, pods)

    # Index pods for MILP variable construction.
    pending_names = list(pending_pods.keys())
    pending_by_index: dict[int, tuple[str, Pod]] = {
        i: (name, pending_pods[name]) for i, name in enumerate(pending_names)
    }
    suspended_names = list(suspended_pods.keys())
    suspended_by_index: dict[int, tuple[str, Pod, str]] = {
        i: (name, *suspended_pods[name]) for i, name in enumerate(suspended_names)
    }
    running_names = list(running_pods.keys())
    running_by_index: dict[int, tuple[str, Pod, str, dict[str, int]]] = {
        i: (name, *running_pods[name]) for i, name in enumerate(running_names)
    }

    # Precompute structurally valid placement variables so the model
    # never reasons about impossible pod/cluster/node combinations.
    queue_cluster_keys: list[tuple[int, str]] = []
    queue_clusters_by_pod: dict[int, tuple[str, ...]] = {}
    queue_nodes_by_cluster: dict[tuple[int, str], tuple[str, ...]] = {}
    queue_nodes_by_pod: dict[int, tuple[str, ...]] = {}
    queue_placement_keys: list[tuple[int, str]] = []
    queue_placement_ub: dict[tuple[int, str], int] = {}

    suspend_nodes_by_pod: dict[int, tuple[str, ...]] = {}
    suspend_placement_keys: list[tuple[int, str]] = []
    suspend_placement_ub: dict[tuple[int, str], int] = {}

    running_usage_by_node: dict[str, list[tuple[int, int]]] = defaultdict(list)
    running_usage_by_quota_pool: dict[tuple[str, str, str], list[tuple[int, int]]] = defaultdict(
        list
    )
    running_chip_usage_by_pod: dict[int, int] = {}

    placements_by_node_queue: dict[str, list[int]] = defaultdict(list)
    placements_by_node_suspend: dict[str, list[int]] = defaultdict(list)
    placements_by_quota_pool_queue: dict[tuple[str, str, str], list[tuple[int, str]]] = defaultdict(
        list
    )
    placements_by_quota_pool_suspend: dict[tuple[str, str, str], list[tuple[int, str]]] = (
        defaultdict(list)
    )

    for pod_index, (_, pod) in pending_by_index.items():
        num_replicas = _num_replicas(pod)
        feasible_clusters: list[str] = []
        feasible_nodes: list[str] = []
        for cluster in clusters:
            free_nodes = _candidate_nodes(pod, cluster.nodes, node_free)
            if (
                _max_placeable_replicas(free_nodes, node_free, pod.chips_per_replica)
                >= num_replicas
            ):
                cluster_nodes = free_nodes
                placement_capacity = node_free
            else:
                cluster_nodes = _candidate_nodes(pod, cluster.nodes, node_capacity)
                placement_capacity = node_capacity
            if (
                not cluster_nodes
                or _max_placeable_replicas(
                    cluster_nodes,
                    placement_capacity,
                    pod.chips_per_replica,
                )
                < num_replicas
            ):
                continue
            quota_pool = (pod.quota, cluster.name, pod.chip_type)
            feasible_clusters.append(cluster.name)
            queue_cluster_keys.append((pod_index, cluster.name))
            queue_nodes_by_cluster[(pod_index, cluster.name)] = cluster_nodes
            feasible_nodes.extend(cluster_nodes)
            for node_name in cluster_nodes:
                queue_placement_keys.append((pod_index, node_name))
                queue_placement_ub[(pod_index, node_name)] = (
                    placement_capacity[node_name] // pod.chips_per_replica
                )
                placements_by_node_queue[node_name].append(pod_index)
                placements_by_quota_pool_queue[quota_pool].append((pod_index, node_name))
        queue_clusters_by_pod[pod_index] = tuple(feasible_clusters)
        queue_nodes_by_pod[pod_index] = tuple(feasible_nodes)

    running_pod_keys = tuple(running_by_index)
    for pod_index, (_, pod, cluster_name, node_placement) in running_by_index.items():
        total_usage = _placed_pod_chips(pod, node_placement)
        running_chip_usage_by_pod[pod_index] = total_usage
        quota_pool = (pod.quota, cluster_name, pod.chip_type)
        running_usage_by_quota_pool[quota_pool].append((pod_index, total_usage))
        # For capacity constraints, count ALL replicas with node assignments
        # (not just RUNNING ones). Non-RUNNING replicas (e.g. FAILED) still
        # consume physical node capacity and are freed when the pod is suspended.
        capacity_placement: dict[str, int] = {}
        for rs in pod.statuses_by_replica:
            if rs.node:
                capacity_placement[rs.node] = capacity_placement.get(rs.node, 0) + 1
        for node_name, replica_count in capacity_placement.items():
            running_usage_by_node[node_name].append(
                (pod_index, replica_count * pod.chips_per_replica)
            )

    suspend_pod_keys = tuple(suspended_by_index)
    cluster_lookup = {cluster.name: cluster for cluster in clusters}
    for pod_index, (_, pod, cluster_name) in suspended_by_index.items():
        cluster = cluster_lookup[cluster_name]
        free_nodes = _candidate_nodes(pod, cluster.nodes, node_free)
        num_replicas = _num_replicas(pod)
        if _max_placeable_replicas(free_nodes, node_free, pod.chips_per_replica) >= num_replicas:
            feasible_nodes_list = free_nodes
            placement_capacity = node_free
        else:
            feasible_nodes_list = _candidate_nodes(pod, cluster.nodes, node_capacity)
            placement_capacity = node_capacity
        quota_pool = (pod.quota, cluster_name, pod.chip_type)
        suspend_nodes_by_pod[pod_index] = feasible_nodes_list
        for node_name in feasible_nodes_list:
            suspend_placement_keys.append((pod_index, node_name))
            suspend_placement_ub[(pod_index, node_name)] = (
                placement_capacity[node_name] // pod.chips_per_replica
            )
            placements_by_node_suspend[node_name].append(pod_index)
            placements_by_quota_pool_suspend[quota_pool].append((pod_index, node_name))

    # -----------------------------------------------------------------------
    # Build Pyomo model
    # -----------------------------------------------------------------------
    model = pyo.ConcreteModel()
    model.queue_cluster_index = pyo.Set(dimen=2, initialize=tuple(queue_cluster_keys))
    model.queue_placement_index = pyo.Set(dimen=2, initialize=tuple(queue_placement_keys))
    model.running_pod_index = pyo.Set(initialize=running_pod_keys)
    model.suspend_pod_index = pyo.Set(initialize=suspend_pod_keys)
    model.suspend_placement_index = pyo.Set(dimen=2, initialize=tuple(suspend_placement_keys))

    model.start = pyo.Var(model.queue_cluster_index, domain=pyo.Binary)
    model.keep_running = pyo.Var(model.running_pod_index, domain=pyo.Binary)
    model.unsuspend = pyo.Var(model.suspend_pod_index, domain=pyo.Binary)

    def queue_bounds(_model: pyo.ConcreteModel, pod_index: int, node_name: str):
        return 0, queue_placement_ub[(pod_index, node_name)]

    def suspend_bounds(_model: pyo.ConcreteModel, pod_index: int, node_name: str):
        return 0, suspend_placement_ub[(pod_index, node_name)]

    model.queue_place = pyo.Var(
        model.queue_placement_index,
        domain=pyo.NonNegativeIntegers,
        bounds=queue_bounds,
    )
    model.unsuspend_place = pyo.Var(
        model.suspend_placement_index,
        domain=pyo.NonNegativeIntegers,
        bounds=suspend_bounds,
    )
    model.quota_guarantee_index = pyo.Set(
        dimen=3, initialize=tuple(sorted(guarantee_by_quota_pool))
    )

    def guaranteed_chip_bounds(
        _model: pyo.ConcreteModel,
        quota_name: str,
        cluster_name: str,
        chip_type: str,
    ):
        return 0, guarantee_by_quota_pool[(quota_name, cluster_name, chip_type)]

    model.guaranteed_chips = pyo.Var(
        model.quota_guarantee_index,
        domain=pyo.NonNegativeIntegers,
        bounds=guaranteed_chip_bounds,
    )

    # Warm-start hints.
    for pod_index, cluster_name in model.queue_cluster_index:
        model.start[pod_index, cluster_name].value = 0
    for pod_index in model.running_pod_index:
        model.keep_running[pod_index].value = 1
    for pod_index in model.suspend_pod_index:
        model.unsuspend[pod_index].value = 0
    for pod_index, node_name in model.queue_placement_index:
        model.queue_place[pod_index, node_name].value = 0
    for pod_index, node_name in model.suspend_placement_index:
        model.unsuspend_place[pod_index, node_name].value = 0
    for quota_pool in model.quota_guarantee_index:
        running_usage = sum(chip_usage for _, chip_usage in running_usage_by_quota_pool[quota_pool])
        model.guaranteed_chips[quota_pool].value = min(
            guarantee_by_quota_pool[quota_pool],
            running_usage,
        )

    # -----------------------------------------------------------------------
    # Constraints
    # -----------------------------------------------------------------------

    # Cluster choice: start a pending pod on at most one cluster.
    model.cluster_choice = pyo.ConstraintList()
    model.gang_queue = pyo.ConstraintList()
    for pod_index, (_, pod) in pending_by_index.items():
        num_replicas = _num_replicas(pod)
        feasible_clusters = queue_clusters_by_pod[pod_index]
        if feasible_clusters:
            model.cluster_choice.add(
                pyo.quicksum(
                    model.start[pod_index, cluster_name] for cluster_name in feasible_clusters
                )
                <= 1
            )
        for cluster_name in feasible_clusters:
            model.gang_queue.add(
                pyo.quicksum(
                    model.queue_place[pod_index, node_name]
                    for node_name in queue_nodes_by_cluster[(pod_index, cluster_name)]
                )
                == num_replicas * model.start[pod_index, cluster_name]
            )

    # Unsuspend: all-or-nothing, cluster is fixed.
    model.gang_unsuspend = pyo.ConstraintList()
    for pod_index, (_, pod, _) in suspended_by_index.items():
        num_replicas = _num_replicas(pod)
        model.gang_unsuspend.add(
            pyo.quicksum(
                model.unsuspend_place[pod_index, node_name]
                for node_name in suspend_nodes_by_pod[pod_index]
            )
            == num_replicas * model.unsuspend[pod_index]
        )

    # Node capacity.
    model.node_capacity_constraints = pyo.ConstraintList()
    for node_name, total_chips in node_capacity.items():
        available_chips = total_chips - passthrough_usage.get(node_name, 0)
        if (
            not running_usage_by_node[node_name]
            and not placements_by_node_queue[node_name]
            and not placements_by_node_suspend[node_name]
        ):
            continue
        running_terms = (
            chip_usage * model.keep_running[pod_index]
            for pod_index, chip_usage in running_usage_by_node[node_name]
        )
        if not placements_by_node_queue[node_name] and not placements_by_node_suspend[node_name]:
            total_usage = pyo.quicksum(running_terms)
            model.node_capacity_constraints.add(total_usage <= available_chips)
            continue
        queue_terms = (
            model.queue_place[pod_index, node_name]
            * pending_by_index[pod_index][1].chips_per_replica
            for pod_index in placements_by_node_queue[node_name]
        )
        suspend_terms = (
            model.unsuspend_place[pod_index, node_name]
            * suspended_by_index[pod_index][1].chips_per_replica
            for pod_index in placements_by_node_suspend[node_name]
        )
        total_usage = (
            pyo.quicksum(running_terms) + pyo.quicksum(queue_terms) + pyo.quicksum(suspend_terms)
        )
        model.node_capacity_constraints.add(total_usage <= available_chips)

    # Quota guarantee coverage.
    model.quota_guarantee_coverage = pyo.ConstraintList()
    for quota_pool in sorted(guarantee_by_quota_pool):
        running_terms = (
            chip_usage * model.keep_running[pod_index]
            for pod_index, chip_usage in running_usage_by_quota_pool[quota_pool]
        )
        queue_terms = (
            model.queue_place[pod_index, node_name]
            * pending_by_index[pod_index][1].chips_per_replica
            for pod_index, node_name in placements_by_quota_pool_queue[quota_pool]
        )
        suspend_terms = (
            model.unsuspend_place[pod_index, node_name]
            * suspended_by_index[pod_index][1].chips_per_replica
            for pod_index, node_name in placements_by_quota_pool_suspend[quota_pool]
        )
        total_usage = (
            pyo.quicksum(running_terms) + pyo.quicksum(queue_terms) + pyo.quicksum(suspend_terms)
        )
        model.quota_guarantee_coverage.add(model.guaranteed_chips[quota_pool] <= total_usage)

    # Gang set constraints: all pods in a gang must have the same schedule decision.
    schedule_expr_by_name: dict[str, pyo.Expression | pyo.Var | int] = {}
    for pod_index, (name, _) in pending_by_index.items():
        feasible_clusters = queue_clusters_by_pod[pod_index]
        if feasible_clusters:
            schedule_expr_by_name[name] = pyo.quicksum(
                model.start[pod_index, cluster_name] for cluster_name in feasible_clusters
            )
        else:
            schedule_expr_by_name[name] = 0
    for pod_index, (name, _, _) in suspended_by_index.items():
        schedule_expr_by_name[name] = model.unsuspend[pod_index]
    for pod_index, (name, _, _, _) in running_by_index.items():
        schedule_expr_by_name[name] = model.keep_running[pod_index]

    model.gang_set_constraints = pyo.ConstraintList()
    for gang in gang_sets:
        members = [name for name in gang if name in schedule_expr_by_name]
        if len(members) <= 1:
            continue
        first = members[0]
        for other in members[1:]:
            constraint_expr = schedule_expr_by_name[first] == schedule_expr_by_name[other]
            if constraint_expr is True:
                continue
            model.gang_set_constraints.add(constraint_expr)

    # -----------------------------------------------------------------------
    # Objective: lexicographic scoring
    # -----------------------------------------------------------------------
    priority_demand_by_index: dict[tuple[str, int], int] = {}
    for pod_index, (_, pod) in pending_by_index.items():
        priority_demand_by_index[("queue", pod_index)] = _pod_demand(pod)
    for pod_index, (_, pod, _) in suspended_by_index.items():
        priority_demand_by_index[("suspend", pod_index)] = _pod_demand(pod)
    for pod_index in running_by_index:
        priority_demand_by_index[("running", pod_index)] = running_chip_usage_by_pod[pod_index]

    priority_terms_by_level: dict[int, list[pyo.Expression | pyo.Var]] = defaultdict(list)
    thrash_terms: list[pyo.Expression | pyo.Var] = []
    pod_count_terms: list[pyo.Expression | pyo.Var] = []
    quota_alignment_terms: list[pyo.Expression | pyo.Var | int] = []

    for pod_index, (_, pod) in pending_by_index.items():
        quota = quota_lookup.get(pod.quota)
        for cluster_name in queue_clusters_by_pod[pod_index]:
            priority_terms_by_level[pod.priority].append(
                priority_demand_by_index["queue", pod_index] * model.start[pod_index, cluster_name]
            )
            pod_count_terms.append(model.start[pod_index, cluster_name])
            quota_alignment_terms.append(
                _quota_bonus(quota, cluster_name, pod.chip_type)
                * model.start[pod_index, cluster_name]
            )
    for pod_index, (_, pod, cluster_name) in suspended_by_index.items():
        priority_terms_by_level[pod.priority].append(
            priority_demand_by_index["suspend", pod_index] * model.unsuspend[pod_index]
        )
        pod_count_terms.append(model.unsuspend[pod_index])
        quota_alignment_terms.append(
            _quota_bonus(quota_lookup.get(pod.quota), cluster_name, pod.chip_type)
            * model.unsuspend[pod_index]
        )
    for pod_index, (_, pod, _, _) in running_by_index.items():
        priority_terms_by_level[pod.priority].append(
            priority_demand_by_index["running", pod_index] * model.keep_running[pod_index]
        )
        thrash_terms.append(running_chip_usage_by_pod[pod_index] * model.keep_running[pod_index])

    priority_levels = tuple(sorted(priority_terms_by_level, reverse=True))
    model.priority_level_index = pyo.Set(initialize=priority_levels)
    model.quota_value = pyo.Expression(
        expr=pyo.quicksum(
            model.guaranteed_chips[quota_pool] for quota_pool in model.quota_guarantee_index
        )
    )
    model.priority_value = pyo.Expression(
        model.priority_level_index,
        rule=lambda _model, priority: pyo.quicksum(priority_terms_by_level[priority]),
    )
    model.thrash_value = pyo.Expression(expr=pyo.quicksum(thrash_terms))
    model.pod_count_value = pyo.Expression(expr=pyo.quicksum(pod_count_terms))
    model.quota_alignment_value = pyo.Expression(expr=pyo.quicksum(quota_alignment_terms))
    model.objective = pyo.Objective(expr=model.quota_alignment_value, sense=pyo.maximize)

    # -----------------------------------------------------------------------
    # Solve lexicographically
    # -----------------------------------------------------------------------
    solver_name = solver or DEFAULT_SOLVER
    optimizer = pyo.SolverFactory(solver_name)
    if not optimizer.available(False):
        raise ValueError(f"Solver '{solver_name}' is not available")
    _configure_solver(optimizer, solver_name, presolve=presolve, verbose=verbose)
    deadline = monotonic() + time_limit

    stage_specs: list[tuple[str, pyo.Expression, str | None]] = [
        ("quota", model.quota_value, "fixed_quota_value")
    ]
    stage_specs.extend(
        (
            f"priority_{priority}",
            model.priority_value[priority],
            f"fixed_priority_value_{priority}",
        )
        for priority in priority_levels
    )
    stage_specs.extend(
        [
            ("thrash", model.thrash_value, "fixed_thrash_value"),
            ("pod_count", model.pod_count_value, "fixed_pod_count_value"),
            ("quota_alignment", model.quota_alignment_value, None),
        ]
    )

    results: SolverResults | None = None
    for _, stage_expr, fix_name in stage_specs:
        remaining_time = max(deadline - monotonic(), 0.1)
        _set_time_limit(optimizer, remaining_time)
        model.objective.set_value(stage_expr)
        results = optimizer.solve(model, tee=verbose, load_solutions=False)
        if _has_solution(results):
            model.solutions.load_from(results)
        else:
            break
        if fix_name is None or not _is_optimal(results):
            break
        stage_value = _int_expr_value(stage_expr)
        model.add_component(
            fix_name,
            pyo.Constraint(expr=stage_expr == stage_value),
        )
    if results is None:
        raise RuntimeError("Solver did not run")

    solver_status = _solver_status(results)

    # -----------------------------------------------------------------------
    # Extract results → build output pods dict
    # -----------------------------------------------------------------------
    result_pods: dict[str, Pod] = {}

    # Passthrough pods (completed, failed, etc.).
    result_pods.update(passthrough_pods)

    # Running pods: keep or suspend.
    for pod_index, (name, pod, _, _) in running_by_index.items():
        if _value(model.keep_running[pod_index]) <= 0.5:
            result_pods[name] = replace(
                pod,
                statuses_by_replica=[
                    PodReplicaStatus(Phase.SUSPENDED) for _ in pod.statuses_by_replica
                ],
            )
        else:
            result_pods[name] = pod

    # Unsuspended pods.
    for pod_index, (name, pod, cluster_name) in suspended_by_index.items():
        if _value(model.unsuspend[pod_index]) > 0.5:
            statuses = _build_placement_statuses(
                pod, model.unsuspend_place, pod_index, suspend_nodes_by_pod[pod_index]
            )
            result_pods[name] = replace(pod, statuses_by_replica=statuses)
        else:
            result_pods[name] = pod

    # Started pending pods.
    waiting_names: list[str] = []
    for pod_index, (name, pod) in pending_by_index.items():
        selected_cluster = next(
            (
                cluster_name
                for cluster_name in queue_clusters_by_pod[pod_index]
                if _value(model.start[pod_index, cluster_name]) > 0.5
            ),
            None,
        )
        if selected_cluster is not None:
            statuses = _build_placement_statuses(
                pod, model.queue_place, pod_index, queue_nodes_by_pod[pod_index]
            )
            result_pods[name] = replace(pod, cluster=selected_cluster, statuses_by_replica=statuses)
        else:
            waiting_names.append(name)

    # Order: placed first, then waiting in priority order.
    waiting_names.sort(
        key=lambda n: (-pending_pods[n].priority, _pod_demand(pending_pods[n]), n),
    )
    for name in waiting_names:
        result_pods[name] = pending_pods[name]

    return ScheduleResult(pods=result_pods, solver_status=solver_status)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _num_replicas(pod: Pod) -> int:
    return len(pod.statuses_by_replica)


def _pod_demand(pod: Pod) -> int:
    return len(pod.statuses_by_replica) * pod.chips_per_replica


def _placed_pod_chips(pod: Pod, node_placement: dict[str, int]) -> int:
    return sum(node_placement.values()) * pod.chips_per_replica


def _build_node_capacity(clusters: Sequence[ClusterState]) -> dict[str, int]:
    return {node.name: node.chips for cluster in clusters for node in cluster.nodes}


def _build_node_free(clusters: Sequence[ClusterState], pods: dict[str, Pod]) -> dict[str, int]:
    node_free = _build_node_capacity(clusters)
    for pod in pods.values():
        for rs in pod.statuses_by_replica:
            if rs.node:
                node_free[rs.node] -= pod.chips_per_replica
    return node_free


def _build_node_to_cluster(clusters: Sequence[ClusterState]) -> dict[str, str]:
    return {node.name: cluster.name for cluster in clusters for node in cluster.nodes}


def _build_quota_guarantees(quotas: Sequence[Quota]) -> dict[tuple[str, str, str], int]:
    guarantees: dict[tuple[str, str, str], int] = {}
    for quota in quotas:
        for cluster_name, chip_guarantees in quota.guarantees.items():
            for chip_type, guaranteed_chips in chip_guarantees.items():
                if guaranteed_chips > 0:
                    guarantees[(quota.name, cluster_name, chip_type)] = guaranteed_chips
    return guarantees


def _candidate_nodes(
    pod: Pod,
    nodes: Sequence[Node],
    available_chips_by_node: dict[str, int],
) -> tuple[str, ...]:
    return tuple(
        node.name
        for node in nodes
        if node.chip_type == pod.chip_type
        and available_chips_by_node[node.name] >= pod.chips_per_replica
    )


def _max_placeable_replicas(
    node_names: Sequence[str],
    node_capacity: dict[str, int],
    chips_per_replica: int,
) -> int:
    return sum(node_capacity[node_name] // chips_per_replica for node_name in node_names)


def _quota_bonus(quota: Quota | None, cluster_name: str, chip_type: str) -> int:
    if quota is None:
        return 0
    return int(quota.guarantees.get(cluster_name, {}).get(chip_type, 0) > 0)


def _build_placement_statuses(
    pod: Pod,
    place_var: pyo.Var,
    pod_index: int,
    feasible_nodes: tuple[str, ...] | Sequence[str],
) -> list[PodReplicaStatus]:
    """Build replica statuses from MILP placement variables."""
    statuses: list[PodReplicaStatus] = []
    for node_name in feasible_nodes:
        count = _int_value(place_var[pod_index, node_name])
        for _ in range(count):
            statuses.append(PodReplicaStatus(Phase.RUNNING, node_name))
    return statuses


# ---------------------------------------------------------------------------
# Pyomo helpers
# ---------------------------------------------------------------------------


def _value(var: pyo.Var) -> float:
    return float(pyo.value(var, exception=False) or 0.0)


def _expr_value(expr: pyo.Expression | pyo.Var) -> float:
    return float(pyo.value(expr, exception=False) or 0.0)


def _int_expr_value(expr: pyo.Expression | pyo.Var) -> int:
    return int(round(_expr_value(expr)))


def _int_value(var: pyo.Var) -> int:
    return int(round(_value(var)))


def _set_time_limit(optimizer: object, time_limit: float) -> None:
    if hasattr(optimizer, "config") and "time_limit" in optimizer.config:
        optimizer.config.time_limit = time_limit
        return
    optimizer.options["time_limit"] = time_limit


def _configure_solver(
    optimizer: object,
    solver_name: str,
    *,
    presolve: bool,
    verbose: bool,
) -> None:
    if solver_name != "highs":
        return
    optimizer.options["presolve"] = "on" if presolve else "off"
    optimizer.options["output_flag"] = bool(verbose)
    optimizer.options["log_to_console"] = bool(verbose)


def _is_optimal(results: SolverResults) -> bool:
    return (
        getattr(results.solver, "termination_condition", None) == pyo.TerminationCondition.optimal
    )


def _has_solution(results: SolverResults) -> bool:
    return bool(getattr(results, "solution", ()))


def _solver_status(results: SolverResults) -> str:
    status = getattr(results.solver, "status", None)
    termination = getattr(results.solver, "termination_condition", None)
    if status is None and termination is None:
        return "unknown"
    if status is None:
        return str(termination)
    if termination is None:
        return str(status)
    return f"{status}/{termination}"
