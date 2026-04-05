"""
Mock solver: Kueue admission control + k8s default scheduler placement.

Models the two-phase scheduling pipeline used in production:

  Phase 1 — Admission (Kueue semantics):
    Each pod is gated on its quota before entering the cluster. Quotas
    provide a guaranteed chip allocation per (cluster, chip_type); any
    capacity beyond the sum of all guarantees is an unguaranteed pool
    that admitted pods may borrow from on a first-come basis. Admission
    checks both quota availability AND node-level feasibility (enough
    free nodes to actually place the replicas), mirroring Kueue's
    Topology-Aware Scheduling (TAS) checks.
    Pods are processed in descending priority order. Preemption rules:
      (a) Same quota: a higher-priority pod may suspend any lower-priority
          pod within the same quota.
      (b) Other quotas within guarantee: safe — cannot be evicted.
      (c) Other quotas borrowing beyond guarantee: borrowed capacity can
          be reclaimed. Victims are selected lowest-priority first, then
          higher-priority (all borrowers are eligible since borrowed
          capacity is not guaranteed).

  Gang scheduling:
    Gang sets define groups of pods that must be admitted, unsuspended,
    and preempted atomically — all or nothing. All pods in a gang set
    must share the same priority and quota (validated at entry). Pods
    not in any gang set are treated as singleton groups. Preempting any
    member of a gang suspends the entire gang.

    Gang cluster coordination: when admitting a gang group, the solver
    first attempts to place all members on the same cluster (tried in
    order of free capacity). If no single cluster can fit the entire
    gang (e.g. different chip types needed, or insufficient capacity),
    it falls back to independent per-pod cluster selection with atomic
    rollback on failure. Note: all replicas within a single Pod always
    land on the same cluster (Pod.cluster is a scalar); the cross-cluster
    dimension is between different Pods in a gang set.

  Phase 2 — Placement (k8s scheduler semantics):
    Admitted pods are assigned to specific nodes. Because Phase 1
    includes a node-level feasibility check, admitted pods are guaranteed
    to fit without requiring node-level eviction — placement is a simple
    best-fit-decreasing assignment:
      1. Sort admitted-but-unplaced pods by chips_per_replica descending,
         then priority descending.
      2. For each pod, assign replicas to nodes via best-fit-decreasing:
         prefer the node with the least free capacity that still fits.

  Cluster selection:
    Unbound pods are sent to the cluster with the most free chips of the
    requested type, breaking ties by cluster name (lexicographic).

  Quota borrowing scope:
    Borrowing is strictly per-cluster: unused guarantees on cluster-1
    can only be borrowed by workloads on cluster-1. There is no
    cross-cluster borrowing. This matches Kueue's model where cohort
    borrowing pools are scoped to a single cluster.

Complexity: O(P * C * N) where P = pods, C = clusters, N = nodes per
cluster.
"""

from dataclasses import replace
from typing import NamedTuple, Sequence

from scheduler.model import (
    ClusterState,
    Node,
    Phase,
    Pod,
    PodReplicaStatus,
    Quota,
    ScheduleResult,
)

# Nested dict shorthand: quota/cluster/chip_type -> int.
UsageMap = dict[str, dict[str, dict[str, int]]]
ChipTotals = dict[str, dict[str, int]]


class QuotaCtx(NamedTuple):
    guarantee: UsageMap  # quota -> cluster -> chip_type -> guaranteed chips
    usage: UsageMap  # quota -> cluster -> chip_type -> current usage (mutable)
    cluster_totals: ChipTotals  # cluster -> chip_type -> total chips
    all_guaranteed: ChipTotals  # cluster -> chip_type -> sum of all guarantees


def solve(
    clusters: Sequence[ClusterState],
    pods: dict[str, Pod],
    gang_sets: Sequence[Sequence[str]],
    quotas: Sequence[Quota],
    *,
    solver: str | None = None,
    time_limit: float = 30.0,
    verbose: bool = False,
) -> ScheduleResult:
    clusters = list(clusters)
    quota_names = {quota.name for quota in quotas}
    for name, pod in pods.items():
        if pod.quota not in quota_names:
            raise ValueError(f"pod {name!r} references unknown quota {pod.quota!r}")

    # Build gang groups: pod_name -> frozenset of group members.
    # Validate that all pods in a gang share priority and quota.
    gang_of = _build_gang_groups(pods, gang_sets)

    node_lookup: dict[str, Node] = {}
    node_to_cluster: dict[str, str] = {}
    for cluster in clusters:
        for node in cluster.nodes:
            node_lookup[node.name] = node
            node_to_cluster[node.name] = cluster.name

    node_free = {name: node.chips for name, node in node_lookup.items()}
    for pod in pods.values():
        for replica in pod.statuses_by_replica:
            if replica.phase == Phase.RUNNING and replica.node and replica.node in node_free:
                node_free[replica.node] -= pod.chips_per_replica

    qctx = _build_quota_ctx(clusters, quotas, pods, node_to_cluster)
    pods = dict(pods)  # mutable copy

    # Phase 1: Admission.
    _restart_failed(pods, qctx, node_free, node_lookup, node_to_cluster)
    just_unsuspended = _unsuspend(pods, qctx, gang_of, node_free, node_lookup, node_to_cluster)
    waiting = _admit_pending(
        pods, clusters, node_free, node_to_cluster, node_lookup, qctx, gang_of, just_unsuspended
    )

    # Phase 2: Placement (no eviction — Phase 1 guarantees feasibility).
    waiting = _place_all(pods, waiting, node_free, node_lookup, node_to_cluster)

    # Result: placed first, then waiting in queue order.
    waiting_set = set(waiting)
    result: dict[str, Pod] = {}
    for name in pods:
        if name not in waiting_set:
            result[name] = pods[name]
    for name in waiting:
        result[name] = pods[name]
    return ScheduleResult(pods=result, solver_status="heuristic")


# ---------------------------------------------------------------------------
# Gang groups
# ---------------------------------------------------------------------------


def _build_gang_groups(
    pods: dict[str, Pod], gang_sets: Sequence[Sequence[str]]
) -> dict[str, frozenset[str]]:
    """Build pod_name -> gang group mapping.

    If gang members have mixed priorities, the highest (max) priority is used
    for all members.  If they have mixed quotas, the first member's quota wins.
    """
    gang_of: dict[str, frozenset[str]] = {}
    for gang in gang_sets:
        members = frozenset(name for name in gang if name in pods)
        if len(members) <= 1:
            continue
        # Normalise: use max priority and a single quota across the gang.
        rep_priority = max(pods[name].priority for name in members)
        rep_quota = pods[next(iter(sorted(members)))].quota
        for name in members:
            pod = pods[name]
            if pod.priority != rep_priority or pod.quota != rep_quota:
                pods[name] = replace(pod, priority=rep_priority, quota=rep_quota)
            gang_of[name] = members
    # Singletons: pods not in any gang.
    for name in pods:
        if name not in gang_of:
            gang_of[name] = frozenset({name})
    return gang_of


# ---------------------------------------------------------------------------
# Quota bookkeeping
# ---------------------------------------------------------------------------


def _build_quota_ctx(clusters, quotas, pods, node_to_cluster) -> QuotaCtx:
    cluster_totals: ChipTotals = {}
    for cluster in clusters:
        totals = cluster_totals.setdefault(cluster.name, {})
        for node in cluster.nodes:
            totals[node.chip_type] = totals.get(node.chip_type, 0) + node.chips

    guarantee: UsageMap = {
        quota.name: {cl: dict(chips) for cl, chips in quota.guarantees.items()} for quota in quotas
    }

    all_guaranteed: ChipTotals = {}
    for quota in quotas:
        for cl, chips in quota.guarantees.items():
            by_chip = all_guaranteed.setdefault(cl, {})
            for chip_type, count in chips.items():
                by_chip[chip_type] = by_chip.get(chip_type, 0) + count

    usage: UsageMap = {}
    for pod in pods.values():
        for replica in pod.statuses_by_replica:
            if replica.phase == Phase.RUNNING and replica.node and replica.node in node_to_cluster:
                _add_usage(
                    usage,
                    pod.quota,
                    node_to_cluster[replica.node],
                    pod.chip_type,
                    pod.chips_per_replica,
                )

    return QuotaCtx(guarantee, usage, cluster_totals, all_guaranteed)


def _add_usage(usage: UsageMap, quota: str, cluster: str, chip_type: str, chips: int) -> None:
    by_cluster = usage.setdefault(quota, {})
    by_chip = by_cluster.setdefault(cluster, {})
    by_chip[chip_type] = by_chip.get(chip_type, 0) + chips


def _sub_usage(usage: UsageMap, quota: str, cluster: str, chip_type: str, chips: int) -> None:
    by_chip = usage.get(quota, {}).get(cluster, {})
    by_chip[chip_type] = max(0, by_chip.get(chip_type, 0) - chips)


def _quota_allows(pod: Pod, num_replicas: int, cluster: str, qctx: QuotaCtx) -> bool:
    needed = num_replicas * pod.chips_per_replica
    current = qctx.usage.get(pod.quota, {}).get(cluster, {}).get(pod.chip_type, 0)
    guaranteed = qctx.guarantee.get(pod.quota, {}).get(cluster, {}).get(pod.chip_type, 0)
    if current + needed <= guaranteed:
        return True
    # Beyond guarantee: borrow from the unguaranteed pool.
    total = qctx.cluster_totals.get(cluster, {}).get(pod.chip_type, 0)
    total_guar = qctx.all_guaranteed.get(cluster, {}).get(pod.chip_type, 0)
    unguaranteed = max(0, total - total_guar)
    total_borrowing = sum(
        max(
            0,
            qc.get(cluster, {}).get(pod.chip_type, 0)
            - qctx.guarantee.get(qname, {}).get(cluster, {}).get(pod.chip_type, 0),
        )
        for qname, qc in qctx.usage.items()
    )
    own_borrowing = max(0, current - guaranteed)
    available = max(0, unguaranteed - total_borrowing + own_borrowing)
    return needed <= guaranteed - current + available


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pending_count(pod: Pod) -> int:
    return sum(1 for rs in pod.statuses_by_replica if rs.phase == Phase.RUNNING and rs.node is None)


def _running_count(pod: Pod) -> int:
    return sum(
        1 for rs in pod.statuses_by_replica if rs.phase == Phase.RUNNING and rs.node is not None
    )


def _group_priority(pods: dict[str, Pod], group: frozenset[str]) -> int:
    """All members share the same priority; return it."""
    return pods[next(iter(group))].priority


def _group_total_demand(pods: dict[str, Pod], group: frozenset[str]) -> int:
    """Total chip demand across all pods in the group."""
    return sum(len(pods[name].statuses_by_replica) * pods[name].chips_per_replica for name in group)


def _unique_groups(names: list[str], gang_of: dict[str, frozenset[str]]) -> list[frozenset[str]]:
    """Deduplicate names into unique gang groups, preserving first-seen order."""
    seen: set[frozenset[str]] = set()
    groups: list[frozenset[str]] = []
    for name in names:
        group = gang_of[name]
        if group not in seen:
            seen.add(group)
            groups.append(group)
    return groups


def _groups_by_priority(pods: dict[str, Pod], groups: list[frozenset[str]]) -> list[frozenset[str]]:
    return sorted(
        groups,
        key=lambda group: (-_group_priority(pods, group), _group_total_demand(pods, group)),
    )


def _nodes_can_fit(
    pod: Pod,
    num_replicas: int,
    cluster: str,
    node_free: dict[str, int],
    node_lookup: dict[str, Node],
    node_to_cluster: dict[str, str],
) -> bool:
    """Check that enough nodes on `cluster` have free capacity for `num_replicas`.

    This is the node-level feasibility check (analogous to Kueue's TAS check)
    that prevents Phase 1 from admitting pods that Phase 2 cannot place.
    """
    if pod.chips_per_replica == 0:
        return True
    available = 0
    for node in node_lookup.values():
        if node_to_cluster[node.name] == cluster and node.chip_type == pod.chip_type:
            available += node_free[node.name] // pod.chips_per_replica
            if available >= num_replicas:
                return True
    return available >= num_replicas


def _clusters_ranked_by_free(chip_type, node_free, node_lookup, node_to_cluster) -> list[str]:
    free_per_cluster: dict[str, int] = {}
    for node in node_lookup.values():
        if node.chip_type == chip_type:
            cl = node_to_cluster[node.name]
            free_per_cluster[cl] = free_per_cluster.get(cl, 0) + node_free[node.name]
    return [
        cl
        for cl, _ in sorted(free_per_cluster.items(), key=lambda x: (-x[1], x[0]))
        if free_per_cluster[cl] > 0
    ]


# ---------------------------------------------------------------------------
# Phase 1: Admission
# ---------------------------------------------------------------------------


def _restart_failed(
    pods: dict[str, Pod],
    qctx: QuotaCtx,
    node_free: dict[str, int],
    node_lookup: dict[str, Node],
    node_to_cluster: dict[str, str],
) -> None:
    for name, pod in list(pods.items()):
        if not pod.cluster:
            continue
        failed_indices = [
            i for i, rs in enumerate(pod.statuses_by_replica) if rs.phase == Phase.FAILED
        ]
        if not failed_indices:
            continue
        num = len(failed_indices)
        if not _quota_allows(pod, num, pod.cluster, qctx) or not _nodes_can_fit(
            pod, num, pod.cluster, node_free, node_lookup, node_to_cluster
        ):
            continue
        statuses = list(pod.statuses_by_replica)
        for idx in failed_indices:
            statuses[idx] = PodReplicaStatus(Phase.RUNNING)
        _add_usage(
            qctx.usage,
            pod.quota,
            pod.cluster,
            pod.chip_type,
            num * pod.chips_per_replica,
        )
        pods[name] = replace(pod, statuses_by_replica=statuses)
        _place_pod(pods, name, node_free, node_lookup, node_to_cluster)


def _unsuspend(
    pods: dict[str, Pod],
    qctx: QuotaCtx,
    gang_of: dict[str, frozenset[str]],
    node_free: dict[str, int],
    node_lookup: dict[str, Node],
    node_to_cluster: dict[str, str],
) -> set[str]:
    suspended_names = [
        name
        for name, pod in pods.items()
        if any(rs.phase == Phase.SUSPENDED for rs in pod.statuses_by_replica)
    ]
    groups = _groups_by_priority(pods, _unique_groups(suspended_names, gang_of))
    unsuspended: set[str] = set()

    for group in groups:
        # All members of the gang must be unsuspendable, or none are.
        suspended_in_group = [
            name
            for name in group
            if any(rs.phase == Phase.SUSPENDED for rs in pods[name].statuses_by_replica)
        ]
        if not suspended_in_group:
            continue

        # Check quota and node feasibility for each member independently
        # (they may be on different clusters).
        all_allowed = True
        for name in suspended_in_group:
            pod = pods[name]
            if not pod.cluster:
                all_allowed = False
                break
            num = len(pod.statuses_by_replica)
            if not _quota_allows(pod, num, pod.cluster, qctx) or not _nodes_can_fit(
                pod, num, pod.cluster, node_free, node_lookup, node_to_cluster
            ):
                all_allowed = False
                break

        if not all_allowed:
            continue

        # Commit: unsuspend all members and place onto nodes immediately
        # so that node_free stays accurate for _admit_pending.
        for name in suspended_in_group:
            pod = pods[name]
            assert pod.cluster is not None  # checked above
            num = len(pod.statuses_by_replica)
            _add_usage(
                qctx.usage, pod.quota, pod.cluster, pod.chip_type, num * pod.chips_per_replica
            )
            pods[name] = replace(
                pod,
                statuses_by_replica=[
                    PodReplicaStatus(Phase.RUNNING) for _ in pod.statuses_by_replica
                ],
            )
            _place_pod(pods, name, node_free, node_lookup, node_to_cluster)
            unsuspended.add(name)

    return unsuspended


def _admit_pending(
    pods: dict[str, Pod],
    clusters: list[ClusterState],
    node_free: dict[str, int],
    node_to_cluster: dict[str, str],
    node_lookup: dict[str, Node],
    qctx: QuotaCtx,
    gang_of: dict[str, frozenset[str]],
    just_unsuspended: set[str] | None = None,
) -> list[str]:
    pending_names = [
        name for name, pod in pods.items() if not pod.cluster and _pending_count(pod) > 0
    ]
    groups = _groups_by_priority(pods, _unique_groups(pending_names, gang_of))

    waiting_groups: list[frozenset[str]] = []
    for group in groups:
        if _try_admit_group(pods, group, clusters, node_free, node_to_cluster, node_lookup, qctx):
            continue
        waiting_groups.append(group)

    # Preemption pass.
    still_waiting: list[str] = []
    for group in waiting_groups:
        if not _try_preempt_admission(
            pods,
            clusters,
            group,
            node_free,
            node_to_cluster,
            node_lookup,
            qctx,
            gang_of,
            just_unsuspended or set(),
        ):
            still_waiting.extend(group)
    return still_waiting


def _try_admit_group(
    pods: dict[str, Pod],
    group: frozenset[str],
    clusters: list[ClusterState],
    node_free: dict[str, int],
    node_to_cluster: dict[str, str],
    node_lookup: dict[str, Node],
    qctx: QuotaCtx,
) -> bool:
    """Try to admit all pods in a gang group. Returns True if all admitted.

    Gang cluster coordination: for multi-member gangs, first try to place
    all members on the same cluster (tried in order of free capacity). This
    prevents gang members from scattering across clusters when a single
    cluster could serve them all. Falls back to independent per-pod cluster
    selection if same-cluster placement is infeasible.
    """
    pending_members = [
        (name, pods[name])
        for name in sorted(group)
        if not pods[name].cluster and _pending_count(pods[name]) > 0
    ]
    if not pending_members:
        return True

    # --- Same-cluster attempt (gang coordination) ---
    # Only try if the gang has more than one pending member.
    if len(pending_members) > 1:
        # Collect all chip types needed; find clusters that have all of them.
        chip_types_needed = {pod.chip_type for _, pod in pending_members}
        # Rank clusters by total free chips across all needed chip types.
        cluster_scores: dict[str, int] = {}
        for node in node_lookup.values():
            if node.chip_type in chip_types_needed:
                cl = node_to_cluster[node.name]
                cluster_scores[cl] = cluster_scores.get(cl, 0) + node_free[node.name]
        ranked_clusters = sorted(cluster_scores, key=lambda cl: (-cluster_scores[cl], cl))

        for cluster_name in ranked_clusters:
            placements: list[tuple[str, str, int]] = []
            all_fit = True
            for name, pod in pending_members:
                num = _pending_count(pod)
                if _quota_allows(pod, num, cluster_name, qctx) and _nodes_can_fit(
                    pod, num, cluster_name, node_free, node_lookup, node_to_cluster
                ):
                    _add_usage(
                        qctx.usage,
                        pod.quota,
                        cluster_name,
                        pod.chip_type,
                        num * pod.chips_per_replica,
                    )
                    pods[name] = replace(pod, cluster=cluster_name)
                    placements.append((name, cluster_name, num))
                else:
                    all_fit = False
                    break
            if all_fit:
                return True
            # Roll back this cluster attempt.
            for placed_name, placed_cluster, placed_num in placements:
                placed_pod = pods[placed_name]
                _sub_usage(
                    qctx.usage,
                    placed_pod.quota,
                    placed_cluster,
                    placed_pod.chip_type,
                    placed_num * placed_pod.chips_per_replica,
                )
                pods[placed_name] = replace(placed_pod, cluster=None)

    # --- Independent per-pod fallback ---
    placements = []
    for name, pod in pending_members:
        num = _pending_count(pod)
        admitted = False
        for cluster_name in _clusters_ranked_by_free(
            pod.chip_type, node_free, node_lookup, node_to_cluster
        ):
            if _quota_allows(pod, num, cluster_name, qctx) and _nodes_can_fit(
                pod, num, cluster_name, node_free, node_lookup, node_to_cluster
            ):
                _add_usage(
                    qctx.usage, pod.quota, cluster_name, pod.chip_type, num * pod.chips_per_replica
                )
                pods[name] = replace(pod, cluster=cluster_name)
                placements.append((name, cluster_name, num))
                admitted = True
                break
        if not admitted:
            # Roll back all placements in this group.
            for placed_name, placed_cluster, placed_num in placements:
                placed_pod = pods[placed_name]
                _sub_usage(
                    qctx.usage,
                    placed_pod.quota,
                    placed_cluster,
                    placed_pod.chip_type,
                    placed_num * placed_pod.chips_per_replica,
                )
                pods[placed_name] = replace(placed_pod, cluster=None)
            return False
    return True


def _try_preempt_admission(
    pods: dict[str, Pod],
    clusters: list[ClusterState],
    group: frozenset[str],
    node_free: dict[str, int],
    node_to_cluster: dict[str, str],
    node_lookup: dict[str, Node],
    qctx: QuotaCtx,
    gang_of: dict[str, frozenset[str]],
    just_unsuspended: set[str] | None = None,
) -> bool:
    """Try to preempt victims to admit a gang group."""
    # Collect total demand per (cluster, chip_type) across the group.
    # For simplicity, try each cluster for each pod independently.
    # We need to find victims whose eviction frees enough quota.

    # Use the priority/quota of the group (all same).
    group_priority = _group_priority(pods, group)
    group_quota = pods[next(iter(group))].quota

    # For each cluster, find evictable pods.
    # Try to admit each pod in the group to some cluster with preemption.
    # This is complex for multi-cluster gangs. Simplification: try to admit
    # each member to its best cluster, collecting all needed victims, then
    # commit atomically.

    # First pass: for each pod in the group, find which cluster it could go to
    # and what victims would be needed.
    all_victims: list[str] = []
    placements: list[tuple[str, str, int]] = []  # (pod_name, cluster_name, num_replicas)

    for name in sorted(group):
        pod = pods[name]
        if pod.cluster or _pending_count(pod) == 0:
            continue
        num = _pending_count(pod)
        chips_needed = num * pod.chips_per_replica
        placed = False

        for cluster in clusters:
            cluster_name = cluster.name
            candidates = []
            for victim_name, victim_pod in pods.items():
                if victim_pod.cluster != cluster_name or victim_pod.chip_type != pod.chip_type:
                    continue
                if any(rs.phase == Phase.SUSPENDED for rs in victim_pod.statuses_by_replica):
                    continue
                # Don't evict members of our own gang, already-selected victims,
                # or pods that were just unsuspended this cycle (honor committed work).
                if victim_name in group:
                    continue
                if just_unsuspended and victim_name in just_unsuspended:
                    continue
                num_running = _running_count(victim_pod)
                if num_running == 0:
                    continue
                if victim_pod.quota == group_quota:
                    if victim_pod.priority >= group_priority:
                        continue
                else:
                    victim_usage = (
                        qctx.usage.get(victim_pod.quota, {})
                        .get(cluster_name, {})
                        .get(victim_pod.chip_type, 0)
                    )
                    victim_guarantee = (
                        qctx.guarantee.get(victim_pod.quota, {})
                        .get(cluster_name, {})
                        .get(victim_pod.chip_type, 0)
                    )
                    if victim_usage <= victim_guarantee:
                        continue
                candidates.append((victim_name, num_running * victim_pod.chips_per_replica))

            # First check if quota allows without preemption.
            if _quota_allows(pod, num, cluster_name, qctx) and _nodes_can_fit(
                pod, num, cluster_name, node_free, node_lookup, node_to_cluster
            ):
                _add_usage(qctx.usage, pod.quota, cluster_name, pod.chip_type, chips_needed)
                pods[name] = replace(pod, cluster=cluster_name)
                placements.append((name, cluster_name, num))
                placed = True
                break

            if not candidates:
                continue

            candidates.sort(key=lambda pair: (pods[pair[0]].priority, -pair[1]))

            # Temporarily remove victim usage (quota + node capacity).
            victims_for_pod: list[tuple[str, int]] = []
            freed_nodes: list[tuple[str, int]] = []  # (node_name, chips) for rollback
            freed = 0
            for victim_name, freeable in candidates:
                victim_pod = pods[victim_name]
                chips = _running_count(victim_pod) * victim_pod.chips_per_replica
                victims_for_pod.append((victim_name, chips))
                _sub_usage(qctx.usage, victim_pod.quota, cluster_name, victim_pod.chip_type, chips)
                # Temporarily free victim node capacity for feasibility check.
                for replica in victim_pod.statuses_by_replica:
                    if (
                        replica.phase == Phase.RUNNING
                        and replica.node
                        and replica.node in node_free
                    ):
                        node_free[replica.node] += victim_pod.chips_per_replica
                        freed_nodes.append((replica.node, victim_pod.chips_per_replica))
                freed += freeable
                if _quota_allows(pod, num, cluster_name, qctx):
                    break

            if _quota_allows(pod, num, cluster_name, qctx) and _nodes_can_fit(
                pod, num, cluster_name, node_free, node_lookup, node_to_cluster
            ):
                _add_usage(qctx.usage, pod.quota, cluster_name, pod.chip_type, chips_needed)
                pods[name] = replace(pod, cluster=cluster_name)
                placements.append((name, cluster_name, num))
                # Expand gang victims: if a victim is in a gang, add all gang members.
                for victim_name, _ in victims_for_pod:
                    for gang_member in gang_of[victim_name]:
                        if gang_member not in all_victims:
                            all_victims.append(gang_member)
                # Revert node_free — actual freeing happens in the commit phase below.
                for node_name, chips in freed_nodes:
                    node_free[node_name] -= chips
                placed = True
                break
            else:
                # Rollback victim usage removal and node capacity.
                for victim_name, chips in victims_for_pod:
                    victim_pod = pods[victim_name]
                    _add_usage(
                        qctx.usage, victim_pod.quota, cluster_name, victim_pod.chip_type, chips
                    )
                for node_name, chips in freed_nodes:
                    node_free[node_name] -= chips

        if not placed:
            # Roll back all placements.
            for placed_name, placed_cluster, placed_num in placements:
                placed_pod = pods[placed_name]
                _sub_usage(
                    qctx.usage,
                    placed_pod.quota,
                    placed_cluster,
                    placed_pod.chip_type,
                    placed_num * placed_pod.chips_per_replica,
                )
                pods[placed_name] = replace(placed_pod, cluster=None)
            return False

    # Commit: suspend all victims (and their gang members).
    for victim_name in all_victims:
        victim_pod = pods[victim_name]
        if any(rs.phase == Phase.SUSPENDED for rs in victim_pod.statuses_by_replica):
            continue
        running = _running_count(victim_pod)
        if running > 0 and victim_pod.cluster:
            _sub_usage(
                qctx.usage,
                victim_pod.quota,
                victim_pod.cluster,
                victim_pod.chip_type,
                running * victim_pod.chips_per_replica,
            )
        for replica in victim_pod.statuses_by_replica:
            if replica.phase == Phase.RUNNING and replica.node and replica.node in node_free:
                node_free[replica.node] += victim_pod.chips_per_replica
        pods[victim_name] = replace(
            victim_pod,
            statuses_by_replica=[
                PodReplicaStatus(Phase.SUSPENDED) for _ in victim_pod.statuses_by_replica
            ],
        )

    return True


# ---------------------------------------------------------------------------
# Phase 2: Placement (bin packing with node-level eviction)
# ---------------------------------------------------------------------------


def _place_all(
    pods: dict[str, Pod],
    waiting: list[str],
    node_free: dict[str, int],
    node_lookup: dict[str, Node],
    node_to_cluster: dict[str, str],
) -> list[str]:
    """Place admitted pods onto nodes via best-fit-decreasing.

    Since Phase 1 checks node-level feasibility before admitting, every
    admitted pod is guaranteed to fit without node-level eviction. This
    eliminates the fragile eviction cascades of the previous design.
    """
    waiting_set = set(waiting)
    needs_nodes = sorted(
        [name for name, pod in pods.items() if name not in waiting_set and _pending_count(pod) > 0],
        key=lambda name: (-pods[name].chips_per_replica, -pods[name].priority),
    )

    for name in needs_nodes:
        _place_pod(pods, name, node_free, node_lookup, node_to_cluster)
    return waiting


def _place_pod(
    pods: dict[str, Pod],
    name: str,
    node_free: dict[str, int],
    node_lookup: dict[str, Node],
    node_to_cluster: dict[str, str],
) -> None:
    pod = pods[name]
    if not pod.cluster:
        return
    unplaced = [
        idx
        for idx, rs in enumerate(pod.statuses_by_replica)
        if rs.phase == Phase.RUNNING and rs.node is None
    ]
    if not unplaced:
        return

    # Best-fit: prefer nodes with least free space (that still fit).
    candidate_nodes = sorted(
        [
            node.name
            for node in node_lookup.values()
            if node_to_cluster[node.name] == pod.cluster and node.chip_type == pod.chip_type
        ],
        key=lambda node_name: node_free[node_name],
    )

    statuses = list(pod.statuses_by_replica)
    placed_count = 0
    for replica_idx in unplaced:
        node_name = _find_node_for_replica(pod, candidate_nodes, node_free)
        if node_name is None:
            break
        statuses[replica_idx] = PodReplicaStatus(Phase.RUNNING, node_name)
        node_free[node_name] -= pod.chips_per_replica
        placed_count += 1

    # Gang: all or nothing.
    if 0 < placed_count < len(unplaced):
        for replica_idx in unplaced[:placed_count]:
            assigned_node = statuses[replica_idx].node
            if assigned_node:
                node_free[assigned_node] += pod.chips_per_replica
            statuses[replica_idx] = PodReplicaStatus(Phase.RUNNING)
    pods[name] = replace(pod, statuses_by_replica=statuses)


def _find_node_for_replica(
    pod: Pod, candidate_nodes: list[str], node_free: dict[str, int]
) -> str | None:
    for node_name in candidate_nodes:
        if node_free[node_name] >= pod.chips_per_replica:
            return node_name
    return None
