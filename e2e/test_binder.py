"""
Binder k8s mechanics: node binding, suspend toggle, managed-by label,
multi-cluster routing, capacity edge cases.

Every test exercises the Rust binder against real kind clusters.
"""

import pytest

from conftest import (
    build_job,
    delete_k8s_workload,
    delete_workload,
    get_job_by_name,
    get_pods_on_cluster,
    submit_job,
    wait_for,
    wait_for_not,
)

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Binding API — node pinning via schedulerName + Binding subresource
# ---------------------------------------------------------------------------


def test_binding_api_pins_to_specific_node(scheduler, k8s_clients):
    """Binder must pin pods to a specific node via the Binding API.

    The Job template carries schedulerName=custom-scheduler (so the default
    k8s scheduler ignores the pods).  The binder calls POST .../pods/{name}/binding
    to bind each pod to its solver-assigned node.  After binding, spec.nodeName
    must be set on the pod and spec.affinity must be absent from the Job template.
    """
    resp = submit_job(scheduler, build_job("binding-test", "h100", priority=5))
    assert resp.status_code == 201

    def job_exists():
        return get_job_by_name(k8s_clients, "cluster-a", "binding-test") is not None

    wait_for(job_exists, desc="job created on cluster-a")

    job = get_job_by_name(k8s_clients, "cluster-a", "binding-test")

    # schedulerName must be set so the default scheduler ignores the pods.
    scheduler_name = job.spec.template.spec.scheduler_name
    assert scheduler_name == "custom-scheduler", (
        f"Expected schedulerName=custom-scheduler, got: {scheduler_name!r}"
    )

    # No nodeAffinity should be injected into the pod template.
    affinity = job.spec.template.spec.affinity
    assert affinity is None, f"nodeAffinity must not be set on Job template, got: {affinity}"

    # Pod must be bound to a specific node via the Binding API.
    def pod_bound():
        pods = get_pods_on_cluster(k8s_clients, "cluster-a")
        for pod in pods:
            refs = pod.metadata.owner_references or []
            if any(r.name == job.metadata.name for r in refs):
                return pod.spec.node_name is not None
        return False

    wait_for(pod_bound, desc="pod bound to a node via Binding API")

    pods = get_pods_on_cluster(k8s_clients, "cluster-a")
    job_pods = [
        p
        for p in pods
        if any(r.name == job.metadata.name for r in (p.metadata.owner_references or []))
    ]
    assert len(job_pods) >= 1
    assert job_pods[0].spec.node_name is not None, "Pod must have spec.nodeName set after binding"


def test_unsuspend_rebinds_pod_to_node(scheduler, k8s_clients):
    """After suspend + unsuspend, pod must be re-bound to a node.

    With the Binding API approach there is no nodeAffinity to update — the
    binder simply unsuspends the Job and rebinds the fresh pods.
    """
    # Submit a low-priority borrower job (team-serve guarantee=0, 4 chips).
    resp = submit_job(
        scheduler, build_job("victim", "h100", priority=1, quota="team-serve", chips=4)
    )
    assert resp.status_code == 201

    def victim_placed():
        j = get_job_by_name(k8s_clients, "cluster-a", "victim")
        return j is not None and j.spec.suspend is False

    wait_for(victim_placed, desc="victim placed")

    # Submit high-priority job to fill cluster and preempt victim (8 chips).
    submit_job(scheduler, build_job("filler", "h100", priority=10, quota="team-train", chips=8))

    def victim_suspended():
        j = get_job_by_name(k8s_clients, "cluster-a", "victim")
        return j is not None and j.spec.suspend is True

    wait_for(victim_suspended, timeout=45, desc="victim suspended by preemption")

    # Delete filler from k8s to free capacity, allowing victim to unsuspend.
    delete_k8s_workload(k8s_clients, "filler")

    def victim_unsuspended():
        j = get_job_by_name(k8s_clients, "cluster-a", "victim")
        return j is not None and j.spec.suspend is False

    wait_for(victim_unsuspended, timeout=45, desc="victim unsuspended")

    # Pod must be rebound to a node after unsuspend.
    job = get_job_by_name(k8s_clients, "cluster-a", "victim")

    def pod_rebound():
        pods = get_pods_on_cluster(k8s_clients, "cluster-a")
        for pod in pods:
            refs = pod.metadata.owner_references or []
            if any(r.name == job.metadata.name for r in refs):
                return pod.spec.node_name is not None
        return False

    wait_for(pod_rebound, timeout=30, desc="pod rebound after unsuspend")


# ---------------------------------------------------------------------------
# Suspend toggle
# ---------------------------------------------------------------------------


def test_suspend_sets_true_on_k8s_object(scheduler, k8s_clients):
    """Preempted job gets spec.suspend=true on the k8s object."""
    # Submit low-priority borrower job on team-serve (guarantee=0 on cluster-a).
    submit_job(scheduler, build_job("to-suspend", "h100", priority=1, quota="team-serve", chips=4))
    wait_for(
        lambda: (
            get_job_by_name(k8s_clients, "cluster-a", "to-suspend") is not None
            and get_job_by_name(k8s_clients, "cluster-a", "to-suspend").spec.suspend is False
        ),
        desc="to-suspend placed",
    )

    # Fill cluster with high-priority job to trigger preemption (8 chips).
    submit_job(scheduler, build_job("preemptor", "h100", priority=10, quota="team-train", chips=8))

    def job_suspended():
        j = get_job_by_name(k8s_clients, "cluster-a", "to-suspend")
        return j is not None and j.spec.suspend is True

    wait_for(job_suspended, timeout=45, desc="to-suspend gets suspend=true")


# ---------------------------------------------------------------------------
# Managed-by label
# ---------------------------------------------------------------------------


def test_unmanaged_objects_ignored(scheduler, k8s_clients):
    """Manually created job (no managed-by label) is not touched by scheduler."""
    from kubernetes.client import (
        V1Container,
        V1Job,
        V1JobSpec,
        V1ObjectMeta,
        V1PodSpec,
        V1PodTemplateSpec,
    )

    batch = k8s_clients["cluster-a"]["batch"]

    # Create a job manually without managed-by label.
    manual_job = V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name="manual-job", labels={"team": "external"}),
        spec=V1JobSpec(
            template=V1PodTemplateSpec(
                spec=V1PodSpec(
                    containers=[
                        V1Container(name="test", image="busybox:1.36", command=["sleep", "10"])
                    ],
                    restart_policy="Never",
                )
            ),
        ),
    )
    batch.create_namespaced_job("default", manual_job)

    # Submit a real scheduler job to trigger a solver cycle.
    submit_job(scheduler, build_job("real-job", "h100", priority=5))
    wait_for(
        lambda: get_job_by_name(k8s_clients, "cluster-a", "real-job") is not None,
        desc="real-job placed",
    )

    # manual-job should still exist, untouched.
    try:
        j = batch.read_namespaced_job("manual-job", "default")
        assert j.spec.suspend is None or j.spec.suspend is False, "manual-job was modified"
    finally:
        batch.delete_namespaced_job("manual-job", "default")


# ---------------------------------------------------------------------------
# Multi-cluster routing
# ---------------------------------------------------------------------------


def test_chip_type_determines_cluster(scheduler, k8s_clients):
    """h100 → cluster-a, a100 → cluster-b."""
    submit_job(scheduler, build_job("h100-job", "h100", priority=5))
    submit_job(scheduler, build_job("a100-job", "a100", priority=5, quota="team-train"))

    wait_for(
        lambda: get_job_by_name(k8s_clients, "cluster-a", "h100-job") is not None,
        desc="h100-job on cluster-a",
    )
    wait_for(
        lambda: get_job_by_name(k8s_clients, "cluster-b", "a100-job") is not None,
        desc="a100-job on cluster-b",
    )

    # Verify they're NOT on the wrong cluster.
    assert get_job_by_name(k8s_clients, "cluster-b", "h100-job") is None
    assert get_job_by_name(k8s_clients, "cluster-a", "a100-job") is None


def test_chip_type_unavailable_stays_queued(scheduler, k8s_clients):
    """Job requesting chip type on neither cluster stays queued."""
    submit_job(scheduler, build_job("tpu-job", "tpu-v5", priority=5))

    # Must NOT be placed on either cluster for at least 2 solver cycles.
    def tpu_placed():
        return (
            get_job_by_name(k8s_clients, "cluster-a", "tpu-job") is not None
            or get_job_by_name(k8s_clients, "cluster-b", "tpu-job") is not None
        )

    wait_for_not(tpu_placed, duration=12, desc="tpu-job should stay queued")


# ---------------------------------------------------------------------------
# Capacity edge cases
# ---------------------------------------------------------------------------


def test_capacity_freed_triggers_placement(scheduler, k8s_clients):
    """Queued job gets placed when capacity frees up."""
    # Fill cluster-a (1 worker * 8 chips) with one 8-chip job.
    submit_job(scheduler, build_job("filler", "h100", priority=5))
    wait_for(
        lambda: get_job_by_name(k8s_clients, "cluster-a", "filler") is not None,
        desc="cluster-a filled",
    )

    # Submit one more — should be queued (no room on the node).
    submit_job(scheduler, build_job("waiting", "h100", priority=5))
    wait_for_not(
        lambda: get_job_by_name(k8s_clients, "cluster-a", "waiting") is not None,
        duration=8,
        desc="waiting stays queued",
    )

    # Delete filler from k8s to free capacity.
    delete_k8s_workload(k8s_clients, "filler")
    # Also delete from scheduler store (best-effort, may already be removed).
    delete_workload(scheduler, "filler")

    # waiting should now get placed (after k8s object deletion + next solver cycle).
    # This may take several solver cycles: reflector must notice freed capacity,
    # then solver must run and place the queued job.
    wait_for(
        lambda: get_job_by_name(k8s_clients, "cluster-a", "waiting") is not None,
        timeout=60,
        desc="waiting placed after capacity freed",
    )
