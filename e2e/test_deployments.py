"""
Deployment workload tests: simulate KEDA-like autoscaler submitting and
removing individual Pods that represent deployment replicas.

Deployments are modelled as independent single-replica Pods (see model.py).
These tests verify that the scheduler handles the incremental scale-up/down
pattern that an external autoscaler like KEDA would produce.
"""

import pytest
from kubernetes import client

from conftest import (
    CHIP_RESOURCE,
    JOB_NAME_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    build_job,
    build_pod,
    delete_k8s_workload,
    delete_workload,
    get_pods_on_cluster,
    get_status,
    submit_job,
    submit_pod,
    wait_for,
    wait_for_not,
)

pytestmark = pytest.mark.e2e


def _create_pod_on_cluster(k8s_clients, cluster, name, chip_type, priority, quota, chips):
    """Create a Pod directly on a k8s cluster, bypassing the scheduler API.

    This is how KEDA would create Pods: directly via the k8s API with the
    correct labels and annotations so the scheduler's reflector picks them up.
    """
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace="default",
            labels={
                "accelerator": chip_type,
                JOB_NAME_LABEL: name,
                MANAGED_BY_LABEL: MANAGED_BY_VALUE,
            },
            annotations={
                "scheduler.example.com/priority": str(priority),
                "scheduler.example.com/quota": quota,
            },
        ),
        spec=client.V1PodSpec(
            tolerations=[
                client.V1Toleration(
                    key="scheduler", operator="Equal", value="custom", effect="NoSchedule"
                )
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="busybox:1.36",
                    command=["sleep", "3600"],
                    resources=client.V1ResourceRequirements(
                        requests={CHIP_RESOURCE: str(chips)},
                        limits={CHIP_RESOURCE: str(chips)},
                    ),
                )
            ],
            restart_policy="Never",
        ),
    )
    k8s_clients[cluster]["core"].create_namespaced_pod("default", pod)


# ---------------------------------------------------------------------------
# Reflector discovery: Pods created directly on cluster (KEDA path)
# ---------------------------------------------------------------------------


def test_reflector_discovers_externally_created_pod(scheduler, k8s_clients):
    """Pod created directly on k8s (as KEDA would) is picked up by the reflector."""
    _create_pod_on_cluster(
        k8s_clients, "cluster-a", "keda-0", "h100", priority=5, quota="team-train", chips=4
    )

    # Wait for the k8s scheduler to bind keda-0 to a node (spec.nodeName set).
    # The solver only counts a pod's chips as occupying a node once nodeName is
    # known; if we proceed before binding completes, the solver sees 8 free
    # chips and places overflow despite keda-0 already consuming 4 chips.
    wait_for(
        lambda: any(
            p.metadata.name == "keda-0" and p.spec and p.spec.node_name is not None
            for p in get_pods_on_cluster(k8s_clients, "cluster-a")
        ),
        desc="keda-0 bound to a node on cluster-a",
    )

    # Submit a job that needs the full node. If the solver sees keda-0's 4 chips
    # as occupied, the job should only get the remaining 4 chips placed.
    resp = submit_job(
        scheduler, build_job("needs-space", "h100", priority=5, quota="team-train", chips=4)
    )
    assert resp.status_code == 201

    wait_for(
        lambda: (
            (s := get_status(scheduler, "needs-space")) is not None and s.get("phase") == "running"
        ),
        desc="needs-space placed alongside keda-0",
    )

    # A second 4-chip job should NOT fit (8 chips already used).
    # Use team-train quota so the same-quota priority check applies:
    # overflow (priority=1) cannot preempt keda-0 or needs-space (priority=5).
    resp = submit_job(
        scheduler, build_job("overflow", "h100", priority=1, quota="team-train", chips=4)
    )
    assert resp.status_code == 201

    wait_for_not(
        lambda: (
            (s := get_status(scheduler, "overflow")) is not None and s.get("phase") == "running"
        ),
        duration=12,
        desc="overflow must not be placed (cluster full)",
    )


def test_externally_created_pod_coexists_with_api_pod(scheduler, k8s_clients):
    """Pods from KEDA (direct k8s create) and from the scheduler API coexist."""
    # Create one pod via KEDA path.
    _create_pod_on_cluster(
        k8s_clients, "cluster-a", "keda-1", "h100", priority=5, quota="team-train", chips=4
    )

    # Create one pod via scheduler API.
    resp = submit_pod(
        scheduler, build_pod("api-1", "h100", priority=5, quota="team-train", chips=4)
    )
    assert resp.status_code == 201

    wait_for(
        lambda: (s := get_status(scheduler, "api-1")) is not None and s.get("phase") == "running",
        desc="api-1 placed",
    )

    # Both should be on cluster-a.
    pods = get_pods_on_cluster(k8s_clients, "cluster-a")
    names = {p.metadata.name for p in pods}
    assert "keda-1" in names
    assert "api-1" in names


# ---------------------------------------------------------------------------
# Scale-up: incremental Pod submissions
# ---------------------------------------------------------------------------


def test_deployment_scale_up(scheduler, k8s_clients):
    """Submit deployment replicas incrementally; all get placed."""
    pods = [
        build_pod(f"web-{i}", "h100", priority=5, quota="team-train", chips=2) for i in range(4)
    ]
    for p in pods:
        resp = submit_pod(scheduler, p)
        assert resp.status_code == 201

    # All 4 pods (2 chips each = 8 total) should fit on cluster-a's single node.
    for i in range(4):
        wait_for(
            lambda i=i: (
                (s := get_status(scheduler, f"web-{i}")) is not None and s.get("phase") == "running"
            ),
            desc=f"web-{i} placed",
        )


def test_deployment_scale_down(scheduler, k8s_clients):
    """Remove some replicas; remaining pods stay running."""
    for i in range(4):
        resp = submit_pod(
            scheduler, build_pod(f"svc-{i}", "h100", priority=5, quota="team-train", chips=2)
        )
        assert resp.status_code == 201

    for i in range(4):
        wait_for(
            lambda i=i: (
                (s := get_status(scheduler, f"svc-{i}")) is not None and s.get("phase") == "running"
            ),
            desc=f"svc-{i} placed",
        )

    # Scale down: remove 2 replicas (simulating KEDA scale-down).
    delete_k8s_workload(k8s_clients, "svc-2")
    delete_workload(scheduler, "svc-2")
    delete_k8s_workload(k8s_clients, "svc-3")
    delete_workload(scheduler, "svc-3")

    # Remaining pods must stay running.
    for i in range(2):
        wait_for_not(
            lambda i=i: (
                (s := get_status(scheduler, f"svc-{i}")) is not None
                and s.get("phase") == "suspended"
            ),
            duration=10,
            desc=f"svc-{i} stays running after scale-down",
        )


# ---------------------------------------------------------------------------
# Preemption: high-priority Job preempts deployment Pods
# ---------------------------------------------------------------------------


def test_job_preempts_deployment_pod(scheduler, k8s_clients):
    """A high-priority Job preempts a low-priority deployment Pod."""
    # Place a borrower deployment pod (team-serve, guarantee=0) using 4 chips.
    resp = submit_pod(
        scheduler, build_pod("dep-0", "h100", priority=1, quota="team-serve", chips=4)
    )
    assert resp.status_code == 201

    wait_for(
        lambda: (s := get_status(scheduler, "dep-0")) is not None and s.get("phase") == "running",
        desc="dep-0 placed",
    )

    # Submit high-priority job that needs the full node (8 chips).
    resp = submit_job(scheduler, build_job("train", "h100", priority=10, quota="team-train"))
    assert resp.status_code == 201

    # Deployment pod must be suspended to make room.
    wait_for(
        lambda: (s := get_status(scheduler, "dep-0")) is not None and s.get("phase") == "suspended",
        timeout=45,
        desc="dep-0 preempted by train job",
    )


def test_preempted_deployment_pod_reschedules(scheduler, k8s_clients):
    """A deployment Pod preempted by a Job gets re-placed when capacity frees."""
    resp = submit_pod(
        scheduler, build_pod("replica-0", "h100", priority=1, quota="team-serve", chips=4)
    )
    assert resp.status_code == 201
    wait_for(
        lambda: (
            (s := get_status(scheduler, "replica-0")) is not None and s.get("phase") == "running"
        ),
        desc="replica-0 placed",
    )

    # Preempt with high-priority job.
    resp = submit_job(scheduler, build_job("hog", "h100", priority=10, quota="team-train"))
    assert resp.status_code == 201

    wait_for(
        lambda: (
            (s := get_status(scheduler, "replica-0")) is not None and s.get("phase") == "suspended"
        ),
        timeout=45,
        desc="replica-0 suspended",
    )

    # Free capacity.
    delete_k8s_workload(k8s_clients, "hog")

    # Deployment pod should be re-placed.
    wait_for(
        lambda: (
            (s := get_status(scheduler, "replica-0")) is not None
            and s.get("phase") in ("running", "assigning")
        ),
        timeout=45,
        desc="replica-0 re-placed after preemptor removed",
    )


# ---------------------------------------------------------------------------
# Mixed workloads: deployment Pods coexist with gang-scheduled Jobs
# ---------------------------------------------------------------------------


def test_deployment_pods_coexist_with_jobs(scheduler, k8s_clients):
    """Deployment Pods and Jobs share a cluster without interfering."""
    # Job uses 4 chips.
    resp = submit_job(
        scheduler, build_job("batch", "h100", priority=5, quota="team-train", chips=4)
    )
    assert resp.status_code == 201

    # Deployment pod uses remaining 4 chips.
    resp = submit_pod(
        scheduler, build_pod("serving", "h100", priority=5, quota="team-train", chips=4)
    )
    assert resp.status_code == 201

    wait_for(
        lambda: (s := get_status(scheduler, "batch")) is not None and s.get("phase") == "running",
        desc="batch job placed",
    )
    wait_for(
        lambda: (s := get_status(scheduler, "serving")) is not None and s.get("phase") == "running",
        desc="serving pod placed",
    )

    # Both placed on cluster-a (h100).
    pods = get_pods_on_cluster(k8s_clients, "cluster-a")
    pod_names = {p.metadata.labels.get("scheduler.example.com/job-name") for p in pods}
    # The job creates child pods too, so just check the deployment pod is there.
    assert "serving" in pod_names
