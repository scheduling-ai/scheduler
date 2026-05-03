"""
Deployment workload tests: Pods created at the cluster level (KEDA path)
that the bridge picks up via reflector.

In v0 we don't accept bare Pods through the HTTP API.  Real autoscaled
inference comes from a Deployment / ReplicaSet / KEDA scaling target.
We model that here by creating Pods directly on the cluster.

What's *not* tested anymore (deliberate v0 cut):
- Pod suspended-and-resumed-on-the-same-cluster: the bridge no longer
  stores Pod specs.  Preemption is just deletion; the owner controller
  (a real Deployment, in production) is responsible for respawning.
"""

import pytest

from conftest import (
    build_job,
    create_pod_on_cluster,
    get_pods_on_cluster,
    get_status,
    submit_job,
    wait_for,
    wait_for_not,
)

pytestmark = pytest.mark.e2e


def test_reflector_discovers_externally_created_pod(scheduler, k8s_clients):
    """Pod created directly on k8s (as KEDA / a Deployment would) is picked
    up by the reflector and counts against cluster capacity.
    """
    create_pod_on_cluster(
        k8s_clients, "cluster-a", "keda-0", "h100", priority=5, quota="team-train", chips=4
    )

    # Wait for the k8s scheduler to bind keda-0 to a node (spec.nodeName set).
    # The bridge only counts a pod's chips as occupying a node once nodeName
    # is known; if we proceed before binding completes, the solver sees 8
    # free chips and over-places.
    wait_for(
        lambda: any(
            p.metadata.name == "keda-0" and p.spec and p.spec.node_name is not None
            for p in get_pods_on_cluster(k8s_clients, "cluster-a")
        ),
        desc="keda-0 bound to a node on cluster-a",
    )

    # A 4-chip Job should fit on the remaining half-node.
    submit_job(scheduler, build_job("needs-space", "h100", priority=5, quota="team-train", chips=4))
    wait_for(
        lambda: (
            (s := get_status(scheduler, "needs-space")) is not None and s.get("phase") == "running"
        ),
        desc="needs-space placed alongside keda-0",
    )

    # A second 4-chip Job at lower priority must NOT fit (cluster full).
    submit_job(scheduler, build_job("overflow", "h100", priority=1, quota="team-train", chips=4))
    wait_for_not(
        lambda: (
            (s := get_status(scheduler, "overflow")) is not None and s.get("phase") == "running"
        ),
        duration=12,
        desc="overflow must not be placed (cluster full)",
    )


def test_multiple_deployment_pods_share_a_node(scheduler, k8s_clients):
    """Four 2-chip pods (KEDA-style) coexist on the single 8-chip node."""
    for i in range(4):
        create_pod_on_cluster(
            k8s_clients,
            "cluster-a",
            f"web-{i}",
            "h100",
            priority=5,
            quota="team-train",
            chips=2,
        )

    for i in range(4):
        wait_for(
            lambda i=i: any(
                p.metadata.name == f"web-{i}" and p.spec and p.spec.node_name is not None
                for p in get_pods_on_cluster(k8s_clients, "cluster-a")
            ),
            desc=f"web-{i} bound",
        )


def test_job_preempts_deployment_pod(scheduler, k8s_clients):
    """A high-priority Job preempts a low-priority deployment Pod.

    After the refactor, preemption *deletes* the Pod — there's no
    suspended state in the bridge.  In production a real Deployment
    controller would respawn it; here, with no owner, the Pod stays gone.
    """
    create_pod_on_cluster(
        k8s_clients, "cluster-a", "dep-0", "h100", priority=1, quota="team-serve", chips=4
    )
    wait_for(
        lambda: any(
            p.metadata.name == "dep-0" and p.spec and p.spec.node_name is not None
            for p in get_pods_on_cluster(k8s_clients, "cluster-a")
        ),
        desc="dep-0 bound",
    )

    # Submit high-priority Job that needs the full node (8 chips).
    submit_job(scheduler, build_job("train", "h100", priority=10, quota="team-train"))

    # dep-0 should be gone from the cluster (preempted via deletion).
    wait_for(
        lambda: (
            not any(
                p.metadata.name == "dep-0" for p in get_pods_on_cluster(k8s_clients, "cluster-a")
            )
        ),
        timeout=45,
        desc="dep-0 deleted by train preemption",
    )


def test_deployment_pods_coexist_with_jobs(scheduler, k8s_clients):
    """A Job and a deployment Pod share the cluster without interfering."""
    submit_job(scheduler, build_job("batch", "h100", priority=5, quota="team-train", chips=4))
    create_pod_on_cluster(
        k8s_clients, "cluster-a", "serving", "h100", priority=5, quota="team-train", chips=4
    )

    wait_for(
        lambda: (s := get_status(scheduler, "batch")) is not None and s.get("phase") == "running",
        desc="batch job placed",
    )
    wait_for(
        lambda: any(
            p.metadata.name == "serving" and p.spec and p.spec.node_name is not None
            for p in get_pods_on_cluster(k8s_clients, "cluster-a")
        ),
        desc="serving pod bound",
    )
