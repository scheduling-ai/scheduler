"""
Preemption at the k8s level: verify suspend toggle, child pod termination,
and guarantee protection through actual k8s objects.
"""

import pytest

from conftest import (
    build_job,
    delete_k8s_workload,
    get_job_by_name,
    submit_job,
    wait_for,
    wait_for_not,
)

pytestmark = pytest.mark.e2e


def test_preemption_suspends_k8s_job(scheduler, k8s_clients):
    """Low-priority borrower gets spec.suspend=true when high-priority arrives."""
    # team-serve has guarantee=0 h100 on cluster-a, so this 4-chip job is a borrower.
    submit_job(scheduler, build_job("borrower", "h100", priority=1, quota="team-serve", chips=4))
    wait_for(
        lambda: (
            (j := get_job_by_name(k8s_clients, "cluster-a", "borrower")) is not None
            and j.spec.suspend is False
        ),
        desc="borrower placed",
    )

    # Submit high-priority team-train job that needs 8 chips (full node).
    # team-train guarantee=4, but borrower is a pure borrower → preempted.
    submit_job(scheduler, build_job("hog", "h100", priority=10, quota="team-train"))

    def borrower_suspended():
        j = get_job_by_name(k8s_clients, "cluster-a", "borrower")
        return j is not None and j.spec.suspend is True

    wait_for(borrower_suspended, timeout=45, desc="borrower suspended")


def test_preempted_job_unsuspends_when_capacity_frees(scheduler, k8s_clients):
    """Preempted job resumes after the preemptor is deleted."""
    submit_job(scheduler, build_job("resumable", "h100", priority=1, quota="team-serve", chips=4))
    wait_for(
        lambda: (
            (j := get_job_by_name(k8s_clients, "cluster-a", "resumable")) is not None
            and j.spec.suspend is False
        ),
        desc="resumable placed",
    )

    # Preempt it with a higher-priority job.
    submit_job(scheduler, build_job("preemptor", "h100", priority=10, quota="team-train"))

    wait_for(
        lambda: (
            (j := get_job_by_name(k8s_clients, "cluster-a", "resumable")) is not None
            and j.spec.suspend is True
        ),
        timeout=45,
        desc="resumable suspended",
    )

    # Free capacity by deleting from k8s directly.
    delete_k8s_workload(k8s_clients, "preemptor")

    def job_unsuspended():
        j = get_job_by_name(k8s_clients, "cluster-a", "resumable")
        return j is not None and j.spec.suspend is False

    wait_for(job_unsuspended, timeout=45, desc="resumable unsuspended")


def test_within_guarantee_not_preempted(scheduler, k8s_clients):
    """Job within its quota guarantee (different quota) must NOT be preempted.

    Uses Kueue's Consistently pattern: verify the job stays running for
    multiple solver cycles.
    """
    # Submit a team-train job (guarantee=4 on cluster-a). Within guarantee.
    submit_job(scheduler, build_job("guaranteed", "h100", priority=1, quota="team-train", chips=4))
    wait_for(
        lambda: (
            (j := get_job_by_name(k8s_clients, "cluster-a", "guaranteed")) is not None
            and j.spec.suspend is False
        ),
        desc="guaranteed placed",
    )

    # Submit team-serve job that also needs h100. team-serve guarantee=0 (borrower).
    submit_job(scheduler, build_job("serve-job", "h100", priority=1, quota="team-serve", chips=4))
    wait_for(
        lambda: (
            (j := get_job_by_name(k8s_clients, "cluster-a", "serve-job")) is not None
            and j.spec.suspend is False
        ),
        desc="serve-job placed",
    )

    # Now cluster is full (8 chips). Submit a very high priority team-serve job.
    # It CANNOT preempt guaranteed (different quota, within guarantee).
    submit_job(
        scheduler, build_job("aggressive", "h100", priority=100, quota="team-serve", chips=4)
    )

    # guaranteed must stay running (Consistently).
    def guaranteed_suspended():
        j = get_job_by_name(k8s_clients, "cluster-a", "guaranteed")
        return j is not None and j.spec.suspend is True

    wait_for_not(guaranteed_suspended, duration=12, desc="guaranteed must not be preempted")
