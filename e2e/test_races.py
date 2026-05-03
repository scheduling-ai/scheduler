"""
Race condition tests: concurrent operations, scheduler restart, mid-flight
deletion.
"""

import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
import requests

from conftest import (
    CLUSTER_A,
    CLUSTER_B,
    QUOTAS_PATH,
    CHIP_RESOURCE,
    SOLVER_ENV,
    Scheduler,
    build_job,
    delete_workload,
    find_free_port,
    get_job_by_name,
    get_jobs_on_cluster,
    submit_job,
    wait_for,
    wait_for_not,
)

pytestmark = pytest.mark.e2e


def test_delete_during_placement(scheduler, k8s_clients):
    """Submit a job then immediately DELETE it. No orphaned k8s object."""
    resp = submit_job(scheduler, build_job("ephemeral", "h100", priority=5))
    assert resp.status_code == 201
    delete_workload(scheduler, "ephemeral")

    # Job should not appear on any cluster after 2 solver cycles.
    # Note: current implementation may leave orphans if deletion races with placement.
    # This test documents the behavior — if ephemeral appears, it's a known issue.
    def ephemeral_placed():
        jobs_a = get_jobs_on_cluster(k8s_clients, "cluster-a")
        jobs_b = get_jobs_on_cluster(k8s_clients, "cluster-b")
        all_names = [
            j.metadata.labels.get("scheduler.example.com/job-name") for j in jobs_a + jobs_b
        ]
        return "ephemeral" in all_names

    wait_for_not(ephemeral_placed, duration=12, desc="ephemeral must not be placed")


# test_pod_resubmit_during_suspension was deleted: bare-Pod submission via
# the bridge API is no longer supported in v0, and the suspended-pod-store
# semantics it tested no longer exist (Pod preemption is delete-only;
# owner controllers respawn).


def test_scheduler_restart_with_existing_cluster_objects(
    rust_binary, kind_clusters, k8s_clients, postgres_url
):
    """Scheduler restarts after placing jobs. Must not crash or duplicate."""
    port = find_free_port()
    restart_tmp = Path(tempfile.mkdtemp(prefix="scheduler-restart-"))
    record_path = restart_tmp / "restart-session.jsonl"

    def start_scheduler():
        proc = subprocess.Popen(
            [
                str(rust_binary),
                "serve",
                "--cluster",
                f"cluster-a:kind-{CLUSTER_A}",
                "--cluster",
                f"cluster-b:kind-{CLUSTER_B}",
                "--port",
                str(port),
                "--quotas",
                str(QUOTAS_PATH),
                "--chip-label",
                "accelerator",
                "--chip-resource",
                CHIP_RESOURCE,
                "--record",
                str(record_path),
                "--solver",
                "milp",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**SOLVER_ENV, "DATABASE_URL": postgres_url},
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                requests.get(f"http://localhost:{port}/jobs", timeout=1)
                return proc
            except requests.ConnectionError:
                time.sleep(0.5)
        proc.kill()
        raise TimeoutError("Scheduler did not start")

    # First run: submit and place a job.
    proc1 = start_scheduler()
    sched = Scheduler(proc=proc1, base_url=f"http://localhost:{port}", record_path=record_path)
    try:
        submit_job(sched, build_job("survivor", "h100", priority=5))
        wait_for(
            lambda: get_job_by_name(k8s_clients, "cluster-a", "survivor") is not None,
            desc="survivor placed",
        )
    finally:
        proc1.terminate()
        proc1.wait(timeout=10)

    # Second run: same port, same clusters. Reflectors discover existing job.
    port = find_free_port()  # new port since old one might be in TIME_WAIT
    proc2 = start_scheduler()
    try:
        # Scheduler should not crash. Give it time to run a solver cycle.
        time.sleep(8)
        assert proc2.poll() is None, "Scheduler crashed on restart"

        # survivor should still exist on cluster, not duplicated.
        jobs = get_jobs_on_cluster(k8s_clients, "cluster-a")
        survivor_count = sum(
            1
            for j in jobs
            if (j.metadata.labels or {}).get("scheduler.example.com/job-name") == "survivor"
        )
        assert survivor_count == 1, f"Expected 1 survivor, found {survivor_count}"
    finally:
        proc2.terminate()
        proc2.wait(timeout=10)
        shutil.rmtree(restart_tmp, ignore_errors=True)


def test_queued_workload_survives_bridge_restart(
    rust_binary, kind_clusters, k8s_clients, postgres_url
):
    """A queued (un-placeable) workload must come back after a bridge SIGKILL.

    Submits a workload pinned to a chip type that no kind cluster has, so the
    solver provably cannot place it.  The workload sits in the queue; we
    SIGKILL the bridge (no graceful drain), restart on the same Postgres URL,
    and assert the workload is still listed.  This exercises the persistence
    write-through + restore path end-to-end.
    """
    tmp = Path(tempfile.mkdtemp(prefix="scheduler-persist-"))
    record_path = tmp / "session.jsonl"

    def start_bridge(p):
        proc = subprocess.Popen(
            [
                str(rust_binary),
                "serve",
                "--cluster",
                f"cluster-a:kind-{CLUSTER_A}",
                "--cluster",
                f"cluster-b:kind-{CLUSTER_B}",
                "--port",
                str(p),
                "--quotas",
                str(QUOTAS_PATH),
                "--chip-label",
                "accelerator",
                "--chip-resource",
                CHIP_RESOURCE,
                "--record",
                str(record_path),
                "--solver",
                "milp",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**SOLVER_ENV, "DATABASE_URL": postgres_url},
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                requests.get(f"http://localhost:{p}/jobs", timeout=1)
                return proc
            except requests.ConnectionError:
                time.sleep(0.5)
        proc.kill()
        raise TimeoutError("bridge did not start")

    # Run 1 — submit unplaceable workload.
    port = find_free_port()
    proc1 = start_bridge(port)
    base = f"http://localhost:{port}"
    try:
        # h200 isn't on any kind cluster → solver can't place → stays queued.
        sched = Scheduler(proc=proc1, base_url=base, record_path=record_path)
        resp = submit_job(sched, build_job("queue-survivor", "h200", priority=1))
        assert resp.status_code == 201
        wait_for(
            lambda: "queue-survivor" in requests.get(f"{base}/jobs", timeout=5).json(),
            desc="workload listed in /jobs",
        )
    finally:
        proc1.kill()  # SIGKILL: no graceful drain, no late writes.
        proc1.wait(timeout=10)

    # Run 2 — same DATABASE_URL, fresh process, fresh in-memory map.
    port = find_free_port()
    proc2 = start_bridge(port)
    base = f"http://localhost:{port}"
    try:
        names = requests.get(f"{base}/jobs", timeout=5).json()
        assert "queue-survivor" in names, f"queued workload lost; /jobs={names}"
        # Cleanup so this row doesn't leak across the session-scoped Postgres.
        requests.delete(f"{base}/jobs/queue-survivor", timeout=5)
    finally:
        proc2.terminate()
        proc2.wait(timeout=10)
        shutil.rmtree(tmp, ignore_errors=True)
