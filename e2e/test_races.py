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
    build_pod,
    delete_k8s_workload,
    delete_workload,
    find_free_port,
    get_job_by_name,
    get_jobs_on_cluster,
    get_status,
    submit_job,
    submit_pod,
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


def test_pod_resubmit_during_suspension(scheduler, k8s_clients):
    """Pod resubmit during suspension: generation counter prevents stale removal.

    Sequence:
    1. Submit pod → placed on cluster
    2. High-priority job preempts pod → pod deleted, re-enters store
    3. DELETE + resubmit same pod name
    4. Next solver cycle places the new workload correctly

    Regression test for generation counter on suspended pod re-entry.
    """
    # Fill most of cluster-a so there's room for exactly one 8-chip pod.
    resp = submit_pod(scheduler, build_pod("victim", "h100", priority=1))
    assert resp.status_code == 201

    wait_for(
        lambda: (s := get_status(scheduler, "victim")) is not None and s.get("phase") == "running",
        desc="victim placed",
    )

    # Submit high-priority job to preempt the pod.
    resp = submit_job(scheduler, build_job("bully", "h100", priority=100))
    assert resp.status_code == 201

    wait_for(
        lambda: (
            (s := get_status(scheduler, "victim")) is not None and s.get("phase") == "suspended"
        ),
        timeout=45,
        desc="victim suspended",
    )

    # Delete victim and resubmit immediately with same name.
    delete_workload(scheduler, "victim")
    resp = submit_pod(scheduler, build_pod("victim", "h100", priority=1))
    assert resp.status_code == 201

    # Let the binder run 2-3 cycles so the generation counter race plays out.
    # If the counter is broken, the old victim's cleanup removes the new entry.
    time.sleep(12)

    # Now free capacity by removing bully from both store and k8s.
    delete_workload(scheduler, "bully")
    delete_k8s_workload(k8s_clients, "bully")

    # The new victim should be placed once bully's capacity is freed.
    # If the generation race caused it to be silently removed from the store,
    # it will stay queued forever (or disappear entirely).
    wait_for(
        lambda: (
            (s := get_status(scheduler, "victim")) is not None
            and s.get("phase") in ("running", "assigning")
        ),
        timeout=45,
        desc="resubmitted victim placed (generation race survived)",
    )


def test_scheduler_restart_with_existing_cluster_objects(rust_binary, kind_clusters, k8s_clients):
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
            env=SOLVER_ENV,
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
