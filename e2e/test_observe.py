"""
E2E for `k8s-bridge serve --mode=observe`: reflect a real cluster's
state and serve a Frame snapshot — no scheduling, no binding.

Validates the path that lets the UI ship as a standalone product against
clusters scheduled by Kueue, kube-scheduler, or anything else.

Why an e2e at all: the observe-mode unit tests cover the snapshot
builder over fake reflector stores, but they can't catch:
  - reflector init order against a real apiserver
  - the actual HTTP /snapshot route serving JSON
  - the snapshot picking up workloads scheduled by something other
    than our binder

This test exercises all three by applying a plain Job (no managed-by
label, kube-scheduler binds the pod) and asserting it surfaces in the
observe bridge's snapshot.
"""

from __future__ import annotations

import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
import requests
from kubernetes import client

from conftest import (
    CHIP_RESOURCE,
    CHIPS_PER_NODE,
    CLUSTER_A,
    SCHEDULER_LOG_DIR,
    find_free_port,
    wait_for,
)

pytestmark = pytest.mark.e2e


@contextmanager
def _observe_bridge(rust_binary: Path):
    """Run `k8s-bridge serve --mode=observe` against e2e-cluster-a.

    Logs to $E2E_LOG_DIR/observe.log so a failure has the same
    forensic surface as the schedule-mode bridge.
    """
    port = find_free_port()
    SCHEDULER_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = SCHEDULER_LOG_DIR / "observe.log"
    log_file = log_path.open("wb")

    proc = subprocess.Popen(
        [
            str(rust_binary),
            "serve",
            "--mode",
            "observe",
            "--cluster",
            f"observed:kind-{CLUSTER_A}",
            "--port",
            str(port),
            "--chip-label",
            "accelerator",
            "--chip-resource",
            CHIP_RESOURCE,
            "--interval-seconds",
            "1",
            "--snapshot-label",
            "test-observed",
        ],
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )

    base_url = f"http://localhost:{port}"
    deadline = time.monotonic() + 30
    last_status: int | None = None
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{base_url}/snapshot", timeout=1)
            last_status = r.status_code
            # 404 is fine — server is up, snapshot just hasn't been
            # built yet.  200 means we're fully ready.
            if r.status_code in (200, 404):
                break
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    else:
        proc.kill()
        log_file.close()
        raise TimeoutError(
            f"observe bridge did not start within 30s; last status={last_status}; see {log_path}"
        )

    try:
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log_file.close()


def _build_unmanaged_job(name: str, chip_type: str) -> client.V1Job:
    """A Job with no scheduler-specific labels.

    Mirrors what a Kueue user submits: the queue label routes the job
    through Kueue's admission, kube-scheduler binds the pods, but our
    binder has no claim on the workload.  The observe bridge should
    still see it.
    """
    return client.V1Job(
        metadata=client.V1ObjectMeta(name=name, namespace="default"),
        spec=client.V1JobSpec(
            suspend=False,
            parallelism=1,
            completions=1,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"accelerator": chip_type}),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    tolerations=[
                        client.V1Toleration(
                            key="scheduler",
                            operator="Equal",
                            value="custom",
                            effect="NoSchedule",
                        )
                    ],
                    containers=[
                        client.V1Container(
                            name="test",
                            image="busybox:1.36",
                            command=["sleep", "3600"],
                            resources=client.V1ResourceRequirements(
                                requests={CHIP_RESOURCE: str(CHIPS_PER_NODE)},
                                limits={CHIP_RESOURCE: str(CHIPS_PER_NODE)},
                            ),
                        )
                    ],
                ),
            ),
        ),
    )


def test_observe_mode_picks_up_kubectl_applied_job(rust_binary, k8s_clients):
    """An unmanaged Job applied directly to the cluster must surface in
    the observe bridge's /snapshot once kube-scheduler binds its pod.

    Asserts the full pipeline against a real apiserver:
      - reflectors initialize and InitDone fires
      - snapshot loop runs
      - HTTP /snapshot serves the Frame JSON
      - solver_status is omitted (no solver)
      - the Job surfaces with the right node assignment + cluster name
    """
    batch = k8s_clients["cluster-a"]["batch"]
    job_name = "observe-e2e-job"

    # Make sure no leftover from a prior run.
    try:
        batch.delete_namespaced_job(
            job_name,
            "default",
            body=client.V1DeleteOptions(
                propagation_policy="Background",
                grace_period_seconds=0,
            ),
        )
    except client.ApiException:
        pass

    job = _build_unmanaged_job(job_name, "h100")
    batch.create_namespaced_job("default", job)

    try:
        with _observe_bridge(rust_binary) as bridge_url:

            def snapshot_has_job() -> bool:
                r = requests.get(f"{bridge_url}/snapshot", timeout=2)
                if r.status_code != 200:
                    return False
                snap = r.json()
                pod = snap.get("pods", {}).get(job_name)
                if pod is None:
                    return False
                # Wait until kube-scheduler has actually bound the pod
                # (otherwise statuses_by_replica[0].node is None and the
                # snapshot is technically right but uninteresting).
                statuses = pod.get("statuses_by_replica") or []
                return any(s.get("node") for s in statuses)

            wait_for(
                snapshot_has_job,
                timeout=60,
                desc=f"{job_name} surfaces in /snapshot with a node assignment",
            )

            snap = requests.get(f"{bridge_url}/snapshot", timeout=2).json()

            # Frame omits solver fields — observe mode publishes None,
            # serde drops the keys via skip_serializing_if.
            assert "solver_status" not in snap, (
                "solver_status must be absent in observe-mode Frame; "
                f"got: {snap.get('solver_status')!r}"
            )
            assert "solver_duration_ms" not in snap, (
                "solver_duration_ms must be absent in observe-mode Frame"
            )
            assert snap["scheduler"] == "test-observed", (
                f"Frame.scheduler should match --snapshot-label, got: {snap['scheduler']!r}"
            )

            pod = snap["pods"][job_name]
            assert pod["cluster"] == "observed", (
                f"pod must be cluster-pinned to the observed cluster, got {pod['cluster']!r}"
            )
            assert pod["statuses_by_replica"][0]["node"], (
                "pod must have a node assignment from kube-scheduler"
            )

            # The cluster snapshot must include the worker nodes from
            # this cluster.  Smoke-checks the node reflector.
            cluster_names = [c["name"] for c in snap["clusters"]]
            assert cluster_names == ["observed"], (
                f"expected single 'observed' cluster, got: {cluster_names}"
            )
            assert snap["clusters"][0]["nodes"], "expected at least one node in snapshot"

            # Default --exclude-namespace list must filter kube-system
            # noise out of the snapshot.  Without this, the UI would
            # be flooded with kube-apiserver / etcd / kube-proxy /
            # kindnet pods on a real cluster.
            kube_system_pods = [
                name
                for name in snap["pods"]
                if name.startswith(("kube-", "etcd-", "kindnet", "coredns", "local-path"))
            ]
            assert not kube_system_pods, (
                f"kube-system pods must be filtered by --exclude-namespace; got: {kube_system_pods}"
            )

    finally:
        try:
            batch.delete_namespaced_job(
                job_name,
                "default",
                body=client.V1DeleteOptions(
                    propagation_policy="Background",
                    grace_period_seconds=0,
                ),
            )
        except client.ApiException:
            pass
