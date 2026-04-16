"""
E2E test fixtures for the multi-cluster scheduler.

Sets up 2 kind clusters with fake GPU extended resources, builds the Rust
binary, and provides helpers for submitting workloads and asserting k8s state.

Requires: kind, cargo, kubectl on PATH.
"""

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pytest
import requests
from kubernetes import client, config

E2E_DIR = Path(__file__).parent
REPO_ROOT = E2E_DIR.parent
QUOTAS_PATH = E2E_DIR / "quotas.json"
SOLVER_ENV = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "py-scheduler")}

CLUSTER_A = "e2e-cluster-a"
CLUSTER_B = "e2e-cluster-b"
CLUSTERS = {
    "cluster-a": {
        "kind_name": CLUSTER_A,
        "chip_type": "h100",
        "config": E2E_DIR / "kind-cluster-a.yaml",
    },
    "cluster-b": {
        "kind_name": CLUSTER_B,
        "chip_type": "a100",
        "config": E2E_DIR / "kind-cluster-b.yaml",
    },
}

CHIP_RESOURCE = "example.com/gpu"
CHIPS_PER_NODE = 8
MANAGED_BY_LABEL = "scheduler.example.com/managed-by"
MANAGED_BY_VALUE = "custom-scheduler"
JOB_NAME_LABEL = "scheduler.example.com/job-name"

PLACEMENT_TIMEOUT = 60
SHORT_POLL = 2


# ---------------------------------------------------------------------------
# Wait helpers (Kueue-inspired Eventually / Consistently)
# ---------------------------------------------------------------------------


def wait_for(
    condition: Callable[[], bool],
    timeout: float = PLACEMENT_TIMEOUT,
    interval: float = SHORT_POLL,
    desc: str = "",
) -> None:
    """Poll condition until True or timeout (Kueue's Eventually)."""
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            if condition():
                return
        except Exception as e:
            last_err = e
        time.sleep(interval)
    msg = f"Timed out ({timeout}s): {desc}"
    if last_err:
        msg += f" (last error: {last_err})"
    raise TimeoutError(msg)


def wait_for_not(
    condition: Callable[[], bool],
    duration: float = 10,
    interval: float = SHORT_POLL,
    desc: str = "",
) -> None:
    """Assert condition stays False for duration (Kueue's Consistently)."""
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        assert not condition(), f"Unexpected condition became true: {desc}"
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Scheduler handle
# ---------------------------------------------------------------------------


@dataclass
class Scheduler:
    proc: subprocess.Popen
    base_url: str
    record_path: Path


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Workload builders
# ---------------------------------------------------------------------------


def build_job(
    name: str,
    chip_type: str,
    priority: int = 0,
    quota: str = "team-train",
    parallelism: int = 1,
    chips: int | None = None,
) -> dict:
    """Build a Job manifest for the scheduler API."""
    chip_count = chips if chips is not None else CHIPS_PER_NODE
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": {
                "accelerator": chip_type,
                JOB_NAME_LABEL: name,
            },
            "annotations": {
                "scheduler.example.com/priority": str(priority),
                "scheduler.example.com/quota": quota,
            },
        },
        "spec": {
            "suspend": True,
            "parallelism": parallelism,
            "completions": parallelism,
            "template": {
                "spec": {
                    "tolerations": [
                        {
                            "key": "scheduler",
                            "operator": "Equal",
                            "value": "custom",
                            "effect": "NoSchedule",
                        }
                    ],
                    "containers": [
                        {
                            "name": "test",
                            "image": "busybox:1.36",
                            "command": ["sleep", "3600"],
                            "resources": {
                                "requests": {CHIP_RESOURCE: str(chip_count)},
                                "limits": {CHIP_RESOURCE: str(chip_count)},
                            },
                        }
                    ],
                    "restartPolicy": "Never",
                }
            },
        },
    }


def build_pod(
    name: str,
    chip_type: str,
    priority: int = 0,
    quota: str = "team-train",
    chips: int | None = None,
) -> dict:
    """Build a Pod manifest for the scheduler API."""
    chip_count = chips if chips is not None else CHIPS_PER_NODE
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": {
                "accelerator": chip_type,
                JOB_NAME_LABEL: name,
            },
            "annotations": {
                "scheduler.example.com/priority": str(priority),
                "scheduler.example.com/quota": quota,
            },
        },
        "spec": {
            "tolerations": [
                {"key": "scheduler", "operator": "Equal", "value": "custom", "effect": "NoSchedule"}
            ],
            "containers": [
                {
                    "name": "test",
                    "image": "busybox:1.36",
                    "command": ["sleep", "3600"],
                    "resources": {
                        "requests": {CHIP_RESOURCE: str(chip_count)},
                        "limits": {CHIP_RESOURCE: str(chip_count)},
                    },
                }
            ],
            "restartPolicy": "Never",
        },
    }


# ---------------------------------------------------------------------------
# Scheduler HTTP helpers
# ---------------------------------------------------------------------------


def submit_job(sched: Scheduler, manifest: dict) -> requests.Response:
    return requests.post(f"{sched.base_url}/jobs", json=manifest, timeout=5)


def submit_pod(sched: Scheduler, manifest: dict) -> requests.Response:
    return requests.post(f"{sched.base_url}/jobs", json=manifest, timeout=5)


def list_workloads(sched: Scheduler) -> list[str]:
    return requests.get(f"{sched.base_url}/jobs", timeout=5).json()


def delete_workload(sched: Scheduler, name: str) -> int:
    return requests.delete(f"{sched.base_url}/jobs/{name}", timeout=5).status_code


def delete_k8s_workload(k8s_clients: dict, name: str) -> None:
    """Delete a managed workload directly from k8s clusters.

    The scheduler API only removes from its internal store. Once a workload is
    placed on a cluster, it must be deleted from k8s directly to free capacity.
    Waits for objects to actually disappear so the reflector sees freed capacity.
    """
    label = f"{JOB_NAME_LABEL}={name}"
    found_any = False
    for cluster_name, apis in k8s_clients.items():
        try:
            jobs = apis["batch"].list_namespaced_job("default", label_selector=label)
            for job in jobs.items:
                found_any = True
                apis["batch"].delete_namespaced_job(
                    job.metadata.name,
                    "default",
                    body=client.V1DeleteOptions(
                        propagation_policy="Background",
                        grace_period_seconds=0,
                    ),
                )
        except Exception:
            pass
        try:
            pods = apis["core"].list_namespaced_pod("default", label_selector=label)
            for pod in pods.items:
                found_any = True
                apis["core"].delete_namespaced_pod(
                    pod.metadata.name,
                    "default",
                    body=client.V1DeleteOptions(grace_period_seconds=0),
                )
        except Exception:
            pass

    if not found_any:
        return

    # Wait for objects to actually disappear so the reflector sees freed capacity.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        all_gone = True
        for cluster_name, apis in k8s_clients.items():
            try:
                jobs = apis["batch"].list_namespaced_job("default", label_selector=label)
                pods = apis["core"].list_namespaced_pod("default", label_selector=label)
                if jobs.items or pods.items:
                    all_gone = False
                    break
            except Exception:
                pass
        if all_gone:
            break
        time.sleep(1)


def get_status(sched: Scheduler, name: str) -> dict | None:
    resp = requests.get(f"{sched.base_url}/status/{name}", timeout=5)
    return resp.json() if resp.ok else None


# ---------------------------------------------------------------------------
# K8s query helpers
# ---------------------------------------------------------------------------


def get_jobs_on_cluster(clients: dict, cluster: str, namespace: str = "default") -> list:
    """Get all managed jobs on a cluster."""
    batch_api = clients[cluster]["batch"]
    jobs = batch_api.list_namespaced_job(
        namespace, label_selector=f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"
    )
    return jobs.items


def get_job_by_name(clients: dict, cluster: str, name: str, namespace: str = "default") -> Any:
    """Get a specific managed job by job-name label."""
    batch_api = clients[cluster]["batch"]
    jobs = batch_api.list_namespaced_job(namespace, label_selector=f"{JOB_NAME_LABEL}={name}")
    return jobs.items[0] if jobs.items else None


def get_pods_on_cluster(clients: dict, cluster: str, namespace: str = "default") -> list:
    """Get all managed pods on a cluster."""
    core_api = clients[cluster]["core"]
    pods = core_api.list_namespaced_pod(
        namespace, label_selector=f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"
    )
    return pods.items


# ---------------------------------------------------------------------------
# Kind cluster management
# ---------------------------------------------------------------------------


def _cluster_exists(kind_name: str) -> bool:
    result = subprocess.run(
        ["kind", "get", "clusters"], capture_output=True, text=True, check=False
    )
    return kind_name in result.stdout.splitlines()


def _create_cluster(name: str, info: dict) -> None:
    kind_name = info["kind_name"]
    if _cluster_exists(kind_name):
        return

    result = subprocess.run(
        ["kind", "create", "cluster", "--config", str(info["config"]), "--name", kind_name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"kind create cluster {kind_name} failed:\n{result.stderr}\n{result.stdout}"
        )

    context = f"kind-{kind_name}"
    kubectl = ["kubectl", "--context", context]

    # Get schedulable nodes. Prefer dedicated workers; fall back to control-plane
    # if the cluster has no workers (control-plane doubles as worker).
    result = subprocess.run(
        [*kubectl, "get", "nodes", "--no-headers", "-o", "custom-columns=:metadata.name"],
        capture_output=True,
        text=True,
        check=True,
    )
    all_nodes = [n.strip() for n in result.stdout.splitlines() if n.strip()]
    workers = [n for n in all_nodes if "worker" in n]
    if not workers:
        # Control-plane-only cluster: remove the NoSchedule taint so it can run pods.
        for node in all_nodes:
            subprocess.run(
                [
                    *kubectl,
                    "taint",
                    "node",
                    node,
                    "node-role.kubernetes.io/control-plane:NoSchedule-",
                ],
                capture_output=True,
                check=False,  # may already be untainted
            )
        workers = all_nodes

    for worker in workers:
        # Label with chip type.
        subprocess.run(
            [*kubectl, "label", "node", worker, f"accelerator={info['chip_type']}", "--overwrite"],
            capture_output=True,
            check=True,
        )
        # Taint for scheduler.
        subprocess.run(
            [*kubectl, "taint", "node", worker, "scheduler=custom:NoSchedule", "--overwrite"],
            capture_output=True,
            check=True,
        )
        # Patch extended resource onto node status.
        patch = json.dumps(
            [
                {
                    "op": "add",
                    "path": f"/status/capacity/{CHIP_RESOURCE.replace('/', '~1')}",
                    "value": str(CHIPS_PER_NODE),
                }
            ]
        )
        subprocess.run(
            [*kubectl, "patch", "node", worker, "--subresource=status", "--type=json", "-p", patch],
            capture_output=True,
            check=True,
        )


def _delete_cluster(kind_name: str) -> None:
    if _cluster_exists(kind_name):
        subprocess.run(["kind", "delete", "cluster", "--name", kind_name], capture_output=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kind_clusters():
    """Create 2 kind clusters sequentially. Tear down after session."""
    for name, info in CLUSTERS.items():
        _create_cluster(name, info)

    yield CLUSTERS

    for info in CLUSTERS.values():
        _delete_cluster(str(info["kind_name"]))


@pytest.fixture(scope="session")
def rust_binary():
    """Build the k8s-bridge binary."""
    subprocess.run(
        ["cargo", "build", "--release"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    binary = REPO_ROOT / "target" / "release" / "k8s-bridge"
    assert binary.exists(), f"Binary not found at {binary}"
    return binary


@pytest.fixture(scope="session")
def k8s_clients(kind_clusters):
    """Return {cluster_name: {core: CoreV1Api, batch: BatchV1Api}} per cluster."""
    clients = {}
    for name, info in kind_clusters.items():
        context = f"kind-{info['kind_name']}"
        api_client = config.new_client_from_config(context=context)
        clients[name] = {
            "core": client.CoreV1Api(api_client),
            "batch": client.BatchV1Api(api_client),
        }
    return clients


@pytest.fixture(scope="session")
def scheduler(rust_binary, kind_clusters):
    """Start a single scheduler process for the entire session. Kill on teardown."""
    port = find_free_port()
    sched_tmp = Path(tempfile.mkdtemp(prefix="scheduler-"))
    record_path = sched_tmp / "session.jsonl"

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

    base_url = f"http://localhost:{port}"
    # Wait for HTTP ready.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            requests.get(f"{base_url}/jobs", timeout=1)
            break
        except requests.ConnectionError:
            time.sleep(0.5)
    else:
        proc.kill()
        raise TimeoutError("Scheduler did not start within 30s")

    yield Scheduler(proc=proc, base_url=base_url, record_path=record_path)

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    shutil.rmtree(sched_tmp, ignore_errors=True)


@pytest.fixture(autouse=True)
def clean_clusters(k8s_clients, scheduler):
    """Delete all managed workloads from both clusters and scheduler store after each test."""
    yield
    _clean_scheduler_store(scheduler)
    _delete_managed_workloads(k8s_clients)


def _clean_scheduler_store(scheduler: Scheduler) -> None:
    """Delete all workloads from the scheduler's internal store via API."""
    try:
        names = requests.get(f"{scheduler.base_url}/jobs", timeout=5).json()
        for name in names:
            requests.delete(f"{scheduler.base_url}/jobs/{name}", timeout=5)
    except Exception:
        pass


def _delete_managed_workloads(k8s_clients: dict) -> None:
    """Delete all managed workloads and wait for them to be gone."""
    label = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"
    found_any = False
    for cluster_name, apis in k8s_clients.items():
        try:
            jobs = apis["batch"].list_namespaced_job("default", label_selector=label)
            for job in jobs.items:
                found_any = True
                apis["batch"].delete_namespaced_job(
                    job.metadata.name,
                    "default",
                    body=client.V1DeleteOptions(
                        propagation_policy="Background",
                        grace_period_seconds=0,
                    ),
                )
        except Exception:
            pass
        try:
            pods = apis["core"].list_namespaced_pod("default", label_selector=label)
            for pod in pods.items:
                found_any = True
                apis["core"].delete_namespaced_pod(
                    pod.metadata.name,
                    "default",
                    body=client.V1DeleteOptions(grace_period_seconds=0),
                )
        except Exception:
            pass

    if not found_any:
        return

    # Wait for all managed objects to actually disappear.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        all_gone = True
        for cluster_name, apis in k8s_clients.items():
            try:
                jobs = apis["batch"].list_namespaced_job("default", label_selector=label)
                pods = apis["core"].list_namespaced_pod("default", label_selector=label)
                if jobs.items or pods.items:
                    all_gone = False
                    break
            except Exception:
                pass
        if all_gone:
            break
        time.sleep(1)
