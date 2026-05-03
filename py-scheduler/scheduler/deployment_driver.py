"""Drive a small set of Kubernetes Deployments on a synthetic scaling
pattern, so the live UI shows real Deployment-spawned Pods flowing
through the bridge alongside Job submissions.

Why: the bridge's KEDA-style code path (Pods owned by a ReplicaSet,
preempted by deletion, respawned by the Deployment controller) is
exercised in unit + e2e tests but never lit up in the deployed live
mode.  A controller patching ``Deployment.spec.replicas`` is the only
input needed to exercise it; the controller doesn't have to be KEDA.

This driver runs alongside the Job submitter (in the same load-generator
pod) and patches replicas on a sine wave per Deployment.  From the
bridge's perspective it is indistinguishable from KEDA — KEDA's contract
with its target is "patch replicas, walk away," which is exactly what
we do.

Configuration is intentionally code-level for v0; the UI knobs live on
the Job-side generator config and we don't surface a second config
surface here.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from dataclasses import dataclass

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

log = logging.getLogger(__name__)

NAMESPACE = os.environ.get("DEPLOYMENT_DRIVER_NAMESPACE", "default")
SCHEDULER_NAME = os.environ.get("SCHEDULER_NAME", "custom-scheduler")
TAINT_KEY = os.environ.get("TAINT_KEY", "scheduler")
TAINT_VALUE = os.environ.get("TAINT_VALUE", "custom")
CHIPS_ANNOTATION = os.environ.get("CHIPS_ANNOTATION", "scheduler.example.com/chips-per-replica")
TICK_SECONDS = float(os.environ.get("DEPLOYMENT_DRIVER_TICK_SECONDS", "30"))
ENABLED = os.environ.get("DEPLOYMENT_DRIVER_ENABLED", "1") == "1"

MANAGED_BY_LABEL = "scheduler.example.com/managed-by"
JOB_NAME_LABEL = "scheduler.example.com/job-name"


@dataclass
class DepSpec:
    name: str
    quota: str
    priority: int
    chip_type: str
    chips_per_replica: int
    min_replicas: int
    max_replicas: int
    period_seconds: float
    phase_offset: float


# Two Deployments with out-of-phase sine waves.  Replica counts kept low
# because the demo cluster's chip pools are small (e2-micro, a few chips
# each); over-provisioning would just produce a queue that never drains.
DEPLOYMENTS: list[DepSpec] = [
    DepSpec(
        name="serve-flagship",
        quota="inference",
        priority=40,
        chip_type="H100",
        chips_per_replica=1,
        min_replicas=0,
        max_replicas=2,
        period_seconds=240,  # 4-minute cycle
        phase_offset=0.0,
    ),
    DepSpec(
        name="serve-batch",
        quota="inference",
        priority=20,
        chip_type="A100",
        chips_per_replica=1,
        min_replicas=0,
        max_replicas=2,
        period_seconds=300,  # 5-minute cycle, out of phase
        phase_offset=math.pi / 2,
    ),
]


def _replicas_at(spec: DepSpec, t_seconds: float) -> int:
    span = spec.max_replicas - spec.min_replicas
    if span <= 0:
        return spec.min_replicas
    angle = 2 * math.pi * t_seconds / spec.period_seconds + spec.phase_offset
    fraction = (math.sin(angle) + 1) / 2  # 0..1
    return spec.min_replicas + round(fraction * span)


def _build_deployment(spec: DepSpec) -> dict:
    """Build a Deployment manifest in the bridge's expected shape.

    The Pod template carries:
    - the managed-by + job-name labels (binder filters reflectors on these),
    - the priority/quota/chips annotations (binder reads these),
    - schedulerName=custom-scheduler (default kube-scheduler ignores the Pod),
    - tolerations for the chip pool taint.

    No GPU resource request — the GKE demo cluster has no device plugin;
    chip count travels via the annotation.  Tiny CPU/memory request so
    kubelet's eviction accounting is honest on e2-micro chip nodes.
    """
    pod_labels = {
        "accelerator": spec.chip_type,
        JOB_NAME_LABEL: spec.name,
        MANAGED_BY_LABEL: SCHEDULER_NAME,
    }
    pod_annotations = {
        "scheduler.example.com/priority": str(spec.priority),
        "scheduler.example.com/quota": spec.quota,
        CHIPS_ANNOTATION: str(spec.chips_per_replica),
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": spec.name,
            "namespace": NAMESPACE,
            "labels": {"app.kubernetes.io/managed-by": "scheduler-load-generator"},
        },
        "spec": {
            "replicas": spec.min_replicas,
            "selector": {"matchLabels": {JOB_NAME_LABEL: spec.name}},
            "template": {
                "metadata": {"labels": pod_labels, "annotations": pod_annotations},
                "spec": {
                    "schedulerName": SCHEDULER_NAME,
                    "tolerations": [
                        {
                            "key": TAINT_KEY,
                            "operator": "Equal",
                            "value": TAINT_VALUE,
                            "effect": "NoSchedule",
                        }
                    ],
                    "containers": [
                        {
                            "name": "workload",
                            "image": "busybox:1.36",
                            "command": ["sleep", "9999"],
                            "resources": {
                                "requests": {"cpu": "1m", "memory": "16Mi"},
                                "limits": {"cpu": "50m", "memory": "64Mi"},
                            },
                        }
                    ],
                    "restartPolicy": "Always",
                },
            },
        },
    }


def _ensure_deployment(apps_api: client.AppsV1Api, spec: DepSpec) -> None:
    """Create the Deployment if absent; otherwise patch the template (in
    case the manifest changed across releases).  Replica count is owned
    by the scaling loop, not by ensure — never overwrite it here.
    """
    manifest = _build_deployment(spec)
    try:
        apps_api.read_namespaced_deployment(name=spec.name, namespace=NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            apps_api.create_namespaced_deployment(namespace=NAMESPACE, body=manifest)
            log.info("created deployment %s", spec.name)
            return
        raise

    # Already exists — patch everything except replicas.
    patch_body = {**manifest, "spec": {**manifest["spec"]}}
    patch_body["spec"].pop("replicas", None)
    apps_api.patch_namespaced_deployment(name=spec.name, namespace=NAMESPACE, body=patch_body)


def _scale_deployment(apps_api: client.AppsV1Api, spec: DepSpec, replicas: int) -> None:
    apps_api.patch_namespaced_deployment_scale(
        name=spec.name,
        namespace=NAMESPACE,
        body={"spec": {"replicas": replicas}},
    )


def driver_loop(stop: threading.Event) -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        # Useful when running locally against a kubeconfig (cluster_setup.py
        # dev kind cluster).  Falls back to the active kubectl context.
        config.load_kube_config()
    apps_api = client.AppsV1Api()

    for spec in DEPLOYMENTS:
        try:
            _ensure_deployment(apps_api, spec)
        except ApiException as e:
            log.warning("ensure %s: %s", spec.name, e)

    start = time.monotonic()
    last_replicas: dict[str, int] = {}
    while not stop.is_set():
        t = time.monotonic() - start
        for spec in DEPLOYMENTS:
            replicas = _replicas_at(spec, t)
            if last_replicas.get(spec.name) == replicas:
                continue  # avoid spurious patches
            try:
                _scale_deployment(apps_api, spec, replicas)
                log.info("scaled %s -> %d replicas", spec.name, replicas)
                last_replicas[spec.name] = replicas
            except ApiException as e:
                log.warning("scale %s: %s", spec.name, e)
        stop.wait(TICK_SECONDS)


def start(stop: threading.Event) -> threading.Thread | None:
    """Spin up the driver loop in a daemon thread.  Returns None if the
    feature is disabled via env."""
    if not ENABLED:
        log.info("deployment driver disabled (DEPLOYMENT_DRIVER_ENABLED=0)")
        return None
    t = threading.Thread(target=driver_loop, args=(stop,), daemon=True)
    t.start()
    log.info(
        "deployment driver started: deployments=%s tick=%ss",
        [spec.name for spec in DEPLOYMENTS],
        TICK_SECONDS,
    )
    return t
