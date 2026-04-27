"""Synthetic load generator driving a real cluster via ``k8s-bridge``.

Runs the same arrival-rate / chip-weight / gang-frequency model as the
simulator's loop runner, but instead of mutating an in-memory world it
submits suspended Kubernetes Jobs to the bridge's ``POST /jobs`` endpoint.
The bridge then places and unsuspends them.

Serves a tiny HTTP API (``GET /config``, ``POST /config``, ``GET /healthz``)
so the UI can read and update the arrival rate / seed / weights without a
restart. Config is persisted to ``LOAD_GENERATOR_CONFIG_PATH`` so Deployment
rollouts don't lose it.
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import random
import threading
import urllib.error
import urllib.request
from pathlib import Path

from scheduler.generator import GeneratorConfig, NewSubmission, generate_submissions

log = logging.getLogger(__name__)

CONFIG_PATH = Path(os.environ.get("LOAD_GENERATOR_CONFIG_PATH", "/data/config.json"))
BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://k8s-bridge:8080").rstrip("/")
JOB_NAMESPACE = os.environ.get("JOB_NAMESPACE", "default")
# When empty, the Job manifest omits the GPU resource request (kubelet would
# otherwise reject pods on clusters without a device plugin). The chip count
# is then communicated to the bridge via `CHIPS_ANNOTATION` instead.
CHIP_RESOURCE = os.environ.get("CHIP_RESOURCE", "nvidia.com/gpu")
CHIPS_ANNOTATION = os.environ.get("CHIPS_ANNOTATION", "")
TAINT_KEY = os.environ.get("TAINT_KEY", "scheduler")
TAINT_VALUE = os.environ.get("TAINT_VALUE", "custom")
SCHEDULER_NAME = os.environ.get("SCHEDULER_NAME", "custom-scheduler")
MANAGED_BY_LABEL = "scheduler.example.com/managed-by"
JOB_NAME_LABEL = "scheduler.example.com/job-name"
PORT = int(os.environ.get("PORT", "8100"))


# ---------------------------------------------------------------------------
# Shared state between the HTTP handler and the tick loop
# ---------------------------------------------------------------------------


class State:
    """Mutable config + rng. Access guarded by a single lock."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.config = _read_config(CONFIG_PATH)
        self.rng = random.Random(self.config.seed)
        self.stop = threading.Event()


def _read_config(path: Path) -> GeneratorConfig:
    if not path.exists():
        return GeneratorConfig()
    try:
        return GeneratorConfig.from_dict(json.loads(path.read_text(encoding="utf-8")))
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        log.warning("bad config file %s, using defaults: %s", path, exc)
        return GeneratorConfig()


def _write_config(path: Path, cfg: GeneratorConfig) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg.to_dict(), indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Job manifest builder
# ---------------------------------------------------------------------------


def _build_job(sub: NewSubmission) -> dict:
    """Translate a synthetic :class:`NewSubmission` into a suspended Job
    manifest the bridge accepts on ``POST /jobs``.

    Fields mirror ``e2e/conftest.py::build_job``: labels/annotations are
    what ``extract_job_metadata`` reads in ``binder.rs``; ``spec.suspend=true``
    is required by ``api.rs::submit_job``.
    """
    pod = sub.pod
    replicas = len(pod.statuses_by_replica)

    annotations: dict[str, str] = {
        "scheduler.example.com/priority": str(pod.priority),
        "scheduler.example.com/quota": pod.quota,
    }
    if sub.gang_id is not None:
        annotations["scheduler.example.com/gang-set"] = sub.gang_id
    if CHIPS_ANNOTATION:
        annotations[CHIPS_ANNOTATION] = str(pod.chips_per_replica)

    container: dict = {
        "name": "workload",
        "image": "busybox:1.36",
        "command": ["sleep", str(int(sub.runtime_seconds))],
    }
    if CHIP_RESOURCE:
        container["resources"] = {
            "requests": {CHIP_RESOURCE: str(pod.chips_per_replica)},
            "limits": {CHIP_RESOURCE: str(pod.chips_per_replica)},
        }
    else:
        # GKE demo cluster has no device plugin; skip the GPU request so
        # kubelet admits the pod. Chip count travels via CHIPS_ANNOTATION.
        # Memory request reflects what busybox-sleep actually consumes
        # plus pod overhead, so kubelet's eviction accounting is honest
        # — the chip nodes are e2-micro and have little headroom.
        container["resources"] = {
            "requests": {"cpu": "1m", "memory": "16Mi"},
            "limits": {"cpu": "50m", "memory": "64Mi"},
        }

    pod_labels = {
        "accelerator": pod.chip_type,
        JOB_NAME_LABEL: sub.job_id,
        MANAGED_BY_LABEL: SCHEDULER_NAME,
    }

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": sub.job_id,
            "namespace": JOB_NAMESPACE,
            "labels": pod_labels,
            "annotations": annotations,
        },
        "spec": {
            "suspend": True,
            "parallelism": replicas,
            "completions": replicas,
            "backoffLimit": 0,
            "template": {
                # Pods must carry the same job-name / managed-by labels —
                # `binder.rs::bind_pending_pods` filters by them when
                # deciding which pods to bind via the k8s Binding API.
                "metadata": {"labels": pod_labels},
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
                    "containers": [container],
                    "restartPolicy": "Never",
                },
            },
        },
    }


def _submit_to_bridge(manifest: dict) -> bool:
    """POST a Job manifest to the bridge. Returns True on success."""
    body = json.dumps(manifest).encode("utf-8")
    req = urllib.request.Request(
        f"{BRIDGE_URL}/jobs",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        log.warning(
            "bridge rejected %s: HTTP %d %s", manifest["metadata"]["name"], e.code, e.reason
        )
        return False
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.warning("bridge unreachable: %s", e)
        return False


# ---------------------------------------------------------------------------
# Tick loop
# ---------------------------------------------------------------------------


def tick_loop(state: State) -> None:
    while not state.stop.is_set():
        with state.lock:
            cfg = state.config
            rng = state.rng
            dt = cfg.loop_interval_seconds
            if cfg.running:
                subs = list(generate_submissions(rng, cfg, dt))
            else:
                subs = []

        for sub in subs:
            _submit_to_bridge(_build_job(sub))

        state.stop.wait(dt)


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------


def make_handler(state: State) -> type[http.server.BaseHTTPRequestHandler]:
    class Handler(http.server.BaseHTTPRequestHandler):
        def _reply(self, data: object, status: int = 200) -> None:
            body = json.dumps(data).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = self.path.split("?")[0]
            if path == "/config":
                with state.lock:
                    self._reply(state.config.to_dict())
                return
            if path == "/healthz":
                self._reply({"ok": True})
                return
            self._reply({"error": "not found"}, 404)

        def do_POST(self) -> None:
            path = self.path.split("?")[0]
            if path != "/config":
                self._reply({"error": "not found"}, 404)
                return
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._reply({"error": "invalid json"}, 400)
                return
            if not isinstance(body, dict):
                self._reply({"error": "expected object"}, 400)
                return
            with state.lock:
                merged = {**state.config.to_dict(), **body}
                state.config = GeneratorConfig.from_dict(merged)
                state.rng = random.Random(state.config.seed)
                _write_config(CONFIG_PATH, state.config)
                self._reply({"running": state.config.running, "config": state.config.to_dict()})

        def log_message(self, format: str, *args: object) -> None:
            if "/config" in str(args[0]):
                super().log_message(format, *args)

    return Handler


def main() -> None:
    import scheduler.observability  # noqa: F401 — initialise logging/sentry

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    state = State()
    log.info(
        "load-generator starting: bridge=%s config=%s namespace=%s",
        BRIDGE_URL,
        CONFIG_PATH,
        JOB_NAMESPACE,
    )

    ticker = threading.Thread(target=tick_loop, args=(state,), daemon=True)
    ticker.start()

    server = http.server.ThreadingHTTPServer(("", PORT), make_handler(state))
    log.info("serving on :%d", PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        state.stop.set()
        server.server_close()


__all__ = ["GeneratorConfig", "State", "tick_loop", "main"]
