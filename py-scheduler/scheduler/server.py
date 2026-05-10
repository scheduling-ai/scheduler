"""Minimal HTTP server: static UI, /api/solve for replay, thin proxy to
``k8s-bridge`` (live-mode frames, job submission) and ``load-generator``
(generator config).

The UI polls ``/state/latest-<name>.json`` and reads/writes the generator at
``/api/generator/config``. These URLs used to be backed by files on a shared
volume written by ``loop-runner``; in cluster deployments they are now
forwarded to the bridge and load-generator Services over HTTP. Two behaviours,
one code path — selected by the presence of ``BRIDGE_URL`` /
``GENERATOR_URL`` env vars so ``docker compose up`` (local dev) still works
unmodified against ``loop-runner`` + the state directory.
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from pathlib import Path

from scheduler.generator import read_config
from scheduler.model import solver_request_from_json
from scheduler.solvers import SOLVERS

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "ui" / "dist"
SCENARIO_DIR = Path(__file__).resolve().parent / "scenarios"
STATE_DIR = Path(os.environ.get("LOOP_RUNNER_STATE_DIR", "/data/live-state"))
BRIDGE_URL = os.environ.get("BRIDGE_URL", "").rstrip("/") or None
GENERATOR_URL = os.environ.get("GENERATOR_URL", "").rstrip("/") or None

SPA_ROUTES = {"/", "/index.html", "/live", "/replay", "/generator"}


def _load_bridge_sources() -> list[dict]:
    """Live-mode source list.

    ``BRIDGE_SOURCES`` is JSON:
    ``[{"name": "...", "label": "...", "shortLabel": "...", "url": "..."}]``.
    The UI surfaces ``label`` in its dropdown (verbose, descriptive)
    and ``shortLabel`` in the header badge (terse, fits a chip-style
    pill); if ``shortLabel`` is omitted, the badge falls back to ``label``.
    Snapshot polling reads ``/state/latest-<name>.json``, served from ``url``.

    If unset, fall back to the single-bridge ``BRIDGE_URL`` (back-compat
    with the original docker-compose deployment) under the synthetic
    name ``live``.
    """
    raw = os.environ.get("BRIDGE_SOURCES", "").strip()
    if raw:
        try:
            sources = json.loads(raw)
        except json.JSONDecodeError as e:
            log.error("invalid BRIDGE_SOURCES JSON: %s", e)
            return []
        out: list[dict] = []
        for s in sources:
            name = s.get("name")
            url = s.get("url", "").rstrip("/")
            if not name or not url:
                log.warning("BRIDGE_SOURCES entry missing name/url: %r", s)
                continue
            label = s.get("label") or name
            entry: dict = {"name": name, "label": label, "url": url}
            short = s.get("shortLabel")
            if short:
                entry["shortLabel"] = short
            out.append(entry)
        return out
    if BRIDGE_URL is not None:
        return [{"name": "live", "label": "Live", "url": BRIDGE_URL}]
    return []


BRIDGE_SOURCES = _load_bridge_sources()
BRIDGE_SOURCES_BY_NAME = {s["name"]: s for s in BRIDGE_SOURCES}


def _json_response(
    handler: http.server.BaseHTTPRequestHandler, data: object, status: int = 200
) -> None:
    body = json.dumps(data).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_json_body(handler: http.server.BaseHTTPRequestHandler) -> dict:
    length = int(handler.headers.get("Content-Length", 0))
    body = handler.rfile.read(length).decode("utf-8") if length else "{}"
    return json.loads(body or "{}")


def _proxy(
    handler: http.server.BaseHTTPRequestHandler,
    method: str,
    url: str,
    body: bytes | None = None,
) -> None:
    """Forward a request to `url` and stream the response body back.

    Errors surface as JSON with the upstream status code, not a Python
    traceback — the UI polls every 500 ms so a warm cache + warm network
    matter more than detail.
    """
    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"} if body else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = resp.read()
            handler.send_response(resp.status)
            handler.send_header(
                "Content-Type", resp.headers.get("Content-Type", "application/json")
            )
            handler.send_header("Content-Length", str(len(payload)))
            handler.end_headers()
            handler.wfile.write(payload)
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", "replace") if e.fp else str(e)
        _json_response(handler, {"error": msg}, e.code)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        _json_response(handler, {"error": f"upstream unreachable: {e}"}, 502)


def make_handler(
    *,
    state_dir: Path = STATE_DIR,
    static_dir: Path = STATIC_DIR,
) -> type[http.server.SimpleHTTPRequestHandler]:
    config_path = state_dir / "config.json"

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(static_dir), **kwargs)

        def do_GET(self):
            path = self.path.split("?")[0]

            if path.startswith("/state/"):
                rel = path[len("/state/") :]

                if rel == "config.json":
                    if GENERATOR_URL is not None:
                        _proxy(self, "GET", f"{GENERATOR_URL}/config")
                    else:
                        _json_response(self, read_config(config_path).to_dict())
                    return

                if rel.startswith("latest-") and rel.endswith(".json"):
                    name = rel[len("latest-") : -len(".json")]
                    source = BRIDGE_SOURCES_BY_NAME.get(name)
                    if source is not None:
                        _proxy(self, "GET", f"{source['url']}/snapshot")
                        return
                    # Local-dev path: read the file loop-runner wrote.
                    # Falls through when no bridge sources are configured —
                    # used by `uv run loop-runner` + `uv run scheduler-ui`.
                    file = state_dir / rel
                    if file.exists() and file.is_file():
                        self._serve_file(file, "application/json")
                    else:
                        _json_response(self, {"error": "not found"}, 404)
                    return

                _json_response(self, {"error": "not found"}, 404)
                return

            if path.startswith("/scenarios/") and path.endswith(".jsonl"):
                rel = path[len("/scenarios/") :]
                file = SCENARIO_DIR / rel
                if file.exists():
                    self._serve_file(file, "application/x-ndjson")
                    return
                _json_response(self, {"error": "not found"}, 404)
                return

            if path == "/scenarios/index.json":
                names = sorted(p.stem for p in SCENARIO_DIR.glob("*.jsonl"))
                desc_file = SCENARIO_DIR / "descriptions.json"
                descriptions: dict[str, str] = {}
                if desc_file.exists():
                    try:
                        descriptions = json.loads(desc_file.read_text())
                    except (json.JSONDecodeError, OSError):
                        pass
                _json_response(
                    self,
                    [{"name": n, "description": descriptions.get(n, "")} for n in names],
                )
                return

            if path == "/api/solvers":
                _json_response(self, [{"name": k, "ref": k} for k in SOLVERS])
                return

            if path == "/api/sources":
                # Live-mode source list for the UI dropdown.
                # Each entry: name (URL key), label (verbose dropdown
                # text), shortLabel (terse header badge text, optional).
                # Empty in dev when LOOP_RUNNER_STATE_DIR-backed mode is
                # in use — the UI then falls back to /api/solvers as
                # before.
                payload = []
                for s in BRIDGE_SOURCES:
                    entry: dict = {"name": s["name"], "label": s["label"]}
                    if "shortLabel" in s:
                        entry["shortLabel"] = s["shortLabel"]
                    payload.append(entry)
                _json_response(self, payload)
                return

            if path in SPA_ROUTES or path.startswith("/scenarios/"):
                self.path = "/index.html"
                super().do_GET()
                return

            super().do_GET()

        def do_POST(self):
            path = self.path.split("?")[0]

            if path == "/api/solve":
                body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8")
                request = solver_request_from_json(body)
                query = dict(
                    p.split("=", 1)
                    for p in (self.path.split("?")[1] if "?" in self.path else "").split("&")
                    if "=" in p
                )
                solver_key = query.get("solver", "heuristic")
                solve_fn = SOLVERS.get(solver_key)
                if solve_fn is None:
                    _json_response(self, {"error": f"Unknown solver: {solver_key!r}"}, 400)
                    return
                started = time.perf_counter()
                result = solve_fn(
                    request.clusters,
                    request.pods,
                    request.gang_sets,
                    request.quotas,
                    time_limit=request.time_limit,
                )
                payload = asdict(result)
                payload["solver_duration_ms"] = round((time.perf_counter() - started) * 1000)
                _json_response(self, payload)
                return

            if path == "/api/generator/config":
                if GENERATOR_URL is not None:
                    body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                    _proxy(self, "POST", f"{GENERATOR_URL}/config", body)
                    return

                # Local-dev path: merge into config.json that loop-runner watches.
                body_json = _read_json_body(self)
                existing = {}
                if config_path.exists():
                    try:
                        existing = json.loads(config_path.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError):
                        pass
                merged = {**existing, **body_json}
                config_path.parent.mkdir(parents=True, exist_ok=True)
                config_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")
                _json_response(self, {"running": merged.get("running", True), "config": merged})
                return

            if path == "/api/jobs":
                if BRIDGE_URL is None:
                    _json_response(self, {"error": "bridge not configured"}, 503)
                    return
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                _proxy(self, "POST", f"{BRIDGE_URL}/jobs", body)
                return

            _json_response(self, {"error": "Not found"}, 404)

        def _serve_file(self, path: Path, content_type: str) -> None:
            data = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: object) -> None:
            if "/api/" in str(args[0]) or "/state/" in str(args[0]):
                super().log_message(format, *args)

    return Handler


def main() -> None:
    """Entry point for the UI server."""
    import scheduler.observability  # noqa: F401 — initialise logging/sentry

    port = int(os.environ.get("PORT", "8000"))
    server = http.server.HTTPServer(("", port), make_handler())
    print(f"Serving scheduler UI on http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()
