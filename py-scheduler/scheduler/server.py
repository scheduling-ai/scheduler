"""Minimal HTTP server: static UI, solve endpoint, state directory."""

from __future__ import annotations

import http.server
import json
import os
import time
from dataclasses import asdict
from pathlib import Path

from scheduler.loop_runner import read_config
from scheduler.model import solver_request_from_json
from scheduler.solvers import SOLVERS

STATIC_DIR = Path(__file__).resolve().parent / "ui" / "dist"
SCENARIO_DIR = Path(__file__).resolve().parent / "scenarios"
STATE_DIR = Path(os.environ.get("LOOP_RUNNER_STATE_DIR", "/data/live-state"))

SPA_ROUTES = {"/", "/index.html", "/live", "/replay", "/generator"}


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

            # State files: latest-*.json, config.json
            if path.startswith("/state/"):
                rel = path[len("/state/") :]
                if rel == "config.json":
                    _json_response(self, read_config(config_path).to_dict())
                    return
                file = state_dir / rel
                if file.exists() and file.is_file():
                    self._serve_file(file, "application/json")
                    return
                _json_response(self, {"error": "not found"}, 404)
                return

            # Scenario files
            if path.startswith("/scenarios/") and path.endswith(".jsonl"):
                rel = path[len("/scenarios/") :]
                file = SCENARIO_DIR / rel
                if file.exists():
                    self._serve_file(file, "application/x-ndjson")
                    return
                _json_response(self, {"error": "not found"}, 404)
                return

            # Scenario index
            if path == "/scenarios/index.json":
                names = sorted(p.stem for p in SCENARIO_DIR.glob("*.jsonl"))
                _json_response(self, [{"name": n} for n in names])
                return

            # Solver list
            if path == "/api/solvers":
                _json_response(self, [{"name": k, "ref": k} for k in SOLVERS])
                return

            # SPA fallback
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
                body = _read_json_body(self)
                existing = {}
                if config_path.exists():
                    try:
                        existing = json.loads(config_path.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError):
                        pass
                merged = {**existing, **body}
                config_path.parent.mkdir(parents=True, exist_ok=True)
                config_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")
                _json_response(self, {"running": merged.get("running", True), "config": merged})
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
