# Scheduler

Multi-cluster GPU job scheduler.

## Repository layout

- `py-scheduler/` — Python package (solver experimentation sandbox, replay UI, live simulator, fake-job generator, tests, not connected to k8s)
- `crates/k8s-bridge/` — Rust crate (k8s reflectors, workload binder, observer, HTTP API)
- `scripts/` — Dev tooling (kind cluster setup)
- `deploy/` — K8s manifests and kind config
- `docs/` — Design docs (see `docs/CATALOGUE.md`). If `docs/private/` exists, check `docs/private/CATALOGUE.md` too.
- `py-scheduler/scheduler/ui/` — Svelte 5 + Vite browser UI (replay, live viewer, generator controls)

## Simulator and UI

```sh
uv run loop-runner     # solver tick loop (writes latest-*.json to $LOOP_RUNNER_STATE_DIR)
uv run scheduler-ui    # UI server (reads state, serves scenarios)
```

Run both in separate terminals. `LOOP_RUNNER_STATE_DIR` defaults to `/data/live-state`. Generator config is `config.json` in the state directory — the Generator tab writes it, the loop runner hot-reloads it.

Built-in scenarios (`py-scheduler/scheduler/scenarios/*.jsonl`) work in the UI without the loop runner.

## Tools

- Use `uv run --with <package>` when you need a one-off dependency (e.g. `uv run --with Pillow python -c "..."`). Do not use `pip install`.

## Style

- Format with `uv run ruff format` before committing.
- All code must pass `uv run ruff check` and `uv run ty check`.
- Tests run with `uv run pytest`.
- Rust builds with `cargo build` from the repo root.
- Format Rust with `cargo fmt --all` before committing.
- UI: format with `npm run format`, type-check with `npm run check`, build with `npm run build` (all from `py-scheduler/scheduler/ui/`).

## Pre-submit checks

**Always run these checks before committing.** Do not commit if any check fails.

```sh
# Python
uv run ruff format
uv run ruff check
uv run ty check
uv run pytest

# UI (from py-scheduler/scheduler/ui/)
npm run format:check
npm run check
npm run build

# Rust
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings

# Docker (verifies the full image builds)
docker compose build
```

## Production deployment

A cron-based deploy loop may be running on this machine (see `scripts/deploy-loop.sh`). It clones the repo to `/tmp/scheduler-deploy` and runs `docker compose up` there. **Do not `cd` into `/tmp/scheduler-deploy` or build there** — just push your changes and the deploy loop picks them up. After pushing, if `/tmp/deploy.log` exists check the deployment by looking at that file as well as docker compose logs health in `/tmp/scheduler-deploy` to verify the deploy succeeded.

## Debugging the UI

- Build and run locally with `docker compose up -d --build`, then open http://localhost:8000.
- Use the `/screenshot` skill to take Playwright screenshots of the running UI and verify visual changes.
- For dev iteration without Docker: `npm run dev` from `py-scheduler/scheduler/ui/` starts a Vite dev server with API proxy to `localhost:8000`.
