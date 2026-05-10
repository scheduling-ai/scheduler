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

## CI

CI runs on CircleCI, not GitHub Actions — `gh run list` will not show
runs. Use `circleci` CLI or the CircleCI web UI for run history and
logs.

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

Production runs in GKE.  **All code-level deployments go through
`scripts/deploy.sh`** — it's the only supported way to ship a change to
the running cluster.  It builds the image, pushes it to Artifact
Registry, and rolls out every Deployment in `scheduler-system`
(`k8s-bridge`, `scheduler-ui`, `load-generator`, and — when the
observed-cluster Secret is present — `k8s-bridge-observed` plus the
Kueue-mode load-generator on cluster #2).

Do NOT hand-craft `kubectl apply` / `docker push` / `rollout restart`
sequences instead — the script is idempotent and handles dirty-tree
tagging, manifest substitution, secret-gated optional rollouts, and
post-rollout wait-for-ready in one place.  Diverging from it leaves
clusters in inconsistent states.

See `infra/README.md` for cluster setup (terraform + RBAC) and
`infra/k8s/` for the manifests.  Cluster #2 onboarding (Kueue install +
kubeconfig Secret) is a one-time `scripts/setup-observed.sh` step,
separate from per-release `deploy.sh`.

The scheduler plane (`k8s-bridge`, `scheduler-ui`, `load-generator`) is
logically separate from each data-plane cluster it schedules into,
even when co-located for cost. Adding a second data-plane cluster is a
config change (extra entry in the kubeconfig Secret + `--cluster` flag
on the bridge), not a code change.

### Observe-only mode (no scheduling)

`k8s-bridge serve-observe` reflects an external cluster (Kueue or
otherwise scheduling natively) and serves the same `/snapshot` Frame
the UI already consumes — no solver subprocess, no binder. Used to
ship the UI as a standalone product against clusters we don't control.
Manifests in `infra/k8s/scheduler-plane/k8s-bridge-observed.yaml`;
read-only RBAC for the observed cluster in
`infra/k8s/observed-cluster/observer-rbac.yaml`. Skipped automatically
by `deploy.sh` if the `observed-cluster-kubeconfig` Secret isn't present.

The UI's live-mode dropdown reads `BRIDGE_SOURCES` (JSON list of
`{name, label, url}`) from the UI server's env. Each entry maps to one
bridge instance; switching the dropdown switches the snapshot source.

## Debugging the UI

- Build and run locally with `docker compose up -d --build`, then open http://localhost:8000.
- Use the `/screenshot` skill to take Playwright screenshots of the running UI and verify visual changes.
- For dev iteration without Docker: `npm run dev` from `py-scheduler/scheduler/ui/` starts a Vite dev server with API proxy to `localhost:8000`.
