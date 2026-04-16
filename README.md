# Multi-Cluster GPU Scheduler


> [!TIP]
> **Try the interactive UI now:** `docker compose up -d --build` then open http://localhost:8000

## Architecture

```
                  ┌──────────────┐
                  │   HTTP API   │
                  │  (submit /   │
                  │   status)    │
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐        ┌─────────────────┐
                  │    Binder    │◄──────►│  Python Solver  │
                  │    Loop      │  JSON  │  (stdin/stdout) │
                  │   (Rust)     │        └─────────────────┘
                  └──┬───────┬───┘
                     │       │
              ┌──────▼──┐ ┌──▼──────┐
              │Cluster 1│ │Cluster 2│  ...
              │Reflector│ │Reflector│
              └─────────┘ └─────────┘
```

- **Rust k8s-bridge**: Watches clusters (nodes, pods, jobs), calls solver, applies placement decisions. Handles workload lifecycle, optimistic concurrency, backoff, reflector auto-recovery.
- **Python solver**: Stateless scheduling function. Two implementations: a heuristic mock (Kueue-style admission + bin packing) and a MILP formulation (Pyomo + HiGHS) with lexicographic scoring. Selectable via `--solver`.
- **Interface**: JSON over stdin/stdout. Solver is crash-isolated and independently testable.

Follows [Kueue](https://kueue.sigs.k8s.io/)'s lifecycle patterns (suspend toggle, quota semantics, Pod preemption via delete) without taking it as a dependency. Key deviation: workloads are held centrally until placed, enabling multi-cluster optimization and API-server backpressure.

## Repository layout

```
py-scheduler/           Python solver, simulation, tests, and Svelte 5 browser UI
crates/k8s-bridge/      Rust k8s bridge (reflectors, binder, API, solver bridge)
e2e/                    End-to-end tests against real kind clusters
scripts/                Dev tooling (kind cluster setup)
deploy/                 K8s manifests and kind config
docs/                   Design docs (see docs/README.md for catalogue)
```

## Quick start

### Prerequisites

[Docker](https://docs.docker.com/get-docker/) ·
[uv](https://docs.astral.sh/uv/) (for local dev) ·
[kind](https://kind.sigs.k8s.io/) + [kubectl](https://kubernetes.io/docs/tasks/tools/) + [Rust](https://rustup.rs/) (for k8s bridge)

### Docker Compose (recommended)

```bash
docker compose up -d --build
```

Opens the full stack at http://localhost:8000 — solver loop, UI, and fake workload generator.

### Local dev (no Docker)

```bash
uv sync
LOOP_RUNNER_STATE_DIR=.state uv run loop-runner     # solver tick loop
LOOP_RUNNER_STATE_DIR=.state uv run scheduler-ui    # UI at http://localhost:8000
```

Run both in separate terminals. The loop runner writes `latest-*.json` to `$LOOP_RUNNER_STATE_DIR` (default `/data/live-state`). The UI server reads those files and serves scenarios from `py-scheduler/scheduler/scenarios/`.

### Container deploy

`docker-compose.yml` runs two services from the same image, sharing a Docker volume at `/data/live-state`:

- `scheduler-ui`: UI on port `8000`
- `loop-runner-service`: solver tick loop (no HTTP server)

`scripts/deploy-loop.sh` auto-rebuilds and restarts on `main` changes. A production overlay at `deploy/docker-compose.prod.yml` hides host ports and adds Pomerium.

### Full stack (needs kind + Docker)

```bash
uv run python scripts/cluster_setup.py up       # create kind cluster
cargo build -p k8s-bridge                        # build Rust bridge
uv run pytest e2e/                               # e2e tests

# Run the scheduler
cargo run -p k8s-bridge -- serve --cluster local --port 8080

# Submit workloads (API accepts JSON; convert the YAML manifest first)
curl -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' \
  -d "$(yq -o=json deploy/test-job.yaml)"
curl http://localhost:8080/status
```

### Useful commands

```bash
# Watch cluster state
cargo run -p k8s-bridge -- observe --resource pods
cargo run -p k8s-bridge -- observe --resource nodes

# Dry-run binder (solve without applying)
cargo run -p k8s-bridge -- bind --cluster local --dry-run
```

## Pre-submit checks

```bash
# Python
uv run ruff format && uv run ruff check && uv run ty check && uv run pytest

# UI (from py-scheduler/scheduler/ui/)
npm run format:check && npm run check && npm run build

# Rust
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings
```

