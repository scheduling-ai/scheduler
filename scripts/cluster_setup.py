"""Create or destroy a local kind cluster for scheduler development.

Usage:
    uv run python scripts/kind_setup.py up      # create cluster
    uv run python scripts/kind_setup.py down     # delete cluster
    uv run python scripts/kind_setup.py status   # show cluster info
"""

import subprocess
from pathlib import Path

import typer

app = typer.Typer(help="Manage local kind cluster for scheduler dev.")

CLUSTER_NAME = "scheduler-dev"
KIND_CONFIG = Path(__file__).resolve().parent.parent / "deploy" / "kind-config.yaml"
GPU_LABELS = ["h100", "h100", "a100"]
KUBECTL = ["kubectl", "--context", f"kind-{CLUSTER_NAME}"]


def _cluster_exists() -> bool:
    result = subprocess.run(
        ["kind", "get", "clusters"], capture_output=True, text=True, check=False
    )
    return CLUSTER_NAME in result.stdout.splitlines()


@app.command()
def up() -> None:
    """Create the kind cluster and label worker nodes with fake GPU types."""
    if _cluster_exists():
        typer.echo(f"Cluster '{CLUSTER_NAME}' already exists.")
        result = subprocess.run(
            [*KUBECTL, "cluster-info"], capture_output=True, text=True, check=True
        )
        typer.echo(result.stdout)
        return

    typer.echo(f"Creating kind cluster '{CLUSTER_NAME}'...")
    subprocess.run(
        [
            "kind",
            "create",
            "cluster",
            "--config",
            str(KIND_CONFIG),
            "--name",
            CLUSTER_NAME,
        ],
        check=True,
    )

    typer.echo("\nCluster ready. Labelling worker nodes with fake GPU resources...")
    result = subprocess.run(
        [
            *KUBECTL,
            "get",
            "nodes",
            "--no-headers",
            "-o",
            "custom-columns=:metadata.name",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    workers = [n for n in result.stdout.splitlines() if "worker" in n]

    for i, node in enumerate(workers):
        label = GPU_LABELS[i % len(GPU_LABELS)]
        subprocess.run(
            [*KUBECTL, "label", "node", node, f"accelerator={label}", "--overwrite"],
            capture_output=True,
            text=True,
            check=True,
        )
        subprocess.run(
            [
                *KUBECTL,
                "taint",
                "node",
                node,
                "scheduler=custom:NoSchedule",
                "--overwrite",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        typer.echo(f"  {node} -> accelerator={label}, taint=scheduler=custom:NoSchedule")

    typer.echo("\nDone. Use 'kubectl get nodes --show-labels' to verify.")


@app.command()
def down() -> None:
    """Delete the kind cluster."""
    typer.echo(f"Deleting kind cluster '{CLUSTER_NAME}'...")
    subprocess.run(["kind", "delete", "cluster", "--name", CLUSTER_NAME], check=True)
    typer.echo("Done.")


@app.command()
def status() -> None:
    """Show cluster info and node status."""
    if not _cluster_exists():
        typer.echo(f"Cluster '{CLUSTER_NAME}' does not exist.")
        raise typer.Exit(code=1)

    result = subprocess.run([*KUBECTL, "cluster-info"], capture_output=True, text=True, check=True)
    typer.echo(result.stdout)
    result = subprocess.run(
        [*KUBECTL, "get", "nodes", "-o", "wide"],
        capture_output=True,
        text=True,
        check=True,
    )
    typer.echo(result.stdout)


if __name__ == "__main__":
    app()
