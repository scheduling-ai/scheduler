"""Tests for the synthetic load generator's manifest builder.

The two submission modes (``bridge`` vs ``kueue``) produce different Job
manifests; this file pins the differences so they don't drift.
"""

from __future__ import annotations

import pytest

from scheduler import load_generator
from scheduler.generator import NewSubmission
from scheduler.model import Phase, Pod, PodReplicaStatus


def _make_submission(*, replicas: int = 2, chip_type: str = "H100") -> NewSubmission:
    pod = Pod(
        chips_per_replica=8,
        chip_type=chip_type,
        priority=42,
        quota="research",
        cluster=None,
        statuses_by_replica=[PodReplicaStatus(phase=Phase.SUSPENDED) for _ in range(replicas)],
    )
    return NewSubmission(
        job_id="test-job-abc",
        pod=pod,
        runtime_seconds=30,
        gang_id=None,
    )


def test_bridge_mode_keeps_scheduler_name_and_toleration() -> None:
    sub = _make_submission()
    job = load_generator._build_job(sub, mode=load_generator.MODE_BRIDGE)

    assert job["spec"]["suspend"] is True
    pod_spec = job["spec"]["template"]["spec"]
    assert pod_spec["schedulerName"] == load_generator.SCHEDULER_NAME
    assert pod_spec["tolerations"][0]["key"] == load_generator.TAINT_KEY
    assert "affinity" not in pod_spec
    # No Kueue label leaks into bridge-mode jobs.
    assert load_generator.KUEUE_QUEUE_LABEL not in job["metadata"]["labels"]


def test_kueue_mode_swaps_in_queue_label_and_node_affinity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(load_generator, "KUEUE_QUEUE_NAME", "default")
    sub = _make_submission(chip_type="A100")
    job = load_generator._build_job(sub, mode=load_generator.MODE_KUEUE)

    # Queue-name lives on the Job (not the pod template) — that's where
    # Kueue's job controller reads it from.
    assert job["metadata"]["labels"][load_generator.KUEUE_QUEUE_LABEL] == "default"
    assert load_generator.KUEUE_QUEUE_LABEL not in job["spec"]["template"]["metadata"]["labels"]

    pod_spec = job["spec"]["template"]["spec"]
    # In kueue mode the default scheduler binds; we don't claim the pod.
    assert "schedulerName" not in pod_spec
    # Tolerations come from the ResourceFlavor, not from us.
    assert "tolerations" not in pod_spec

    # Pod is pinned to the chip pool matching the Pod's chip_type, so
    # Kueue picks the matching ResourceFlavor.
    terms = pod_spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][
        "nodeSelectorTerms"
    ]
    assert terms == [
        {"matchExpressions": [{"key": "accelerator", "operator": "In", "values": ["A100"]}]}
    ]


def test_kueue_mode_preserves_suspend_and_chip_annotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        load_generator, "CHIPS_ANNOTATION", "scheduler.example.com/chips-per-replica"
    )
    sub = _make_submission()
    job = load_generator._build_job(sub, mode=load_generator.MODE_KUEUE)

    # Kueue requires Jobs to start suspended so its admission controller
    # can gate them — same contract as the bridge.
    assert job["spec"]["suspend"] is True
    assert job["metadata"]["annotations"]["scheduler.example.com/chips-per-replica"] == str(
        sub.pod.chips_per_replica
    )
