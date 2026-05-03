"""
HTTP API validation edge cases.

Tests the scheduler's HTTP API error handling: malformed input, duplicate
submissions, concurrent races.
"""

import concurrent.futures

import pytest
import requests

from conftest import build_job, build_pod, submit_job, submit_pod

pytestmark = pytest.mark.e2e


def test_reject_non_suspended_job(scheduler):
    """Job with suspend:false → 400."""
    manifest = build_job("bad-suspend", "h100")
    manifest["spec"]["suspend"] = False
    resp = submit_job(scheduler, manifest)
    assert resp.status_code == 400


def test_reject_missing_name(scheduler):
    """Job without metadata.name → 400."""
    manifest = build_job("temp", "h100")
    del manifest["metadata"]["name"]
    resp = submit_job(scheduler, manifest)
    assert resp.status_code == 400


def test_reject_unsupported_kind(scheduler):
    """kind:Deployment → 400."""
    manifest = build_job("bad-kind", "h100")
    manifest["kind"] = "Deployment"
    resp = submit_job(scheduler, manifest)
    assert resp.status_code == 400


def test_reject_bare_pod(scheduler):
    """kind:Pod → 400. Bare Pod submissions aren't supported in v0; the
    user should submit a Job, or run a Deployment / ReplicaSet that the
    bridge picks up via the cluster reflector."""
    resp = submit_pod(scheduler, build_pod("bare-pod", "h100"))
    assert resp.status_code == 400


def test_reject_unknown_quota(scheduler):
    """Job referencing a quota the bridge doesn't know about → 400 at
    submission time, with the list of known quotas in the message.

    Without this check, a typo'd quota poisons every solver cycle until
    the workload's backoff threshold trips (~3 cycles of stalled work).
    """
    job = build_job("typo-quota", "h100", quota="not-a-real-quota")
    resp = submit_job(scheduler, job)
    assert resp.status_code == 400, resp.text
    assert "unknown quota" in resp.text.lower()
    assert "not-a-real-quota" in resp.text


def test_reject_invalid_json(scheduler):
    """Malformed body → 400."""
    resp = requests.post(
        f"{scheduler.base_url}/jobs",
        data="not json {{{",
        headers={"Content-Type": "application/json"},
        timeout=5,
    )
    assert resp.status_code == 400


def test_reject_duplicate_name(scheduler):
    """Same name twice → 409."""
    resp1 = submit_job(scheduler, build_job("dup-test", "h100"))
    assert resp1.status_code == 201
    resp2 = submit_job(scheduler, build_job("dup-test", "h100"))
    assert resp2.status_code == 409


def test_delete_nonexistent(scheduler):
    """DELETE unknown name → 404."""
    resp = requests.delete(f"{scheduler.base_url}/jobs/does-not-exist", timeout=5)
    assert resp.status_code == 404


def test_concurrent_same_name_submissions(scheduler):
    """Two threads POST same name. Exactly one gets 201, the other 409."""
    manifest = build_job("race-test", "h100")
    results = []

    def do_submit():
        return submit_job(scheduler, manifest).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(do_submit) for _ in range(2)]
        results = [f.result() for f in futures]

    assert sorted(results) == [201, 409], f"Expected [201, 409], got {sorted(results)}"
