import scheduler.milp_solver as milp_module
from scheduler.model import ClusterState, Node, Phase, Pod, PodReplicaStatus, Quota
from scheduler.milp_solver import solve


def _tiny_h100_case() -> tuple[list[ClusterState], dict[str, Pod], list[Quota]]:
    clusters = [ClusterState("tiny", [Node("tiny-h100-0000", "H100", 8)])]
    pods = {
        "one-shot": Pod(
            chips_per_replica=1,
            chip_type="H100",
            priority=1,
            quota="quota-a",
            cluster=None,
            statuses_by_replica=[PodReplicaStatus(Phase.RUNNING)],
        )
    }
    quotas = [Quota("quota-a", {"tiny": {"H100": 1}})]
    return clusters, pods, quotas


def test_highs_presolve_defaults_off(monkeypatch):
    clusters, pods, quotas = _tiny_h100_case()
    real_factory = milp_module.pyo.SolverFactory
    captured: dict[str, object] = {}

    def capture_factory(name, *args, **kwargs):
        optimizer = real_factory(name, *args, **kwargs)
        captured["optimizer"] = optimizer
        return optimizer

    monkeypatch.setattr(milp_module.pyo, "SolverFactory", capture_factory)
    solve(clusters, pods, [], quotas)

    assert captured["optimizer"].options["presolve"] == "off"
    assert captured["optimizer"].options["output_flag"] is False
    assert captured["optimizer"].options["log_to_console"] is False


def test_highs_presolve_can_be_enabled(monkeypatch):
    clusters, pods, quotas = _tiny_h100_case()
    real_factory = milp_module.pyo.SolverFactory
    captured: dict[str, object] = {}

    def capture_factory(name, *args, **kwargs):
        optimizer = real_factory(name, *args, **kwargs)
        captured["optimizer"] = optimizer
        return optimizer

    monkeypatch.setattr(milp_module.pyo, "SolverFactory", capture_factory)
    solve(clusters, pods, [], quotas, presolve=True)

    assert captured["optimizer"].options["presolve"] == "on"


def test_highs_console_output_can_be_enabled(monkeypatch):
    clusters, pods, quotas = _tiny_h100_case()
    real_factory = milp_module.pyo.SolverFactory
    captured: dict[str, object] = {}

    def capture_factory(name, *args, **kwargs):
        optimizer = real_factory(name, *args, **kwargs)
        captured["optimizer"] = optimizer
        return optimizer

    monkeypatch.setattr(milp_module.pyo, "SolverFactory", capture_factory)
    solve(clusters, pods, [], quotas, verbose=True)

    assert captured["optimizer"].options["output_flag"] is True
    assert captured["optimizer"].options["log_to_console"] is True
