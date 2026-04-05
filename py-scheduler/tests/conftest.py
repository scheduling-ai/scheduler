import pytest

from scheduler.solver import solve as heuristic_solve


@pytest.fixture(params=["heuristic"], ids=["heuristic"])
def solver_fn(request):
    return heuristic_solve
