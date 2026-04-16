import pytest

from scheduler.milp_solver import solve as milp_solve
from scheduler.solver import solve as mock_solve


@pytest.fixture(params=["mock", "milp"], ids=["mock", "milp"])
def solver_fn(request):
    if request.param == "mock":
        return mock_solve
    return milp_solve
