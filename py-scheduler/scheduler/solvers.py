"""Solver registry."""

from scheduler.milp_solver import solve as milp_solve
from scheduler.solver import solve as heuristic_solve

SOLVERS = {
    "milp": milp_solve,
    "heuristic": heuristic_solve,
}
