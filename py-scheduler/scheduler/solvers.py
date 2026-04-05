"""Solver registry."""

from scheduler.solver import solve as heuristic_solve

SOLVERS = {
    "heuristic": heuristic_solve,
}
