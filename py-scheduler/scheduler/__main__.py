"""Entry point for ``python -m scheduler``.

Reads a SolverRequest as JSON from stdin, runs the selected solver, and
writes a ScheduleResult as JSON to stdout.
"""

import json
import sys
from dataclasses import asdict

from scheduler.model import solver_request_from_json
from scheduler.solvers import SOLVERS


def main() -> None:
    solver_name = sys.argv[1] if len(sys.argv) > 1 else "heuristic"
    solve = SOLVERS.get(solver_name)
    if solve is None:
        print(
            f"Unknown solver: {solver_name!r} (expected one of {list(SOLVERS)})",
            file=sys.stderr,
        )
        sys.exit(1)

    request = solver_request_from_json(sys.stdin.read())
    result = solve(
        request.clusters,
        request.pods,
        request.gang_sets,
        request.quotas,
        time_limit=request.time_limit,
    )
    json.dump(asdict(result), sys.stdout)


if __name__ == "__main__":
    main()
