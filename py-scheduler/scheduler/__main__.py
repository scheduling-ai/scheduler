"""Entry point for ``python -m scheduler``.

Reads a SolverRequest as JSON from stdin, runs the selected solver, and
writes a ScheduleResult as JSON to stdout.
"""

import json
import sys
from dataclasses import asdict
from time import perf_counter

import scheduler.observability  # noqa: F401 — initialise logging/sentry
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
    started = perf_counter()
    result = solve(
        request.clusters,
        request.pods,
        request.gang_sets,
        request.quotas,
        time_limit=request.time_limit,
    )
    duration_ms = round((perf_counter() - started) * 1000)

    # Emit per-cycle metrics so the Sentry Application Metrics view shows
    # solver health.  Heuristic is too fast and too frequent to be useful;
    # only the real solvers are reported.
    if solver_name != "heuristic":
        try:
            import sentry_sdk

            attrs = {"solver": solver_name}
            sentry_sdk.metrics.distribution(
                "solver.duration_ms", duration_ms, unit="millisecond", attributes=attrs
            )
            sentry_sdk.metrics.gauge("solver.pod_count", len(request.pods), attributes=attrs)
        except Exception:
            pass

    json.dump(asdict(result), sys.stdout)


if __name__ == "__main__":
    main()
