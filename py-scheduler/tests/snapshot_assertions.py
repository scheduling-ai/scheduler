from __future__ import annotations

import difflib
import json
import os
import re
from pathlib import Path
from typing import Any

import pytest

SNAPSHOT_ROOT = Path(__file__).with_name("__snapshots__")
UPDATE_SNAPSHOTS_ENV = "UPDATE_SNAPSHOTS"


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
    return slug or "snapshot"


def _render_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


class JsonSnapshot:
    def __init__(self, request: pytest.FixtureRequest):
        module_name = request.node.path.stem
        snapshot_name = _slugify(request.node.name)
        self.path = SNAPSHOT_ROOT / module_name / f"{snapshot_name}.json"
        self.update = os.getenv(UPDATE_SNAPSHOTS_ENV) == "1"

    def assert_match(self, value: Any) -> None:
        actual = _render_json(value)
        if self.update:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(actual, encoding="utf-8")
            return

        if not self.path.exists():
            pytest.fail(
                f"Missing snapshot at {self.path}. "
                f"Re-run with {UPDATE_SNAPSHOTS_ENV}=1 to create it."
            )

        expected = self.path.read_text(encoding="utf-8")
        if expected == actual:
            return

        diff = "".join(
            difflib.unified_diff(
                expected.splitlines(keepends=True),
                actual.splitlines(keepends=True),
                fromfile=str(self.path),
                tofile="current",
            )
        )
        pytest.fail(f"Snapshot mismatch for {self.path}\n{diff}")
