"""Unit tests for the UI server's bridge-source loading.

The dropdown source list is parsed from the BRIDGE_SOURCES env var.
Silent failure here = empty dropdown in production with no error,
so this is the boundary worth pinning down.
"""

from __future__ import annotations

import importlib

import pytest


def _reload_server(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> object:
    """Reimport scheduler.server with the given env vars set.

    BRIDGE_SOURCES / BRIDGE_URL are read at module import; reloading
    rebuilds the BRIDGE_SOURCES list from the new env.
    """
    for key in ("BRIDGE_SOURCES", "BRIDGE_URL"):
        monkeypatch.delenv(key, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import scheduler.server as srv

    return importlib.reload(srv)


def test_bridge_sources_from_json(monkeypatch: pytest.MonkeyPatch) -> None:
    srv = _reload_server(
        monkeypatch,
        {
            "BRIDGE_SOURCES": (
                '[{"name": "solver", "label": "Solver", "url": "http://a:8080/"},'
                ' {"name": "kueue", "label": "Kueue", "url": "http://b:8080"}]'
            )
        },
    )
    assert [s["name"] for s in srv.BRIDGE_SOURCES] == ["solver", "kueue"]
    # Trailing slashes are stripped so concatenating "/snapshot" works.
    assert srv.BRIDGE_SOURCES_BY_NAME["solver"]["url"] == "http://a:8080"
    assert srv.BRIDGE_SOURCES_BY_NAME["kueue"]["url"] == "http://b:8080"


def test_bridge_sources_label_defaults_to_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """A source missing `label` should fall back to `name` so the
    dropdown still has something to render."""
    srv = _reload_server(
        monkeypatch,
        {"BRIDGE_SOURCES": '[{"name": "kueue", "url": "http://b:8080"}]'},
    )
    assert srv.BRIDGE_SOURCES[0]["label"] == "kueue"


def test_bridge_sources_skips_invalid_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Entries missing name or url are dropped; the rest still load.
    A typo in one source must not nuke the entire dropdown."""
    srv = _reload_server(
        monkeypatch,
        {
            "BRIDGE_SOURCES": (
                '[{"name": "ok", "url": "http://a:8080"}, {"label": "no-name"}, {"name": "no-url"}]'
            )
        },
    )
    assert [s["name"] for s in srv.BRIDGE_SOURCES] == ["ok"]


def test_bridge_sources_invalid_json_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unparseable BRIDGE_SOURCES leaves the list empty rather than
    crashing the UI server at startup."""
    srv = _reload_server(monkeypatch, {"BRIDGE_SOURCES": "not json"})
    assert srv.BRIDGE_SOURCES == []


def test_bridge_url_backcompat(monkeypatch: pytest.MonkeyPatch) -> None:
    """When BRIDGE_SOURCES is unset but BRIDGE_URL is, derive a single
    'live' source.  This keeps the original docker-compose deployment
    working without manifest changes."""
    srv = _reload_server(monkeypatch, {"BRIDGE_URL": "http://bridge:8080/"})
    assert len(srv.BRIDGE_SOURCES) == 1
    assert srv.BRIDGE_SOURCES[0]["name"] == "live"
    assert srv.BRIDGE_SOURCES[0]["url"] == "http://bridge:8080"


def test_no_env_means_no_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """With neither var set, return an empty list — local dev path
    where the UI falls back to /api/solvers + file-backed state."""
    srv = _reload_server(monkeypatch, {})
    assert srv.BRIDGE_SOURCES == []
