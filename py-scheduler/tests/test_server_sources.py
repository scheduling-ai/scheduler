"""Unit tests for the UI server's bridge-source loading.

The dropdown source list is parsed from the BRIDGE_SOURCES env var.
Silent failure here = empty dropdown in production with no error,
so this is the boundary worth pinning down.
"""

from __future__ import annotations

import importlib

import pytest


def _reload_server(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    """Reimport scheduler.server with the given env vars set.

    Env-driven config (BRIDGE_SOURCES / BRIDGE_URL / UI_LANDING_PATH) is
    read at module import; reloading rebuilds those constants from the
    new env.

    Returns nothing on purpose — `importlib.reload` mutates the module
    in place, and tests re-import via ``from scheduler import server as
    srv`` so the type checker resolves module attributes (a return
    value typed as ``ModuleType`` would be opaque).
    """
    for key in ("BRIDGE_SOURCES", "BRIDGE_URL", "UI_LANDING_PATH"):
        monkeypatch.delenv(key, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import scheduler.server

    importlib.reload(scheduler.server)


def test_bridge_sources_from_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _reload_server(
        monkeypatch,
        {
            "BRIDGE_SOURCES": (
                '[{"name": "solver", "label": "Solver", "url": "http://a:8080/"},'
                ' {"name": "kueue", "label": "Kueue", "url": "http://b:8080"}]'
            )
        },
    )
    from scheduler import server as srv

    assert [s["name"] for s in srv.BRIDGE_SOURCES] == ["solver", "kueue"]
    # Trailing slashes are stripped so concatenating "/snapshot" works.
    assert srv.BRIDGE_SOURCES_BY_NAME["solver"]["url"] == "http://a:8080"
    assert srv.BRIDGE_SOURCES_BY_NAME["kueue"]["url"] == "http://b:8080"


def test_bridge_sources_label_defaults_to_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """A source missing `label` should fall back to `name` so the
    dropdown still has something to render."""
    _reload_server(
        monkeypatch,
        {"BRIDGE_SOURCES": '[{"name": "kueue", "url": "http://b:8080"}]'},
    )
    from scheduler import server as srv

    assert srv.BRIDGE_SOURCES[0]["label"] == "kueue"
    # shortLabel is optional — absent unless the YAML supplied one.
    assert "shortLabel" not in srv.BRIDGE_SOURCES[0]


def test_bridge_sources_short_label_round_trips(monkeypatch: pytest.MonkeyPatch) -> None:
    """When `shortLabel` is set, it survives the env round-trip and is
    emitted on /api/sources for the header badge to use."""
    _reload_server(
        monkeypatch,
        {
            "BRIDGE_SOURCES": (
                '[{"name": "solver", "label": "Long descriptive label",'
                ' "shortLabel": "Short", "url": "http://a:8080"}]'
            )
        },
    )
    from scheduler import server as srv

    assert srv.BRIDGE_SOURCES[0]["shortLabel"] == "Short"


def test_bridge_sources_skips_invalid_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Entries missing name or url are dropped; the rest still load.
    A typo in one source must not nuke the entire dropdown."""
    _reload_server(
        monkeypatch,
        {
            "BRIDGE_SOURCES": (
                '[{"name": "ok", "url": "http://a:8080"}, {"label": "no-name"}, {"name": "no-url"}]'
            )
        },
    )
    from scheduler import server as srv

    assert [s["name"] for s in srv.BRIDGE_SOURCES] == ["ok"]


def test_bridge_sources_invalid_json_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unparseable BRIDGE_SOURCES leaves the list empty rather than
    crashing the UI server at startup."""
    _reload_server(monkeypatch, {"BRIDGE_SOURCES": "not json"})
    from scheduler import server as srv

    assert srv.BRIDGE_SOURCES == []


def test_bridge_url_backcompat(monkeypatch: pytest.MonkeyPatch) -> None:
    """When BRIDGE_SOURCES is unset but BRIDGE_URL is, derive a single
    'live' source.  This keeps the original docker-compose deployment
    working without manifest changes."""
    _reload_server(monkeypatch, {"BRIDGE_URL": "http://bridge:8080/"})
    from scheduler import server as srv

    assert len(srv.BRIDGE_SOURCES) == 1
    assert srv.BRIDGE_SOURCES[0]["name"] == "live"
    assert srv.BRIDGE_SOURCES[0]["url"] == "http://bridge:8080"


def test_no_env_means_no_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """With neither var set, return an empty list — local dev path
    where the UI falls back to /api/solvers + file-backed state."""
    _reload_server(monkeypatch, {})
    from scheduler import server as srv

    assert srv.BRIDGE_SOURCES == []


def test_landing_path_unset_serves_dev_bundle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No UI_LANDING_PATH = local-dev install: SPA fallback points at
    the dev bundle (chooser + replay + generator + scenarios), and
    every dev-tool URL routes there so the dev bundle's router can
    dispatch."""
    _reload_server(monkeypatch, {})
    from scheduler import server as srv

    assert srv.UI_LANDING_PATH is None
    assert srv.SPA_ENTRY == "/dev.html"
    assert srv.SPA_ROUTES == {"/", "/live", "/replay", "/generator"}


def test_landing_path_serves_prod_bundle_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """UI_LANDING_PATH set = production install: SPA serves only the
    prod bundle and only at the landing surface. `/` and `/index.html`
    302 to the landing path; /replay, /generator, /scenarios, and
    /dev.html fall through to a 404 instead of booting a broken UI or
    exposing the dev bundle."""
    _reload_server(monkeypatch, {"UI_LANDING_PATH": "/live"})
    from scheduler import server as srv

    assert srv.UI_LANDING_PATH == "/live"
    assert srv.SPA_ENTRY == "/index.html"
    assert srv.SPA_ROUTES == {"/live"}
