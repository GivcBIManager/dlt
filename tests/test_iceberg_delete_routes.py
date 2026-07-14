"""DELETE /api/iceberg/tables[...] routes: guard, param passing, error mapping."""
from __future__ import annotations

import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run", lambda: None)
    return gui_app.app.test_client()


def test_delete_table_route(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(
        gui_app.iceberg_browser, "delete_table",
        lambda t: {"deleted": [t], "watermarks_cleared": [t], "rows": 1,
                   "size_bytes": 2, "errors": {}},
    )
    resp = client.delete("/api/iceberg/tables/patient_ad", json={})
    assert resp.status_code == 200
    assert resp.get_json()["deleted"] == ["patient_ad"]


def test_delete_table_blocked_while_run_live(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run",
                        lambda: {"id": "r1", "label": "x"})
    resp = client.delete("/api/iceberg/tables/patient_ad", json={})
    assert resp.status_code == 409
    assert "r1" in resp.get_json()["error"]


def test_delete_table_protected_maps_to_400(client, monkeypatch):
    import app as gui_app

    def boom(t):
        raise ValueError("table not deletable")
    monkeypatch.setattr(gui_app.iceberg_browser, "delete_table", boom)
    resp = client.delete("/api/iceberg/tables/_dlt_loads", json={})
    assert resp.status_code == 400


def test_delete_all_passes_include_system(client, monkeypatch):
    import app as gui_app

    seen = {}

    def fake(include_system=False):
        seen["include_system"] = include_system
        return {"deleted": [], "watermarks_cleared": [], "errors": {}}
    monkeypatch.setattr(gui_app.iceberg_browser, "delete_all_tables", fake)

    resp = client.delete("/api/iceberg/tables", json={"include_system": True})
    assert resp.status_code == 200
    assert seen["include_system"] is True

    resp = client.delete("/api/iceberg/tables", json={})
    assert seen["include_system"] is False
