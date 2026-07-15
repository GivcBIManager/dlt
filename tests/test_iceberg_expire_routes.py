"""POST /api/iceberg/tables/<t>/expire-snapshots: guard + error mapping."""
from __future__ import annotations

import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run", lambda: None)
    return gui_app.app.test_client()


def test_expire_route(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(
        gui_app.iceberg_maintenance, "expire_snapshots",
        lambda t: {"table": t, "expired": 3, "remaining": 1,
                   "orphans_deleted": 5, "bytes_freed": 1024, "errors": {}},
    )
    resp = client.post("/api/iceberg/tables/patient_ad/expire-snapshots", json={})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["expired"] == 3
    assert body["table"] == "patient_ad"


def test_expire_blocked_while_run_live(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run",
                        lambda: {"id": "r1", "label": "x"})
    resp = client.post("/api/iceberg/tables/patient_ad/expire-snapshots", json={})
    assert resp.status_code == 409
    assert "r1" in resp.get_json()["error"]


def test_expire_unknown_table_maps_404(client, monkeypatch):
    import app as gui_app

    def boom(t):
        raise FileNotFoundError(t)
    monkeypatch.setattr(gui_app.iceberg_maintenance, "expire_snapshots", boom)
    resp = client.post("/api/iceberg/tables/nope/expire-snapshots", json={})
    assert resp.status_code == 404


def test_expire_protected_name_maps_400(client, monkeypatch):
    import app as gui_app

    def boom(t):
        raise ValueError("snapshots not expirable")
    monkeypatch.setattr(gui_app.iceberg_maintenance, "expire_snapshots", boom)
    resp = client.post("/api/iceberg/tables/_dlt_loads/expire-snapshots", json={})
    assert resp.status_code == 400
