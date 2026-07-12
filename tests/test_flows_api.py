"""Test the Flows API endpoint."""
from __future__ import annotations

import pytest


def test_flows_list_includes_valid_server_timezone(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(gui_app.flows_store, "load_flows", lambda: [])
    monkeypatch.setattr(gui_app.pipelines_store, "load_pipelines", lambda: [])
    client = gui_app.app.test_client()
    resp = client.get("/api/flows")
    assert resp.status_code == 200
    tz = resp.get_json()["server_timezone"]
    from zoneinfo import ZoneInfo
    ZoneInfo(tz)  # must be a real zone (raises otherwise)
