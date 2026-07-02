"""Smoke tests for the Runs API routes."""
from __future__ import annotations

import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(
        gui_app.iceberg_browser, "read_run_summary",
        lambda limit_runs=100: {"runs": [{"run_id": "r1", "rows_total": 5}]},
    )
    monkeypatch.setattr(
        gui_app.iceberg_browser, "read_run_detail",
        lambda run_id: {"run_id": run_id, "columns": ["table_name"], "rows": []},
    )
    return gui_app.app.test_client()


def test_runs_summary_route(client):
    resp = client.get("/api/iceberg/runs")
    assert resp.status_code == 200
    assert resp.get_json()["runs"][0]["run_id"] == "r1"


def test_runs_detail_route(client):
    resp = client.get("/api/iceberg/runs/abc123")
    assert resp.status_code == 200
    assert resp.get_json()["run_id"] == "abc123"
