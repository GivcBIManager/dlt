"""Smoke check: the /run and /iceberg pages still render after JS/display
tweaks to the table-name derivation (query-entry 'name' preference)."""
from __future__ import annotations

import pytest


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


def test_run_page_renders(client):
    resp = client.get("/run")
    assert resp.status_code == 200


def test_iceberg_page_renders(client):
    resp = client.get("/iceberg")
    assert resp.status_code == 200


def test_logs_page_has_flow_runs_tab(client):
    html = client.get("/logs").get_data(as_text=True)
    assert 'data-tab="flowruns"' in html
    assert 'data-panel="flowruns"' in html
