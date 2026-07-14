"""Browser-facing Dagster URLs must inherit the host the client used to reach
the GUI (a remote browser can't use the server's 127.0.0.1)."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_DAGSTER_HOST", raising=False)
    monkeypatch.delenv("OASIS_DAGSTER_PORT", raising=False)
    monkeypatch.delenv("OASIS_GUI_HOST", raising=False)


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


def test_dagster_status_url_uses_request_host(client):
    resp = client.get("/api/dagster/status", headers={"Host": "etl.example.com:8765"})
    assert resp.get_json()["url"] == "http://etl.example.com:3000"


def test_flows_api_dagster_url_uses_request_host(client):
    resp = client.get("/api/flows", headers={"Host": "10.1.2.3:8765"})
    assert resp.get_json()["dagster"]["url"] == "http://10.1.2.3:3000"


def test_publicise_dagster_links_rewrites_local_prefix():
    import app as gui_app
    items = [
        {"link": "http://127.0.0.1:3000/jobs/x",
         "run_link": "http://127.0.0.1:3000/runs/abc"},
        {"link": "http://127.0.0.1:3000/jobs/y", "run_link": None},
    ]
    with gui_app.app.test_request_context(base_url="http://etl.example.com:8765/"):
        out = gui_app._publicise_dagster_links(items)
    assert out[0]["link"] == "http://etl.example.com:3000/jobs/x"
    assert out[0]["run_link"] == "http://etl.example.com:3000/runs/abc"
    assert out[1]["run_link"] is None
