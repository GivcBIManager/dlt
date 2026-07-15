"""Test the Flows API endpoint."""
from __future__ import annotations


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


def test_api_flow_runs_enriches_flow_name(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(gui_app.dagster_client, "flow_runs", lambda limit=50: [
        {"run_id": "r1", "job": "flow_nightly__a1", "flow_id": "a1",
         "status": "SUCCESS", "start_time": 1.0, "end_time": 2.0,
         "run_link": "http://127.0.0.1:3000/runs/r1"}])
    monkeypatch.setattr(gui_app.flows_store, "load_flows",
                        lambda: [{"id": "a1", "name": "Nightly"}])
    rows = gui_app.app.test_client().get("/api/flow-runs").get_json()
    assert rows[0]["flow_name"] == "Nightly"


def test_api_flow_runs_falls_back_to_job_name(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(gui_app.dagster_client, "flow_runs", lambda limit=50: [
        {"run_id": "r1", "job": "flow_gone__zz", "flow_id": "zz",
         "status": "FAILURE", "start_time": 1.0, "end_time": None,
         "run_link": None}])
    monkeypatch.setattr(gui_app.flows_store, "load_flows", lambda: [])
    rows = gui_app.app.test_client().get("/api/flow-runs").get_json()
    assert rows[0]["flow_name"] == "flow_gone__zz"


def test_api_flow_run_log_passes_cursor(monkeypatch):
    import app as gui_app
    seen = {}
    def fake_tail(run_id, cursor=None):
        seen.update(run_id=run_id, cursor=cursor)
        return {"chunk": "x\n", "cursor": "c1", "has_more": False,
                "status": "SUCCESS", "error": None}
    monkeypatch.setattr(gui_app.dagster_client, "run_log_tail", fake_tail)
    r = gui_app.app.test_client().get("/api/flow-runs/r9/log?cursor=c0").get_json()
    assert seen == {"run_id": "r9", "cursor": "c0"}
    assert r["chunk"] == "x\n"
