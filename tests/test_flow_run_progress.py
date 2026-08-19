"""Live per-step progress for a Dagster flow run (Monitor -> Flow runs tab).

Covers the two pure shaping layers: Dagster's ``stepStats`` payload ->
GUI step rows (``dagster_client``), and those rows joined to the flow's saved
node plan (``app``).
"""
from __future__ import annotations

import pytest


def _stats(step_key, status, start=None, end=None, attempts=1):
    return {"stepKey": step_key, "status": status, "startTime": start, "endTime": end,
            "attempts": [{"startTime": start}] * attempts, "materializations": []}


def test_step_stats_become_status_pill_and_duration():
    import dagster_client as dc

    step = dc._step_from_stats(_stats("flow__n1", "SUCCESS", 100.0, 160.5))
    assert step["step_key"] == "flow__n1"
    assert step["pill"] == "success"
    assert step["duration_s"] == 60.5

    running = dc._step_from_stats(_stats("flow__n2", "IN_PROGRESS", 200.0))
    assert running["pill"] == "running"
    assert running["duration_s"] is None    # still running: no duration yet


def test_detail_orders_steps_and_counts_progress():
    import dagster_client as dc

    node = {
        "runId": "r1", "jobName": "flow_nightly__abc", "status": "STARTED",
        "startTime": 100.0, "endTime": None,
        "stats": {"launchTime": 99.0, "enqueuedTime": 95.0},
        "stepStats": [
            _stats("flow__n2", "IN_PROGRESS", 160.0),
            _stats("flow__n1", "SUCCESS", 100.0, 160.0),
        ],
    }
    out = dc._detail_from_payload(node)
    assert [s["step_key"] for s in out["steps"]] == ["flow__n1", "flow__n2"]
    assert (out["steps_done"], out["steps_running"], out["steps_failed"]) == (1, 1, 0)
    assert out["launch_time"] == 99.0 and out["enqueued_time"] == 95.0


def test_run_detail_returns_an_error_shape_when_dagster_is_unreachable(monkeypatch):
    import dagster_client as dc

    monkeypatch.setenv("OASIS_DAGSTER_PORT", "59999")
    out = dc.run_detail("r1")
    assert out["steps"] == [] and out["error"]


def test_unstarted_nodes_come_back_pending():
    """Dagster reports nothing for a step it has not reached, so the plan is
    what turns "2 steps ran" into "2 of 5 steps"."""
    import app as gui_app

    plan = [{"node_id": "n1", "kind": "pipeline", "label": "masters", "deps": []},
            {"node_id": "n2", "kind": "dbt", "label": "dbt run marts", "deps": ["n1"]},
            {"node_id": "n3", "kind": "pipeline", "label": "dq", "deps": ["n2"]}]
    steps = [{"step_key": "nightly__abc__n1", "status": "SUCCESS", "pill": "success",
              "duration_s": 12.0}]
    merged = gui_app._merge_run_steps(plan, steps)
    assert [s["status"] for s in merged] == ["SUCCESS", "PENDING", "PENDING"]
    assert merged[0]["label"] == "masters" and merged[0]["duration_s"] == 12.0
    assert merged[1]["pill"] == "pending"


def test_steps_from_an_edited_flow_still_appear():
    """A node deleted after the run must not make its step vanish from the
    timeline -- the run really did execute it."""
    import app as gui_app

    merged = gui_app._merge_run_steps(
        [{"node_id": "n1", "kind": "pipeline", "label": "masters", "deps": []}],
        [{"step_key": "nightly__abc__gone", "status": "FAILURE", "pill": "failed"}])
    assert [s["status"] for s in merged] == ["PENDING", "FAILURE"]
    assert merged[1]["label"] == "nightly__abc__gone"


def test_node_labels_follow_the_orchestrator_naming():
    import app as gui_app

    pipelines = {"p1": {"id": "p1", "name": "oracle_to_iceberg INCREMENTAL masters"}}
    plan = gui_app._flow_plan({"nodes": [
        {"node_id": "n1", "kind": "pipeline", "pipeline_id": "p1"},
        {"node_id": "n2", "kind": "dbt", "dbt": {"dbt_command": "run", "select": "marts"}},
        {"node_id": "n3", "kind": "command", "command": "echo hi"},
    ]}, pipelines)
    assert [n["label"] for n in plan] == [
        "oracle_to_iceberg INCREMENTAL masters", "dbt run marts", "custom: echo hi"]


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


def test_run_list_enriches_only_the_active_runs(client, monkeypatch):
    """Step progress costs an extra GraphQL round trip per run, so the list
    must fetch it for the (few) live runs and nothing else."""
    import app as gui_app

    monkeypatch.setattr(gui_app.dagster_client, "flow_runs", lambda limit: [
        {"run_id": "r1", "job": "flow_a__f1", "flow_id": "f1", "status": "STARTED",
         "start_time": 1.0, "end_time": None, "run_link": None},
        {"run_id": "r2", "job": "flow_a__f1", "flow_id": "f1", "status": "SUCCESS",
         "start_time": 1.0, "end_time": 2.0, "run_link": None},
    ])
    monkeypatch.setattr(gui_app.flows_store, "load_flows", lambda: [
        {"id": "f1", "name": "Nightly", "nodes": [{"node_id": "n1"}, {"node_id": "n2"}]}])
    called = []

    def fake_detail(run_id):
        called.append(run_id)
        return {"steps_done": 1, "steps_running": 1, "steps_failed": 0, "steps_total": 2,
                "steps": [{"step_key": "a__n2", "status": "IN_PROGRESS"}]}

    monkeypatch.setattr(gui_app.dagster_client, "run_detail", fake_detail)

    rows = client.get("/api/flow-runs").get_json()
    assert called == ["r1"]
    assert rows[0]["active"] is True and rows[0]["progress"]["steps_done"] == 1
    assert rows[0]["progress"]["running"] == ["a__n2"]
    assert rows[0]["flow_name"] == "Nightly" and rows[0]["steps_planned"] == 2
    assert rows[1]["active"] is False and "progress" not in rows[1]


def test_detail_route_joins_the_flow_plan(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.dagster_client, "run_detail", lambda run_id: {
        "run_id": run_id, "job": "flow_nightly__f1", "status": "STARTED",
        "start_time": 1.0, "end_time": None, "steps_total": 1,
        "steps": [{"step_key": "nightly__f1__n1", "status": "SUCCESS", "pill": "success"}],
    })
    monkeypatch.setattr(gui_app.flows_store, "get_flow", lambda fid: {
        "id": "f1", "name": "Nightly",
        "nodes": [{"node_id": "n1", "kind": "pipeline", "pipeline_id": "p1"},
                  {"node_id": "n2", "kind": "pipeline", "pipeline_id": "p1"}]})
    monkeypatch.setattr(gui_app.pipelines_store, "load_pipelines",
                        lambda: [{"id": "p1", "name": "masters"}])

    out = client.get("/api/flow-runs/r1/detail").get_json()
    assert out["flow_name"] == "Nightly"
    assert out["steps_total"] == 2
    assert [s["status"] for s in out["steps"]] == ["SUCCESS", "PENDING"]
    assert out["run_link"].endswith("/runs/r1")
