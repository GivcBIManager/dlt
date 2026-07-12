def test_link_builders(monkeypatch):
    import config
    import dagster_client as dc
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3000")
    monkeypatch.setenv("OASIS_DAGSTER_HOST", "127.0.0.1")
    assert dc.graphql_url() == "http://127.0.0.1:3000/graphql"
    assert dc.run_link("abc") == "http://127.0.0.1:3000/runs/abc"
    assert "flow_f1" in dc.job_link("flow_f1")


def test_flow_status_returns_empty_when_unreachable(monkeypatch):
    import dagster_client as dc
    # Point at a closed port so the request fails fast.
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "59999")
    assert dc.flow_status() == []


def test_rows_from_repos_parses_flow_id():
    import dagster_client as dc
    nodes = [{
        "jobs": [{"name": "flow_nightly__a1b2c3d4",
                  "runs": [{"runId": "r1", "status": "SUCCESS", "startTime": 1.0}]}],
        "schedules": [{"name": "flow_nightly__a1b2c3d4_schedule",
                       "scheduleState": {"status": "RUNNING"}}],
    }]
    rows = dc._rows_from_repos(nodes)
    assert len(rows) == 1
    assert rows[0]["flow_id"] == "a1b2c3d4"
    assert rows[0]["job"] == "flow_nightly__a1b2c3d4"
    assert rows[0]["schedule_state"] == "RUNNING"
    assert rows[0]["last_run_status"] == "SUCCESS"
    assert rows[0]["run_link"].endswith("/runs/r1")
