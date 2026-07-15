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


def test_runs_from_payload_filters_flow_jobs():
    import dagster_client as dc
    rows = dc._runs_from_payload([
        {"runId": "r1", "jobName": "flow_nightly__a1b2c3d4", "status": "SUCCESS",
         "startTime": 100.0, "endTime": 160.0},
        {"runId": "r2", "jobName": "__ASSET_JOB", "status": "SUCCESS",
         "startTime": 1.0, "endTime": 2.0},
    ])
    assert len(rows) == 1
    assert rows[0]["run_id"] == "r1"
    assert rows[0]["flow_id"] == "a1b2c3d4"
    assert rows[0]["status"] == "SUCCESS"
    assert rows[0]["start_time"] == 100.0
    assert rows[0]["end_time"] == 160.0
    assert rows[0]["run_link"].endswith("/runs/r1")


def test_flow_runs_empty_when_unreachable(monkeypatch):
    import dagster_client as dc
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "59999")
    assert dc.flow_runs() == []


def test_fmt_event_formats_line():
    import dagster_client as dc
    line = dc._fmt_event({"message": "hello", "timestamp": "1752570000000",
                          "level": "INFO"})
    assert line.endswith("| INFO     | hello")
    assert dc._fmt_event({"message": "", "timestamp": "1", "level": "INFO"}) is None


def test_run_log_tail_parses_connection(monkeypatch):
    import dagster_client as dc
    payload = {"data": {
        "logsForRun": {"__typename": "EventConnection",
                       "events": [{"message": "m1", "timestamp": "1752570000000",
                                   "level": "INFO"},
                                  {"message": "m2", "timestamp": "1752570001000",
                                   "level": "ERROR"}],
                       "cursor": "c2", "hasMore": False},
        "runOrError": {"status": "STARTED"},
    }}
    monkeypatch.setattr(dc, "_query", lambda q, v=None: payload)
    r = dc.run_log_tail("r1", cursor="c0")
    assert "m1" in r["chunk"] and "m2" in r["chunk"]
    assert r["cursor"] == "c2"
    assert r["has_more"] is False
    assert r["status"] == "STARTED"
    assert r["error"] is None


def test_run_log_tail_respects_dagster_page_cap(monkeypatch):
    # Dagster rejects logsForRun limits above 1000 with a PythonError
    # ("Limit of N is too large. Max is 1000"), so the query must stay <= 1000.
    import re
    import dagster_client as dc
    captured = {}
    monkeypatch.setattr(dc, "_query", lambda q, v=None: captured.update(q=q) or {"data": {}})
    dc.run_log_tail("r1")
    m = re.search(r"limit:\s*(\d+)", captured["q"])
    assert m and int(m.group(1)) <= 1000


def test_run_log_tail_run_not_found(monkeypatch):
    import dagster_client as dc
    payload = {"data": {"logsForRun": {"__typename": "RunNotFoundError",
                                       "message": "no run"},
                        "runOrError": {}}}
    monkeypatch.setattr(dc, "_query", lambda q, v=None: payload)
    r = dc.run_log_tail("nope")
    assert r["chunk"] == "" and r["error"] == "no run"
