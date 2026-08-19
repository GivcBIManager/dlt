"""The Monitor Insights tab's aggregation layer (gui/run_insights.py).

``summarize`` is a pure function over already-fetched etl_run_log rows, so every
cut the dashboard draws is checked here without a database.
"""
from __future__ import annotations

import datetime as dt

import pytest

import run_insights  # gui/ is on sys.path via conftest


def _row(**kw):
    base = {
        "pipeline_run_id": "run-1", "table_name": "customers", "branch_id": "1",
        "load_mode": "INCREMENTAL", "row_count": 100,
        "start_time": dt.datetime(2026, 8, 1, 6, 0, 0),
        "end_time": dt.datetime(2026, 8, 1, 6, 0, 10),
        "duration_ms": 10_000, "status": "SUCCESS", "attempts": 1,
        "write_disposition": "merge", "load_status": "LOADED",
        "error_details": None, "schema_discrepancy": None,
        "recorded_at": dt.datetime(2026, 8, 1, 6, 0, 10),
    }
    base.update(kw)
    return base


def test_kpis_count_units_rows_and_scope():
    out = run_insights.summarize([
        _row(table_name="customers", branch_id="1", row_count=10),
        _row(table_name="customers", branch_id="2", row_count=20),
        _row(table_name="orders", branch_id="1", row_count=30),
    ])
    k = out["kpi"]
    assert (k["units"], k["units_ok"], k["units_failed"]) == (3, 3, 0)
    assert k["rows"] == 60
    assert (k["tables"], k["branches"]) == (2, 2)
    assert k["success_rate"] == 100.0


def test_run_status_is_derived_from_its_units():
    """All-ok is SUCCESS, none-ok is FAILED, anything between is PARTIAL --
    the distinction the runs-by-outcome chart exists to draw."""
    rows = [
        _row(pipeline_run_id="clean", status="SUCCESS"),
        _row(pipeline_run_id="mixed", status="SUCCESS"),
        _row(pipeline_run_id="mixed", table_name="orders", status="FAILED"),
        _row(pipeline_run_id="broken", status="FAILED"),
    ]
    out = run_insights.summarize(rows)
    by_id = {r["run_id"]: r["status"] for r in out["runs"]}
    assert by_id == {"clean": "SUCCESS", "mixed": "PARTIAL", "broken": "FAILED"}
    assert out["kpi"]["runs_partial"] == 1
    assert {e["key"]: e["n"] for e in out["runs_by_status"]} == \
        {"SUCCESS": 1, "PARTIAL": 1, "FAILED": 1}


def test_run_duration_is_wall_clock_not_summed_units():
    """Units run in parallel, so a run's duration is max(end) - min(start);
    summing per-unit durations would report ~2x the real elapsed time."""
    rows = [
        _row(start_time=dt.datetime(2026, 8, 1, 6, 0, 0),
             end_time=dt.datetime(2026, 8, 1, 6, 1, 0), duration_ms=60_000),
        _row(table_name="orders", start_time=dt.datetime(2026, 8, 1, 6, 0, 30),
             end_time=dt.datetime(2026, 8, 1, 6, 1, 30), duration_ms=60_000),
    ]
    run = run_insights.summarize(rows)["runs"][0]
    assert run["wall_ms"] == 90_000
    assert run["busy_ms"] == 120_000


def test_daily_buckets_carry_units_rows_and_run_outcomes():
    rows = [
        _row(start_time=dt.datetime(2026, 8, 1, 6, 0), row_count=5),
        _row(pipeline_run_id="run-2", start_time=dt.datetime(2026, 8, 2, 6, 0),
             row_count=7, status="FAILED"),
    ]
    daily = {d["key"]: d for d in run_insights.summarize(rows)["daily"]}
    assert set(daily) == {"2026-08-01", "2026-08-02"}
    assert daily["2026-08-01"]["rows"] == 5
    assert daily["2026-08-01"]["runs_ok"] == 1
    assert daily["2026-08-02"]["runs_failed"] == 1
    assert daily["2026-08-02"]["failed"] == 1


def test_filters_narrow_the_slice_but_not_the_facet_lists():
    """A filter must never remove its own option from the dropdown."""
    rows = [_row(branch_id="1"), _row(branch_id="2", row_count=999)]
    out = run_insights.summarize(rows, branch="1")
    assert out["kpi"]["units"] == 1
    assert out["kpi"]["rows"] == 100
    assert out["facets"]["branch_id"] == ["1", "2"]
    assert out["filters"]["branch"] == "1"


def test_per_branch_and_per_table_rollups_rank_by_volume():
    rows = [
        _row(table_name="orders", branch_id="1", row_count=10, duration_ms=1_000),
        _row(table_name="customers", branch_id="2", row_count=50, duration_ms=3_000),
        _row(table_name="customers", branch_id="2", row_count=40, duration_ms=5_000),
    ]
    out = run_insights.summarize(rows)
    assert [b["key"] for b in out["by_branch"]] == ["2", "1"]
    top = out["by_table"][0]
    assert top["key"] == "customers"
    assert top["rows"] == 90
    assert top["avg_ms"] == 4_000
    assert top["max_ms"] == 5_000


def test_retries_and_schema_drift_are_tallied():
    out = run_insights.summarize([
        _row(attempts=3), _row(table_name="orders", schema_discrepancy="new col x"),
    ])
    assert out["kpi"]["retries"] == 1
    assert out["kpi"]["drift"] == 1


def test_failures_and_slowest_lists_are_bounded_and_ordered():
    rows = [_row(table_name=f"t{i}", duration_ms=i * 1000,
                 status="FAILED", error_details=f"boom {i}",
                 start_time=dt.datetime(2026, 8, 1, 6, i)) for i in range(1, 40)]
    out = run_insights.summarize(rows)
    assert len(out["failures"]) == 25
    assert out["failures"][0]["table"] == "t39"          # newest first
    assert len(out["slowest"]) == 10
    assert out["slowest"][0]["duration_ms"] == 39_000    # slowest first


def test_heatmap_axes_are_capped_and_cells_carry_failures():
    rows = [_row(table_name=f"t{i}", branch_id=str(i % 3),
                 status="FAILED" if i == 0 else "SUCCESS") for i in range(20)]
    heat = run_insights.summarize(rows, top_n=5)["heatmap"]
    assert len(heat["tables"]) == 5 and len(heat["branches"]) == 3
    assert all(c["branch"] in heat["branches"] and c["table"] in heat["tables"]
               for c in heat["cells"])


def test_percentile_is_nearest_rank_and_empty_safe():
    assert run_insights.percentile([], 0.95) is None
    assert run_insights.percentile([1], 0.95) == 1
    assert run_insights.percentile(list(range(1, 101)), 0.95) == 95
    assert run_insights.percentile(list(range(1, 101)), 0.50) == 50


def test_rows_without_a_start_time_fall_back_to_recorded_at():
    """etl_run_log rows for a load that never started still have recorded_at;
    dropping them would silently under-count failures."""
    out = run_insights.summarize([
        _row(start_time=None, end_time=None, duration_ms=None, status="FAILED",
             recorded_at=dt.datetime(2026, 8, 3, 5, 0)),
    ])
    assert out["daily"][0]["key"] == "2026-08-03"
    assert out["kpi"]["units_failed"] == 1


def test_empty_window_still_returns_a_full_payload():
    out = run_insights.summarize([])
    assert out["kpi"]["runs"] == 0 and out["kpi"]["rows"] == 0
    for key in ("daily", "hourly", "by_branch", "by_table", "runs", "failures"):
        assert isinstance(out[key], list)


def test_read_window_filters_and_caps_in_sql(pg_meta, monkeypatch):
    """Integration: the window predicate and cap are pushed into Postgres."""
    import metastore_read

    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    now = dt.datetime(2026, 8, 20, 12, 0)
    pg_meta.append_run_log([
        {"pipeline_run_id": "old", "table_name": "customers", "branch_id": "1",
         "status": "SUCCESS", "row_count": 1, "start_time": now - dt.timedelta(days=90)},
        {"pipeline_run_id": "new", "table_name": "customers", "branch_id": "1",
         "status": "SUCCESS", "row_count": 2, "start_time": now - dt.timedelta(days=1)},
    ])
    rows, truncated = run_insights.read_window(30, now=now)
    assert [r["pipeline_run_id"] for r in rows] == ["new"]
    assert truncated is False

    rows, truncated = run_insights.read_window(0, cap=1, now=now)
    assert len(rows) == 1 and truncated is True


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


def test_monitor_page_serves_the_insights_tab(client):
    html = client.get("/logs").get_data(as_text=True)
    assert 'data-tab="insights"' in html and 'data-panel="insights"' in html
    for asset in ("charts.js", "insights.js"):
        assert asset in html
        assert client.get(f"/static/{asset}").status_code == 200


def test_monitor_hosts_two_independent_log_dashboards(client):
    """The file tab and the flow-run tab each render _dash.html; without the
    id prefix they would fight over the same nodes."""
    html = client.get("/logs").get_data(as_text=True)
    assert 'id="run-dash"' in html and 'id="fr-run-dash"' in html
    assert 'id="rd-tbody"' in html and 'id="fr-rd-tbody"' in html
    assert 'createLogDash({ prefix: "fr-" })' in \
        client.get("/static/flowruns.js").get_data(as_text=True)


def test_insights_route_passes_filters_through(client, monkeypatch):
    import app as gui_app

    seen = {}

    def fake(**kwargs):
        seen.update(kwargs)
        return {"kpi": {}}

    monkeypatch.setattr(gui_app.run_insights, "insights", fake)
    resp = client.get("/api/insights/run-log?days=7&branch=2&table=orders&load_mode=FULL")
    assert resp.status_code == 200
    assert seen == {"days": 7, "branch": "2", "table": "orders",
                    "load_mode": "FULL", "status": ""}
