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
        "duration_ms": 10_000, "read_duration_ms": 10_000,
        "load_duration_ms": 2_000, "total_duration_ms": 12_000,
        "status": "SUCCESS", "attempts": 1,
        "write_disposition": "merge", "load_status": "LOADED",
        "error_details": None, "schema_discrepancy": None,
        "recorded_at": dt.datetime(2026, 8, 1, 6, 0, 10),
    }
    base.update(kw)
    return base


def _summarize(rows, **kw):
    """Summarize without touching secrets.toml / tables.json for the labels.

    ``label_rows`` is what resolves branch names and table types; the tests that
    care about it call it explicitly with their own maps.
    """
    return run_insights.summarize(
        run_insights.label_rows(rows, branches={}, types={}), labelled=True, **kw)


def test_kpis_count_units_rows_and_scope():
    out = _summarize([
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
    out = _summarize(rows)
    by_id = {r["run_id"]: r["status"] for r in run_insights._run_rollup(rows)}
    assert by_id == {"clean": "SUCCESS", "mixed": "PARTIAL", "broken": "FAILED"}
    assert out["kpi"]["runs_partial"] == 1
    assert {e["key"]: e["n"] for e in out["runs_by_status"]} == \
        {"SUCCESS": 1, "PARTIAL": 1, "FAILED": 1}


def test_run_duration_is_wall_clock_not_summed_units():
    """Units run in parallel, so a run's duration is max(end) - min(start);
    summing per-unit durations would report ~2x the real elapsed time."""
    rows = [
        _row(start_time=dt.datetime(2026, 8, 1, 6, 0, 0),
             end_time=dt.datetime(2026, 8, 1, 6, 1, 0),
             duration_ms=60_000, read_duration_ms=60_000,
             load_duration_ms=None, total_duration_ms=None),
        _row(table_name="orders", start_time=dt.datetime(2026, 8, 1, 6, 0, 30),
             end_time=dt.datetime(2026, 8, 1, 6, 1, 30),
             duration_ms=60_000, read_duration_ms=60_000,
             load_duration_ms=None, total_duration_ms=None),
    ]
    run = run_insights._run_rollup(rows)[0]
    assert run["wall_ms"] == 90_000
    assert run["busy_ms"] == 120_000


def test_run_load_time_counts_each_table_once():
    """One Iceberg commit covers every branch of a table, so the pipeline stamps
    the SAME load_duration_ms on each of that table's units. Summing it unit by
    unit would multiply the write phase by the branch count."""
    rows = [
        _row(table_name="customers", branch_id="1", read_duration_ms=1_000,
             load_duration_ms=5_000, total_duration_ms=6_000),
        _row(table_name="customers", branch_id="2", read_duration_ms=1_000,
             load_duration_ms=5_000, total_duration_ms=6_000),
        _row(table_name="orders", branch_id="1", read_duration_ms=1_000,
             load_duration_ms=3_000, total_duration_ms=4_000),
    ]
    run = run_insights._run_rollup(rows)[0]
    assert run["read_ms"] == 3_000        # per unit, so all three add up
    assert run["load_ms"] == 8_000        # 5s for customers + 3s for orders
    assert run["busy_ms"] == 11_000


def test_total_duration_is_read_plus_load_and_never_a_silent_fallback():
    """A row with no load timing has an UNKNOWN total, not a total equal to its
    read -- charting read-only numbers as a total would understate every one."""
    assert run_insights.total_ms(_row(read_duration_ms=1_000, load_duration_ms=250,
                                      total_duration_ms=None)) == 1_250
    assert run_insights.total_ms(_row(load_duration_ms=None, total_duration_ms=None)) is None
    # duration_ms is the pre-split name for the read phase.
    assert run_insights.read_ms({"duration_ms": 900}) == 900
    assert run_insights.read_ms({"duration_ms": 900, "read_duration_ms": 700}) == 700


def test_load_coverage_is_reported_so_the_gui_can_say_so():
    out = _summarize([
        _row(load_duration_ms=2_000, total_duration_ms=12_000),
        _row(table_name="orders", load_duration_ms=None, total_duration_ms=None),
    ])
    assert out["coverage"] == {"units": 2, "with_load_ms": 1}
    assert out["kpi"]["load_n"] == 1
    assert out["kpi"]["read_n"] == 2


def test_time_buckets_carry_units_rows_and_run_outcomes():
    rows = [
        _row(start_time=dt.datetime(2026, 8, 1, 6, 0), row_count=5),
        _row(pipeline_run_id="run-2", start_time=dt.datetime(2026, 8, 2, 6, 0),
             row_count=7, status="FAILED"),
    ]
    trend = {d["key"]: d for d in _summarize(rows)["trend"]}
    assert set(trend) == {"2026-08-01", "2026-08-02"}
    assert trend["2026-08-01"]["rows"] == 5
    assert trend["2026-08-01"]["runs_ok"] == 1
    assert trend["2026-08-02"]["runs_failed"] == 1
    assert trend["2026-08-02"]["failed"] == 1


def test_a_short_window_buckets_by_hour_not_by_day():
    """"Last 24 hours" charted per day is one column wide and says nothing."""
    assert run_insights.granularity_for(1) == "hour"
    assert run_insights.granularity_for(7) == "day"
    assert run_insights.granularity_for(0) == "day"     # all history
    rows = [
        _row(start_time=dt.datetime(2026, 8, 1, 6, 10)),
        _row(pipeline_run_id="run-2", start_time=dt.datetime(2026, 8, 1, 9, 40)),
    ]
    trend = _summarize(rows, granularity="hour")["trend"]
    assert [d["key"] for d in trend] == ["2026-08-01 06", "2026-08-01 09"]
    assert [d["label"] for d in trend] == ["06:00", "09:00"]
    assert trend[0]["runs_ok"] == 1 and trend[1]["runs_ok"] == 1


def test_branch_ids_are_labelled_with_their_configured_name():
    """The run log stores an id; every axis, table and filter shows the name."""
    rows = run_insights.label_rows(
        [_row(branch_id="1"), _row(branch_id="7")],
        branches={"1": "Al Rabwah", "7": "Al Nakheel"}, types={})
    out = run_insights.summarize(rows, labelled=True)
    assert {b["key"]: b["label"] for b in out["by_branch"]} == \
        {"1": "Al Rabwah", "7": "Al Nakheel"}
    assert out["facets"]["branch"] == [
        {"value": "7", "label": "Al Nakheel"}, {"value": "1", "label": "Al Rabwah"}]
    assert sorted(out["heatmap"]["branches"]) == ["Al Nakheel", "Al Rabwah"]
    # An id with no configured name still shows something rather than blank.
    unnamed = run_insights.label_rows([_row(branch_id="9")], branches={}, types={})
    assert unnamed[0]["branch"] == "9"


def test_table_type_is_resolved_filtered_and_rolled_up():
    rows = run_insights.label_rows(
        [_row(table_name="customers", row_count=10),
         _row(table_name="orders", row_count=90)],
        branches={}, types={"customers": "masters", "orders": "transactions"})
    out = run_insights.summarize(rows, labelled=True)
    assert {e["key"]: e["n"] for e in out["rows_by_table_type"]} == \
        {"masters": 10, "transactions": 90}
    assert {t["key"]: t["table_type"] for t in out["by_table"]} == \
        {"customers": "masters", "orders": "transactions"}
    assert out["facets"]["table_type"] == ["masters", "transactions"]
    # A table missing from tables.json is labelled, not dropped.
    assert run_insights.label_rows([_row(table_name="ghost")],
                                   branches={}, types={})[0]["table_type"] == "UNKNOWN"
    narrowed = run_insights.summarize(rows, table_type="masters", labelled=True)
    assert narrowed["kpi"]["rows"] == 10
    assert narrowed["facets"]["table_type"] == ["masters", "transactions"]


def test_filters_narrow_the_slice_but_not_the_facet_lists():
    """A filter must never remove its own option from the dropdown."""
    rows = [_row(branch_id="1"), _row(branch_id="2", row_count=999)]
    out = _summarize(rows, branch="1")
    assert out["kpi"]["units"] == 1
    assert out["kpi"]["rows"] == 100
    assert [b["value"] for b in out["facets"]["branch"]] == ["1", "2"]
    assert out["filters"]["branch"] == "1"


def test_per_branch_and_per_table_rollups_rank_by_volume():
    rows = [
        _row(table_name="orders", branch_id="1", row_count=10,
             read_duration_ms=1_000, load_duration_ms=500, total_duration_ms=1_500),
        _row(table_name="customers", branch_id="2", row_count=50,
             read_duration_ms=3_000, load_duration_ms=1_000, total_duration_ms=4_000),
        _row(table_name="customers", branch_id="2", row_count=40,
             read_duration_ms=5_000, load_duration_ms=1_000, total_duration_ms=6_000),
    ]
    out = _summarize(rows)
    assert [b["key"] for b in out["by_branch"]] == ["2", "1"]
    top = out["by_table"][0]
    assert top["key"] == "customers"
    assert top["rows"] == 90
    assert top["read_avg_ms"] == 4_000
    assert top["load_avg_ms"] == 1_000
    assert top["total_avg_ms"] == 5_000
    assert top["total_max_ms"] == 6_000


def test_retries_and_schema_drift_are_tallied():
    out = _summarize([
        _row(attempts=3), _row(table_name="orders", schema_discrepancy="new col x"),
    ])
    assert out["kpi"]["retries"] == 1
    assert out["kpi"]["drift"] == 1


def test_slowest_list_is_bounded_and_ordered():
    rows = [_row(table_name=f"t{i}", read_duration_ms=i * 1000, load_duration_ms=0,
                 total_duration_ms=i * 1000,
                 start_time=dt.datetime(2026, 8, 1, 6, i)) for i in range(1, 40)]
    out = _summarize(rows)
    assert len(out["slowest"]) == 10
    assert out["slowest"][0]["total_ms"] == 39_000     # slowest first
    assert out["slowest"][0]["read_ms"] == 39_000


def test_payload_carries_only_the_cuts_the_tab_draws():
    """One response per filter change, so a rollup nothing charts is bytes spent
    on every interaction. Pin the key set: when a card is removed, its cut has to
    go with it, and this is what fails if it does not."""
    assert set(_summarize([_row()])) == {
        "facets", "filters", "granularity", "coverage", "kpi", "trend",
        "runs_by_status", "rows_by_table_type", "by_branch", "by_table",
        "by_table_branch", "by_table_type", "heatmap", "slowest",
    }


def test_heatmap_axes_are_capped_and_cells_carry_every_duration():
    """Branches and tables cap separately: the grid has few branch rows and many
    table columns, so one cap for both would waste most of the canvas."""
    rows = [_row(table_name=f"t{i}", branch_id=str(i % 3),
                 status="FAILED" if i == 0 else "SUCCESS") for i in range(20)]
    heat = _summarize(rows, top_n=5, heat_n=8)["heatmap"]
    assert len(heat["tables"]) == 8 and len(heat["branches"]) == 3
    assert all(c["branch"] in heat["branches"] and c["table"] in heat["tables"]
               for c in heat["cells"])
    cell = heat["cells"][0]
    assert cell["total_avg_ms"] == 12_000
    assert cell["read_avg_ms"] == 10_000
    assert cell["load_avg_ms"] == 2_000
    # The branch cap still applies -- a deployment with many branches must not
    # push the grid past what fits.
    assert len(_summarize(rows, top_n=2, heat_n=8)["heatmap"]["branches"]) == 2


def test_heatmap_table_columns_rank_by_total_read_time():
    """The grid answers "where does the read phase spend its time", so a table
    loaded often but quickly must not outrank one loaded once that burns an
    hour. Ranking by load COUNT would invert exactly that pair."""
    rows = (
        # 6 quick loads: 6s of read time in total, but the highest unit count.
        [_row(table_name="chatty", read_duration_ms=1_000, branch_id=str(i))
         for i in range(6)]
        # 1 slow load: 60s of read time, the heaviest table in the window.
        + [_row(table_name="heavy", read_duration_ms=60_000)]
        # 2 middling loads: 20s total.
        + [_row(table_name="middling", read_duration_ms=10_000, branch_id=str(i))
           for i in range(2)]
    )
    out = _summarize(rows)
    assert out["heatmap"]["tables"] == ["heavy", "middling", "chatty"]
    by_table = {t["key"]: t for t in out["by_table"]}
    assert by_table["chatty"]["units"] > by_table["heavy"]["units"]   # count says otherwise
    assert by_table["heavy"]["read_total_ms"] == 60_000


def test_table_branch_leaves_back_the_collapsible_tree():
    """The by-table card is a table type -> table -> branch tree; the leaf level
    is its own rollup because by_table alone cannot be expanded per branch."""
    rows = run_insights.label_rows([
        _row(table_name="customers", branch_id="1", row_count=10,
             read_duration_ms=1_000, load_duration_ms=500, total_duration_ms=1_500),
        _row(table_name="customers", branch_id="2", row_count=30,
             read_duration_ms=3_000, load_duration_ms=500, total_duration_ms=3_500),
        _row(table_name="orders", branch_id="1", row_count=5, status="FAILED"),
    ], branches={"1": "Alrabwah", "2": "Khamis"},
       types={"customers": "masters", "orders": "transactions"})
    out = run_insights.summarize(rows, labelled=True)
    leaves = {(e["table"], e["branch_id"]): e for e in out["by_table_branch"]}
    assert set(leaves) == {("customers", "1"), ("customers", "2"), ("orders", "1")}
    assert leaves[("customers", "2")]["branch"] == "Khamis"
    assert leaves[("customers", "2")]["rows"] == 30
    assert leaves[("customers", "1")]["total_avg_ms"] == 1_500
    assert leaves[("orders", "1")]["failed"] == 1
    # Each leaf's table must exist in by_table, and each table's type in
    # by_table_type -- the tree is built by joining the three on these keys.
    tables = {t["key"]: t for t in out["by_table"]}
    types_seen = {t["key"] for t in out["by_table_type"]}
    assert all(e["table"] in tables for e in out["by_table_branch"])
    assert all(t["table_type"] in types_seen for t in out["by_table"])
    # A type row is the average over its loads, not an average of averages.
    masters = next(t for t in out["by_table_type"] if t["key"] == "masters")
    assert masters["units"] == 2 and masters["rows"] == 40
    assert masters["read_avg_ms"] == 2_000


def test_percentile_is_nearest_rank_and_empty_safe():
    assert run_insights.percentile([], 0.95) is None
    assert run_insights.percentile([1], 0.95) == 1
    assert run_insights.percentile(list(range(1, 101)), 0.95) == 95
    assert run_insights.percentile(list(range(1, 101)), 0.50) == 50


def test_rows_without_a_start_time_fall_back_to_recorded_at():
    """etl_run_log rows for a load that never started still have recorded_at;
    dropping them would silently under-count failures."""
    out = _summarize([
        _row(start_time=None, end_time=None, duration_ms=None, read_duration_ms=None,
             load_duration_ms=None, total_duration_ms=None, status="FAILED",
             recorded_at=dt.datetime(2026, 8, 3, 5, 0)),
    ])
    assert out["trend"][0]["key"] == "2026-08-03"
    assert out["kpi"]["units_failed"] == 1


def test_empty_window_still_returns_a_full_payload():
    out = _summarize([])
    assert out["kpi"]["runs"] == 0 and out["kpi"]["rows"] == 0
    assert out["kpi"]["total_avg_ms"] is None
    for key in ("trend", "by_branch", "by_table", "by_table_branch",
                "by_table_type", "rows_by_table_type", "slowest"):
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


def test_read_window_projects_the_split_duration_columns(pg_meta, monkeypatch):
    """The three duration columns must survive the projection, or every chart
    that separates the read phase from the write phase silently reads null."""
    import metastore_read

    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    now = dt.datetime(2026, 8, 20, 12, 0)
    pg_meta.append_run_log([
        {"pipeline_run_id": "r", "table_name": "customers", "branch_id": "1",
         "status": "SUCCESS", "row_count": 2, "start_time": now - dt.timedelta(hours=1),
         "duration_ms": 800, "read_duration_ms": 800, "load_duration_ms": 200,
         "total_duration_ms": 1_000},
    ])
    rows, _ = run_insights.read_window(1, now=now)
    assert rows[0]["read_duration_ms"] == 800
    assert rows[0]["load_duration_ms"] == 200
    assert run_insights.total_ms(rows[0]) == 1_000


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


def test_insights_tab_offers_a_last_24_hours_window(client):
    js = client.get("/static/insights.js").get_data(as_text=True)
    assert '[1, "Last 24 hours"]' in js


def test_monitor_no_longer_carries_a_runs_tab(client):
    """The per-run rollup tab was removed; the Insights tab covers runs now.
    Its markup, its view module and its wiring must all be gone together -- a
    leftover onclick on a deleted node throws and kills the page's script."""
    html = client.get("/logs").get_data(as_text=True)
    for leftover in ('data-tab="runs"', 'data-panel="runs"', "runsView",
                     "refresh-runs", "runs-list", "runs-bar", "runs-stat"):
        assert leftover not in html, leftover
    # The remaining tabs still pair a button with a panel.
    for tab in ("files", "insights", "flowruns", "dq", "runlog", "control"):
        assert f'data-tab="{tab}"' in html and f'data-panel="{tab}"' in html


def test_insights_tab_reports_pipeline_run_duration(client):
    """Wall clock for a whole run gets its own tile and its own trend, distinct
    from the per-load durations."""
    js = client.get("/static/insights.js").get_data(as_text=True)
    assert '"Avg pipeline run"' in js
    assert "run_wall_avg_ms" in js and "wall_avg_ms" in js


def test_insights_route_passes_filters_through(client, monkeypatch):
    import app as gui_app

    seen = {}

    def fake(**kwargs):
        seen.update(kwargs)
        return {"kpi": {}}

    monkeypatch.setattr(gui_app.run_insights, "insights", fake)
    resp = client.get(
        "/api/insights/run-log?days=1&branch=2&table=orders&table_type=masters&load_mode=FULL")
    assert resp.status_code == 200
    assert seen == {"days": 1, "branch": "2", "table": "orders",
                    "table_type": "masters", "load_mode": "FULL", "status": ""}
