"""Tests for the Monitor 'Runs' rollup transforms in iceberg_browser."""
from __future__ import annotations

import datetime as dt

import iceberg_browser as ib


def _log_row(run="r1", table="APPT", branch=1, status="SUCCESS", rows=100,
             start="2026-07-02 06:00:00", end="2026-07-02 06:05:00",
             mode="INCREMENTAL", drift=None, err=None, recorded="2026-07-02 06:05:01"):
    def _p(s):
        return dt.datetime.fromisoformat(s) if s else None
    return {
        "pipeline_run_id": run, "table_name": table, "branch_id": branch,
        "load_mode": mode, "row_count": rows, "status": status,
        "start_time": _p(start), "end_time": _p(end),
        "schema_discrepancy": drift, "error_details": err, "recorded_at": _p(recorded),
    }


def test_summarize_groups_by_run():
    rows = [
        _log_row(run="r1", table="APPT", branch=1, rows=100),
        _log_row(run="r1", table="APPT", branch=2, rows=50),
        _log_row(run="r1", table="VISITS", branch=1, rows=25),
        _log_row(run="r2", table="APPT", branch=1, rows=10),
    ]
    out = ib._summarize_runs(rows)
    by_id = {r["run_id"]: r for r in out}
    assert set(by_id) == {"r1", "r2"}
    assert by_id["r1"]["units"] == 3
    assert by_id["r1"]["ok"] == 3
    assert by_id["r1"]["failed"] == 0
    assert by_id["r1"]["rows_total"] == 175
    assert by_id["r1"]["tables"] == 2


def test_wall_clock_duration_not_summed():
    # Two 5-minute units that overlap: wall clock is 10 min, summed would be 10 min too,
    # so stagger them. Unit A 06:00-06:05, Unit B 06:03-06:12 -> wall clock = 12 min.
    rows = [
        _log_row(branch=1, start="2026-07-02 06:00:00", end="2026-07-02 06:05:00"),
        _log_row(branch=2, start="2026-07-02 06:03:00", end="2026-07-02 06:12:00"),
    ]
    out = ib._summarize_runs(rows)
    assert out[0]["duration_wall_ms"] == 12 * 60 * 1000  # 06:00 -> 06:12


def test_failed_drift_and_error_counts():
    rows = [
        _log_row(branch=1, status="SUCCESS", drift=None, err=None),
        _log_row(branch=2, status="FAILED", drift=None, err="boom"),
        _log_row(branch=3, status="SUCCESS", drift='{"added": ["x"]}', err=None),
    ]
    out = ib._summarize_runs(rows)
    r = out[0]
    assert r["ok"] == 2
    assert r["failed"] == 1
    assert r["schema_drift"] == 1
    assert r["errors"] == 1


def test_newest_first_and_limit():
    rows = [
        _log_row(run="old", start="2026-07-01 06:00:00", end="2026-07-01 06:05:00"),
        _log_row(run="new", start="2026-07-02 06:00:00", end="2026-07-02 06:05:00"),
        _log_row(run="mid", start="2026-07-01 18:00:00", end="2026-07-01 18:05:00"),
    ]
    out = ib._summarize_runs(rows, limit_runs=2)
    assert [r["run_id"] for r in out] == ["new", "mid"]


def test_null_times_are_tolerated():
    rows = [_log_row(start=None, end=None, recorded=None)]
    out = ib._summarize_runs(rows)
    assert out[0]["duration_wall_ms"] is None
    assert out[0]["start_time"] is None


def _ctrl_row(table="APPT", branch=1, status="OK", cdc="500", date="2026-07-02",
              updated="2026-07-02 06:05:00"):
    return {
        "table_name": table, "branch_id": branch, "status": status,
        "last_cdc_value": cdc, "last_date_value": date,
        "updated_at": dt.datetime.fromisoformat(updated),
    }


def test_detail_joins_control_watermark():
    logs = [_log_row(table="APPT", branch=1, status="SUCCESS")]
    ctrl = [_ctrl_row(table="APPT", branch=1, status="OK", cdc="500")]
    out = ib._run_detail_rows(logs, ctrl)
    assert len(out) == 1
    row = out[0]
    assert set(ib.RUN_DETAIL_COLUMNS).issubset(row.keys())
    assert row["control_status"] == "OK"
    assert row["last_cdc_value"] == "500"


def test_detail_null_control_when_no_match():
    logs = [_log_row(table="APPT", branch=9, status="SUCCESS")]
    out = ib._run_detail_rows(logs, [])  # no control rows at all
    assert out[0]["control_status"] is None
    assert out[0]["last_cdc_value"] is None
    assert out[0]["control_updated_at"] is None


def test_detail_failed_rows_first():
    logs = [
        _log_row(table="APPT", branch=1, status="SUCCESS"),
        _log_row(table="APPT", branch=2, status="FAILED"),
    ]
    out = ib._run_detail_rows(logs, [])
    assert out[0]["status"] == "FAILED"
    assert out[1]["status"] == "SUCCESS"


def test_read_run_summary_uses_scan(monkeypatch):
    rows = [_log_row(run="r1", branch=1), _log_row(run="r1", branch=2)]
    monkeypatch.setattr(ib, "_scan_pylist", lambda table: rows)
    out = ib.read_run_summary()
    assert out["runs"][0]["run_id"] == "r1"
    assert out["runs"][0]["units"] == 2


def test_read_run_summary_missing_table(monkeypatch):
    def boom(table):
        raise FileNotFoundError(table)
    monkeypatch.setattr(ib, "_scan_pylist", boom)
    assert ib.read_run_summary() == {"runs": []}


def test_read_run_detail_filters_and_joins(monkeypatch):
    logs = [
        _log_row(run="r1", table="APPT", branch=1),
        _log_row(run="r2", table="APPT", branch=1),  # different run, must be excluded
    ]
    ctrl = [_ctrl_row(table="APPT", branch=1, cdc="777")]

    def fake_scan(table, row_filter=None):
        return {"etl_run_log": logs, "etl_control": ctrl}[table]
    monkeypatch.setattr(ib, "_scan_pylist", fake_scan)

    out = ib.read_run_detail("r1")
    assert out["run_id"] == "r1"
    assert out["columns"] == ib.RUN_DETAIL_COLUMNS
    assert len(out["rows"]) == 1
    assert out["rows"][0]["last_cdc_value"] == "777"


def test_read_run_detail_missing_control(monkeypatch):
    logs = [_log_row(run="r1", table="APPT", branch=1)]

    def fake_scan(table, row_filter=None):
        if table == "etl_control":
            raise FileNotFoundError(table)
        return logs
    monkeypatch.setattr(ib, "_scan_pylist", fake_scan)

    out = ib.read_run_detail("r1")
    assert len(out["rows"]) == 1
    assert out["rows"][0]["control_status"] is None
