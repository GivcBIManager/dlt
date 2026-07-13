"""Tests for the GUI's 1000-record load caps (data-table pagination)."""
from __future__ import annotations

import iceberg_browser as ib


def test_sample_endpoint_caps_limit_at_1000(monkeypatch):
    import app as gui_app
    seen = {}

    def fake_sample(table, limit=50, **kw):
        seen["limit"] = limit
        return {"columns": [], "rows": [], "snapshot_id": None}

    monkeypatch.setattr(gui_app.iceberg_browser, "sample_rows", fake_sample)
    resp = gui_app.app.test_client().get("/api/iceberg/tables/foo/sample?limit=5000")
    assert resp.status_code == 200
    assert seen["limit"] == 1000


def test_system_endpoint_caps_limit_at_1000(monkeypatch):
    import app as gui_app
    seen = {}

    def fake_sys(table, limit=200):
        seen["limit"] = limit
        return {"table": table, "columns": [], "rows": [], "total": 0}

    monkeypatch.setattr(gui_app.iceberg_browser, "read_system_table", fake_sys)
    resp = gui_app.app.test_client().get("/api/iceberg/system/etl_run_log?limit=5000")
    assert resp.status_code == 200
    assert seen["limit"] == 1000


def test_run_detail_caps_rows_at_1000(monkeypatch):
    # 1500 units for one run -> the response is capped to 1000 rows.
    logs = [
        {
            "pipeline_run_id": "r1", "table_name": f"T{i}", "branch_id": i,
            "load_mode": "INCREMENTAL", "row_count": 1, "status": "SUCCESS",
            "start_time": None, "end_time": None,
            "schema_discrepancy": None, "error_details": None, "recorded_at": None,
        }
        for i in range(1500)
    ]

    def fake_scan(table):
        return {"etl_run_log": logs, "etl_control": []}[table]

    monkeypatch.setattr(ib, "_scan_pylist", fake_scan)
    out = ib.read_run_detail("r1")
    assert len(out["rows"]) == 1000
