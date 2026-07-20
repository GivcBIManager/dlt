"""GUI reads watermarks + system tables from the Postgres app metastore."""
from __future__ import annotations

import metastore_read  # gui/ is on sys.path via conftest


def test_gui_reads_control_state(pg_meta, monkeypatch):
    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    pg_meta.upsert_control_state([{
        "table_name": "customers", "branch_id": "1",
        "last_cdc_value": "5", "last_cdc_kind": "number",
        "last_date_value": None, "last_date_kind": None,
        "status": "SUCCESS", "row_count": 3, "duration_ms": 1,
        "last_run_at": "2026-07-20T10:00:00"}])
    rows = metastore_read.read_table_rows("control_state")
    assert rows[0]["table_name"] == "customers"


def test_gui_reads_system_tables(pg_meta, monkeypatch):
    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    pg_meta.upsert_etl_control([{
        "table_name": "customers", "branch_id": "1", "status": "SUCCESS",
        "row_count": 3, "last_cdc_value": "5"}])
    pg_meta.append_run_log([{
        "pipeline_run_id": "run-1", "table_name": "customers", "branch_id": "1",
        "status": "SUCCESS", "row_count": 3}])
    pg_meta.append_dq_results([{
        "pipeline_run_id": "run-1", "table_name": "customers", "branch_id": "1",
        "status": "PASS"}])

    ctrl = metastore_read.read_table_rows("etl_control")
    assert ctrl[0]["table_name"] == "customers"
    log = metastore_read.read_table_rows("etl_run_log")
    assert log[0]["pipeline_run_id"] == "run-1"
    dq = metastore_read.read_table_rows("etl_dq_results")
    assert dq[0]["status"] == "PASS"


def test_workspace_control_rows_from_postgres(pg_meta, monkeypatch):
    """workspace.control_rows() flattens the Postgres control_state rows."""
    import metastore_read as mr
    import workspace

    monkeypatch.setattr(mr, "open_metastore", lambda: pg_meta)
    pg_meta.upsert_control_state([{
        "table_name": "customers", "branch_id": "1",
        "last_cdc_value": "5", "last_cdc_kind": "number",
        "last_date_value": "2026-07-20", "last_date_kind": "date",
        "status": "SUCCESS", "row_count": 3, "duration_ms": 42,
        "last_run_at": "2026-07-20T10:00:00"}])

    rows = workspace.control_rows()
    assert len(rows) == 1
    row = rows[0]
    assert row["table"] == "customers"
    assert row["branch"] == "1"
    assert row["status"] == "SUCCESS"
    assert row["row_count"] == 3
    assert row["last_cdc"] == "5"
    assert row["last_date"] == "2026-07-20"

    summary = workspace.control_summary()
    assert summary["units"] == 1
    assert summary["total_rows"] == 3


def test_read_system_table_from_postgres_sorts_newest_first(pg_meta, monkeypatch):
    """read_system_table sources the etl_* tables from Postgres, newest first."""
    import iceberg_browser as ib
    import metastore_read as mr

    monkeypatch.setattr(mr, "open_metastore", lambda: pg_meta)
    pg_meta.append_run_log([
        {"pipeline_run_id": "old", "table_name": "APPT", "branch_id": "1",
         "status": "SUCCESS", "start_time": "2026-07-01T06:00:00"},
        {"pipeline_run_id": "new", "table_name": "APPT", "branch_id": "1",
         "status": "SUCCESS", "start_time": "2026-07-03T06:00:00"},
    ])

    out = ib.read_system_table("etl_run_log", limit=1)
    assert out["table"] == "etl_run_log"
    assert "pipeline_run_id" in out["columns"]
    assert out["total"] == 2  # total reflects all rows, not the limited slice
    assert len(out["rows"]) == 1
    assert out["rows"][0]["pipeline_run_id"] == "new"  # newest by start_time
    # timestamps are rendered JSON-safe (str), not raw datetime objects
    assert isinstance(out["rows"][0]["start_time"], str)


def test_clear_control_state_deletes_from_postgres(pg_meta, monkeypatch):
    """_clear_control_state removes the table's watermark rows from Postgres."""
    import iceberg_browser as ib
    import metastore_read as mr

    monkeypatch.setattr(mr, "open_metastore", lambda: pg_meta)
    pg_meta.upsert_control_state([
        {"table_name": "customers", "branch_id": "1", "status": "SUCCESS",
         "row_count": 3},
        {"table_name": "orders", "branch_id": "1", "status": "SUCCESS",
         "row_count": 9},
    ])

    cleared = ib._clear_control_state(["customers", "missing"])
    assert cleared == ["customers"]

    remaining = {r["table_name"] for r in mr.read_table_rows("control_state")}
    assert remaining == {"orders"}
