"""_scan_pylist reads the etl_* system tables from Postgres now.

The three observability tables (etl_control / etl_run_log / etl_dq_results) were
moved out of Iceberg into the Postgres app metastore, so _scan_pylist sources
their rows from ``metastore_read.read_table_rows`` and applies the (only ever
``EqualTo``) predicate in Python. read_run_detail still pushes the run-id
predicate down; that predicate object reaches _scan_pylist unchanged.
"""
from __future__ import annotations

import iceberg_browser as ib


def test_scan_pylist_forwards_row_filter(monkeypatch):
    """With an EqualTo filter, only the matching Postgres rows are returned."""
    from pyiceberg.expressions import EqualTo

    import metastore_read

    all_rows = [
        {"pipeline_run_id": "r1", "table_name": "APPT"},
        {"pipeline_run_id": "r2", "table_name": "VISITS"},
        {"pipeline_run_id": "r1", "table_name": "CLAIMS"},
    ]
    monkeypatch.setattr(metastore_read, "read_table_rows", lambda t: list(all_rows))

    out = ib._scan_pylist("etl_run_log", row_filter=EqualTo("pipeline_run_id", "r1"))
    assert [r["table_name"] for r in out] == ["APPT", "CLAIMS"]


def test_scan_pylist_without_filter_scans_all(monkeypatch):
    """Without a filter, every Postgres row for the table is returned."""
    import metastore_read

    all_rows = [
        {"table_name": "APPT", "branch_id": "1"},
        {"table_name": "VISITS", "branch_id": "2"},
    ]
    monkeypatch.setattr(metastore_read, "read_table_rows", lambda t: list(all_rows))

    out = ib._scan_pylist("etl_control")
    assert out == all_rows


def test_read_run_detail_pushes_run_id_filter(monkeypatch):
    from pyiceberg.expressions import EqualTo

    calls = []

    def fake_scan(table, row_filter=None):
        calls.append((table, row_filter))
        return []

    monkeypatch.setattr(ib, "_scan_pylist", fake_scan)
    ib.read_run_detail("run-123")

    log = [c for c in calls if c[0] == "etl_run_log"]
    assert log, "etl_run_log must be scanned"
    assert isinstance(log[0][1], EqualTo), "run-id predicate must be pushed down"

    ctrl = [c for c in calls if c[0] == "etl_control"]
    assert ctrl and ctrl[0][1] is None, "etl_control has no per-run filter"
