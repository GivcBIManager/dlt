"""_scan_pylist reads the etl_* system tables from Postgres now.

The three observability tables (etl_control / etl_run_log / etl_dq_results) were
moved out of Iceberg into the Postgres app metastore. The row-filter predicate
(only ever ``EqualTo``, used by the run-id pushdown) is pushed all the way into
the SQL WHERE clause via ``metastore_read.read_table_rows`` -- ``_scan_pylist``
does no Python-side filtering of Postgres rows anymore. The two tests below run
against a real Postgres schema (the ``pg_meta`` fixture) so they prove the
filtering actually happens server-side, not just that a Python wrapper forwards
a dict unchanged.
"""
from __future__ import annotations

import iceberg_browser as ib


def test_scan_pylist_forwards_row_filter(pg_meta, monkeypatch):
    """With an EqualTo filter, only the matching Postgres rows come back."""
    from pyiceberg.expressions import EqualTo

    import metastore_read

    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    pg_meta.append_run_log([
        {"pipeline_run_id": "r1", "table_name": "APPT"},
        {"pipeline_run_id": "r2", "table_name": "VISITS"},
        {"pipeline_run_id": "r1", "table_name": "CLAIMS"},
    ])

    out = ib._scan_pylist("etl_run_log", row_filter=EqualTo("pipeline_run_id", "r1"))
    assert len(out) == 2  # only the r1 rows -- not all 3 seeded rows
    assert {r["table_name"] for r in out} == {"APPT", "CLAIMS"}
    assert all(r["pipeline_run_id"] == "r1" for r in out)


def test_scan_pylist_without_filter_scans_all(pg_meta, monkeypatch):
    """Without a filter, every Postgres row for the table is returned."""
    import metastore_read

    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    pg_meta.append_run_log([
        {"pipeline_run_id": "r1", "table_name": "APPT"},
        {"pipeline_run_id": "r2", "table_name": "VISITS"},
    ])

    out = ib._scan_pylist("etl_run_log")
    assert len(out) == 2
    assert {r["table_name"] for r in out} == {"APPT", "VISITS"}


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
