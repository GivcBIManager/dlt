"""read_run_detail should push the run-id predicate into the Iceberg scan."""
from __future__ import annotations

import pyarrow as pa

import iceberg_browser as ib


def test_scan_pylist_forwards_row_filter(monkeypatch):
    seen = {}

    class FakeScan:
        def to_arrow(self):
            return pa.table({"pipeline_run_id": ["r1"]})

    class FakeTable:
        def scan(self, row_filter=None, **kw):
            seen["rf"] = row_filter
            return FakeScan()

    monkeypatch.setattr(ib, "_open_static", lambda t: FakeTable())
    ib._scan_pylist("etl_run_log", row_filter="SENTINEL")
    assert seen["rf"] == "SENTINEL"


def test_scan_pylist_without_filter_scans_all(monkeypatch):
    seen = {"called": False}

    class FakeScan:
        def to_arrow(self):
            return pa.table({"x": [1]})

    class FakeTable:
        def scan(self, row_filter=None, **kw):
            seen["called"] = True
            seen["rf"] = row_filter
            return FakeScan()

    monkeypatch.setattr(ib, "_open_static", lambda t: FakeTable())
    ib._scan_pylist("etl_control")
    assert seen["called"] and seen["rf"] is None


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
