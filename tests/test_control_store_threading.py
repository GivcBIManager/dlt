"""ControlStore.advance/save must be safe under concurrent load workers."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from etl.iceberg_load import ControlStore
from etl.oracle_extract import Watermark


class _FakeStore:
    def __init__(self):
        self.saved_rows = []

    def upsert_control_state(self, rows):
        self.saved_rows.append(rows)


class _Result:
    """Duck-typed ExtractResult: only what advance() touches."""

    def __init__(self, table, branch):
        self.table, self.branch = table, branch
        self.new_cdc = Watermark(value=None)
        self.new_date = Watermark(value=None)
        self.status = "SUCCESS"
        self.row_count = 1
        self.duration_ms = 1


def test_concurrent_advance_and_save_lose_nothing():
    control = ControlStore(_FakeStore())

    def work(i):
        for j in range(50):
            control.advance(_Result(f"t{i}_{j}", f"b{j % 3}"))
            control.save()     # iterates the dict others are mutating

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(work, range(8)))   # re-raises any worker exception

    assert len(control.data) == 8 * 50
    final_tables = {r["table_name"] for r in control.store.saved_rows[-1]}
    assert len(final_tables) == 8 * 50
