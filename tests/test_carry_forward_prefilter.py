"""Carry-forward prefilter: `existing` shrinks to the rows the delta can touch."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef
from etl.iceberg_load import _merge_hash_array, _staged_delta_hashes
from etl.oracle_extract import ExtractResult
from etl.progress import PipelineMonitor


def _tdef():
    return TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _write_staged(dir_, name, ids, branches):
    p = dir_ / name
    pq.write_table(pa.table({"ID": pa.array(ids, pa.int64()),
                             "BRANCH_ID": pa.array(branches, pa.int64()),
                             "VAL": pa.array([f"v{i}" for i in ids])}), p)
    return p


def _unified_schema():
    return pa.schema([pa.field("ID", pa.int64()),
                      pa.field("BRANCH_ID", pa.int64()),
                      pa.field("VAL", pa.string())])


def _hashes_of(ids, branches):
    keys = pa.table({"ID": pa.array(ids, pa.int64()),
                     "BRANCH_ID": pa.array(branches, pa.int64())})
    return _merge_hash_array(keys, ["ID", "BRANCH_ID"])


def test_delta_hashes_match_finish_batch_hashes(tmp_path):
    p = _write_staged(tmp_path, "b1.parquet", [1, 2, 2], [7, 7, 7])
    got = _staged_delta_hashes([p], ["ID", "BRANCH_ID"], _unified_schema())
    want = {v.as_py() for v in _hashes_of([1, 2], [7, 7])}
    assert {v.as_py() for v in got} == want       # deduped, digest-identical


def test_delta_hashes_none_on_unreadable_file(tmp_path):
    got = _staged_delta_hashes([tmp_path / "gone.parquet"],
                               ["ID", "BRANCH_ID"], _unified_schema())
    assert got is None                            # best-effort: no prefilter


def test_merge_shrinks_existing_to_delta_rows(tmp_path, monkeypatch):
    tdef = _tdef()
    staged_dir = tmp_path / tdef.dataset_table_name
    staged_dir.mkdir(parents=True)
    p = _write_staged(staged_dir, "b1.parquet", [1, 2], [7, 7])
    result = ExtractResult(table_def=tdef, branch="b1", branch_id=7,
                           status="SUCCESS", row_count=2, staged_path=p)

    # Stored carry-forward rows for ids 1..4; only 1 and 2 are in the delta.
    existing = pa.table({
        "merge_hash": _hashes_of([1, 2, 3, 4], [7, 7, 7, 7]),
        "insert_at": pa.array([None] * 4, pa.timestamp("us")),
    })

    captured = {}

    def fake_resource(plan, s, paths, disposition, existing_insert_at=None,
                      write_hash=False, carry_keys=None):
        captured["existing"] = existing_insert_at
        return None

    monkeypatch.setattr(iceberg_load, "_coerce_unified_nulls", lambda p_, t, s: s)
    monkeypatch.setattr(iceberg_load, "_widen_schema_to_destination",
                        lambda p_, t, s: s)
    monkeypatch.setattr(iceberg_load, "_table_is_hash_ready", lambda *a, **k: True)
    monkeypatch.setattr(iceberg_load, "_existing_insert_at",
                        lambda *a, **k: existing)
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", fake_resource)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)

    class _FakeControl:
        def advance(self, r):
            pass

        def save(self):
            pass

    monitor = PipelineMonitor(total_units=1, total_tables=1, enabled=False)
    # total_branches=2, branches_in_run=1 -> branch-subset -> merge disposition.
    plan = iceberg_load._load_one_table(
        None, tdef, [result],
        Settings(mode=MODE_INCREMENTAL, snapshot_maintenance=False),
        _FakeControl(), 2, 1, monitor)

    assert plan.disposition == "merge"
    assert plan.load_status == "SUCCESS"
    kept = captured["existing"]
    assert kept.num_rows == 2                     # ids 3,4 filtered out
    want = {v.as_py() for v in _hashes_of([1, 2], [7, 7])}
    assert {v.as_py() for v in kept.column("merge_hash")} == want
