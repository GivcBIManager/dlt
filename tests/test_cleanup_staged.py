"""Best-effort deletion of a branch's staged parquet after it is committed."""
from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef
from etl.oracle_extract import ExtractResult
from etl.progress import PipelineMonitor


def _result(path):
    # _cleanup_staged is duck-typed on .staged_path and .table.
    return SimpleNamespace(staged_path=path, table="FOO")


def _staged(dir_, branch="b1"):
    tbl_dir = dir_ / "FOO"
    tbl_dir.mkdir(parents=True, exist_ok=True)
    p = tbl_dir / f"{branch}.parquet"
    p.write_bytes(b"parquet")
    return p


def test_deletes_file_when_enabled(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings())
    assert not p.exists()


def test_removes_empty_table_dir(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings())
    assert not p.parent.exists()


def test_keeps_dir_with_other_branch(tmp_path):
    p1 = _staged(tmp_path, "b1")
    p2 = _staged(tmp_path, "b2")
    iceberg_load._cleanup_staged(_result(p1), Settings())
    assert not p1.exists()
    assert p2.exists()            # sibling untouched
    assert p2.parent.exists()     # dir kept — still has b2


def test_noop_when_disabled(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings(cleanup_staging_after_load=False))
    assert p.exists()


def test_tolerates_missing_file(tmp_path):
    p = tmp_path / "FOO" / "gone.parquet"   # never created
    iceberg_load._cleanup_staged(_result(p), Settings())  # must not raise


def test_noop_when_path_none():
    iceberg_load._cleanup_staged(_result(None), Settings())  # must not raise


class _FakeControl:
    """Records advance() calls; save() is a no-op (no Postgres needed)."""
    def __init__(self):
        self.advanced = []

    def advance(self, r):
        self.advanced.append(r)

    def save(self):
        pass


def _merge_tdef():
    return TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _staged_parquet(base, tdef, branch, rows=2):
    d = base / tdef.dataset_table_name
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{branch}.parquet"
    pq.write_table(pa.table({"ID": pa.array(list(range(rows)), pa.int64())}), p)
    return p


def _extract_result(tdef, branch, branch_id, staged, rows=2):
    # Named distinctly from the module-level _result(path) helper above
    # (used by the 6 pre-existing _cleanup_staged tests) to avoid shadowing it.
    return ExtractResult(table_def=tdef, branch=branch, branch_id=branch_id,
                         status="SUCCESS", row_count=rows, staged_path=staged)


def test_per_branch_rebuild_deletes_staged(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _extract_result(tdef, "b1", 1, staged)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=[result], failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_rebuild(None, plan, Settings(), control)

    assert control.advanced == [result]
    assert not staged.exists()


def test_per_branch_rebuild_keeps_staged_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _extract_result(tdef, "b1", 1, staged)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=[result], failed=[])

    iceberg_load._run_per_branch_rebuild(
        None, plan, Settings(cleanup_staging_after_load=False), _FakeControl())

    assert staged.exists()   # retained for dq_check --self-test


def test_per_branch_append_deletes_staged(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _extract_result(tdef, "b1", 1, staged)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=[result], failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_append(None, plan, Settings(), control)

    assert control.advanced == [result]
    assert not staged.exists()


def test_load_one_table_zero_row_deletes_staged(tmp_path):
    # 0-row load: early-return SUCCESS path, no dlt run, no pipeline touched.
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1", rows=0)
    result = _extract_result(tdef, "b1", 1, staged, rows=0)
    monitor = PipelineMonitor(total_units=1, total_tables=1, enabled=False)

    plan = iceberg_load._load_one_table(
        None, tdef, [result], Settings(mode=MODE_INCREMENTAL),
        _FakeControl(), 1, 1, monitor)

    assert plan.load_status == "SUCCESS"
    assert not staged.exists()


def test_load_one_table_merge_deletes_staged(tmp_path, monkeypatch):
    # Merge branch: stub the dlt run + destination reads so no real commit
    # happens, then assert the advance-loop cleanup deleted the parquet.
    monkeypatch.setattr(iceberg_load, "_coerce_unified_nulls", lambda p, t, s: s)
    monkeypatch.setattr(iceberg_load, "_widen_schema_to_destination", lambda p, t, s: s)
    monkeypatch.setattr(iceberg_load, "_table_is_hash_ready", lambda *a, **k: False)
    monkeypatch.setattr(iceberg_load, "_existing_insert_at", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _extract_result(tdef, "b1", 1, staged)
    monitor = PipelineMonitor(total_units=1, total_tables=1, enabled=False)

    # total_branches=2, branches_in_run=1 -> branch-subset INCREMENTAL -> merge.
    plan = iceberg_load._load_one_table(
        None, tdef, [result],
        Settings(mode=MODE_INCREMENTAL, snapshot_maintenance=False),
        _FakeControl(), 2, 1, monitor)

    assert plan.disposition == "merge"
    assert plan.load_status == "SUCCESS"
    assert not staged.exists()
