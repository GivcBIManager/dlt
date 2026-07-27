"""_open_dest_table: catalog-backed destination reads, independent of any dlt
pipeline's local schema. Best-effort: absent table or broken catalog -> None."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef
from etl.oracle_extract import ExtractResult
from etl.progress import PipelineMonitor


def _cat(tmp_path):
    cat = SqlCatalog(
        "t", uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    return cat


def test_open_dest_table_loads_registered_table(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    cat.create_table("oasis.foo", schema=rows.schema).append(rows)
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    tbl = iceberg_load._open_dest_table(Settings(), "foo")

    assert tbl is not None
    assert {f.name for f in tbl.schema().fields} == {"id"}


def test_open_dest_table_none_when_absent(tmp_path, monkeypatch):
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog",
                        lambda: _cat(tmp_path))
    assert iceberg_load._open_dest_table(Settings(), "nope") is None


def test_open_dest_table_none_on_catalog_failure(monkeypatch):
    def boom():
        raise RuntimeError("catalog down")

    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", boom)
    assert iceberg_load._open_dest_table(Settings(), "foo") is None


def test_open_dest_table_asks_for_dataset_qualified_identifier(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    cat.create_table("oasis.bar", schema=rows.schema)
    asked = []
    real = cat.load_table

    def spy(ident):
        asked.append(ident)
        return real(ident)

    monkeypatch.setattr(cat, "load_table", spy)
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    assert iceberg_load._open_dest_table(Settings(), "bar") is not None
    assert asked == ["oasis.bar"]


def test_snapshot_ids_come_from_catalog_not_pipeline(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    tbl = cat.create_table("oasis.hist", schema=rows.schema)
    tbl.append(rows)
    tbl.append(rows)          # 2 snapshots of prior-run history
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    ids = iceberg_load._table_snapshot_ids(Settings(), "hist")

    assert len(ids) == 2      # a fresh per-table pipeline still sees history


def test_snapshot_ids_none_on_catalog_failure(monkeypatch):
    def boom():
        raise RuntimeError("catalog down")

    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", boom)

    assert iceberg_load._table_snapshot_ids(Settings(), "foo") is None


def test_snapshot_ids_empty_for_absent_table(tmp_path, monkeypatch):
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog",
                        lambda: _cat(tmp_path))

    assert iceberg_load._table_snapshot_ids(Settings(), "nope") == set()


class _FakeControl:
    """Records advance() calls; save() is a no-op (no Postgres needed)."""
    def advance(self, r):
        pass

    def save(self):
        pass


def test_unreadable_baseline_skips_squash(tmp_path, monkeypatch):
    # If the catalog can't answer the snapshot baseline before a load, the
    # post-load squash must not run at all -- squashing against an unknown
    # baseline (treated as empty) would expire every snapshot from prior
    # runs, mistaking them for this run's own. See _table_snapshot_ids.
    monkeypatch.setattr(iceberg_load, "_coerce_unified_nulls", lambda *a: a[-1])
    monkeypatch.setattr(iceberg_load, "_widen_schema_to_destination", lambda *a: a[-1])
    monkeypatch.setattr(iceberg_load, "_table_is_hash_ready", lambda *a, **k: False)
    monkeypatch.setattr(iceberg_load, "_existing_insert_at", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_table_snapshot_ids", lambda *a, **k: None)
    squash_calls = []
    monkeypatch.setattr(iceberg_load, "_squash_table_run_snapshots",
                        lambda *a, **k: squash_calls.append(a))

    tdef = TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)
    staged_dir = tmp_path / tdef.dataset_table_name
    staged_dir.mkdir(parents=True)
    p = staged_dir / "b1.parquet"
    pq.write_table(pa.table({"ID": pa.array([1, 2], pa.int64())}), p)
    result = ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                           status="SUCCESS", row_count=2, staged_path=p)
    monitor = PipelineMonitor(total_units=1, total_tables=1, enabled=False)

    # total_branches=2, branches_in_run=1 -> branch-subset INCREMENTAL -> merge.
    plan = iceberg_load._load_one_table(
        None, tdef, [result],
        Settings(mode=MODE_INCREMENTAL, snapshot_maintenance=True),
        _FakeControl(), 2, 1, monitor)

    assert plan.disposition == "merge"
    assert plan.load_status == "SUCCESS"
    assert squash_calls == []
