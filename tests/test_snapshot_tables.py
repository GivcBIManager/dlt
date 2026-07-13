"""Tests for the append-only 'snapshots' table category.

Snapshot tables are pulled in full on every run and appended (never merged /
replaced), and every record of a run -- across all branches -- is stamped with a
single shared ``version`` timestamp plus a derived ``version_date`` partition.
"""
from __future__ import annotations

import datetime as dt
import json

import pyarrow as pa
import pyarrow.parquet as pq

from etl import config, iceberg_load, oracle_extract
from etl.config import (
    CATEGORY_MASTER,
    CATEGORY_SNAPSHOT,
    MODE_INCREMENTAL,
    MODE_INITIAL,
    Settings,
    TableDef,
)
from etl.oracle_extract import ExtractResult, Watermark


def _tdef(category: str = CATEGORY_SNAPSHOT, cdc_column="AMEND_LAST_DATE") -> TableDef:
    return TableDef(
        table="OASIS.SNAP_FOO",
        unique_key="",
        cdc_column=cdc_column,
        where_date_column=None,
        where_operator=None,
        where_value_of_initial_run=None,
        category=category,
    )


def test_is_snapshot_property():
    assert _tdef(CATEGORY_SNAPSHOT).is_snapshot is True
    assert _tdef(CATEGORY_MASTER).is_snapshot is False


def test_build_query_snapshot_is_full_copy_ignoring_watermark():
    tdef = _tdef()
    settings = Settings(mode=MODE_INCREMENTAL)
    cdc_wm = Watermark(value="2026-07-01 00:00:00.000000", kind="datetime")
    date_wm = Watermark(value=None)
    # Even with a stored watermark in INCREMENTAL, a snapshot table is a full copy.
    assert oracle_extract.build_query(tdef, settings, cdc_wm, date_wm) == \
        "SELECT * FROM OASIS.SNAP_FOO"


def test_inject_columns_stamps_shared_version_across_branches():
    version = dt.datetime(2026, 7, 6, 10, 30, 0)
    settings = Settings(snapshot_ts=version)
    tdef = _tdef()

    base = pa.table({"ID": pa.array([1, 2, 3], pa.int64())})
    t1 = oracle_extract.inject_columns(base, branch_id=11, settings=settings, tdef=tdef)
    t2 = oracle_extract.inject_columns(base, branch_id=22, settings=settings, tdef=tdef)

    vcol = settings.snapshot_version_column
    dcol = settings.snapshot_date_column
    assert vcol in t1.column_names and dcol in t1.column_names
    # Every record (and every branch) shares the exact same version timestamp.
    assert set(t1.column(vcol).to_pylist()) == {version}
    assert t1.column(vcol).to_pylist() == t2.column(vcol).to_pylist()
    assert set(t1.column(dcol).to_pylist()) == {version.date()}


def test_inject_columns_constant_columns_values_and_types():
    now = dt.datetime(2026, 7, 6, 12, 0, 0)
    settings = Settings()
    tdef = _tdef(CATEGORY_MASTER)
    base = pa.table({"ID": pa.array([1, 2, 3], pa.int64())})
    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=tdef, now=now)

    bid = out.column(settings.branch_id_column)
    assert bid.type == pa.int64()
    assert bid.to_pylist() == [7, 7, 7]

    ins = out.column(settings.inserted_ts_column)
    rec = out.column(settings.recorded_ts_column)
    assert ins.type == pa.timestamp("us") and rec.type == pa.timestamp("us")
    assert ins.to_pylist() == [now, now, now]
    assert rec.to_pylist() == [now, now, now]


def test_inject_columns_no_version_for_non_snapshot():
    settings = Settings(snapshot_ts=dt.datetime(2026, 7, 6, 10, 0, 0))
    tdef = _tdef(CATEGORY_MASTER)
    base = pa.table({"ID": pa.array([1], pa.int64())})
    out = oracle_extract.inject_columns(base, branch_id=1, settings=settings, tdef=tdef)
    assert settings.snapshot_version_column not in out.column_names
    assert settings.snapshot_date_column not in out.column_names


def test_plan_table_snapshot_is_append(tmp_path):
    settings = Settings(mode=MODE_INITIAL, snapshot_ts=dt.datetime(2026, 7, 6, 9, 0, 0))
    tdef = _tdef()

    # Stage a tiny parquet the planner can read the schema from.
    staged = tmp_path / "b1.parquet"
    table = oracle_extract.inject_columns(
        pa.table({"ID": pa.array([1, 2], pa.int64())}),
        branch_id=1, settings=settings, tdef=tdef)
    pq.write_table(table, staged)

    r = ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                      status="SUCCESS", row_count=2, staged_path=staged)
    # Even a full-coverage INITIAL stays append for snapshot tables.
    plan = iceberg_load._plan_table(
        tdef, [r], settings, total_branches=1, branches_in_run=1)
    assert plan.disposition == "append"


def test_plan_table_master_initial_full_is_replace(tmp_path):
    settings = Settings(mode=MODE_INITIAL)
    tdef = _tdef(CATEGORY_MASTER)
    staged = tmp_path / "b1.parquet"
    table = oracle_extract.inject_columns(
        pa.table({"ID": pa.array([1, 2], pa.int64())}),
        branch_id=1, settings=settings, tdef=tdef)
    pq.write_table(table, staged)
    r = ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                      status="SUCCESS", row_count=2, staged_path=staged)
    plan = iceberg_load._plan_table(
        tdef, [r], settings, total_branches=1, branches_in_run=1)
    assert plan.disposition == "replace"


def test_load_table_defs_parses_snapshots(tmp_path):
    doc = {
        "masters": [{"table": "OASIS.M", "unique_key": "ID"}],
        "transactions": [],
        "snapshots": [{"table": "OASIS.SNAP_A"}, {"table": "OASIS.SNAP_B"}],
    }
    p = tmp_path / "tables.json"
    p.write_text(json.dumps(doc), encoding="utf-8")
    defs = config.load_table_defs(p)
    snaps = [d for d in defs if d.is_snapshot]
    assert {d.table for d in snaps} == {"OASIS.SNAP_A", "OASIS.SNAP_B"}
