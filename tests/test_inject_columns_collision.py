"""Source tables that already carry an ETL-injected column name must not
produce duplicate fields.

OASIS.STAFF_POSTS has its own BRANCH_ID; inject_columns used to append the
pipeline's BRANCH_ID on top, and the staged parquet then had the field twice.
Any later lookup by name (cast_table_to_schema's table.column("BRANCH_ID"))
raised 'Field "BRANCH_ID" exists 2 times in schema' and failed the extract.

The source column is renamed with a SRC_ prefix so its data survives and the
injected column keeps the reserved name (it is the partition/merge key).
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from etl.config import CATEGORY_MASTER, CATEGORY_SNAPSHOT, Settings, TableDef
from etl import oracle_extract

NOW = dt.datetime(2026, 7, 6, 12, 0, 0)


def _tdef(category=CATEGORY_MASTER):
    return TableDef(
        table="OASIS.STAFF_POSTS", unique_key="POST_NUMBER", cdc_column=None,
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=category)


def test_source_branch_id_is_renamed_not_duplicated():
    settings = Settings()
    base = pa.table({
        "POST_NUMBER": pa.array([1, 2], pa.int64()),
        "BRANCH_ID": pa.array([501, 502], pa.int64()),  # the table's own column
    })

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    # No duplicate field names anywhere in the result.
    assert len(out.column_names) == len(set(out.column_names))
    # The reserved name holds the injected branch id (lookup must not raise).
    assert out.column(settings.branch_id_column).to_pylist() == [7, 7]
    # The source data survives under the SRC_ prefix.
    assert out.column("SRC_BRANCH_ID").to_pylist() == [501, 502]


def test_collision_detection_is_case_insensitive():
    # Oracle names arrive uppercase; dlt normalizes case-insensitively, so
    # INSERT_AT would collide with the injected insert_at at normalize time.
    settings = Settings()
    base = pa.table({
        "POST_NUMBER": pa.array([1], pa.int64()),
        "INSERT_AT": pa.array([dt.datetime(2020, 1, 1)], pa.timestamp("us")),
    })

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    assert "SRC_INSERT_AT" in out.column_names
    assert "INSERT_AT" not in out.column_names
    assert out.column(settings.inserted_ts_column).to_pylist() == [NOW]


def test_snapshot_version_columns_also_guarded():
    settings = Settings(snapshot_ts=dt.datetime(2026, 7, 6, 10, 0, 0))
    base = pa.table({
        "POST_NUMBER": pa.array([1], pa.int64()),
        "VERSION": pa.array(["v9"], pa.string()),  # source's own VERSION
    })

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(CATEGORY_SNAPSHOT), now=NOW)

    assert len(out.column_names) == len(set(out.column_names))
    assert out.column("SRC_VERSION").to_pylist() == ["v9"]
    assert out.column(settings.snapshot_version_column).to_pylist() == [
        settings.snapshot_ts]


def test_no_collision_leaves_table_unchanged():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array([1, 2], pa.int64())})

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    assert out.column("POST_NUMBER").to_pylist() == [1, 2]
    assert not [c for c in out.column_names if c.startswith("SRC_")]
