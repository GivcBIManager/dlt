"""ROWID columns must not break the native Arrow fetch (DPY-3030).

python-oracledb's dataframe fetch (``fetch_df_batches``) has no Arrow
conversion for ``DB_TYPE_ROWID``/``DB_TYPE_UROWID`` and raises DPY-3030 --
e.g. OASIS.STAFF_VACATIONS.OFFICIAL_ROWID, on every branch. The pipeline must
rewrite the query to cast such columns server-side (``ROWIDTOCHAR``) and retry
the fast path, keeping the cursor stream as the last resort. The DQ checker's
source reader must survive the same tables instead of hard-failing the check.
"""
from __future__ import annotations

import datetime as dt

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from etl import config as cfg
from etl import dq_check as dq
from etl import oracle_extract as ox

NOW = dt.datetime(2026, 7, 15, 12, 0, 0)

DPY_3030 = ("DPY-3030: conversion from Oracle Database type DB_TYPE_ROWID "
            "to Apache Arrow format is not supported")


def _tdef():
    return cfg.TableDef(
        table="OASIS.STAFF_VACATIONS", unique_key="STAFF_VACATION_ID",
        cdc_column=None, where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=cfg.CATEGORY_MASTER)


class _FetchInfo:
    def __init__(self, name, type_code):
        self.name = name
        self.type_code = type_code


_DESCRIPTION = [
    _FetchInfo("STAFF_VACATION_ID", oracledb.DB_TYPE_NUMBER),
    _FetchInfo("OFFICIAL_ROWID", oracledb.DB_TYPE_ROWID),
]


class _PeekCursor:
    """Cursor stub for the ROWNUM<1 description peek."""

    arraysize = 0
    prefetchrows = 0

    def __init__(self, description, fail_peek=False):
        self.description = description
        self._fail_peek = fail_peek

    def execute(self, q):
        self.q = q
        if self._fail_peek and "ROWNUM" in q.upper():
            raise Exception("ORA-00942: table or view does not exist")

    def close(self):
        pass


class _RowidConn:
    """fetch_df_batches raises DPY-3030 until the query casts the ROWID column."""

    def __init__(self, batches, fail_peek=False):
        self._batches = batches
        self._fail_peek = fail_peek
        self.df_queries = []

    def fetch_df_batches(self, query, size):
        self.df_queries.append(query)
        if "ROWIDTOCHAR" not in query.upper():
            raise Exception(DPY_3030)
        return iter(self._batches)

    def cursor(self):
        return _PeekCursor(_DESCRIPTION, fail_peek=self._fail_peek)


def _batch():
    return pa.table({
        "STAFF_VACATION_ID": pa.array([1, 2], pa.int64()),
        "OFFICIAL_ROWID": pa.array(["AAAT5uAAOAAAgu7AAC", "AAAT5uAAOAAAgu7AAD"],
                                   pa.string()),
    })


# --------------------------------------------------------------------------- #
# arrow_safe_rewrite
# --------------------------------------------------------------------------- #
def test_arrow_safe_rewrite_casts_rowid_columns():
    query = "SELECT t.* FROM OASIS.STAFF_VACATIONS t"
    rewritten = ox.arrow_safe_rewrite(_RowidConn([]), query)
    assert rewritten is not None
    assert 'ROWIDTOCHAR("OFFICIAL_ROWID") AS "OFFICIAL_ROWID"' in rewritten
    assert '"STAFF_VACATION_ID"' in rewritten
    assert f"FROM ({query})" in rewritten


def test_arrow_safe_rewrite_returns_none_without_rowid():
    class _PlainConn:
        def cursor(self):
            return _PeekCursor([_FetchInfo("ID", oracledb.DB_TYPE_NUMBER)])

    assert ox.arrow_safe_rewrite(_PlainConn(), "SELECT * FROM T") is None


# --------------------------------------------------------------------------- #
# extract: fetch_and_stage
# --------------------------------------------------------------------------- #
def test_fetch_and_stage_retries_arrow_with_rowidtochar(tmp_path):
    settings = cfg.Settings(staging_dir=tmp_path)
    conn = _RowidConn([_batch()])

    rc, schema, path = ox.fetch_and_stage(
        conn, "SELECT t.* FROM OASIS.STAFF_VACATIONS t", _tdef(), "b1", 7,
        settings, 100, NOW)

    assert rc == 2
    assert len(conn.df_queries) == 2  # original (DPY-3030) then rewritten
    assert "ROWIDTOCHAR" in conn.df_queries[1]
    tbl = pq.read_table(path)
    assert tbl.column("OFFICIAL_ROWID").to_pylist() == [
        "AAAT5uAAOAAAgu7AAC", "AAAT5uAAOAAAgu7AAD"]


def test_fetch_and_stage_cursor_fallback_when_peek_fails(monkeypatch, tmp_path):
    settings = cfg.Settings(staging_dir=tmp_path)
    conn = _RowidConn([_batch()], fail_peek=True)
    monkeypatch.setattr(ox, "_cursor_arrow_stream", lambda cur, size: iter([_batch()]))

    rc, schema, path = ox.fetch_and_stage(
        conn, "SELECT t.* FROM OASIS.STAFF_VACATIONS t", _tdef(), "b1", 7,
        settings, 100, NOW)

    assert rc == 2
    assert len(conn.df_queries) == 1  # rewrite unavailable -> no arrow retry
    assert pq.read_table(path).num_rows == 2


# --------------------------------------------------------------------------- #
# dq_check: _oracle_batches
# --------------------------------------------------------------------------- #
def test_dq_oracle_batches_retries_arrow_with_rowidtochar():
    conn = _RowidConn([_batch()])
    out = list(dq._oracle_batches(conn, "SELECT * FROM OASIS.STAFF_VACATIONS", 100))
    assert sum(t.num_rows for t in out) == 2
    assert "ROWIDTOCHAR" in conn.df_queries[-1]


def test_dq_oracle_batches_cursor_fallback_when_peek_fails(monkeypatch):
    conn = _RowidConn([_batch()], fail_peek=True)
    monkeypatch.setattr(ox, "_cursor_arrow_stream", lambda cur, size: iter([_batch()]))
    out = list(dq._oracle_batches(conn, "SELECT * FROM OASIS.STAFF_VACATIONS", 100))
    assert sum(t.num_rows for t in out) == 2
