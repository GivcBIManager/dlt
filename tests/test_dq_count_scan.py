"""check_unit must not issue a separate COUNT(*) when it also pulls rows to hash.

The windowed hash SELECT returns exactly the windowed COUNT(*) row set, so the
row count is taken from the rows actually pulled instead of a second full scan.
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from etl import config as cfg
from etl import dq_check


class _FakeDesc:
    def __init__(self, name):
        self.name = name


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._one = None

    def execute(self, sql):
        self.conn.executed.append(sql)
        if "COUNT(*)" in sql:
            self._one = (self.conn.count,)

    @property
    def description(self):
        return [_FakeDesc(n) for n in self.conn.columns]

    def fetchone(self):
        return self._one

    def close(self):
        pass


class _FakeConn:
    """Minimal stand-in for an oracledb connection recording executed SQL."""

    def __init__(self, columns, rows):
        self.columns = columns
        self.rows = rows
        self.count = len(rows)
        self.executed: list[str] = []

    def cursor(self):
        return _FakeCursor(self)

    def fetch_df_batches(self, query, size):
        self.executed.append(query)
        yield {c: [r[i] for r in self.rows] for i, c in enumerate(self.columns)}


def _tdef():
    return cfg.TableDef(
        table="OASIS.T", unique_key="ID", cdc_column=None, where_date_column=None,
        where_operator=None, where_value_of_initial_run=None,
        category=cfg.CATEGORY_MASTER)


def _branch():
    return cfg.BranchConfig(key="b", name="B", id=1, host="h", port=1521,
                            username="u", password="p", database="d",
                            fetch_batch_size=100)


def _run(do_hash, conn):
    return dq_check.check_unit(
        _tdef(), _branch(), cfg.Settings(), static_table=None, control_entry={},
        since=dt.date(2020, 1, 1), until=None, do_hash=do_hash, conn=conn)


def test_hash_path_skips_count_and_uses_pulled_rows():
    conn = _FakeConn(["ID", "NAME"], [(1, "a"), (2, "b"), (3, "c")])
    res = _run(do_hash=True, conn=conn)
    assert res.oracle_row_count == 3
    assert not any("COUNT(*)" in sql for sql in conn.executed), \
        "hash path must not issue a separate COUNT(*) scan"


def test_no_hash_path_still_counts():
    conn = _FakeConn(["ID", "NAME"], [(1, "a"), (2, "b")])
    res = _run(do_hash=False, conn=conn)
    assert res.oracle_row_count == 2
    assert any("COUNT(*)" in sql for sql in conn.executed)
