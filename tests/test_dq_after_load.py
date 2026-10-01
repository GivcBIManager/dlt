"""DQ compares the snapshot the last load saw: rows Oracle changed after it are
left out of both sides instead of reading as drift until the next run."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pyarrow as pa

from etl import config as cfg
from etl import dq_check

WM = {"value": "2026-10-01 06:00:00.000000", "kind": "datetime"}
BEFORE = dt.datetime(2026, 10, 1, 5, 0)
AFTER = dt.datetime(2026, 10, 1, 7, 0)


def _plain_tdef():
    return cfg.TableDef(
        table="OASIS.ORDER_LINES", unique_key="ORDER_LINE", cdc_column="AMEND_LAST_DATE",
        where_date_column="CREATION_DATE", where_operator=">=",
        where_value_of_initial_run="2022-01-01", category=cfg.CATEGORY_TRANSACTION)


def _helper_tdef(insert_key=None):
    return cfg.TableDef(
        table="OASIS.DELIVERY_CHARGE", unique_key="DELIVERY_CHARGE_ID", cdc_column=None,
        where_date_column="DELIVERY_DATE", where_operator=">=",
        where_value_of_initial_run="2022-01-01", category=cfg.CATEGORY_TRANSACTION,
        helper=cfg.HelperJoin(table="OASIS.DELIVERY_LINES", cdc_column="AMEND_LAST_DATE",
                              join_keys=(("DELIVERY_LINE", "DELIVERY_LINE"),),
                              where_date_column="AMEND_LAST_DATE"),
        insert_key_column=insert_key, insert_key_lookback=1000 if insert_key else 0)


def _mask(tdef, tbl, entry):
    return dq_check._after_load_mask(tbl, tdef, entry).to_pylist()


def test_cdc_stamp_after_the_watermark_is_after_load():
    tbl = pa.table({"ORDER_LINE": [1.0, 2.0, 3.0],
                    "AMEND_LAST_DATE": [BEFORE, AFTER, None]})
    # a NULL stamp can't be placed in time: it stays in the compare
    assert _mask(_plain_tdef(), tbl, {"last_cdc": WM}) == [False, True, False]


def test_no_watermark_excludes_nothing():
    tbl = pa.table({"ORDER_LINE": [1.0], "AMEND_LAST_DATE": [AFTER]})
    assert _mask(_plain_tdef(), tbl, {}) == [False]


def test_insert_key_above_last_key_is_after_load():
    tbl = pa.table({"DELIVERY_CHARGE_ID": [100.0, 101.0],
                    "DELIVERY_DATE": [BEFORE, BEFORE]})
    entry = {"last_cdc": WM, "last_key": {"value": 100, "kind": "number"}}
    assert _mask(_helper_tdef("DELIVERY_CHARGE_ID"), tbl, entry) == [False, True]


def test_helper_driven_own_date_after_the_helper_watermark_is_after_load():
    # the helper's CDC watermark is the edge of the load in time; the child's
    # own date column past it was never seen by that load
    tbl = pa.table({"DELIVERY_CHARGE_ID": [1.0, 2.0],
                    "DELIVERY_DATE": [BEFORE, AFTER]})
    assert _mask(_helper_tdef(), tbl, {"last_cdc": WM}) == [False, True]


def test_timezone_aware_column_compares_on_wall_clock():
    col = pa.array([BEFORE, AFTER], pa.timestamp("us", tz="UTC"))
    tbl = pa.table({"ORDER_LINE": [1.0, 2.0], "AMEND_LAST_DATE": col})
    assert _mask(_plain_tdef(), tbl, {"last_cdc": WM}) == [False, True]


# --------------------------------------------------------------------------- #
# check_unit end to end
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql):
        pass

    @property
    def description(self):
        return [SimpleNamespace(name=n) for n in self.conn.rows]

    def close(self):
        pass


class _FakeConn:
    def __init__(self, rows: dict):
        self.rows = rows

    def cursor(self):
        return _FakeCursor(self)

    def fetch_df_batches(self, query, size):
        yield self.rows


def _branch():
    return cfg.BranchConfig(key="b", name="B", id=1, host="h", port=1521,
                            username="u", password="p", database="d",
                            fetch_batch_size=100)


def test_check_unit_leaves_post_load_rows_out_of_both_sides(monkeypatch):
    created = dt.datetime(2026, 10, 1, 1, 0)
    # 1: unchanged; 2: edited after the load (lake holds the old version);
    # 3: inserted after the load (not in the lake yet)
    oracle = {"ORDER_LINE": [1.0, 2.0, 3.0],
              "CREATION_DATE": [created] * 3,
              "AMEND_LAST_DATE": [BEFORE, AFTER, AFTER],
              "STATUS": ["R", "D", "R"]}
    lake = pa.table({"order_line": [1.0, 2.0],
                     "creation_date": [created] * 2,
                     "amend_last_date": [BEFORE, BEFORE],
                     "status": ["R", "R"]})
    monkeypatch.setattr(dq_check, "_lake_scan_batches", lambda *a, **k: iter([lake]))
    static = SimpleNamespace(schema=lambda: SimpleNamespace(
        fields=[SimpleNamespace(name=n) for n in lake.column_names]))
    entry = {"last_cdc": WM, "last_date": WM}

    res = dq_check.check_unit(
        _plain_tdef(), _branch(), cfg.Settings(), static_table=static,
        control_entry=entry, since=dt.date(2026, 10, 1), until=None,
        do_hash=True, conn=_FakeConn(oracle))

    assert res.error is None
    assert res.rows_after_load == 2
    assert (res.oracle_row_count, res.iceberg_row_count) == (1, 1)
    assert (res.hash.only_in_oracle, res.hash.only_in_iceberg, res.hash.mismatch) == (0, 0, 0)
    assert res.status == dq_check.STATUS_OK


def test_result_rows_carry_the_post_load_count():
    res = dq_check.DqResult(table="t", source_table="T", branch="b", rows_after_load=7)
    row = dq_check._result_rows([res], cfg.Settings(), "run")[0]
    assert row["rows_after_load"] == 7


def test_metastore_has_the_post_load_column():
    from sqlalchemy import MetaData

    from etl.metastore import _etl_dq_results_table
    assert "rows_after_load" in _etl_dq_results_table(MetaData(), "s").columns
