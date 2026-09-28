"""Repair of future-dated control_state watermarks (maintenance/repair_watermarks.py)."""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from etl.config import CATEGORY_MASTER, CATEGORY_TRANSACTION, HelperJoin, TableDef
from etl.watermark_repair import (
    FutureMark,
    _max_loaded_before,
    apply_marks,
    find_future_marks,
    repair_value,
)

NOW = dt.datetime(2026, 9, 28, 12, 0, 0)


def _master(name: str) -> TableDef:
    return TableDef(table=f"OASIS.{name.upper()}", unique_key="ID", cdc_column="AMEND_LAST_DATE",
                    where_date_column=None, where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _helper_txn(name: str) -> TableDef:
    return TableDef(table=f"OASIS.{name.upper()}", unique_key="ID", cdc_column=None,
                    where_date_column="DELIVERY_DATE", where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_TRANSACTION,
                    helper=HelperJoin(table="OASIS.PARENT", join_keys=(("P", "P"),),
                                      cdc_column="AMEND_LAST_DATE",
                                      where_date_column="AMEND_LAST_DATE"))


def _mark(value: str, kind: str = "datetime") -> dict:
    return {"value": value, "kind": kind}


CONTROL = {
    "staff_contracts": {
        "unaizah": {"last_cdc": _mark("2029-03-06 17:06:13.000000"), "last_date": None},
        "jazan": {"last_cdc": _mark("2026-09-27 15:47:54.000000"), "last_date": None},
    },
    "appointments": {"abha": {"last_cdc": None, "last_date": _mark("2461252.0", "number")}},
    "delivery_lines": {"khamis": {"last_cdc": None, "last_date": _mark("2027-01-01 00:21:05")}},
}
TDEFS = {"staff_contracts": _master("staff_contracts"),
         "delivery_lines": _helper_txn("delivery_lines")}


def test_only_future_datetime_marks_are_reported():
    marks = find_future_marks(CONTROL, TDEFS, NOW)
    assert [(m.table, m.branch, m.field) for m in marks] == [
        ("delivery_lines", "khamis", "last_date"),
        ("staff_contracts", "unaizah", "last_cdc"),
    ]


def test_a_mark_without_fractional_seconds_is_still_parsed():
    (dl,) = [m for m in find_future_marks(CONTROL, TDEFS, NOW) if m.table == "delivery_lines"]
    assert dl.value == "2027-01-01 00:21:05"


def test_plain_table_mark_tracks_its_own_column_helper_table_has_none():
    marks = {m.table: m for m in find_future_marks(CONTROL, TDEFS, NOW)}
    assert marks["staff_contracts"].column == "AMEND_LAST_DATE"
    # The helper's watermark alias is stripped before the Iceberg write, so the
    # lake cannot tell us the resume point: manual.
    assert marks["delivery_lines"].column is None


def test_repair_value_formats_the_lake_max_as_a_watermark():
    mark = FutureMark("staff_contracts", "unaizah", "last_cdc",
                      "2029-03-06 17:06:13.000000", "AMEND_LAST_DATE")
    got = repair_value(mark, lambda t, b, c: dt.datetime(2026, 8, 23, 7, 48, 46))
    assert got == "2026-08-23 07:48:46.000000"


def test_repair_value_is_none_when_it_cannot_be_derived():
    helper = FutureMark("delivery_lines", "khamis", "last_date", "2027-01-01 00:21:05", None)
    assert repair_value(helper, lambda t, b, c: dt.datetime(2026, 1, 1)) is None
    plain = FutureMark("staff_contracts", "unaizah", "last_cdc", "2029-...", "AMEND_LAST_DATE")
    assert repair_value(plain, lambda t, b, c: None) is None


LOADED = pa.timestamp("us")  # Recorded_updated_at: naive local load time


def test_max_loaded_before_keeps_wall_clock_and_skips_the_poisoned_row():
    # Lake stores the source wall clock tagged UTC; the poisoned 2029 row is in there too.
    vals = pa.chunked_array([pa.array(
        [dt.datetime(2026, 7, 21, 10, 46, 56), dt.datetime(2029, 3, 6, 17, 6, 13), None],
        pa.timestamp("us", tz="UTC"))])
    loaded = pa.array([dt.datetime(2026, 7, 21, 13, 28, 17)] * 3, LOADED)
    assert _max_loaded_before(vals, loaded) == dt.datetime(2026, 7, 21, 10, 46, 56)


def test_a_bad_date_that_has_since_passed_is_not_a_resume_point():
    # item_barcode/jazan: bad rows stamped 11:39:43 on assorted future dates;
    # by now some of those dates have passed, so "not after now" would pick
    # one -- but no real change can be stamped after the row was loaded.
    vals = pa.array([dt.datetime(2026, 7, 21, 11, 39, 43), dt.datetime(2026, 9, 28, 11, 39, 43),
                     dt.datetime(2029, 12, 27, 11, 39, 43)], pa.timestamp("us", tz="UTC"))
    loaded = pa.array([dt.datetime(2026, 7, 21, 13, 27, 3)] * 3, LOADED)
    assert _max_loaded_before(vals, loaded) == dt.datetime(2026, 7, 21, 11, 39, 43)


def test_max_loaded_before_handles_date_columns_and_empty_input():
    dates = pa.array([dt.date(2026, 9, 1), dt.date(2030, 1, 1)], pa.date32())
    loaded = pa.array([NOW, NOW], LOADED)
    assert _max_loaded_before(dates, loaded) == dt.datetime(2026, 9, 1)
    assert _max_loaded_before(pa.array([], pa.timestamp("us")), pa.array([], LOADED)) is None


class _FakeStore:
    def __init__(self, data):
        self.data = data
        self.saved = 0

    def save(self):
        self.saved += 1


def test_apply_marks_rewrites_only_the_named_field_and_saves_once():
    store = _FakeStore({"staff_contracts": {"unaizah": {
        "last_cdc": _mark("2029-03-06 17:06:13.000000"), "status": "SUCCESS"}}})
    apply_marks(store, [("staff_contracts", "unaizah", "last_cdc",
                         _mark("2026-08-23 07:48:46.000000"))])
    entry = store.data["staff_contracts"]["unaizah"]
    assert entry["last_cdc"] == _mark("2026-08-23 07:48:46.000000")
    assert entry["status"] == "SUCCESS"
    assert store.saved == 1


# --- insert-key seeding (Task 7) --------------------------------------------- #
from etl.watermark_repair import min_key_since, seed_key_mark  # noqa: E402


def test_min_key_since_uses_wall_clock_dates_and_ignores_nulls():
    keys = pa.array([1490400.0, 1492547.0, 1505159.0, None], pa.float64())
    dates = pa.array([dt.datetime(2026, 8, 31, 23, 0), dt.datetime(2026, 9, 2, 21, 30),
                      dt.datetime(2026, 9, 1, 19, 48), dt.datetime(2026, 9, 5)],
                     pa.timestamp("us", tz="UTC"))
    assert min_key_since(keys, dates, dt.datetime(2026, 9, 1)) == "1492547.0"


def test_min_key_since_none_when_nothing_qualifies():
    keys = pa.array([1.0], pa.float64())
    dates = pa.array([dt.datetime(2026, 1, 1)], pa.timestamp("us"))
    assert min_key_since(keys, dates, dt.datetime(2026, 9, 1)) is None


def test_seed_key_mark_is_a_number_watermark():
    assert seed_key_mark("1492547.0") == {"value": "1492547.0", "kind": "number"}
