"""A stored future-dated watermark must not outrank a fresh, real capture.

Captures are clamped to now (``_clamp_future_watermark``), but marks stored
before that clamp existed stay in control_state, and ``_wm_advance`` used to
keep the greater value -- so a 2029 mark could never be replaced. Any stored
datetime mark later than now is a bad source row by construction.
"""
from __future__ import annotations

import datetime as dt

from etl.iceberg_load import _is_future_mark, _wm_advance
from etl.oracle_extract import Watermark

POISON = {"value": "2029-03-06 17:06:13.000000", "kind": "datetime"}


def _ago(days: float) -> str:
    return (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S.%f")


def test_future_stored_mark_yields_to_a_fresh_capture():
    new = Watermark(value=_ago(1), kind="datetime")
    assert _wm_advance(dict(POISON), new) == new.to_dict()


def test_future_stored_mark_is_kept_when_the_run_captured_nothing():
    # Nothing to replace it with: the repair tool (maintenance/repair_watermarks.py) owns this case.
    assert _wm_advance(dict(POISON), Watermark(value=None)) == POISON


def test_past_stored_mark_still_only_moves_forward():
    old = {"value": _ago(1), "kind": "datetime"}
    assert _wm_advance(old, Watermark(value=_ago(5), kind="datetime")) == old


def test_number_marks_are_never_treated_as_future():
    # APPOINTMENTS.JULIAN_DATE-style marks are numbers, not datetimes.
    old = {"value": "2461252.0", "kind": "number"}
    assert not _is_future_mark(old)
    assert _wm_advance(old, Watermark(value="2461200.0", kind="number")) == old


def test_unparseable_or_absent_marks_are_not_future():
    assert not _is_future_mark(None)
    assert not _is_future_mark({"value": None, "kind": "datetime"})
    assert not _is_future_mark({"value": "garbage", "kind": "datetime"})
