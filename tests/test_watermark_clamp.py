"""A future-dated source row must not become the high-water mark.

The watermark feeds a ``>=``/``>`` predicate on the next run, so a mark past
every real row switches that branch of the incremental query off. Capture is a
plain max() and ``_wm_advance`` only moves forward, so the damage is permanent
until something clamps it.
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from etl import oracle_extract
from etl.oracle_extract import Watermark, _clamp_future_watermark, _column_max_watermark


def _wm(value: str) -> Watermark:
    return Watermark(value=value, kind="datetime")


def test_future_datetime_is_clamped_to_now():
    far = (dt.datetime.now() + dt.timedelta(days=1200)).strftime("%Y-%m-%d %H:%M:%S.%f")
    out = _clamp_future_watermark(_wm(far), "doc.DOC_DATE")
    assert out.kind == "datetime"
    assert dt.datetime.strptime(out.value, "%Y-%m-%d %H:%M:%S.%f") <= dt.datetime.now()


def test_past_datetime_is_left_alone():
    past = "2026-01-15 09:30:00.000000"
    assert _clamp_future_watermark(_wm(past), "doc.DOC_DATE").value == past


def test_clamp_is_a_no_op_for_absent_or_non_datetime_marks():
    assert _clamp_future_watermark(Watermark(value=None), "x").value is None
    num = Watermark(value="2461252", kind="number")
    assert _clamp_future_watermark(num, "appointments.JULIAN_DATE").value == "2461252"
    junk = _wm("not-a-timestamp")
    assert _clamp_future_watermark(junk, "x").value == "not-a-timestamp"


def test_one_bad_row_cannot_drag_the_mark_forward():
    # The regression in the wild: ORDERS_MASTER/khamis sat at 2526-04-12 because
    # a single row carried that date.
    good = dt.datetime.now() - dt.timedelta(days=3)
    tbl = pa.table({"d": pa.array([good, good, dt.datetime(2526, 4, 12)],
                                  pa.timestamp("us"))})
    raw = _column_max_watermark(tbl, "d")
    assert raw.value.startswith("2526-")            # capture itself is unbounded
    clamped = _clamp_future_watermark(raw, "orders_master.ORDER_DATE")
    assert dt.datetime.strptime(clamped.value, "%Y-%m-%d %H:%M:%S.%f") <= dt.datetime.now()


def test_clamped_mark_still_re_selects_the_future_row_next_run():
    # Clamping must not hide the row: with the mark at now, a row dated in the
    # future still satisfies `date >= watermark`, so it is re-read, not dropped.
    future = dt.datetime.now() + dt.timedelta(days=900)
    clamped = _clamp_future_watermark(
        _wm(future.strftime("%Y-%m-%d %H:%M:%S.%f")), "doc.DOC_DATE")
    mark = dt.datetime.strptime(clamped.value, "%Y-%m-%d %H:%M:%S.%f")
    assert future >= mark
