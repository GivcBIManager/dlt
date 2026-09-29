"""fact_delivery's incremental cursor must see changes on ALL joined tables.

The merge no longer re-stamps a re-read row whose content is unchanged
(recorded_updated_at is the ETL stamp, not content). delivery_charge rows are
re-extracted when their DELIVERY_LINES parent changes, but their own columns stay
the same -- so dc.recorded_updated_at no longer moves when only the line (or the
master delivery) changed. fact_delivery takes line/master columns too
(unit_price, product_code, batch_no, amend_last_date, md.*), so its cursor and its
version stamp must be the latest stamp of dc, dl and md, or those edits never
reach the fact.
"""
from __future__ import annotations

import re
from pathlib import Path

SQL = (Path(__file__).resolve().parents[1] / "dbt" / "models" / "intermediate"
       / "fact_delivery.sql").read_text(encoding="utf-8")


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", s).lower()


STAMP = _norm(
    "greatest(ifNull(dc.recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)),"
    " ifNull(dl.recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)),"
    " ifNull(md.recorded_updated_at, toDateTime64('1970-01-01 00:00:00', 6)))")


def test_output_stamp_is_the_latest_of_all_three_sources():
    assert _norm(SQL).count(STAMP + "recorded_updated_at") == 1   # "<expr> recorded_updated_at"


def test_incremental_cursor_uses_the_same_combined_stamp():
    where = _norm(SQL.split("{% if is_incremental() %}", 1)[1])
    assert "where" + STAMP + ">todatetime64(" in where


def test_cursor_no_longer_filters_on_the_charge_stamp_alone():
    assert "wheredc.recorded_updated_at>" not in _norm(SQL)
