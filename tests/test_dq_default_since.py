"""The DQ window's default lower bound is the 1st of the current month."""
from __future__ import annotations

import datetime as dt

import pytest

import dq_check


@pytest.mark.parametrize("today,expected", [
    (dt.date(2026, 9, 3), dt.date(2026, 9, 1)),
    (dt.date(2026, 9, 1), dt.date(2026, 9, 1)),   # already the 1st
    (dt.date(2026, 1, 31), dt.date(2026, 1, 1)),  # January is not special
    (dt.date(2024, 2, 29), dt.date(2024, 2, 1)),  # leap day
])
def test_default_since_is_start_of_month(today, expected):
    assert dq_check._default_since(None, today) == expected


def test_year_flag_still_selects_that_years_jan_1():
    assert dq_check._default_since(2025, dt.date(2026, 9, 3)) == dt.date(2025, 1, 1)


def test_explicit_since_beats_the_default():
    args = dq_check.parse_args(["--since", "2026-06-01"])
    assert dq_check._parse_date(args.since) == dt.date(2026, 6, 1)
