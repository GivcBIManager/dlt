"""The DQ window's default lower bound is a rolling ``dq_window_days`` back from
today, so the row base -- and with it the tolerance -- is the same size every
day instead of collapsing to a few hours on the 1st of the month."""
from __future__ import annotations

import datetime as dt

import pytest

import dq_check
from etl import config
from etl.config import Settings


@pytest.mark.parametrize("today,days,expected", [
    (dt.date(2026, 10, 1), 30, dt.date(2026, 9, 1)),   # the 1st no longer starts empty
    (dt.date(2026, 9, 15), 30, dt.date(2026, 8, 16)),
    (dt.date(2026, 3, 1), 7, dt.date(2026, 2, 22)),
    (dt.date(2024, 3, 1), 1, dt.date(2024, 2, 29)),    # leap day
])
def test_default_since_is_a_rolling_window(today, days, expected):
    assert dq_check._default_since(None, today, days=days) == expected


def test_year_flag_still_selects_that_years_jan_1():
    assert dq_check._default_since(2025, dt.date(2026, 9, 3), days=30) == dt.date(2025, 1, 1)


def test_explicit_since_beats_the_default():
    args = dq_check.parse_args(["--since", "2026-06-01"])
    assert dq_check._parse_date(args.since) == dt.date(2026, 6, 1)


def test_settings_default_window_is_30_days():
    assert Settings().dq_window_days == 30


def test_load_settings_reads_etl_window_days(monkeypatch):
    orig = config._cfg
    monkeypatch.setattr(
        config, "_cfg",
        lambda key, default: 14 if key == "etl.dq_window_days" else orig(key, default))
    assert config.load_settings().dq_window_days == 14
