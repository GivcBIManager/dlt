"""Unified re-read window ([etl] resync_days): catch rows Oracle changes without
moving the CDC column (C1) by re-reading recent rows every incremental run.
Covers the setting (default, parsing, validation) and the query branch."""
from __future__ import annotations

import pytest

from etl import config
from etl.config import Settings


# --- the setting ------------------------------------------------------------ #
def _load_with(monkeypatch, value):
    real = config._cfg
    monkeypatch.setattr(config, "_cfg",
                        lambda key, default: value if key == "etl.resync_days" else real(key, default))
    return config.load_settings({})


def test_default_window_is_60_days():
    assert Settings().resync_days == 60


def test_load_settings_reads_resync_days(monkeypatch):
    assert _load_with(monkeypatch, 14).resync_days == 14


def test_zero_disables_and_is_accepted(monkeypatch):
    assert _load_with(monkeypatch, 0).resync_days == 0


def test_digit_string_from_an_env_override_is_accepted(monkeypatch):
    assert _load_with(monkeypatch, "30").resync_days == 30


@pytest.mark.parametrize("bad", [-1, "abc", 1.5, True, "-3"])
def test_bad_values_are_rejected(monkeypatch, bad):
    with pytest.raises(ValueError, match="resync_days"):
        _load_with(monkeypatch, bad)


from etl.config import CATEGORY_MASTER, CATEGORY_TRANSACTION, MODE_INCREMENTAL, MODE_INITIAL, HelperJoin, Settings, TableDef  # noqa: E402
from etl.oracle_extract import Watermark, build_query  # noqa: E402

WM = "2026-09-28 06:05:39.000000"
LIT = "TO_DATE('2026-09-28 06:05:39', 'YYYY-MM-DD HH24:MI:SS')"


def _ol(**over):
    kw = dict(table="OASIS.ORDER_LINES", unique_key="ORDER_LINE", cdc_column="AMEND_LAST_DATE",
              where_date_column="CREATION_DATE", where_operator=">=",
              where_value_of_initial_run="2022-01-01", category=CATEGORY_TRANSACTION)
    kw.update(over)
    return TableDef(**kw)


def _q(tdef, days=60, date=WM, key=None, mode=MODE_INCREMENTAL, date_kind="datetime"):
    return build_query(tdef, Settings(mode=mode, resync_days=days), Watermark(value=WM, kind="datetime"),
                       Watermark(value=date, kind=date_kind) if date else Watermark(value=None),
                       Watermark(value=key, kind="number") if key else None)


def test_resync_branch_is_appended_and_disjoint():
    parts = _q(_ol()).split("\nUNION ALL\n")
    assert len(parts) == 3
    base = parts[0].split(" WHERE ")[0]
    assert parts[2] == (f"{base} WHERE t.CREATION_DATE >= TRUNC(SYSDATE) - 60"
                        f" AND t.CREATION_DATE < {LIT}"
                        f" AND LNNVL(t.AMEND_LAST_DATE > {LIT})")


def test_zero_days_disables_and_leaves_the_query_unchanged():
    assert "TRUNC(SYSDATE)" not in _q(_ol(), days=0)
    assert _q(_ol()).split("\nUNION ALL\n")[:2] == _q(_ol(), days=0).split("\nUNION ALL\n")


def test_resync_excludes_rows_the_insert_key_branch_takes():
    helper = HelperJoin(table="OASIS.DELIVERY_LINES", join_keys=(("DELIVERY_LINE", "DELIVERY_LINE"),),
                        cdc_column="AMEND_LAST_DATE", where_date_column="AMEND_LAST_DATE")
    t = _ol(table="OASIS.DELIVERY_CHARGE", unique_key="DELIVERY_CHARGE_ID", cdc_column=None,
            where_date_column="DELIVERY_DATE", helper=helper,
            insert_key_column="DELIVERY_CHARGE_ID", insert_key_lookback=1000)
    parts = _q(t, key="1505160.0").split("\nUNION ALL\n")
    assert len(parts) == 4
    base = parts[0].split(" WHERE ")[0]
    # Windowed on the charge's OWN date (a charge changed under a line untouched
    # for > N days is still re-read), kept disjoint from the new-rows branch by
    # LNNVL of its helper-date predicate since the two date columns differ.
    assert parts[3] == (f"{base} WHERE t.DELIVERY_DATE >= TRUNC(SYSDATE) - 60"
                        f" AND LNNVL(h.AMEND_LAST_DATE >= {LIT})"
                        f" AND LNNVL(h.AMEND_LAST_DATE > {LIT})"
                        f" AND LNNVL(t.DELIVERY_CHARGE_ID > 1504160.0)")


def test_helper_table_without_its_own_date_windows_on_the_helper_date():
    helper = HelperJoin(table="DEVDBA.CLAIM_VISIT_DETAIL", join_keys=(("VISIT_ID", "VISIT_ID"),),
                        cdc_column="AMEND_LAST_DATE", where_date_column="STAT_END_DATE")
    t = _ol(table="DEVDBA.CLAIM_SERVICE_DETAIL", unique_key="VISIT_ID,SERVICE_ID", cdc_column=None,
            where_date_column=None, helper=helper)
    last = _q(t).split("\nUNION ALL\n")[-1]
    assert f"WHERE h.STAT_END_DATE >= TRUNC(SYSDATE) - 60 AND h.STAT_END_DATE < {LIT}" in last


def test_julian_date_watermark_gets_a_julian_bound():
    q = _q(_ol(where_date_column="JULIAN_DATE"), date="2461252.0", date_kind="number")
    assert "t.JULIAN_DATE >= TO_NUMBER(TO_CHAR(TRUNC(SYSDATE) - 60, 'J'))" in q


def test_not_applied_without_date_column_watermark_cdc_only_or_initial():
    assert "TRUNC(SYSDATE)" not in _q(_ol(), date=None)
    assert "TRUNC(SYSDATE)" not in _q(_ol(), mode=MODE_INITIAL)
    assert "TRUNC(SYSDATE)" not in _q(_ol(incremental_cdc_only=True))
    master = _ol(table="OASIS.ROOM_MASTER", unique_key="ROOM_NO", where_date_column=None,
                 where_operator=None, where_value_of_initial_run=None, category=CATEGORY_MASTER)
    assert "TRUNC(SYSDATE)" not in _q(master)
