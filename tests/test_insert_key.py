"""Insert-key watermark: a monotonic child key that catches rows inserted under
a parent whose CDC column never moves (DELIVERY_CHARGE under DELIVERY_LINES).
Covers config parse/validation (Task 4) and the query branch (Task 5)."""
from __future__ import annotations

import json

import pytest

from etl.config import load_table_defs


def _dc_entry(**over) -> dict:
    entry = {
        "table": "OASIS.DELIVERY_CHARGE", "unique_key": "DELIVERY_CHARGE_ID",
        "cdc_column": None, "where_date_column": "DELIVERY_DATE",
        "helper": {"table": "OASIS.DELIVERY_LINES",
                   "join": [["DELIVERY_LINE", "DELIVERY_LINE"]],
                   "cdc_column": "AMEND_LAST_DATE", "where_date_column": "AMEND_LAST_DATE"},
        "insert_key_column": "DELIVERY_CHARGE_ID", "insert_key_lookback": 1000,
    }
    entry.update(over)
    return {k: v for k, v in entry.items() if v is not ...}


def _load(tmp_path, entry, category="transactions"):
    p = tmp_path / "tables.json"
    p.write_text(json.dumps({category: [entry]}), encoding="utf-8")
    return load_table_defs(p)


# --- config ------------------------------------------------------------------ #
def test_insert_key_is_parsed(tmp_path):
    (tdef,) = _load(tmp_path, _dc_entry())
    assert tdef.insert_key_column == "DELIVERY_CHARGE_ID"
    assert tdef.insert_key_lookback == 1000


def test_insert_key_defaults_off(tmp_path):
    (tdef,) = _load(tmp_path, _dc_entry(insert_key_column=..., insert_key_lookback=...))
    assert tdef.insert_key_column is None
    assert tdef.insert_key_lookback == 0


@pytest.mark.parametrize("over, category, match", [
    (dict(incremental_cdc_only=True), "transactions", "incremental_cdc_only"),
    (dict(insert_key_column=...), "transactions", "insert_key_lookback"),
    (dict(insert_key_lookback=-1), "transactions", "non-negative"),
    (dict(insert_key_column="ID; DROP TABLE X"), "transactions", "identifier"),
    (dict(helper=...), "transactions", "CDC source"),
    (dict(), "snapshots", "snapshot"),
])
def test_insert_key_rejects_bad_config(tmp_path, over, category, match):
    with pytest.raises(ValueError, match=match):
        _load(tmp_path, _dc_entry(**over), category)


# --- query branch (Task 5) --------------------------------------------------- #
from etl.config import (  # noqa: E402
    CATEGORY_TRANSACTION, MODE_INCREMENTAL, MODE_INITIAL, HelperJoin, Settings, TableDef,
)
from etl.oracle_extract import Watermark, build_query  # noqa: E402

WM = "2026-09-28 06:05:39.000000"
LIT = "TO_DATE('2026-09-28 06:05:39', 'YYYY-MM-DD HH24:MI:SS')"
HELPER = HelperJoin(table="OASIS.DELIVERY_LINES", join_keys=(("DELIVERY_LINE", "DELIVERY_LINE"),),
                    cdc_column="AMEND_LAST_DATE", where_date_column="AMEND_LAST_DATE")


def _dc(**over) -> TableDef:
    kw = dict(table="OASIS.DELIVERY_CHARGE", unique_key="DELIVERY_CHARGE_ID", cdc_column=None,
              where_date_column="DELIVERY_DATE", where_operator=None,
              where_value_of_initial_run=None, category=CATEGORY_TRANSACTION, helper=HELPER,
              insert_key_column="DELIVERY_CHARGE_ID", insert_key_lookback=1000)
    kw.update(over)
    return TableDef(**kw)


def _q(tdef, key="1505160.0", date=WM, mode=MODE_INCREMENTAL):
    return build_query(
        tdef, Settings(mode=mode, resync_days=0),  # insert-key shape only; resync covered in test_resync
        Watermark(value=WM, kind="datetime"),
        Watermark(value=date, kind="datetime") if date else Watermark(value=None),
        Watermark(value=key, kind="number") if key else None,
    )


def test_key_branch_is_a_third_disjoint_union_branch():
    parts = _q(_dc()).split("\nUNION ALL\n")
    assert len(parts) == 3
    base = parts[0].split(" WHERE ")[0]
    # LNNVL, not NOT(...): a NULL helper CDC must still qualify (Review Focus 1);
    # the lookback renders as a plain decimal (Review Focus 2).
    assert parts[2] == (
        f"{base} WHERE t.DELIVERY_CHARGE_ID > 1504160.0"
        f" AND LNNVL(h.AMEND_LAST_DATE > {LIT})"
        f" AND LNNVL(h.AMEND_LAST_DATE >= {LIT})"
    )


def test_first_two_branches_are_unchanged_by_the_key():
    with_key = _q(_dc()).split("\nUNION ALL\n")[:2]
    without = _q(_dc(insert_key_column=None, insert_key_lookback=0)).split("\nUNION ALL\n")
    assert with_key == without


def test_no_key_watermark_yet_means_no_key_branch():
    # First run after enabling: the run captures last_key from its own rows.
    assert _q(_dc(), key=None) == _q(_dc(insert_key_column=None, insert_key_lookback=0))


def test_without_a_date_watermark_the_key_branch_excludes_only_cdc_rows():
    parts = _q(_dc(), date=None).split("\nUNION ALL\n")
    assert len(parts) == 2
    assert parts[1].endswith(
        f"WHERE t.DELIVERY_CHARGE_ID > 1504160.0 AND LNNVL(h.AMEND_LAST_DATE > {LIT})")


def test_zero_lookback_uses_the_mark_itself():
    assert "t.DELIVERY_CHARGE_ID > 1505160.0 AND" in _q(_dc(insert_key_lookback=0))


def test_initial_mode_ignores_the_key():
    assert "DELIVERY_CHARGE_ID >" not in _q(_dc(), mode=MODE_INITIAL)
