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
