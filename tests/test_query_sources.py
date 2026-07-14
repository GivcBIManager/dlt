"""Query-based sources: an inline-view subquery in ``table`` plus an explicit
``name`` that becomes the Iceberg table / staging / control-state name."""
from __future__ import annotations

import json

import pytest

from etl.config import (
    CATEGORY_SNAPSHOT,
    CATEGORY_TRANSACTION,
    MODE_INCREMENTAL,
    MODE_INITIAL,
    Settings,
    TableDef,
    load_table_defs,
)
from etl.oracle_extract import Watermark, build_query

QUERY = (
    "(SELECT v.VISIT_ID, v.AMEND_LAST_DATE, v.VISIT_DATE, m.STATUS "
    "FROM OASIS.VISITS v JOIN OASIS.VISIT_MASTER m ON m.VISIT_ID = v.VISIT_ID)"
)


def _tdef(**over) -> TableDef:
    kw = dict(
        table=QUERY,
        unique_key="VISIT_ID",
        cdc_column="AMEND_LAST_DATE",
        where_date_column="VISIT_DATE",
        where_operator=None,
        where_value_of_initial_run=None,
        category=CATEGORY_TRANSACTION,
        name="visits_enriched",
    )
    kw.update(over)
    return TableDef(**kw)


# --- TableDef identifiers --------------------------------------------------- #
def test_is_query_true_for_subquery():
    assert _tdef().is_query is True


def test_is_query_false_for_plain_table():
    assert _tdef(table="OASIS.VISITS").is_query is False


def test_dataset_table_name_comes_from_name():
    assert _tdef().dataset_table_name == "visits_enriched"


def test_dataset_table_name_is_normalized():
    assert _tdef(name="Visits-Enriched").dataset_table_name == "visits_enriched"


def test_object_name_is_name_for_query_entry():
    # --tables CLI filter matches on object_name, so query entries match by name
    assert _tdef().object_name == "visits_enriched"


def test_owner_empty_for_query_entry():
    assert _tdef().owner == ""


def test_plain_table_derives_names_from_identifier():
    t = _tdef(table="OASIS.VISITS", name=None)
    assert t.owner == "OASIS"
    assert t.object_name == "VISITS"
    assert t.dataset_table_name == "visits"


# --- loader ------------------------------------------------------------------ #
def _write_tables_json(tmp_path, entry):
    p = tmp_path / "tables.json"
    p.write_text(json.dumps({"transactions": [entry]}), encoding="utf-8")
    return p


def test_load_table_defs_query_entry(tmp_path):
    p = _write_tables_json(tmp_path, {
        "table": QUERY,
        "name": "visits_enriched",
        "unique_key": "VISIT_ID",
        "cdc_column": "AMEND_LAST_DATE",
        "where_date_column": "VISIT_DATE",
    })
    (tdef,) = load_table_defs(p)
    assert tdef.is_query
    assert tdef.dataset_table_name == "visits_enriched"


def test_load_table_defs_query_entry_without_name_raises(tmp_path):
    p = _write_tables_json(tmp_path, {"table": QUERY, "unique_key": "VISIT_ID"})
    with pytest.raises(ValueError, match="name"):
        load_table_defs(p)


# --- SQL rendering: the subquery is a valid Oracle inline view --------------- #
def test_build_query_initial_wraps_inline_view():
    tdef = _tdef(where_operator=">=", where_value_of_initial_run="2026-06-01")
    settings = Settings(mode=MODE_INITIAL)
    q = build_query(tdef, settings, Watermark(value=None), Watermark(value=None))
    assert q.startswith(f"SELECT t.* FROM {QUERY} t")
    assert "t.VISIT_DATE >= TO_DATE('2026-06-01', 'YYYY-MM-DD')" in q


def test_build_query_incremental_union_uses_inline_view():
    tdef = _tdef()
    settings = Settings(mode=MODE_INCREMENTAL)
    cdc_wm = Watermark(value="2026-07-01 00:00:00.000000", kind="datetime")
    date_wm = Watermark(value="2026-07-01 00:00:00.000000", kind="datetime")
    q = build_query(tdef, settings, cdc_wm, date_wm)
    # both UNION ALL branches select from the inline view
    assert q.count(f"FROM {QUERY} t") == 2
    assert "UNION ALL" in q
    assert "t.AMEND_LAST_DATE >" in q
    assert "t.VISIT_DATE >=" in q


def test_build_query_snapshot_full_copy_of_query():
    tdef = _tdef(category=CATEGORY_SNAPSHOT, unique_key="",
                 cdc_column=None, where_date_column=None)
    settings = Settings(mode=MODE_INCREMENTAL)
    q = build_query(tdef, settings, Watermark(value=None), Watermark(value=None))
    assert q == f"SELECT * FROM {QUERY}"
