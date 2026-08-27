"""``incremental_cdc_only``: collapse the INCREMENTAL refresh to one CDC predicate.

The default incremental form is a UNION ALL of a date branch and a CDC branch
(see ``_build_incremental_query``). Tables whose CDC column is indexed and
stamped on inserts as well as updates don't need the split, so this flag reduces
the query to ``WHERE <cdc> >= <watermark>``. Covers the query shape, the config
parse/guard, the frozen date watermark, and the GUI validator.
"""
from __future__ import annotations

import json

import pytest

from etl.config import (
    CATEGORY_MASTER,
    CATEGORY_SNAPSHOT,
    CATEGORY_TRANSACTION,
    MODE_INCREMENTAL,
    MODE_INITIAL,
    HelperJoin,
    Settings,
    TableDef,
    load_table_defs,
)
from etl.oracle_extract import Watermark, build_query

WM = "2026-07-30 12:07:04.000000"
LITERAL = "TO_DATE('2026-07-30 12:07:04', 'YYYY-MM-DD HH24:MI:SS')"


def _tdef(**over) -> TableDef:
    kw = dict(
        table="OASIS.VISITS",
        unique_key="VISIT_ID",
        cdc_column="AMEND_LAST_DATE",
        where_date_column="VISIT_DATE",
        where_operator=">=",
        where_value_of_initial_run="2026-01-01",
        category=CATEGORY_TRANSACTION,
        incremental_cdc_only=True,
    )
    kw.update(over)
    return TableDef(**kw)


def _query(tdef: TableDef, mode: str = MODE_INCREMENTAL, cdc: str | None = WM) -> str:
    return build_query(
        tdef,
        Settings(mode=mode),
        Watermark(value=cdc, kind="datetime") if cdc else Watermark(value=None),
        Watermark(value=WM, kind="datetime"),
    )


# --- the query shape -------------------------------------------------------- #
def test_cdc_only_query_is_the_single_predicate_form():
    assert _query(_tdef()) == (
        f"SELECT t.* FROM OASIS.VISITS t WHERE t.AMEND_LAST_DATE >= {LITERAL}"
    )


def test_cdc_only_query_has_no_union_or_date_branch():
    sql = _query(_tdef())
    assert "UNION ALL" not in sql
    assert "VISIT_DATE" not in sql


def test_cdc_only_ignores_the_window_ceiling():
    # the ceiling exists to bound the date branch; with no date branch it's moot
    sql = _query(_tdef(where_value_max="2026-12-31", where_operator_max="<="))
    assert "2026-12-31" not in sql
    assert sql.count("WHERE") == 1


def test_cdc_only_uses_ge_not_gt():
    # >= re-reads the boundary second (absorbed by the merge) rather than
    # risking a row stamped inside it against a truncated TO_DATE literal
    assert ">= " + LITERAL in _query(_tdef())
    assert "> " + LITERAL not in _query(_tdef()).replace(">= " + LITERAL, "")


def test_default_table_still_gets_the_union_form():
    sql = _query(_tdef(incremental_cdc_only=False))
    assert "UNION ALL" in sql
    assert "VISIT_DATE" in sql


def test_cdc_only_uses_the_helper_cdc_reference():
    tdef = _tdef(
        cdc_column=None,
        where_date_column=None,
        helper=HelperJoin(
            table="OASIS.VISIT_MASTER",
            join_keys=(("VISIT_ID", "VISIT_ID"),),
            cdc_column="AMEND_LAST_DATE",
            where_date_column=None,
        ),
    )
    sql = _query(tdef)
    assert f"WHERE h.AMEND_LAST_DATE >= {LITERAL}" in sql
    assert "UNION ALL" not in sql


# --- other load modes are untouched ----------------------------------------- #
def test_initial_load_is_unaffected():
    sql = _query(_tdef(), mode=MODE_INITIAL)
    assert "AMEND_LAST_DATE" not in sql
    assert "t.VISIT_DATE >= TO_DATE('2026-01-01', 'YYYY-MM-DD')" in sql


def test_incremental_without_a_watermark_falls_back_to_initial():
    sql = _query(_tdef(), cdc=None)
    assert "AMEND_LAST_DATE" not in sql
    assert "t.VISIT_DATE >= " in sql


def test_master_table_cdc_only_still_filters_on_cdc():
    sql = _query(_tdef(category=CATEGORY_MASTER))
    assert sql == f"SELECT t.* FROM OASIS.VISITS t WHERE t.AMEND_LAST_DATE >= {LITERAL}"


def test_snapshot_ignores_the_flag_and_stays_a_full_copy():
    sql = _query(_tdef(category=CATEGORY_SNAPSHOT, unique_key=None))
    assert sql == "SELECT * FROM OASIS.VISITS"


# --- the date watermark freezes while the flag is on ------------------------ #
def test_cdc_only_stops_tracking_the_date_watermark():
    assert _tdef().tracks_date_watermark is False
    # the capture column itself is unchanged -- only the advance is suppressed
    assert _tdef().date_capture_column == "VISIT_DATE"


def test_default_table_tracks_the_date_watermark():
    assert _tdef(incremental_cdc_only=False).tracks_date_watermark is True


# --- tables.json parsing ---------------------------------------------------- #
def _write(tmp_path, entry):
    path = tmp_path / "tables.json"
    path.write_text(json.dumps({"transactions": [entry]}), encoding="utf-8")
    return path


def test_flag_is_parsed_from_tables_json(tmp_path):
    path = _write(tmp_path, {
        "table": "OASIS.VISITS", "unique_key": "VISIT_ID",
        "cdc_column": "AMEND_LAST_DATE", "incremental_cdc_only": True,
    })
    assert load_table_defs(path)[0].incremental_cdc_only is True


def test_flag_defaults_to_false_when_absent(tmp_path):
    path = _write(tmp_path, {
        "table": "OASIS.VISITS", "unique_key": "VISIT_ID",
        "cdc_column": "AMEND_LAST_DATE",
    })
    assert load_table_defs(path)[0].incremental_cdc_only is False


def test_flag_without_a_cdc_source_is_rejected_at_load(tmp_path):
    path = _write(tmp_path, {
        "table": "OASIS.VISITS", "unique_key": "VISIT_ID",
        "cdc_column": None, "incremental_cdc_only": True,
    })
    with pytest.raises(ValueError, match="incremental_cdc_only"):
        load_table_defs(path)


# --- GUI validation --------------------------------------------------------- #
def _errs(entry, category="transactions"):
    import tables_store
    doc = {"masters": [], "transactions": [], "snapshots": []}
    doc[category] = [entry]
    return tables_store.validate(doc)


def _entry(**over):
    e = {"table": "OASIS.VISITS", "unique_key": "VISIT_ID",
         "cdc_column": "AMEND_LAST_DATE", "incremental_cdc_only": True}
    e.update(over)
    return e


def test_validator_accepts_the_flag():
    assert _errs(_entry()) == []


def test_validator_accepts_the_flag_turned_off():
    assert _errs(_entry(incremental_cdc_only=False)) == []


def test_validator_rejects_a_non_boolean():
    assert any("true or false" in e for e in _errs(_entry(incremental_cdc_only="yes")))


def test_validator_requires_a_cdc_source():
    errs = _errs(_entry(cdc_column=None))
    assert any("requires a 'cdc_column'" in e for e in errs)


def test_validator_accepts_a_helper_supplied_cdc():
    entry = _entry(cdc_column=None, helper={
        "table": "OASIS.VISIT_MASTER",
        "cdc_column": "AMEND_LAST_DATE",
        "join": [["VISIT_ID", "VISIT_ID"]],
    })
    assert _errs(entry) == []


def test_validator_rejects_the_flag_on_a_snapshot():
    errs = _errs(_entry(unique_key=None), category="snapshots")
    assert any("does not apply to snapshots" in e for e in errs)
