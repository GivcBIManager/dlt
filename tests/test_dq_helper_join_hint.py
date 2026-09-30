"""DQ's source query for a helper-driven table must drive from the CHILD's window.

docl on alrabwah took ~11 hours per DQ run: one DOC row dated year 0018 makes the
optimizer think ``h.DOC_DATE >= 2022-01-01`` (the pipeline's initial floor, which DQ
carries for scope) matches 0.4% of DOC, so it walked every document since 2022 and
only then discarded lines outside the month. ``LEADING(t) USE_NL(h)`` forces the
plan the healthy branches pick on their own -- the child's date range first, then a
primary-key lookup of each parent (measured: 596k rows in 250 s instead of ~11 h).
``LEADING(t)`` alone is not enough: Oracle then hash-joins against the same
full walk of the parent.
"""
from __future__ import annotations

import datetime as dt

from etl import dq_check
from etl.config import CATEGORY_MASTER, CATEGORY_TRANSACTION, TableDef, _parse_helper

HINT = "/*+ LEADING(t) USE_NL(h) */"
_ENTRY = {"last_cdc": {"value": "2026-09-02 11:54:57.000000", "kind": "datetime"}}


def _helper_tdef(date_col):
    entry = {"table": "DEVDBA.DOCL", "unique_key": "LINE_ID", "cdc_column": None,
             "where_date_column": date_col, "where_operator": ">=",
             "where_value_of_initial_run": "2022-01-01",
             "helper": {"table": "DEVDBA.DOC", "cdc_column": "AMEND_LAST_DATE",
                        "where_date_column": "DOC_DATE", "join": [["DOC_ID", "DOC_ID"]]}}
    return TableDef(
        table=entry["table"], unique_key=entry["unique_key"], cdc_column=None,
        where_date_column=date_col, where_operator=">=",
        where_value_of_initial_run="2022-01-01", category=CATEGORY_TRANSACTION,
        helper=_parse_helper(entry))


def _sqls(tdef):
    win = dq_check._make_window(tdef, _ENTRY, dt.date(2026, 9, 1), None)
    coverage, _ = dq_check._coverage_predicates(tdef, _ENTRY)
    return (dq_check._oracle_select(tdef, win, coverage),
            dq_check._oracle_count_sql(tdef, win, coverage))


def test_windowed_helper_table_leads_with_the_child():
    select, count = _sqls(_helper_tdef("DOC_DATE"))
    assert select.startswith(f"SELECT {HINT} t.*")
    assert count.startswith(f"SELECT {HINT} COUNT(*) FROM DEVDBA.DOCL t JOIN DEVDBA.DOC h")
    # the scope predicates are unchanged
    assert "t.DOC_DATE >= TO_DATE('2026-09-01', 'YYYY-MM-DD')" in select
    assert "h.DOC_DATE >= TO_DATE('2022-01-01', 'YYYY-MM-DD')" in select


def test_helper_table_without_its_own_window_is_not_hinted():
    # Full compare: there is no child range to lead with, and a nested loop per
    # child row over the whole table would be the wrong plan.
    select, count = _sqls(_helper_tdef(None))
    assert "/*+" not in select and "/*+" not in count


def test_plain_table_is_not_hinted():
    tdef = TableDef(table="OASIS.ORDER_LINES", unique_key="ORDER_LINE",
                    cdc_column="AMEND_LAST_DATE", where_date_column="CREATION_DATE",
                    where_operator=">=", where_value_of_initial_run="2022-01-01",
                    category=CATEGORY_TRANSACTION, helper=None)
    select, count = _sqls(tdef)
    assert "/*+" not in select and "/*+" not in count


def test_master_is_not_hinted():
    tdef = TableDef(table="OASIS.CONTRACTS", unique_key="CONTRACT_NO",
                    cdc_column="AMEND_LAST_DATE", where_date_column=None,
                    where_operator=None, where_value_of_initial_run=None,
                    category=CATEGORY_MASTER, helper=None)
    assert "/*+" not in _sqls(tdef)[0]
