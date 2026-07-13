"""tables_store.validate rejects SQL-injection-shaped values in config fields.

These fields are concatenated into Oracle SQL unparameterized, so structural
identifiers must look like identifiers and value expressions must not smuggle in
statement terminators or comments.
"""
from __future__ import annotations

import copy

import pytest


def _doc(**over):
    entry = {"table": "OASIS.MY_TABLE", "unique_key": "ID", "cdc_column": "AMEND_LAST_DATE"}
    entry.update(over)
    return {"masters": [entry], "transactions": [], "snapshots": []}


def _errs(doc):
    import tables_store
    return tables_store.validate(doc)


# --- structural identifiers ------------------------------------------------ #
def test_schema_qualified_table_ok():
    assert _errs(_doc()) == []


def test_composite_unique_key_ok():
    assert _errs(_doc(unique_key="CODE_FROM,CODE_TO")) == []


def test_inline_view_subquery_table_ok():
    # a source table may be an inline-view subquery (raw-SQL escape hatch)
    doc = _doc(table="(SELECT ID, AMT FROM DEVDBA.GL_INTERFACE WHERE AMT > 0)")
    assert _errs(doc) == []


def test_subquery_table_with_terminator_rejected():
    doc = _doc(table="(SELECT 1 FROM DUAL); DROP TABLE X --)")
    assert _errs(doc)


def test_table_with_injection_rejected():
    errs = _errs(_doc(table="OASIS.T; DROP TABLE X"))
    assert errs and any("table" in e.lower() or "identifier" in e.lower() for e in errs)


def test_unique_key_with_injection_rejected():
    errs = _errs(_doc(unique_key="ID; DELETE FROM X"))
    assert errs


def test_cdc_column_with_comment_rejected():
    errs = _errs(_doc(cdc_column="AMEND_LAST_DATE--"))
    assert errs


# --- operator whitelist ---------------------------------------------------- #
def test_valid_operator_ok():
    assert _errs(_doc(where_operator=">=", where_value_of_initial_run="2020-01-01")) == []


def test_operator_injection_rejected():
    errs = _errs(_doc(where_operator="; DROP", where_value_of_initial_run="1"))
    assert errs


# --- value expressions: allow functions, reject terminators/comments ------- #
def test_function_value_expression_ok():
    doc = _doc(where_operator=">=",
               where_value_of_initial_run="TO_DATE('2020-01-01','YYYY-MM-DD')")
    assert _errs(doc) == []


def test_value_with_semicolon_rejected():
    errs = _errs(_doc(where_operator=">=", where_value_of_initial_run="1; DROP TABLE X"))
    assert errs


def test_value_with_sql_comment_rejected():
    errs = _errs(_doc(where_operator=">=", where_value_of_initial_run="2020 -- x"))
    assert errs
