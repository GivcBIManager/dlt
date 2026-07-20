"""Floating-point merge keys must be coerced to a run-stable type at extract.

oracledb's native Arrow fetch maps an *unconstrained* Oracle NUMBER (no
precision/scale metadata -- typical of many master-table id/code columns) to
Arrow ``double``. When such a column is the merge key, the load-side
run-stability guard (_serialize_keys) rejects it: hashing a float key is not
run-stable across runs -> silent duplicate rows.

inject_columns (the shared choke point for both the Arrow and cursor staging
paths) makes every merge-key column run-stable:

* floating with integer values  -> scale-0 ``decimal128`` (hashes like an int);
* floating with fractional values -> rejected (a float's string cast drifts);
* ``decimal`` with scale > 0 (a genuinely fractional OR scale-drifting key, e.g.
  ``RULE_IOS``/``IOS`` fetched as decimal128(_,3)/(_,14) in some branches and
  decimal128(_,0) in others) -> canonical string with trailing zeros stripped,
  so ``114945`` and ``114945.00000000000000`` converge to the same hash.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest

from etl.config import CATEGORY_MASTER, Settings, TableDef
from etl import oracle_extract, types_map
from etl.iceberg_load import _append_merge_hash

NOW = dt.datetime(2026, 7, 20, 12, 0, 0)


def _tdef(unique_key="POST_NUMBER", category=CATEGORY_MASTER):
    return TableDef(
        table="OASIS.STAFF_POSTS", unique_key=unique_key, cdc_column=None,
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=category)


def test_float_merge_key_is_coerced_to_scale0_decimal():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array([1.0, 123.0, 4500001.0], pa.float64())})

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    t = out.column("POST_NUMBER").type
    assert pa.types.is_decimal(t) and t.scale == 0
    assert out.column("POST_NUMBER").to_pylist() == [
        Decimal(1), Decimal(123), Decimal(4500001)]


def test_fractional_float_merge_key_is_rejected():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array([1.5, 2.0], pa.float64())})

    with pytest.raises(ValueError, match="not run-stable"):
        oracle_extract.inject_columns(
            base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)


def test_float_key_with_nulls_coerces_and_preserves_nulls():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array([1.0, None, 3.0], pa.float64())})

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    t = out.column("POST_NUMBER").type
    assert pa.types.is_decimal(t) and t.scale == 0
    assert out.column("POST_NUMBER").to_pylist() == [Decimal(1), None, Decimal(3)]


def test_non_key_float_column_left_as_double():
    settings = Settings()
    base = pa.table({
        "POST_NUMBER": pa.array([1, 2], pa.int64()),      # the merge key
        "SALARY": pa.array([1.5, 2.5], pa.float64()),     # a non-key float
    })

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    # Non-key floats are never hashed -> left untouched.
    assert pa.types.is_floating(out.column("SALARY").type)
    # An already run-stable integer key is left untouched.
    assert pa.types.is_integer(out.column("POST_NUMBER").type)


def test_string_merge_key_left_unchanged():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array(["A1", "B2"], pa.string())})

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    assert pa.types.is_string(out.column("POST_NUMBER").type)
    assert out.column("POST_NUMBER").to_pylist() == ["A1", "B2"]


def test_composite_float_key_all_parts_coerced():
    settings = Settings()
    base = pa.table({
        "DOC_ID": pa.array([10.0, 20.0], pa.float64()),
        "GL_CODE": pa.array([100.0, 200.0], pa.float64()),
    })

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings,
        tdef=_tdef(unique_key="DOC_ID,GL_CODE"), now=NOW)

    for name in ("DOC_ID", "GL_CODE"):
        t = out.column(name).type
        assert pa.types.is_decimal(t) and t.scale == 0, name


def test_coerced_float_key_passes_load_guard_and_hashes_like_int():
    """End-to-end: a key that arrives as float (the INITIAL-run failure mode)
    must pass the real load-side hash guard and hash identically to the same
    id fetched as an int -- otherwise a run that infers the type differently
    would silently produce duplicate rows."""
    settings = Settings()
    tdef = _tdef(unique_key="PRIORITY")
    key_cols = tdef.key_columns + [settings.branch_id_column]
    hash_col = settings.merge_hash_column

    float_tbl = oracle_extract.inject_columns(
        pa.table({"PRIORITY": pa.array([1.0, 2.0, 30.0], pa.float64())}),
        branch_id=7, settings=settings, tdef=tdef, now=NOW)
    int_tbl = oracle_extract.inject_columns(
        pa.table({"PRIORITY": pa.array([1, 2, 30], pa.int64())}),
        branch_id=7, settings=settings, tdef=tdef, now=NOW)

    # _append_merge_hash runs the run-stability guard; it used to raise here.
    h_float = _append_merge_hash(float_tbl, key_cols, hash_col).column(hash_col)
    h_int = _append_merge_hash(int_tbl, key_cols, hash_col).column(hash_col)
    assert h_float.to_pylist() == h_int.to_pylist()


# --------------------------------------------------------------------------- #
# Fractional / scale-drifting decimal key components -> canonical string
# --------------------------------------------------------------------------- #
def test_fractional_decimal_key_becomes_canonical_string():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array(
        [Decimal("115.000"), Decimal("3.185")], pa.decimal128(8, 3))})

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    assert pa.types.is_string(out.column("POST_NUMBER").type)
    # Trailing zeros stripped so an integral value renders like an int.
    assert out.column("POST_NUMBER").to_pylist() == ["115", "3.185"]


def test_integral_scaled_decimal_key_strips_to_int_string():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array(
        [Decimal("114945.00000000000000")], pa.decimal128(20, 14))})

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    assert out.column("POST_NUMBER").to_pylist() == ["114945"]


def test_scale0_decimal_key_left_numeric():
    settings = Settings()
    base = pa.table({"POST_NUMBER": pa.array(
        [Decimal("114945")], pa.decimal128(38, 0))})

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=_tdef(), now=NOW)

    # An already run-stable scale-0 decimal stays numeric (not stringified).
    t = out.column("POST_NUMBER").type
    assert pa.types.is_decimal(t) and t.scale == 0


def test_scale_drift_across_branches_hashes_equal():
    """The core run-stability invariant: the SAME id fetched as a scale-0
    decimal in one branch and a scaled decimal in another must hash identically
    once the load unifies the two staged schemas."""
    settings = Settings()
    tdef = _tdef(unique_key="POST_NUMBER")
    key_cols = tdef.key_columns + [settings.branch_id_column]
    hash_col = settings.merge_hash_column

    # Branch A: scale-0 decimal (left numeric by inject_columns)
    a = oracle_extract.inject_columns(
        pa.table({"POST_NUMBER": pa.array([Decimal("114945")], pa.decimal128(38, 0))}),
        branch_id=5, settings=settings, tdef=tdef, now=NOW)
    # Branch B: same id, scaled decimal (stringified by inject_columns)
    b = oracle_extract.inject_columns(
        pa.table({"POST_NUMBER": pa.array([Decimal("114945.00000000000000")],
                                          pa.decimal128(20, 14))}),
        branch_id=5, settings=settings, tdef=tdef, now=NOW)

    # Load unifies the branch schemas and casts each to it before hashing.
    unified = types_map.unify_schemas([a.schema, b.schema])
    a2 = types_map.cast_table_to_schema(a, unified)
    b2 = types_map.cast_table_to_schema(b, unified)
    ha = _append_merge_hash(a2, key_cols, hash_col).column(hash_col).to_pylist()
    hb = _append_merge_hash(b2, key_cols, hash_col).column(hash_col).to_pylist()
    assert ha == hb


def test_composite_key_with_fractional_component_loads():
    """policy_class_rules shape: a composite key where one member is a scaled
    decimal. The scaled member becomes a string; the others keep their type."""
    settings = Settings()
    tdef = _tdef(unique_key="CLASS_ID,RULE_IOS,TERM_ID")
    key_cols = tdef.key_columns + [settings.branch_id_column]
    hash_col = settings.merge_hash_column
    base = pa.table({
        "CLASS_ID": pa.array([10.0, 20.0], pa.float64()),           # integral float
        "RULE_IOS": pa.array([Decimal("3.185"), Decimal("7.000")],  # scaled decimal
                             pa.decimal128(8, 3)),
        "TERM_ID": pa.array([1, 2], pa.int64()),                    # plain int
    })

    out = oracle_extract.inject_columns(
        base, branch_id=7, settings=settings, tdef=tdef, now=NOW)

    assert pa.types.is_decimal(out.column("CLASS_ID").type)   # float -> decimal(0)
    assert pa.types.is_string(out.column("RULE_IOS").type)    # scaled -> string
    assert pa.types.is_integer(out.column("TERM_ID").type)    # int untouched
    # The guard must accept the whole composite key.
    _append_merge_hash(out, key_cols, hash_col)
