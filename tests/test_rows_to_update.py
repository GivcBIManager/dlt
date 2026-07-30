"""``_rows_to_update`` must match pyiceberg's row-by-row original exactly.

It replaces ``upsert_util.get_rows_to_update`` in the merge lookup for speed
(pyiceberg compares matched rows in Python at ~2.7k rows/s, which is what kept
the big-delta tables from ever finishing a merge). Speed is worthless if the
answer changes, so every test below asserts against the real pyiceberg function
on the same input rather than against a hand-written expectation.
"""
from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.table import upsert_util

from etl.iceberg_load import _rows_to_update

KEY = "k"


def _assert_same(source: pa.Table, target: pa.Table, key: str = KEY) -> pa.Table:
    """Both implementations must select the same SET of source rows."""
    expected = upsert_util.get_rows_to_update(source, target, [key])
    actual = _rows_to_update(source, target, key)

    assert actual.schema.equals(expected.schema)
    assert actual.num_rows == expected.num_rows
    # Row order follows the join, which neither implementation pins; compare as
    # sets of rows so an ordering difference is not a false failure.
    assert sorted(map(str, actual.to_pylist())) == sorted(map(str, expected.to_pylist()))
    return actual


def test_detects_changed_rows():
    source = pa.table({KEY: [1, 2, 3], "v": ["a", "CHANGED", "c"]})
    target = pa.table({KEY: [1, 2, 3], "v": ["a", "b", "c"]})

    out = _assert_same(source, target)
    assert out.num_rows == 1
    assert out.column("v").to_pylist() == ["CHANGED"]


def test_identical_rows_produce_nothing():
    """Unchanged-row elision: an all-identical delta must commit nothing."""
    rows = pa.table({KEY: [1, 2, 3], "v": ["a", "b", "c"]})

    assert _assert_same(rows, rows).num_rows == 0


def test_rows_absent_from_target_are_not_updates():
    """Unmatched keys are INSERTs; get_rows_to_update must ignore them."""
    source = pa.table({KEY: [1, 2, 99], "v": ["a", "b", "new"]})
    target = pa.table({KEY: [1, 2], "v": ["a", "b"]})

    assert _assert_same(source, target).num_rows == 0


def test_empty_target_returns_nothing():
    source = pa.table({KEY: [1], "v": ["a"]})
    target = pa.table({KEY: pa.array([], pa.int64()), "v": pa.array([], pa.string())})

    assert _assert_same(source, target).num_rows == 0


def test_null_to_value_is_a_change():
    source = pa.table({KEY: [1], "v": pa.array(["now set"], pa.string())})
    target = pa.table({KEY: [1], "v": pa.array([None], pa.string())})

    assert _assert_same(source, target).num_rows == 1


def test_value_to_null_is_a_change():
    source = pa.table({KEY: [1], "v": pa.array([None], pa.string())})
    target = pa.table({KEY: [1], "v": pa.array(["was set"], pa.string())})

    assert _assert_same(source, target).num_rows == 1


def test_null_on_both_sides_is_not_a_change():
    """The case a naive not_equal gets wrong: NULL != NULL is NULL, not True."""
    source = pa.table({KEY: [1, 2], "v": pa.array([None, "x"], pa.string())})
    target = pa.table({KEY: [1, 2], "v": pa.array([None, "x"], pa.string())})

    assert _assert_same(source, target).num_rows == 0


def test_change_in_any_column_counts():
    source = pa.table({KEY: [1, 2], "a": ["x", "x"], "b": [1, 1], "c": [9, 8]})
    target = pa.table({KEY: [1, 2], "a": ["x", "x"], "b": [1, 1], "c": [9, 7]})

    out = _assert_same(source, target)
    assert out.column(KEY).to_pylist() == [2]


def test_multiple_column_types():
    source = pa.table({
        KEY: [1, 2, 3],
        "s": pa.array(["a", "b", "c"], pa.string()),
        "i": pa.array([1, 2, 3], pa.int64()),
        "f": pa.array([1.5, 2.5, 3.5], pa.float64()),
        "b": pa.array([True, False, True], pa.bool_()),
        "d": pa.array(["1.10", "2.20", "3.30"], pa.string()).cast(pa.decimal128(5, 2)),
    })
    target = source.set_column(
        source.schema.get_field_index("f"), "f",
        pa.array([1.5, 99.9, 3.5], pa.float64()),
    )

    out = _assert_same(source, target)
    assert out.column(KEY).to_pylist() == [2]


def test_nested_column_falls_back_and_still_matches():
    """list<> has no not_equal kernel; the Python fallback must agree."""
    source = pa.table({KEY: [1, 2], "tags": pa.array([["a"], ["b", "c"]], pa.list_(pa.string()))})
    target = pa.table({KEY: [1, 2], "tags": pa.array([["a"], ["b"]], pa.list_(pa.string()))})

    out = _assert_same(source, target)
    assert out.column(KEY).to_pylist() == [2]


def test_key_only_table_has_nothing_to_compare():
    rows = pa.table({KEY: [1, 2, 3]})

    assert _assert_same(rows, rows).num_rows == 0


def test_duplicate_target_keys_raise_like_pyiceberg():
    source = pa.table({KEY: [1], "v": ["a"]})
    target = pa.table({KEY: [1, 1], "v": ["a", "b"]})

    with pytest.raises(ValueError, match="Target table has duplicate rows"):
        upsert_util.get_rows_to_update(source, target, [KEY])
    with pytest.raises(ValueError, match="Target table has duplicate rows"):
        _rows_to_update(source, target, KEY)


def test_target_row_order_does_not_matter():
    """The join aligns by key, not position."""
    source = pa.table({KEY: [1, 2, 3], "v": ["a", "b", "CHANGED"]})
    target = pa.table({KEY: [3, 1, 2], "v": ["c", "a", "b"]})

    out = _assert_same(source, target)
    assert out.column(KEY).to_pylist() == [3]


def test_binary_merge_hash_key():
    """The real merge key is a 128-bit binary hash, not an int."""
    keys = [b"\x00" * 15 + bytes([i]) for i in range(3)]
    source = pa.table({KEY: pa.array(keys, pa.binary()), "v": ["a", "CHANGED", "c"]})
    target = pa.table({KEY: pa.array(keys, pa.binary()), "v": ["a", "b", "c"]})

    assert _assert_same(source, target).column("v").to_pylist() == ["CHANGED"]


def test_large_random_delta_matches_pyiceberg():
    """Bulk cross-check: many rows, several columns, scattered edits and nulls."""
    n = 4000
    keys = list(range(n))
    base_s = [f"v{i}" for i in range(n)]
    base_i = [i * 3 for i in range(n)]

    target = pa.table({
        KEY: keys,
        "s": pa.array(base_s, pa.string()),
        "i": pa.array(base_i, pa.int64()),
        "n": pa.array([None if i % 7 == 0 else float(i) for i in range(n)], pa.float64()),
    })
    # Edit every 5th string, every 11th int, and flip some nulls both ways.
    src_s = [f"EDIT{i}" if i % 5 == 0 else base_s[i] for i in range(n)]
    src_i = [base_i[i] + 1 if i % 11 == 0 else base_i[i] for i in range(n)]
    src_n = [
        0.0 if i % 7 == 0 and i % 3 == 0            # NULL -> value
        else None if i % 13 == 0                    # value -> NULL (or NULL->NULL)
        else (None if i % 7 == 0 else float(i))
        for i in range(n)
    ]
    source = pa.table({
        KEY: keys,
        "s": pa.array(src_s, pa.string()),
        "i": pa.array(src_i, pa.int64()),
        "n": pa.array(src_n, pa.float64()),
    })

    out = _assert_same(source, target)
    assert 0 < out.num_rows < n  # a meaningful mix, not all-or-nothing
