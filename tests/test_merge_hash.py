# tests/test_merge_hash.py
from __future__ import annotations
import subprocess
import sys
from decimal import Decimal
import pytest
import pyarrow as pa
from etl.iceberg_load import _serialize_keys, _merge_hash_array


def _t(ids, branch):
    return pa.table({"id": pa.array(ids, pa.int64()),
                     "branch_id": pa.array(branch, pa.int64())})


def test_hash_is_16_byte_binary_row_aligned():
    t = _t([1, 2, 3], [7, 7, 7])
    arr = _merge_hash_array(t, ["id", "branch_id"])
    assert arr.type == pa.binary()
    assert len(arr) == 3
    assert arr.null_count == 0
    assert all(len(v.as_py()) == 16 for v in arr)


def test_equal_keys_hash_equal_distinct_keys_differ():
    a = _merge_hash_array(_t([1], [7]), ["id", "branch_id"])
    b = _merge_hash_array(_t([1], [7]), ["id", "branch_id"])
    c = _merge_hash_array(_t([1], [8]), ["id", "branch_id"])   # different branch
    assert a[0].as_py() == b[0].as_py()
    assert a[0].as_py() != c[0].as_py()


def test_serialize_is_injective_across_column_boundary():
    # ("a","bc") must not serialize the same as ("ab","c")
    s1 = _serialize_keys(pa.table({"x": pa.array(["a"]), "y": pa.array(["bc"])}), ["x", "y"])
    s2 = _serialize_keys(pa.table({"x": pa.array(["ab"]), "y": pa.array(["c"])}), ["x", "y"])
    assert s1[0] != s2[0]


def test_serialize_null_differs_from_empty_string():
    s_null = _serialize_keys(pa.table({"x": pa.array([None], pa.string())}), ["x"])
    s_empty = _serialize_keys(pa.table({"x": pa.array([""], pa.string())}), ["x"])
    assert s_null[0] != s_empty[0]


def test_hash_stable_across_a_fresh_process():
    # A salted/nondeterministic hash would change between interpreter runs.
    code = (
        "import pyarrow as pa;"
        "from etl.iceberg_load import _merge_hash_array;"
        "t=pa.table({'id':pa.array([12345],pa.int64()),"
        "'branch_id':pa.array([7],pa.int64())});"
        "print(_merge_hash_array(t,['id','branch_id'])[0].as_py().hex())"
    )
    out1 = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    out2 = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out1.stdout.strip() == out2.stdout.strip()
    assert len(out1.stdout.strip()) == 32   # 16 bytes hex


def test_int_and_decimal_representations_of_same_key_hash_equal():
    # Oracle NUMBER ids may be inferred as int64 on one run and decimal128 on
    # another; the same logical key MUST hash identically regardless.
    as_int = pa.table({"id": pa.array([123], pa.int64()),
                       "branch_id": pa.array([7], pa.int64())})
    as_dec = pa.table({"id": pa.array([123], pa.decimal128(18, 0)),
                       "branch_id": pa.array([7], pa.decimal128(18, 0))})
    assert (_merge_hash_array(as_int, ["id", "branch_id"])[0].as_py()
            == _merge_hash_array(as_dec, ["id", "branch_id"])[0].as_py())


def test_decimal_hash_stable_across_a_fresh_process():
    # A decimal128 key column must be deterministic across interpreter runs too.
    code = (
        "import pyarrow as pa;"
        "from etl.iceberg_load import _merge_hash_array;"
        "t=pa.table({'id':pa.array([12345],pa.decimal128(18,0)),"
        "'branch_id':pa.array([7],pa.decimal128(18,0))});"
        "print(_merge_hash_array(t,['id','branch_id'])[0].as_py().hex())"
    )
    out1 = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    out2 = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out1.stdout.strip() == out2.stdout.strip()
    assert len(out1.stdout.strip()) == 32   # 16 bytes hex


from etl.iceberg_load import _append_merge_hash, _sort_by_hash
from etl.config import Settings


def test_append_adds_binary_hash_column():
    t = _t([1, 2], [7, 7])
    out = _append_merge_hash(t, ["id", "branch_id"], "merge_hash")
    assert "merge_hash" in out.column_names
    assert out.schema.field("merge_hash").type == pa.binary()
    assert out.num_rows == 2


def test_sort_by_hash_orders_rows_and_is_stable():
    t = _append_merge_hash(_t([3, 1, 2], [7, 7, 7]), ["id", "branch_id"], "merge_hash")
    out = _sort_by_hash(t, "merge_hash")
    hashes = [v.as_py() for v in out.column("merge_hash")]
    assert hashes == sorted(hashes)


def test_sort_by_hash_noop_when_missing_or_empty():
    t = _t([1], [7])
    assert _sort_by_hash(t, "merge_hash").equals(t)        # column absent
    empty = t.slice(0, 0)
    assert _sort_by_hash(_append_merge_hash(empty, ["id", "branch_id"], "merge_hash"),
                         "merge_hash").num_rows == 0


def test_settings_has_merge_hash_column_default():
    assert Settings().merge_hash_column == "merge_hash"


def test_merge_hash_column_normalized_to_lowercase():
    from etl.config import Settings
    assert Settings(merge_hash_column="MERGE_HASH").merge_hash_column == "merge_hash"
    assert Settings().merge_hash_column == "merge_hash"     # default unchanged


def test_hash_rejects_fractional_decimal_key():
    t = pa.table({"id": pa.array([Decimal("1.50")], pa.decimal128(18, 2)),
                  "branch_id": pa.array([1], pa.int64())})
    with pytest.raises(ValueError, match="not run-stable"):
        _merge_hash_array(t, ["id", "branch_id"])


def test_hash_allows_scale_zero_decimal_key():
    t = pa.table({"id": pa.array([Decimal("123")], pa.decimal128(18, 0)),
                  "branch_id": pa.array([1], pa.int64())})
    assert len(_merge_hash_array(t, ["id", "branch_id"])) == 1   # scale-0 decimal id OK
