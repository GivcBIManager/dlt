# tests/test_merge_hash.py
from __future__ import annotations
import subprocess
import sys
import pyarrow as pa
from etl.iceberg_load import _serialize_keys, _merge_hash_array


def _t(ids, branch, codes=None):
    cols = {"id": pa.array(ids, pa.int64()),
            "branch_id": pa.array(branch, pa.int64())}
    if codes is not None:
        cols["code"] = pa.array(codes, pa.string())
    return pa.table(cols)


def test_hash_is_16_byte_binary_row_aligned():
    t = _t([1, 2, 3], [7, 7, 7])
    arr = _merge_hash_array(t, ["id", "branch_id"])
    assert arr.type == pa.binary()
    assert len(arr) == 3
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
