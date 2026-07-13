"""DQ payload digest is a compact 16-byte binary, not a 32-char hex string.

Halves the hash column in the accumulated (key, hash) tables and the comparison
join -- memory is the binding constraint on the large windows -- while the join
and mismatch detection behave identically.
"""
from __future__ import annotations

import pyarrow as pa

from etl import dq_check


def test_key_and_hash_returns_binary16():
    tbl = pa.table({"ID": pa.array([1, 2], pa.int64()),
                    "V": pa.array(["a", "b"], pa.string())})
    keys, hashes = dq_check._key_and_hash(tbl, ["ID"], ["V"])
    assert hashes.type == pa.binary(16)
    assert len(hashes) == 2
    assert keys.to_pylist() == ["1", "2"]


def test_identical_rows_hash_equal():
    t1 = pa.table({"ID": pa.array([1], pa.int64()), "V": pa.array(["x"], pa.string())})
    t2 = pa.table({"ID": pa.array([1], pa.int64()), "V": pa.array(["x"], pa.string())})
    _, h1 = dq_check._key_and_hash(t1, ["ID"], ["V"])
    _, h2 = dq_check._key_and_hash(t2, ["ID"], ["V"])
    assert h1.to_pylist() == h2.to_pylist()


def test_compare_buckets_with_binary_hash():
    # key 1 matches, key 2 differs in payload -> mismatch, key 3 only in oracle,
    # key 4 only in iceberg.
    ot = pa.table({"ID": pa.array([1, 2, 3], pa.int64()),
                   "V": pa.array(["a", "b", "c"], pa.string())})
    it = pa.table({"ID": pa.array([1, 2, 4], pa.int64()),
                   "V": pa.array(["a", "DIFF", "d"], pa.string())})
    ok, oh = dq_check._key_and_hash(ot, ["ID"], ["V"])
    ik, ih = dq_check._key_and_hash(it, ["ID"], ["V"])
    ora = pa.table({"k": ok, "h": oh})
    ice = pa.table({"k": ik, "h": ih})
    d = dq_check._compare(ora, ice)
    assert d.matched == 1
    assert d.mismatch == 1
    assert d.only_in_oracle == 1
    assert d.only_in_iceberg == 1


def test_compare_handles_empty_side():
    ot = pa.table({"ID": pa.array([1], pa.int64()), "V": pa.array(["a"], pa.string())})
    ok, oh = dq_check._key_and_hash(ot, ["ID"], ["V"])
    ora = pa.table({"k": ok, "h": oh})
    empty = pa.table({"k": pa.array([], pa.string()), "h": pa.array([], pa.binary(16))})
    d = dq_check._compare(ora, empty)
    assert d.only_in_oracle == 1 and d.matched == 0
