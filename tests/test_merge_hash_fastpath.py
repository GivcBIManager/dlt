"""The fast merge hash must be byte-identical to the reference serializer.

_serialize_keys is the FROZEN reference: stored tables already carry its
digests, so any drift silently duplicates rows on later merges. Every case
here hashes both ways and demands equality.
"""
from __future__ import annotations

import hashlib
from decimal import Decimal

import pyarrow as pa
import pytest

from etl.iceberg_load import _merge_hash_array, _reject_unstable_key_types, _serialize_keys


def _reference_digests(table, key_cols):
    return [hashlib.blake2b(b, digest_size=16).digest()
            for b in _serialize_keys(table, key_cols)]


CASES = {
    "ints": pa.table({"id": pa.array([1, 2, 3, None], pa.int64()),
                      "branch_id": pa.array([7, 7, 8, 8], pa.int64())}),
    "decimals": pa.table({"id": pa.array([Decimal(1), None, Decimal(123456789)],
                                         pa.decimal128(38, 0)),
                          "branch_id": pa.array([1, 2, 3], pa.int64())}),
    "strings": pa.table({"id": pa.array(["", None, "abc", "münchen\U0001F600",
                                         "a\x00b", "x" * 300],  # > prefix cache
                                        pa.string()),
                         "branch_id": pa.array([1, 1, 2, 2, 3, 3], pa.int64())}),
    "single_col": pa.table({"id": pa.array(list(range(100)), pa.int64())}),
    "all_null": pa.table({"id": pa.array([None, None], pa.string()),
                          "branch_id": pa.array([1, 2], pa.int64())}),
    "empty": pa.table({"id": pa.array([], pa.int64()),
                       "branch_id": pa.array([], pa.int64())}),
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_fast_hash_matches_reference(name):
    t = CASES[name]
    got = [v.as_py() for v in _merge_hash_array(t, t.column_names)]
    assert got == _reference_digests(t, t.column_names)


def test_fast_hash_matches_reference_on_chunked_input():
    t = pa.concat_tables([
        pa.table({"id": pa.array([1, 2], pa.int64()),
                  "branch_id": pa.array([7, 7], pa.int64())}),
        pa.table({"id": pa.array([3, None], pa.int64()),
                  "branch_id": pa.array([8, 8], pa.int64())}),
    ])                                            # 2 chunks per column
    got = [v.as_py() for v in _merge_hash_array(t, t.column_names)]
    assert got == _reference_digests(t, t.column_names)


def test_fast_hash_matches_reference_on_sliced_input():
    t = pa.table({"id": pa.array(list(range(10)), pa.int64()),
                  "branch_id": pa.array([7] * 10, pa.int64())}).slice(3, 4)
    got = [v.as_py() for v in _merge_hash_array(t, t.column_names)]
    assert got == _reference_digests(t, t.column_names)


def test_reject_helper_raises_on_float_and_fractional_decimal():
    bad_float = pa.table({"id": pa.array([1.5], pa.float64())})
    bad_dec = pa.table({"id": pa.array([Decimal("1.50")], pa.decimal128(18, 2))})
    for t in (bad_float, bad_dec):
        with pytest.raises(ValueError, match="not run-stable"):
            _reject_unstable_key_types(t, ["id"])
