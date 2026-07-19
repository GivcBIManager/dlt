"""``_finish_batch`` reshapes one streamed batch: cast to schema, (optionally)
derive+sort the merge hash, carry forward insert_at. The hash gate must be a
true no-op when disabled -- no ``merge_hash`` column, no reordering -- so
existing (non-hash) tables keep loading byte-identical output.
"""
from __future__ import annotations

import pyarrow as pa
from etl.iceberg_load import _finish_batch


def _schema():
    return pa.schema([("id", pa.int64()), ("branch_id", pa.int64()), ("v", pa.string())])


def _batch(ids, vs, branch=7):
    return pa.table({"id": pa.array(ids, pa.int64()),
                     "branch_id": pa.array([branch] * len(ids), pa.int64()),
                     "v": pa.array(vs)})


def test_finish_batch_appends_and_sorts_hash_when_enabled():
    out = _finish_batch(_batch([3, 1, 2], ["c", "a", "b"]), _schema(),
                        existing_insert_at=None, insert_col="insert_at",
                        write_hash=True, hash_key_cols=["id", "branch_id"],
                        hash_col="merge_hash", carry_keys=["id", "branch_id"])
    assert "merge_hash" in out.column_names
    assert out.schema.field("merge_hash").type == pa.binary()
    hashes = [h.as_py() for h in out.column("merge_hash")]
    assert hashes == sorted(hashes)                       # sorted by hash
    assert out.num_rows == 3                              # no rows lost


def test_finish_batch_no_hash_when_disabled():
    out = _finish_batch(_batch([1, 2], ["a", "b"]), _schema(),
                        existing_insert_at=None, insert_col="insert_at",
                        write_hash=False, hash_key_cols=["id", "branch_id"],
                        hash_col="merge_hash", carry_keys=["id", "branch_id"])
    assert "merge_hash" not in out.column_names
    assert out.column_names == ["id", "branch_id", "v"]


def test_finish_batch_hashes_then_carries_forward_then_sorts():
    # write_hash=True AND existing rows present: hash is appended, carry-forward
    # brings the old insert_at for the matching key, and the batch is left sorted
    # by hash — the composition Task 6 keys on.
    schema = pa.schema([("id", pa.int64()), ("branch_id", pa.int64()),
                        ("insert_at", pa.string())])
    batch = pa.table({"id": pa.array([5, 6], pa.int64()),
                      "branch_id": pa.array([1, 1], pa.int64()),
                      "insert_at": pa.array(["2026-07-19", "2026-07-19"])})
    existing = pa.table({"id": pa.array([5], pa.int64()),
                         "branch_id": pa.array([1], pa.int64()),
                         "insert_at": pa.array(["2020-01-01"])})
    out = _finish_batch(batch, schema, existing_insert_at=existing,
                        insert_col="insert_at", write_hash=True,
                        hash_key_cols=["id", "branch_id"], hash_col="merge_hash",
                        carry_keys=["id", "branch_id"])
    assert "merge_hash" in out.column_names            # hash survives the join
    by_id = dict(zip(out.column("id").to_pylist(),
                     out.column("insert_at").to_pylist()))
    assert by_id[5] == "2020-01-01"                    # existing row: old insert_at kept
    assert by_id[6] == "2026-07-19"                    # new row: batch insert_at
    hashes = out.column("merge_hash").to_pylist()
    assert hashes == sorted(hashes)                    # sorted by hash after the join
    assert out.num_rows == 2                           # no rows lost
