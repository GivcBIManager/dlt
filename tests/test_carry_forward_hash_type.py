"""Carry-forward on the merge hash: the stored hash must come back join-able.

``_merge_hash_array`` produces plain ``binary`` on the batch side. A stored
``merge_hash`` normally reads back as ``binary`` too -- but pyiceberg concats
each data file's batch with ``promote_options="permissive"``, and a file whose
Arrow batch came back large-typed promotes the WHOLE column to ``large_binary``.
Arrow refuses to join across the two ("Incompatible data types for corresponding
join field keys"), which failed the DOC/DOCL loads at extract. Both tables here
are therefore built from mixed-width data files -- a single-file table reads
back as plain ``binary`` and would not reproduce it.
"""
from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, Settings, TableDef
from etl.iceberg_load import (_carry_forward_insert_at, _existing_insert_at,
                              _merge_hash_array)


def _tdef():
    return TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _hashes_of(ids, branches):
    return _merge_hash_array(
        pa.table({"ID": pa.array(ids, pa.int64()),
                  "BRANCH_ID": pa.array(branches, pa.int64())}),
        ["ID", "BRANCH_ID"])


def _rows(hashes, insert_ats, hash_type):
    return pa.table({
        "merge_hash": hashes.cast(hash_type),
        "branch_id": pa.array([7] * len(insert_ats), pa.int64()),
        "insert_at": pa.array(insert_ats, pa.timestamp("us")),
    })


def _stored_table(tmp_path, monkeypatch, hashes, insert_ats):
    """Register an oasis.foo holding merge_hash + insert_at, via a real catalog.

    Written as two data files of differing Arrow binary width -- the shape that
    makes the scan's permissive concat hand back ``large_binary``.
    """
    cat = SqlCatalog(
        "t", uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    small = _rows(hashes, insert_ats, pa.binary())
    tbl = cat.create_table("oasis.foo", schema=small.schema)
    tbl.append(small)
    # A second, unrelated row written large-typed: enough to promote the column.
    tbl.append(_rows(pa.array([b"z" * 16]), [None], pa.large_binary()))
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)


def test_stored_hash_is_aligned_to_the_batch_hash_type(tmp_path, monkeypatch):
    stored = _hashes_of([1, 2], [7, 7])
    _stored_table(tmp_path, monkeypatch, stored, [None, None])

    existing = _existing_insert_at(
        Settings(), _tdef(), [7], pa.schema([]), hash_ready=True)

    assert existing is not None
    # Reads back large_binary; must be handed on as the batch-side binary.
    assert existing.column("merge_hash").type.equals(pa.binary())
    got = {v.as_py() for v in existing.column("merge_hash")}
    assert got >= {v.as_py() for v in stored}   # digests survive the cast


def test_carry_forward_join_succeeds_on_stored_hash(tmp_path, monkeypatch):
    """End of the chain: the join that raised on DOC/DOCL now resolves."""
    prior = pa.array([1_000_000], pa.timestamp("us"))     # id 1's original load
    _stored_table(tmp_path, monkeypatch, _hashes_of([1], [7]), prior)

    existing = _existing_insert_at(
        Settings(), _tdef(), [7], pa.schema([]), hash_ready=True)

    # A delta touching id 1 (already stored) and id 2 (new), as _finish_batch
    # builds it: hash computed in-process, insert_at set to this run's time.
    now = 9_000_000
    batch = pa.table({
        "merge_hash": _hashes_of([1, 2], [7, 7]),
        "insert_at": pa.array([now, now], pa.timestamp("us")),
    })

    out = _carry_forward_insert_at(batch, existing, ["merge_hash"], "insert_at")

    # Compare raw us since epoch -- .as_py() datetimes would drag in the local zone.
    raw = out.column("insert_at").cast(pa.int64())
    by_hash = {h.as_py(): t.as_py() for h, t in zip(out.column("merge_hash"), raw)}
    id1, id2 = (v.as_py() for v in _hashes_of([1, 2], [7, 7]))
    assert by_hash[id1] == 1_000_000            # carried forward from the store
    assert by_hash[id2] == now                  # new row keeps this run's time
