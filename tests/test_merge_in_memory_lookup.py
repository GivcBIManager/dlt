"""Single-key Iceberg merges find matched rows in memory, not via a giant ``In`` scan.

pyiceberg's ``table.upsert`` locates matched rows with
``scan(row_filter=In(key, <every delta key>))``, and the scan re-translates,
re-binds and re-compiles that predicate for EVERY data file
(``_task_to_record_batches``) -- O(files x delta_keys) pure-Python work. On
delivery_charge (165k-key delta, 34 files, 5.9M rows) that lookup alone took
367s, blew the 15-minute commit watchdog, the work was thrown away, and the
next run's delta was even bigger: the table has been stuck in that loop since
22 July, never able to catch up.

``_upsert_in_memory_lookup`` reads the table's key column once per file (a
skinny columnar read, no row filter, so no predicate machinery at all) and
matches the two key sets in memory with ``pc.is_in`` -- same answer, same
rows, without the per-file predicate rebuild. Only files that actually
contain matched keys are then read in full for the change diff. The write
side keeps pyiceberg's own ``overwrite(In(changed keys))`` + ``append``: the
delete path binds its predicate ONCE, so it never had the per-file cost.

Semantics must be IDENTICAL to ``table.upsert``: same matched/new split, same
unchanged-row elision, same duplicate-key errors, same snapshot shape.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from etl.iceberg_load import (
    _append_merge_hash,
    _merge_iceberg_single_commit,
    _upsert_in_memory_lookup,
)

HASH = "merge_hash"


def _cat(tmp_path, tag):
    cat = SqlCatalog("t", uri=f"sqlite:///{(tmp_path/f'c_{tag}.db').as_posix()}",
                     warehouse=(tmp_path/f"w_{tag}").as_uri(),
                     # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them.
                     **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    return cat


def _rows(ids, names, branch=1, with_hash=True):
    t = pa.table({"id": pa.array(ids, pa.int64()),
                  "name": pa.array(names),
                  "branch_id": pa.array([branch] * len(ids), pa.int64())})
    return _append_merge_hash(t, ["id", "branch_id"], HASH) if with_hash else t


def _schema_dict():
    return {
        "x-merge-strategy": "upsert",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True},
            "branch_id": {"name": "branch_id", "data_type": "bigint", "primary_key": True},
            "name": {"name": "name", "data_type": "text"},
        },
    }


def _seed(tmp_path, tag, *appends):
    """Create a table and append each ``appends`` table as its own data file."""
    cat = _cat(tmp_path, tag)
    t = cat.create_table(f"oasis.m_{tag}", schema=appends[0].schema)
    for part in appends:
        t.append(part)
    return t


def _names_by_id(t) -> dict[int, str]:
    got = t.scan().to_arrow()
    return dict(zip(got.column("id").to_pylist(), got.column("name").to_pylist()))


# --------------------------------------------------------------------------- #
# Routing: _merge_iceberg_single_commit must not go through table.upsert (the
# giant-In scan) for a single-column join, and must keep using it for the
# composite fallback.
# --------------------------------------------------------------------------- #

def test_single_key_merge_bypasses_table_upsert(tmp_path, monkeypatch):
    t = _seed(tmp_path, "route", _rows([0], ["seed"]))

    def _boom(self, *a, **k):
        raise AssertionError("table.upsert (giant-In lookup) must not be used "
                             "for a single-column merge key")

    monkeypatch.setattr(Table, "upsert", _boom)
    _merge_iceberg_single_commit(t, _rows([0, 1], ["u0", "n1"]), _schema_dict(), "m")
    t.refresh()
    assert _names_by_id(t) == {0: "u0", 1: "n1"}


def test_composite_key_merge_still_uses_table_upsert(tmp_path, monkeypatch):
    t = _seed(tmp_path, "comp", _rows([0], ["seed"], with_hash=False))
    calls = []
    original = Table.upsert

    def _spy(self, *a, **k):
        calls.append(1)
        return original(self, *a, **k)

    monkeypatch.setattr(Table, "upsert", _spy)
    _merge_iceberg_single_commit(t, _rows([0, 1], ["u0", "n1"], with_hash=False),
                                 _schema_dict(), "m")
    t.refresh()
    assert calls  # composite path unchanged: still pyiceberg's upsert
    assert _names_by_id(t) == {0: "u0", 1: "n1"}


# --------------------------------------------------------------------------- #
# The in-memory-lookup upsert itself: pyiceberg-identical semantics.
# --------------------------------------------------------------------------- #

def test_updates_inserts_and_leaves_unchanged_rows(tmp_path):
    t = _seed(tmp_path, "upd", _rows([0, 1], ["seed", "keep"]))
    delta = _rows([0, 1, 2], ["u0", "keep", "n2"])  # changed, identical, new
    _upsert_in_memory_lookup(t, delta, HASH, update_matched=True)
    t.refresh()
    assert _names_by_id(t) == {0: "u0", 1: "keep", 2: "n2"}


def test_untouched_data_files_survive_the_merge(tmp_path):
    # Two separate appends -> two data files. A delta touching only file A's
    # keys must leave file B's data file in place (pyiceberg's delete only
    # rewrites files it actually removed rows from).
    t = _seed(tmp_path, "files", _rows([0, 1], ["a0", "a1"]),
              _rows([10, 11], ["b10", "b11"]))
    before = {task.file.file_path for task in t.scan().plan_files()}
    assert len(before) == 2
    _upsert_in_memory_lookup(t, _rows([0], ["u0"]), HASH, update_matched=True)
    t.refresh()
    after = {task.file.file_path for task in t.scan().plan_files()}
    assert len(before & after) == 1          # exactly one original file kept
    assert _names_by_id(t) == {0: "u0", 1: "a1", 10: "b10", 11: "b11"}


def test_no_change_delta_commits_no_snapshot(tmp_path):
    t = _seed(tmp_path, "nochange", _rows([0, 1], ["seed", "keep"]))
    before = len(list(t.metadata.snapshots))
    _upsert_in_memory_lookup(t, _rows([0, 1], ["seed", "keep"]), HASH,
                             update_matched=True)
    t.refresh()
    assert len(list(t.metadata.snapshots)) == before   # elision: nothing written


def test_insert_only_never_touches_matched_rows(tmp_path):
    t = _seed(tmp_path, "insonly", _rows([0], ["seed"]))
    _upsert_in_memory_lookup(t, _rows([0, 5], ["changed", "new"]), HASH,
                             update_matched=False)
    t.refresh()
    assert _names_by_id(t) == {0: "seed", 5: "new"}


def test_first_merge_into_empty_table_inserts_everything(tmp_path):
    cat = _cat(tmp_path, "empty")
    delta = _rows([1, 2], ["a", "b"])
    t = cat.create_table("oasis.m_empty", schema=delta.schema)   # no snapshot yet
    _upsert_in_memory_lookup(t, delta, HASH, update_matched=True)
    t.refresh()
    assert _names_by_id(t) == {1: "a", 2: "b"}


def test_duplicate_delta_keys_raise_like_pyiceberg(tmp_path):
    t = _seed(tmp_path, "dupdelta", _rows([0], ["seed"]))
    dup = pa.concat_tables([_rows([1], ["x"]), _rows([1], ["y"])])
    with pytest.raises(ValueError, match="Duplicate rows found"):
        _upsert_in_memory_lookup(t, dup, HASH, update_matched=True)


def test_duplicate_stored_keys_abort_the_upsert(tmp_path):
    # Corrupt stored data (same key twice) must abort, mirroring pyiceberg's
    # get_rows_to_update guard, instead of silently double-matching.
    t = _seed(tmp_path, "dupstored", _rows([0], ["seed"]), _rows([0], ["seed2"]))
    with pytest.raises(ValueError, match="duplicate"):
        _upsert_in_memory_lookup(t, _rows([0], ["u0"]), HASH, update_matched=True)
