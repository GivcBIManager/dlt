"""Best-effort deletion of a branch's staged parquet after it is committed."""
from __future__ import annotations

from types import SimpleNamespace

from etl import iceberg_load
from etl.config import Settings


def _result(path):
    # _cleanup_staged is duck-typed on .staged_path and .table.
    return SimpleNamespace(staged_path=path, table="FOO")


def _staged(dir_, branch="b1"):
    tbl_dir = dir_ / "FOO"
    tbl_dir.mkdir(parents=True, exist_ok=True)
    p = tbl_dir / f"{branch}.parquet"
    p.write_bytes(b"parquet")
    return p


def test_deletes_file_when_enabled(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings())
    assert not p.exists()


def test_removes_empty_table_dir(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings())
    assert not p.parent.exists()


def test_keeps_dir_with_other_branch(tmp_path):
    p1 = _staged(tmp_path, "b1")
    p2 = _staged(tmp_path, "b2")
    iceberg_load._cleanup_staged(_result(p1), Settings())
    assert not p1.exists()
    assert p2.exists()            # sibling untouched
    assert p2.parent.exists()     # dir kept — still has b2


def test_noop_when_disabled(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings(cleanup_staging_after_load=False))
    assert p.exists()


def test_tolerates_missing_file(tmp_path):
    p = tmp_path / "FOO" / "gone.parquet"   # never created
    iceberg_load._cleanup_staged(_result(p), Settings())  # must not raise


def test_noop_when_path_none():
    iceberg_load._cleanup_staged(_result(None), Settings())  # must not raise
