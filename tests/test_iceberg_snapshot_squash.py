"""Intra-run snapshot squash: each table keeps 1 snapshot per pipeline run.

The loader writes one branch per ``pipeline.run`` (memory bound), so a run
commits one Iceberg snapshot per branch. ``_squash_run_snapshots`` expires the
intermediate snapshots created during the run, keeping only the final one --
history stays 1 snapshot per run instead of 1 per branch per run.
"""
from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from etl.iceberg_load import _squash_run_snapshots


def _rows(offset: int) -> pa.Table:
    return pa.table({
        "id": pa.array([offset, offset + 1], pa.int64()),
        "name": pa.array([f"a{offset}", f"b{offset}"]),
    })


@pytest.fixture
def table(tmp_path):
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("oasis")
    return catalog.create_table("oasis.appt", schema=_rows(0).schema)


def _ids(tbl) -> set[int]:
    return {s.snapshot_id for s in tbl.metadata.snapshots}


def test_squash_keeps_final_snapshot_and_prior_history(table):
    table.append(_rows(0))          # previous run's surviving snapshot
    before = _ids(table)
    table.append(_rows(10))         # branch 1
    table.append(_rows(20))         # branch 2
    table.append(_rows(30))         # branch 3 (final)
    current = table.metadata.current_snapshot_id

    expired = _squash_run_snapshots(table, before)

    assert expired == 2             # the two intra-run intermediates
    assert _ids(table) == before | {current}
    assert table.metadata.current_snapshot_id == current
    # all branches' rows still readable from the surviving snapshot
    assert table.scan().to_arrow().num_rows == 8


def test_squash_first_run_keeps_only_current(table):
    before = _ids(table)            # empty: table just created
    table.append(_rows(0))
    table.append(_rows(10))
    current = table.metadata.current_snapshot_id

    expired = _squash_run_snapshots(table, before)

    assert expired == 1
    assert _ids(table) == {current}


def test_squash_single_commit_is_noop(table):
    table.append(_rows(0))
    before = _ids(table)
    table.append(_rows(10))
    snaps_before = list(table.metadata.snapshots)

    assert _squash_run_snapshots(table, before) == 0
    assert list(table.metadata.snapshots) == snaps_before
