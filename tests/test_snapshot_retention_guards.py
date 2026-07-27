"""apply_snapshot_retention makes zero commits when there is nothing to do."""
from __future__ import annotations

import time

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from etl import iceberg_load
from etl.config import Settings


def _rows(offset: int) -> pa.Table:
    return pa.table({
        "id": pa.array([offset, offset + 1], pa.int64()),
        "name": pa.array([f"a{offset}", f"b{offset}"]),
    })


@pytest.fixture
def table(tmp_path):
    """Real Iceberg table: 2 appends + 1 overwrite -> 3 snapshots."""
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("oasis")
    tbl = catalog.create_table(
        "oasis.foo", schema=_rows(0).schema,
        location=(tmp_path / "lake" / "foo").as_uri(),
    )
    tbl.append(_rows(0))
    tbl.append(_rows(10))
    tbl.overwrite(_rows(20))
    return tbl


def _retention(monkeypatch, tbl, **settings_kw):
    import dlt.common.libs.pyiceberg as ice

    monkeypatch.setattr(ice, "get_iceberg_tables", lambda pipeline: {"foo": tbl})
    iceberg_load.apply_snapshot_retention(object(), Settings(**settings_kw))
    tbl.refresh()


def _metadata_files(tmp_path) -> int:
    """Every commit writes a new metadata.json; the count is the commit count."""
    return len(list((tmp_path / "lake" / "foo" / "metadata").glob("*.metadata.json")))


def test_first_run_sets_properties_once(tmp_path, table, monkeypatch):
    _retention(monkeypatch, table)
    assert table.properties["history.expire.max-snapshot-age-ms"] == str(
        7 * 24 * 60 * 60 * 1000)
    assert table.properties["history.expire.min-snapshots-to-keep"] == "1"
    assert table.properties["write.metadata.delete-after-commit.enabled"] == "true"
    assert table.properties["write.metadata.previous-versions-max"] == "25"


def test_steady_state_run_commits_nothing(tmp_path, table, monkeypatch):
    _retention(monkeypatch, table)               # first run: property commit
    before = _metadata_files(tmp_path)
    _retention(monkeypatch, table)               # steady state: all guards hit
    assert _metadata_files(tmp_path) == before   # ZERO new commits


def test_old_snapshots_still_expire(tmp_path, table, monkeypatch):
    assert len(table.metadata.snapshots) > 1
    time.sleep(0.05)      # make existing snapshots strictly older than cutoff=now
    _retention(monkeypatch, table, snapshot_expire_days=0)
    assert len(table.metadata.snapshots) == 1    # only the current ref survives


def test_noop_when_maintenance_disabled(tmp_path, table, monkeypatch):
    before = _metadata_files(tmp_path)
    _retention(monkeypatch, table, snapshot_maintenance=False)
    assert _metadata_files(tmp_path) == before
