"""Snapshot expiry + orphan cleanup for staging Iceberg tables."""
from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog


def _rows(offset: int) -> pa.Table:
    return pa.table({
        "id": pa.array([offset, offset + 1], pa.int64()),
        "name": pa.array([f"a{offset}", f"b{offset}"]),
    })


@pytest.fixture
def lake(tmp_path, monkeypatch):
    import iceberg_maintenance as im

    root = tmp_path / "oasis"
    root.mkdir()
    monkeypatch.setattr(im, "ICEBERG_ROOT", root)
    return root


@pytest.fixture
def table(lake, tmp_path, monkeypatch):
    """Real Iceberg table under the fake lake: 2 appends + 1 overwrite.

    The overwrite makes the appends' parquet files unreferenced once their
    snapshots are expired — that's what proves orphan cleanup frees data files.
    """
    import iceberg_maintenance as im

    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("oasis")
    tbl = catalog.create_table(
        "oasis.patient_ad", schema=_rows(0).schema,
        location=(lake / "patient_ad").as_uri(),
    )
    tbl.append(_rows(0))
    tbl.append(_rows(10))
    tbl.overwrite(_rows(20))
    monkeypatch.setattr(im, "_writable_table", lambda name: tbl)
    return tbl


def test_expire_keeps_only_current_snapshot(lake, table):
    import iceberg_maintenance as im

    before = len(table.metadata.snapshots)
    assert before > 1
    current = table.metadata.current_snapshot_id

    out = im.expire_snapshots("patient_ad")

    assert out["table"] == "patient_ad"
    assert out["expired"] == before - 1
    assert out["remaining"] == 1
    assert out["errors"] == {}
    snaps = table.metadata.snapshots
    assert [s.snapshot_id for s in snaps] == [current]


def test_expire_deletes_orphans_keeps_referenced(lake, table):
    import iceberg_maintenance as im

    data_dir = lake / "patient_ad" / "data"
    meta_dir = lake / "patient_ad" / "metadata"
    parquet_before = len(list(data_dir.rglob("*.parquet")))
    metadata_json_before = len(list(meta_dir.glob("*.metadata.json")))
    assert parquet_before == 3  # 2 appends + 1 overwrite

    out = im.expire_snapshots("patient_ad")

    # the appends' parquet files are orphaned by the overwrite + expiry
    assert len(list(data_dir.rglob("*.parquet"))) == 1
    assert out["orphans_deleted"] > 0
    assert out["bytes_freed"] > 0
    # every *.metadata.json survives; the remaining snapshot's files survive
    assert len(list(meta_dir.glob("*.metadata.json"))) >= metadata_json_before
    snap = table.metadata.snapshots[0]
    manifest_list_name = snap.manifest_list.rsplit("/", 1)[-1]
    assert (meta_dir / manifest_list_name).exists()
    # current data still readable and correct
    got = table.scan().to_arrow().sort_by("id")
    assert got.column("id").to_pylist() == [20, 21]


def test_expire_is_idempotent(lake, table):
    import iceberg_maintenance as im

    im.expire_snapshots("patient_ad")
    out = im.expire_snapshots("patient_ad")
    assert out["expired"] == 0
    assert out["orphans_deleted"] == 0
    assert out["remaining"] == 1


@pytest.mark.parametrize("bad", ["_dlt_loads", "..", "a/b", "a\\b", ""])
def test_expire_rejects_protected_and_unsafe_names(lake, bad):
    import iceberg_maintenance as im

    with pytest.raises(ValueError):
        im.expire_snapshots(bad)


def test_expire_unknown_table_is_not_found(lake):
    import iceberg_maintenance as im

    with pytest.raises(FileNotFoundError):
        im.expire_snapshots("nope")
