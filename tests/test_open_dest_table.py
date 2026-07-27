"""_open_dest_table: catalog-backed destination reads, independent of any dlt
pipeline's local schema. Best-effort: absent table or broken catalog -> None."""
from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from etl import iceberg_load
from etl.config import Settings


def _cat(tmp_path):
    cat = SqlCatalog(
        "t", uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    return cat


def test_open_dest_table_loads_registered_table(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    cat.create_table("oasis.foo", schema=rows.schema).append(rows)
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    tbl = iceberg_load._open_dest_table(Settings(), "foo")

    assert tbl is not None
    assert {f.name for f in tbl.schema().fields} == {"id"}


def test_open_dest_table_none_when_absent(tmp_path, monkeypatch):
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog",
                        lambda: _cat(tmp_path))
    assert iceberg_load._open_dest_table(Settings(), "nope") is None


def test_open_dest_table_none_on_catalog_failure(monkeypatch):
    def boom():
        raise RuntimeError("catalog down")

    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", boom)
    assert iceberg_load._open_dest_table(Settings(), "foo") is None


def test_open_dest_table_asks_for_dataset_qualified_identifier(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    cat.create_table("oasis.bar", schema=rows.schema)
    asked = []
    real = cat.load_table

    def spy(ident):
        asked.append(ident)
        return real(ident)

    monkeypatch.setattr(cat, "load_table", spy)
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    assert iceberg_load._open_dest_table(Settings(), "bar") is not None
    assert asked == ["oasis.bar"]
