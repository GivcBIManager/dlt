from __future__ import annotations
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from etl.iceberg_load import _table_is_hash_ready


class _Tdef:
    dataset_table_name = "m_ready"


def _cat(tmp_path, tag):
    cat = SqlCatalog("t", uri=f"sqlite:///{(tmp_path/f'c_{tag}.db').as_posix()}",
                     warehouse=(tmp_path/f"w_{tag}").as_uri(),
                     **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    return cat


def test_hash_ready_true_only_when_column_present(tmp_path, monkeypatch):
    cat = _cat(tmp_path, "r")
    with_hash = pa.table({"id": pa.array([1], pa.int64()),
                          "merge_hash": pa.array([b"x" * 16], pa.binary())})
    without = pa.table({"id": pa.array([1], pa.int64())})
    t_ready = cat.create_table("oasis.ready", schema=with_hash.schema)
    t_ready.append(with_hash)
    t_plain = cat.create_table("oasis.plain", schema=without.schema)
    t_plain.append(without)

    # The function does a call-time `from dlt.common.libs.pyiceberg import
    # get_iceberg_tables`, so patch the attribute on that source module.
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_iceberg_tables",
                        lambda pipeline, *_names: pipeline)   # pipeline IS the {name: table} map

    assert _table_is_hash_ready({_Tdef.dataset_table_name: t_ready}, _Tdef, "merge_hash") is True
    assert _table_is_hash_ready({_Tdef.dataset_table_name: t_plain}, _Tdef, "merge_hash") is False
    assert _table_is_hash_ready({}, _Tdef, "merge_hash") is False   # table missing
