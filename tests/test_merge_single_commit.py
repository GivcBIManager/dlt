"""An Iceberg merge commits a CONSTANT number of snapshots, not one per 1,000 rows.

dlt's ``merge_iceberg_table`` upserts the delta in 1,000-row chunks, each chunk
its own set of Iceberg commits -- so commits grow with delta size (O(n/1000)) and
a large delta rewrites the table metadata thousands of times (catastrophically
slow). ``_merge_iceberg_single_commit`` upserts the whole delta in one
``table.upsert`` call: pyiceberg still emits an overwrite (for updated rows) plus
an append (for new rows), so it is a small *constant* number of snapshots
independent of delta size -- and the kept intra-run squash collapses those to one
snapshot per run. It reuses dlt's already-normalized arrow data + primary-key
detection, so naming/typing/merge semantics are unchanged.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from etl.iceberg_load import _install_single_commit_merge, _merge_iceberg_single_commit


def _rows(ids, names, branch=1):
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "name": pa.array(names),
        "branch_id": pa.array([branch] * len(ids), pa.int64()),
    })


def _make_table(tmp_path, tag):
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / f'cat_{tag}.db').as_posix()}",
        warehouse=(tmp_path / f"wh_{tag}").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them.
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("oasis")
    t = catalog.create_table(f"oasis.m_{tag}", schema=_rows([0], ["seed"]).schema)
    t.append(_rows([0], ["seed"]))   # one existing row (id=0) + baseline snapshot
    return t


def _schema(strategy="upsert"):
    return {
        "x-merge-strategy": strategy,
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True},
            "branch_id": {"name": "branch_id", "data_type": "bigint", "primary_key": True},
            "name": {"name": "name", "data_type": "text"},
        },
    }


def _merge_delta(tmp_path, tag, n):
    """Merge an n-row delta (id 0 updates the seed, 1..n-1 insert); return commits."""
    t = _make_table(tmp_path, tag)
    before = len(list(t.metadata.snapshots))
    _merge_iceberg_single_commit(
        t, _rows(list(range(n)), [f"v{i}" for i in range(n)]), _schema(), "m")
    t.refresh()
    return t, len(list(t.metadata.snapshots)) - before


def test_merge_commit_count_is_independent_of_delta_size(tmp_path):
    # Same update/insert shape, wildly different sizes -> same commit count.
    _, small = _merge_delta(tmp_path, "small", n=50)      # < 1 dlt chunk
    _, large = _merge_delta(tmp_path, "large", n=2500)     # 3 dlt chunks
    assert large == small           # O(1) in delta size, NOT ceil(n/1000)
    assert large <= 3               # bounded: overwrite (=delete+append) + insert-append


def test_merge_upserts_correctly(tmp_path):
    t, _ = _merge_delta(tmp_path, "correct", n=2500)
    got = t.scan().to_arrow()
    assert got.num_rows == 2500                          # id 0 updated in place, 1..2499 inserted
    row0 = got.filter(pc.equal(got["id"], 0)).to_pydict()
    assert row0["name"] == ["v0"]                        # existing row updated, not the seed


def test_insert_only_strategy_appends_without_updating(tmp_path):
    t = _make_table(tmp_path, "insonly")
    _merge_iceberg_single_commit(
        t, _rows([0, 1, 2], ["ignored", "b", "c"]), _schema("insert-only"), "m")
    t.refresh()
    got = t.scan().to_arrow()
    row0 = got.filter(pc.equal(got["id"], 0)).to_pydict()
    assert row0["name"] == ["seed"]                      # untouched by insert-only
    assert got.num_rows == 3                             # ids 1 and 2 inserted


def test_unsupported_strategy_raises(tmp_path):
    t = _make_table(tmp_path, "bad")
    with pytest.raises(ValueError, match="not supported"):
        _merge_iceberg_single_commit(t, _rows([1], ["x"]), _schema("delete"), "m")


def test_installer_replaces_dlt_merge_idempotently():
    import dlt.common.libs.pyiceberg as ice
    original = ice.merge_iceberg_table
    try:
        _install_single_commit_merge()
        assert ice.merge_iceberg_table is _merge_iceberg_single_commit
        _install_single_commit_merge()  # second call is a no-op
        assert ice.merge_iceberg_table is _merge_iceberg_single_commit
    finally:
        ice.merge_iceberg_table = original
