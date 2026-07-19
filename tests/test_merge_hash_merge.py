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


import pyarrow.compute as pc
from etl.iceberg_load import (_merge_join_cols, _merge_iceberg_single_commit,
                              _append_merge_hash)


def _schema_dict():
    return {
        "x-merge-strategy": "upsert",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True},
            "branch_id": {"name": "branch_id", "data_type": "bigint", "primary_key": True},
            "name": {"name": "name", "data_type": "text"},
        },
    }


def _rows(ids, names, branch=1, with_hash=False):
    t = pa.table({"id": pa.array(ids, pa.int64()),
                  "name": pa.array(names),
                  "branch_id": pa.array([branch] * len(ids), pa.int64())})
    return _append_merge_hash(t, ["id", "branch_id"], "merge_hash") if with_hash else t


def test_join_cols_picks_hash_only_when_both_sides_have_it(tmp_path):
    cat = _cat(tmp_path, "jc")
    ready = _rows([1], ["a"], with_hash=True)
    t = cat.create_table("oasis.jc", schema=ready.schema)
    t.append(ready)
    assert _merge_join_cols(t, _rows([2], ["b"], with_hash=True),
                            ["id", "branch_id"], "merge_hash") == ["merge_hash"]
    assert _merge_join_cols(t, _rows([2], ["b"], with_hash=False),
                            ["id", "branch_id"], "merge_hash") == ["id", "branch_id"]


def _seed(tmp_path, tag, with_hash):
    cat = _cat(tmp_path, tag)
    seed = _rows([0], ["seed"], with_hash=with_hash)
    t = cat.create_table(f"oasis.m_{tag}", schema=seed.schema)
    t.append(seed)
    return t


def test_hash_ready_merge_updates_and_inserts(tmp_path):
    t = _seed(tmp_path, "hm", with_hash=True)
    before = len(list(t.metadata.snapshots))
    _merge_iceberg_single_commit(t, _rows([0, 1], ["u0", "n1"], with_hash=True),
                                 _schema_dict(), "m")
    t.refresh()
    got = t.scan().to_arrow()
    assert got.num_rows == 2
    assert got.filter(pc.equal(got["id"], 0)).to_pydict()["name"] == ["u0"]   # updated
    # pyiceberg's Table.upsert() always stages DELETE+APPEND for matched-row
    # updates plus a separate APPEND for newly-inserted rows -- verified empirically
    # identical (3-snapshot) for both the composite-key and merge_hash join, since
    # the split is Transaction.upsert()'s internal structure, not a function of
    # which columns are in join_cols. Same bound as test_merge_single_commit.py's
    # test_merge_commit_count_is_independent_of_delta_size (`<= 3`, unchanged by
    # this task): ONE physical table.upsert() call/metadata-swap, not literally
    # one snapshot.
    assert len(list(t.metadata.snapshots)) - before <= 3                      # single physical commit


def test_not_ready_merge_falls_back_and_adds_no_hash(tmp_path):
    t = _seed(tmp_path, "cm", with_hash=False)
    _merge_iceberg_single_commit(t, _rows([0, 1], ["u0", "n1"], with_hash=False),
                                 _schema_dict(), "m")
    t.refresh()
    assert "merge_hash" not in {f.name for f in t.schema().fields}   # stayed not-ready
    assert t.scan().to_arrow().num_rows == 2


def test_carry_forward_preserves_insert_at_via_hash(tmp_path):
    # existing row keyed by hash with an OLD insert_at; batch re-loads same key
    from etl.iceberg_load import _finish_batch
    existing = _append_merge_hash(
        pa.table({"id": pa.array([5], pa.int64()),
                  "branch_id": pa.array([1], pa.int64())}),
        ["id", "branch_id"], "merge_hash").append_column(
            "insert_at", pa.array([pa.scalar("2020-01-01")], pa.string()))
    existing = existing.select(["merge_hash", "insert_at"])

    schema = pa.schema([("id", pa.int64()), ("branch_id", pa.int64()),
                        ("insert_at", pa.string())])
    batch = pa.table({"id": pa.array([5], pa.int64()),
                      "branch_id": pa.array([1], pa.int64()),
                      "insert_at": pa.array(["2026-07-19"], pa.string())})
    out = _finish_batch(batch, schema, existing_insert_at=existing,
                        insert_col="insert_at", write_hash=True,
                        hash_key_cols=["id", "branch_id"], hash_col="merge_hash",
                        carry_keys=["merge_hash"])
    assert out.column("insert_at").to_pylist() == ["2020-01-01"]   # old value kept


def test_join_cols_stays_composite_when_stored_table_lacks_hash(tmp_path):
    cat = _cat(tmp_path, "jc_stored")
    plain = _rows([1], ["a"], with_hash=False)          # stored table: no merge_hash
    t = cat.create_table("oasis.jc_stored", schema=plain.schema)
    t.append(plain)
    assert _merge_join_cols(t, _rows([2], ["b"], with_hash=True),
                            ["id", "branch_id"], "merge_hash") == ["id", "branch_id"]
