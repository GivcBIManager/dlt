"""Cursor-fallback fetch streams to parquet instead of materializing all rows.

The fallback path used to build every column as a Python list before writing one
parquet, which OOMs on the large tables it is most likely to trip on. It now
streams fetchmany chunks straight to a ParquetWriter.
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq

from etl import config as cfg
from etl import oracle_extract as ox

NOW = dt.datetime(2026, 7, 6, 12, 0, 0)


def _tdef():
    return cfg.TableDef(
        table="OASIS.T", unique_key="ID", cdc_column=None, where_date_column=None,
        where_operator=None, where_value_of_initial_run=None,
        category=cfg.CATEGORY_MASTER)


class _FakeCursor:
    arraysize = 0
    prefetchrows = 0

    def execute(self, q):
        self.q = q

    def close(self):
        pass


class _FakeConn:
    def cursor(self):
        return _FakeCursor()


# --- streaming stage ------------------------------------------------------- #
def test_stage_via_cursor_streams_all_batches(monkeypatch, tmp_path):
    settings = cfg.Settings(staging_dir=tmp_path)
    batches = [pa.table({"ID": pa.array([1, 2], pa.int64())}),
               pa.table({"ID": pa.array([3], pa.int64())})]
    monkeypatch.setattr(ox, "_cursor_arrow_stream", lambda cur, size: iter(batches))

    rc, schema, path = ox._stage_via_cursor(
        _FakeConn(), "q", _tdef(), "b1", 7, settings, 100, NOW)

    assert rc == 3
    tbl = pq.read_table(path)
    assert tbl.num_rows == 3
    assert tbl.column("ID").to_pylist() == [1, 2, 3]
    # injected constant column present and stamped with the branch id
    assert set(tbl.column(settings.branch_id_column).to_pylist()) == {7}


def test_stage_via_cursor_empty_keeps_schema(monkeypatch, tmp_path):
    settings = cfg.Settings(staging_dir=tmp_path)
    empty = pa.table({"ID": pa.array([], pa.int64())})
    monkeypatch.setattr(ox, "_cursor_arrow_stream", lambda cur, size: iter([empty]))

    rc, schema, path = ox._stage_via_cursor(
        _FakeConn(), "q", _tdef(), "b1", 1, settings, 100, NOW)

    assert rc == 0
    tbl = pq.read_table(path)
    assert tbl.num_rows == 0
    assert "ID" in tbl.column_names
    assert settings.branch_id_column in tbl.column_names


# --- the per-chunk generator ---------------------------------------------- #
class _Desc:
    def __init__(self, name):
        self.name = name


def _patch_types(monkeypatch):
    monkeypatch.setattr(ox.types_map, "oracle_field_to_arrow", lambda d: pa.int64())
    monkeypatch.setattr(ox.types_map, "build_arrow_column",
                        lambda vals, t: pa.array(list(vals), t))


def test_cursor_arrow_stream_yields_one_table_per_chunk(monkeypatch):
    _patch_types(monkeypatch)

    class Cur:
        description = [_Desc("ID")]

        def __init__(self):
            self._chunks = [[(1,), (2,)], [(3,)], []]
            self.i = 0

        def fetchmany(self, n):
            c = self._chunks[self.i]
            self.i += 1
            return c

    tables = list(ox._cursor_arrow_stream(Cur(), 2))
    assert len(tables) == 2
    assert tables[0].column("ID").to_pylist() == [1, 2]
    assert tables[1].column("ID").to_pylist() == [3]


def test_cursor_arrow_stream_empty_emits_schema(monkeypatch):
    _patch_types(monkeypatch)

    class Cur:
        description = [_Desc("ID")]

        def fetchmany(self, n):
            return []

    tables = list(ox._cursor_arrow_stream(Cur(), 2))
    assert len(tables) == 1
    assert tables[0].num_rows == 0
    assert tables[0].column("ID").type == pa.int64()
