"""Mid-stream schema drift must widen the staged parquet, not abort the run.

Unconstrained NUMBER columns (typical for query-based/computed sources) carry no
precision metadata, so the Arrow type is inferred per batch from the values.
When the first batch's values are narrow (e.g. all <= 4 digits -> decimal(4,0))
and a later batch is wider (decimal(6,0)), the old code safe-cast the later
batch to the first batch's schema and died with
``ArrowInvalid: Decimal value does not fit in precision 4`` -- aborting the
whole phase. The staging writer must widen instead.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from etl import config as cfg
from etl import oracle_extract as ox

NOW = dt.datetime(2026, 7, 15, 12, 0, 0)


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


def _dec_batch(values: list[str], precision: int, scale: int) -> pa.Table:
    return pa.table({
        "ID": pa.array(range(len(values)), pa.int64()),
        "AMT": pa.array([Decimal(v) for v in values], pa.decimal128(precision, scale)),
    })


def test_stage_via_cursor_widens_decimal_precision_drift(monkeypatch, tmp_path):
    settings = cfg.Settings(staging_dir=tmp_path)
    batches = [_dec_batch(["9999", "123"], 4, 0),      # first batch: narrow inference
               _dec_batch(["123456"], 6, 0)]           # later batch: wider values
    monkeypatch.setattr(ox, "_cursor_arrow_stream", lambda cur, size: iter(batches))

    rc, schema, path = ox._stage_via_cursor(
        _FakeConn(), "q", _tdef(), "b1", 7, settings, 100, NOW)

    assert rc == 3
    tbl = pq.read_table(path)
    assert tbl.num_rows == 3
    assert tbl.column("AMT").to_pylist() == [
        Decimal("9999"), Decimal("123"), Decimal("123456")]
    amt_type = tbl.schema.field("AMT").type
    assert pa.types.is_decimal(amt_type) and amt_type.precision >= 6
    assert schema.equals(tbl.schema)


def test_stage_via_cursor_widens_decimal_scale_drift(monkeypatch, tmp_path):
    settings = cfg.Settings(staging_dir=tmp_path)
    batches = [_dec_batch(["12"], 2, 0),               # integers first
               _dec_batch(["3.75"], 3, 2)]             # fractional values later
    monkeypatch.setattr(ox, "_cursor_arrow_stream", lambda cur, size: iter(batches))

    rc, schema, path = ox._stage_via_cursor(
        _FakeConn(), "q", _tdef(), "b1", 7, settings, 100, NOW)

    assert rc == 2
    tbl = pq.read_table(path)
    assert tbl.column("AMT").to_pylist() == [Decimal("12.00"), Decimal("3.75")]
    amt_type = tbl.schema.field("AMT").type
    assert pa.types.is_decimal(amt_type) and amt_type.scale == 2


def test_stage_via_arrow_widens_decimal_precision_drift(tmp_path):
    settings = cfg.Settings(staging_dir=tmp_path)
    batches = [_dec_batch(["9999"], 4, 0),
               _dec_batch(["123456"], 6, 0)]

    class _ArrowConn:
        def fetch_df_batches(self, query, size):
            return iter(batches)

    rc, schema, path = ox._stage_via_arrow(
        _ArrowConn(), "q", _tdef(), "b1", 7, settings, 100, NOW)

    assert rc == 2
    tbl = pq.read_table(path)
    assert tbl.column("AMT").to_pylist() == [Decimal("9999"), Decimal("123456")]
    amt_type = tbl.schema.field("AMT").type
    assert pa.types.is_decimal(amt_type) and amt_type.precision >= 6
