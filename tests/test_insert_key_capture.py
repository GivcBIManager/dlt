"""last_key: captured from the staged parquet, advanced, and persisted."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from etl import oracle_extract
from etl.config import CATEGORY_TRANSACTION, MODE_INCREMENTAL, BranchConfig, HelperJoin, Settings, TableDef
from etl.iceberg_load import ControlStore
from etl.oracle_extract import ExtractResult, Watermark, _key_watermark_from_parquet, extract_table

HELPER = HelperJoin(table="OASIS.DELIVERY_LINES", join_keys=(("DELIVERY_LINE", "DELIVERY_LINE"),),
                    cdc_column="AMEND_LAST_DATE", where_date_column="AMEND_LAST_DATE")


def _dc(key="DELIVERY_CHARGE_ID") -> TableDef:
    return TableDef(table="OASIS.DELIVERY_CHARGE", unique_key="DELIVERY_CHARGE_ID",
                    cdc_column=None, where_date_column="DELIVERY_DATE", where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_TRANSACTION,
                    helper=HELPER, insert_key_column=key, insert_key_lookback=1000 if key else 0)


def _staged(tmp_path, ids):
    path = tmp_path / "Muhayil.parquet"
    # Oracle NUMBER arrives as Arrow double.
    pq.write_table(pa.table({"DELIVERY_CHARGE_ID": pa.array(ids, pa.float64())}), path)
    return path


def test_key_mark_is_the_max_of_the_staged_key(tmp_path):
    wm = _key_watermark_from_parquet(_staged(tmp_path, [1492547.0, 1505160.0]), _dc())
    assert (wm.value, wm.kind) == ("1505160.0", "number")


def test_no_insert_key_or_missing_file_gives_no_mark(tmp_path):
    assert _key_watermark_from_parquet(_staged(tmp_path, [1.0]), _dc(key=None)).value is None
    assert _key_watermark_from_parquet(tmp_path / "absent.parquet", _dc()).value is None


class _Conn:
    def close(self):
        pass


class _Pool:
    def acquire(self):
        return _Conn()


def test_extract_table_uses_and_captures_last_key(tmp_path, monkeypatch):
    staged = _staged(tmp_path, [1505160.0])
    seen = {}

    def fake_fetch(conn, query, *a, **k):
        seen["query"] = query
        return 1, None, staged

    monkeypatch.setattr(oracle_extract, "fetch_and_stage", fake_fetch)
    branch = BranchConfig(key="Muhayil", name="m", id=9, host="h", port=1,
                          username="u", password="p", database="d")
    wms = {"last_cdc": {"value": "2026-09-28 05:27:09.000000", "kind": "datetime"},
           "last_date": {"value": "2026-09-28 05:27:09.000000", "kind": "datetime"},
           "last_key": {"value": "1500000.0", "kind": "number"}}
    res = extract_table(_Pool(), branch, _dc(), Settings(mode=MODE_INCREMENTAL), wms)
    assert "t.DELIVERY_CHARGE_ID > 1499000.0" in seen["query"]
    assert res.new_key.value == "1505160.0"


class _FakeMeta:
    def __init__(self, rows=None):
        self.rows = rows or []

    def ensure_schema(self):
        pass

    def read_control_state(self):
        return self.rows

    def upsert_control_state(self, rows):
        self.rows = rows


def test_control_store_round_trips_last_key():
    meta = _FakeMeta()
    cs = ControlStore(meta).load()
    cs.advance(ExtractResult(table_def=_dc(), branch="Muhayil", branch_id=9, status="SUCCESS",
                             new_key=Watermark(value="1505160.0", kind="number")))
    cs.save()
    (row,) = meta.rows
    assert (row["last_key_value"], row["last_key_kind"]) == ("1505160.0", "number")
    again = ControlStore(meta).load()
    assert again.entry("delivery_charge", "Muhayil")["last_key"] == {
        "value": "1505160.0", "kind": "number"}


def test_control_store_loads_rows_written_before_the_column_existed():
    # Older deployments' rows have NULL last_key_* -> no key mark.
    meta = _FakeMeta([{"table_name": "doc", "branch_id": "abha", "last_cdc_value": None,
                       "last_cdc_kind": None, "last_date_value": None, "last_date_kind": None,
                       "last_key_value": None, "last_key_kind": None, "status": "SUCCESS",
                       "row_count": 1, "duration_ms": 1, "last_run_at": None}])
    assert ControlStore(meta).load().entry("doc", "abha")["last_key"] is None
