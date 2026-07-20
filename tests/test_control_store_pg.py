from etl.config import CATEGORY_MASTER, TableDef
from etl.iceberg_load import ControlStore
from etl.oracle_extract import ExtractResult, Watermark


def _tdef(table: str) -> TableDef:
    # ExtractResult.table is a derived property (table_def.dataset_table_name),
    # so build a minimal TableDef whose normalized name matches ``table``.
    return TableDef(
        table=f"OASIS.{table.upper()}", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _result(table, branch_id, cdc_val):
    return ExtractResult(
        table_def=_tdef(table), branch=str(branch_id), branch_id=branch_id,
        status="SUCCESS", row_count=3, attempts=1, duration_ms=10,
        new_cdc=Watermark(value=cdc_val, kind="number"),
        new_date=Watermark(value=None, kind="datetime"),
        staged_path="x", start_time=None, end_time=None, error=None)


def test_control_store_roundtrip(pg_meta):
    cs = ControlStore(pg_meta)
    cs.load()
    cs.advance(_result("customers", 1, "100"))
    cs.save()

    cs2 = ControlStore(pg_meta).load()
    entry = cs2.entry("customers", "1")
    assert entry["last_cdc"]["value"] == "100"
    assert entry["status"] == "SUCCESS"
