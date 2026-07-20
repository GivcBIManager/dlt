from etl import iceberg_load
from etl.config import Settings, TableDef, CATEGORY_MASTER
from etl.iceberg_load import TableLoadPlan
from etl.oracle_extract import ExtractResult, Watermark


def _tdef() -> TableDef:
    # ExtractResult.table is a derived property (table_def.dataset_table_name),
    # normalizes "OASIS.CUSTOMERS" -> "customers".
    return TableDef(
        table="OASIS.CUSTOMERS", unique_key="ID", cdc_column="UPDATED",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _plan():
    tdef = _tdef()
    r = ExtractResult(table_def=tdef, branch="1", branch_id=1, status="SUCCESS",
                      row_count=3, attempts=1, duration_ms=10,
                      new_cdc=Watermark("100", "number"), new_date=Watermark(None, "datetime"),
                      staged_path="x", start_time=None, end_time=None, error=None)
    p = TableLoadPlan(tdef=tdef, success=[r], failed=[])
    p.disposition = "merge"
    p.load_status = "SUCCESS"
    return p


def test_write_observability_lands_in_postgres(pg_meta):
    settings = Settings()
    iceberg_load._write_observability(pg_meta, [_plan()], settings, "run-xyz")
    with pg_meta.engine.connect() as conn:
        from sqlalchemy import select, func
        ctl = conn.execute(select(func.count()).select_from(pg_meta.etl_control)).scalar()
        log = conn.execute(select(func.count()).select_from(pg_meta.etl_run_log)).scalar()
    assert ctl == 1
    assert log == 1
