from sqlalchemy import inspect


def test_ensure_schema_creates_all_tables(pg_meta):
    insp = inspect(pg_meta.engine)
    tables = set(insp.get_table_names(schema=pg_meta.cfg.schema))
    assert {"control_state", "etl_control", "etl_run_log", "etl_dq_results"} <= tables


def test_ensure_schema_is_idempotent(pg_meta):
    pg_meta.ensure_schema()  # second call must not raise


def test_control_state_upsert_and_read(pg_meta):
    pg_meta.upsert_control_state([{
        "table_name": "customers", "branch_id": "1",
        "last_cdc_value": "100", "last_cdc_kind": "number",
        "last_date_value": None, "last_date_kind": "datetime",
        "status": "SUCCESS", "row_count": 5, "duration_ms": 12,
        "last_run_at": "2026-07-20T10:00:00",
    }])
    # second upsert on same key updates in place (no duplicate)
    pg_meta.upsert_control_state([{
        "table_name": "customers", "branch_id": "1",
        "last_cdc_value": "200", "last_cdc_kind": "number",
        "last_date_value": None, "last_date_kind": "datetime",
        "status": "SUCCESS", "row_count": 9, "duration_ms": 20,
        "last_run_at": "2026-07-20T11:00:00",
    }])
    rows = pg_meta.read_control_state()
    assert len(rows) == 1
    assert rows[0]["last_cdc_value"] == "200"
    assert rows[0]["row_count"] == 9


def test_append_run_log_accumulates(pg_meta):
    pg_meta.append_run_log([{"pipeline_run_id": "r1", "table_name": "t",
                             "branch_id": "1", "status": "SUCCESS"}])
    pg_meta.append_run_log([{"pipeline_run_id": "r1", "table_name": "t",
                             "branch_id": "2", "status": "SUCCESS"}])
    with pg_meta.engine.connect() as conn:
        from sqlalchemy import select, func
        n = conn.execute(select(func.count()).select_from(pg_meta.etl_run_log)).scalar()
    assert n == 2
