from sqlalchemy import inspect


def test_ensure_schema_creates_all_tables(pg_meta):
    insp = inspect(pg_meta.engine)
    tables = set(insp.get_table_names(schema=pg_meta.cfg.schema))
    assert {"control_state", "etl_control", "etl_run_log", "etl_dq_results"} <= tables


def test_ensure_schema_is_idempotent(pg_meta):
    pg_meta.ensure_schema()  # second call must not raise
