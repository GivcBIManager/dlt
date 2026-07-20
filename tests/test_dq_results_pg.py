from etl import dq_check
from etl.config import Settings


def test_write_results_postgres(pg_meta):
    from etl.dq_check import DqResult
    r = DqResult(table="customers", source_table="OASIS.CUSTOMERS", branch="1")
    name = dq_check.write_results_postgres([r], Settings(), "dq-1", store=pg_meta)
    assert name == "etl_dq_results"
    with pg_meta.engine.connect() as conn:
        from sqlalchemy import select, func
        n = conn.execute(select(func.count()).select_from(pg_meta.etl_dq_results)).scalar()
    assert n == 1
