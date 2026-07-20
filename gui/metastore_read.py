"""GUI read access to the Postgres app metastore (etl_meta.*).

The three observability tables (``etl_control``, ``etl_run_log``,
``etl_dq_results``) and the CDC watermark store (``control_state``) now live in
Postgres (Tasks 5-7). The GUI reads them through this helper instead of Iceberg;
arbitrary lake data tables are still browsed via Iceberg through the Postgres
catalog (see ``iceberg_browser``), unchanged.
"""
from __future__ import annotations

import functools

from etl import config
from etl.metastore import MetaStore


@functools.lru_cache(maxsize=1)
def open_metastore() -> MetaStore:
    cfg = config.load_postgres_config()
    if cfg is None:
        raise RuntimeError("no [postgres] config found in .dlt/secrets.toml")
    store = MetaStore(cfg)
    store.ensure_schema()
    return store


def read_table_rows(table_name: str) -> list[dict]:
    store = open_metastore()
    table = {"etl_control": store.etl_control, "etl_run_log": store.etl_run_log,
             "etl_dq_results": store.etl_dq_results,
             "control_state": store.control_state}[table_name]
    from sqlalchemy import select
    with store.engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(select(table))]
