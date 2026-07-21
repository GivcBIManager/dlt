"""GUI read access to the Postgres app metastore (etl_meta.*).

The three observability tables (``etl_control``, ``etl_run_log``,
``etl_dq_results``) and the CDC watermark store (``control_state``) now live in
Postgres (Tasks 5-7). The GUI reads them through this helper instead of Iceberg;
arbitrary lake data tables are still browsed via Iceberg through the Postgres
catalog (see ``iceberg_browser``), unchanged.
"""
from __future__ import annotations

import functools
import sys
from pathlib import Path

# The GUI runs as `python gui/app.py`, which puts ONLY the gui/ directory on
# sys.path (gui/app.py), not the repo root. This module imports the ``etl``
# package (at the repo root), so ensure the repo root is importable first --
# mirrors gui/iceberg_maintenance.py. Without this, the Postgres-backed
# /api/iceberg/system/* endpoints 500 with "No module named 'etl'".
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from etl import config  # noqa: E402
from etl.metastore import MetaStore  # noqa: E402


@functools.lru_cache(maxsize=1)
def open_metastore() -> MetaStore:
    cfg = config.load_postgres_config()
    if cfg is None:
        raise RuntimeError("no [postgres] config found in .dlt/secrets.toml")
    store = MetaStore(cfg)
    store.ensure_schema()
    return store


def _table(store: MetaStore, table_name: str):
    return {"etl_control": store.etl_control, "etl_run_log": store.etl_run_log,
            "etl_dq_results": store.etl_dq_results,
            "control_state": store.control_state}[table_name]


def read_table_rows(table_name: str, *, equals: tuple[str, object] | None = None,
                     order_by: str | None = None, descending: bool = True,
                     limit: int | None = None) -> list[dict]:
    """Rows of one system table, with the predicate/order/limit pushed into SQL.

    ``equals`` is an ``(column, value)`` equality filter. ``order_by`` sorts by
    one of the table's own columns (silently ignored if it is not a column on
    this table). ``limit`` caps the number of rows returned by Postgres itself.
    """
    store = open_metastore()
    table = _table(store, table_name)
    from sqlalchemy import select
    stmt = select(table)
    if equals is not None:
        col, val = equals
        stmt = stmt.where(table.c[col] == val)
    if order_by is not None and order_by in table.c:
        oc = table.c[order_by]
        stmt = stmt.order_by(oc.desc() if descending else oc.asc())
    if limit is not None:
        stmt = stmt.limit(limit)
    with store.engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(stmt)]


def table_columns(table_name: str) -> list[str]:
    """Column names of one system table, from its schema (no row read needed)."""
    store = open_metastore()
    return list(_table(store, table_name).c.keys())


def count_table_rows(table_name: str) -> int:
    """Full row count of one system table (``SELECT count(*)``, not a Python len())."""
    store = open_metastore()
    table = _table(store, table_name)
    from sqlalchemy import func, select
    with store.engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(table)).scalar()


def read_recent_run_log(limit_runs: int) -> list[dict]:
    """Rows of the newest ``limit_runs`` runs only (bounds the unbounded etl_run_log).

    ``etl_run_log`` grows one row per (table, branch) per run forever, so a bare
    "select *" becomes a full-table scan as the pipeline accumulates history.
    This instead picks the newest ``limit_runs`` ``pipeline_run_id`` values (by
    their run's max ``start_time``) and returns only those runs' rows.
    """
    store = open_metastore()
    t = store.etl_run_log
    from sqlalchemy import func, select
    sub = (select(t.c.pipeline_run_id).group_by(t.c.pipeline_run_id)
           .order_by(func.max(t.c.start_time).desc().nulls_last()).limit(limit_runs))
    with store.engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(select(t).where(t.c.pipeline_run_id.in_(sub)))]
