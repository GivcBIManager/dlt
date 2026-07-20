"""Postgres app metastore: watermarks + observability + DQ.

Owns a SQLAlchemy engine to the ``oasis_meta`` database and four tables under
``PostgresConfig.schema`` (default ``etl_meta``). Naive local wall-clock times
are stored as ``TIMESTAMP WITHOUT TIME ZONE`` (no timezone tagging needed here,
unlike Iceberg). All DDL is idempotent (CREATE ... IF NOT EXISTS via checkfirst).
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import (BigInteger, Column, Float, MetaData, String, Table,
                        TIMESTAMP, create_engine, text)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from .config import PostgresConfig

log = logging.getLogger("etl.metastore")


def _control_state_table(md: MetaData, schema: str) -> Table:
    return Table(
        "control_state", md,
        Column("table_name", String, primary_key=True),
        Column("branch_id", String, primary_key=True),
        Column("last_cdc_value", String), Column("last_cdc_kind", String),
        Column("last_date_value", String), Column("last_date_kind", String),
        Column("status", String), Column("row_count", BigInteger),
        Column("duration_ms", BigInteger), Column("last_run_at", String),
        schema=schema,
    )


def _etl_control_table(md: MetaData, schema: str) -> Table:
    return Table(
        "etl_control", md,
        Column("table_name", String, primary_key=True),
        Column("branch_id", String, primary_key=True),
        Column("load_mode", String), Column("status", String),
        Column("row_count", BigInteger), Column("attempts", BigInteger),
        Column("last_cdc_value", String), Column("last_cdc_kind", String),
        Column("last_date_value", String), Column("last_date_kind", String),
        Column("duration_ms", BigInteger),
        Column("start_time", TIMESTAMP(timezone=False)),
        Column("end_time", TIMESTAMP(timezone=False)),
        Column("error_details", String), Column("pipeline_run_id", String),
        Column("updated_at", TIMESTAMP(timezone=False)),
        schema=schema,
    )


def _etl_run_log_table(md: MetaData, schema: str) -> Table:
    return Table(
        "etl_run_log", md,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("pipeline_run_id", String), Column("table_name", String),
        Column("branch_id", String), Column("load_mode", String),
        Column("row_count", BigInteger),
        Column("start_time", TIMESTAMP(timezone=False)),
        Column("end_time", TIMESTAMP(timezone=False)),
        Column("duration_ms", BigInteger), Column("status", String),
        Column("attempts", BigInteger), Column("write_disposition", String),
        Column("load_status", String), Column("error_details", String),
        Column("schema_discrepancy", String),
        Column("recorded_at", TIMESTAMP(timezone=False)),
        schema=schema,
    )


def _etl_dq_results_table(md: MetaData, schema: str) -> Table:
    return Table(
        "etl_dq_results", md,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("check_time", TIMESTAMP(timezone=False)),
        Column("pipeline_run_id", String), Column("table_name", String),
        Column("source_table", String), Column("branch_id", String),
        Column("date_column", String), Column("window_start", String),
        Column("window_end", String), Column("window_note", String),
        Column("oracle_row_count", BigInteger), Column("iceberg_row_count", BigInteger),
        Column("row_count_delta", BigInteger), Column("hash_columns", BigInteger),
        Column("oracle_hashed_rows", BigInteger), Column("iceberg_hashed_rows", BigInteger),
        Column("hash_matched", BigInteger), Column("hash_only_in_oracle", BigInteger),
        Column("hash_only_in_iceberg", BigInteger), Column("hash_mismatch", BigInteger),
        Column("hash_total_delta", BigInteger), Column("hash_delta_pct", Float),
        Column("columns_only_in_oracle", String), Column("columns_only_in_iceberg", String),
        Column("status", String), Column("error_details", String),
        schema=schema,
    )


class MetaStore:
    """Handle to the Postgres app metastore. Cheap to construct; connects lazily."""

    def __init__(self, cfg: PostgresConfig) -> None:
        self.cfg = cfg
        self.engine: Engine = create_engine(cfg.sqlalchemy_url(), pool_pre_ping=True)
        self.md = MetaData()
        self.control_state = _control_state_table(self.md, cfg.schema)
        self.etl_control = _etl_control_table(self.md, cfg.schema)
        self.etl_run_log = _etl_run_log_table(self.md, cfg.schema)
        self.etl_dq_results = _etl_dq_results_table(self.md, cfg.schema)

    def ensure_schema(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{self.cfg.schema}"'))
        self.md.create_all(self.engine, checkfirst=True)
        log.info("metastore schema '%s' ready", self.cfg.schema)
