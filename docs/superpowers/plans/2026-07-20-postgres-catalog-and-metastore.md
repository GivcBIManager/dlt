# Postgres Iceberg Catalog + Postgres Metastore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the Iceberg catalog and all pipeline metadata (watermarks, observability, DQ) into Postgres, leaving Iceberg data files in place.

**Architecture:** Point dlt's filesystem-Iceberg path at a Postgres SQL catalog via `[iceberg_catalog]` config (Approach A — keep the dlt write path). Add an `etl/metastore.py` module that owns a Postgres app database and four tables; rewire `ControlStore`, observability, DQ, and GUI readers to it. Cutover is a fresh full INITIAL — no data migration.

**Tech Stack:** Python 3.10+, dlt 1.28, pyiceberg 0.11 (`SqlCatalog`), SQLAlchemy 2.0 Core, psycopg2, Flask GUI, pytest.

## Global Constraints

- **Two separate Postgres databases:** `oasis_catalog` (Iceberg SQL catalog) and `oasis_meta` (app tables). Same server permitted; config allows different hosts.
- **Keep `control_state` and `etl_control` as two separate tables** — no collapse.
- **Postgres is an external prerequisite** — do NOT script its installation.
- **Iceberg data files stay on the local filesystem** (`iceberg_output`); only catalog pointers move to Postgres.
- **Keep the dlt write path** and all custom load machinery unchanged (single-commit merge patch, per-branch rebuild, snapshot squash, insert_at carry-forward, commit watchdog).
- **Start fresh:** no migration of existing watermarks/tables/history. First run after cutover is `--mode INITIAL`.
- **Generated timestamps are naive local wall-clock** stored as `TIMESTAMP WITHOUT TIME ZONE` (no timezone hint needed in Postgres).
- **`ControlStore` keeps its public surface** (`load`, `entry`, `advance`, `as_dict`, `save`) so pipeline callers barely change.
- Spec: [docs/superpowers/specs/2026-07-20-postgres-catalog-and-metastore-design.md](../specs/2026-07-20-postgres-catalog-and-metastore-design.md).

## File Structure

- Create `etl/metastore.py` — Postgres engine, DDL, typed read/write helpers (one responsibility: the app metastore).
- Modify `etl/config.py` — add `PostgresConfig` + loader, expose on `Settings`, drop `control_state_path`.
- Modify `etl/iceberg_load.py` — `ControlStore` → Postgres; `_write_observability` → metastore.
- Modify `etl/dq_check.py` — `write_results_iceberg` → `write_results_postgres`.
- Modify `dq_check.py`, `oracle_to_iceberg.py` — `ControlStore` construction + DQ writer call.
- Modify `gui/config.py`, `gui/workspace.py`, `gui/iceberg_browser.py` — read the 3 tables + watermarks from Postgres.
- Modify `.dlt/config.toml`, `.dlt/secrets.toml` — `[iceberg_catalog]` + `[postgres]`.
- Modify `requirements.txt`, `requirements-gui.txt`, `fresh_run.sh`, `fresh_run.cmd`, `README.md`.
- Modify `tests/conftest.py` — add an env-gated Postgres fixture. New `tests/test_metastore.py`, `tests/test_control_store_pg.py`, `tests/test_catalog_config.py`.

## Testing Strategy

Metastore tests require a live Postgres. They read a DSN from `OASIS_TEST_PG_DSN` and are **skipped** when it is unset, so the suite still runs in environments without Postgres. Each test runs inside a uniquely-named throwaway schema that the fixture drops on teardown. Non-Postgres logic (config parsing, row builders) is tested with plain unit tests that need no database.

---

### Task 1: Dependencies + Postgres config

**Files:**
- Modify: `requirements.txt`
- Modify: `requirements-gui.txt`
- Modify: `etl/config.py` (add `PostgresConfig`, loader, `Settings.postgres`)
- Test: `tests/test_config_postgres.py` (create)

**Interfaces:**
- Produces: `config.PostgresConfig(host: str, port: int, database: str, username: str, password: str, schema: str = "etl_meta")`; `config.load_postgres_config(raw: dict | None) -> PostgresConfig | None`; `config.PostgresConfig.sqlalchemy_url() -> str`; `Settings.postgres: Optional[PostgresConfig]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_config_postgres.py`:

```python
from etl import config


def test_load_postgres_config_parses_section():
    raw = {"host": "db", "port": 5432, "database": "oasis_meta",
           "username": "u", "password": "p"}
    pg = config.load_postgres_config(raw)
    assert pg.host == "db"
    assert pg.port == 5432
    assert pg.schema == "etl_meta"
    assert pg.sqlalchemy_url() == "postgresql+psycopg2://u:p@db:5432/oasis_meta"


def test_load_postgres_config_none_when_absent():
    assert config.load_postgres_config(None) is None
    assert config.load_postgres_config({}) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config_postgres.py -v`
Expected: FAIL with `AttributeError: module 'etl.config' has no attribute 'load_postgres_config'`

- [ ] **Step 3: Add `PostgresConfig` + loader to `etl/config.py`**

After the `BranchConfig` dataclass (around line 239), add:

```python
@dataclass(frozen=True)
class PostgresConfig:
    """Connection details for the app metastore database (oasis_meta)."""

    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "etl_meta"

    def sqlalchemy_url(self) -> str:
        return (f"postgresql+psycopg2://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}")
```

In the `load_settings` loaders section (near `load_branches`, ~line 445), add:

```python
def load_postgres_config(raw: Optional[dict] = None) -> Optional[PostgresConfig]:
    """Build PostgresConfig from a ``[postgres]`` secrets dict (None if absent)."""
    if raw is None:
        raw = dlt.secrets.get("postgres")
    if not raw:
        return None
    return PostgresConfig(
        host=str(raw["host"]),
        port=int(raw.get("port", 5432)),
        database=str(raw["database"]),
        username=str(raw["username"]),
        password=str(raw["password"]),
        schema=str(raw.get("schema", "etl_meta")),
    )
```

Add `postgres: Optional[PostgresConfig] = None` to the `Settings` dataclass (after `pipeline_name`, ~line 273), and set it in `load_settings` before the `return s` (~line 537):

```python
    s.postgres = load_postgres_config()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_config_postgres.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Update requirements**

In `requirements.txt`, change the pyiceberg line and add psycopg2:

```
pyiceberg[sql-postgres]>=0.11.0   # SQL catalog on Postgres (pulls SQLAlchemy + psycopg2)
psycopg2-binary>=2.9              # metastore driver (etl/metastore.py)
```

In `requirements-gui.txt`, add `psycopg2-binary>=2.9` if not pulled transitively.

- [ ] **Step 6: Commit**

```bash
git add etl/config.py tests/test_config_postgres.py requirements.txt requirements-gui.txt
git commit -m "feat(etl): add PostgresConfig + postgres/pyiceberg deps"
```

---

### Task 2: Metastore engine + DDL

**Files:**
- Create: `etl/metastore.py`
- Modify: `tests/conftest.py` (add `pg_meta` fixture)
- Test: `tests/test_metastore.py` (create)

**Interfaces:**
- Consumes: `config.PostgresConfig` (Task 1).
- Produces: `metastore.MetaStore(cfg: PostgresConfig)`; `MetaStore.ensure_schema() -> None` (idempotent CREATE SCHEMA + 4 tables); `MetaStore.engine` (SQLAlchemy Engine); table objects `MetaStore.control_state`, `.etl_control`, `.etl_run_log`, `.etl_dq_results` (SQLAlchemy `Table`).

- [ ] **Step 1: Add the env-gated Postgres fixture to `tests/conftest.py`**

Append:

```python
import os
import uuid


@pytest.fixture
def pg_meta():
    """A MetaStore pointed at a throwaway schema in the test Postgres.

    Skips when OASIS_TEST_PG_DSN is unset. DSN form:
    postgresql+psycopg2://user:pass@host:5432/dbname
    """
    dsn = os.environ.get("OASIS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("OASIS_TEST_PG_DSN not set; skipping Postgres metastore test")
    from etl import config, metastore
    from sqlalchemy.engine import make_url

    url = make_url(dsn)
    schema = f"etl_meta_test_{uuid.uuid4().hex[:8]}"
    cfg = config.PostgresConfig(
        host=url.host, port=url.port or 5432, database=url.database,
        username=url.username, password=url.password, schema=schema)
    store = metastore.MetaStore(cfg)
    store.ensure_schema()
    try:
        yield store
    finally:
        with store.engine.begin() as conn:
            from sqlalchemy import text
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        store.engine.dispose()
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_metastore.py`:

```python
from sqlalchemy import inspect


def test_ensure_schema_creates_all_tables(pg_meta):
    insp = inspect(pg_meta.engine)
    tables = set(insp.get_table_names(schema=pg_meta.cfg.schema))
    assert {"control_state", "etl_control", "etl_run_log", "etl_dq_results"} <= tables


def test_ensure_schema_is_idempotent(pg_meta):
    pg_meta.ensure_schema()  # second call must not raise
```

- [ ] **Step 3: Run test to verify it fails**

Run: `OASIS_TEST_PG_DSN=postgresql+psycopg2://postgres:postgres@localhost:5432/postgres python -m pytest tests/test_metastore.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'etl.metastore'` (or SKIP if no DSN — set one to run this task)

- [ ] **Step 4: Create `etl/metastore.py`**

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `OASIS_TEST_PG_DSN=postgresql+psycopg2://postgres:postgres@localhost:5432/postgres python -m pytest tests/test_metastore.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: Commit**

```bash
git add etl/metastore.py tests/conftest.py tests/test_metastore.py
git commit -m "feat(etl): add Postgres MetaStore module + idempotent DDL"
```

---

### Task 3: Metastore write helpers

**Files:**
- Modify: `etl/metastore.py`
- Test: `tests/test_metastore.py`

**Interfaces:**
- Produces on `MetaStore`:
  - `upsert_control_state(rows: list[dict]) -> None` (upsert on `(table_name, branch_id)`)
  - `read_control_state() -> list[dict]` (all rows as flat dicts)
  - `upsert_etl_control(rows: list[dict]) -> None` (upsert on `(table_name, branch_id)`)
  - `append_run_log(rows: list[dict]) -> None`
  - `append_dq_results(rows: list[dict]) -> None`
- Each row dict's keys are exactly the non-autoincrement column names of the target table.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_metastore.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_metastore.py -k "upsert or run_log" -v`
Expected: FAIL with `AttributeError: 'MetaStore' object has no attribute 'upsert_control_state'`

- [ ] **Step 3: Add the write helpers to `MetaStore` in `etl/metastore.py`**

```python
    def _upsert(self, table: Table, rows: list[dict], key_cols: list[str]) -> None:
        if not rows:
            return
        with self.engine.begin() as conn:
            for row in rows:
                stmt = pg_insert(table).values(**row)
                update_cols = {c.name: stmt.excluded[c.name]
                               for c in table.columns
                               if c.name not in key_cols and not c.primary_key}
                stmt = stmt.on_conflict_do_update(
                    index_elements=key_cols, set_=update_cols)
                conn.execute(stmt)

    def _append(self, table: Table, rows: list[dict]) -> None:
        if not rows:
            return
        with self.engine.begin() as conn:
            conn.execute(table.insert(), rows)

    def upsert_control_state(self, rows: list[dict]) -> None:
        self._upsert(self.control_state, rows, ["table_name", "branch_id"])

    def upsert_etl_control(self, rows: list[dict]) -> None:
        self._upsert(self.etl_control, rows, ["table_name", "branch_id"])

    def append_run_log(self, rows: list[dict]) -> None:
        self._append(self.etl_run_log, rows)

    def append_dq_results(self, rows: list[dict]) -> None:
        self._append(self.etl_dq_results, rows)

    def read_control_state(self) -> list[dict]:
        from sqlalchemy import select
        with self.engine.connect() as conn:
            result = conn.execute(select(self.control_state))
            return [dict(r._mapping) for r in result]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_metastore.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add etl/metastore.py tests/test_metastore.py
git commit -m "feat(etl): metastore upsert/append/read helpers"
```

---

### Task 4: Iceberg catalog config + routing validation (risk-first)

This verifies the load-bearing assumption of Approach A (spec §7.1): dlt's `get_iceberg_tables` resolves through the configured `[iceberg_catalog]` Postgres SQL catalog. Do this before wiring the rest so a failure surfaces immediately.

**Files:**
- Modify: `.dlt/config.toml`
- Modify: `.dlt/secrets.toml`
- Test: `tests/test_catalog_config.py` (create)

**Interfaces:**
- Consumes: dlt `get_catalog()` / `get_iceberg_tables()` from `dlt.common.libs.pyiceberg`.

- [ ] **Step 1: Add catalog config**

In `.dlt/config.toml`, after the `[destination.filesystem.kwargs]` block, add:

```toml
[iceberg_catalog]
iceberg_catalog_name = "oasis"
iceberg_catalog_type = "sql"
```

In `.dlt/secrets.toml`, add (fill in real host/creds; the catalog DB is `oasis_catalog`, the app DB is `oasis_meta`):

```toml
[postgres]
host = "localhost"
port = 5432
database = "oasis_meta"
username = "oasis"
password = "CHANGE_ME"

[iceberg_catalog.iceberg_catalog_config]
uri = "postgresql+psycopg2://oasis:CHANGE_ME@localhost:5432/oasis_catalog"
warehouse = "file:///abs/path/to/iceberg_output"
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_catalog_config.py`:

```python
import os

import pytest


@pytest.mark.skipif(not os.environ.get("OASIS_TEST_PG_DSN"),
                    reason="requires a Postgres catalog DB")
def test_get_catalog_returns_sql_catalog_on_postgres():
    from dlt.common.libs.pyiceberg import get_catalog
    from pyiceberg.catalog.sql import SqlCatalog

    dsn = os.environ["OASIS_TEST_PG_DSN"]
    cat = get_catalog(
        iceberg_catalog_name="oasis",
        iceberg_catalog_type="sql",
        iceberg_catalog_config={"uri": dsn, "warehouse": "file:///tmp/wh_test"},
    )
    assert isinstance(cat, SqlCatalog)
    assert cat.properties["uri"].startswith("postgresql")
```

- [ ] **Step 3: Run test to verify it fails / then passes**

Run: `OASIS_TEST_PG_DSN=postgresql+psycopg2://oasis:pass@localhost:5432/oasis_catalog python -m pytest tests/test_catalog_config.py -v`
Expected: PASS once a reachable `oasis_catalog` DB exists (SKIP without the env var). If it FAILS with a driver error, install `psycopg2-binary` (Task 1).

- [ ] **Step 4: Manual routing check (record result in commit message)**

Run a one-off to confirm `get_iceberg_tables` uses the configured catalog, not the SQLite fallback:

```bash
python -c "import dlt; from etl.iceberg_load import build_pipeline; from etl.config import load_settings; from dlt.common.libs.pyiceberg import get_iceberg_tables; p=build_pipeline(load_settings()); print(get_iceberg_tables(p))"
```

Expected: no `sqlite:///:memory:` catalog log line; a `SqlCatalog` on Postgres is used (empty dict is fine before any table exists). If this uses SQLite, STOP — Approach A is invalidated and the spec's fallback (PyIceberg-direct) must be revisited.

- [ ] **Step 5: Commit**

```bash
git add .dlt/config.toml .dlt/secrets.toml tests/test_catalog_config.py
git commit -m "feat(etl): point Iceberg catalog at Postgres SQL catalog + routing test"
```

---

### Task 5: ControlStore → Postgres

**Files:**
- Modify: `etl/iceberg_load.py:94-125` (`ControlStore`)
- Modify: `oracle_to_iceberg.py:166`
- Modify: `dq_check.py:130`
- Modify: `etl/config.py` (drop `control_state_path`, `--control-state`)
- Test: `tests/test_control_store_pg.py` (create)

**Interfaces:**
- Consumes: `metastore.MetaStore` (Task 2/3), `_wm_advance` (existing).
- Produces: `ControlStore(store: MetaStore)` with unchanged `load()`, `entry(table, branch)`, `advance(result)`, `as_dict()`, `save()`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_control_store_pg.py`:

```python
from etl.iceberg_load import ControlStore
from etl.oracle_extract import ExtractResult, Watermark


def _result(table, branch_id, cdc_val):
    return ExtractResult(
        table=table, branch=str(branch_id), branch_id=branch_id,
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_control_store_pg.py -v`
Expected: FAIL with `TypeError` (ControlStore still expects a path).

- [ ] **Step 3: Rewrite `ControlStore` in `etl/iceberg_load.py`**

Replace the class body (lines 94-125) with:

```python
class ControlStore:
    """Postgres-backed per-(table, branch) CDC state (schema etl_meta.control_state).

    Keeps the same public surface it had as a JSON store so the pipeline callers
    are unchanged: an in-memory nested dict {table: {branch: {...}}} loaded on
    ``load()``, mutated by ``advance()``, upserted whole on ``save()``.
    """

    def __init__(self, store: "MetaStore"):
        self.store = store
        self.data: dict = {}

    def load(self) -> "ControlStore":
        self.store.ensure_schema()
        self.data = {}
        for r in self.store.read_control_state():
            tbl = self.data.setdefault(r["table_name"], {})
            tbl[str(r["branch_id"])] = {
                "last_cdc": ({"value": r["last_cdc_value"], "kind": r["last_cdc_kind"]}
                             if r["last_cdc_value"] is not None else None),
                "last_date": ({"value": r["last_date_value"], "kind": r["last_date_kind"]}
                              if r["last_date_value"] is not None else None),
                "status": r["status"],
                "row_count": r["row_count"],
                "duration_ms": r["duration_ms"],
                "last_run_at": r["last_run_at"],
            }
        return self

    def as_dict(self) -> dict:
        return self.data

    def entry(self, table: str, branch: str) -> dict:
        return self.data.get(table, {}).get(branch, {})

    def advance(self, result: ExtractResult) -> None:
        tbl = self.data.setdefault(result.table, {})
        cur = tbl.setdefault(result.branch, {})
        cur["last_cdc"] = _wm_advance(cur.get("last_cdc"), result.new_cdc)
        cur["last_date"] = _wm_advance(cur.get("last_date"), result.new_date)
        cur["status"] = result.status
        cur["row_count"] = result.row_count
        cur["duration_ms"] = result.duration_ms
        cur["last_run_at"] = now_local().isoformat()

    def save(self) -> None:
        rows = []
        for table, branches in self.data.items():
            for branch, info in branches.items():
                cdc = info.get("last_cdc") or {}
                date = info.get("last_date") or {}
                rows.append({
                    "table_name": table, "branch_id": str(branch),
                    "last_cdc_value": cdc.get("value"), "last_cdc_kind": cdc.get("kind"),
                    "last_date_value": date.get("value"), "last_date_kind": date.get("kind"),
                    "status": info.get("status"), "row_count": info.get("row_count"),
                    "duration_ms": info.get("duration_ms"),
                    "last_run_at": info.get("last_run_at"),
                })
        self.store.upsert_control_state(rows)
```

Add the import at the top of `etl/iceberg_load.py` (near the other `from .` imports, ~line 47):

```python
from .metastore import MetaStore
```

- [ ] **Step 4: Update the two ControlStore constructors**

In `oracle_to_iceberg.py`, replace line 166:

```python
    control = iceberg_load.ControlStore(
        iceberg_load.MetaStore(settings.postgres)).load()
```

Add near the top of `oracle_to_iceberg.py` (the existing `from etl import ...`) — `MetaStore` is re-exported from `iceberg_load`, so no extra import is needed; if you prefer, `from etl.metastore import MetaStore` and use it directly.

In `dq_check.py`, replace line 130:

```python
    from etl.metastore import MetaStore
    control = ControlStore(MetaStore(settings.postgres)).load().as_dict()
```

- [ ] **Step 5: Drop the obsolete path setting**

In `etl/config.py`: remove `control_state_path` from `Settings` (line 316) and the `s.control_state_path = Path(...)` normalization (line 536). In `oracle_to_iceberg.py`: remove the `--control-state` argument (lines 71) and its override (lines 103-104).

- [ ] **Step 6: Run tests**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_control_store_pg.py -v`
Expected: PASS. Also run `python -m pytest tests/ -k "control or config" -v` — no import errors.

- [ ] **Step 7: Commit**

```bash
git add etl/iceberg_load.py etl/config.py oracle_to_iceberg.py dq_check.py tests/test_control_store_pg.py
git commit -m "feat(etl): back ControlStore with Postgres control_state table"
```

---

### Task 6: Observability writes → Postgres

**Files:**
- Modify: `etl/iceberg_load.py:662-702` (`_write_observability`)
- Modify: `etl/iceberg_load.py:1179-1275` (`load_and_record` — pass the MetaStore through)
- Test: `tests/test_observability_pg.py` (create)

**Interfaces:**
- Consumes: `_control_rows`, `_log_rows` (existing, unchanged), `MetaStore.upsert_etl_control`, `MetaStore.append_run_log`.
- Produces: `_write_observability(store, plans, settings, run_id)` writing to Postgres.

- [ ] **Step 1: Write the failing test**

Create `tests/test_observability_pg.py`:

```python
from etl import iceberg_load
from etl.config import Settings, TableDef, CATEGORY_MASTER
from etl.iceberg_load import TableLoadPlan
from etl.oracle_extract import ExtractResult, Watermark


def _plan():
    tdef = TableDef(table="OASIS.CUSTOMERS", unique_key="ID", cdc_column="UPDATED",
                    where_date_column=None, where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_MASTER)
    r = ExtractResult(table="customers", branch="1", branch_id=1, status="SUCCESS",
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_observability_pg.py -v`
Expected: FAIL (`_write_observability` still takes a `pipeline` and writes Iceberg resources).

- [ ] **Step 3: Rewrite `_write_observability`**

Replace the function body (lines 662-702) with:

```python
def _write_observability(store: MetaStore, plans, settings, run_id) -> None:
    """Upsert etl_control + append etl_run_log to Postgres for this run."""
    control_rows = _control_rows(plans, settings, run_id)
    log_rows = _log_rows(plans, settings, run_id)
    store.ensure_schema()
    if control_rows:
        store.upsert_etl_control(control_rows)
    if log_rows:
        store.append_run_log(log_rows)
```

The `_control_rows` / `_log_rows` builders are unchanged. Their `updated_at` / `recorded_at` / `start_time` / `end_time` values are naive datetimes from `now_local()` and the extract results — they land directly in the `TIMESTAMP` columns.

- [ ] **Step 4: Thread the MetaStore through `load_and_record`**

In `load_and_record` (line 1201) replace `holder = _PipelineHolder(settings)` region so a MetaStore is created and used for observability:

```python
    holder = _PipelineHolder(settings)
    store = MetaStore(settings.postgres)
    store.ensure_schema()
```

The `ControlStore` passed into `load_and_record` by the caller is already Postgres-backed (Task 5). At finalize (line 1269) replace:

```python
        _write_observability(store, plans, settings, run_id)
```

Leave `apply_snapshot_retention(holder.pipeline, settings)` as-is (data tables still need it).

- [ ] **Step 5: Run test to verify it passes**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_observability_pg.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add etl/iceberg_load.py tests/test_observability_pg.py
git commit -m "feat(etl): write etl_control/etl_run_log to Postgres"
```

---

### Task 7: DQ results → Postgres

**Files:**
- Modify: `etl/dq_check.py:1059-1076` (`write_results_iceberg` → `write_results_postgres`)
- Modify: `dq_check.py:150-153` (caller)
- Test: `tests/test_dq_results_pg.py` (create)

**Interfaces:**
- Consumes: `_result_rows` (existing), `MetaStore.append_dq_results`.
- Produces: `dq_check.write_results_postgres(results, settings, run_id) -> str` (returns table name `"etl_dq_results"`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_dq_results_pg.py`:

```python
from etl import dq_check
from etl.config import Settings


def test_write_results_postgres(pg_meta, monkeypatch):
    # one synthetic DqResult-shaped row via the module's row builder path
    from etl.dq_check import DqResult  # dataclass used by _result_rows
    r = DqResult(table="customers", source_table="OASIS.CUSTOMERS", branch="1",
                 status="OK")  # remaining fields default; see dataclass
    monkeypatch.setattr(dq_check, "_MetaStoreFactory", lambda s: pg_meta, raising=False)
    name = dq_check.write_results_postgres([r], Settings(), "dq-1", store=pg_meta)
    assert name == "etl_dq_results"
    with pg_meta.engine.connect() as conn:
        from sqlalchemy import select, func
        n = conn.execute(select(func.count()).select_from(pg_meta.etl_dq_results)).scalar()
    assert n == 1
```

Note: adjust the `DqResult(...)` construction to the dataclass's actual required fields (read `etl/dq_check.py` `DqResult` definition); the assertion on the row count is the real check.

- [ ] **Step 2: Run test to verify it fails**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_dq_results_pg.py -v`
Expected: FAIL (`write_results_postgres` does not exist).

- [ ] **Step 3: Replace the writer in `etl/dq_check.py`**

Replace `write_results_iceberg` (lines 1059-1076) with:

```python
def write_results_postgres(results, settings, run_id, store=None) -> str:
    """Append the DQ results to the Postgres ``etl_dq_results`` table."""
    from .metastore import MetaStore

    rows = _result_rows(results, settings, run_id)
    if not rows:
        return _TABLE_NAME
    store = store or MetaStore(settings.postgres)
    store.ensure_schema()
    store.append_dq_results(rows)
    return _TABLE_NAME
```

`_result_rows` and `_DQ_HINTS` are unchanged (`_DQ_HINTS` now only documents column types; the DDL in `metastore.py` is the source of truth). `_TABLE_NAME` stays `"etl_dq_results"`.

- [ ] **Step 4: Update the CLI caller**

In `dq_check.py`, replace lines 150-153:

```python
    if not args.no_write:
        name = dq_check.write_results_postgres(results, settings, run_id)
        print(f"-> wrote {len(results)} row(s) to Postgres table "
              f"'{settings.postgres.schema}.{name}'")
```

- [ ] **Step 5: Run test to verify it passes**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_dq_results_pg.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add etl/dq_check.py dq_check.py tests/test_dq_results_pg.py
git commit -m "feat(etl): write etl_dq_results to Postgres"
```

---

### Task 8: GUI reads watermarks + system tables from Postgres

**Files:**
- Modify: `gui/config.py:20,149-151` (drop `CONTROL_STATE` file path; add PG accessor)
- Modify: `gui/workspace.py:189-240` (`load_control_state`, `control_rows`)
- Modify: `gui/iceberg_browser.py:410-437,535-578,584-605` (`read_system_table`, `_scan_pylist`, `_clear_control_state`)
- Test: `tests/test_gui_pg_reads.py` (create)

**Interfaces:**
- Consumes: `etl.metastore.MetaStore`, `etl.config.load_postgres_config`.
- Produces: `gui/metastore_read.py` helper `open_metastore() -> MetaStore`; system-table reads and control reads sourced from Postgres.

- [ ] **Step 1: Add a GUI-side metastore accessor**

Create `gui/metastore_read.py`:

```python
"""GUI read access to the Postgres app metastore (etl_meta.*)."""
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
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_gui_pg_reads.py`:

```python
import metastore_read  # gui/ is on sys.path via conftest


def test_gui_reads_control_state(pg_meta, monkeypatch):
    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    pg_meta.upsert_control_state([{
        "table_name": "customers", "branch_id": "1",
        "last_cdc_value": "5", "last_cdc_kind": "number",
        "last_date_value": None, "last_date_kind": None,
        "status": "SUCCESS", "row_count": 3, "duration_ms": 1,
        "last_run_at": "2026-07-20T10:00:00"}])
    rows = metastore_read.read_table_rows("control_state")
    assert rows[0]["table_name"] == "customers"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_gui_pg_reads.py -v`
Expected: FAIL (`ModuleNotFoundError: metastore_read`) until Step 1's file exists; then PASS.

- [ ] **Step 4: Repoint `workspace.load_control_state` / `control_rows`**

In `gui/workspace.py`, replace `load_control_state` (lines 189-195) and keep `control_rows`/`control_summary` working by feeding them the Postgres rows. Replace `load_control_state` with a nested-dict builder from Postgres so `control_rows` (which expects `{table: {branch: {...}}}`) is unchanged:

```python
def load_control_state() -> dict[str, Any]:
    try:
        from metastore_read import read_table_rows
        rows = read_table_rows("control_state")
    except Exception:
        return {}
    state: dict[str, Any] = {}
    for r in rows:
        state.setdefault(r["table_name"], {})[str(r["branch_id"])] = {
            "status": r["status"], "row_count": r["row_count"],
            "duration_ms": r["duration_ms"], "last_run_at": r["last_run_at"],
            "last_cdc": {"value": r["last_cdc_value"]},
            "last_date": {"value": r["last_date_value"]},
        }
    return state
```

- [ ] **Step 5: Repoint the system-table reads in `gui/iceberg_browser.py`**

`read_system_table` (line 410) and `_scan_pylist` (line 535) currently open Iceberg via `_open_static`. For the three moved tables, source rows from Postgres. Change `_scan_pylist` to:

```python
def _scan_pylist(table: str, row_filter=None) -> list[dict]:
    """System table as plain dicts. etl_* tables now live in Postgres."""
    if table in ("etl_control", "etl_run_log", "etl_dq_results"):
        from metastore_read import read_table_rows
        rows = read_table_rows(table)
        if row_filter is not None:  # only EqualTo(pipeline_run_id, X) is used
            field, value = row_filter.term.name, row_filter.literal.value
            rows = [r for r in rows if str(r.get(field)) == str(value)]
        return rows
    tbl = _open_static(table)
    scan = tbl.scan(row_filter=row_filter) if row_filter is not None else tbl.scan()
    return scan.to_arrow().to_pylist()
```

And change `read_system_table` (line 416) to branch on the same set, reading from Postgres and applying the existing timestamp-descending sort in Python:

```python
def read_system_table(table: str, limit: int = 200) -> dict[str, Any]:
    if table in ("etl_control", "etl_run_log", "etl_dq_results"):
        from metastore_read import read_table_rows
        rows = read_table_rows(table)
        columns = list(rows[0].keys()) if rows else []
        sort_col = next((c for c in ("check_time", "start_time", "recorded_at",
                                     "updated_at", "last_run_at") if c in columns), None)
        if sort_col:
            rows.sort(key=lambda r: (r.get(sort_col) is not None, r.get(sort_col)),
                      reverse=True)
        rows = [{k: _jsonable(v) for k, v in r.items()} for r in rows[:limit]]
        return {"table": table, "columns": columns, "rows": rows, "total": len(rows)}
    # ... existing Iceberg path unchanged ...
```

- [ ] **Step 6: Repoint `_clear_control_state`**

Replace `_clear_control_state` (lines 584-605) to delete rows from Postgres:

```python
def _clear_control_state(tables: list[str]) -> list[str]:
    """Delete the tables' watermark rows from etl_meta.control_state."""
    try:
        from metastore_read import open_metastore
        store = open_metastore()
    except Exception:
        return []
    from sqlalchemy import delete
    cleared: list[str] = []
    with store.engine.begin() as conn:
        for t in tables:
            res = conn.execute(delete(store.control_state).where(
                store.control_state.c.table_name == t))
            if res.rowcount:
                cleared.append(t)
    return cleared
```

Remove the now-unused `CONTROL_STATE` import from `gui/iceberg_browser.py` and delete `CONTROL_STATE = REPO_ROOT / "control_state.json"` from `gui/config.py:20`.

- [ ] **Step 7: Run tests**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/test_gui_pg_reads.py tests/test_iceberg_run_rollup.py -v`
Expected: PASS. `test_iceberg_run_rollup.py` may need its fixtures updated to seed Postgres instead of Iceberg — update it to use `pg_meta` + `read_table_rows` and keep the rollup assertions.

- [ ] **Step 8: Commit**

```bash
git add gui/metastore_read.py gui/workspace.py gui/iceberg_browser.py gui/config.py tests/test_gui_pg_reads.py tests/test_iceberg_run_rollup.py
git commit -m "feat(gui): read watermarks + system tables from Postgres"
```

---

### Task 9: Fresh-start cleanup + docs

**Files:**
- Modify: `fresh_run.sh`
- Modify: `fresh_run.cmd`
- Modify: `README.md`, `gui/README.md`

**Interfaces:** none (scripts + docs).

- [ ] **Step 1: Update `fresh_run.sh`**

Replace the `control_state.json` removal (line 23) and add Postgres + catalog reset. Keep the `iceberg_output` / `_staging` removals. After line 24, add:

```bash
# --- Postgres app metastore + Iceberg catalog reset ----------------------- #
# Requires psql on PATH and $OASIS_META_DSN / $OASIS_CATALOG_DSN pointing at the
# two databases (postgresql://user:pass@host:port/db). Skipped if unset.
if [ -n "${OASIS_META_DSN:-}" ]; then
  psql "${OASIS_META_DSN}" -c 'DROP SCHEMA IF EXISTS etl_meta CASCADE;' || true
fi
if [ -n "${OASIS_CATALOG_DSN:-}" ]; then
  psql "${OASIS_CATALOG_DSN}" -c 'DROP TABLE IF EXISTS iceberg_tables, iceberg_namespace_properties CASCADE;' || true
fi
```

Delete the `rm -f control_state.json` line (line 23) — the file no longer exists.

- [ ] **Step 2: Mirror the changes in `fresh_run.cmd`**

Add equivalent `psql` calls guarded by `if defined OASIS_META_DSN` / `if defined OASIS_CATALOG_DSN`, and remove any `control_state.json` deletion.

- [ ] **Step 3: Update docs**

In `README.md`: add Postgres to the prerequisites list (alongside ClickHouse) — note the two databases `oasis_catalog` + `oasis_meta`, the `[postgres]` + `[iceberg_catalog]` config blocks, and that first run after cutover is `--mode INITIAL`. In `gui/README.md`: change the Dashboard/Logs descriptions from "`control_state.json`" / "Iceberg observability tables" to "Postgres `etl_meta` tables".

- [ ] **Step 4: Commit**

```bash
git add fresh_run.sh fresh_run.cmd README.md gui/README.md
git commit -m "chore(etl): fresh-start resets Postgres metastore + catalog; docs"
```

---

### Task 10: End-to-end self-test + full-suite gate

**Files:** none (verification).

- [ ] **Step 1: Provision two empty databases**

On the test Postgres: `CREATE DATABASE oasis_catalog;` and `CREATE DATABASE oasis_meta;`. Point `.dlt/secrets.toml` at them.

- [ ] **Step 2: Run the offline pipeline self-test**

Run: `python oracle_to_iceberg.py --mode INITIAL --self-test`
Expected: completes; data tables written to Iceberg via the Postgres catalog; `etl_meta.control_state`, `etl_control`, `etl_run_log` populated. Verify:

```bash
psql "$OASIS_META_DSN" -c 'SELECT count(*) FROM etl_meta.control_state;'
psql "$OASIS_META_DSN" -c 'SELECT count(*) FROM etl_meta.etl_run_log;'
```

- [ ] **Step 3: Run the DQ self-test**

Run: `python dq_check.py --self-test`
Expected: completes; `SELECT count(*) FROM etl_meta.etl_dq_results;` > 0.

- [ ] **Step 4: Launch the GUI and confirm reads**

Run: `python gui/app.py` (or `setup.cmd -NoStart` then launch). Open the Dashboard + Logs pages; confirm watermarks, `etl_run_log`, `etl_control`, `etl_dq_results` render from Postgres.

- [ ] **Step 5: Full suite**

Run: `OASIS_TEST_PG_DSN=... python -m pytest tests/ -v`
Expected: PASS (Postgres tests run; everything else green). Investigate and fix any Iceberg system-table tests still assuming the old on-disk tables.

- [ ] **Step 6: Commit any test fixups**

```bash
git add -A
git commit -m "test(etl): green suite on Postgres catalog + metastore"
```

---

## Self-Review

**1. Spec coverage:**
- §5.1 Topology & config → Task 1 (config), Task 4 (catalog + secrets). ✓
- §5.2 Catalog swap + validation → Task 4 (incl. §7.1 routing check, done risk-first). ✓
- §5.3 Metastore module + 4 tables → Tasks 2–3. ✓
- §5.4 ControlStore/observability/DQ wiring → Tasks 5, 6, 7. ✓
- §5.5 Removed machinery → Task 5 (control_state_path/flag), Task 6 (obs dlt resources), Task 7 (dq Iceberg write); `_naive_ts_hint` for these tables is moot (Postgres naive timestamps). ✓
- §5.6 GUI readers → Task 8. ✓
- §5.7 Fresh-start cleanup → Task 9. ✓
- §6 Testing → per-task tests + Task 10 end-to-end. ✓

**2. Placeholder scan:** Task 7's test notes "adjust `DqResult(...)` to the dataclass's actual required fields" — this is a directed instruction (read the dataclass), not a placeholder; the row-count assertion is concrete. Config examples use `CHANGE_ME`/`/abs/path` deliberately (operator-supplied secrets). No `TBD`/`implement later`.

**3. Type consistency:** `MetaStore` constructed as `MetaStore(settings.postgres)` everywhere; `ControlStore(store)` takes a `MetaStore`; helper names (`upsert_control_state`, `read_control_state`, `upsert_etl_control`, `append_run_log`, `append_dq_results`) are used identically across Tasks 3, 5, 6, 7, 8; `_write_observability(store, ...)` signature matches its Task 6 caller; `read_table_rows`/`open_metastore` names match across `gui/metastore_read.py` and its consumers.

**Open items flagged for the implementer:** the pre-existing Iceberg system-table tests (`test_iceberg_run_rollup.py`, and any in `test_run_iceberg_pages_render.py`) will need their seeds moved from Iceberg to Postgres — surfaced in Task 8 Step 7 and Task 10 Step 5 rather than hidden.
