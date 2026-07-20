# Postgres Iceberg catalog + Postgres metastore — design

- **Date:** 2026-07-20
- **Status:** Proposed (awaiting spec review)
- **Scope owner:** ETL (oracle → Iceberg pipeline)

## 1. Context

The pipeline extracts Oracle (multi-branch) → Iceberg via dlt's **filesystem** destination
with `table_format="iceberg"` ([.dlt/config.toml](../../../.dlt/config.toml)). That path keeps a
**per-table SQLite "technical catalog"** next to the data files — the priority-3
backward-compat fallback in dlt's catalog loader. Pipeline metadata lives in two places
today:

- **`control_state.json`** — the authoritative per-`(table, branch)` CDC watermark store
  (`iceberg_load.ControlStore`), a local JSON file.
- **Three Iceberg observability tables** — `etl_control` (a queryable upsert *mirror* of the
  watermark store), `etl_run_log` (append-only run history), and `etl_dq_results` (append-only
  data-quality results, written by `etl/dq_check.py`).

The GUI reads all three observability tables from Iceberg
(`gui/iceberg_browser.py`, `gui/workspace.py`, `dashboard.html` / `logs.html` / `iceberg.html`).

## 2. Goal

Move all pipeline *metadata and catalog* state into Postgres, leaving the Iceberg **data
files** exactly where they are:

1. Replace the SQLite/filesystem Iceberg catalog with a **Postgres SQL catalog**.
2. Move the watermark store (`control_state.json`) into a Postgres table.
3. Move `etl_control`, `etl_run_log`, `etl_dq_results` into Postgres tables.

Postgres itself is an **external prerequisite** the operator provides (like ClickHouse today);
setup does **not** install it.

## 3. Locked decisions

| # | Decision |
|---|----------|
| Provisioning | Postgres is an external prerequisite. Setup is **not** scripted to install it. Only add the Python client (`psycopg2-binary`) to requirements. |
| Databases | **Two separate databases**: one for the Iceberg catalog, one for the app metastore tables. |
| control_state vs etl_control | **Keep as two separate tables** — no collapse. `control_state` stays the authoritative watermark store; `etl_control` stays its observability mirror. Preserves current behavior. |
| Existing data | **Start completely fresh.** No migration of watermarks, tables, or history. First run after cutover is a full INITIAL that rebuilds under the new catalog. |
| Write engine | **Keep the dlt write path** (Approach A). No PyIceberg-direct rewrite. |

## 4. Approach

**Approach A (chosen) — config-driven catalog swap, keep the dlt write path.**
dlt's `get_catalog()` ([dlt/common/libs/pyiceberg.py](../../../.venv/Lib/site-packages/dlt/common/libs/pyiceberg.py))
takes an explicit `iceberg_catalog_config` dict at **priority 1**, ahead of the SQLite fallback.
Pointing `[iceberg_catalog]` at a Postgres SQL catalog routes the whole filesystem-Iceberg path —
including the `get_iceberg_tables` reads used for carry-forward / hash-ready / snapshot-squash —
through Postgres. All custom load machinery (single-commit merge patch, per-branch rebuild,
snapshot squashing, insert_at carry-forward, commit watchdog) operates on `pyiceberg` `Table`
objects from whatever catalog is configured, so it carries over unchanged.

**Approach B (rejected for now) — PyIceberg-direct rewrite.** Drop dlt from the load path and
call `SqlCatalog` + `append`/`overwrite`/`upsert` directly. Cleaner long-term but re-implements
per-branch memory bounding, carry-forward, squashing, and the watchdog by hand. Out of scope;
revisit independently if shedding dlt overhead ever becomes the goal.

## 5. Design

### 5.1 Postgres topology & configuration

Two databases (same server is fine; the config permits different hosts):

- **`oasis_catalog`** — the Iceberg SQL catalog. pyiceberg's `SqlCatalog` auto-creates its
  `iceberg_tables` + `iceberg_namespace_properties` tables here on first use.
- **`oasis_meta`** — the app metastore, holding the four tables in §5.3.

**Iceberg data files stay on the local filesystem** (`iceberg_output`, a `file://` warehouse).
Only catalog *pointers* live in Postgres.

Credentials follow the existing `[clickhouse]` / `[oracle_branches]` pattern. Password-bearing
values go in `.dlt/secrets.toml`:

```toml
# .dlt/secrets.toml
[postgres]                       # app metastore DB (oasis_meta)
host = "..."
port = 5432
database = "oasis_meta"
username = "..."
password = "..."

[iceberg_catalog.iceberg_catalog_config]   # Iceberg catalog DB (oasis_catalog)
uri = "postgresql+psycopg2://user:pass@host:5432/oasis_catalog"
warehouse = "file:///abs/path/to/iceberg_output"
```

```toml
# .dlt/config.toml
[iceberg_catalog]
iceberg_catalog_name = "oasis"
iceberg_catalog_type = "sql"
```

`etl/config.py` gains a typed `PostgresConfig` (host/port/database/user/password/schema) loaded
from `[postgres]`, exposed on `Settings`. The catalog config is consumed by dlt directly from
`[iceberg_catalog]`; the pipeline code does not read it.

New dependency: **`psycopg2-binary`** (SQLAlchemy driver for both the pyiceberg SqlCatalog and the
metastore). Added to `requirements-gui.txt` / the pipeline requirements. `sqlalchemy` (2.0.x) is
already present via pyiceberg.

### 5.2 Iceberg catalog swap (#2)

Set the config in §5.1; no change to `iceberg_load.py`'s write logic is expected. Data files
land at the `warehouse` path. **Validation checkpoints (verified during implementation, not
assumed):**

1. `get_iceberg_tables(pipeline, table_name)` resolves via `get_catalog()` and returns tables
   from the Postgres catalog (so carry-forward / hash-ready / squash reads hit Postgres).
2. New tables' data + metadata files are written under the `warehouse` path, not a stray CWD.
3. The existing Iceberg test suite (`tests/test_iceberg_*.py`) passes against the Postgres catalog.
4. Snapshot retention / expiry (`apply_snapshot_retention`) still operates through the catalog.

### 5.3 Metastore module and tables (#3 + #4)

New module **`etl/metastore.py`** owns:

- a lazily-created SQLAlchemy engine to `oasis_meta` (from `PostgresConfig`),
- idempotent DDL (`CREATE SCHEMA IF NOT EXISTS etl_meta` + `CREATE TABLE IF NOT EXISTS ...`) run
  once per process,
- typed helpers: `upsert_control_state(...)`, `read_control_state()`, `upsert_etl_control(rows)`,
  `append_run_log(rows)`, `append_dq_results(rows)`.

All generated timestamps are `TIMESTAMP WITHOUT TIME ZONE` holding naive local wall-clock — the
same semantics `_naive_ts_hint` fought for in Iceberg, but native to Postgres, so the
timezone-hint workaround is unnecessary for these tables.

Four tables in schema `etl_meta` (**two watermark/control tables kept separate per decision**):

**`control_state`** — authoritative watermark store (upsert; PK `(table_name, branch_id)`).
Replaces `control_state.json`. Columns: `table_name`, `branch_id`, `last_cdc_value`,
`last_cdc_kind`, `last_date_value`, `last_date_kind`, `status`, `row_count`, `duration_ms`,
`last_run_at`. (The JSON `{value, kind}` watermark dicts become explicit `_value` / `_kind`
column pairs.)

**`etl_control`** — observability mirror (upsert; PK `(table_name, branch_id)`). Columns as
produced by `_control_rows`: `table_name`, `branch_id`, `load_mode`, `status`, `row_count`,
`attempts`, `last_cdc_value`, `last_cdc_kind`, `last_date_value`, `last_date_kind`,
`duration_ms`, `start_time`, `end_time`, `error_details`, `pipeline_run_id`, `updated_at`.

**`etl_run_log`** — append-only run history. Columns as produced by `_log_rows`:
`pipeline_run_id`, `table_name`, `branch_id`, `load_mode`, `row_count`, `start_time`,
`end_time`, `duration_ms`, `status`, `attempts`, `write_disposition`, `load_status`,
`error_details`, `schema_discrepancy`, `recorded_at`.

**`etl_dq_results`** — append-only DQ results. Columns as produced by `etl/dq_check._result_rows`
+ `_DQ_HINTS`: `check_time`, `pipeline_run_id`, `table_name`, `source_table`, `branch_id`,
`date_column`, `window_start`, `window_end`, `window_note`, `oracle_row_count`,
`iceberg_row_count`, `row_count_delta`, `hash_columns`, `oracle_hashed_rows`,
`iceberg_hashed_rows`, `hash_matched`, `hash_only_in_oracle`, `hash_only_in_iceberg`,
`hash_mismatch`, `hash_total_delta`, `hash_delta_pct`, `columns_only_in_oracle`,
`columns_only_in_iceberg`, `status`, `error_details`. (`bigint` / `double precision` / `text` /
`timestamp` per the existing hints.)

### 5.4 Wiring the pipeline to the metastore

- **`ControlStore`** (`iceberg_load.py`) keeps its public surface (`load`, `entry`, `advance`,
  `as_dict`, `save`) so `oracle_to_iceberg.py` and the load thread are unchanged. Internally it
  reads/writes `etl_meta.control_state` instead of JSON. `load()` selects existing rows into the
  in-memory dict; `save()`/`advance()` upsert. `control_state_path` is dropped from `Settings`.
- **`_write_observability`** (`iceberg_load.py`) stops building dlt Iceberg resources for
  `etl_control` / `etl_run_log`; it calls `metastore.upsert_etl_control(...)` and
  `metastore.append_run_log(...)`. The `_control_rows` / `_log_rows` row builders are reused as-is.
- **`etl/dq_check.write_results_iceberg`** becomes `write_results_postgres` (or is renamed at the
  call site), calling `metastore.append_dq_results(...)` with the same `_result_rows` output.
  `_DQ_HINTS` is retained only as the column/type source for the DDL.

### 5.5 Removed machinery

Deleted or bypassed as a consequence of moving these three tables off Iceberg:

- dlt resource definitions for `etl_control` / `etl_run_log` in `_write_observability`, and for
  `etl_dq_results` in `dq_check`.
- snapshot squash + retention handling **for those three tables only** (data tables keep it).
- `_naive_ts_hint` usage in the observability/DQ hint maps (Postgres timestamps are naive
  natively). The data-table timestamp hints in `_iceberg_resource` are untouched.
- `control_state.json` file I/O; `Settings.control_state_path`; `--control-state` CLI flag and its
  override.

### 5.6 GUI reader updates

The three observability reads switch from Iceberg to Postgres via a small read helper in
`etl/metastore.py` (or a thin `gui`-side wrapper):

- `gui/iceberg_browser.py` / `gui/workspace.py` — the code paths that load `etl_control`,
  `etl_run_log`, `etl_dq_results`.
- `dashboard.html` / `logs.html` / `iceberg.html` — served by the above; no template logic
  change beyond the data source, assuming column names are preserved (they are).

The **general lake-table browser** (arbitrary Iceberg data tables) is unchanged — those tables
still live in Iceberg, now resolved through the Postgres catalog.

### 5.7 Fresh-start cutover & cleanup

Per the "start fresh" decision there is no migration code. `fresh_run.sh` (and `fresh_run.cmd`)
gain steps to:

- `TRUNCATE` the four `etl_meta` tables (or `DROP SCHEMA etl_meta CASCADE` + recreate on next run),
- drop the Iceberg catalog registrations in `oasis_catalog` (so a re-INITIAL re-creates tables
  cleanly),
- keep clearing `iceberg_output` / `_staging` as today; stop deleting `control_state.json` (gone).

The first run after cutover is `--mode INITIAL`.

## 6. Testing

- **Metastore unit tests** against a disposable Postgres schema: DDL create is idempotent;
  `control_state` upsert + read round-trips watermark dicts; `etl_control` upsert is keyed
  correctly; `run_log` / `dq_results` append.
- **Catalog integration**: re-run `tests/test_iceberg_*.py` against the Postgres catalog; confirm
  §5.2 checkpoints.
- **End-to-end `--self-test`**: synthetic extract → catalog write → Postgres observability → GUI
  read, with no Oracle and no ClickHouse.

## 7. Risks & open validation points

- **`get_iceberg_tables` catalog routing** (§5.2.1) is the load-bearing assumption for Approach A.
  If it does not honor `[iceberg_catalog]`, the reads in `iceberg_load.py` (carry-forward,
  hash-ready, squash) would fall back to a different catalog. Verified first, before other work.
- **Warehouse path** for a SQL catalog must be set explicitly or new data files may land in an
  unexpected location.
- **Connection lifecycle**: the load thread is single-threaded and serialized; the metastore
  engine must be used from that thread (or be connection-pooled) to avoid cross-thread session use.
- **Two-phase runs** (`masters` then `transactions`) share one `ControlStore`; the Postgres-backed
  store must preserve the same in-memory snapshot semantics `oracle_to_iceberg.main` relies on
  (`copy.deepcopy(control.as_dict())` per phase).

## 8. Rollback

The change is config- and code-level; data files are untouched. Rollback = revert the code,
remove `[iceberg_catalog]` (reverting to the SQLite fallback) and re-point the pipeline at
`control_state.json`. Because cutover is a fresh INITIAL, the pre-cutover Iceberg data and JSON
watermark file can be retained as a backup until the new stack is validated.
