# Oracle 11g → Iceberg multi-branch ETL (dlt)

A production-grade [dlt](https://dlthub.com) pipeline that extracts tables from
**7 OASIS Oracle 11g branches in parallel** and lands them as **Apache Iceberg**
datasets, with CDC incremental loads, cross-branch schema unification, resilient
retries, and a control/observability layer.

## Layout

```
oracle_to_iceberg.py     # CLI entry point (extraction → load → control/log)
dq_check.py              # CLI entry point for the data-quality reconciliation
tables.json              # table definitions (key, cdc column, load filters)
.dlt/secrets.toml        # [oracle_branches.*] connection sections
.dlt/config.toml         # Iceberg destination + [etl] tuning
etl/
  config.py              # typed config objects + loaders
  types_map.py           # Oracle→Arrow type mapping + cross-branch schema union
  oracle_extract.py      # connection pools, query builder, threaded extraction, retries
  iceberg_load.py        # write strategy, control state, control + log Iceberg tables
  dq_check.py            # data-quality checks (row-count + row-hash reconciliation)
```

## Install

```bash
pip install -r requirements.txt
```

The Python code is cross-platform — it runs on **Windows and Ubuntu/Linux**
unchanged (paths use `pathlib`, and the peak-memory probe has per-OS backends).
The only host-specific pieces are the Oracle Instant Client location and the
output directory, both resolved automatically (see **Platform notes** below).

### Oracle Instant Client (thick mode)

Oracle 11g requires python-oracledb **thick mode** (the thin driver only supports
12.1+), so the Instant Client native libraries must be installed on each host:

- **Windows** — download the Instant Client, then either add its folder to `PATH`
  or point `oracle_client_lib_dir` at it (see below). Example dir:
  `C:\oracle\instantclient_19_24`.
- **Ubuntu/Linux** — install the Instant Client (`.deb`/`.zip`) and its
  prerequisite `libaio1`, then make the libs discoverable via `LD_LIBRARY_PATH`
  (or `ldconfig`):

```bash
sudo apt-get install -y libaio1
export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_24:$LD_LIBRARY_PATH
```

### Platform notes (how paths resolve on each OS)

The same `.dlt/config.toml` works on both platforms — nothing needs editing when
moving between them:

- **Instant Client dir** — resolution order is `$ORACLE_CLIENT_LIB_DIR` →
  `etl.oracle_client_lib_dir` in `config.toml` → none. A configured path that
  **does not exist on the current host is ignored**, falling back to the system
  loader path (`PATH` on Windows, `LD_LIBRARY_PATH` on Linux). So a Windows path
  left in `config.toml` is harmless on Linux. Override per-host with
  `$ORACLE_CLIENT_LIB_DIR` or `--oracle-client-lib-dir`.
- **Output location** — `destination.filesystem.bucket_url` accepts a relative /
  scheme-less path (default `iceberg_output`), resolved against the repo root and
  converted to a `file://` URI for the current OS. Override with
  `$OASIS_BUCKET_URL`, or set an explicit URL (`file:///abs/path`, `s3://…`).

## Run

```bash
# Full initial load of every table from every branch
python oracle_to_iceberg.py --mode INITIAL

# Incremental (CDC) load — only changed/new rows since the last run
python oracle_to_iceberg.py --mode INCREMENTAL

# Scope to specific branches / tables
python oracle_to_iceberg.py --mode INCREMENTAL --branch alrabwah,khamis --tables APPOINTMENTS

# Run only one group (masters and transactions run as separate phases)
python oracle_to_iceberg.py --mode INCREMENTAL --category masters
python oracle_to_iceberg.py --mode INCREMENTAL --category transactions

# Exercise the entire pipeline offline with synthetic data (no Oracle needed)
python oracle_to_iceberg.py --mode INITIAL --self-test
```

`--help` lists every flag (worker counts, pool size, retry policy, DSN mode, …).
Flags override the `[etl]` section in `.dlt/config.toml`.

## How it works

### 1. Configuration
- `tables.json` defines each table: `unique_key`, `cdc_column`,
  `where_date_column`, and the INITIAL range (`where_operator` +
  `where_value_of_initial_run`). Tables are split into `masters` (full load) and
  `transactions` (ranged load).
- **Helper-driven CDC (tables with no CDC column of their own).** A child table
  that lacks a usable CDC/date column can borrow one from a parent/helper table
  over a declared join. Leave the child's own `cdc_column`/`where_date_column`
  null and add a `helper` block; the helper's columns then drive both the
  *changed* and *new* row filters and the watermark, while only the child's rows
  are written. `where_operator`/`where_value_of_initial_run` still set the
  INITIAL range, applied to the **helper's** date column.

  ```json
  {
    "table": "OASIS.ORDER_LINES",
    "unique_key": "ORDER_LINE",
    "cdc_column": null,
    "where_date_column": null,
    "where_operator": ">=",
    "where_value_of_initial_run": "2026-06-01",
    "helper": {
      "table": "OASIS.ORDERS_MASTER",
      "join": [["MASTER_ORDER_NO", "MASTER_ORDER_NO"]],
      "cdc_column": "AMEND_LAST_DATE",
      "where_date_column": "ORDER_DATE"
    }
  }
  ```

  `join` is a list of `[child_column, helper_column]` equi-join pairs (composite
  keys allowed, e.g. `[["PATIENT_ID","PATIENT_ID"],["EPISODE_NO","EPISODE_NO"]]`).
  `helper.where_date_column` is optional (omit for a cdc-only helper). The child
  is INNER-joined to the helper (one helper row per child assumed, e.g.
  line → header), and the helper's cdc/date are projected internally as
  `ETL_HELPER_CDC` / `ETL_HELPER_DATE` to capture the watermark — these reserved
  columns are stripped before the Iceberg write, so the lake table holds only the
  child's columns (plus `BRANCH_ID` / `insert_at` / `Recorded_updated_at`). Requires that the
  helper's CDC moves whenever a child row is inserted or updated.
- Branch connections come from `[oracle_branches.<key>]` in `.dlt/secrets.toml`.
  The `<key>` is what you pass to `--branch`. Fetch tuning is **per branch**:
  each section may set its own optional `fetch_batch_size` (the Oracle
  round-trip / arraysize); a section that omits it falls back to
  `DEFAULT_FETCH_BATCH_SIZE` in `etl/config.py`.

### 2. Parallel extraction
- **Masters and transactions run as separate, sequential phases** — masters
  (dimensions) first, then transactions (facts). They are never extracted or
  loaded together. Use `--category masters|transactions|both` (default `both`);
  if a phase aborts on a fatal error, later phases do not run.
- **Nested thread pools** (within a phase): an outer pool over branches (≤ 7)
  and an inner pool over tables per branch (`max_table_workers`, default 3).
- Each branch gets its own **oracledb connection pool** with bounded size and
  **exponential backoff** on pool exhaustion, so we never thrash the listener.
- `BRANCH_ID`, `insert_at`, and `Recorded_updated_at` are injected into every
  result set. `insert_at` is the row's **first-load** time (preserved across
  later updates — see §5) and `Recorded_updated_at` is the **latest-load** time,
  so the two together tell inserts from updates when validating. Both use the
  **server's local** wall-clock time (not a fixed timezone).
- **Fast reads**: uses python-oracledb's native Arrow fetch
  (`fetch_df_batches`) to build columnar data in C and stream it straight to
  staged parquet — no per-row Python objects. A classic cursor fetch is the
  automatic fallback for empty results or any column type the Arrow fetch
  rejects (an Oracle 11g safety net). `fetch_lobs=False` avoids a round trip per
  LOB row; tune each branch's `fetch_batch_size` (in its `[oracle_branches.<key>]`
  section) for the round-trip size — high-latency branches can use a larger batch.

### 3. Resilience
- **Only connection/transient failures are retried** — up to **5 times, 5
  minutes apart** (configurable). Any other error (bad SQL, missing table,
  type/conversion error, invalid credentials) is **raised immediately** during
  reading rather than swallowed, so real problems surface loudly.
- A branch/table whose **connection** retries are exhausted is collected as
  `FAILED` and **never blocks the others** — the run proceeds and writes the
  successful branches.
- Extraction stages every `(branch, table)` to parquet; **each table is written
  as soon as all of its branches have finished** (succeeded or exhausted
  retries) — not after the whole extraction — concurrently with the remaining
  tables. Successful branches are written; failed ones are skipped. Because a
  table is flushed the moment it is ready, tables completed before a fatal error
  are already persisted.

### 4. Schema & types
- Oracle types are mapped to Arrow → Iceberg: `NUMBER → DECIMAL`,
  `VARCHAR2/CHAR/CLOB → STRING`, `DATE/TIMESTAMP → TIMESTAMP`,
  `BINARY_DOUBLE → DOUBLE`, `RAW/BLOB → BINARY`. Unconstrained `NUMBER` is
  inferred from the data to avoid clipping large ids.
- The 7 branch schemas for a table are **unioned**: columns present in any branch
  are kept, types are widened (e.g. `decimal(18,2)` + `decimal(18,4)` →
  `decimal(20,4)`), and columns missing from a branch become nullable. Drift is
  logged and recorded in `etl_run_log.schema_discrepancy`.

### 5. Write strategy
- **INITIAL**: full `replace` when the run cleanly covers *all* branches;
  otherwise `merge` so a skipped/failed branch's existing rows are preserved.
- **INCREMENTAL**: `merge` upsert on the **compound key = table PK + BRANCH_ID**.
  Incremental SQL selects updated rows (`cdc_column > watermark`) **and** new rows
  (`where_date_column >= watermark`). For **helper-driven** tables (see §1) these
  predicates run against the joined helper's columns instead, and the watermark is
  captured from the helper via the join — the write disposition, compound key, and
  partitioning are otherwise identical.
- **`insert_at` carry-forward**: a `merge` replaces a matched row wholesale, which
  would reset `insert_at`. So before each incremental merge the loader reads the
  existing `insert_at` for the rows being touched (scoped to this run's branches
  via the `BRANCH_ID` partition, key + `insert_at` columns only) and carries it
  forward, so `insert_at` keeps each row's original first-load time while
  `Recorded_updated_at` tracks the latest. On a full `replace` (INITIAL across all
  branches) the table is rebuilt, so `insert_at` is (re)set to that load's time.
- **Partitioning**: every data table is partitioned by **`BRANCH_ID`** (Iceberg
  identity transform), so each branch's rows live in their own partition —
  branch-scoped reads prune to a single partition and per-branch merges only
  rewrite that branch's files. The partition spec is set when a table is first
  created; tables that already exist unpartitioned must be recreated (re-run
  `--mode INITIAL` after clearing the dataset) to pick it up.
- **Snapshot retention**: after each load, every Iceberg table is configured with
  `history.expire.max-snapshot-age-ms` (default **7 days**) and snapshots older
  than that window are expired, so metadata/manifests don't grow unbounded.
  `min-snapshots-to-keep` always retains the current snapshot. Tune via
  `snapshot_expire_days` / `snapshot_min_to_keep` (or disable with
  `snapshot_maintenance = false`) in `.dlt/config.toml`.

### 6. Observability
- **`etl_control`** (Iceberg, merged on `table_name + branch_id`): per-table,
  per-branch CDC state — `last_cdc_value`, `last_date_value`, status, row count,
  duration, timestamps.
- **`etl_run_log`** (Iceberg, append): one row per `(run, table, branch)` with
  `pipeline_run_id`, row count, start/end, `duration_ms`, status, attempts,
  `write_disposition`, and schema discrepancies.
- The authoritative CDC watermark store is the local `control_state.json`
  (fast/transactional); `etl_control` is its queryable Iceberg mirror.

## Output

Each table becomes one Iceberg dataset under the destination
(`<bucket_url>/<dataset_name>/<table>`) holding all 7 branches, alongside the
`etl_control` and `etl_run_log` Iceberg tables in the same dataset.

## Data-quality checks (`dq_check.py`)

A standalone reconciliation app that compares the **Oracle source** against the
**Iceberg lake**, per branch, and writes its findings back into the same dataset
as a new Iceberg table. It runs two checks for every `(table, branch)` over **one
shared window**, so the count delta and the hash delta describe the same row set:

1. **Row-count comparison** — windowed `COUNT(*)` on Oracle vs the number of rows
   in the Iceberg branch partition in the same window, and their delta.
2. **Row-hash delta** — a per-row content **hash** (over the *common* business
   columns) is computed on both sides, joined on the table's unique key, and the
   rows are bucketed into `matched` / `only_in_oracle` / `only_in_iceberg` /
   `hash_mismatch`. This catches content drift a bare count would miss.

### Window: YTD → last run

The window runs from `--since` (default **Jan 1 of the current year** — YTD) up to
each `(table, branch)`'s **last-run watermark** in `control_state.json`
(`--until` overrides it). Both checks use this same window.

- **Master tables** (no date column) are compared in full.
- **Helper-driven tables** whose watermark is the *helper's* column (not their own
  date column) skip the upper bound rather than apply a watermark in the wrong
  units — the result row's `window_note` records this.
- Numeric/Julian date columns (e.g. `APPOINTMENTS.JULIAN_DATE`) are windowed with
  the equivalent Julian-day literal on both sides.

### How the hash stays comparable across engines

The source is read through the **same native-Arrow fetch the pipeline uses**
(`connection.fetch_df_batches`), so Oracle `NUMBER` arrives as the same Arrow
`double` the lake stores and dates as the same `timestamp`. A canonicalizer then
erases the only differences that remain so equal values hash equal: the lake's
`timestamp[tz=UTC]` and the source's naive timestamp (same wall clock) both
normalize to second-resolution `YYYY-MM-DD HH:MM:SS`, and decimal scale is
normalized (`123.40` ≡ `123.4000`). Columns present on only one side (e.g. a
cross-branch schema-union column a given branch lacks) are reported in
`columns_only_in_*` and excluded from the hash.

### Run the checks

```bash
# All tables, all branches: write etl_dq_results + print the summary
python dq_check.py

# Counts only (skip the heavier hash pull), scoped to branches/tables
python dq_check.py --branch jazan,khamis --tables APPOINTMENTS --no-hash

# Explicit window, also dump a CSV, and don't write the Iceberg table
python dq_check.py --since 2026-06-01 --until 2026-06-23 --csv exports --no-write

# Offline smoke test: reconcile the lake against the staged parquet (no Oracle)
python dq_check.py --self-test
```

Branches run in parallel (one Oracle connection each); the process exits non-zero
if any unit is `MISMATCH` or `ERROR`, so it drops into CI/cron cleanly. `--help`
lists every flag.

### Result table — `etl_dq_results`

Written as an **append** Iceberg table in the same dataset (next to `etl_control`
/ `etl_run_log`). One row per `(run, table, branch)` with the window bounds, the
count check (`oracle_row_count`, `iceberg_row_count`, `row_count_delta`), the hash
check (`hash_matched`, `hash_only_in_oracle`, `hash_only_in_iceberg`,
`hash_mismatch`, `hash_total_delta`, `hash_columns`), the `columns_only_in_*`
drift lists, a `status` (`OK` / `MISMATCH` / `ERROR` / `SKIPPED`), and
`error_details`. Each run is tagged with a `dq-…` `pipeline_run_id`.

## GUI & scheduling

A Flask control panel lives in `gui/` and is launched with `python gui/app.py`
(or via `setup.sh` / `setup.cmd`). See `gui/README.md` for full details.

### Scheduling model

Scheduling uses **Dagster** rather than cron:

1. **Pipeline library** (`/pipelines`) — define named pipeline specs (run command
   and parameters) stored in `gui/state/pipelines.json`.
2. **Flow (DAG) builder** (`/flows`) — compose pipelines into a Dagster job, set a
   cron schedule / timezone, and configure email alerts.
3. **Dagster scheduling** — the GUI auto-starts Dagster on launch. To opt out:
   `OASIS_DAGSTER_AUTOSTART=0`. To change the Dagster UI port:
   `OASIS_DAGSTER_PORT=<port>` (default 3000).

One-time setup (after `pip install -r requirements.txt`):

```bash
pip install -e orchestrator
```

### SMTP / email alerts

Configure outbound email (for Flow alert notifications) in the Connections page
under **SMTP / email settings**, or via `PUT /api/smtp`. The password is
write-only and never returned to the browser.
