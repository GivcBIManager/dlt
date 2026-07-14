# HNH ETLPipeline Manager (GUI)

A small Flask web UI for the Oracle → Iceberg pipeline. It lets you **run**,
**monitor**, and **orchestrate** the pipeline via Dagster Flows, edit
**`tables.json`**, and browse the **Iceberg lake** — all from a browser.

## Quick start

From the repo root:

```bash
# Ubuntu / Linux / macOS
./setup.sh

# Windows
setup.cmd          # or:  powershell -ExecutionPolicy Bypass -File setup.ps1
```

The script creates `.venv`, installs `requirements-gui.txt` (the full pipeline
stack + Flask), and opens the panel at <http://127.0.0.1:8765>.

Already have a venv? Just run:

```bash
python gui/app.py
```

### One-time orchestrator install

```bash
pip install -e orchestrator
```

This installs the Dagster workspace that backs the Flows page. The GUI
auto-starts Dagster on launch.

### Environment knobs

| Variable                  | Default     | Purpose                                           |
|---------------------------|-------------|---------------------------------------------------|
| `OASIS_GUI_HOST`          | `127.0.0.1` | Bind address (use `0.0.0.0` for LAN)              |
| `OASIS_GUI_PORT`          | `8765`      | HTTP port                                         |
| `OASIS_GUI_DEBUG`         | `0`         | `1` enables Flask debug (loopback binds only)     |
| `OASIS_GUI_USER`          | _(unset)_   | Login username; **required** (with `OASIS_GUI_PASSWORD`) for any non-loopback bind. Remote browsers sign in at `/login`; loopback clients need no login. |
| `OASIS_GUI_PASSWORD`      | _(unset)_   | Login password. The old `OASIS_GUI_TOKEN` / `?token=` URL authentication has been removed and no longer works. |
| `OASIS_ALLOW_CUSTOM_CMD`  | `0`         | `1` permits the free-form `custom` run script (arbitrary argv); off by default |
| `OASIS_PYTHON`            | venv python | Interpreter used to launch pipeline runs          |
| `OASIS_DAGSTER_AUTOSTART` | `1`         | Set to `0` to skip auto-starting Dagster          |
| `OASIS_DAGSTER_HOST`      | GUI host    | Bind address for the embedded Dagster UI; follows `OASIS_GUI_HOST` so remote clients can open it. **Dagster has no auth of its own** — override to `127.0.0.1` to keep it local. |
| `OASIS_DAGSTER_PORT`      | `3000`      | Port for the embedded Dagster UI                  |

## Pages

- **Dashboard** — workspace health from `control_state.json`, branches, `[etl]`
  settings. The per-table/branch state table has table/branch/status dropdown
  filters, and the `[etl]` settings cards are editable in place (writes
  `.dlt/config.toml`, keeping a timestamped backup).
- **Connections** — create / edit / delete the Oracle branch connections in
  `.dlt/secrets.toml` and test connectivity. Edits are surgical (only the
  `[oracle_branches.*]` block is rewritten; comments and other sections are
  preserved) and a backup is kept. Passwords are write-only — never sent back to
  the browser. Also hosts the **SMTP settings** form (host, port, credentials,
  from address, TLS toggle) and a test-email button; bound to `GET/PUT /api/smtp`.
- **Run** — build an `oracle_to_iceberg` / `dq_check` / `snapshot_diff` / custom
  command (mode, category, branch/table filters, extra-args picker, …), run it,
  and watch live output.
- **Pipelines** — define and manage named pipeline specs (the building blocks for
  Flows). Each pipeline wraps a run command configuration stored in
  `gui/state/pipelines.json`.
- **Flows** — compose Dagster DAGs from pipeline nodes, set a cron schedule and
  timezone, configure email alerts, and enable/disable schedules. The GUI
  auto-starts Dagster (`OASIS_DAGSTER_AUTOSTART=0` to opt out,
  `OASIS_DAGSTER_PORT` to change the Dagster UI port). Scheduling model:
  **Pipeline library → Flow (DAG) builder → Dagster scheduling**.
- **Monitor** — tail run log files (`run_logs/`), purge logs older than a chosen
  date, and view the Iceberg observability tables. `etl_dq_results` adds a date
  filter and a branch × table summary; `etl_run_log` / `etl_control` add
  date/table/branch/status filters.
- **Tables** — structured add/edit/delete + raw-JSON editor for `tables.json`,
  with validation that mirrors the pipeline loader. The masters/transactions
  containers scroll and have a wildcard name filter. Saves keep a backup.
- **Iceberg** — datasets under `iceberg_output/oasis` as a table; inspect schema,
  partition spec and snapshot history (snapshot ids link to the sample tab),
  sample rows (branch dropdown + date filter, horizontal scroll, CSV export that
  ignores the preview row limit), and branch/date/both count aggregation.

## Notes

- Runs execute with the working directory set to the **repo root**, so they read
  the same `tables.json` / `.dlt` / `control_state.json` as the CLI.
- The panel never exposes branch passwords from `.dlt/secrets.toml`.
- This is an **admin tool meant for trusted/local use** (it can launch commands).
  Keep it bound to `127.0.0.1` unless you put authentication in front of it.
- Generated artefacts (`run_logs/`, `gui/state/`) are git-ignored.
