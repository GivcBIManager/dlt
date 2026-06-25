# HNH ETLPipeline Manager (GUI)

A small Flask web UI for the Oracle → Iceberg pipeline. It lets you **run**,
**schedule** (cron), and **monitor** the pipeline, edit **`tables.json`**, and
browse the **Iceberg lake** — all from a browser.

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

### Environment knobs

| Variable           | Default     | Purpose                                   |
|--------------------|-------------|-------------------------------------------|
| `OASIS_GUI_HOST`   | `127.0.0.1` | Bind address (use `0.0.0.0` for LAN)      |
| `OASIS_GUI_PORT`   | `8765`      | HTTP port                                 |
| `OASIS_GUI_DEBUG`  | `0`         | `1` enables Flask debug                   |
| `OASIS_PYTHON`     | venv python | Interpreter used to launch pipeline runs  |

## Pages

- **Dashboard** — workspace health from `control_state.json`, branches, `[etl]`
  settings. The per-table/branch state table has table/branch/status dropdown
  filters, and the `[etl]` settings cards are editable in place (writes
  `.dlt/config.toml`, keeping a timestamped backup).
- **Connections** — create / edit / delete the Oracle branch connections in
  `.dlt/secrets.toml` and test connectivity. Edits are surgical (only the
  `[oracle_branches.*]` block is rewritten; comments and other sections are
  preserved) and a backup is kept. Passwords are write-only — never sent back to
  the browser.
- **Run** — build an `oracle_to_iceberg` / `dq_check` / `snapshot_diff` / custom
  command (mode, category, branch/table filters, extra-args picker, …), run it,
  and watch live output. **Command generation lives here**: *Schedule this…*
  carries the built command to the Schedule page.
- **Schedule** — pick *when* a command (built on the Run page) recurs via a
  per-field cron builder (no presets), and review **All schedules** with their
  status (enabled, live-in-crontab, last log write/size). Job definitions live in
  `gui/state/schedules.json`; **Apply** renders enabled jobs into a managed
  crontab block (hand-written lines preserved). On Windows the block is shown to
  copy onto the Ubuntu host.
- **Monitor** — tail run/cron log files (`run_logs/`), purge logs older than a
  chosen date, and view the Iceberg observability tables. `etl_dq_results` adds a
  date filter and a branch × table summary; `etl_run_log` / `etl_control` add
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
