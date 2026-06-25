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

- **Dashboard** — workspace health from `control_state.json`, branches, `[etl]` settings.
- **Run** — build an `oracle_to_iceberg` / `dq_check` / `snapshot_diff` / custom
  command (mode, category, branch/table filters, …), run it, and watch live output.
- **Schedule** — define recurring jobs and install them into the Ubuntu `crontab`.
  Job definitions live in `gui/state/schedules.json` (the source of truth); on a
  host with `cron`, **Apply to crontab** renders them into a managed block
  (hand-written cron lines are preserved). On Windows the block is shown for you
  to copy onto the Ubuntu host.
- **Monitor** — tail run/cron log files (`run_logs/`) and view the Iceberg
  observability tables `etl_run_log`, `etl_dq_results`, `etl_control`.
- **Tables** — structured add/edit/delete + raw-JSON editor for `tables.json`,
  with validation that mirrors the pipeline loader. Saves keep a timestamped backup.
- **Iceberg** — list datasets under `iceberg_output/oasis`; inspect schema,
  partition spec, snapshot history, sample rows, and per-branch counts.

## Notes

- Runs execute with the working directory set to the **repo root**, so they read
  the same `tables.json` / `.dlt` / `control_state.json` as the CLI.
- The panel never exposes branch passwords from `.dlt/secrets.toml`.
- This is an **admin tool meant for trusted/local use** (it can launch commands).
  Keep it bound to `127.0.0.1` unless you put authentication in front of it.
- Generated artefacts (`run_logs/`, `gui/state/`) are git-ignored.
