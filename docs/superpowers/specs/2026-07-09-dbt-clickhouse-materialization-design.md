# dbt-core materialization layer: Iceberg → ClickHouse

**Date:** 2026-07-09
**Status:** Approved (design)
**Scope:** add a dbt-core materialization stage to the OASIS pipeline and its GUI control panel — a new Models & Tests page, a `dbt` command type, dbt-aware flow nodes, and app-owned dbt/ClickHouse configuration.

## Summary

Extend the pipeline with a **materialization layer**: `Oracle → Iceberg (existing) → ClickHouse (new)`.

dbt-core plus the `dbt-clickhouse` adapter run models that read the **local Iceberg** tables (via ClickHouse's `icebergLocal(...)` table function) and materialize them into **native ClickHouse tables**. Five deliverables, mapped to the request:

1. **Setup** — `dbt-core` + `dbt-clickhouse` installed by the existing setup scripts.
2. **Models & Tests page** — browse the dbt project, list models & tests, create/edit `.sql`/`.yml` in-browser, save to the project dir.
3. **Run / test / debug** — run models and tests from the UI with live streamed output.
4. **Orchestration** — a flow node can directly reference a dbt model or test (`kind: "dbt"`).
5. **Global config** — all ClickHouse + dbt parameters live in the app's config (`.dlt/secrets.toml` + `.dlt/config.toml`), from which the app generates `profiles.yml`.

The design routes dbt through the app's single command seam — `gui/commands.py::build_argv`, which `orchestrator/state.py` re-exports — so "run now," saving, live-log streaming, and Dagster execution all reuse existing machinery instead of a parallel dbt runner.

### Key decisions (from brainstorming)

- **ClickHouse is external**: already installed/running, path-sharing `iceberg_output/`. The app connects only; setup installs the Python adapter, not the server.
- **dbt as a flow node**: a **direct model/test picker** in the flow editor (a new node `kind`), not "save a pipeline first."
- **Model authoring**: a **full in-browser editor** (list, create-from-template, edit files, save, run/test).
- **Iceberg references are fully manual**: no source auto-discovery or macro; the user hand-writes `icebergLocal('<path>')`. Ship exactly one example model as documentation.
- **Config is app-owned**: creds in `.dlt/secrets.toml`, non-secret dbt settings in `.dlt/config.toml [dbt]`; the app **generates** `profiles.yml` from them (single source of truth = app config).
- **Defaults**: project dir `dbt/`; default materialization `table`; exposed dbt commands = `run, test, build, compile, debug`.

### Hard constraint (call out to operators)

`icebergLocal('<path>')` reads from the **ClickHouse server's own filesystem**. Because ClickHouse is external, every path written in a model must be valid **on the ClickHouse host**, not (necessarily) on the machine running this app. The app cannot validate that path — it is the operator's responsibility. This is documented in the Models page, the example model, and the README. ClickHouse must be **24.x+** for `icebergLocal` support.

## Context (current architecture the design builds on)

- **One command seam.** `gui/commands.py::build_argv(spec) -> (argv, label)` maps a UI spec dict to an argv. `SCRIPT_CHOICES` lists the script types (`oracle_to_iceberg`, `dq_check`, `snapshot_diff`, `fresh_run`, `custom`). `orchestrator/src/orchestrator/state.py` does `build_argv = _commands.build_argv`, so the Run page and every Dagster asset run the *same* argv.
- **Run page** (`gui/templates/run.html`) builds a spec, previews it (`/api/command/preview`), runs it now via `RunManager` (`gui/pipeline_runner.py`, streamed output), or saves it as a Pipeline.
- **Pipelines** (`gui/pipelines_store.py` → `gui/state/pipelines.json`): named saved specs. Each becomes one Dagster asset.
- **Flows** (`gui/flows_store.py` → `gui/state/flows.json`): a DAG of **nodes**; today each node is `{node_id, pipeline_id, deps[]}`. `orchestrator/build.py` + `assets.py` turn each flow into assets + a job + a schedule + email sensors; `assets.build_asset` runs `state.build_argv(spec)` as a subprocess, streams stdout to the Dagster log, and fails on non-zero exit.
- **Global config.** `.dlt/config.toml` holds the `[etl]` block; `gui/workspace.py::update_etl_settings` edits only keys already present and on the `EDITABLE_ETL_KEYS` allowlist, keeping a timestamped backup and re-parsing before commit. `run.html` mirrors that allowlist in JS (`SET_EDITABLE`/`SET_NUMERIC`/`SET_SHOWN`). Secrets (Oracle branches) live in `.dlt/secrets.toml`, managed by `gui/connections.py` with passwords redacted in the UI.
- **Iceberg lake.** Local files under `iceberg_output/oasis/<table>/{data,metadata}`; `gui/iceberg_browser.py` lists them (used to populate table pickers).
- **Pages** are registered in `gui/app.py` (`@app.route("/...")` → `render_template`) with a nav in `gui/templates/base.html`; API handlers are wrapped by `@api` (exceptions → JSON errors). Front-end helpers (`el`, `apiGet/apiPost/apiPut/apiDel`, `ok`, `err`, `pill`, `esc`) live in `gui/static/app.js`.

## Part 1 — Setup & dependencies (req 1)

- Add to `requirements-gui.txt` (picked up unchanged by `setup.sh` / `setup.ps1`, which already `pip install -r requirements-gui.txt`):
  - `dbt-core` and `dbt-clickhouse`, pinned to a compatible pair (exact versions chosen at implementation; both track the same minor, e.g. `dbt-core~=1.9` with the matching `dbt-clickhouse`).
- Setup scripts: add a printed reminder that **ClickHouse 24.x+ is an external prerequisite** and must be able to read the `iceberg_output/` path used in `icebergLocal(...)`. No server install.
- `README.md`: a "dbt → ClickHouse materialization" section covering the prerequisite, the `icebergLocal` filesystem constraint, and where config lives.
- `.gitignore`: add `dbt/profiles.yml`, `dbt/target/`, `dbt/logs/`, `dbt/dbt_packages/`.

## Part 2 — dbt project scaffold

New `dbt/` directory at repo root:

- `dbt_project.yml` — `name: oasis`, `profile: oasis`, standard `model-paths: ["models"]`, `test-paths: ["tests"]`, `macro-paths: ["macros"]`, `target-path: "target"`; `models.oasis.+materialized:` from `[dbt].default_materialization` (default `table`).
- `profiles.yml` — **generated** by the app (git-ignored; holds the password). Never hand-edited.
- `models/example_iceberg_clickhouse.sql` — the single shipped example, demonstrating the pattern:
  ```sql
  -- Materialize a local Iceberg table into a native ClickHouse table.
  -- NOTE: the path below must be readable BY THE CLICKHOUSE SERVER, not this app host.
  {{ config(materialized='table') }}
  select * from icebergLocal('/abs/path/on/clickhouse/iceberg_output/oasis/product_base')
  ```
- `models/example_iceberg_clickhouse.yml` — a schema `.yml` with one example test (e.g. `not_null`) so the Tests list is populated on day one.
- Empty `tests/`, `macros/` dirs (with `.gitkeep`).

No sources.yml / macros wrapping `icebergLocal` (fully-manual decision).

## Part 3 — Global configuration (req 5): app-owned, generates `profiles.yml`

### 3a. ClickHouse credentials — `.dlt/secrets.toml [clickhouse]`

New module `gui/clickhouse_config.py` (mirrors `gui/connections.py` conventions):

- Reads/writes a single `[clickhouse]` section: `host`, `port` (default `8123`), `user` (default `default`), `password`, `database` (default `default`), `secure` (bool, default `false`), `connect_timeout` (default `10`).
- `get_clickhouse()` returns the section with **password redacted** (`has_password: bool`), like `list_branches()`.
- `save_clickhouse(body)` writes the section, preserving an existing password when the field is left blank (same pattern connections use).
- `test_connection()` runs `dbt debug` against the generated profile and returns pass/fail + captured output.

### 3b. dbt settings — `.dlt/config.toml [dbt]`

New non-secret block, editable through the existing allowlist mechanism:

- Keys: `project_dir` (default `"dbt"`), `target` (default `"dev"`), `threads` (default `4`), `default_materialization` (default `"table"`), `dbt_executable` (default `"dbt"`).
- Extend `gui/workspace.py`: factor the existing in-place `[etl]` editor into an internal helper `_update_toml_block(section, allowlist, updates)` (same in-place / allowlisted / backed-up / re-parsed behavior), then have both `update_etl_settings` and a new `update_dbt_settings` call it with their own section + allowlist. Add a `dbt_settings()` reader alongside `etl_settings()`.
- Add the `[dbt]` block to `.dlt/config.toml` with explanatory comments so the in-place editor can find each key.

### 3c. profiles.yml generation — `gui/dbt_config.py`

- `render_profiles()` builds the `profiles.yml` dict from `[clickhouse]` + `[dbt]`:
  ```yaml
  oasis:
    target: dev
    outputs:
      dev:
        type: clickhouse
        host: ...
        port: ...
        user: ...
        password: ...
        schema: <database>
        secure: <bool>
        connect_timeout: ...
        threads: <threads>
  ```
- `write_profiles()` writes it to `<project_dir>/profiles.yml` atomically (temp + replace).
- Called **on config save** and **before every dbt run** (so a stale profile can't drift from app config). This is the single source of truth: app config → generated `profiles.yml`.

### 3d. Config paths

Add to `gui/config.py`: `DBT_DIR = REPO_ROOT / "dbt"` (resolved from `[dbt].project_dir` when set), `DBT_PROFILES = DBT_DIR / "profiles.yml"`, and a `dbt_executable()` helper (honours `[dbt].dbt_executable`, falls back to `"dbt"` on PATH).

## Part 4 — Command layer: the `dbt` script type

`gui/commands.py`:

- Add `"dbt"` to `SCRIPT_CHOICES`.
- Spec shape:
  ```json
  { "script": "dbt", "dbt_command": "run",
    "select": "<dbt selector>", "full_refresh": false, "extra": "" }
  ```
  - `dbt_command ∈ {run, test, build, compile, debug}` (validated; default `run`).
  - `select` optional for `build`/`test`/`compile`, ignored for `debug`.
- `build_argv` branch for `dbt`:
  - `argv = [dbt_executable(), <dbt_command>, "--project-dir", <DBT_DIR>, "--profiles-dir", <DBT_DIR>, "--target", <target>]`
  - append `--select <select>` when set; `--full-refresh` when `full_refresh` and command is `run`/`build`.
  - append `_split(spec.get("extra"))`.
  - label = e.g. `dbt run <select>`.
- **Before returning a dbt argv, ensure `profiles.yml` is current** by calling `dbt_config.write_profiles()`. (Keeps run-now, saved-pipeline, and orchestrator paths all consistent, since all three go through `build_argv`.) Guard so a missing `[clickhouse]` section raises a clear `ValueError` ("configure ClickHouse first") rather than emitting a broken profile.

Because `orchestrator/state.py` re-exports `build_argv`, dbt nodes execute through the identical seam with no orchestrator-specific command code.

## Part 5 — Models & Tests page (req 2 & 3)

### 5a. Backend — `gui/dbt_project_store.py`

- **Path safety**: every file operation resolves the target and asserts it stays within `DBT_DIR` (reuse the `Path(...).resolve()` containment check pattern from `workspace.read_log_file`). Only `.sql`/`.yml`/`.yaml` under `models/`, `tests/`, `macros/` are readable/writable.
- `list_models()` / `list_tests()`: prefer `dbt ls --resource-type model|test --output json` (parsed); fall back to a filesystem scan if `dbt ls` fails (e.g. compile error), so the list still renders. Each entry: `name`, `relative path`, `resource_type`, `tags` (best-effort).
- `read_file(rel)` / `write_file(rel, content)` (atomic write) / `create_from_template(name, kind, materialization)` / `delete_file(rel)`.
- Templates: a **model** template (the `icebergLocal` example with the operator-path warning comment) and a **schema-test** `.yml` stub.

### 5b. API — `gui/app.py`

New `@api` routes:
- `GET /api/dbt/models`, `GET /api/dbt/tests` — the lists.
- `GET /api/dbt/file?path=…`, `PUT /api/dbt/file` — read/write a file.
- `POST /api/dbt/file` — create-from-template; `DELETE /api/dbt/file` — delete.
- `GET /api/dbt/config`, `PUT /api/dbt/config` — the `[dbt]` block (via workspace) + `[clickhouse]` (via clickhouse_config).
- `POST /api/dbt/test-connection` — `dbt debug`.
- `POST /api/dbt/run` — build a dbt spec → `runner.start(argv)` (reuses `RunManager`), returns the run id so the page can tail it exactly like the Run page does.

### 5c. Frontend — `gui/templates/dbt.html` (+ nav in `base.html`, page route in `app.py`)

- Layout mirrors the app's existing two-column panels:
  - **ClickHouse + dbt settings** panel at top (edit/save; "Test connection" button → `dbt debug` output), mirroring the Run page's ETL-settings panel and reusing its JS conventions.
  - **Left**: two lists — *Models* and *Tests* — each item selectable; a "New model" / "New test" button.
  - **Right**: a code editor (textarea styled `mono`, consistent with the app's no-CDN/self-contained approach — no external editor CDN) with **Save**, plus action buttons **Run** (`dbt run --select <sel>`), **Test** (`dbt test --select <sel>`), **Compile**, **Debug**. Actions stream into a live console reusing the Run page's tail loop (`/api/runs/<id>/tail`).
- New nav entry "Models" (a `fa-cubes`-style icon) added to `base.html`, and `@app.route("/models")` → `render_template("dbt.html", active="models")` in `app.py`.

## Part 6 — Orchestration integration (req 4): the `dbt` node kind

### 6a. Flow node schema

Nodes gain an optional `kind`:
- `kind: "pipeline"` (default when absent — **backward compatible** with existing `flows.json`): `{node_id, kind, pipeline_id, deps[]}`.
- `kind: "dbt"`: `{node_id, kind, dbt: {dbt_command, select}, deps[]}`.

### 6b. Validation — `gui/flows_store.py`

- `validate_flow`: branch on `kind`.
  - pipeline node → require `pipeline_id ∈ known_pipeline_ids` (unchanged path).
  - dbt node → require `dbt.dbt_command ∈ {run, test, build}` and non-empty `dbt.select`.
  - `node_id` regex, duplicate-id, deps-exist, and `_assert_acyclic` checks unchanged.
- `referencing_flows(pipeline_id)` only matches pipeline nodes (dbt nodes never reference a pipeline), so pipeline-deletion guards still work.

### 6c. Orchestrator build — `orchestrator/assets.py` + `build.py`

- `build.py::_build_flow`: for each node, branch on `kind`. For a dbt node, synthesize the spec `{"script":"dbt", "dbt_command":…, "select":…}` and pass it to `assets.build_asset` (which already takes a `spec` and calls `state.build_argv`). Asset name uses the node's `dbt.select` (or `node_id`) as the description.
- `assets.build_asset` is essentially unchanged — it already runs whatever argv `build_argv(spec)` returns. `asset_key(flow_id, node_id)` and dep wiring are identical.
- A dbt node whose selector is empty/invalid is skipped with a logged warning (same resilience as the existing per-flow try/except).

### 6d. Flow editor — `gui/templates/flows.html`

- The node add/edit UI gains a **kind toggle**: *Pipeline* (existing pipeline dropdown) or *dbt* (a `dbt_command` select + a model/test picker populated from `/api/dbt/models` and `/api/dbt/tests`). Serializes to the schema in 6a.
- Node rendering shows the pipeline name or `dbt <command> <select>` accordingly.

## Data flow (end to end)

```
Oracle 11g ──(oracle_to_iceberg, existing)──▶ iceberg_output/oasis/<table>/
                                                        │
                              icebergLocal('<path on CH host>')   ← read by ClickHouse
                                                        │
   dbt run --select <model>  ──(dbt-clickhouse)──▶  native ClickHouse table
        ▲                                                   ▲
        │ build_argv({script:"dbt", …})                     │ profiles.yml (generated from
        │                                                    │ [clickhouse] + [dbt])
   Run page / Models page / Dagster dbt-node  ───────────────┘
```

## Testing

- `commands.build_argv`: dbt argv for each `dbt_command`, `--select`, `--full-refresh`, `--project-dir/--profiles-dir/--target`, and the "no `[clickhouse]` configured" `ValueError`.
- `dbt_config.render_profiles`: correct YAML shape; password present in the written file but redacted via `clickhouse_config.get_clickhouse()`.
- `dbt_project_store`: `list_models/list_tests` (json path + filesystem fallback), `create_from_template`, and **path-traversal rejection** (`../`, absolute paths, disallowed extensions).
- `flows_store.validate_flow`: accepts a valid dbt node, rejects empty `select` / bad `dbt_command`, and still enforces acyclicity with mixed node kinds; existing pipeline-node flows (no `kind`) still validate.
- `orchestrator/build.build_all_defs`: a flow with a dbt node builds an asset without importing Flask; a bad dbt node is skipped, not fatal.
- Connection sanity is exercised by `dbt debug` behind `POST /api/dbt/test-connection` (manual/integration; no live ClickHouse in unit tests).

## Out of scope (YAGNI)

- Installing or managing the ClickHouse server (external, connect-only).
- Auto-discovering Iceberg tables as dbt sources or an `icebergLocal` macro (fully-manual decision).
- dbt docs site, exposures, snapshots, seeds, or `dbt-clickhouse` distributed/replicated engine tuning.
- Editing dbt-node specs by "saving a pipeline first" — the flow editor references models/tests directly.
- Validating that an `icebergLocal(...)` path exists on the ClickHouse host (operator responsibility; surfaced in UI/docs).

## Files touched (summary)

**New**: `gui/clickhouse_config.py`, `gui/dbt_config.py`, `gui/dbt_project_store.py`, `gui/templates/dbt.html`, `dbt/dbt_project.yml`, `dbt/models/example_iceberg_clickhouse.{sql,yml}`, `dbt/{tests,macros}/.gitkeep`, tests under `tests/`.
**Edited**: `requirements-gui.txt`, `setup.sh`, `setup.ps1`, `README.md`, `.gitignore`, `gui/config.py`, `gui/commands.py`, `gui/workspace.py`, `gui/app.py`, `gui/templates/base.html`, `gui/templates/flows.html`, `gui/flows_store.py`, `.dlt/config.toml`, `orchestrator/src/orchestrator/build.py` (and `assets.py` if the description field needs the dbt branch).
