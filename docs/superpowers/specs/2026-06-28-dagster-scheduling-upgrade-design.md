# Dagster Scheduling Upgrade — Design

- **Date:** 2026-06-28
- **Status:** Approved (design); pending implementation plan
- **Component:** HNH ETLPipeline Manager GUI (`gui/`) + new `orchestrator/` Dagster code location

## 1. Summary

Replace the current cron-based scheduling module with a Dagster-backed
orchestration layer. Users compose **DAGs ("Flows")** in the GUI from
**pre-configured pipelines** (saved, parameterized command specs), wire
dependencies between them, schedule the whole DAG, and receive success/failure
emails on every run. The GUI supervises a local Dagster instance (webserver +
daemon), lists the resulting jobs with live status, and deep-links each into the
Dagster UI. Everything runs in the existing `.venv` on both Windows and Linux.

This supersedes `gui/cron_manager.py` + `gui/templates/schedule.html`, which only
worked on Linux (crontab) and could not express multi-step DAGs, dependencies, or
notifications.

## 2. Goals & Requirements

From the request:

1. Build pipeline-execution **flows (DAGs)** in the UI: select one or more
   pre-configured pipelines as **assets**, define dependencies, and schedule the
   whole DAG so each step triggers the next **on success**.
2. Each DAG run sends **email on success and on failure**.
3. The app **builds the Dagster definitions** from user-configurable parameters
   (no hand-written Dagster code per DAG).
4. **List the Dagster jobs** created, with live status from Dagster, plus a
   **hyperlink** to each job/run in the Dagster UI.
5. **Dagster lives in the app `.venv`**; works on **Windows and Linux**.

### Non-goals (YAGNI)

- Distributed / multi-host execution, Dagster+ / Dagster Cloud.
- Partitions, backfills, asset freshness / declarative automation.
- Per-run ad-hoc configuration beyond saved pipeline parameters.
- Rewriting the ETL entry-points to run in-process (assets subprocess them).

## 3. Decisions (from brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Runtime model | **GUI supervises Dagster** | One command launches everything; cross-platform via existing setup scripts. Scheduled runs pause if the GUI is down (acceptable for a single-host control panel). |
| Definitions model | **Dynamic from JSON** | DAGs stored as JSON; one module builds all Dagster objects at load. No codegen, no stale files, single source of truth. |
| Asset execution | **Subprocess existing scripts** | Reuses the proven `commands.build_argv` path; identical behavior to the Run page; preserves the pipeline's memory/tuning profile. Success/failure = exit code. |
| Email config | **Global SMTP, per-DAG recipients** | SMTP set once in `secrets.toml`; each flow has its own success/failure recipient lists. |
| Building blocks | **New saved Pipeline library** | Named, reusable, parameterized pipelines become assets across multiple flows. |
| Old scheduler | **Replaced** | Dagster is the single, cross-platform scheduler. |
| Code location | **Scaffolded via `create-dagster`** (not hand-rolled) | Per dagster-expert guidance: never hand-create a Dagster project; the scaffold yields a correct package + `pyproject.toml` + code-location entry point. |

## 4. Core Concepts

Three new concepts layered on the existing app:

1. **Pipeline (library item)** — a named, saved command spec, built with the
   existing Run-page form (`script`, `mode`, `category`, `branch`, `tables`,
   flags). Reuses `commands.build_argv` verbatim. Stored in
   `gui/state/pipelines.json`. Each pipeline becomes **one Dagster asset**.
   Example: `masters-patient-incr` →
   `python oracle_to_iceberg.py --mode INCREMENTAL --category masters --tables PATIENT_MASTER_DATA --log-level INFO`.

2. **Flow (the DAG)** — a named set of pipelines chosen as assets, plus:
   - dependency **edges** (which node depends on which),
   - a **cron schedule** + IANA **timezone**,
   - **email recipients** (success list, failure list) and a "notify on" toggle set,
   - an `enabled` flag.
   Stored in `gui/state/flows.json`.

3. **Dagster bridge** — the supervised Dagster instance plus the dynamic
   definitions module that turns `pipelines.json` + `flows.json` into Dagster
   assets/jobs/schedules/sensors.

> Note: `gui/state/` is gitignored today (runtime state), so `pipelines.json` and
> `flows.json` follow the same convention as `schedules.json` — local source of
> truth, not committed.

## 5. How a Flow maps to Dagster

For each flow, `build_all_defs()` produces (all programmatically, by iterating
JSON):

- **One asset per node** — built by an **asset factory**. Asset key
  `flow__<flow_id>__<node_id>`, `group_name="flow_<flow_id>"`. The asset body:
  1. resolves the pipeline's argv via `commands.build_argv(spec)`,
  2. subprocesses it (venv `python_executable()`, `cwd=REPO_ROOT`), streaming
     stdout/stderr into `context.log`,
  3. raises on non-zero exit (→ asset/run FAILURE),
  4. returns `dg.MaterializeResult` with metadata (exit code, duration, command
     line, log path).
  Edges become `deps=[dg.AssetKey([...])]` (the `deps=` model — no data passed
  between assets; each asset does its own work). Because Dagster runs an asset
  job in topological order and only starts a downstream asset after its upstreams
  **succeed**, this *is* the "trigger each other on success" requirement — no
  custom wiring.

- **One asset job** — `dg.define_asset_job("flow_<flow_id>", selection=<that flow's assets>)`.

- **One schedule** — `dg.ScheduleDefinition(job=..., cron_schedule=flow.cron,
  execution_timezone=flow.tz, default_status=RUNNING if flow.enabled else STOPPED)`.

- **Two email sensors** — `@dg.run_status_sensor(run_status=SUCCESS,
  monitored_jobs=[job])` and `@dg.run_failure_sensor(monitored_jobs=[job])`,
  each sending one email per run to the flow's recipient lists via the shared
  SMTP config. (Custom sensors rather than the built-in
  `make_email_on_run_failure_sensor`, because we need success **and** failure and
  our own SMTP settings/body.) One email per DAG run satisfies "email on success
  and failure on each run."

### Acyclic guarantee

The flow graph must be a DAG. `flows_store` validates there are no cycles before
saving; the builder additionally skips/flags any flow that fails validation so a
bad flow can't crash the whole code location.

## 6. The `orchestrator/` code location

Scaffolded once with `uvx create-dagster project orchestrator` (committed to the
repo) — **not** hand-written. Dagster deps are added to `requirements-gui.txt` and
installed into the existing `.venv` (we do **not** use `--uv-sync`, which would
create a separate venv).

The scaffolded project exposes a top-level `Definitions` built from JSON:

```
orchestrator/
  src/orchestrator/
    definitions.py     # defs = build_all_defs()
    build.py           # JSON -> assets, jobs, schedules, sensors (dg.Definitions)
    assets.py          # asset factory: one subprocess-running asset per node
    email.py           # smtplib send + success/failure sensor factories
    state.py           # read gui/state/{pipelines,flows}.json, resolve REPO_ROOT
  pyproject.toml       # code-location entry point (from create-dagster)
```

`build_all_defs()` reads both JSON files, builds the per-flow objects above, and
returns a single `dg.Definitions(assets=..., jobs=..., schedules=..., sensors=...)`.

> The `orchestrator` package reuses the GUI's path constants / `build_argv`. To
> avoid a hard import coupling, shared helpers (`build_argv`, repo paths,
> `python_executable`) are imported from the existing `gui/` modules by ensuring
> `REPO_ROOT` and `gui/` are importable when Dagster launches (the service sets
> `cwd`/`PYTHONPATH` accordingly), or by factoring those helpers into a small
> shared module both sides import. The implementation plan will pick the cleaner
> of the two; the contract is: **assets run exactly the argv the Run page would.**

## 7. Supervision — the GUI owns Dagster

New `gui/dagster_service.py`:

- Resolves an absolute `DAGSTER_HOME` (`<repo>/.dagster_home`), creating it and a
  generated `dagster.yaml` on first run. `dagster.yaml` sets a **run-concurrency
  limit** so multiple heavy pipelines don't fire simultaneously (protects the
  known two-peak memory profile).
- Starts **one** combined process — `dg dev` (webserver + daemon) with
  `cwd=<repo>/orchestrator` and `DAGSTER_HOME` in the environment, on a host/port
  (default `127.0.0.1:3000`, env-overridable via `OASIS_DAGSTER_PORT`). Tracks
  the PID, redirects output to `run_logs/dagster.log`, exposes status
  (running?, webserver URL), and health-checks / can restart.
- On Flask shutdown, terminates the Dagster process group, **reusing the existing
  cross-platform process-group kill logic** from `pipeline_runner.py`
  (`CREATE_NEW_PROCESS_GROUP` / `CTRL_BREAK_EVENT` on Windows, `killpg` on Linux).

The GUI can also be configured to **not** auto-start Dagster (env flag), for hosts
that run it separately.

## 8. Dagster client — status, reload, control

New `gui/dagster_client.py` talks to the Dagster **GraphQL API**
(`http://<host>:<port>/graphql`):

- **Status/list:** schedules and their state, latest run status per job, recent
  runs per flow (for the Flows list page).
- **Reload:** after the GUI writes JSON for a flow/pipeline change, call
  `reloadRepositoryLocation` so new definitions load **without restarting the
  daemon** and without interrupting running jobs.
- **Control:** `startSchedule` / `stopSchedule` for enable/disable; launch a job
  run for "Run now".
- **Deep links:** build Dagster UI URLs — run: `…/runs/<runId>`, job:
  `…/locations/<loc>/jobs/<job>`.

Queries are written against a **pinned Dagster version** to avoid GraphQL schema
drift.

## 9. GUI surface

### 9.1 Pipeline library — `/pipelines`
Reuses the Run-page form to build a spec, plus **Save as named pipeline**. List /
edit / delete; each row shows its previewed command line via `commands.preview`.
This is the asset catalog. Deleting a pipeline still referenced by a flow is
blocked with a clear message.

### 9.2 Flow builder — `/flows` → "New flow"
**Form-based per-node dependency picker** (fits the vanilla-JS app; no heavy graph
library):
- Add nodes → each picks a pipeline from the library.
- Each node has an **upstream-dependencies** multi-select of the *other* nodes →
  defines edges.
- A **live read-only graph preview** (lightweight inline SVG / Mermaid) renders
  the DAG.
- Schedule: reuse the existing cron-field widget + a timezone dropdown.
- Email: success-recipients + failure-recipients (comma-separated) + "notify on"
  toggles.
- **Validation:** reject cycles; require ≥1 node; require a valid cron.

(A drag-and-drop canvas was considered and set aside as heavier than the stack
warrants; the read-only preview gives the visual without the build cost.)

### 9.3 Flows / jobs list — `/flows`
Table of flows, each row showing: schedule state (running/stopped), next tick,
**last run status** (success/failure/running, live from GraphQL), last run time.
Actions: enable/disable (start/stop schedule), **Run now**, edit, delete, and
**Open in Dagster** (deep link). A banner offers **Start Dagster** if the service
is down.

### 9.4 Email settings — Settings → Email (or on Connections)
Fields `host, port, username, password, from, use_tls` stored under `[smtp]` in
`.dlt/secrets.toml` (same pattern/redaction as Oracle creds). A **Send test
email** button.

### 9.5 Navigation
Add **Pipelines** and **Flows** nav entries; **remove Schedule**. Retire
`cron_manager.py` and `schedule.html` from the active path.

## 10. Email content

Sent with stdlib `smtplib` from the sensors, using `[smtp]` config:
- Subject: `[OASIS] Flow <name> SUCCEEDED|FAILED — run <short-id>`.
- Body: flow name, job, run id, start/end, status, **deep link to the Dagster
  run**, and on failure the error + step-failure summary
  (`RunFailureSensorContext.get_step_failure_events()`).
- If SMTP is unconfigured, sensors no-op (and the Flows page surfaces a hint)
  rather than erroring.

## 11. Cross-platform & venv

- Add to `requirements-gui.txt` (pinned to a 3.13-compatible release, ≥ the
  version the dagster-expert skill targets): `dagster`, `dagster-webserver`,
  `dagster-graphql`, `dagster-dg-cli`.
- `setup.ps1` / `setup.sh` already `pip install -r requirements-gui.txt` into
  `.venv` → no new install path.
- `DAGSTER_HOME` is set programmatically (absolute path) before launch → no manual
  env setup on either OS.
- Assets subprocess with `python_executable()` (the venv interpreter) → identical
  deps/behavior on both OSes.
- Add `.dagster_home/` and `orchestrator/` build artifacts to `.gitignore` as
  appropriate (keep the scaffolded source; ignore the instance/storage dir).

## 12. Data shapes (illustrative)

`gui/state/pipelines.json`:
```json
[
  {
    "id": "a1b2c3d4",
    "name": "masters-patient-incr",
    "spec": { "script": "oracle_to_iceberg", "mode": "INCREMENTAL",
              "category": "masters", "tables": "PATIENT_MASTER_DATA",
              "branches": ["jazan"], "log_level": "INFO" },
    "created_at": "2026-06-28T10:00:00"
  }
]
```

`gui/state/flows.json`:
```json
[
  {
    "id": "f0091a",
    "name": "nightly-masters-then-dq",
    "nodes": [
      { "node_id": "n1", "pipeline_id": "a1b2c3d4", "deps": [] },
      { "node_id": "n2", "pipeline_id": "9f8e7d6c", "deps": ["n1"] }
    ],
    "cron": "0 2 * * *",
    "timezone": "Asia/Riyadh",
    "email": { "on_success": ["ops@x.com"], "on_failure": ["ops@x.com","lead@x.com"] },
    "enabled": true,
    "created_at": "2026-06-28T10:05:00"
  }
]
```

## 13. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Dagster ↔ Python 3.13 compatibility | Pin a known-compatible Dagster release; verify on `.venv` (3.13.6) during setup. |
| GraphQL schema drift across versions | Pin Dagster; centralize queries in `dagster_client.py`. |
| Windows process-tree kill of `dg dev` | Reuse `pipeline_runner.py`'s process-group/`CTRL_BREAK_EVENT` pattern. |
| A bad flow crashes the whole code location | `build_all_defs()` validates each flow (acyclic, refs exist) and skips/flags invalid ones. |
| Deleting a pipeline referenced by a flow | Block deletion with a clear message listing referencing flows. |
| Heavy pipelines running concurrently | `dagster.yaml` run-concurrency limit. |
| Port conflict (3000) | `OASIS_DAGSTER_PORT` override; service reports a clear bind error. |
| `orchestrator` importing `gui` helpers | Set `cwd`/`PYTHONPATH` at launch, or factor shared helpers into a small module both import; contract: assets run exactly the Run-page argv. |

## 14. Acceptance criteria

1. A user can save ≥2 pipelines, build a flow wiring them with a dependency, set a
   cron + timezone, and save it.
2. On schedule (or "Run now"), Dagster runs the assets in dependency order; the
   downstream asset runs only after the upstream **succeeds**.
3. On run completion, success **and** failure emails are sent to the flow's
   configured recipients (verified via the test SMTP / a real run).
4. The Flows page lists each flow with live status from Dagster and a working
   deep link into the Dagster UI.
5. `setup.ps1` (Windows) and `setup.sh` (Linux) install Dagster into `.venv` and
   the GUI starts and supervises Dagster on both OSes.
6. The old cron Schedule page is removed; no regression in the Run / Logs /
   Iceberg / Connections pages.

## 15. Out-of-band setup steps (one-time, by implementer)

- Run `uvx create-dagster project orchestrator` and commit the scaffold.
- Confirm the pinned Dagster version installs and runs on Python 3.13.
