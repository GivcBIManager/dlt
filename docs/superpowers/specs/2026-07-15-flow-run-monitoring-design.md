# Flow-run monitoring in the GUI (Dagster GraphQL)

**Date:** 2026-07-15
**Status:** approved

## Problem

Manual runs launched from the Run page are visible in the Monitor page because
`pipeline_runner` writes `run_logs/run-*.log` plus a registry entry. Scheduled
flows (and "run now" flow launches) execute inside Dagster: the asset streams
the ETL child's output into Dagster's event log under `.dagster_home/`, so the
GUI shows nothing — a failing scheduled run (e.g. the 2026-07-15 masters-phase
decimal abort) is invisible outside the Dagster UI.

## Decision

Pull run history and per-run logs from the Dagster GraphQL API into a new
**Flow runs** tab on the Monitor page. Dagster stays the single source of
truth; no orchestrator changes, no double-written logs, no new storage or
dependencies. (Rejected alternatives: teeing asset output into `run_logs/` +
the runs registry — two writers on one JSON file; directory-scan listing —
no status/duration metadata.)

## Components

### 1. `gui/dagster_client.py` — two new functions

Same conventions as the rest of the module: stdlib urllib, best-effort, never
raises on connection errors.

- `flow_runs(limit: int = 50) -> list[dict]`
  GraphQL `runsOrError(limit: $n)`; keep only runs whose `jobName` starts with
  `flow_`. Each row: `run_id`, `job`, `flow_id` (via
  `flow_naming.flow_id_from_job`), `status`, `start_time`, `end_time`
  (epoch seconds as returned by Dagster), and `run_link`. Unreachable
  Dagster → `[]`.

- `run_log_tail(run_id: str, cursor: str | None = None) -> dict`
  GraphQL `logsForRun(runId: $id, afterCursor: $cursor)` selecting
  `... on MessageEvent { timestamp level message }` plus the connection's
  `cursor` and `hasMore`, and `runOrError { ... on Run { status } }` for the
  run state. Returns `{"chunk": str, "cursor": str, "has_more": bool,
  "status": str|None, "error": str|None}` where `chunk` is newline-joined
  `HH:MM:SS | LEVEL | message` lines (empty when nothing new). Cursor-based so
  polling transfers only new events — the GraphQL analogue of the byte-offset
  tail used for log files. Errors → `{"chunk": "", "error": ...}`.

### 2. `gui/app.py` — two routes

- `GET /api/flow-runs?limit=` → `flow_runs()`, enriched with the flow's
  display name by joining `flow_id` against `flows_store` (fallback: job
  name), links rewritten with the existing `_publicise_dagster_links`.
- `GET /api/flow-runs/<run_id>/log?cursor=` → `run_log_tail()`.

Both use the standard `@api` decorator.

### 3. `gui/templates/logs.html` — "Flow runs" tab

New tab alongside Log files / DQ / Runs / ETL run log / ETL control:

- Table of recent runs: flow name, status pill, started, duration, link out
  to the Dagster UI run page.
- Clicking a run opens the same console panel + `createLogDash()` parsed
  dashboard used for log files, fed by polling the cursor tail. The existing
  auto-refresh checkbox drives polling; polling keeps refreshing the runs list
  and, while the selected run's status is `QUEUED`/`STARTING`/`STARTED`, its
  log tail — live progress for in-flight scheduled runs.

## Error handling

- Dagster down: runs list renders a warn banner (same UX as flows page);
  page stays usable.
- Unknown/foreign run id: `run_log_tail` returns `error`, shown in the panel.
- `chunk` is escaped by the existing console rendering (textContent).

## Testing

- `tests/test_dagster_client.py` style: pure-parse tests for the new
  functions with fake GraphQL payloads (monkeypatched `_query`); unreachable
  → empty/err shapes.
- Route tests (Flask test client) for `/api/flow-runs` and
  `/api/flow-runs/<id>/log`, including flow-name enrichment.

## Out of scope

Flows-page changes, historical filtering/search (Dagster UI does this),
stopping runs from the GUI, log persistence beyond Dagster's retention.
