# Monitor page — per-run insights ("Runs" tab)

**Date:** 2026-07-02
**Status:** Approved design, ready for implementation plan

## Problem

The Monitor page ([gui/templates/logs.html](../../../gui/templates/logs.html)) exposes the
Iceberg observability tables `etl_run_log` and `etl_control` only as **raw filterable
table dumps** (the "ETL run log" and "ETL control" tabs). There is no way to see an ETL
execution as a single unit — how many rows a run loaded, how long it took, how many
`(table, branch)` units succeeded or failed, or whether it hit schema drift/errors —
without eyeballing hundreds of flat rows.

`etl_run_log` is append-only and carries a `pipeline_run_id`, so its rows can be grouped
into runs. `etl_control` is a merge table holding only the *latest* state per
`(table_name, branch_id)`, so it represents "where things stand now" rather than run
history.

## Goal

Add a read-only **"Runs" tab** to the Monitor page that rolls `etl_run_log` up **by run**
(one card per `pipeline_run_id`, newest first) with the headline insights, and lets the
user expand a run to see its per-`(table, branch)` detail with the current `etl_control`
watermark folded in.

## Decisions (from brainstorming)

- **Primary view:** per-run rollup + drill-down (not charts, not a standalone health board).
- **etl_control:** folded into the run drill-down (current watermark per `(table, branch)`),
  not given its own summary board.
- **Rollup metrics (collapsed card):** rows + wall-clock duration; success/failure counts;
  schema-drift + error counts. (Mode + disposition mix is *not* a headline metric — mode is
  shown as a small context label; disposition lives in the drill-down.)
- **Tab layout:** **additive** — keep all four existing tabs untouched, add a fifth "Runs"
  tab. Nothing is removed or renamed.
- **Aggregation:** **server-side** (Approach A). Grouping in the browser off the existing
  `/api/iceberg/system/etl_run_log?limit=2000` endpoint would silently truncate history to
  ~11 runs (≈175 rows/run), so runs are grouped in Python/pyarrow instead.

## Architecture / data flow

```
etl_run_log (append)   ──scan + group by pipeline_run_id──►  run summaries
etl_control (latest)   ──index by (table_name, branch_id)──►  watermark lookup
                                                               │
gui/iceberg_browser.py:  read_run_summary()  ──────────────────┤
                         read_run_detail(run_id)  ◄── joins control watermark
                                                               │
gui/app.py:  GET /api/iceberg/runs           GET /api/iceberg/runs/<run_id>
                                                               │
gui/templates/logs.html:  new "Runs" tab  ──► rollup cards + expandable detail
```

No new dependencies. Follows the existing `read_system_table` → route → template pattern.
All read-only.

## Components

### 1. `gui/iceberg_browser.py` — two new functions

Both mirror `read_system_table`'s handling of a not-yet-created table (return an empty,
well-formed payload rather than raising). Grouping uses pyarrow, already imported in the
module.

**`read_run_summary(limit_runs: int = 100) -> dict`**

Scans `etl_run_log`, groups rows by `pipeline_run_id`, and returns the newest
`limit_runs` runs, newest first. Per run:

| field              | derivation                                              |
| ------------------ | ------------------------------------------------------- |
| `run_id`           | `pipeline_run_id`                                       |
| `load_mode`        | the run's mode (INITIAL / INCREMENTAL)                  |
| `start_time`       | `min(start_time)` across the run's rows                 |
| `end_time`         | `max(end_time)` across the run's rows                   |
| `duration_wall_ms` | `end_time − start_time` (**wall clock, not summed**)    |
| `rows_total`       | `sum(row_count)`                                         |
| `units`            | count of `(table, branch)` rows in the run              |
| `ok`               | count where `status == "SUCCESS"`                        |
| `failed`           | `units − ok`                                             |
| `schema_drift`     | count of rows with non-null `schema_discrepancy`        |
| `errors`           | count of rows with non-null `error_details`             |
| `tables`           | distinct `table_name` count                             |

Runs are ordered by `start_time` descending (fall back to `recorded_at` if `start_time`
is null, consistent with the sort-column selection in `read_system_table`).

**`read_run_detail(run_id: str) -> dict`**

Returns the `etl_run_log` rows for a single `run_id`, each **joined to the current
`etl_control` state** for its `(table_name, branch_id)`. Columns:

- From run log: `table_name`, `branch_id`, `load_mode`, `row_count`, `duration_ms`,
  `status`, `write_disposition`, `attempts`, `error_details`, `schema_discrepancy`,
  `start_time`, `end_time`.
- Folded-in control (nullable when control has no matching key): `control_status`,
  `last_cdc_value`, `last_date_value`, `control_updated_at`.

Rows sorted by `table_name` then `branch_id`, with **failed rows first**. Returned as
`{ "run_id", "columns", "rows" }`. `etl_control` is read once and indexed by
`(table_name, branch_id)` for the join; a missing control table yields all-null control
columns rather than an error.

### 2. `gui/app.py` — two routes

Mirror the existing `api_ib_system` route (same JSON error handling, same `limit` clamp
pattern):

- `GET /api/iceberg/runs?limit=100` → `read_run_summary(limit_runs=min(limit, 500))`
- `GET /api/iceberg/runs/<run_id>` → `read_run_detail(run_id)`

### 3. `gui/templates/logs.html` — new "Runs" tab

- Add `<button class="btn tab" data-tab="runs">Runs</button>` to the tab row and a
  `<section data-panel="runs" hidden>` panel. The existing four tabs/panels are unchanged.
- Wire `showTab("runs")` to load the rollup (lazy, like the other tabs).
- **Rollup list:** one card/row per run, newest first:
  - Headline: `rows_total` (via `fmtNum`) + wall-clock duration (formatted `Hh Mm Ss`).
  - `ok` / `failed` counts; `failed` rendered as a red badge only when `> 0`.
  - `schema_drift` / `errors` counts rendered as warning badges only when `> 0`.
  - Muted context label: `load_mode` + formatted `start_time`.
- **Drill-down:** each run is a native `<details>` (like the existing `#rd-issues-box`).
  On first expand, lazy-fetch `/api/iceberg/runs/<run_id>` and render the detail rows with
  the existing `renderTable(columns, rows, { pillCols: ["status","control_status"],
  numCols: ["row_count","duration_ms","attempts"] })` helper. Failed/drift rows surface via
  the existing `pill()` styling.
- **Filter bar** on the rollup: `mode` select + `from`/`to` date inputs, reusing the page's
  existing `.filter-bar` styling and the client-side filter approach already in `SysTable`.
- Small header stat: `N runs · last run <relative time>`.
- New CSS limited to a couple of badge tweaks in `gui/static/style.css`; reuse existing
  `.pill`, `.filter-bar`, `.table-wrap`, `.panel` classes otherwise.

## Error handling

- Missing/empty `etl_run_log` or `etl_control`: functions return an empty, well-formed
  payload (`rows: []`), exactly as `read_system_table` does today; the tab shows "No rows."
- Null `start_time` / `end_time` on a row: excluded from the min/max; `duration_wall_ms`
  is `None` when it can't be computed and renders as "—".
- Control join miss (no matching `(table, branch)`): control columns are `None`, rendered
  as "—".
- Route errors are converted to JSON `{ "error": ... }` with a non-200 status, matching
  `api_ib_system`; the template surfaces them in a `banner warn` like the other tabs.

## Testing

Mirror the existing data-layer tests under `tests/`:

- `read_run_summary`:
  - grouping — N rows across 2 runs collapse to 2 summaries with correct `units`, `ok`,
    `failed`, `tables`, `rows_total`.
  - **wall-clock duration = `max(end_time) − min(start_time)`, not the sum** of per-row
    `duration_ms` (guards the core insight).
  - `schema_drift` / `errors` count only non-null occurrences.
  - newest-first ordering; `limit_runs` truncation.
- `read_run_detail`:
  - returns only the requested run's rows; failed rows sorted first.
  - control watermark joined onto the correct `(table, branch)`; **`None` control columns
    when `etl_control` has no matching key**.
- Missing-table guard: both functions return an empty payload when the Iceberg table does
  not exist yet (no exception).
- Endpoint smoke tests: `/api/iceberg/runs` and `/api/iceberg/runs/<run_id>` return the
  expected JSON shape and handle a missing table gracefully.

## Out of scope (YAGNI)

- Charts / trend lines across runs.
- A standalone `etl_control` health board (control is folded into the drill-down instead).
- Linking a run to its `run_logs/*.log` file — the GUI log filename and the ETL
  `pipeline_run_id` are not reliably the same key.
- Any write/delete/mutation actions. The feature is entirely read-only.
