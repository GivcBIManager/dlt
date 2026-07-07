# DQ hash-delta tolerance + per-command-type run/log observability

**Date:** 2026-07-07
**Status:** Approved (design)
**Scope:** three related changes to the OASIS Oracle→Iceberg pipeline and its GUI control panel.

## Summary

Three features, sharing one frontend refactor:

1. **DQ hash-delta tolerance** — a global, configurable tolerance (default **10%**) below
   which a table's row-hash drift is reported as a new `WITHIN_TOLERANCE` status instead of a
   hard `MISMATCH`.
2. **Run-panel progress per command type** — `dq_check` gains a live progress heartbeat, and
   the Run page's live dashboard renders a `dq_check`-specific view (today it only understands
   `oracle_to_iceberg`).
3. **Per-type log summaries** — the Logs page renders a comprehensive, type-tailored summary
   for *any* file in `run_logs/`, so a reader captures the outcome without scrolling the whole
   log.

Parts 2 and 3 are built on one shared change: turning the single hardwired dashboard parser
into a **log-type dispatcher** with pluggable per-type views.

## Context (current behavior)

- `etl/dq_check.py` `check_unit` sets status by:
  ```python
  bad_count = res.row_count_delta not in (None, 0)
  bad_hash  = res.hash is not None and res.hash.total_delta > 0
  res.status = "MISMATCH" if (bad_count or bad_hash) else "OK"
  ```
  Any single differing row (`total_delta = only_in_oracle + only_in_iceberg + mismatch > 0`)
  makes the whole `(table, branch)` a `MISMATCH`.
- DQ config lives in `etl/config.py` `Settings`, loaded from `.dlt/config.toml [etl]` via
  `_cfg(...)`. The Run page shows/edits the `[etl]` block; `gui/workspace.py`
  `update_etl_settings` edits only keys already present in `[etl]` and on `EDITABLE_ETL_KEYS`.
- `dq_check.py` (CLI) runs `run_dq`, prints `render_summary`, writes `etl_dq_results`. `run_dq`
  emits **no per-unit progress** — only an opening `DQ run …` line and the final summary.
- The GUI live dashboard is `gui/static/runparse.js` (`createLogDash`) + `gui/templates/_dash.html`.
  It parses only `oracle_to_iceberg` lines (`PROGRESS …`, `[branch/table] N rows`,
  `[table] loaded: …`, the final summary rows). Both the Run page (live tail) and the Logs
  page (whole-file) include `_dash.html` and call `createLogDash`. `base.html` loads
  `app.js` + `runparse.js` on every page.
- Every run log begins with a header the runner writes
  (`gui/pipeline_runner.py`): `# command : <argv>` … and ends with
  `[runner] exited with code N`. The command line is the reliable log-type signal.
- Status pills (`gui/static/app.js` `pill()`) lowercase the status into a CSS class, so a new
  status only needs one CSS rule. `.panel.raw-hidden .console { display:none }` already gives
  summary-first via the existing **Raw log** toggle.

## Part 1 — DQ hash-delta tolerance

### Configuration (global)

- Add `dq_hash_delta_tolerance_pct: float = 10.0` to `Settings` (`etl/config.py`). Units:
  **percent** (10.0 means 10%).
- Load it in `load_settings`: `dq_hash_delta_tolerance_pct=float(_cfg("etl.dq_hash_delta_tolerance_pct", 10.0))`.
- Add the key to `.dlt/config.toml [etl]` with an explanatory comment (needed so
  `update_etl_settings` can edit it in place).
- Surface + edit in the Run page ETL settings panel:
  - `gui/workspace.py`: add `dq_hash_delta_tolerance_pct` to `EDITABLE_ETL_KEYS`.
  - `gui/templates/run.html`: add it to `SET_EDITABLE`, `SET_NUMERIC`, and `SET_SHOWN`.

### Status logic (`etl/dq_check.py` `check_unit`)

New status constant `WITHIN_TOLERANCE`. The status becomes a precedence (evaluated after the
existing per-unit `try/except`, which still yields `ERROR`):

| Condition | Status |
|---|---|
| row-count delta ∉ {None, 0} | `MISMATCH` (row-count stays **zero-tolerance** — scope is hash only) |
| hash absent, or `total_delta == 0` | `OK` |
| `total_delta / oracle_hashed_rows ≤ tol` and `oracle_hashed_rows > 0` | `WITHIN_TOLERANCE` |
| otherwise (incl. `oracle_hashed_rows == 0` with `total_delta > 0`) | `MISMATCH` |

where `tol = settings.dq_hash_delta_tolerance_pct / 100.0` and
`oracle_hashed_rows = res.hash.oracle_rows`.

- Compute `hash_delta_pct = 100 * total_delta / oracle_hashed_rows`. Store on `DqResult` as
  `hash_delta_pct: Optional[float]`. It is `None` when no hash ran, when `total_delta == 0`
  (i.e. `OK`, so `0.0`), **and** when `oracle_hashed_rows == 0` with `total_delta > 0` (ratio
  undefined → `None`, status resolves to `MISMATCH`). Concretely: `OK` → `0.0`;
  undefined-denominator `MISMATCH` → `None`; no-hash → `None`; otherwise the computed percent.
- `check_unit` needs the tolerance value: pass `settings` (already available) — read
  `settings.dq_hash_delta_tolerance_pct` inside `check_unit`.

### Outputs

- **`etl_dq_results`** (`_result_rows` + `_DQ_HINTS`): add `hash_delta_pct` as a `double`
  column (`{"data_type": "double"}`). Additive schema evolution; prior rows read back null.
- **`render_summary`**: add a `TOL%` column (formatted `hash_delta_pct`, `-` when None) and
  include `WITHIN_TOLERANCE` in the tally line, e.g.
  `N unit(s): X OK, W WITHIN_TOLERANCE, Y MISMATCH, Z ERROR, S SKIPPED`.
- **Exit code**: unchanged. `WITHIN_TOLERANCE` and `MISMATCH` both exit 0; only `ERROR` exits
  non-zero (`dq_check.py main`).
- **GUI DQ results view** (`gui/templates/logs.html`, `dqView`): add `hash_delta_pct` to
  `numCols`. The status filter picks up `WITHIN_TOLERANCE` automatically (distinct values).
- **CSS** (`gui/static/style.css`): add `.pill.within_tolerance { background: var(--warning-light); color: #b45309; }`.

## Shared refactor — log-type dispatcher (foundation for Parts 2 & 3)

Refactor `gui/static/runparse.js` and `gui/templates/_dash.html` so the dashboard is
type-aware. Public API of `createLogDash` (`reset / feed / flush / render / load`) is
**unchanged**, so `run.html` and `logs.html` need no structural edits.

- **`detectType(text)`** — classify from the `# command : …` header line:
  `oracle_to_iceberg` / `dq_check` / `snapshot_diff` / `fresh_run` / `custom`. Fallback sniffs
  characteristic lines when the header is absent (streaming from offset 0 always includes it):
  `PROGRESS ` → pipeline, `DQ run `/`DQ-PROGRESS`/`DQ-UNIT` → dq, `Baseline (as-of` → snapshot.
  Type is latched on first detection; `reset()` clears it.
- **Per-type views**, each `{ reset(), feedLine(line), render() }` with its own model and its
  own container in `_dash.html`:
  - `#rd-pipeline` — the current oracle dashboard markup and logic, moved verbatim (the `rd-*`
    ids stay under this container).
  - `#rd-dq` — new (Part 2).
  - `#rd-generic` — new; snapshot_diff, fresh_run, custom, and unknown (Part 3).
- The dispatcher routes each parsed line to the active view, shows that view's container, hides
  the others. When no type is detected yet, all stay hidden (unchanged empty-state behavior).
- All views share a small **meta strip** parsed from the header/trailer: command label,
  started, elapsed (from first/last timestamp), and exit-code/status pill.

## Part 2 — dq_check live progress (Run panel)

### Backend (`etl/dq_check.py`, `dq_check.py`)

Emit progress on the `etl.dq` logger so lines land in the run log with timestamps.

- **Per unit**, logged as each `check_unit` result is collected (under the existing results
  lock in `run_dq`):
  `DQ-UNIT <table>/<branch> | ora=<n|-> ice=<n|-> cnt=<Δ|-> | match=<n|-> delta=<total|-> pct=<p|-> | <STATUS>`
- **Heartbeat** every `progress_interval_s` from a daemon thread:
  `DQ-PROGRESS <H:MM:SS> | units <done>/<total> | ok <O> tol <T> mismatch <M> err <E>`
  - `total = len(tables) * len(branches)`; `done` is a thread-safe counter bumped as each unit
    completes. Counters are plain integers under a lock — no per-unit measurement cost.
  - Honors `settings.progress_enabled`; add a `--no-progress` flag to `dq_check.py` that sets it
    off (mirrors `oracle_to_iceberg.py`). `gui/commands.py` already exposes `--no-progress` via
    `EXTRA_ARGS`; optionally add a dedicated checkbox to the `dq_check` builder group.
  - A small helper (e.g. `_DqProgress`) owns the counter, the daemon thread, and the two line
    formats; `run_dq` starts it, records each result, stops it. `render_summary` and the
    `-> wrote …` lines are unchanged.

### Frontend (`#rd-dq` view)

Parses `DQ run …` (scope/window), `DQ-PROGRESS` (overall), `DQ-UNIT` (rows), and
`| ERROR |` lines (issues). Renders:
- Overall bar: units done/total, elapsed, tallies OK / within-tol / mismatch / err.
- Live-filling **units table**: table, branch, ora rows, ice rows, cnt Δ, hash Δ, pct, status
  pill — sorted most-severe-first (ERROR → MISMATCH → WITHIN_TOLERANCE → OK).
- Scope/window line (branches, table count, hash on/off, self-test).
- Issues feed (reuses the existing issue-feed pattern).

## Part 3 — comprehensive per-type log summary (Logs page)

Delivered mostly by the shared refactor: selecting any `run_logs/` file renders its type's
summary via `fileDash.load(content)`. Specific additions:

- **snapshot_diff** summary (`#rd-generic`, snapshot mode): table; baseline vs latest snapshot
  ids + timestamps; identity `(branch_id, key) | N business columns`; **Updated / Inserted /
  Deleted** counts (from `Updated : n   Inserted : n   Deleted : n`); output file paths;
  duplicate-key warnings; "No updated records" case.
- **generic / fresh_run / custom / unknown** (`#rd-generic`, default mode): meta strip
  (command, started, duration, exit-code/status pill from `[runner] exited with code N`), a
  short list of key result lines (`-> wrote …`, final summary rows), and a warnings/errors
  feed.
- **Summary-first UX** (`gui/templates/logs.html`): once a summary renders for the selected
  file, collapse the raw log by default (add the `raw-hidden` class to `#file-panel`); the
  existing **Raw log** button reveals it. If no summary could be built, leave raw visible.

## Files touched

- `etl/config.py` — new `Settings.dq_hash_delta_tolerance_pct`; load it.
- `.dlt/config.toml` — add the key (with comment) under `[etl]`.
- `etl/dq_check.py` — tolerance status logic, `hash_delta_pct`, `WITHIN_TOLERANCE`,
  `render_summary` column + tally, `_result_rows`/`_DQ_HINTS` new column, DQ progress emitter.
- `dq_check.py` — `--no-progress` flag; wire progress into `run_dq`.
- `gui/workspace.py` — `EDITABLE_ETL_KEYS` += tolerance key.
- `gui/templates/run.html` — settings panel key lists; optional dq `--no-progress` checkbox.
- `gui/templates/logs.html` — `hash_delta_pct` numCol; summary-first collapse.
- `gui/static/runparse.js` — dispatcher + per-type views (pipeline moved, dq + generic new).
- `gui/templates/_dash.html` — split into `#rd-pipeline` / `#rd-dq` / `#rd-generic`.
- `gui/static/style.css` — `.pill.within_tolerance`; any new `rd-*` classes for dq/generic.

## Testing

- **Unit (Python):** `check_unit` status precedence across the tolerance boundary
  (delta 0 → OK; small delta within tol → WITHIN_TOLERANCE; over tol → MISMATCH; count delta ≠ 0
  → MISMATCH regardless of hash; `oracle_hashed_rows == 0` with delta > 0 → MISMATCH). Assert
  `hash_delta_pct` value. `render_summary` includes the new column/tally. A `--self-test` DQ run
  emits `DQ-UNIT`/`DQ-PROGRESS` lines.
- **Frontend:** feed each view a captured sample log and assert the parsed model + rendered
  container (units table rows, tallies, snapshot counts, generic exit-code). `detectType`
  classifies each command form. Existing `oracle_to_iceberg` parsing is unchanged (regression).
- **Manual:** run a `dq_check` from the Run page (live `#rd-dq` fills), open its log on the
  Logs page (summary-first), open an `oracle_to_iceberg` and a `snapshot_diff` log, edit the
  tolerance in the settings panel and confirm a WITHIN_TOLERANCE result.

## Decisions (resolved during brainstorming)

- Within-tolerance → **new `WITHIN_TOLERANCE` status** (not silently `OK`).
- Tolerance denominator → **`oracle_hashed_rows`**.
- dq_check progress → **real backend heartbeat** (not frontend-only).
- Summary scope → **every file in `run_logs/`** (all types + generic fallback).
- Tolerance applies to **hash delta only**; row-count delta stays a hard `MISMATCH`.
- **Add `hash_delta_pct`** to `etl_dq_results` (additive).

## Out of scope / YAGNI

- Per-table tolerance overrides (global only for now).
- Tolerance on the row-count delta.
- Historical trend/alerting on tolerance breaches.
- Reworking the `etl_dq_results` DQ *results* tab beyond the new column/pill.
