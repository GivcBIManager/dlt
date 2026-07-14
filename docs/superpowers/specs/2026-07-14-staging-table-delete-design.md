# Staging Layer Explorer: table deletion

**Date:** 2026-07-14
**Status:** approved (approach A, with system tables deletable)

## Problem

There is no way to drop a table from the Iceberg staging layer
(`iceberg_output/oasis`) from the GUI. Cleaning up a mis-loaded table means
hand-deleting folders and remembering to reset watermarks — error-prone, and
forgetting the watermark step silently loses history on the next incremental
run.

## Decision

Add delete actions to the existing Staging Layer Explorer page
(`gui/templates/iceberg.html` + `gui/iceberg_browser.py`): a per-table delete
and a delete-all, both behind typed confirmations. Deleting a table also
clears its `control_state.json` watermarks so the next run re-extracts from
scratch.

## Backend

New functions in `gui/iceberg_browser.py`, new routes in `gui/app.py`:

- `DELETE /api/iceberg/tables/<table>` → `delete_table(name)`
- `DELETE /api/iceberg/tables` (JSON body `{"include_system": bool}`) →
  `delete_all_tables(include_system)`

Rules, applied in order:

1. **Name safety:** `Path(name).name` only; must be an existing directory
   under `ICEBERG_ROOT` containing a `metadata/` dir (i.e. a real Iceberg
   table).
2. **`_dlt_*` folders are never deletable** (`_dlt_loads`, `_dlt_version`,
   `_dlt_pipeline_state`) — internal dlt bookkeeping, not tables.
3. **System tables** (`etl_control`, `etl_run_log`, `etl_dq_results`) ARE
   deletable: per-table with the same typed confirmation, and included in
   delete-all only when `include_system` is true (UI checkbox). The pipeline
   recreates them on the next run's observability write.
4. **Run guard:** if any pipeline run in the runs registry is alive
   (status running/detached with a live PID), refuse with HTTP 409 —
   deleting a table mid-load corrupts the lake (see 2026-07-14 incident).
5. **Delete:** `shutil.rmtree(table_dir)`, then pop the table's key from
   `control_state.json` (atomic tmp-write + replace, same pattern as the runs
   registry). System tables have no control_state entry — skipped.
6. **Response:** `{deleted: [...], watermarks_cleared: [...], errors: {...}}`
   with per-table rows/size from the last snapshot summary. A locked file
   (Windows) surfaces as that table's error; the UI refreshes the list so it
   shows whatever survived.

The dlt pipeline schema (`~/.dlt/pipelines/oracle_to_iceberg/schemas`) keeps a
stale entry for deleted tables; this is harmless — the next load recreates the
table folder.

## Frontend (`iceberg.html`)

- Each table row (data AND system) gets a small delete (trash) action.
- A "Delete all…" button sits above the table list.
- Both open a custom modal (no native `confirm()`):
  - **Per table:** shows name, rows, size, and the note "also clears
    extraction watermarks — the next run re-extracts this table from its
    initial window". Delete button enabled only after typing the exact table
    name. Deleting a system table shows an extra warning line instead of the
    watermark note.
  - **Delete all:** shows table count + total size, a checkbox
    "Include system tables (etl_control, etl_run_log, etl_dq_results)"
    (default checked), and requires typing `DELETE ALL`.
- On success/failure show the response summary and reload the table list.

## Testing

Unit tests (pytest) for the backend against a temp `ICEBERG_ROOT` fixture:

- rejects path traversal (`..`, separators) and unknown tables
- always refuses `_dlt_*`
- deletes a data table and removes its `control_state.json` entry
- deletes a system table when named explicitly
- delete-all skips system tables when `include_system=false`, includes them
  when true, never touches `_dlt_*`
- returns 409 when a run is alive (RunManager stubbed)

## Out of scope

Per-branch (partition-level) deletes; deleting `_dlt_*` state; any change to
pipeline locking (tracked separately).
