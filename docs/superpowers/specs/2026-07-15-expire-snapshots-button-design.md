# Expire-snapshots button for staging Iceberg tables — Design

**Date:** 2026-07-15
**Status:** Approved

## Goal

Add a per-table button on the GUI staging explorer (`/iceberg`) that expires all
Iceberg snapshots except the latest one and deletes the data/manifest files that
become unreferenced, reclaiming disk space in the staging lake.

## Background

- The staging lake is written by dlt's filesystem destination in Iceberg format
  (`etl/iceberg_load.py`). Merge loads produce many snapshots per table.
- pyiceberg 0.11 `expire_snapshots` is **metadata-only**: it removes snapshot
  entries but leaves the underlying data/manifest files on disk as orphans.
  The button must therefore do expiry **plus** orphan-file cleanup to free space.
- The GUI browser (`gui/iceberg_browser.py`) opens tables read-only via
  `StaticTable` and cannot commit. Writable commits must go through the same
  path the ETL retention code uses: `etl.iceberg_load.build_pipeline()` +
  `dlt.common.libs.pyiceberg.get_iceberg_tables(pipeline)` (proven in
  `apply_snapshot_retention()`, `etl/iceberg_load.py:621-658`).

## Components

### 1. Backend logic — new module `gui/iceberg_maintenance.py`

Kept separate from `iceberg_browser.py`, which stays read-only.

`expire_snapshots(table: str) -> dict`:

1. Lazily import `etl.config.load_settings()` and
   `etl.iceberg_load.build_pipeline()` (dlt import is heavy; do it inside the
   function). Obtain the writable table via
   `get_iceberg_tables(pipeline)[table]`. Raise `FileNotFoundError` if the
   table does not exist (maps to HTTP 404).
2. Expire all snapshots except branch/tag heads:
   `tbl.maintenance.expire_snapshots().by_ids([ids]).commit()` where `ids` is
   every snapshot id not in the protected set (pyiceberg auto-skips protected
   head snapshots, so the current snapshot survives by construction). Skip the
   commit when there is nothing to expire.
3. Orphan cleanup: reload the table, walk remaining snapshots → manifest
   lists → manifests → data files to build the referenced-file set. Always
   keep `*.metadata.json` and version-hint files. Delete any file under the
   table's `data/` and `metadata/` directories not in the referenced set.
4. Return `{"table", "expired", "orphans_deleted", "bytes_freed"}`.

### 2. API route — `gui/app.py`

`POST /api/iceberg/tables/<table>/expire-snapshots`, `@api`-wrapped, placed
next to the existing delete routes. Guarded by `_run_guard()` (HTTP 409 while
a pipeline run is live) — this is the race protection for orphan deletion,
same as table delete.

### 3. UI — `gui/templates/iceberg.html`

- Second small ghost button (`fa-broom`) beside the trash icon in each row.
  Hidden for `_dlt*` system tables and for tables with ≤ 1 snapshot.
- Simple confirmation modal (no typed confirmation — current data survives;
  only time-travel history is lost).
- On success: toast like "Expired 12 snapshots, freed 340 MB", refresh the
  table list and, if the table is selected, the detail panel.
- `event.stopPropagation()` on the button (rows have their own onclick).

## Error handling

- Route decorator maps `ValueError` → 400, `FileNotFoundError`/`KeyError` →
  404, other exceptions → 500 (existing `@api` behavior).
- Live-run guard returns 409 with a message; UI surfaces it as an error toast.
- Orphan deletion failures on individual files are collected and reported,
  not fatal (metadata commit has already succeeded).

## Testing

`tests/test_iceberg_expire.py`, mirroring `tests/test_iceberg_delete.py`:

- Build a temp Iceberg table with several snapshots (pyiceberg SqlCatalog in
  tmp dir, multiple appends).
- Run `expire_snapshots()`; assert exactly one snapshot remains, current data
  is still readable, orphaned files are gone, referenced files are intact,
  and the returned counts/bytes are correct.
- Route tests: 404 for unknown table, 409 during a live run, success path.
