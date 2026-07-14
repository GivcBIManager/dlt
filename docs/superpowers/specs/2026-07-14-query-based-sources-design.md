# Query-based sources in tables.json — Design

**Date:** 2026-07-14
**Status:** Approved

## Problem

Every entry in `tables.json` must currently name a physical Oracle table
(`OWNER.TABLE`). The Iceberg table / staging / control-state name is derived
from that identifier (`TableDef.dataset_table_name` splits on `.` and
normalizes). There is no way to load the result of a SQL query (joins,
projections, filters) as its own Iceberg table — and even though the GUI
validator already accepts a `(SELECT ...)` subquery in `table` as an escape
hatch, the derived name for such an entry is garbage.

## Decision

Allow an entry's `table` to be an Oracle inline-view subquery, and add a new
`name` key that supplies the Iceberg table name explicitly.

```json
{
  "table": "(SELECT v.*, m.STATUS FROM OASIS.VISITS v JOIN OASIS.VISIT_MASTER m ON m.VISIT_ID = v.VISIT_ID)",
  "name": "visits_enriched",
  "unique_key": "VISIT_ID",
  "cdc_column": "AMEND_LAST_DATE",
  "where_date_column": "VISIT_DATE"
}
```

Rules:

- `name` is **required** when `table` is a subquery (starts with `(`).
- `name` is **optional** on plain-table entries, acting as a rename of the
  Iceberg table; when absent, naming behaves exactly as today.
- `name` is normalized with the existing `_normalize_name` (lower-snake).

## Changes

### etl/config.py

- `TableDef` gains `name: Optional[str] = None` and an `is_query` property
  (true when `table` starts with `(`).
- `dataset_table_name` returns `_normalize_name(name)` when `name` is set,
  else the current derivation from `object_name`.
- `object_name` returns `name` for query entries so the `--tables` CLI filter
  (`oracle_to_iceberg.py`, `dq_check.py`) matches query entries by their given
  name.
- `load_table_defs` raises `ValueError` if a query entry lacks `name`.

### SQL builders — no structural change

`FROM (SELECT ...) t` is a valid Oracle inline view, so the existing query
shapes in `etl/oracle_extract.py` (plain, helper-join, snapshot) and the DQ
count/hash queries in `etl/dq_check.py` work unchanged.

**Constraint (documented, not enforced):** `cdc_column`,
`where_date_column`, and `unique_key` must be columns *projected by the
subquery*, since predicates are written as `t.<col>` against the inline view.
Incremental/CDC therefore works normally for query entries.

**Caveat:** a subquery that projects a column named `BRANCH_ID` collides with
the injected branch column, same as the known physical-table case — alias it
inside the subquery.

### gui/tables_store.py

- Add `"name"` to `KNOWN_KEYS`.
- Validate `name` as a plain identifier.
- Require `name` when `table` is a subquery.
- Duplicate detection uses the effective name (`name` or `table`).

### gui/templates/tables.html

- New "Iceberg name" input wired into the entry load/save round-trip and
  shown in the entry summary line.

## Testing

- `load_table_defs`: query entry with `name` loads; query entry without
  `name` raises; plain entry with `name` renames; plain entry without `name`
  unchanged.
- `TableDef`: `dataset_table_name` / `object_name` overrides; `is_query`.
- `build_query`: initial, incremental, and snapshot SQL for a query entry
  render `FROM (SELECT ...)` correctly.
- `tables_store.validate`: accepts query entry with valid `name`; rejects
  query entry without `name`; rejects invalid `name`; duplicate detection by
  effective name.

## Out of scope

- Enforcing that watermark/key columns exist in the subquery projection
  (fails at run time with a clear Oracle/dlt error instead).
- Any change to the injected-columns (`BRANCH_ID`, `insert_at`, ...) logic.
