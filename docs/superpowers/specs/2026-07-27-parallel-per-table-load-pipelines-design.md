# Parallel Per-Table Load Pipelines — Design

**Date:** 2026-07-27
**Status:** Approved for planning
**Follow-up to:** [2026-07-26-iceberg-load-fast-path.md](../plans/2026-07-26-iceberg-load-fast-path.md) (its explicit non-goal "parallel load slots")

## Goal

Let N tables load into Iceberg concurrently — one dlt pipeline per table, a
configurable worker pool — while everything keeps landing in the same bucket,
same dataset, same tables. `load_workers = 1` must reproduce today's serial
behavior exactly.

## Motivation (measured)

The load stage is one shared dlt pipeline drained by a
`ThreadPoolExecutor(max_workers=1)` (`etl/iceberg_load.py`,
`load_and_record`). dlt pipelines are not safe to run concurrently *with
themselves* — but that is per-pipeline, not per-process. Measured on the
2026-07-20 INITIAL baseline: the serial load thread is busy 92–98% of the run,
dominated by per-commit IO waits of 13–32 s (Postgres catalog pointer swap +
filesystem metadata writes). Extraction is already parallel, so ready tables
queue behind a single loader. Overlapping commits across tables converts those
serial waits into parallel ones.

## Architecture

```
extraction (threaded, unchanged)
    └─ on_table_done ── table ready ──> ThreadPoolExecutor(load_workers)
                                            worker A: pipeline oracle_to_iceberg__appointments ─┐
                                            worker B: pipeline oracle_to_iceberg__claims ───────┼─> same bucket,
                                            worker C: pipeline oracle_to_iceberg__patients ─────┘   same dataset,
                                                                                                    Postgres SqlCatalog
```

Each table gets its own dlt pipeline, named deterministically, built inside
the worker thread that loads it. dlt 1.28.1's `Container` contexts are
thread-affine and `thread_pool_prefix()` maps a pipeline's internal pools back
to the starting thread, so one-pipeline-per-thread is a supported concurrency
model. Cross-table Iceberg commits are safe because the catalog is a
persistent Postgres `SqlCatalog` (transactional pointer swap per table);
same-table conflicts are structurally impossible because a table becomes
"ready" exactly once per run.

Data placement is invariant: no custom layout is configured, so data paths are
`{table_name}/...` and Iceberg table locations are `<dataset>/<table_name>` —
neither embeds the pipeline name. dlt's `_dlt_pipeline_state`/`_dlt_loads`
bookkeeping files in the shared dataset are namespaced by pipeline name and
load id, so concurrent pipelines write disjoint files.

## Components

### 1. Per-table pipelines

- `_table_pipeline_name(settings, tdef)` returns
  `f"{settings.pipeline_name}__{tdef.dataset_table_name}"`
  (`dataset_table_name` is already normalized lowercase). Stable across runs
  so each table's local schema/state/pending packages persist in its own
  working dir under `~/.dlt/pipelines/`.
- `build_pipeline(settings, pipelines_dir=None, pipeline_name=None)` grows an
  optional name override; default behavior unchanged for existing callers.
- `_load_task` builds (attaches) the table's pipeline on the worker thread,
  runs `clear_pending_packages(pipeline, f"{table}:pre-load")` first — the
  per-table replacement for today's single startup sweep. A poisoned package
  can now only ever block its own table.
- `_PipelineHolder` is **deleted**. A commit timeout poisons only that table's
  pipeline, which nothing else touches for the rest of the run — no rebuild,
  no swap. `plan.load_timed_out` is kept for logging/observability. The next
  run's pre-load sweep drops whatever the abandoned zombie left behind.

### 2. Parallel load executor

- `load_and_record` uses `ThreadPoolExecutor(max_workers=settings.load_workers,
  thread_name_prefix="load")`. Dispatch (`on_table_done` ready-counting),
  phase sequencing (masters → transactions via separate `load_and_record`
  calls with `shutdown(wait=True)` between), watermark semantics, and the
  result/summary flow are unchanged.
- Worker threads live for the pool's lifetime, so dlt's per-thread container
  contexts are stable and bounded by the worker count.

### 3. Catalog-based destination reads (correctness prerequisite)

`get_iceberg_tables(pipeline, ...)` resolves through the pipeline's **local
schema** (`client.schema.data_tables()`). A fresh per-table pipeline has an
empty schema until its first successful run, so on each table's first parallel
run every destination read would silently degrade — worst cases:
`_table_snapshot_ids` returns `∅` making the post-load squash expire **prior
runs' history**, and `_existing_insert_at` returns `None` permanently
overwriting original `insert_at` values on merge. (Today the same degradation
exists after a watchdog rebuild; it is rare there but would be every table's
first run here.)

Fix: a new helper routes destination reads through the persistent catalog,
independent of any pipeline's local state:

```python
def _open_dest_table(settings, table_name) -> Optional[IcebergTable]:
    """Load <dataset>.<table> from the configured catalog; None if absent."""
    from dlt.common.libs.pyiceberg import get_catalog
    return get_catalog().load_table(f"{settings.dataset_name}.{table_name}")
    # NoSuchTableError / any failure -> None (best-effort, callers degrade)
```

Converted call sites (read-only helpers; the dlt **write** path is untouched):
`_coerce_unified_nulls`, `_read_destination_arrow_types`
(via `_widen_schema_to_destination`), `_existing_insert_at`,
`_table_is_hash_ready`, `_table_snapshot_ids`, `_squash_table_run_snapshots`.
Their `pipeline` parameter is replaced by `settings`. This mirrors
`gui/iceberg_browser.py`, which already uses `get_catalog()` +
`f"{dataset}.{table}"` identifiers, and it fixes the pre-existing
post-rebuild degradation as a side effect.

`gui/iceberg_maintenance.py`'s `_load_table` converts the same way, removing
the last production consumer of the shared pipeline's stale local schema.

### 4. ControlStore locking

`advance()` and `save()` mutate and iterate one shared nested dict — safe
today only because loads are serial. An internal `threading.RLock` wraps both
(row-building and the Postgres upsert both inside the lock; upserts are
ms-scale, contention is negligible). `as_dict()`/`entry()` are only called
between phases when no loads are in flight; documented as such.

### 5. Monitor: active-load set

`set_activity("load:X")` from concurrent workers would clobber each other and
corrupt peak-memory attribution. `PipelineMonitor` gains
`begin_load(table)` / `end_load(table)` maintaining an insertion-ordered set
under the existing lock. The heartbeat/peak label renders
`load[2]: appointments,claims` while the set is non-empty, else the
phase-level activity (`extract`, `draining-loads`, `finalize`) set by
`set_activity` as today. Peak attribution now names *all* tables in flight at
the peak — strictly more useful for the two-peak memory profile.

### 6. Snapshot retention via catalog

`apply_snapshot_retention(pipeline, settings)` enumerated tables from the
shared pipeline's local schema, which no longer exists. New signature
`apply_snapshot_retention(settings)`: enumerate
`get_catalog().list_tables(settings.dataset_name)`, skip names starting with
`_dlt` (preserves `include_dlt_tables=False` semantics), load each table and
apply the existing guarded property/expiry logic unchanged. Bonus: the
catalog remembers every table ever loaded, so retention now also covers
tables a local-schema rebuild had forgotten. Still best-effort end-of-run.

### 7. Config / CLI / GUI surface

- `Settings.load_workers: int = 2` (clamped to ≥ 1), read from
  `etl.load_workers`. Field comment documents the memory interplay (below).
- `oracle_to_iceberg.py` gains `--load-workers` (parity with the other worker
  flags).
- `"load_workers"` joins `EDITABLE_ETL_KEYS` in `gui/workspace.py` so the
  Settings page picks it up.

### 8. Postgres connection hygiene

Concurrency multiplies connections to both Postgres databases (catalog + app
metastore), which amplifies the known Docker-PG wedge failure mode (proxy
accepts, server never answers, no timeout → silent hang). `MetaStore`'s
engine gains `connect_args={"connect_timeout": 10}`. The catalog URI's
`connect_timeout` is a config value in `.dlt/secrets.toml` — documented in
the README/runbook, not code.

## Failure handling

- **Commit timeout:** watchdog unchanged (`_run_with_timeout`). The poisoned
  per-table pipeline is abandoned in place; the worker moves on to its next
  table with that table's own pipeline. No shared state to rebuild.
- **Zombie late-commit:** an abandoned daemon thread may still commit its
  package later. Unchanged residual risk from today, with the same bounds:
  watermarks did not advance (next run re-pulls), merge is hash-keyed upsert
  (idempotent), and the table is not retried within the run (per-run
  exclusivity), so no same-table race is introduced.
- **Load failure (non-timeout):** `clear_pending_packages` for that table's
  pipeline, watermarks preserved for committed groups — unchanged, now
  per-table by construction.
- **Crash (OOM/kill):** leftover pending packages sit in per-table working
  dirs; each table's next pre-load sweep clears its own. No startup sweep
  needed.

## Memory model and tuning

Peak load RSS ≈ `load_workers × (load_group_max_bytes × parquet→Arrow
expansion, ~3–6×)` plus merge carry-forward tables — the second peak of the
known two-peak profile multiplies by the worker count. Defaults ship
conservative: `load_workers = 2`. Runbook guidance: raise workers only after
observing server RSS headroom; when raising workers, lower
`load_group_max_bytes` proportionally; consider capping
`PYICEBERG_MAX_WORKERS` since each concurrent write also runs pyiceberg's
internal pool. A weighted staged-bytes admission semaphore is a deliberate
non-goal until server numbers demand it.

## Invariants preserved

- Merge-hash bytes frozen (no changes anywhere near hashing).
- Bucket layout, dataset name, Iceberg table names/locations: byte-identical.
- Watermark semantics: advance-on-commit per group, `save()` idempotent
  whole-state upsert.
- Phase ordering (masters before transactions, snapshots explicit-only).
- Single-commit merge patch: global, stateless, installed once — thread-safe.
- `load_workers = 1`: serial behavior, same ordering guarantees as today.

## Cutover

No data migration. The old shared working dir
(`~/.dlt/pipelines/oracle_to_iceberg`) becomes inert (deletable at leisure).
First parallel run behaves correctly against existing destination tables
because all destination reads go through the catalog (§3) — no degraded
first run, no history squash hazard. Deploy note for the Linux server: copy
branch, no new dependencies.

## Testing strategy

- **Concurrency dispatch:** with stubbed pipelines/runs, N ready tables and
  `load_workers=2` overlap (assert via barrier/latch), each `_load_task` sees
  a pipeline named for its table; `load_workers=1` preserves serial order.
- **ControlStore lock:** threaded stress — concurrent `advance()` +
  `save()` never raises and loses no watermark (compare final dict against
  expected).
- **Catalog reads:** against a real `SqlCatalog` (sqlite, as in
  `test_snapshot_retention_guards.py`): `_open_dest_table` returns the table
  when registered, `None` when absent; squash `before_ids` sees prior
  snapshots through a *fresh* pipeline-less path; retention enumerates via
  `list_tables` and skips `_dlt*`.
- **Timeout isolation:** a timed-out table's plan is marked, its pipeline is
  never reused, and a subsequent table on the same worker loads with its own
  pipeline (replaces the two `_PipelineHolder` rebuild tests).
- **Monitor:** active-set rendering and peak attribution under interleaved
  `begin_load`/`end_load`.
- **Settings/GUI:** default, clamp, TOML round-trip through
  `EDITABLE_ETL_KEYS`.
- Existing suites must pass unmodified where they don't touch removed
  surfaces; tests monkeypatching converted helpers update signatures only.

## Non-goals

- Process-per-table isolation (kill-able hung commits, RSS isolation) —
  escalation path if profiling shows GIL contention; the per-table task
  structure built here carries over.
- Direct pyiceberg writes bypassing dlt (fast-path phase 3) — complementary;
  slots into the same per-table tasks.
- Weighted memory-budget admission control — revisit after server RSS
  validation.
- `tables.json` changes, extraction changes, hash changes: none.
