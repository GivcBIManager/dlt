# Clear staged parquet after successful load — design

- **Date:** 2026-07-22
- **Status:** Proposed (awaiting spec review)
- **Scope owner:** ETL (oracle → Iceberg pipeline)

## 1. Context

The pipeline extracts Oracle → Iceberg in two stages. Extraction streams each
`(table, branch)` result to a local staged parquet at
`_staging/<table>/<branch>.parquet` ([`etl/oracle_extract.py`](../../../etl/oracle_extract.py)
`_stage`), and the load reads those parquet files back and commits them to
Iceberg ([`etl/iceberg_load.py`](../../../etl/iceberg_load.py)).

The staged parquet is written once per run and never cleaned up afterward, so
`_staging/` grows without bound across runs — one file per branch per table,
overwritten only when that same table+branch is re-extracted. For wide/high-row
tables this is a large amount of redundant local disk sitting idle after the
data is already durably in Iceberg.

`_staging/` is gitignored. There is already best-effort cleanup of the
intermediate `.parquet.tmp` files (`_cleanup_tmp`), but not of the final
`.parquet`.

### The one non-load consumer of staged parquet

Besides the load, the staged parquet is read by exactly one other code path:
`dq_check --self-test` ([`etl/dq_check.py`](../../../etl/dq_check.py) `_staged_file`),
an **offline** DQ mode that reconciles the lake against the staged parquet
instead of querying Oracle. Normal DQ hits Oracle directly, and ingest and DQ
are separate runs (the GUI/orchestrator launch `oracle_to_iceberg.py` and
`dq_check.py` as independent commands). So deleting the parquet after a load
only affects a subsequent `--self-test` DQ run — which must therefore be
opt-out-able.

## 2. Goal

Reclaim local disk by deleting a branch's staged parquet as soon as that
branch's rows are durably committed to Iceberg, controlled by a setting that
defaults **on**, with a per-run escape hatch that preserves the
`dq_check --self-test` workflow.

Non-goals (YAGNI): GUI toggle, deleting unrelated files, parquet compaction,
cleaning staged files for tables not loaded this run.

## 3. Design

### 3.1 Config — `etl/config.py`

- Add a `Settings` field:

  ```python
  cleanup_staging_after_load: bool = True
  ```

- Read it in `load_settings` from the `[etl]` section, mirroring the other
  `_cfg(...)` reads:

  ```python
  cleanup_staging_after_load=bool(_cfg("etl.cleanup_staging_after_load", True)),
  ```

  Overrides already flow through the generic `hasattr`/`setattr` loop in
  `load_settings`, so a CLI override for this key requires no other change.

### 3.2 CLI — `oracle_to_iceberg.py`

- Add a flag:

  ```python
  p.add_argument("--keep-staging", action="store_true",
                 help="keep staged parquet after load (default: delete it to "
                      "reclaim disk; keep it to run dq_check --self-test)")
  ```

- In `build_overrides`, translate it (only when set, so the config default wins
  otherwise):

  ```python
  if args.keep_staging:
      overrides["cleanup_staging_after_load"] = False
  ```

### 3.3 Cleanup mechanism — `etl/iceberg_load.py`

Add a module-level, best-effort helper:

```python
def _cleanup_staged(result: ExtractResult, settings: Settings) -> None:
    """Delete a branch's staged parquet once its rows are durably in Iceberg.

    The staged parquet exists only to feed the load; after the branch's
    watermark advances it is dead weight. Best-effort — a failed unlink is
    logged and never fails the load (mirrors _cleanup_tmp). No-op when
    cleanup is disabled (e.g. to run dq_check --self-test against the staged
    files afterward).
    """
    if not settings.cleanup_staging_after_load:
        return
    path = result.staged_path
    if path is None:
        return
    try:
        Path(path).unlink(missing_ok=True)
        # Drop the now-empty table dir when this was the last branch; an
        # OSError just means other branches' files remain — leave it.
        try:
            Path(path).parent.rmdir()
        except OSError:
            pass
    except OSError as exc:
        log.warning("[%s] could not delete staged parquet %s: %s",
                    result.table, path, exc)
```

Call it at the **per-branch commit point** — immediately after
`control.advance(r)` — in all four success paths:

1. `_run_per_branch_rebuild` (replace) — inside the `for r in plan.success` loop.
2. `_run_per_branch_append` (append) — inside the `for r in plan.success` loop.
3. Merge path in `_load_one_table` — in the `for r in plan.success:
   control.advance(r)` loop.
4. 0-row early-return path in `_load_one_table` — in its `for r in plan.success`
   loop.

`settings` is already in scope in all four; the per-branch loop functions
receive it as a parameter.

### 3.4 Rationale for the timing

`control.advance(r)` is the exact moment a branch becomes durably loaded — its
watermark advances, so it will not be re-extracted next run. Deleting the
parquet there:

- reclaims disk as early as possible (per branch, not after the whole table);
- correctly cleans branches that committed even when a **later** branch in the
  same table fails (that table is marked `FAILED`, but the committed branches'
  parquet is still safe to drop);
- is safe relative to `control.save()`: the parquet is fully reproducible from
  Oracle, so if the later `save()` fails, the branch simply re-extracts (and
  re-stages) next run — no data is lost by having deleted the parquet first.

Nothing after the advance point reads the parquet: snapshot squashing operates
on Iceberg metadata, and observability (`_write_observability`) uses only
`ExtractResult` metadata (`row_count`, `status`, …), never the file contents.

## 4. Edge cases

- **Cleanup failure** (file locked, permission): logged at WARNING, never fatal.
- **0-row branches** still stage a parquet (schema-only); covered by path 4.
- **`--self-test` ingest** (synthetic data) also stages and loads, so cleanup
  applies there too — consistent behavior.
- **`--keep-staging`** leaves every staged file in place, preserving a
  subsequent `dq_check --self-test`.
- **Branch-subset runs**: `rmdir` fails (dir still holds other branches' files)
  and is swallowed; only the loaded branch's file is removed.

## 5. Testing

Unit tests for `_cleanup_staged` (no Oracle, no dlt run needed):

- deletes the file when `cleanup_staging_after_load=True`;
- no-op (file remains) when `cleanup_staging_after_load=False`;
- tolerates an already-missing file (`missing_ok`);
- removes the table dir when it becomes empty, but keeps a dir that still holds
  another branch's parquet;
- `staged_path is None` is a no-op.

Behavioral check via the existing self-test synthetic-load harness (see
`tests/`): after a successful `--self-test` load, `_staging/<table>/<branch>.parquet`
is gone by default and present when `cleanup_staging_after_load=False`.

## 6. Files touched

- `etl/config.py` — new setting + `load_settings` read.
- `oracle_to_iceberg.py` — `--keep-staging` flag + `build_overrides`.
- `etl/iceberg_load.py` — `_cleanup_staged` helper + 4 call sites.
- `tests/` — new unit test module for `_cleanup_staged`.
