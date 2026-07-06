"""Write strategy + observability.

Loading is streamed from extraction: as soon as a table has a result from every
branch, that table is handed to a dedicated load executor and written -- without
waiting for the other tables. For each table this module:

* unifies the per-branch schemas and casts each branch to it,
* loads the table as one Iceberg dataset holding all successful branches,
* picks the write disposition (full ``replace`` for a clean INITIAL across all
  branches, otherwise ``merge`` on the compound key so failed branches are
  skipped without clobbering their previously-loaded rows),
* advances the control state (per table+branch CDC watermarks).

A ``replace`` (full rebuild) is written one branch per dlt run -- first branch
``replace``, the rest ``append`` -- because the filesystem-Iceberg loader
materializes a whole load package into memory before writing; one branch per run
bounds that peak to the largest branch instead of the entire table. A ``merge``
(incremental / branch subset) is small, so it stays a single run.

Loads are serialized (a dlt pipeline is not safe to run concurrently with
itself) but start eagerly, so a table is persisted the moment it is ready. Once
everything finishes, the ``etl_control`` and ``etl_run_log`` Iceberg tables are
written and snapshot retention is applied.

The authoritative watermark store is a local JSON file (``ControlStore``); the
``etl_control`` Iceberg table is a queryable mirror of it for observability.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import dlt
import pyarrow as pa
import pyarrow.parquet as pq

from .config import HELPER_RESERVED_COLUMNS, MODE_INITIAL, Settings, TableDef, now_local
from .oracle_extract import ExtractResult, Watermark
from .progress import PipelineMonitor
from . import types_map

log = logging.getLogger("etl.load")

# Hint for every timestamp column the pipeline *generates* (insert_at /
# Recorded_updated_at data columns + the etl_control / etl_run_log time columns).
# dlt defaults timestamp columns to ``timezone=True`` and, at write time,
# attaches UTC to our naive ``now_local()`` values WITHOUT shifting them -- so a
# local 17:00 is stored as the instant 17:00Z and any UTC+offset reader renders
# it shifted (the "+3h" bug). ``timezone: False`` makes dlt keep the column naive
# (Iceberg ``timestamp`` without zone), so the stored value IS the local
# wall-clock and reads back identically in any reader's timezone. precision=6
# matches the ``pa.timestamp("us")`` / Python-datetime microsecond resolution.
#
# This is a factory (returns a FRESH dict each call) on purpose: dlt stamps the
# column ``name`` into the hint dict *in place*. Sharing a single dict across
# several columns of one ``columns={...}`` map makes the last column's name
# overwrite all the earlier ones, so they normalize to the same name, collide, and
# get merged into a single column ("... collides with other column. Both columns
# got merged into one") -- which silently drops the timezone hint from every column
# but the last. Give each column its own dict so its hint survives.
def _naive_ts_hint() -> dict:
    return {"data_type": "timestamp", "timezone": False, "precision": 6}


# --------------------------------------------------------------------------- #
# Control state (authoritative local watermark store)
# --------------------------------------------------------------------------- #
def _wm_advance(old: Optional[dict], new: Watermark) -> Optional[dict]:
    """Return the greater of an existing stored watermark and a fresh one."""
    if new.value is None:
        return old
    if old is None:
        return new.to_dict()
    try:
        if new.kind == "number":
            greater = float(new.value) > float(old["value"])
        else:  # datetime/string compare lexically (fixed format)
            greater = str(new.value) > str(old["value"])
    except (ValueError, KeyError, TypeError):
        greater = True
    return new.to_dict() if greater else old


class ControlStore:
    """Local JSON store of per-(table, branch) CDC state."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.data: dict = {}

    def load(self) -> "ControlStore":
        if self.path.exists():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        return self

    def as_dict(self) -> dict:
        return self.data

    def entry(self, table: str, branch: str) -> dict:
        return self.data.get(table, {}).get(branch, {})

    def advance(self, result: ExtractResult) -> None:
        """Move watermarks forward for a successfully loaded (table, branch)."""
        tbl = self.data.setdefault(result.table, {})
        cur = tbl.setdefault(result.branch, {})
        cur["last_cdc"] = _wm_advance(cur.get("last_cdc"), result.new_cdc)
        cur["last_date"] = _wm_advance(cur.get("last_date"), result.new_date)
        cur["status"] = result.status
        cur["row_count"] = result.row_count
        cur["duration_ms"] = result.duration_ms
        cur["last_run_at"] = now_local().isoformat()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, default=str), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Per-table load plan
# --------------------------------------------------------------------------- #
def _require_non_null(schema: pa.Schema, names: list[str]) -> pa.Schema:
    """Mark the named columns NOT NULL in an Arrow schema.

    The merge-key columns (table PK + BRANCH_ID) are declared via the resource's
    ``primary_key`` hint, which dlt treats as *required*. Our unified Arrow schema
    reports them as nullable, so on every load dlt logged "column hints were
    different" and kept its own (required) hint. Aligning the Arrow schema's
    nullability with that hint silences the warning -- dlt already enforces it, so
    nothing about the data or the destination schema changes.
    """
    wanted = set(names)
    fields = [
        pa.field(f.name, f.type, nullable=False, metadata=f.metadata)
        if (f.name in wanted and f.nullable) else f
        for f in schema
    ]
    return pa.schema(fields)


def _strip_reserved(schema: pa.Schema) -> pa.Schema:
    """Drop reserved helper watermark columns from a staged-parquet schema.

    A helper-driven table projects ``ETL_HELPER_CDC`` / ``ETL_HELPER_DATE`` into
    the staged parquet purely so the watermark can be captured. They must never
    reach Iceberg: removing them from the unified schema is enough, because
    ``cast_table_to_schema`` keeps only target-schema fields, so each streamed
    batch drops them automatically.
    """
    keep = [f for f in schema if f.name.upper() not in HELPER_RESERVED_COLUMNS]
    return pa.schema(keep)


@dataclass
class TableLoadPlan:
    tdef: TableDef
    success: list[ExtractResult]
    failed: list[ExtractResult]
    unified_schema: Optional[pa.Schema] = None
    disposition: str = "merge"
    schema_diffs: dict = field(default_factory=dict)
    load_status: str = "PENDING"     # SUCCESS | FAILED | SKIPPED
    load_error: Optional[str] = None


def _plan_table(
    tdef: TableDef,
    results: list[ExtractResult],
    settings: Settings,
    total_branches: int,
    branches_in_run: int,
) -> TableLoadPlan:
    success = [r for r in results if r.status == "SUCCESS" and r.staged_path]
    failed = [r for r in results if r.status != "SUCCESS"]
    plan = TableLoadPlan(tdef=tdef, success=success, failed=failed)

    if not success:
        plan.load_status = "SKIPPED"
        plan.load_error = "no successful branches"
        return plan

    # Snapshot (append-only) tables append a full copy every run and are never
    # merged or replaced, so the schema-unify still runs below but the write is
    # a plain append regardless of mode/branch coverage.
    # Unify schemas across the successful branches and record per-branch drift.
    # Reserved helper watermark columns are stripped first so they are absent
    # from the unified schema (and thus dropped on cast) and never flagged as
    # branch drift.
    branch_schemas = {r.branch: _strip_reserved(pq.read_schema(r.staged_path))
                      for r in success}
    plan.unified_schema = types_map.unify_schemas(list(branch_schemas.values()))
    for r in success:
        diff = types_map.schema_diff(branch_schemas[r.branch], plan.unified_schema)
        if diff["missing_in_branch"] or diff["extra_in_branch"] or diff["type_widened"]:
            plan.schema_diffs[r.branch] = diff
            log.info("[%s] schema drift on branch %s: %s",
                     tdef.dataset_table_name, r.branch, diff)

    # Write disposition.
    #
    # A from-scratch INITIAL that *attempts* every configured branch is written
    # as a single bulk `replace` (one Iceberg commit). We do this even when some
    # branches failed: INITIAL is a full rebuild, so a failed branch simply
    # contributes no rows this round (re-run INITIAL once it is reachable).
    #
    # The alternative -- dlt's `merge` -- upserts in 1,000-row chunks and commits
    # a new Iceberg snapshot per chunk (see dlt.common.libs.pyiceberg), each
    # rewriting the growing table metadata. On a large table that is thousands of
    # commits and is catastrophically slow, so we avoid it for full rebuilds.
    #
    # `merge` is still used when we are NOT covering all branches (an INITIAL over
    # a `--branch` subset, or any INCREMENTAL), so we never clobber branches that
    # are not part of this run. INCREMENTAL deltas are small, so the chunked
    # upsert cost there is acceptable.
    full_rebuild = (
        settings.mode == MODE_INITIAL
        and branches_in_run == total_branches
    )
    if tdef.is_snapshot:
        plan.disposition = "append"
    else:
        plan.disposition = "replace" if full_rebuild else "merge"
    return plan


def _coerce_unified_nulls(pipeline, tdef: TableDef, schema: pa.Schema) -> pa.Schema:
    """Replace ``null``-typed columns in the unified schema with concrete types.

    pyarrow infers the ``null`` type for a column that is entirely null across
    every branch this run (an unconstrained NUMBER with no/all-null values, or a
    0-row incremental delta where every column is empty). Feeding that ``null``
    column to dlt/pyiceberg fails when the destination already has the column
    typed -- pyiceberg tries to cast the existing column to ``null`` and raises
    ``Unsupported cast from <type> to null using function cast_null``.

    Prefer the destination's existing Arrow type for each null column (read
    best-effort from the Iceberg table) so a merge sees identical types and is a
    no-op; fall back to string for a column the destination doesn't have yet.
    Returns ``schema`` unchanged when it has no null columns.
    """
    null_names = [f.name for f in schema if pa.types.is_null(f.type)]
    if not null_names:
        return schema

    overrides: dict[str, pa.DataType] = {}
    try:
        from dlt.common.libs.pyiceberg import get_iceberg_tables
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        tbl = get_iceberg_tables(pipeline).get(tdef.dataset_table_name)
        if tbl is not None:
            # dlt normalizes identifiers to lower snake; for these clean
            # UPPER_SNAKE / already-lower names that is just lower-casing.
            dest = {f.name: f.type for f in schema_to_pyarrow(tbl.schema())}
            for name in null_names:
                t = dest.get(name.lower())
                if t is not None and not pa.types.is_null(t):
                    overrides[name] = t
    except Exception as exc:  # noqa: BLE001 - best effort; string fallback is safe
        log.warning("[%s] could not read destination types for null-column "
                    "coercion: %s", tdef.dataset_table_name, exc)

    coerced = types_map.replace_null_types(schema, overrides=overrides)
    log.info("[%s] coerced all-null columns to concrete types: %s",
             tdef.dataset_table_name,
             {n: str(coerced.field(n).type) for n in null_names})
    return coerced


def _carry_forward_insert_at(
    batch: pa.Table, existing: pa.Table, join_keys: list[str], insert_col: str
) -> pa.Table:
    """Replace ``insert_col`` in ``batch`` with the existing value where the
    compound key already exists, keeping new rows' load-time value.

    ``existing`` carries the merge key columns + ``insert_col`` for rows already
    in the table (aligned to the batch's column names/types). A left-outer join
    on the key brings each row's prior insert_at in; ``coalesce`` keeps it for
    updates and falls back to the batch's now() for genuinely new rows. Output
    column order matches the input batch (rows may be reordered, which is
    irrelevant for a merge upsert).
    """
    import pyarrow.compute as pc

    joined = batch.join(existing, keys=join_keys, join_type="left outer",
                        right_suffix="__prev")
    prev_col = f"{insert_col}__prev"
    if prev_col not in joined.column_names:
        return batch
    merged = pc.coalesce(joined.column(prev_col), joined.column(insert_col))
    return pa.table(
        {name: (merged if name == insert_col else joined.column(name))
         for name in batch.column_names}
    )


def _existing_insert_at(
    pipeline, tdef: TableDef, settings: Settings, branches: list[int],
    unified_schema: pa.Schema,
) -> Optional[pa.Table]:
    """Read existing rows' insert_at (+ merge key) for ``branches`` for carry-forward.

    ``branches`` holds the numeric BRANCH_ID values for this run (see BranchConfig.id).

    Returns an Arrow table whose columns are named/typed like the incoming batch
    (merge key columns + ``insert_at``), or ``None`` when there's nothing to
    carry forward -- the table doesn't exist yet (first load), it predates the
    insert_at column, or the read fails. Best-effort: a failure here never fails
    the load, it only means updated rows fall back to this run's load time.

    The scan is pruned to the branches in this run (BRANCH_ID is the partition
    column) and projects only the key + insert_at columns, so it reads a small
    slice of the table rather than all of it.
    """
    insert_col = settings.inserted_ts_column
    join_keys = list(tdef.key_columns) + [settings.branch_id_column]
    try:
        from dlt.common.libs.pyiceberg import get_iceberg_tables
        from pyiceberg.expressions import In
        tables = get_iceberg_tables(pipeline)
    except Exception as exc:  # noqa: BLE001 - best effort
        log.warning("[%s] insert_at carry-forward unavailable: %s",
                    tdef.dataset_table_name, exc)
        return None

    tbl = tables.get(tdef.dataset_table_name)
    if tbl is None:
        return None  # first load of this table: nothing to preserve

    # dlt normalizes identifiers to lower snake; for these clean UPPER_SNAKE /
    # already-lower names that is just lower-casing.
    insert_norm = insert_col.lower()
    key_norms = [k.lower() for k in join_keys]
    branch_norm = settings.branch_id_column.lower()
    iceberg_cols = {f.name for f in tbl.schema().fields}
    if insert_norm not in iceberg_cols or any(k not in iceberg_cols for k in key_norms):
        return None  # table predates insert_at (or key) -> skip

    try:
        existing = tbl.scan(
            row_filter=In(branch_norm, set(branches)),
            selected_fields=tuple(key_norms + [insert_norm]),
        ).to_arrow()
    except Exception as exc:  # noqa: BLE001 - best effort
        log.warning("[%s] insert_at carry-forward scan failed: %s",
                    tdef.dataset_table_name, exc)
        return None
    if existing.num_rows == 0:
        return None

    # Rename normalized columns back to the batch's names, then align key/branch
    # column types to the unified schema so the join keys match exactly.
    rename = {insert_norm: insert_col}
    for orig, normed in zip(join_keys, key_norms):
        rename[normed] = orig
    existing = existing.rename_columns([rename.get(n, n) for n in existing.column_names])
    for name in existing.column_names:
        if name != insert_col and name in unified_schema.names:
            target = unified_schema.field(name).type
            col = existing.column(name)
            if not col.type.equals(target):
                idx = existing.schema.get_field_index(name)
                existing = existing.set_column(idx, name, col.cast(target, safe=False))
    return existing


def _iceberg_resource(
    plan: TableLoadPlan,
    settings: Settings,
    paths: list,
    disposition: str,
    existing_insert_at: Optional[pa.Table] = None,
):
    """Create a dlt Iceberg resource that streams ``paths`` under ``disposition``.

    The Iceberg table is partitioned by BRANCH_ID (identity transform) so every
    branch's rows land in their own partition -- branch-scoped reads prune to a
    single partition and per-branch merges/rewrites only touch that branch's
    files. The ``partition`` column hint is applied at table creation; dlt
    normalizes the column name to match the destination schema. The hints are
    identical on every call so successive replace/append runs see one stable
    schema + partition spec.

    Each staged parquet is streamed in ``load_batch_rows``-row Arrow batches
    rather than materialized whole, so the *normalize* side is bounded by one
    batch. The *load* side's peak (dlt's ``arrow_dataset.to_table()``) is bounded
    separately by how many paths go into one run -- see
    ``_run_per_branch_rebuild``.

    When ``existing_insert_at`` is given (incremental merge), each batch's
    ``insert_at`` is rewritten so updated rows keep their original first-load
    time -- a true ``created_at`` -- while new rows keep this run's load time.
    """
    tdef = plan.tdef
    is_snapshot = tdef.is_snapshot
    # Snapshot tables are append-only: no merge key. Every other table merges on
    # its compound (PK + BRANCH_ID) key.
    primary_key = [] if is_snapshot else list(tdef.key_columns) + [settings.branch_id_column]
    # Make the merge-key columns NOT NULL so the Arrow schema agrees with the
    # resource's primary_key hint (otherwise dlt warns about differing hints on
    # every load -- see _require_non_null).
    schema = _require_non_null(plan.unified_schema, primary_key)
    batch_rows = settings.load_batch_rows
    insert_col = settings.inserted_ts_column

    def _finish(tbl: pa.Table) -> pa.Table:
        tbl = types_map.cast_table_to_schema(tbl, schema)
        if existing_insert_at is not None and tbl.num_rows:
            tbl = _carry_forward_insert_at(tbl, existing_insert_at,
                                           primary_key, insert_col)
        return tbl

    # Partition by BRANCH_ID always; snapshot tables additionally partition by
    # the snapshot date (version_date) so a run/day's copy prunes to its own
    # files. Snapshot tables also carry the run-scoped ``version`` timestamp.
    columns = {
        settings.branch_id_column: {"partition": True},
        settings.inserted_ts_column: _naive_ts_hint(),
        settings.recorded_ts_column: _naive_ts_hint(),
    }
    if is_snapshot:
        columns[settings.snapshot_date_column] = {"partition": True}
        columns[settings.snapshot_version_column] = _naive_ts_hint()

    @dlt.resource(
        name=tdef.dataset_table_name,
        write_disposition=disposition,
        primary_key=primary_key or None,
        table_format="iceberg",
        columns=columns,
    )
    def _resource():
        for path in paths:
            pf = pq.ParquetFile(path)
            yielded = False
            for batch in pf.iter_batches(batch_size=batch_rows):
                yield _finish(pa.Table.from_batches([batch]))
                yielded = True
            # A 0-row branch produces no batches; still emit one empty table so
            # the unified schema is carried even if every branch is empty.
            if not yielded:
                yield _finish(pf.read())

    return _resource()


def _run_per_branch_rebuild(
    pipeline,
    plan: TableLoadPlan,
    settings: Settings,
    control: ControlStore,
) -> None:
    """Load a full-rebuild (``replace``) table one branch per dlt run.

    The dlt filesystem-Iceberg loader materializes a whole load package into one
    Arrow table (``arrow_dataset.to_table()``) before writing -- regardless of
    write disposition -- so a single all-branches ``replace`` peaks at the entire
    table (e.g. ~11GB for 3.5M rows). By writing one branch per ``pipeline.run``
    instead -- the first branch with ``replace`` (which truncates any prior
    table via pyiceberg ``overwrite``) and every later branch with ``append`` --
    each load package holds a single branch, so the peak drops to the largest
    branch.

    Watermarks advance per branch as it commits, so a mid-stream failure still
    leaves the already-loaded branches (and their watermarks) correct; the failed
    and not-yet-attempted branches keep their old watermark and are re-pulled next
    run. The caller persists ``control`` and marks the table FAILED if this raises.
    """
    disposition = "replace"  # first branch truncates the prior table
    for r in plan.success:
        pipeline.run([_iceberg_resource(plan, settings, [r.staged_path], disposition)])
        control.advance(r)
        disposition = "append"  # everything after the first adds on


def _run_per_branch_append(
    pipeline,
    plan: TableLoadPlan,
    settings: Settings,
    control: ControlStore,
) -> None:
    """Append a snapshot table one branch per dlt run (memory-bounded).

    Like ``_run_per_branch_rebuild`` this bounds the loader's peak to a single
    branch, but *every* branch appends -- including the first -- so snapshots
    stored by earlier runs are preserved (the whole point of a snapshot table).
    Watermarks advance per branch as it commits so a mid-stream failure leaves
    the already-appended branches correct.
    """
    for r in plan.success:
        pipeline.run([_iceberg_resource(plan, settings, [r.staged_path], "append")])
        control.advance(r)


# --------------------------------------------------------------------------- #
# Control + log Iceberg tables
# --------------------------------------------------------------------------- #
def _control_rows(plans: list[TableLoadPlan], settings: Settings, run_id: str) -> list[dict]:
    now = now_local()
    rows = []
    for plan in plans:
        load_mode = "SNAPSHOT" if plan.tdef.is_snapshot else settings.mode
        for r in plan.success + plan.failed:
            rows.append({
                "table_name": r.table,
                "branch_id": r.branch_id,
                "load_mode": load_mode,
                "status": r.status if plan.load_status != "FAILED" else "FAILED",
                "row_count": r.row_count,
                "attempts": r.attempts,
                "last_cdc_value": (r.new_cdc.value if r.status == "SUCCESS" else None),
                "last_cdc_kind": r.new_cdc.kind,
                "last_date_value": (r.new_date.value if r.status == "SUCCESS" else None),
                "last_date_kind": r.new_date.kind,
                "duration_ms": r.duration_ms,
                "start_time": r.start_time,
                "end_time": r.end_time,
                "error_details": r.error,
                "pipeline_run_id": run_id,
                "updated_at": now,
            })
    return rows


def _log_rows(plans: list[TableLoadPlan], settings: Settings, run_id: str) -> list[dict]:
    now = now_local()
    rows = []
    for plan in plans:
        load_mode = "SNAPSHOT" if plan.tdef.is_snapshot else settings.mode
        for r in plan.success + plan.failed:
            diff = plan.schema_diffs.get(r.branch)
            rows.append({
                "pipeline_run_id": run_id,
                "table_name": r.table,
                "branch_id": r.branch_id,
                "load_mode": load_mode,
                "row_count": r.row_count,
                "start_time": r.start_time,
                "end_time": r.end_time,
                "duration_ms": r.duration_ms,
                "status": r.status,
                "attempts": r.attempts,
                "write_disposition": plan.disposition,
                "load_status": plan.load_status,
                "error_details": r.error or plan.load_error,
                "schema_discrepancy": json.dumps(diff) if diff else None,
                "recorded_at": now,
            })
    return rows


def _write_observability(pipeline, plans, settings, run_id) -> None:
    control_rows = _control_rows(plans, settings, run_id)
    log_rows = _log_rows(plans, settings, run_id)

    # Explicit text hints: these columns are often all-null on a clean run, so
    # without a hint dlt can't infer a type and the schema would drift run-to-run.
    control_hints = {
        "error_details": {"data_type": "text"},
        "last_cdc_value": {"data_type": "text"},
        "last_date_value": {"data_type": "text"},
        # Keep generated time columns as naive local wall-clock (see _naive_ts_hint).
        "updated_at": _naive_ts_hint(),
        "start_time": _naive_ts_hint(),
        "end_time": _naive_ts_hint(),
    }
    log_hints = {
        "error_details": {"data_type": "text"},
        "schema_discrepancy": {"data_type": "text"},
        "recorded_at": _naive_ts_hint(),
        "start_time": _naive_ts_hint(),
        "end_time": _naive_ts_hint(),
    }

    @dlt.resource(name=settings.control_table_name, write_disposition="merge",
                  primary_key=["table_name", "branch_id"], table_format="iceberg",
                  columns=control_hints)
    def control_res():
        yield control_rows

    @dlt.resource(name=settings.log_table_name, write_disposition="append",
                  table_format="iceberg", columns=log_hints)
    def log_res():
        yield log_rows

    resources = []
    if control_rows:
        resources.append(control_res())
    if log_rows:
        resources.append(log_res())
    if resources:
        pipeline.run(resources)


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #
@dataclass
class LoadSummary:
    run_id: str
    plans: list[TableLoadPlan]
    results: list[ExtractResult] = field(default_factory=list)
    extraction_error: Optional[BaseException] = None

    def render(self) -> str:
        lines = [f"Run {self.run_id} summary:"]
        for p in self.plans:
            lines.append(
                f"  {p.tdef.dataset_table_name:<28} {p.load_status:<8} "
                f"disp={p.disposition:<7} ok={len(p.success)} "
                f"fail={len(p.failed)} rows={sum(r.row_count for r in p.success)}"
            )
        return "\n".join(lines)


def apply_snapshot_retention(pipeline, settings: Settings) -> None:
    """Configure + enforce per-table snapshot retention (keep last N days).

    Sets the Iceberg table properties on first creation (and keeps them set),
    then expires snapshots older than the window so metadata/manifests don't grow
    unbounded. ``min-snapshots-to-keep`` guarantees the current snapshot survives.
    Best-effort: a maintenance failure never fails the load.
    """
    if not settings.snapshot_maintenance:
        return
    try:
        from dlt.common.libs.pyiceberg import get_iceberg_tables
    except ImportError:
        log.warning("pyiceberg table access unavailable; skipping snapshot retention")
        return

    max_age_ms = settings.snapshot_expire_days * 24 * 60 * 60 * 1000
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=settings.snapshot_expire_days)
    props = {
        "history.expire.max-snapshot-age-ms": str(max_age_ms),
        "history.expire.min-snapshots-to-keep": str(settings.snapshot_min_to_keep),
    }

    try:
        tables = get_iceberg_tables(pipeline)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not open Iceberg tables for retention: %s", exc)
        return

    for name, tbl in tables.items():
        try:
            with tbl.transaction() as txn:
                txn.set_properties(props)
            tbl.maintenance.expire_snapshots().older_than(cutoff).commit()
            log.info("[%s] retention applied: keep %dd, %d snapshot(s) remain",
                     name, settings.snapshot_expire_days, len(list(tbl.snapshots())))
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] snapshot retention failed: %s", name, exc)


def build_pipeline(settings: Settings):
    return dlt.pipeline(
        pipeline_name=settings.pipeline_name,
        destination=dlt.destinations.filesystem(
            bucket_url=settings.destination_bucket_url
        ),
        dataset_name=settings.dataset_name,
    )


def _load_one_table(
    pipeline,
    tdef: TableDef,
    results: list[ExtractResult],
    settings: Settings,
    control: ControlStore,
    total_branches: int,
    branches_in_run: int,
    monitor: PipelineMonitor,
) -> TableLoadPlan:
    """Plan, write and record a single table once all its branches are done.

    Runs as its own dlt load package so it lands independently of other tables.
    """
    plan = _plan_table(tdef, results, settings, total_branches, branches_in_run)
    if not plan.success:
        log.info("[%s] skipped: %s", tdef.dataset_table_name, plan.load_error)
        monitor.record_table_loaded(plan.load_status)
        return plan

    # Nothing extracted across every branch (a common incremental no-op): there
    # is no data to write, so skip the dlt run entirely. This both avoids an
    # empty Iceberg snapshot per unchanged table and sidesteps the all-null
    # schema problem -- a 0-row branch infers every unconstrained-NUMBER column
    # as the Arrow ``null`` type, which pyiceberg cannot reconcile against the
    # table's existing typed columns ("Unsupported cast from <type> to null").
    # Watermarks/status still advance for the successful (0-row) branches.
    if sum(r.row_count for r in plan.success) == 0:
        for r in plan.success:
            control.advance(r)
        control.save()
        plan.load_status = "SUCCESS"
        log.info("[%s] no rows; load skipped (disp=%s ok=%d fail=%d)",
                 tdef.dataset_table_name, plan.disposition,
                 len(plan.success), len(plan.failed))
        monitor.record_table_loaded(plan.load_status)
        return plan

    # A column that is all-null in the rows that *were* extracted (an
    # unconstrained NUMBER with only null values) is likewise inferred as the
    # Arrow ``null`` type; coerce it to a concrete type so the load never asks
    # pyiceberg to cast an existing column to ``null``.
    plan.unified_schema = _coerce_unified_nulls(pipeline, tdef, plan.unified_schema)

    # Label the peak-memory window: this is where the staged parquet is read
    # back into Arrow and committed to Iceberg.
    monitor.set_activity(f"load:{tdef.dataset_table_name}")
    try:
        if plan.disposition == "replace":
            # Full rebuild: one branch per dlt run (first replace, rest append)
            # so the loader never materializes more than one branch at a time.
            # Watermarks advance inside the loop as each branch commits.
            _run_per_branch_rebuild(pipeline, plan, settings, control)
        elif plan.disposition == "append":
            # Snapshot append: one branch per dlt run (all append) so history
            # from earlier runs is preserved and memory stays bounded.
            _run_per_branch_append(pipeline, plan, settings, control)
        else:
            # Incremental / branch-subset merge: small deltas, one run is fine.
            # Preserve each row's original insert_at across updates by carrying
            # forward the existing value for rows already in the table.
            existing = _existing_insert_at(
                pipeline, tdef, settings,
                [r.branch_id for r in plan.success], plan.unified_schema)
            pipeline.run([_iceberg_resource(
                plan, settings, [r.staged_path for r in plan.success],
                plan.disposition, existing_insert_at=existing)])
            # Advance watermarks only for a table that actually loaded.
            for r in plan.success:
                control.advance(r)
        control.save()
        plan.load_status = "SUCCESS"
        log.info("[%s] loaded: disp=%s ok=%d fail=%d rows=%d",
                 tdef.dataset_table_name, plan.disposition, len(plan.success),
                 len(plan.failed), sum(r.row_count for r in plan.success))
    except Exception as exc:  # noqa: BLE001 - isolate per-table load failures
        plan.load_status = "FAILED"
        plan.load_error = f"{type(exc).__name__}: {exc}"
        log.error("[%s] load failed: %s", tdef.dataset_table_name, exc)
        # Persist any per-branch watermarks that committed before the failure.
        control.save()
    finally:
        # Hand the label back to extraction (which may still be running).
        monitor.set_activity("extract")
        monitor.record_table_loaded(plan.load_status)
    return plan


def load_and_record(
    run_extraction_fn: Callable[[Callable[[ExtractResult], None]], object],
    tables: list[TableDef],
    settings: Settings,
    control: ControlStore,
    run_id: str,
    total_branches: int,
    branches_in_run: int,
) -> LoadSummary:
    """Stream extraction into loads: write each table the moment its branches finish.

    ``run_extraction_fn(on_table_done)`` runs the (threaded) extraction and calls
    ``on_table_done`` for every (branch, table) result. When a table has received
    a result from all ``branches_in_run`` branches, it is handed to a dedicated
    single-threaded load executor and written immediately -- concurrently with
    the extraction (and loading) of the other tables. Loads are serialized
    because a dlt pipeline is not safe to run concurrently with itself, but each
    table still lands as soon as it is ready rather than waiting for the rest.
    """
    pipeline = build_pipeline(settings)
    table_defs = {t.dataset_table_name: t for t in tables}
    order = {t.dataset_table_name: i for i, t in enumerate(tables)}

    remaining = {name: branches_in_run for name in table_defs}
    collected: dict[str, list[ExtractResult]] = {name: [] for name in table_defs}
    all_results: list[ExtractResult] = []
    lock = threading.Lock()
    load_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="load")
    load_futures = []

    monitor = PipelineMonitor(
        total_units=branches_in_run * len(tables),
        total_tables=len(tables),
        interval_s=settings.progress_interval_s,
        enabled=settings.progress_enabled,
    ).start()

    def on_table_done(result: ExtractResult) -> None:
        name = result.table
        monitor.record_unit(result.row_count, result.status)
        with lock:
            collected[name].append(result)
            all_results.append(result)
            remaining[name] -= 1
            ready = remaining[name] == 0
            batch = list(collected[name]) if ready else None
        if ready:
            load_futures.append(load_pool.submit(
                _load_one_table, pipeline, table_defs[name], batch,
                settings, control, total_branches, branches_in_run, monitor))

    extraction_error: Optional[BaseException] = None
    try:
        monitor.set_activity("extract")
        run_extraction_fn(on_table_done)
    except Exception as exc:  # noqa: BLE001 - surfaced after partial flush
        extraction_error = exc
        log.error("Extraction aborted: %s", exc)
    finally:
        monitor.set_activity("draining-loads")
        load_pool.shutdown(wait=True)

    plans = [f.result() for f in load_futures]
    plans.sort(key=lambda p: order.get(p.tdef.dataset_table_name, 1_000_000))

    # Finalize under try/finally so the memory/progress summary is always emitted
    # -- a finalization failure is exactly when the peak numbers are most useful.
    try:
        control.save()
        # Observability + retention reflect everything that completed this run.
        monitor.set_activity("finalize")
        _write_observability(pipeline, plans, settings, run_id)
        apply_snapshot_retention(pipeline, settings)
    finally:
        monitor.stop()

    return LoadSummary(run_id=run_id, plans=plans, results=all_results,
                       extraction_error=extraction_error)
