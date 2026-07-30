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

Loads run on a small worker pool (``Settings.load_workers``); each table loads
through its own per-table dlt pipeline (a dlt pipeline is not safe to run
concurrently with itself, but distinct pipelines on distinct threads are
fine), starting eagerly the moment the table is ready. Once everything
finishes, observability rows are written to Postgres and snapshot retention
is applied via the persistent Iceberg catalog.

The authoritative watermark store is the Postgres ``etl_meta.control_state``
table (``ControlStore``); the ``etl_control`` Iceberg table is a queryable
mirror of it for observability.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import struct
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Optional

import dlt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import HELPER_RESERVED_COLUMNS, MODE_INITIAL, Settings, TableDef, now_local
from .metastore import MetaStore
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
    """Postgres-backed per-(table, branch) CDC state (schema etl_meta.control_state).

    Keeps the same public surface it had as a JSON store so the pipeline callers
    are unchanged: an in-memory nested dict {table: {branch: {...}}} loaded on
    ``load()``, mutated by ``advance()``, upserted whole on ``save()``.
    """

    def __init__(self, store: "MetaStore"):
        self.store = store
        self.data: dict = {}
        # advance()/save() run on concurrent load workers; the lock keeps dict
        # mutation and save's iteration consistent. load()/as_dict()/entry()
        # are only called when no loads are in flight (run start / between
        # phases), so they stay lock-free.
        self._lock = threading.RLock()

    def load(self) -> "ControlStore":
        self.store.ensure_schema()
        self.data = {}
        for r in self.store.read_control_state():
            tbl = self.data.setdefault(r["table_name"], {})
            tbl[str(r["branch_id"])] = {
                "last_cdc": ({"value": r["last_cdc_value"], "kind": r["last_cdc_kind"]}
                             if r["last_cdc_value"] is not None else None),
                "last_date": ({"value": r["last_date_value"], "kind": r["last_date_kind"]}
                              if r["last_date_value"] is not None else None),
                "status": r["status"],
                "row_count": r["row_count"],
                "duration_ms": r["duration_ms"],
                "last_run_at": r["last_run_at"],
            }
        return self

    def as_dict(self) -> dict:
        return self.data

    def entry(self, table: str, branch: str) -> dict:
        return self.data.get(table, {}).get(branch, {})

    def advance(self, result: ExtractResult) -> None:
        """Move watermarks forward for a successfully loaded (table, branch)."""
        with self._lock:
            tbl = self.data.setdefault(result.table, {})
            cur = tbl.setdefault(result.branch, {})
            cur["last_cdc"] = _wm_advance(cur.get("last_cdc"), result.new_cdc)
            cur["last_date"] = _wm_advance(cur.get("last_date"), result.new_date)
            cur["status"] = result.status
            cur["row_count"] = result.row_count
            cur["duration_ms"] = result.duration_ms
            cur["last_run_at"] = now_local().isoformat()

    def save(self) -> None:
        with self._lock:
            rows = []
            for table, branches in self.data.items():
                for branch, info in branches.items():
                    cdc = info.get("last_cdc") or {}
                    date = info.get("last_date") or {}
                    rows.append({
                        "table_name": table, "branch_id": str(branch),
                        "last_cdc_value": cdc.get("value"), "last_cdc_kind": cdc.get("kind"),
                        "last_date_value": date.get("value"), "last_date_kind": date.get("kind"),
                        "status": info.get("status"), "row_count": info.get("row_count"),
                        "duration_ms": info.get("duration_ms"),
                        "last_run_at": info.get("last_run_at"),
                    })
            self.store.upsert_control_state(rows)


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
    # A commit that hung past the watchdog: the pipeline is poisoned (a daemon
    # worker is still stuck inside it) and must be rebuilt before the next table.
    load_timed_out: bool = False


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
    #
    # A table with no CDC source (no own ``cdc_column`` and no helper) cannot do a
    # real incremental -- ``build_query`` re-extracts it in full every run -- so
    # merging that full extract is a pointless commit storm. Treat it as a full
    # rebuild (single bulk ``replace``) whenever the run covers every branch; a
    # branch subset still has to merge, since ``replace`` overwrites the whole
    # table and would drop the branches it isn't loading.
    no_cdc = not tdef.is_snapshot and tdef.cdc_capture_column is None
    full_rebuild = (
        branches_in_run == total_branches
        and (settings.mode == MODE_INITIAL or no_cdc)
    )
    if tdef.is_snapshot:
        plan.disposition = "append"
    else:
        plan.disposition = "replace" if full_rebuild else "merge"
    return plan


def _open_dest_table(settings: Settings, table_name: str, strict: bool = False):
    """Open ``<dataset>.<table>`` from the configured Iceberg catalog, or None.

    Destination READS go through the persistent catalog (``[iceberg_catalog]``
    config), NOT a dlt pipeline's local schema: a per-table pipeline's schema
    is empty until its first successful run, so a schema-based read
    (``get_iceberg_tables``) would silently degrade every destination-dependent
    helper on that table's first run under a fresh pipeline -- lost insert_at
    carry-forward, a squash that treats prior history as this run's snapshots.
    The catalog knows every table ever loaded regardless of local state. The
    dlt WRITE path is untouched.

    Best-effort by default and never raises then: a missing table (normal
    first load) returns None silently; any other failure logs a warning and
    returns None so the caller degrades exactly as before. ``strict=True`` is
    for correctness decisions (``_table_is_hash_ready``): a missing table
    still returns None, but any other failure PROPAGATES -- degrading there
    means answering a yes/no question with a guess.
    """
    try:
        from dlt.common.libs.pyiceberg import get_catalog
        from pyiceberg.exceptions import NoSuchTableError
    except ImportError as exc:
        if strict:
            raise
        log.warning("pyiceberg catalog access unavailable: %s", exc)
        return None
    try:
        return get_catalog().load_table(f"{settings.dataset_name}.{table_name}")
    except NoSuchTableError:
        return None  # first load: the table does not exist yet
    except Exception as exc:  # noqa: BLE001 - best effort unless strict
        if strict:
            raise
        log.warning("[%s] could not open destination table via catalog: %s",
                    table_name, exc)
        return None


def _coerce_unified_nulls(settings: Settings, tdef: TableDef, schema: pa.Schema) -> pa.Schema:
    """Replace ``null``-typed columns in the unified schema with concrete types.

    pyarrow infers the ``null`` type for a column that is entirely null across
    every branch this run (an unconstrained NUMBER with no/all-null values, or a
    0-row incremental delta where every column is empty). Feeding that ``null``
    column to dlt/pyiceberg fails when the destination already has the column
    typed -- pyiceberg tries to cast the existing column to ``null`` and raises
    ``Unsupported cast from <type> to null using function cast_null``.

    Prefer the destination's existing Arrow type for each null column (read
    best-effort through the persistent catalog -- see ``_open_dest_table``) so a
    merge sees identical types and is a no-op; fall back to string for a column
    the destination doesn't have yet. Returns ``schema`` unchanged when it has
    no null columns.
    """
    null_names = [f.name for f in schema if pa.types.is_null(f.type)]
    if not null_names:
        return schema

    overrides: dict[str, pa.DataType] = {}
    try:
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        tbl = _open_dest_table(settings, tdef.dataset_table_name)
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


def _read_destination_arrow_types(settings: Settings, tdef: TableDef) -> dict:
    """Best-effort ``{column-name-lower: Arrow type}`` for the stored Iceberg table.

    Empty dict when the table does not exist yet or cannot be read. Reads
    through the persistent catalog (``_open_dest_table``).
    """
    try:
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        tbl = _open_dest_table(settings, tdef.dataset_table_name)
        if tbl is None:
            return {}
        return {f.name.lower(): f.type for f in schema_to_pyarrow(tbl.schema())}
    except Exception as exc:  # noqa: BLE001 - best effort; no widening is safe
        log.warning("[%s] could not read destination types for merge schema "
                    "widening: %s", tdef.dataset_table_name, exc)
        return {}


def _widen_schema_to_destination(
    settings: Settings, tdef: TableDef, schema: pa.Schema
) -> pa.Schema:
    """Widen a merge run's unified schema by the destination table's stored types.

    ``unify_schemas`` only sees the branches present in THIS run, so a merge-key
    column that the all-branches INITIAL load widened to ``string`` (a fractional
    branch wins -- ``_coerce_keys_run_stable`` renders a fractional/scale-drifting
    key as a canonical decimal string, an integer-valued one as ``decimal(38, 0)``)
    is re-inferred as ``decimal(38, 0)`` by an incremental run whose branch subset
    happens to be integer-only. The load then dies with
    ``Cannot change column type: rule_ios: string -> decimal(38, 0)`` -- Iceberg
    does not allow ``string -> decimal``.

    The destination table already holds the type the initial load crystallised
    across every branch, so fold it back in: for each column present in both,
    widen the run type by the stored type (``widen_types`` -- string wins, decimals
    widen precision/scale). The batch is then cast to the widened type by
    ``cast_table_to_schema`` exactly as the initial load cast that same branch, so
    the merge-key hash is identical (``_finish_batch`` casts before hashing) and
    Iceberg is asked for no type change.

    Only columns already in ``schema`` are touched -- a column that exists solely
    in the destination is never pulled into the batch (that would null it out for
    updated rows on merge). Best effort: any read failure leaves ``schema`` as-is.
    """
    dest = _read_destination_arrow_types(settings, tdef)
    if not dest:
        return schema
    fields = []
    changed = False
    for f in schema:
        stored = dest.get(f.name.lower())
        if stored is not None and not stored.equals(f.type):
            widened = types_map.widen_types(f.type, stored)
            if not widened.equals(f.type):
                fields.append(pa.field(f.name, widened, nullable=f.nullable,
                                       metadata=f.metadata))
                changed = True
                continue
        fields.append(f)
    if not changed:
        return schema
    out = pa.schema(fields)
    log.info("[%s] widened merge schema to destination types: %s",
             tdef.dataset_table_name,
             {f.name: str(f.type) for f in out
              if not f.type.equals(schema.field(f.name).type)})
    return out


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
    settings: Settings, tdef: TableDef, branches: list[int],
    unified_schema: pa.Schema, hash_ready: bool = False,
) -> Optional[pa.Table]:
    """Read existing rows' insert_at (+ merge key) for ``branches`` for carry-forward.

    ``branches`` holds the numeric BRANCH_ID values for this run (see BranchConfig.id).

    Returns an Arrow table whose columns are named/typed like the incoming batch
    (merge key columns + ``insert_at``), or ``None`` when there's nothing to
    carry forward -- the table doesn't exist yet (first load), it predates the
    insert_at column, or the read fails. Best-effort: a failure here never fails
    the load, it only means updated rows fall back to this run's load time.

    The scan is pruned to the branches in this run (BRANCH_ID is the partition
    column). When ``hash_ready`` (the stored table already carries the merge
    hash -- see ``_table_is_hash_ready``), it projects ``merge_hash`` +
    ``insert_at`` instead of the composite key, so the carry-forward join in
    ``_finish_batch`` can key on the single hash column.
    """
    insert_col = settings.inserted_ts_column
    hash_col = settings.merge_hash_column
    join_keys = list(tdef.key_columns) + [settings.branch_id_column]
    try:
        from pyiceberg.expressions import In
    except ImportError as exc:
        log.warning("[%s] insert_at carry-forward unavailable: %s",
                    tdef.dataset_table_name, exc)
        return None

    tbl = _open_dest_table(settings, tdef.dataset_table_name)
    if tbl is None:
        return None  # first load of this table: nothing to preserve

    # dlt normalizes identifiers to lower snake; for these clean UPPER_SNAKE /
    # already-lower names that is just lower-casing. Physical Iceberg field names
    # are always lowercase, so every settings-derived name is lowered before use
    # (matching _table_is_hash_ready, which lowers hash_col too -- a non-lowercase
    # merge_hash_column would otherwise say ready=True yet miss the column here).
    insert_norm = insert_col.lower()
    hash_norm = hash_col.lower()
    branch_norm = settings.branch_id_column.lower()
    iceberg_cols = {f.name for f in tbl.schema().fields}
    if insert_norm not in iceberg_cols:
        return None  # table predates insert_at -> skip

    if hash_ready:
        if hash_norm not in iceberg_cols:
            return None
        try:
            existing = tbl.scan(
                row_filter=In(branch_norm, set(branches)),
                selected_fields=(hash_norm, insert_norm),
            ).to_arrow()
        except Exception as exc:  # noqa: BLE001 - best effort
            log.warning("[%s] insert_at carry-forward scan failed: %s",
                        tdef.dataset_table_name, exc)
            return None
        if existing.num_rows == 0:
            return None
        return existing.rename_columns(
            [insert_col if n == insert_norm else n for n in existing.column_names])

    # --- composite path (unchanged from today) ---
    key_norms = [k.lower() for k in join_keys]
    if any(k not in iceberg_cols for k in key_norms):
        return None  # table predates key -> skip

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


def _staged_delta_hashes(
    paths: list, key_cols: list[str], unified_schema: pa.Schema,
    table_name: str = "",
) -> Optional[pa.Array]:
    """Distinct merge-hash values present in a merge delta's staged parquets.

    Reads ONLY the key columns (columnar), casts them to the unified schema's
    types -- the same cast ``_finish_batch`` applies before hashing, so the
    digests are byte-identical to the ones the batches will carry -- and
    hashes. Used to pre-shrink the carry-forward ``existing`` table to just
    the rows the delta can actually touch: ``_finish_batch`` joins ``existing``
    once per streamed batch, so an unfiltered multi-million-row table would be
    re-hashed for the join on every 50k-row batch. Best-effort: returns
    ``None`` (no prefilter, today's behavior) on any failure.
    """
    try:
        key_schema = pa.schema([unified_schema.field(c) for c in key_cols])
        chunks = []
        for path in paths:
            t = pq.read_table(path, columns=key_cols)
            t = types_map.cast_table_to_schema(t, key_schema)
            chunks.append(_merge_hash_array(t, key_cols))
        if not chunks:
            return None
        return pc.unique(pa.chunked_array(chunks, type=pa.binary()))
    except Exception as exc:  # noqa: BLE001 - prefilter is an optimization only
        log.warning("[%s] delta hash prefilter unavailable: %s",
                    table_name or "?", exc)
        return None


def _table_is_hash_ready(settings: Settings, tdef: TableDef, hash_col: str) -> bool:
    """True iff the stored Iceberg table already carries ``hash_col`` -- i.e. a
    prior full replace wrote it for every row. Missing table (normal first
    load) -> not ready; the merge falls back to the composite key.

    Unlike the best-effort destination reads, an unreadable catalog RAISES and
    fails this table's load. This is a correctness decision, not an
    optimization: guessing "not ready" for a table that IS hash-ready makes
    the resource skip hashing while the pipeline's synced schema still
    declares the column, so dlt's normalize fabricates it as all-NULL and the
    commit would poison the stored hashes (see the guard in
    ``_merge_iceberg_single_commit``). Per-table isolation contains the
    failure; the table simply retries next run.
    """
    tbl = _open_dest_table(settings, tdef.dataset_table_name, strict=True)
    if tbl is None:
        return False
    return hash_col.lower() in {f.name for f in tbl.schema().fields}


def _finish_batch(
    tbl: pa.Table, schema: pa.Schema, existing_insert_at: Optional[pa.Table],
    insert_col: str, write_hash: bool, hash_key_cols: list[str], hash_col: str,
    carry_keys: list[str],
) -> pa.Table:
    """Reshape one streamed batch: cast to the unified schema, (optionally)
    derive the merge hash, carry forward insert_at for existing rows, and
    (when hashing) leave the batch clustered by the hash.

    The hash is appended BEFORE carry-forward so the carry-forward join can key
    on it (``carry_keys`` may be the hash column). The final sort-by-hash runs
    after the join, since a join does not preserve row order.
    """
    tbl = types_map.cast_table_to_schema(tbl, schema)
    if write_hash:
        tbl = _append_merge_hash(tbl, hash_key_cols, hash_col)
    if existing_insert_at is not None and tbl.num_rows:
        tbl = _carry_forward_insert_at(tbl, existing_insert_at, carry_keys, insert_col)
    if write_hash:
        tbl = _sort_by_hash(tbl, hash_col)
    return tbl


def _iceberg_resource(
    plan: TableLoadPlan,
    settings: Settings,
    paths: list,
    disposition: str,
    existing_insert_at: Optional[pa.Table] = None,
    write_hash: bool = False,
    carry_keys: Optional[list[str]] = None,
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
    hash_col = settings.merge_hash_column
    hash_key_cols = primary_key   # PK + BRANCH_ID, same list, original casing

    def _finish(tbl: pa.Table) -> pa.Table:
        return _finish_batch(
            tbl, schema, existing_insert_at, insert_col,
            write_hash, hash_key_cols, hash_col,
            carry_keys=carry_keys if carry_keys is not None else primary_key)

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
    if write_hash:
        columns[hash_col] = {"data_type": "binary"}

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


def _cleanup_staged(result: ExtractResult, settings: Settings) -> None:
    """Delete a branch's staged parquet once its rows are durably in Iceberg.

    The staged parquet exists only to feed the load; after the branch's
    watermark advances it is dead weight, so we reclaim the disk. Best-effort:
    a failed unlink is logged and never fails the load (mirrors _cleanup_tmp).
    No-op when cleanup is disabled -- e.g. to run ``dq_check --self-test``
    against the staged files afterward.
    """
    if not settings.cleanup_staging_after_load:
        return
    path = result.staged_path
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
        # Drop the now-empty table dir when this was the last branch; an OSError
        # just means other branches' files remain (or it's already gone) -- leave it.
        try:
            path.parent.rmdir()
        except OSError:
            pass
    except OSError as exc:
        log.warning("[%s] could not delete staged parquet %s: %s",
                    result.table, path, exc)


def _group_by_staged_bytes(
    results: list[ExtractResult], max_bytes: int
) -> list[list[ExtractResult]]:
    """Greedy, order-preserving packing of branch results into size-budgeted groups.

    Each group's staged parquet bytes total at most ``max_bytes``, except a
    single branch that alone exceeds the budget, which gets its own group (the
    pre-grouping behavior). One group = one dlt run = one Iceberg commit, so
    small tables collapse to a single commit while the loader's in-memory peak
    (dlt materializes each load package whole) stays bounded by the budget
    times the parquet->Arrow expansion factor. A branch whose staged file
    cannot be stat'ed is treated as budget-sized so it runs alone.
    """
    groups: list[list[ExtractResult]] = []
    cur: list[ExtractResult] = []
    cur_bytes = 0
    for r in results:
        try:
            size = r.staged_path.stat().st_size
        except OSError as exc:
            # unknown size: quarantine in its own group
            size = max_bytes
            log.warning("[%s] could not stat staged parquet %s; treating as "
                        "budget-sized: %s", r.table, r.staged_path, exc)
        if cur and cur_bytes + size > max_bytes:
            groups.append(cur)
            cur, cur_bytes = [], 0
        cur.append(r)
        cur_bytes += size
    if cur:
        groups.append(cur)
    return groups


def _run_per_branch_rebuild(
    pipeline,
    plan: TableLoadPlan,
    settings: Settings,
    control: ControlStore,
) -> None:
    """Load a full-rebuild (``replace``) table in size-budgeted branch groups.

    The dlt filesystem-Iceberg loader materializes a whole load package into one
    Arrow table (``arrow_dataset.to_table()``) before writing -- regardless of
    write disposition -- so each run's package must fit in memory. Branches are
    greedily packed into groups whose staged bytes fit
    ``settings.load_group_max_bytes``: a small table becomes ONE run (one
    Iceberg commit instead of one per branch), while an oversized branch still
    runs alone, keeping the peak bounded exactly as before grouping.

    The first group is written with ``replace`` (which truncates any prior
    table via pyiceberg ``overwrite``); every later group ``append``s.
    Watermarks advance per group as it commits, so a mid-stream failure still
    leaves the already-committed groups' branches (and their watermarks)
    correct; the failed and not-yet-attempted branches keep their old watermark
    and are re-pulled next run. The caller persists ``control`` and marks the
    table FAILED if this raises.
    """
    disposition = "replace"  # first group truncates the prior table
    for group in _group_by_staged_bytes(plan.success, settings.load_group_max_bytes):
        branch_ids = ",".join(str(r.branch_id) for r in group)
        _run_pipeline(
            pipeline,
            [_iceberg_resource(plan, settings, [r.staged_path for r in group],
                               disposition, write_hash=not plan.tdef.is_snapshot)],
            settings,
            f"{plan.tdef.dataset_table_name}:branches={branch_ids}:{disposition}")
        for r in group:
            control.advance(r)
            _cleanup_staged(r, settings)
        disposition = "append"  # everything after the first group adds on


def _run_per_branch_append(
    pipeline,
    plan: TableLoadPlan,
    settings: Settings,
    control: ControlStore,
) -> None:
    """Append a snapshot table in size-budgeted branch groups (memory-bounded).

    Like ``_run_per_branch_rebuild`` this packs branches into
    ``load_group_max_bytes`` groups, but *every* group appends -- including the
    first -- so snapshots stored by earlier runs are preserved (the whole point
    of a snapshot table). Watermarks advance per group as it commits so a
    mid-stream failure leaves the already-appended branches correct.
    """
    for group in _group_by_staged_bytes(plan.success, settings.load_group_max_bytes):
        branch_ids = ",".join(str(r.branch_id) for r in group)
        _run_pipeline(
            pipeline,
            [_iceberg_resource(plan, settings, [r.staged_path for r in group],
                               "append")],
            settings,
            f"{plan.tdef.dataset_table_name}:branches={branch_ids}:append")
        for r in group:
            control.advance(r)
            _cleanup_staged(r, settings)


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


def _write_observability(store: MetaStore, plans, settings, run_id) -> None:
    """Upsert etl_control + append etl_run_log to Postgres for this run."""
    control_rows = _control_rows(plans, settings, run_id)
    log_rows = _log_rows(plans, settings, run_id)
    store.ensure_schema()
    if control_rows:
        store.upsert_etl_control(control_rows)
    if log_rows:
        store.append_run_log(log_rows)


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


def _table_snapshot_ids(settings: Settings, table_name: str) -> Optional[set[int]]:
    """Snapshot ids currently in the table's metadata, read directly from the
    catalog rather than via ``_open_dest_table``.

    ``_open_dest_table`` deliberately collapses "table absent" and "catalog
    failure" into the same ``None`` -- fine for a read that just wants to
    degrade quietly, but this baseline feeds the intra-run squash, which must
    tell the two apart: squashing against a baseline that is empty because
    the catalog was merely unreachable would expire every snapshot from prior
    runs, mistaking them for this run's own.

    Returns:
        ``set()`` if the table genuinely does not exist yet (first load --
        nothing to protect). ``None`` if the catalog could not be read
        (import failure, transient error, or an unreadable snapshot list):
        the caller MUST treat this as "unknown" and skip any squash keyed off
        it, or prior history would look like it belongs to this run and get
        expired. Otherwise, the set of snapshot ids present in the table.
    """
    try:
        from dlt.common.libs.pyiceberg import get_catalog
        from pyiceberg.exceptions import NoSuchTableError
    except ImportError as exc:
        log.warning("pyiceberg catalog access unavailable: %s", exc)
        return None
    try:
        tbl = get_catalog().load_table(f"{settings.dataset_name}.{table_name}")
    except NoSuchTableError:
        return set()  # first load: nothing to protect
    except Exception as exc:  # noqa: BLE001 - baseline must be known-good or skipped
        log.warning("[%s] snapshot baseline unreadable: %s", table_name, exc)
        return None
    try:
        return {s.snapshot_id for s in tbl.metadata.snapshots}
    except Exception as exc:  # noqa: BLE001 - as above
        log.warning("[%s] snapshot baseline unreadable: %s", table_name, exc)
        return None


def _squash_run_snapshots(tbl, before_ids: set[int]) -> int:
    """Expire this run's intermediate snapshots so one snapshot remains per run.

    Per-group loading (``_run_per_branch_rebuild``/``_run_per_branch_append``)
    and dlt's chunked ``merge`` commit one snapshot per branch group/chunk; only
    the last one matters for history. Expires every snapshot not in
    ``before_ids`` except the current snapshot and any ref targets, so prior
    runs' history is untouched and time travel keeps exactly one point per run.
    """
    meta = tbl.metadata
    protected = {ref.snapshot_id for ref in meta.refs.values()}
    if meta.current_snapshot_id is not None:
        protected.add(meta.current_snapshot_id)
    ids = [s.snapshot_id for s in meta.snapshots
           if s.snapshot_id not in before_ids and s.snapshot_id not in protected]
    if ids:
        tbl.maintenance.expire_snapshots().by_ids(ids).commit()
    return len(ids)


def _squash_table_run_snapshots(settings: Settings, table_name: str, before_ids: set[int]) -> None:
    """Best-effort squash of the snapshots this run just committed to a table."""
    try:
        tbl = _open_dest_table(settings, table_name)
        if tbl is None:
            return
        expired = _squash_run_snapshots(tbl, before_ids)
        if expired:
            log.info("[%s] squashed %d intra-run snapshot(s); 1 kept for this run",
                     table_name, expired)
    except Exception as exc:  # noqa: BLE001 - maintenance never fails the load
        log.warning("[%s] snapshot squash failed: %s", table_name, exc)


def apply_snapshot_retention(settings: Settings) -> None:
    """Configure + enforce per-table snapshot retention (keep last N days).

    Sets the Iceberg table properties on first creation (and keeps them set),
    then expires snapshots older than the window so metadata/manifests don't grow
    unbounded. ``min-snapshots-to-keep`` guarantees the current snapshot survives.
    Best-effort: a maintenance failure never fails the load.

    Enumerates tables from the persistent Iceberg catalog (every table ever
    loaded, regardless of any pipeline's local schema); ``_dlt*`` names are
    skipped.
    """
    if not settings.snapshot_maintenance:
        return
    try:
        from dlt.common.libs.pyiceberg import get_catalog
    except ImportError:
        log.warning("pyiceberg table access unavailable; skipping snapshot retention")
        return

    max_age_ms = settings.snapshot_expire_days * 24 * 60 * 60 * 1000
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=settings.snapshot_expire_days)
    props = {
        "history.expire.max-snapshot-age-ms": str(max_age_ms),
        "history.expire.min-snapshots-to-keep": str(settings.snapshot_min_to_keep),
        # Every commit rewrites table metadata and keeps the previous
        # metadata.json around; without a cap the metadata dir grows forever
        # and each commit's metadata-log grows with it. Both keys are
        # honored by pyiceberg (TableProperties, 0.11.1).
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "25",
    }

    try:
        catalog = get_catalog()
        idents = catalog.list_tables(settings.dataset_name)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not list Iceberg tables for retention: %s", exc)
        return

    for ident in idents:
        name = ident[-1] if isinstance(ident, tuple) else str(ident).rsplit(".", 1)[-1]
        if name.startswith("_dlt"):
            continue  # dlt bookkeeping tables are not retention targets
        try:
            tbl = catalog.load_table(ident)
            # Property writes and no-op expiry are real catalog commits (a new
            # metadata.json per table per run) -- pyiceberg's set_properties
            # commits even when the values are unchanged. Guard both so a
            # steady-state run makes ZERO maintenance commits.
            current = tbl.properties
            if any(current.get(k) != v for k, v in props.items()):
                with tbl.transaction() as txn:
                    txn.set_properties(props)
            cutoff_ms = int(cutoff.timestamp() * 1000)
            protected = {ref.snapshot_id for ref in tbl.metadata.refs.values()}
            expirable = [
                s for s in tbl.metadata.snapshots
                if s.timestamp_ms < cutoff_ms and s.snapshot_id not in protected
            ]
            if expirable:
                tbl.maintenance.expire_snapshots().older_than(cutoff).commit()
                log.info("[%s] retention applied: keep %dd, %d snapshot(s) remain",
                         name, settings.snapshot_expire_days,
                         len(list(tbl.snapshots())))
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] snapshot retention failed: %s", name, exc)


def _table_pipeline_name(settings: Settings, tdef: TableDef) -> str:
    """Stable per-table dlt pipeline name: ``<pipeline_name>__<table>``.

    Deterministic (no assignment state) and stable across runs, so a table's
    local schema/state/pending packages persist in its own working dir under
    dlt's pipelines_dir. ``dataset_table_name`` is already normalized
    lowercase, so the result is a valid pipeline/directory name.
    """
    return f"{settings.pipeline_name}__{tdef.dataset_table_name}"


def build_pipeline(settings: Settings, pipelines_dir: Optional[str] = None,
                   pipeline_name: Optional[str] = None):
    # pipelines_dir is dlt's LOCAL bookkeeping (schema/state/load packages), not
    # the destination. None keeps dlt's default (~/.dlt/pipelines); callers
    # (mainly tests) may point it at an isolated dir. pipeline_name overrides
    # the settings-level name for per-table load pipelines (see
    # _table_pipeline_name); None keeps the settings-level shared name (used
    # by tests/tooling). Neither affects where data lands.
    return dlt.pipeline(
        pipeline_name=pipeline_name or settings.pipeline_name,
        destination=dlt.destinations.filesystem(
            bucket_url=settings.destination_bucket_url
        ),
        dataset_name=settings.dataset_name,
        pipelines_dir=pipelines_dir,
    )


def _reject_unstable_key_types(table: pa.Table, key_cols: list[str]) -> None:
    """Refuse float / fractional-decimal merge-key columns.

    Their string cast can drift across runs, so hashing them risks silent
    duplicate rows. Shared by the reference serializer and the fast hasher so
    both reject identically.
    """
    for name in key_cols:
        t = table.column(name).type
        if pa.types.is_floating(t) or (pa.types.is_decimal(t) and t.scale > 0):
            raise ValueError(
                f"merge-key column {name!r} has type {t}, which is not "
                f"run-stable: hashing a floating or fractional-decimal key is "
                f"not run-stable across runs (its string cast can vary -> "
                f"silent duplicate rows). Merge keys must be integer, "
                f"scale-0 decimal, or string."
            )


def _serialize_keys(table: pa.Table, key_cols: list[str]) -> list[bytes]:
    """Canonical, injective, run-stable byte encoding of each row's key.

    Each key column is rendered to its base-10 string form, then framed per row
    as: a 1-byte null flag; for a present value, a 4-byte big-endian length
    prefix then the UTF-8 bytes. Length-prefixing makes the concatenation
    injective across column boundaries; the null flag distinguishes null from an
    empty string.

    Stringifying every column (rather than special-casing integers) keeps the
    hash INVARIANT to a numeric key's Arrow representation: the same integer id
    hashes identically whether a run materializes it as int32, int64, or
    decimal128(p,0) -- Oracle NUMBER ids may be inferred as any of these across
    runs. A floating or fractional-decimal (scale > 0) key column is NOT
    run-stable this way (its string cast can drift across runs -> silent
    duplicate rows), so such columns are rejected up front rather than hashed.
    """
    _reject_unstable_key_types(table, key_cols)

    col_strs = [pc.cast(table.column(name), pa.string()).to_pylist()
                for name in key_cols]
    out: list[bytes] = []
    for i in range(table.num_rows):
        parts = bytearray()
        for strs in col_strs:
            v = strs[i]
            if v is None:
                parts += b"\x01"
            else:
                b = v.encode("utf-8")
                parts += b"\x00" + struct.pack(">I", len(b)) + b
        out.append(bytes(parts))
    return out


# Cached ``b"\x00" + 4-byte big-endian length`` frames for short values --
# stringified numeric keys are almost always < 128 bytes, so the per-value
# prefix is a list lookup instead of a struct.pack call.
_LEN_PREFIX = [b"\x00" + struct.pack(">I", n) for n in range(128)]


def _key_column_view(col) -> "tuple[memoryview, memoryview, Optional[list]]":
    """Zero-copy per-row view of one stringified key column.

    Returns ``(payload, offsets, valid)``: ``payload[offsets[i]:offsets[i+1]]``
    is row i's UTF-8 bytes straight from the Arrow data buffer (no Python
    string is ever materialized), ``offsets`` is the int64 offsets buffer, and
    ``valid`` is a per-row validity list or ``None`` when no row is null.
    Raises when the cast/combine yields a non-zero array offset -- the caller
    falls back to the reference serializer.
    """
    arr = pc.cast(col, pa.large_string())
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if isinstance(arr, pa.ChunkedArray):  # defensive: combine_chunks contract varies
        arr = arr.chunk(0) if arr.num_chunks else pa.array([], pa.large_string())
    if arr.offset != 0:
        raise ValueError("expected offset-0 array after cast/combine")
    if len(arr) == 0:
        return memoryview(b""), memoryview(struct.pack("q", 0)).cast("q"), None
    offsets = memoryview(arr.buffers()[1]).cast("q")  # int64 offsets, zero-copy
    payload_buf = arr.buffers()[2]
    payload = memoryview(payload_buf) if payload_buf is not None else memoryview(b"")
    valid = None if arr.null_count == 0 else arr.is_valid().to_pylist()
    return payload, offsets, valid


def _merge_hash_array(table: pa.Table, key_cols: list[str]) -> pa.Array:
    """128-bit blake2b of each row's canonical key serialization -> pa.binary().

    Deterministic across processes and library versions (unlike the salted
    built-in hash()). Every value is exactly 16 bytes. Fast path: instead of
    materializing every key value as a Python string (``_serialize_keys``),
    each column is cast to ``large_string`` once and rows are fed to blake2b
    as slices of the Arrow data buffer -- byte-identical framing (null flag /
    ``\\x00`` + 4-byte BE length + UTF-8), no per-row Python objects. Any
    input the fast path cannot view falls back to the reference serializer,
    which stays the canonical definition of the hash bytes.
    """
    _reject_unstable_key_types(table, key_cols)
    try:
        views = [_key_column_view(table.column(name)) for name in key_cols]
    except Exception as exc:  # noqa: BLE001 - reference path is always correct
        log.warning("fast merge-hash framing unavailable (%s); "
                    "using reference serializer", exc)
        digests = [hashlib.blake2b(b, digest_size=16).digest()
                   for b in _serialize_keys(table, key_cols)]
        return pa.array(digests, type=pa.binary())

    out = []
    for i in range(table.num_rows):
        h = hashlib.blake2b(digest_size=16)
        for payload, offsets, valid in views:
            if valid is not None and not valid[i]:
                h.update(b"\x01")
                continue
            a, b = offsets[i], offsets[i + 1]
            ln = b - a
            h.update(_LEN_PREFIX[ln] if ln < 128 else
                     b"\x00" + struct.pack(">I", ln))
            h.update(payload[a:b])
        out.append(h.digest())
    return pa.array(out, type=pa.binary())


def _append_merge_hash(tbl: pa.Table, key_cols: list[str], hash_col: str) -> pa.Table:
    """Append the derived merge-hash column (no sort). Row-aligned to ``tbl``."""
    return tbl.append_column(hash_col, _merge_hash_array(tbl, key_cols))


def _sort_by_hash(tbl: pa.Table, hash_col: str) -> pa.Table:
    """Cluster a batch by the merge hash so per-file min/max can prune the In
    scan (and less copy-on-write rewrite). No-op if the column is absent or the
    table is empty; row content/count is unchanged either way."""
    if hash_col not in tbl.column_names or tbl.num_rows == 0:
        return tbl
    return tbl.sort_by([(hash_col, "ascending")])


def _delta_partition_filter(table, data: pa.Table):
    """``In(<partition col>, <values in the delta>)`` for an identity-partitioned
    table, or None when nothing can be pruned.

    Restricted to identity transforms on a column the delta carries: the mapping
    from a row's value to its file's partition is then the identity, so a file
    whose partition value is absent from the delta cannot hold a delta key. Any
    other transform (bucket, truncate) would need the source values hashed the
    same way, and a partition column missing from the delta tells us nothing --
    both fall back to the full plan. Returns None for an unpartitioned table too,
    so callers keep today's behaviour.
    """
    from pyiceberg.expressions import And, In
    from pyiceberg.transforms import IdentityTransform

    spec = table.spec()
    if not spec.fields:
        return None

    schema = table.schema()
    predicates = []
    for field in spec.fields:
        if not isinstance(field.transform, IdentityTransform):
            return None  # non-identity: value -> partition is not 1:1
        name = schema.find_field(field.source_id).name
        if name not in data.column_names:
            return None  # delta cannot constrain this dimension
        values = {v for v in data.column(name).to_pylist() if v is not None}
        if not values:
            return None
        predicates.append(In(name, values))

    expr = predicates[0]
    for extra in predicates[1:]:
        expr = And(expr, extra)
    return expr


def _rows_to_update(source: pa.Table, target: pa.Table, join_col: str) -> pa.Table:
    """Vectorized drop-in for ``upsert_util.get_rows_to_update`` (single key).

    pyiceberg compares matched rows in a pure-Python loop: per matched row it
    slices BOTH tables to one row and calls ``.as_py()`` on every non-key column
    until it finds a difference. Measured on claim_service_detail (74 columns)
    that runs at ~2,700 rows/s, so bintran's 4.97M-row delta would spend ~30
    minutes there before any I/O -- which is why the big-delta tables never
    reached the ``merge lookup`` log line and were killed by the commit
    watchdog, while every table that DID complete had <170k matched rows.

    Same result, computed with Arrow kernels: align the matched pairs with one
    inner join, then OR a per-column "differs" mask across them. Semantics are
    pyiceberg's exactly -- two NULLs count as equal, one NULL as a difference,
    rows come from the ORIGINAL (uncast) source, and a duplicated target key
    raises the same error. Nested columns, which Arrow cannot compare with
    ``not_equal``, keep the elementwise Python path.
    """
    from pyiceberg.table import upsert_util

    if upsert_util.has_duplicate_rows(target, [join_col]):
        raise ValueError("Target table has duplicate rows, aborting upsert")

    empty = source.schema.empty_table()
    if target.num_rows == 0 or source.num_rows == 0:
        return empty

    non_key = [c for c in source.column_names if c != join_col]
    if not non_key:
        return empty  # nothing outside the key can differ

    # pyiceberg casts the source to the target's schema before joining (Arrow
    # cannot join across differing types); the ROWS it returns still come from
    # the original source, so cast a copy only for the comparison.
    cast_source = source if source.schema.equals(target.schema) else source.cast(target.schema)

    src_pos = pa.array(range(cast_source.num_rows), pa.int64())
    tgt_pos = pa.array(range(target.num_rows), pa.int64())
    matches = (
        pa.table({join_col: cast_source.column(join_col), "__src": src_pos})
        .join(pa.table({join_col: target.column(join_col), "__tgt": tgt_pos}),
              keys=[join_col], join_type="inner")
    )
    if matches.num_rows == 0:
        return empty

    src_take = matches.column("__src").combine_chunks()
    tgt_take = matches.column("__tgt").combine_chunks()
    matched_source = cast_source.take(src_take)
    matched_target = target.take(tgt_take)

    differs = None
    for name in non_key:
        a, b = matched_source.column(name), matched_target.column(name)
        if pa.types.is_null(a.type) and pa.types.is_null(b.type):
            continue  # every value on both sides is NULL -> equal
        if pa.types.is_nested(a.type) or pa.types.is_nested(b.type):
            # not_equal has no kernel for list/struct/map (the same Arrow gap
            # that made pyiceberg go row-by-row for EVERY column).
            col = pa.array([x != y for x, y in zip(a.to_pylist(), b.to_pylist())],
                           pa.bool_())
        else:
            # not_equal yields NULL if either side is NULL: fill those with True
            # (one-sided NULL is a change), then force both-NULL back to False.
            col = pc.if_else(
                pc.and_(pc.is_null(a), pc.is_null(b)),
                pa.scalar(False),
                pc.fill_null(pc.not_equal(a, b), True),
            )
        differs = col if differs is None else pc.or_(differs, col)

    if differs is None:
        return empty
    changed = pc.filter(src_take, pc.fill_null(differs, True))
    return source.take(changed) if len(changed) else empty


def _upsert_in_memory_lookup(
    table, data: pa.Table, join_col: str, update_matched: bool, label: str = "",
) -> None:
    """Single-key upsert whose matched-rows lookup runs in memory, not in a scan.

    pyiceberg's ``table.upsert`` finds matched rows with
    ``scan(row_filter=In(key, <every delta key>))``, and the scan re-translates,
    re-binds and re-compiles that predicate for EVERY data file
    (``_task_to_record_batches``) -- O(files x delta_keys) pure-Python literal
    churn that dwarfs the actual I/O on a big delta (165k keys x 34 files took
    367s on delivery_charge, blowing the commit watchdog; the aborted work then
    grew the next delta, so the table could never catch up). Reading the key
    column per file with NO row filter skips that machinery entirely, and
    ``pc.is_in`` gives the same matched/new split from the two key sets.

    The write side does NOT use ``tx.overwrite(rows, In(changed_keys))``. That
    path re-plans the table and fully re-reads every planned file, duplicating
    the read this function already did, and its pruning collapses on a big
    delta: an ``In`` above ``IN_PREDICATE_LIMIT`` (200) makes the metrics
    evaluator answer ROWS_MIGHT_MATCH for every file -- short-circuiting there
    SKIPS pruning rather than making it cheap. doc's 1.02M changed rows thus
    re-read all 7.6GB and blew the watchdog with nothing committed. Instead each
    matched file is rewritten in place from the copy already in hand and the
    commit swaps exactly those files (see the rewrite loop below).

    The file PLAN is pruned to the branch partitions the delta actually carries
    (``_delta_partition_filter``). These tables are partitioned by
    ``branch_id`` (identity) and a staged delta covers one branch -- or the few
    grouped into one dlt run -- so an unpruned plan reads every other branch's
    files for nothing: appointments held 763 files / 19GB across 8 branches, and
    a 16MB single-branch delta was scanning all of it. Pruning is exact for an
    identity partition (a file's rows are all one branch), so the matched/insert
    split is unchanged -- only the files never holding a candidate key are
    skipped.

    Everything else mirrors ``table.upsert`` exactly: duplicate-delta refusal
    (same message -- the run log patterns on it), duplicate-stored abort (via
    ``get_rows_to_update``), unchanged-row elision, only-matched files read in
    full, and one transaction so the merge stays a single physical commit.
    """
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow
    from pyiceberg.schema import Schema
    from pyiceberg.table import upsert_util

    if upsert_util.has_duplicate_rows(data, [join_col]):
        raise ValueError(
            "Duplicate rows found in source dataset based on the key columns. "
            "No upsert executed"
        )

    key_schema = Schema(table.schema().find_field(join_col))

    def _read(task, projection: Schema) -> pa.Table:
        return ArrowScan(
            table_metadata=table.metadata, io=table.io,
            projected_schema=projection, row_filter=AlwaysTrue(),
        ).to_table(tasks=[task])

    def _cast_like(values, target_type: pa.DataType):
        # Stored keys read back Arrow-large (binary -> large_binary etc.) while
        # the delta carries the plain type; is_in needs them aligned.
        return values if values.type.equals(target_type) else values.cast(target_type)

    stored_key_type = schema_to_pyarrow(key_schema).field(join_col).type
    delta_keys = _cast_like(data.column(join_col), stored_key_type)

    part_filter = _delta_partition_filter(table, data)
    tasks = list(
        table.scan().plan_files() if part_filter is None
        else table.scan(row_filter=part_filter).plan_files()
    )
    if part_filter is not None:
        log.info("[%s] merge plan: %d data file(s) after partition pruning (%s)",
                 label or "?", len(tasks), part_filter)

    matched_tasks = []
    matched_key_parts: list[pa.ChunkedArray] = []
    for task in tasks:
        stored = _read(task, key_schema).column(join_col)
        mask = pc.is_in(stored, value_set=delta_keys)
        if pc.any(mask).as_py():
            matched_tasks.append(task)
            matched_key_parts.append(stored.filter(mask))

    if matched_key_parts:
        matched_keys = pa.chunked_array(
            [chunk for part in matched_key_parts for chunk in part.chunks])
        unmatched = pc.invert(pc.is_in(
            _cast_like(data.column(join_col), matched_keys.type),
            value_set=matched_keys))
        rows_to_insert = data.filter(unmatched)
    else:
        rows_to_insert = data

    rows_to_update = None
    replaced: list[tuple[object, list]] = []
    commit_uuid = None
    if update_matched and matched_tasks:
        # Concat target schema: the table's own Arrow schema, every field made
        # nullable. ArrowScan reports each file's OWN Arrow types, not the
        # projected schema's, so files written by different dlt/pyarrow versions
        # read back with different widths for the same Iceberg column -- docl
        # holds 84 files with `cost_updated: string` and 326 with
        # `large_string`. concat_tables demands byte-identical schemas and
        # raised "Schema at index 21 was different", which dlt classified as
        # transient and retried until the commit watchdog abandoned the table.
        # Casting each part to the table's canonical schema unifies both the
        # width and any per-file nullability. Only get_rows_to_update consumes
        # this table and it returns rows taken from `data`, so the relaxation
        # cannot reach the committed rows.
        target_schema = pa.schema(
            [f.with_nullable(True) for f in schema_to_pyarrow(table.schema())]
        )
        # SURGICAL REWRITE. ``tx.overwrite(rows, In(changed_keys))`` cannot be
        # used for a large scattered delta: its delete step re-plans the table
        # and FULLY RE-READS every planned file (pyiceberg table/__init__.py --
        # ArrowScan per file, then df.filter(preserve_row_filter)), on top of
        # the read this loop already did. doc's 1.02M changed rows sit in 124 of
        # 359 files, so the range bound spans nearly the whole key space, prunes
        # almost nothing, and the commit re-read 7.6GB -- 54 minutes, past the
        # watchdog, with nothing committed.
        #
        # Instead each matched file is read ONCE here and its replacement is
        # written immediately: survivors (rows this delta does not change) plus
        # the updated rows. The commit then swaps exactly those files. Files
        # holding matched keys whose values are all IDENTICAL are left alone --
        # a rewrite that changes nothing is pure cost.
        #
        # Row conservation is exact per file: `full` splits into survivors and
        # the rows whose key is in `changed`, and `upd` carries exactly one row
        # per changed key (the delta is duplicate-free, checked above), so
        # len(survivors) + len(upd) == len(full).
        import itertools
        import uuid as _uuid

        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        def _align(tbl: pa.Table) -> pa.Table:
            return tbl if tbl.schema.equals(target_schema) else tbl.cast(target_schema)

        commit_uuid = _uuid.uuid4()
        counter = itertools.count(0)
        upd_parts: list[pa.Table] = []
        seen_keys: set = set()

        for task in matched_tasks:  # full read of ONLY the files with matches
            full = _read(task, table.schema())
            keys = _cast_like(full.column(join_col), delta_keys.type)
            in_delta = pc.is_in(keys, value_set=delta_keys)
            matched_rows = _align(full.filter(in_delta))
            if matched_rows.num_rows == 0:
                continue

            # A key stored in two different files would make the per-file
            # rewrites disagree (each would keep its own updated copy), where
            # pyiceberg's predicate delete removed both. The key is unique by
            # construction, so treat a collision as corruption and abort with
            # pyiceberg's own message rather than write divergent rows.
            file_keys = set(matched_rows.column(join_col).to_pylist())
            if seen_keys & file_keys:
                raise ValueError("Target table has duplicate rows, aborting upsert")
            seen_keys |= file_keys

            # Compare only the delta rows belonging to THIS file.
            src_keys = _cast_like(data.column(join_col),
                                  matched_rows.column(join_col).type)
            src_subset = data.filter(
                pc.is_in(src_keys, value_set=matched_rows.column(join_col)))
            upd = _rows_to_update(src_subset, matched_rows, join_col)
            if upd.num_rows == 0:
                continue  # every matched row is unchanged: do not rewrite the file

            upd = _align(upd)
            upd_parts.append(upd)
            changed = _cast_like(upd.column(join_col), keys.type)
            survivors = _align(full.filter(pc.invert(pc.is_in(keys, value_set=changed))))
            new_content = pa.concat_tables([survivors, upd])
            if new_content.num_rows != full.num_rows:  # invariant, not a guess
                raise RuntimeError(
                    f"merge rewrite would change row count for {label or '?'}: "
                    f"{full.num_rows} -> {new_content.num_rows}")
            replaced.append((task.file, list(_dataframe_to_data_files(
                table_metadata=table.metadata, df=new_content, io=table.io,
                write_uuid=commit_uuid, counter=counter))))

        rows_to_update = pa.concat_tables(upd_parts) if upd_parts else None

    n_upd = rows_to_update.num_rows if rows_to_update is not None else 0
    rewritten = len(replaced)
    log.info("[%s] merge lookup: %d/%d data files hold matched keys; "
             "%d row(s) to update in %d file(s), %d to insert",
             label or "?", len(matched_tasks), len(tasks), n_upd, rewritten,
             rows_to_insert.num_rows)

    if n_upd == 0 and rows_to_insert.num_rows == 0:
        return  # nothing changed -- commit nothing, like table.upsert's elision

    with table.transaction() as tx:
        if rewritten:
            # Swap only the rewritten files. _OverwriteFiles keeps every
            # manifest whose entries are untouched and rewrites just the ones
            # referencing a deleted file, so the rest of the table is carried
            # forward by reference -- no re-read, no re-write.
            with tx.update_snapshot().overwrite(commit_uuid=commit_uuid) as ow:
                for old_file, new_files in replaced:
                    ow.delete_data_file(old_file)
                    for new_file in new_files:
                        ow.append_data_file(new_file)
        if rows_to_insert.num_rows:
            tx.append(rows_to_insert)


def _merge_join_cols(table, data, composite: list[str], hash_col: str) -> list[str]:
    """Join on the single hash column when BOTH the stored table and the delta
    carry it (hash-ready) -- pyiceberg then takes the fast In path. Otherwise the
    composite key, unchanged. 128-bit width makes equal-hash <=> equal-key, so
    match/insert and duplicate detection are semantically identical either way."""
    stored = {f.name for f in table.schema().fields}
    if hash_col in stored and hash_col in data.column_names:
        return [hash_col]
    return composite


def _merge_iceberg_single_commit(table, data, schema, load_table_name: str) -> None:
    """Upsert an Iceberg merge delta in ONE commit (drop-in for dlt's batched merge).

    Mirrors dlt's ``merge_iceberg_table`` exactly -- same schema union, same
    primary-key / parent-unique join-key detection, same upsert flags -- but
    calls ``table.upsert`` once over the whole (already dlt-normalized) delta
    instead of once per 1,000-row chunk. That turns a large-delta merge from
    thousands of Iceberg commits into a single snapshot, changing only the commit
    granularity: naming, typing and merge semantics are untouched.
    """
    from dlt.common.libs.pyiceberg import (
        ensure_iceberg_compatible_arrow_data,
        ensure_iceberg_compatible_arrow_schema,
    )
    from dlt.common.schema.utils import (
        get_columns_names_with_prop,
        get_first_column_name_with_prop,
    )

    strategy = schema["x-merge-strategy"]
    if strategy not in ("upsert", "insert-only"):
        raise ValueError(
            f'Merge strategy "{strategy}" is not supported for Iceberg tables. '
            f'Table: "{load_table_name}".'
        )

    # A merge_hash column that is present but not fully populated was NOT
    # computed from the keys: dlt's arrow normalization inserts a missing
    # schema column as all-NULL (``normalize_py_arrow_item`` rule 3), which is
    # how a delta built with ``write_hash=False`` against a schema that
    # declares the column arrives here. Nulls never match -- Arrow joins drop
    # null keys and ``NULL IN (...)`` is never true -- so joining on it
    # mis-merges, and even committing it around a composite join would
    # overwrite stored hashes with NULL (or evolve an all-NULL column into a
    # not-ready table), breaking hash-readiness' presence => populated
    # invariant for every later run. The keys are NOT NULL, so a computed hash
    # can never be null: any null means fabrication. Refuse before touching
    # the table; the per-table handler marks the table FAILED and it retries
    # next run.
    from etl.config import Settings
    hash_col = Settings().merge_hash_column
    if hash_col in data.column_names:
        null_hashes = data.column(hash_col).null_count
        if null_hashes:
            raise ValueError(
                f'Merge delta for "{load_table_name}" carries {null_hashes} '
                f'null {hash_col} value(s) -- fabricated by schema '
                f'normalization, not computed from the keys. Refusing to '
                f'merge it.'
            )

    # Evolve the table schema so any new column in the delta is accepted.
    with table.update_schema() as update:
        update.union_by_name(ensure_iceberg_compatible_arrow_schema(data.schema))

    # The upsert's matched-rows scan projects the branch-head SNAPSHOT's
    # schema, and the union above evolves only the table metadata -- no
    # snapshot -- so a column it just added stays invisible to the scan and
    # pyiceberg's positional cast of the delta against the scanned rows
    # fails. Nothing heals this later: the next run's union is a no-op, so
    # the head keeps referencing the old schema and the table's merge stays
    # wedged. Advance the head to the current schema with an empty append
    # (one extra snapshot, only on runs that actually add a column).
    snap = table.current_snapshot()
    if (
        snap is not None
        and snap.schema_id is not None
        and snap.schema_id != table.schema().schema_id
    ):
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        table.append(schema_to_pyarrow(table.schema()).empty_table())

    if "parent" in schema:
        join_cols = [get_first_column_name_with_prop(schema, "unique")]
    else:
        join_cols = get_columns_names_with_prop(schema, "primary_key")

    normalized = ensure_iceberg_compatible_arrow_data(data)

    # That same positional cast also requires the delta's column ORDER to
    # match the scanned rows', not just the name set. The table's order
    # diverges from the source's as soon as any column is added after
    # creation -- union_by_name appends it at the tail while the source
    # keeps emitting it in its upstream position -- so project the delta
    # onto the table's column order (a zero-copy select).
    in_delta = set(normalized.column_names)
    normalized = normalized.select(
        [f.name for f in table.schema().fields if f.name in in_delta]
    )

    join_cols = _merge_join_cols(table, normalized, join_cols, hash_col)

    # A single-column key (merge_hash, or a parent table's unique column) takes
    # the in-memory lookup: ``table.upsert``'s matched-rows scan re-binds its
    # In(<every delta key>) predicate once per data file, which is what kept
    # delivery_charge's 165k-row delta from ever fitting the commit watchdog.
    # Composite keys keep pyiceberg's upsert unchanged (their Or(And(EqualTo))
    # filter has different, pre-existing costs -- and they disappear as tables
    # become hash-ready).
    if len(join_cols) == 1:
        _upsert_in_memory_lookup(
            table, normalized, join_cols[0],
            update_matched=(strategy == "upsert"), label=load_table_name)
        return

    table.upsert(
        df=normalized,
        join_cols=join_cols,
        when_matched_update_all=(strategy == "upsert"),
        when_not_matched_insert_all=True,
        case_sensitive=True,
    )


_merge_iceberg_single_commit._single_commit = True  # tag for idempotent install


def _install_single_commit_merge() -> None:
    """Replace dlt's per-1,000-row Iceberg merge with a single-commit upsert.

    dlt hardcodes ``max_chunksize=1_000`` in ``merge_iceberg_table`` -- one
    Iceberg commit per chunk, a metadata-rewrite storm on any sizeable delta.
    ``IcebergLoadFilesystemJob.run`` re-imports that symbol from the module on
    every load, so swapping the module attribute makes each merge commit exactly
    one snapshot. Idempotent; safe (and cheap) to call at the start of every run.
    """
    import dlt.common.libs.pyiceberg as _dlt_ice

    if getattr(_dlt_ice.merge_iceberg_table, "_single_commit", False):
        return
    _dlt_ice.merge_iceberg_table = _merge_iceberg_single_commit
    log.info("installed single-commit Iceberg merge (1 snapshot per table per merge)")


def clear_pending_packages(pipeline, context: str) -> None:
    """Drop any pending (extracted/normalized) load packages left on the pipeline.

    Every ``pipeline.run`` first retries pending packages before touching new
    data. Each table now has its own pipeline, so a poisoned package (e.g. a
    bad unique_key failing at normalize) can only ever block that table's own
    next run -- it is swept here just before the table's load. Watermarks only
    advance on successful commits, so a dropped table simply re-extracts next
    run. Best-effort: cleanup must never mask the failure that triggered it.
    """
    try:
        if pipeline.has_pending_data:
            log.warning("[%s] dropping pending load packages so they cannot "
                        "block subsequent runs", context)
            pipeline.drop_pending_packages(with_partial_loads=True)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] could not drop pending packages: %s", context, exc)


# Watchdog-abandoned commit threads. A thread stuck in a native pyiceberg call
# cannot be killed, and abandonment is not free: executors created inside the
# abandoned ``pipeline.run`` (dlt's load pool, pyiceberg's ExecutorFactory)
# register worker threads that concurrent.futures' exit hook joins at
# interpreter shutdown -- hung ones forever, daemon flag notwithstanding. The
# entry point checks this registry and hard-exits (``os._exit``) while any are
# still alive; see ``hung_commit_threads``.
_abandoned_commits: list[threading.Thread] = []


def hung_commit_threads() -> list[str]:
    """Names of watchdog-abandoned commit threads that are still alive."""
    return [t.name for t in _abandoned_commits if t.is_alive()]


def _log_hung_stack(worker: threading.Thread, label: str, timeout_s: float) -> None:
    """Log where the abandoned commit thread is stuck.

    This is the hung-vs-slow evidence for chronically timed-out tables: a stack
    parked in socket/catalog I/O means the fix is a connect/request timeout, one
    deep in upsert work means the commit is merely slow and needs a bigger
    budget (or a faster merge), not abandonment.
    """
    frame = sys._current_frames().get(worker.ident)
    if frame is None:  # finished inside the race window; nothing to show
        return
    log.warning("[%s] commit watchdog fired after %ss; abandoned thread's "
                "stack:\n%s", label, timeout_s,
                "".join(traceback.format_stack(frame)))


def _run_with_timeout(fn: Callable[[], object], timeout_s: Optional[float], label: str):
    """Run ``fn`` under a wall-clock watchdog, turning a hang into ``TimeoutError``.

    A pyiceberg commit can block forever with no error and never return -- dlt's
    ``.reference`` followup job spins inside ``pipeline.run`` -- so the per-table
    ``except`` recovery can never fire and the whole run deadlocks. Running the
    commit on a daemon worker and waiting ``timeout_s`` lets a genuine hang
    surface as a ``TimeoutError`` (the caller's ``except`` then marks the table
    FAILED and the run continues), while a normal -- even minutes-long -- commit
    still returns its result untouched.

    ``timeout_s`` of 0/None disables the watchdog and runs ``fn`` inline. On
    timeout the worker thread is abandoned: a Python thread blocked in a native
    pyiceberg call cannot be force-killed, so the caller MUST NOT reuse the
    now-poisoned pipeline. Abandonment does NOT end at the daemon flag -- the
    executors spawned inside the abandoned ``pipeline.run`` would still hang
    interpreter shutdown (see ``_abandoned_commits``), so the timed-out worker
    is recorded for the entry point's exit decision and its stack is logged
    while it is still stuck.
    """
    if not timeout_s or timeout_s <= 0:
        return fn()
    box: dict = {}

    def _target() -> None:
        try:
            box["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller thread
            box["error"] = exc

    worker = threading.Thread(target=_target, name=f"commit:{label}", daemon=True)
    worker.start()
    worker.join(timeout_s)
    if worker.is_alive():
        _abandoned_commits.append(worker)
        _log_hung_stack(worker, label, timeout_s)
        raise TimeoutError(
            f"Iceberg commit for '{label}' exceeded {timeout_s}s (likely a hung "
            f"pyiceberg .reference job); abandoning it and failing the table"
        )
    if "error" in box:
        raise box["error"]
    return box.get("value")


def _run_pipeline(pipeline, resources, settings: Settings, label: str):
    """``pipeline.run(resources)`` under the per-commit hang watchdog.

    Every Iceberg commit goes through here so a single hung ``.reference`` job
    can never deadlock the whole run -- it surfaces as a ``TimeoutError`` that the
    caller's ``except`` turns into a FAILED table (see ``_run_with_timeout``).
    """
    return _run_with_timeout(
        lambda: pipeline.run(resources), settings.load_commit_timeout_s, label)


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
            _cleanup_staged(r, settings)
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
    plan.unified_schema = _coerce_unified_nulls(settings, tdef, plan.unified_schema)

    # A merge only unifies THIS run's branches, so a key column the all-branches
    # INITIAL load widened to string (e.g. RULE_IOS -- fractional on some branch)
    # is re-inferred as decimal by an integer-only incremental subset and dies at
    # load with "Cannot change column type: <col>: string -> decimal". Fold the
    # destination's stored types back in so the merge conforms to what the initial
    # load wrote -- no reload, no disallowed Iceberg type change. (replace/append
    # rebuild the table this run, so they must NOT adopt the old type.)
    if plan.disposition == "merge":
        plan.unified_schema = _widen_schema_to_destination(
            settings, tdef, plan.unified_schema)

    # Snapshot ids present before this run's commits: per-branch loading (and
    # chunked merge) commits several snapshots; after a successful load every
    # snapshot newer than these is squashed down to the final one so history
    # keeps exactly 1 snapshot per run instead of 1 per branch per run.
    before_ids = (_table_snapshot_ids(settings, tdef.dataset_table_name)
                  if settings.snapshot_maintenance else set())

    # Label the peak-memory window: this is where the staged parquet is read
    # back into Arrow and committed to Iceberg.
    monitor.begin_load(tdef.dataset_table_name)
    try:
        if plan.disposition == "replace":
            # Full rebuild: one size-budgeted branch group per dlt run (first group
            # replace, rest append) so the loader never materializes more than one
            # group at a time. Watermarks advance inside the loop as each group
            # commits.
            _run_per_branch_rebuild(pipeline, plan, settings, control)
        elif plan.disposition == "append":
            # Snapshot append: one branch group per dlt run (all append) so history
            # from earlier runs is preserved and memory stays bounded.
            _run_per_branch_append(pipeline, plan, settings, control)
        else:
            # Incremental / branch-subset merge: small deltas, one run is fine.
            # Preserve each row's original insert_at across updates by carrying
            # forward the existing value for rows already in the table.
            hash_ready = _table_is_hash_ready(settings, tdef, settings.merge_hash_column)
            existing = _existing_insert_at(
                settings, tdef,
                [r.branch_id for r in plan.success], plan.unified_schema,
                hash_ready=hash_ready)
            if hash_ready and existing is not None:
                # Shrink carry-forward to the rows the delta can touch: the
                # batch loop joins `existing` once per streamed batch, so an
                # unfiltered multi-million-row table would be re-hashed for
                # the join on every 50k-row batch.
                delta_hashes = _staged_delta_hashes(
                    [r.staged_path for r in plan.success],
                    list(tdef.key_columns) + [settings.branch_id_column],
                    plan.unified_schema, tdef.dataset_table_name)
                if delta_hashes is not None:
                    existing = existing.filter(pc.is_in(
                        existing.column(settings.merge_hash_column),
                        value_set=delta_hashes))
                    if existing.num_rows == 0:
                        existing = None   # nothing to carry: skip joins entirely
            carry_keys = ([settings.merge_hash_column] if hash_ready
                          else list(tdef.key_columns) + [settings.branch_id_column])
            _run_pipeline(
                pipeline,
                [_iceberg_resource(
                    plan, settings, [r.staged_path for r in plan.success],
                    plan.disposition, existing_insert_at=existing,
                    write_hash=hash_ready, carry_keys=carry_keys)],
                settings, f"{tdef.dataset_table_name}:{plan.disposition}")
            # Advance watermarks only for a table that actually loaded.
            for r in plan.success:
                control.advance(r)
                _cleanup_staged(r, settings)
        control.save()
        if settings.snapshot_maintenance:
            if before_ids is None:
                log.warning("[%s] snapshot baseline was unreadable before the load; "
                            "skipping intra-run squash to protect prior history",
                            tdef.dataset_table_name)
            else:
                _squash_table_run_snapshots(settings, tdef.dataset_table_name, before_ids)
        plan.load_status = "SUCCESS"
        log.info("[%s] loaded: disp=%s ok=%d fail=%d rows=%d",
                 tdef.dataset_table_name, plan.disposition, len(plan.success),
                 len(plan.failed), sum(r.row_count for r in plan.success))
    except TimeoutError as exc:
        # The commit hung past the watchdog: a daemon worker is still stuck
        # inside this pipeline and cannot be killed, so we must NOT touch it
        # (clear_pending_packages / reuse would race the zombie). Flag the plan
        # so the orchestrator simply abandons this pipeline -- nothing else
        # uses it this run, and there is no rebuild. Watermarks for any branch
        # that committed before the hang were already advanced.
        plan.load_status = "FAILED"
        plan.load_timed_out = True
        plan.load_error = f"{type(exc).__name__}: {exc}"
        log.error("[%s] load commit timed out; abandoning pipeline: %s",
                  tdef.dataset_table_name, exc)
        control.save()
    except Exception as exc:  # noqa: BLE001 - isolate per-table load failures
        plan.load_status = "FAILED"
        plan.load_error = f"{type(exc).__name__}: {exc}"
        log.error("[%s] load failed: %s", tdef.dataset_table_name, exc)
        # Drop this table's stuck package: pipeline.run retries pending
        # packages before new data, so leaving it would fail this table's own
        # next run (observability no longer runs through this pipeline).
        clear_pending_packages(pipeline, tdef.dataset_table_name)
        # Persist any per-branch watermarks that committed before the failure.
        control.save()
    finally:
        monitor.end_load(tdef.dataset_table_name)
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

    ``on_table_done`` is called for every (branch, table) result; when a table
    has results from all ``branches_in_run`` branches it is handed to a
    ``settings.load_workers``-wide pool and written immediately -- concurrently
    with extraction and with other tables' loads. A dlt pipeline is not safe to
    run concurrently with ITSELF, so every table loads through its own
    per-table pipeline (see _table_pipeline_name); same-table exclusivity is
    structural (a table becomes ready exactly once per run).
    """
    # Make every Iceberg merge commit a single snapshot instead of one per
    # 1,000 rows (see _install_single_commit_merge). Must run before any load.
    _install_single_commit_merge()
    table_defs = {t.dataset_table_name: t for t in tables}
    order = {t.dataset_table_name: i for i, t in enumerate(tables)}

    remaining = {name: branches_in_run for name in table_defs}
    collected: dict[str, list[ExtractResult]] = {name: [] for name in table_defs}
    all_results: list[ExtractResult] = []
    lock = threading.Lock()
    load_pool = ThreadPoolExecutor(max_workers=settings.load_workers,
                                   thread_name_prefix="load")
    load_futures = []

    monitor = PipelineMonitor(
        total_units=branches_in_run * len(tables),
        total_tables=len(tables),
        interval_s=settings.progress_interval_s,
        enabled=settings.progress_enabled,
    ).start()

    def _load_task(tdef: TableDef, batch: list[ExtractResult]) -> TableLoadPlan:
        # Each table gets its OWN pipeline object with its own local working
        # dir; pipeline.run is called on this explicit object rather than
        # resolved from dlt's (thread-affine) global context, and the commit
        # itself runs on an ephemeral commit:<label> watchdog thread, not here
        # (unless load_commit_timeout_s is 0, which runs it inline on this
        # worker). The name is stable across runs, so pending debris from a crashed/
        # failed earlier run can only ever belong to this table -- swept here
        # instead of once at startup. A commit timeout poisons only this
        # pipeline; nothing else touches it this run, so it is simply
        # abandoned. The zombie's leftover package is dropped by this same
        # sweep on the table's next run.
        try:
            pipeline = build_pipeline(
                settings, pipeline_name=_table_pipeline_name(settings, tdef))
            clear_pending_packages(pipeline, f"{tdef.dataset_table_name}:pre-load")
        except Exception as exc:  # noqa: BLE001 - isolate per-table setup failures
            # A build/sweep failure must still produce a plan so this table's
            # results flow through finalize (observability + retention) like
            # every other table -- an uncaught exception here would instead
            # propagate through f.result() outside the finalize try/finally,
            # silently dropping observability for tables that DID load.
            log.error("[%s] pipeline build failed: %s", tdef.dataset_table_name, exc)
            plan = TableLoadPlan(tdef=tdef, success=[], failed=list(batch))
            plan.load_status = "FAILED"
            plan.load_error = f"{type(exc).__name__}: {exc}"
            monitor.record_table_loaded("FAILED")
            return plan
        plan = _load_one_table(pipeline, tdef, batch, settings, control,
                               total_branches, branches_in_run, monitor)
        if plan.load_timed_out:
            log.warning("[%s] abandoning this table's pipeline after commit "
                        "timeout", tdef.dataset_table_name)
        return plan

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
            load_futures.append(load_pool.submit(_load_task, table_defs[name], batch))

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
        # Observability writes go straight to Postgres via control.store (the
        # same MetaStore ControlStore already holds -- no second engine/pool).
        # Retention enumerates tables from the persistent Iceberg catalog.
        monitor.set_activity("finalize")
        _write_observability(control.store, plans, settings, run_id)
        apply_snapshot_retention(settings)
    finally:
        monitor.stop()

    return LoadSummary(run_id=run_id, plans=plans, results=all_results,
                       extraction_error=extraction_error)
