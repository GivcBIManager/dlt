"""Parallel extraction layer.

Per branch we open a small oracledb connection pool and pull each table on an
inner thread pool, so the shape is: outer pool over branches (<=7) x inner pool
over tables per branch. Every (branch, table) pull:

* builds an INITIAL or INCREMENTAL query from the TableDef + watermark,
* fetches into an Arrow table with explicit Oracle->Arrow typing,
* injects BRANCH_ID + Recorded_updated_at,
* stages the result as parquet (so the load step only runs after *all* branches
  have finished, and failed branches can simply be skipped),
* retries connection failures up to ``max_retries`` every ``retry_interval_s``.

A ``--self-test`` path generates synthetic data instead of touching Oracle so the
whole extract -> unify -> load -> control/log chain can be exercised offline.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Optional

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from .config import (
    HELPER_CDC_ALIAS,
    HELPER_DATE_ALIAS,
    MODE_INCREMENTAL,
    MODE_INITIAL,
    BranchConfig,
    Settings,
    TableDef,
    now_local,
)
from . import types_map

log = logging.getLogger("etl.extract")

# Oracle errors worth retrying (listener down, timeouts, instance restarting,
# disconnects, pool exhaustion). Anything else (bad SQL, missing table, no
# privilege) fails fast so we don't burn 25 minutes on a typo.
_RETRYABLE_ORA = {
    12541, 12170, 12514, 12521, 12537, 12547, 12560, 12571,
    3113, 3114, 3135, 1033, 1034, 1089, 257, 12152,
}
_THICK_LOCK = threading.Lock()
_thick_initialized = False


# --------------------------------------------------------------------------- #
# Result + watermark records
# --------------------------------------------------------------------------- #
@dataclass
class Watermark:
    """A single CDC/date high-water value plus how to render it back into SQL."""

    value: Optional[str] = None
    kind: str = "datetime"  # datetime | number | string

    def to_dict(self) -> Optional[dict]:
        if self.value is None:
            return None
        return {"value": self.value, "kind": self.kind}

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "Watermark":
        if not d:
            return cls(value=None)
        return cls(value=d.get("value"), kind=d.get("kind", "datetime"))


@dataclass
class ExtractResult:
    table_def: TableDef
    branch: str                        # section key, e.g. "alrabwah"
    branch_id: int = 0                 # numeric id stamped into the BRANCH_ID column
    status: str = "PENDING"            # SUCCESS | FAILED | SKIPPED
    row_count: int = 0
    attempts: int = 0
    start_time: Optional[dt.datetime] = None
    end_time: Optional[dt.datetime] = None
    duration_ms: int = 0
    error: Optional[str] = None
    staged_path: Optional[Path] = None
    schema: Optional[pa.Schema] = None
    new_cdc: Watermark = field(default_factory=Watermark)
    new_date: Watermark = field(default_factory=Watermark)

    @property
    def table(self) -> str:
        return self.table_def.dataset_table_name


# --------------------------------------------------------------------------- #
# SQL literal / query building
# --------------------------------------------------------------------------- #
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}")
_NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")


def _sql_quote(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def format_initial_value(raw: str) -> str:
    """Render ``where_value_of_initial_run`` as a SQL literal/expression.

    Already-SQL expressions (e.g. ``TO_NUMBER(...)``) pass through untouched;
    bare date strings get wrapped in TO_DATE; numbers pass through; everything
    else is quoted.
    """
    raw = str(raw).strip()
    if "(" in raw:                       # an Oracle expression already
        return raw
    if _DATE_ONLY_RE.match(raw):
        return f"TO_DATE('{raw}', 'YYYY-MM-DD')"
    if _DATETIME_RE.match(raw):
        return f"TO_TIMESTAMP('{raw}', 'YYYY-MM-DD HH24:MI:SS')"
    if _NUMERIC_RE.match(raw):
        return raw
    return _sql_quote(raw)


def format_watermark(wm: Watermark) -> str:
    """Render a captured watermark back into a SQL literal."""
    if wm.kind == "number":
        return wm.value
    if wm.kind == "string":
        return _sql_quote(wm.value)
    # datetime
    return f"TO_TIMESTAMP('{wm.value}', 'YYYY-MM-DD HH24:MI:SS.FF6')"


@dataclass
class _QueryShape:
    """Resolved SELECT/FROM and cdc/date column references for a table.

    Abstracts over a plain table vs a helper-driven one so the query builders
    don't care which they're dealing with:

    * plain table  -> ``SELECT t.* FROM <table> t``; predicates on ``t.<col>``.
    * helper table -> ``SELECT t.*, h.<cdc> AS ETL_HELPER_CDC[, h.<date> AS
      ETL_HELPER_DATE] FROM <child> t JOIN <helper> h ON ...``; predicates on
      ``h.<col>``. The reserved aliases carry the helper watermark columns into
      the staged parquet (and are stripped before the Iceberg write).
    """

    select: str
    frm: str
    cdc_ref: Optional[str]
    date_ref: Optional[str]


def _query_shape(tdef: TableDef) -> _QueryShape:
    """Compute the SELECT/FROM clauses and cdc/date column refs for ``tdef``."""
    extras: list[str] = []
    if tdef.helper is not None:
        h = tdef.helper
        on = " AND ".join(f"t.{child} = h.{helper}" for child, helper in h.join_keys)
        frm = f"{tdef.table} t JOIN {h.table} h ON {on}"
        extras.append(f"h.{h.cdc_column} AS {HELPER_CDC_ALIAS}")
        cdc_ref = f"h.{h.cdc_column}"
        date_ref = None
        if h.where_date_column:
            extras.append(f"h.{h.where_date_column} AS {HELPER_DATE_ALIAS}")
            date_ref = f"h.{h.where_date_column}"
    else:
        frm = f"{tdef.table} t"
        cdc_ref = f"t.{tdef.cdc_column}" if tdef.cdc_column else None
        date_ref = (
            f"t.{tdef.where_date_column}" if tdef.where_date_column else None
        )

    # Expression keys are projected into a single DERIVED_KEY column; for a
    # helper-driven table the author qualifies child columns with ``t.``.
    if tdef.key_is_expression:
        extras.append(f"({tdef.unique_key}) AS {tdef.derived_key_alias}")

    select = "t.*" + ("".join(f", {e}" for e in extras))
    return _QueryShape(select=select, frm=frm, cdc_ref=cdc_ref, date_ref=date_ref)


def _date_ceiling_pred(tdef: TableDef, shape: _QueryShape) -> Optional[str]:
    """Optional upper-bound predicate on the date column (``date <= ceiling``).

    Bounds an otherwise open-ended ``date >= floor`` filter into a window. Used
    for tables whose date column is a *future* scheduled date (e.g.
    APPOINTMENTS.JULIAN_DATE) so we don't pull the whole forward booking book.
    The ceiling is usually a rolling SYSDATE expression evaluated server-side, so
    the window edge advances on every run. Returns None when no ceiling is set or
    the table has no date column to bound.
    """
    if shape.date_ref and tdef.where_value_max:
        op = tdef.where_operator_max or "<="
        return f"{shape.date_ref} {op} {format_initial_value(tdef.where_value_max)}"
    return None


def build_query(
    tdef: TableDef,
    settings: Settings,
    cdc_wm: Watermark,
    date_wm: Watermark,
) -> str:
    """Construct the SELECT for a table given the load mode + watermarks."""
    shape = _query_shape(tdef)
    base = f"SELECT {shape.select} FROM {shape.frm}"
    ceiling_pred = _date_ceiling_pred(tdef, shape)

    incremental_ready = (
        settings.mode == MODE_INCREMENTAL
        and cdc_wm.value is not None
        and shape.cdc_ref is not None
    )
    if incremental_ready:
        return _build_incremental_query(shape, base, cdc_wm, date_wm, ceiling_pred)

    # INITIAL (or INCREMENTAL with no prior watermark -> behave as initial).
    # Masters: full load. Transactions: configured range filter applied to the
    # date column (``t.<date>`` for a plain table, ``h.<date>`` for a helper).
    where: list[str] = []
    if (
        not tdef.is_master
        and shape.date_ref
        and tdef.where_value_of_initial_run
    ):
        op = tdef.where_operator or ">="
        val = format_initial_value(tdef.where_value_of_initial_run)
        where.append(f"{shape.date_ref} {op} {val}")
    # Cap the window from above (e.g. JULIAN_DATE <= today) when configured.
    if ceiling_pred:
        where.append(ceiling_pred)

    if where:
        base += " WHERE " + " AND ".join(where)
    return base


def _build_incremental_query(
    shape: _QueryShape,
    base: str,
    cdc_wm: Watermark,
    date_wm: Watermark,
    ceiling_pred: Optional[str] = None,
) -> str:
    """Build the INCREMENTAL SELECT, split so each branch can use an index.

    The old single-statement form was ``WHERE (cdc > wm OR date >= wm)``. An OR
    across two columns can never use an index when *either* column is unindexed
    (the CDC column ``AMEND_LAST_DATE`` is unindexed on every branch), so Oracle
    always full-scanned the table - 100M+ rows on the big transaction tables.

    We split it into a UNION ALL of two *disjoint* branches:

      * new rows     -> WHERE date >= wm                         (uses date index)
      * updated rows -> WHERE cdc > wm AND (date < wm OR date IS NULL)

    The branches are disjoint (a row is "new" XOR "updated-but-old"), so UNION
    ALL needs no dedup, and rows with a NULL date that were updated are still
    captured. The new-rows branch becomes an index range scan immediately; the
    updated-rows branch becomes one too once ``cdc_column`` is indexed (the OR
    form could not use that index even if it existed).

    The cdc/date column references come from ``shape``, so this is identical for
    a plain table (``t.<col>``) and a helper-driven one (``h.<col>``).

    ``ceiling_pred`` (e.g. ``JULIAN_DATE <= today``) bounds the *new rows* branch
    from above. The updated-rows branch is already bounded (it requires
    ``date < watermark``), so the ceiling only needs to apply where we select
    "new by date".
    """
    has_cdc = shape.cdc_ref is not None
    has_date = shape.date_ref is not None and date_wm.value is not None

    cdc_pred = (
        f"{shape.cdc_ref} > {format_watermark(cdc_wm)}" if has_cdc else None
    )
    date_pred = (
        f"{shape.date_ref} >= {format_watermark(date_wm)}" if has_date else None
    )
    ceil = f" AND {ceiling_pred}" if ceiling_pred else ""

    # Only one dimension available -> a single filtered query, no UNION needed.
    if has_date and not has_cdc:
        return f"{base} WHERE {date_pred}{ceil}"
    if has_cdc and not has_date:
        return f"{base} WHERE {cdc_pred}"
    if not has_cdc and not has_date:
        return base  # nothing to filter on (treated as full load)

    # Both available -> disjoint UNION ALL so each branch keeps its own index.
    date_wm_sql = format_watermark(date_wm)
    new_rows = f"{base} WHERE {date_pred}{ceil}"
    updated_old = (
        f"{base} WHERE {cdc_pred} "
        f"AND ({shape.date_ref} < {date_wm_sql} "
        f"OR {shape.date_ref} IS NULL)"
    )
    return f"{new_rows}\nUNION ALL\n{updated_old}"


# --------------------------------------------------------------------------- #
# Oracle client / connection pools
# --------------------------------------------------------------------------- #
def ensure_oracle_client(settings: Settings) -> None:
    """Initialise thick mode once (required for Oracle 11g)."""
    global _thick_initialized
    if not settings.thick_mode or _thick_initialized:
        return
    with _THICK_LOCK:
        if _thick_initialized:
            return
        lib = settings.oracle_client_lib_dir
        if lib and not Path(lib).is_dir():
            log.warning("oracle_client_lib_dir %r not found on this host; relying "
                        "on the system library path (PATH / LD_LIBRARY_PATH)", lib)
            lib = None
        kwargs = {"lib_dir": lib} if lib else {}
        oracledb.init_oracle_client(**kwargs)
        _thick_initialized = True
        log.info("Initialised python-oracledb thick client (Oracle 11g mode%s)",
                 f", lib_dir={lib}" if lib else ", system library path")


class BranchPool:
    """A connection pool for one branch, with acquire backoff."""

    def __init__(self, branch: BranchConfig, settings: Settings):
        self.branch = branch
        self.settings = settings
        self.pool = oracledb.create_pool(
            user=branch.username,
            password=branch.password,
            dsn=branch.dsn(settings.dsn_mode),
            min=settings.pool_min,
            max=settings.pool_max,
            increment=settings.pool_increment,
            getmode=oracledb.POOL_GETMODE_TIMEDWAIT,
            wait_timeout=settings.pool_acquire_timeout_s * 1000,
        )

    def acquire(self):
        """Acquire with exponential backoff on pool exhaustion/timeout."""
        last_err: Optional[Exception] = None
        for attempt in range(1, self.settings.pool_acquire_attempts + 1):
            try:
                return self.pool.acquire()
            except oracledb.Error as exc:
                last_err = exc
                wait = min(
                    self.settings.pool_backoff_cap_s,
                    self.settings.pool_backoff_base_s * (2 ** (attempt - 1)),
                )
                log.warning(
                    "[%s] pool acquire attempt %d/%d failed (%s); backoff %.1fs",
                    self.branch.key, attempt, self.settings.pool_acquire_attempts,
                    exc, wait,
                )
                time.sleep(wait)
        raise RuntimeError(
            f"[{self.branch.key}] pool exhausted after "
            f"{self.settings.pool_acquire_attempts} attempts"
        ) from last_err

    def close(self):
        try:
            self.pool.close(force=True)
        except oracledb.Error:
            pass


def _ora_code(exc: Exception) -> Optional[int]:
    try:
        return exc.args[0].code  # type: ignore[attr-defined]
    except (AttributeError, IndexError, TypeError):
        return None


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, RuntimeError):       # our own "pool exhausted"
        return True
    code = _ora_code(exc)
    if code in _RETRYABLE_ORA:
        return True
    msg = str(exc).upper()
    # Fallback: parse ORA-NNNNN from the message in case the code wasn't on the
    # exception object (driver/version differences, wrapped errors).
    if any(int(m) in _RETRYABLE_ORA for m in re.findall(r"ORA-0*(\d+)", msg)):
        return True
    return any(tok in msg for tok in ("DPY-4011", "DPY-6005", "TIMEOUT", "TNS"))


# --------------------------------------------------------------------------- #
# Fetch -> Arrow
# --------------------------------------------------------------------------- #
def fetch_to_arrow(conn, query: str, fetch_batch_size: int) -> pa.Table:
    """Run a query and build a typed Arrow table from the cursor."""
    cur = conn.cursor()
    cur.arraysize = fetch_batch_size
    cur.prefetchrows = fetch_batch_size + 1
    cur.execute(query)

    description = cur.description
    names = [d.name for d in description]
    target_types = [types_map.oracle_field_to_arrow(d) for d in description]
    columns: list[list[Any]] = [[] for _ in names]

    while True:
        rows = cur.fetchmany(fetch_batch_size)
        if not rows:
            break
        for row in rows:
            for i, value in enumerate(row):
                columns[i].append(value)
    cur.close()

    arrays = [
        types_map.build_arrow_column(columns[i], target_types[i])
        for i in range(len(names))
    ]
    return pa.table(dict(zip(names, arrays)))


def inject_columns(
    table: pa.Table,
    branch_id: int,
    settings: Settings,
    now: Optional[dt.datetime] = None,
) -> pa.Table:
    """Add BRANCH_ID + insert_at + Recorded_updated_at to every result set.

    ``now`` is passed in so every batch of one extraction shares a single
    load timestamp. ``insert_at`` is stamped with the load time here as a
    default; for INCREMENTAL merges the load step carries forward the *existing*
    insert_at for rows that are updates (see iceberg_load), so it ends up holding
    each row's first-load time while ``Recorded_updated_at`` tracks the latest.
    """
    n = table.num_rows
    if now is None:
        now = now_local()
    table = table.append_column(
        settings.branch_id_column, pa.array([branch_id] * n, pa.int64())
    )
    table = table.append_column(
        settings.inserted_ts_column, pa.array([now] * n, pa.timestamp("us"))
    )
    table = table.append_column(
        settings.recorded_ts_column, pa.array([now] * n, pa.timestamp("us"))
    )
    return table


def _column_max_watermark(table: pa.Table, column: str) -> Watermark:
    """Compute a high-water value + kind for a column (max over the batch)."""
    if column not in table.column_names or table.num_rows == 0:
        return Watermark(value=None)
    import pyarrow.compute as pc

    col = table.column(column)
    mx = pc.max(col).as_py()
    if mx is None:
        return Watermark(value=None)
    if isinstance(mx, dt.datetime):
        return Watermark(value=mx.strftime("%Y-%m-%d %H:%M:%S.%f"), kind="datetime")
    if isinstance(mx, dt.date):
        return Watermark(value=mx.strftime("%Y-%m-%d 00:00:00.000000"), kind="datetime")
    if isinstance(mx, (int, float, Decimal)):
        return Watermark(value=str(mx), kind="number")
    return Watermark(value=str(mx), kind="string")


def _stage(table: pa.Table, tdef: TableDef, branch_key: str, settings: Settings) -> Path:
    out_dir = settings.staging_dir / tdef.dataset_table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{branch_key}.parquet"
    pq.write_table(table, path)
    return path


def _staged_paths(tdef: TableDef, branch_key: str, settings: Settings) -> tuple[Path, Path]:
    out_dir = settings.staging_dir / tdef.dataset_table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    return (out_dir / f"{branch_key}.parquet.tmp", out_dir / f"{branch_key}.parquet")


def _cleanup_tmp(tdef: TableDef, branch_key: str, settings: Settings) -> None:
    tmp_path, _ = _staged_paths(tdef, branch_key, settings)
    try:
        if tmp_path.exists():
            tmp_path.unlink()
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# Fetch -> staged parquet
#
# Fast path: oracledb's native Arrow fetch (fetch_df_batches) builds columnar
# Arrow data in C and streams it straight to parquet -- no per-row Python tuples.
# Fallback path: the classic cursor builder (fetch_to_arrow), used automatically
# for empty results or any column type the Arrow fetch can't handle (an Oracle
# 11g safety net). Connection errors are NOT swallowed here -- they propagate to
# the caller's retry loop.
# --------------------------------------------------------------------------- #
class _ArrowEmpty(Exception):
    """Native Arrow fetch produced no rows -> use the cursor path for the schema."""


def _stage_via_arrow(conn, query, tdef, branch_key, branch_id, settings, fetch_batch_size, now) -> tuple[int, pa.Schema, Path]:
    tmp_path, final_path = _staged_paths(tdef, branch_key, settings)
    writer: Optional[pq.ParquetWriter] = None
    out_schema: Optional[pa.Schema] = None
    row_count = 0
    try:
        # OracleDataFrame batches -> zero-copy-ish pyarrow tables.
        for odf in conn.fetch_df_batches(query, size=fetch_batch_size):
            batch = pa.table(odf)
            if batch.num_rows == 0:
                continue
            batch = inject_columns(batch, branch_id, settings, now=now)
            if writer is None:
                out_schema = batch.schema
                writer = pq.ParquetWriter(tmp_path, out_schema)
            elif not batch.schema.equals(out_schema):
                batch = batch.cast(out_schema)
            writer.write_table(batch)
            row_count += batch.num_rows
        if writer is None:
            raise _ArrowEmpty()
    finally:
        if writer is not None:
            writer.close()
    os.replace(tmp_path, final_path)
    return row_count, out_schema, final_path


def _stage_via_cursor(conn, query, tdef, branch_key, branch_id, settings, fetch_batch_size, now) -> tuple[int, pa.Schema, Path]:
    table = fetch_to_arrow(conn, query, fetch_batch_size)
    table = inject_columns(table, branch_id, settings, now=now)
    path = _stage(table, tdef, branch_key, settings)
    return table.num_rows, table.schema, path


def fetch_and_stage(conn, query, tdef, branch_key, branch_id, settings, fetch_batch_size, now) -> tuple[int, pa.Schema, Path]:
    """Stage one (branch, table) result, preferring the native Arrow fast path.

    ``branch_key`` names the staged file; ``branch_id`` is stamped into the
    BRANCH_ID column. ``fetch_batch_size`` is the branch's own round-trip size
    (see BranchConfig).
    """
    try:
        return _stage_via_arrow(conn, query, tdef, branch_key, branch_id, settings, fetch_batch_size, now)
    except _ArrowEmpty:
        return _stage_via_cursor(conn, query, tdef, branch_key, branch_id, settings, fetch_batch_size, now)
    except Exception as exc:  # noqa: BLE001
        if is_retryable(exc):
            raise  # genuine connection/transient error -> caller retries
        log.warning(
            "[%s/%s] native Arrow fetch unavailable (%s); using cursor fetch",
            branch_key, tdef.dataset_table_name, exc,
        )
        _cleanup_tmp(tdef, branch_key, settings)
        return _stage_via_cursor(conn, query, tdef, branch_key, branch_id, settings, fetch_batch_size, now)


def _watermarks_from_parquet(path: Path, tdef: TableDef) -> tuple[Watermark, Watermark]:
    """Read just the CDC/date columns back from the staged parquet for max().

    Uses the *capture* column names (``cdc_capture_column`` / ``date_capture_column``):
    the table's own cdc/date for a plain table, or the reserved helper aliases
    (``ETL_HELPER_CDC`` / ``ETL_HELPER_DATE``) for a helper-driven one.
    """
    cdc, date = Watermark(value=None), Watermark(value=None)
    cdc_col = tdef.cdc_capture_column
    date_col = tdef.date_capture_column
    # Dedupe: a table may use the same column for both CDC and date (e.g.
    # DELIVERY_LINES, where cdc_column == where_date_column == AMEND_LAST_DATE).
    # Passing the name twice to read_table yields a table with two identically
    # named columns, and the later .column(name) max() would raise
    # 'Field "X" exists 2 times in schema'.
    cols = list(dict.fromkeys(c for c in (cdc_col, date_col) if c))
    if not cols:
        return cdc, date
    try:
        available = set(pq.read_schema(path).names)
        read_cols = [c for c in cols if c in available]
        if not read_cols:
            return cdc, date
        table = pq.read_table(path, columns=read_cols)
    except Exception:  # noqa: BLE001 - watermark read is best-effort
        return cdc, date
    if cdc_col and cdc_col in table.column_names:
        cdc = _column_max_watermark(table, cdc_col)
    if date_col and date_col in table.column_names:
        date = _column_max_watermark(table, date_col)
    return cdc, date


# --------------------------------------------------------------------------- #
# Single (branch, table) extraction with retry
# --------------------------------------------------------------------------- #
def extract_table(
    pool: BranchPool,
    branch: BranchConfig,
    tdef: TableDef,
    settings: Settings,
    watermarks: dict,
) -> ExtractResult:
    result = ExtractResult(table_def=tdef, branch=branch.key, branch_id=branch.id)
    result.start_time = now_local()

    cdc_wm = Watermark.from_dict(watermarks.get("last_cdc"))
    date_wm = Watermark.from_dict(watermarks.get("last_date"))
    query = build_query(tdef, settings, cdc_wm, date_wm)
    log.debug("[%s/%s] query: %s", branch.key, tdef.dataset_table_name, query)

    for attempt in range(1, settings.max_retries + 1):
        result.attempts = attempt
        conn = None
        try:
            conn = pool.acquire()
            now = now_local()
            row_count, schema, staged_path = fetch_and_stage(
                conn, query, tdef, branch.key, branch.id, settings, branch.fetch_batch_size, now)

            # Capture watermarks from the source CDC/date columns (read back the
            # staged parquet so this works identically for both fetch paths).
            result.new_cdc, result.new_date = _watermarks_from_parquet(staged_path, tdef)
            result.row_count = row_count
            result.schema = schema
            result.staged_path = staged_path
            result.status = "SUCCESS"
            log.info(
                "[%s/%s] %d rows (attempt %d)",
                branch.key, tdef.dataset_table_name, result.row_count, attempt,
            )
            break
        except Exception as exc:  # noqa: BLE001 - we classify below
            # Only connection/transient errors are retried. Any other error
            # (bad SQL, missing table, type/conversion error, ...) is raised
            # immediately so it surfaces during reading instead of being
            # swallowed into a FAILED row.
            if not is_retryable(exc):
                log.error("[%s/%s] non-connection error during read: %s",
                          branch.key, tdef.dataset_table_name, exc)
                raise
            result.error = f"{type(exc).__name__}: {exc}"
            if attempt < settings.max_retries:
                log.warning(
                    "[%s/%s] connection error attempt %d/%d: %s; sleeping %ds",
                    branch.key, tdef.dataset_table_name, attempt,
                    settings.max_retries, exc, settings.retry_interval_s,
                )
                time.sleep(settings.retry_interval_s)
                continue
            # Connection retries exhausted -> collect as FAILED so other
            # branches/tables are not blocked (resilience requirement).
            result.status = "FAILED"
            log.error(
                "[%s/%s] connection failed after %d attempt(s): %s",
                branch.key, tdef.dataset_table_name, attempt, exc,
            )
            break
        finally:
            if conn is not None:
                try:
                    conn.close()
                except oracledb.Error:
                    pass

    result.end_time = now_local()
    result.duration_ms = int(
        (result.end_time - result.start_time).total_seconds() * 1000
    )
    return result


# --------------------------------------------------------------------------- #
# Branch-level extraction (inner pool over tables)
# --------------------------------------------------------------------------- #
def extract_branch(
    branch: BranchConfig,
    tables: list[TableDef],
    settings: Settings,
    control: dict,
    on_table_done: Optional[Callable[[ExtractResult], None]] = None,
) -> list[ExtractResult]:
    """Extract every table for one branch; pool failure fails the branch only.

    ``on_table_done`` (if given) is called with each (branch, table) result as
    soon as it completes, so the caller can load that table without waiting for
    the rest.
    """
    # Try to stand up the branch pool, retrying like a connection failure so a
    # temporarily-down branch doesn't block the other six.
    pool: Optional[BranchPool] = None
    pool_error: Optional[str] = None
    for attempt in range(1, settings.max_retries + 1):
        try:
            pool = BranchPool(branch, settings)
            break
        except Exception as exc:  # noqa: BLE001
            # Only connection/transient errors are retried; anything else
            # (e.g. invalid credentials) is raised immediately.
            if not is_retryable(exc):
                log.error("[%s] non-connection error opening pool: %s",
                          branch.key, exc)
                raise
            pool_error = f"{type(exc).__name__}: {exc}"
            if attempt < settings.max_retries:
                log.warning(
                    "[%s] pool create attempt %d/%d failed: %s; sleeping %ds",
                    branch.key, attempt, settings.max_retries, exc,
                    settings.retry_interval_s,
                )
                time.sleep(settings.retry_interval_s)
                continue
            break

    if pool is None:
        log.error("[%s] branch unreachable: %s", branch.key, pool_error)
        results = []
        for tdef in tables:
            r = ExtractResult(table_def=tdef, branch=branch.key, branch_id=branch.id,
                              status="FAILED", error=pool_error,
                              attempts=settings.max_retries)
            r.start_time = r.end_time = now_local()
            results.append(r)
            if on_table_done is not None:
                on_table_done(r)
        return results

    results: list[ExtractResult] = []
    try:
        workers = max(1, min(settings.max_table_workers, len(tables)))
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix=f"tbl-{branch.key}") as inner:
            futs = {
                inner.submit(
                    extract_table, pool, branch, tdef, settings,
                    control.get(tdef.dataset_table_name, {}).get(branch.key, {}),
                ): tdef
                for tdef in tables
            }
            for fut in as_completed(futs):
                result = fut.result()
                results.append(result)
                if on_table_done is not None:
                    on_table_done(result)
    finally:
        pool.close()
    return results


# --------------------------------------------------------------------------- #
# Top-level extraction (outer pool over branches)
# --------------------------------------------------------------------------- #
def run_extraction(
    branches: list[BranchConfig],
    tables: list[TableDef],
    settings: Settings,
    control: dict,
    on_table_done: Optional[Callable[[ExtractResult], None]] = None,
) -> list[ExtractResult]:
    """Run the nested extraction (branches outer, tables inner).

    ``on_table_done`` is invoked for every (branch, table) result the moment it
    finishes -- the load layer uses this to write a table as soon as all of its
    branches are done, instead of waiting for the whole extraction to complete.
    """
    if settings.self_test:
        return _self_test_extraction(branches, tables, settings, on_table_done)

    ensure_oracle_client(settings)
    results: list[ExtractResult] = []
    workers = max(1, min(settings.max_branch_workers, len(branches)))
    log.info("Extracting %d tables x %d branches (%d branch workers)",
             len(tables), len(branches), workers)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="branch") as outer:
        futs = {
            outer.submit(extract_branch, b, tables, settings, control, on_table_done): b
            for b in branches
        }
        for fut in as_completed(futs):
            results.extend(fut.result())
    return results


# --------------------------------------------------------------------------- #
# Offline self-test: synthetic data, no Oracle required
# --------------------------------------------------------------------------- #
def _synthetic_table(tdef: TableDef, branch: BranchConfig, settings: Settings) -> pa.Table:
    """Generate a small typed table that mimics the source shape.

    Deliberately varies columns/types per branch so schema unification and type
    widening get exercised end to end.
    """
    rng = random.Random(f"{tdef.table}:{branch.key}")
    n = rng.randint(3, 8)
    data: dict[str, pa.Array] = {}

    # key column(s)
    for k in tdef.key_columns:
        data[k] = pa.array([f"{branch.key[:3].upper()}-{i}" for i in range(n)], pa.string())

    # cdc column as a recent timestamp. For a helper-driven table the child has
    # no cdc/date column of its own, so we emit the reserved helper aliases
    # (ETL_HELPER_CDC / ETL_HELPER_DATE) instead -- mirroring the projected
    # columns a real helper JOIN would carry into the staged parquet.
    cdc_name = tdef.cdc_capture_column
    date_name = tdef.date_capture_column
    if cdc_name:
        base = dt.datetime(2024, 1, 1)
        data[cdc_name] = pa.array(
            [base + dt.timedelta(days=rng.randint(0, 500), seconds=rng.randint(0, 86399))
             for _ in range(n)], pa.timestamp("us")
        )

    # where_date column: numeric (Julian-style) or date depending on the config
    if date_name:
        init = str(tdef.where_value_of_initial_run or "")
        if "TO_NUMBER" in init or _NUMERIC_RE.match(init):
            data[date_name] = pa.array(
                [2459000 + rng.randint(0, 900) for _ in range(n)], pa.int64()
            )
        else:
            d0 = dt.datetime(2022, 6, 1)
            data[date_name] = pa.array(
                [d0 + dt.timedelta(days=rng.randint(0, 800)) for _ in range(n)],
                pa.timestamp("us"),
            )

    # a NUMBER-ish amount whose scale varies per branch -> forces decimal widening
    scale = 2 if (hash(branch.key) % 2 == 0) else 4
    data["AMOUNT"] = pa.array(
        [Decimal(f"{rng.randint(1, 9999)}.{rng.randint(0, 10**scale - 1):0{scale}d}")
         for _ in range(n)],
        pa.decimal128(18, scale),
    )

    # a VARCHAR2-ish description
    data["DESCRIPTION"] = pa.array([f"row {i} @ {branch.name}" for i in range(n)], pa.string())

    # one branch carries an extra column -> forces column-union + nullability
    if branch.key.endswith(("h", "n")):
        data["EXTRA_FLAG"] = pa.array([rng.choice(["Y", "N"]) for _ in range(n)], pa.string())

    return pa.table(data)


def _self_test_extraction(
    branches: list[BranchConfig],
    tables: list[TableDef],
    settings: Settings,
    on_table_done: Optional[Callable[[ExtractResult], None]] = None,
) -> list[ExtractResult]:
    log.warning("SELF-TEST mode: generating synthetic data (Oracle is NOT contacted)")
    results: list[ExtractResult] = []
    # Iterate tables-outer so each table's branches complete together, mirroring
    # how the streaming loader sees completions in a real run.
    for tdef in tables:
        for branch in branches:
            r = ExtractResult(table_def=tdef, branch=branch.key, branch_id=branch.id, attempts=1)
            r.start_time = now_local()
            table = _synthetic_table(tdef, branch, settings)
            if tdef.cdc_capture_column:
                r.new_cdc = _column_max_watermark(table, tdef.cdc_capture_column)
            if tdef.date_capture_column:
                r.new_date = _column_max_watermark(table, tdef.date_capture_column)
            table = inject_columns(table, branch.id, settings)
            r.row_count = table.num_rows
            r.schema = table.schema
            r.staged_path = _stage(table, tdef, branch.key, settings)
            r.status = "SUCCESS"
            r.end_time = now_local()
            r.duration_ms = int((r.end_time - r.start_time).total_seconds() * 1000)
            results.append(r)
            if on_table_done is not None:
                on_table_done(r)
    return results
