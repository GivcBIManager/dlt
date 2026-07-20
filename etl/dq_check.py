"""Data-quality reconciliation: Oracle (source) vs Iceberg (lake), per branch.

Two checks are run for every ``(table, branch)`` over **one shared window**:

* **Row-count comparison** -- ``COUNT(*)`` of the source rows in the window vs the
  number of rows in the Iceberg branch partition in the same window. The delta
  (``oracle - iceberg``) flags loads that dropped or duplicated rows.
* **Row-hash delta** -- a per-row content fingerprint (hash over the *common*
  business columns) is computed on both sides, the two are joined on the table's
  unique key, and the rows are bucketed into ``matched`` / ``only_in_oracle`` /
  ``only_in_iceberg`` / ``hash_mismatch``. This catches content drift that a bare
  count would miss.

The window is **YTD .. last run**: from January 1 of the current year (the
``--since`` default) up to each ``(table, branch)``'s last-run watermark in the
Postgres ``control_state`` table (via ``ControlStore``/``MetaStore``) (the
``--until`` default). Both checks use the *same*
window so the count delta and the hash delta describe the same row set. Master
tables (no date column) are compared in full; helper-driven tables whose
watermark column differs from their own date column skip the upper bound (see
``_make_window``).

Results are written to the Iceberg table ``etl_dq_results`` (append) in the same
dataset as the pipeline output -- alongside ``etl_control`` / ``etl_run_log`` --
and printed as a console summary.

Type parity with the lake is the whole game for the hash check, so the source is
read through the *same* native-Arrow fetch the pipeline uses
(``connection.fetch_df_batches``): Oracle ``NUMBER`` lands as the same Arrow
``double`` the lake stores, dates as the same ``timestamp``. The canonicalizer
then erases the only representational differences that remain (a tz tag on the
lake's timestamps, decimal scale) so equal values hash equal -- see
``_canon_array``.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from hashlib import blake2b
from pathlib import Path
from typing import Iterable, Iterator, Optional
from urllib.parse import urlparse
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import (
    HELPER_RESERVED_COLUMNS,
    BranchConfig,
    Settings,
    TableDef,
    now_local,
)

log = logging.getLogger("etl.dq")

# Oracle's TO_CHAR(date,'J') Julian day == proleptic-Gregorian ordinal + this
# offset (verified: 2000-01-01 -> ordinal 730120 -> Oracle J 2451545).
_JULIAN_OFFSET = 1721425

# Field separators used when concatenating canonical column values into a row
# fingerprint; control chars so they can't collide with real data.
_SEP = "\x1f"
_NULL = "\x00\x00NULL"

_NUMERIC_INIT_RE = re.compile(r"^-?\d+(\.\d+)?$")
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Rolling "as of now" ceilings (evaluated against the server clock); pinned to
# today so both engines bound the identical day -- see ``_ceiling_bounds``.
_NOW_EXPR_RE = re.compile(r"SYSDATE|SYSTIMESTAMP|CURRENT_DATE|CURRENT_TIMESTAMP", re.I)
_WM_DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
_TABLE_NAME = "etl_dq_results"

STATUS_OK = "OK"
STATUS_WITHIN_TOLERANCE = "WITHIN_TOLERANCE"
STATUS_MISMATCH = "MISMATCH"


def _norm(name: str) -> str:
    """Lower-snake a column name the same way dlt normalizes lake identifiers."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def _injected_norms(settings: Settings) -> set[str]:
    """Normalized names of the ETL-injected columns + reserved helper aliases.

    These are excluded from the business-column set on both sides so they never
    enter a row fingerprint (the source doesn't have them; the lake does).
    """
    injected = {
        _norm(settings.branch_id_column),
        _norm(settings.inserted_ts_column),
        _norm(settings.recorded_ts_column),
    }
    injected |= {_norm(c) for c in HELPER_RESERVED_COLUMNS}
    return injected


# --------------------------------------------------------------------------- #
# Canonicalization + row fingerprint
# --------------------------------------------------------------------------- #
def _canon_decimal(v) -> str:
    """Scale-insensitive decimal string ('123.40' and '123.4000' -> '123.4').

    The lake widens decimal scales across branches, so the same value can read
    back with extra trailing zeros; normalizing removes that as a hash diff.
    """
    if v is None:
        return _NULL
    from decimal import Decimal

    s = format(Decimal(v), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("", "-", "-0") else s


def _canon_other(v) -> str:
    if v is None:
        return _NULL
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    return str(v)


def _canon_array(col) -> pa.Array:
    """Map one Arrow column to canonical UTF-8 strings (vectorized per type).

    Parity rules, applied identically to the source and the lake column:

    * timestamp -> ``YYYY-MM-DD HH:MM:SS`` (seconds; a tz tag is dropped first, so
      the lake's ``timestamp[tz=UTC]`` and the source's naive timestamp -- which
      hold the *same* wall clock -- canonicalize equal).
    * date      -> ``YYYY-MM-DD``.
    * float/int/bool -> Arrow's own ``cast`` to string (identical on both sides
      because both arrive as the same Arrow type with the same value).
    * decimal   -> scale-normalized (see ``_canon_decimal``).
    * string    -> as-is.
    * other     -> ``str``/hex fallback.

    Nulls become a sentinel so a missing value is distinct from an empty string
    and the row-wise join below never sees a null component.
    """
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()
    t = col.type
    if pa.types.is_timestamp(t):
        # Cast to seconds (and drop any tz) BEFORE formatting: pyarrow's strftime
        # renders %S with a fractional part sized to the column's unit, so a
        # timestamp[ms] and a timestamp[us] holding the same instant would
        # otherwise stringify as '...25.000' vs '...25.000000'. Seconds unit ->
        # no fraction, and a dropped tz gives the UTC wall clock the source's
        # naive timestamp also holds.
        if t.tz is not None:
            col = col.cast(pa.timestamp("us"))
        s = pc.strftime(col.cast(pa.timestamp("s")), format="%Y-%m-%d %H:%M:%S")
    elif pa.types.is_date(t):
        s = pc.strftime(col, format="%Y-%m-%d")
    elif pa.types.is_floating(t) or pa.types.is_integer(t) or pa.types.is_boolean(t):
        s = pc.cast(col, pa.string())
    elif pa.types.is_large_string(t):
        s = pc.cast(col, pa.string())
    elif pa.types.is_string(t):
        s = col
    elif pa.types.is_decimal(t):
        # Vectorized equivalent of _canon_decimal: a decimal column has a fixed
        # scale, so every value casts to a string with exactly `scale` fractional
        # digits. Strip trailing zeros then a bare trailing dot (only when scale
        # > 0, i.e. a '.' is present). Arrow decimals can't be negative-zero
        # (the unscaled value is an integer), so no "-0" special case is needed.
        s = pc.cast(col, pa.string())
        if t.scale > 0:
            s = pc.replace_substring_regex(s, "0+$", "")
            s = pc.replace_substring_regex(s, "\\.$", "")
    else:
        s = pa.array([_canon_other(v) for v in col.to_pylist()], pa.string())
    if isinstance(s, pa.ChunkedArray):
        s = s.combine_chunks()
    return pc.if_else(pc.is_null(s), pa.scalar(_NULL, pa.string()), s)


def _fingerprint(tbl: pa.Table, cols_actual: list[str]) -> pa.Array:
    """Concatenate the canonical form of ``cols_actual`` into one string per row."""
    if not cols_actual:
        return pa.array([""] * tbl.num_rows, pa.string())
    arrs = [_canon_array(tbl.column(c)) for c in cols_actual]
    if len(arrs) == 1:
        return arrs[0]
    return pc.binary_join_element_wise(*arrs, _SEP)


def _key_and_hash(
    tbl: pa.Table, key_actual: list[str], payload_actual: list[str]
) -> tuple[pa.Array, pa.Array]:
    """Return (key string, payload hash) arrays for the rows of ``tbl``.

    The key is the raw canonical join key; the payload is hashed to a compact
    16-byte digest so the per-window key/hash table that feeds the comparison
    join stays small even when the business columns are wide.
    """
    keys = _fingerprint(tbl, key_actual)
    payload = _fingerprint(tbl, payload_actual)
    # 16-byte binary digest (not 32-char hex): half the memory in the per-window
    # (key, hash) tables and the comparison join, which is the binding constraint
    # on the large windows. Equality/min over binary behave identically.
    hashes = pa.array(
        [blake2b(p.encode("utf-8"), digest_size=16).digest()
         for p in payload.to_pylist()],
        pa.binary(16),
    )
    return keys, hashes


# --------------------------------------------------------------------------- #
# Comparison
# --------------------------------------------------------------------------- #
@dataclass
class HashDelta:
    matched: int = 0
    only_in_oracle: int = 0
    only_in_iceberg: int = 0
    mismatch: int = 0
    oracle_rows: int = 0
    iceberg_rows: int = 0
    columns: int = 0

    @property
    def total_delta(self) -> int:
        return self.only_in_oracle + self.only_in_iceberg + self.mismatch


def _hash_delta_pct(hash: Optional[HashDelta]) -> Optional[float]:
    """Percent of Oracle hashed rows that diverged (None when undefined).

    ``0.0`` for a clean hash, ``None`` when no hash ran or when Oracle hashed 0
    rows yet a delta exists (an undefined ratio -- treated as a mismatch upstream).
    """
    if hash is None:
        return None
    if hash.total_delta == 0:
        return 0.0
    if hash.oracle_rows <= 0:
        return None
    return 100.0 * hash.total_delta / hash.oracle_rows


def classify_status(
    row_count_delta: Optional[int],
    hash: Optional[HashDelta],
    tolerance_pct: float,
) -> tuple[str, Optional[float]]:
    """Return ``(status, hash_delta_pct)`` for a completed unit.

    ERROR is decided by the caller (a check that could not complete). Row-count
    drift is a hard MISMATCH (zero tolerance). Hash drift is tolerated up to
    ``tolerance_pct`` percent of the Oracle hashed rows -> WITHIN_TOLERANCE.
    """
    pct = _hash_delta_pct(hash)
    if row_count_delta not in (None, 0):
        return STATUS_MISMATCH, pct
    if hash is None or hash.total_delta == 0:
        return STATUS_OK, pct
    if pct is None:  # oracle_rows == 0 with delta > 0 -> undefined ratio
        return STATUS_MISMATCH, None
    return (STATUS_WITHIN_TOLERANCE if pct <= tolerance_pct else STATUS_MISMATCH), pct


def _dedupe_by_key(kh: pa.Table) -> pa.Table:
    """Collapse duplicate keys to one row (min hash) so the join can't fan out.

    The unique key is a true PK on both sides, so this is defensive: a genuinely
    non-unique key would otherwise multiply the join. Duplicates still surface in
    the count check (``oracle_rows`` vs distinct keys).
    """
    if kh.num_rows == 0:
        return kh
    grouped = kh.group_by("k").aggregate([("h", "min")])
    # group_by names the aggregate 'h_min' and may reorder columns; select by name.
    return pa.table({"k": grouped.column("k"), "h": grouped.column("h_min")})


def _compare(ora: pa.Table, ice: pa.Table) -> HashDelta:
    """Full-outer-join two (k, h) tables and bucket the rows."""
    d = HashDelta(oracle_rows=ora.num_rows, iceberg_rows=ice.num_rows)
    o = _dedupe_by_key(ora).rename_columns(["k", "ho"])
    i = _dedupe_by_key(ice).rename_columns(["k", "hi"])
    joined = o.join(i, keys="k", join_type="full outer")
    ho, hi = joined.column("ho"), joined.column("hi")
    o_null, i_null = pc.is_null(ho), pc.is_null(hi)
    both = pc.and_(pc.invert(o_null), pc.invert(i_null))

    def _count(mask) -> int:
        return pc.sum(pc.cast(mask, pa.int64())).as_py() or 0

    d.only_in_oracle = _count(pc.and_(pc.invert(o_null), i_null))
    d.only_in_iceberg = _count(pc.and_(o_null, pc.invert(i_null)))
    equal = pc.and_(both, pc.equal(ho, hi))
    d.matched = _count(equal)
    d.mismatch = _count(pc.and_(both, pc.invert(pc.equal(ho, hi))))
    return d


# --------------------------------------------------------------------------- #
# Window
# --------------------------------------------------------------------------- #
def _is_numeric_date(tdef: TableDef) -> bool:
    """True when the date column holds a number (e.g. a Julian day), inferred
    from the configured INITIAL value the same way the pipeline renders it."""
    init = (tdef.where_value_of_initial_run or "").strip().upper()
    return bool(_NUMERIC_INIT_RE.match(init)) or "TO_NUMBER" in init or "'J'" in init


def _oracle_date_literal(d: dt.date, numeric: bool) -> str:
    iso = d.strftime("%Y-%m-%d")
    if numeric:
        return f"TO_NUMBER(TO_CHAR(TO_DATE('{iso}', 'YYYY-MM-DD'), 'J'))"
    return f"TO_DATE('{iso}', 'YYYY-MM-DD')"


def _ice_date_literal(d: dt.date, numeric: bool):
    if numeric:
        return d.toordinal() + _JULIAN_OFFSET
    return dt.datetime(d.year, d.month, d.day)


@dataclass
class _Window:
    date_col: Optional[str]            # source column name (UPPER), None for masters
    numeric: bool = False
    oracle_lower: Optional[str] = None  # SQL literal/expression
    oracle_upper: Optional[str] = None
    ice_lower: object = None            # python value for pyarrow compare
    ice_upper: object = None
    note: Optional[str] = None          # why a bound was dropped (for transparency)

    @property
    def date_col_norm(self) -> Optional[str]:
        return _norm(self.date_col) if self.date_col else None

    def oracle_where(self) -> str:
        parts = []
        if self.oracle_lower is not None:
            parts.append(f"{self.date_col} >= {self.oracle_lower}")
        if self.oracle_upper is not None:
            parts.append(f"{self.date_col} <= {self.oracle_upper}")
        return " AND ".join(parts)

    def label(self) -> tuple[str, str]:
        return (str(self.ice_lower) if self.ice_lower is not None else "(min)",
                str(self.ice_upper) if self.ice_upper is not None else "(max)")


def _ceiling_bounds(tdef: TableDef, numeric: bool) -> tuple[Optional[str], object]:
    """Resolve the configured ``where_value_max`` ceiling for both engines.

    Mirrors the pipeline's date ceiling (see ``oracle_extract._date_ceiling_pred``)
    so the DQ window matches the row set the pipeline actually loads. Tables whose
    date column is a *future* scheduled date (e.g. ``APPOINTMENTS.JULIAN_DATE``)
    set this ceiling so neither side scans the whole forward-booking book.

    A rolling SYSDATE ceiling is pinned to *today* and rendered for both engines
    from that one date, so the Oracle predicate and the Arrow filter bound the
    identical day. A literal numeric (Julian) or ``YYYY-MM-DD`` ceiling is rendered
    verbatim. Returns ``(oracle_literal, ice_value)``; ``ice_value`` is ``None``
    when the ceiling is an opaque expression that can't be evaluated lake-side.
    """
    raw = (tdef.where_value_max or "").strip()
    if not raw:
        return None, None
    if _NOW_EXPR_RE.search(raw):
        today = now_local().date()
        return _oracle_date_literal(today, numeric), _ice_date_literal(today, numeric)
    if numeric and _NUMERIC_INIT_RE.match(raw):
        return raw, float(raw)
    if _DATE_ONLY_RE.match(raw):
        d = dt.datetime.strptime(raw, "%Y-%m-%d").date()
        return _oracle_date_literal(d, numeric), _ice_date_literal(d, numeric)
    # Opaque expression: bound the Oracle pull, leave the lake side open.
    return raw, None


def _apply_upper(win: _Window, ora: Optional[str], ice, ceil_ora: Optional[str], ceil_ice) -> None:
    """Set ``win``'s upper bound to the tighter of the requested bound and the
    configured ceiling. Either may be absent; matching Oracle/Arrow forms stay
    paired so the two engines bound the same instant."""
    options = [(o, i) for (o, i) in ((ora, ice), (ceil_ora, ceil_ice)) if i is not None]
    if options:
        win.oracle_upper, win.ice_upper = min(options, key=lambda oi: oi[1])
    elif ceil_ora is not None:
        # Opaque ceiling expression with no Arrow-comparable value: bound the
        # source pull but leave the lake side open.
        win.oracle_upper = ceil_ora


def _make_window(
    tdef: TableDef,
    control_entry: dict,
    since: dt.date,
    until: Optional[dt.date],
) -> _Window:
    """Resolve the shared [since .. until] window for one (table, branch).

    Lower bound: ``since`` (default Jan 1, this year). Upper bound: ``until`` if
    given, else the branch's last-run date watermark from the Postgres
    ``control_state`` table (via ``ControlStore``/``MetaStore``), in either case
    capped by the table's configured ``where_value_max`` ceiling
    (e.g. ``APPOINTMENTS.JULIAN_DATE <= today``) so a future-dated column never
    pulls the whole forward-booking book. A master table (no date column) gets no
    window (full compare). A helper-driven table whose watermark is the *helper's*
    column -- not its own date column -- drops the watermark upper bound (but the
    ceiling, if any, still applies).
    """
    if not tdef.where_date_column:
        return _Window(date_col=None, note="no date column (full compare)")

    numeric = _is_numeric_date(tdef)
    win = _Window(
        date_col=tdef.where_date_column,
        numeric=numeric,
        oracle_lower=_oracle_date_literal(since, numeric),
        ice_lower=_ice_date_literal(since, numeric),
    )
    ceil_ora, ceil_ice = _ceiling_bounds(tdef, numeric)

    if until is not None:
        _apply_upper(win, _oracle_date_literal(until, numeric),
                     _ice_date_literal(until, numeric), ceil_ora, ceil_ice)
        return win

    if tdef.is_helper_driven:
        _apply_upper(win, None, None, ceil_ora, ceil_ice)
        win.note = ("helper-driven: watermark is the helper's column; "
                    + ("upper bound is the configured ceiling"
                       if win.oracle_upper is not None else "upper bound skipped"))
        return win

    wm = (control_entry or {}).get("last_date")
    if not wm or wm.get("value") is None:
        _apply_upper(win, None, None, ceil_ora, ceil_ice)
        win.note = ("no last-run watermark; upper bound is the configured ceiling"
                    if win.oracle_upper is not None else
                    "no last-run watermark; upper bound is open")
        return win

    value, kind = wm["value"], wm.get("kind", "datetime")
    if kind == "number" and numeric:
        _apply_upper(win, str(value), float(value), ceil_ora, ceil_ice)
    elif kind in ("datetime", "string") and not numeric:
        try:
            ice_wm = dt.datetime.strptime(str(value), _WM_DT_FORMAT)
            _apply_upper(win, f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                         ice_wm, ceil_ora, ceil_ice)
        except ValueError:
            _apply_upper(win, None, None, ceil_ora, ceil_ice)
            win.note = f"unparseable watermark {value!r}; upper bound capped at ceiling only"
    else:
        _apply_upper(win, None, None, ceil_ora, ceil_ice)
        win.note = f"watermark kind {kind!r} mismatches date column; upper bound capped at ceiling only"
    return win


def _apply_window_arrow(tbl: pa.Table, win: _Window) -> pa.Table:
    """Filter an Arrow table to the window on the (normalized) date column."""
    if win.date_col is None or (win.ice_lower is None and win.ice_upper is None):
        return tbl
    name = _resolve_actual(tbl.column_names, win.date_col_norm)
    if name is None:
        return tbl  # date column absent on this side -> can't window, compare all
    col = tbl.column(name)
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()
    if pa.types.is_timestamp(col.type) and col.type.tz is not None:
        col = col.cast(pa.timestamp("us"))
    mask = None
    if win.ice_lower is not None:
        mask = pc.greater_equal(col, pa.scalar(win.ice_lower, type=col.type))
    if win.ice_upper is not None:
        upper = pc.less_equal(col, pa.scalar(win.ice_upper, type=col.type))
        mask = upper if mask is None else pc.and_(mask, upper)
    return tbl.filter(mask) if mask is not None else tbl


# --------------------------------------------------------------------------- #
# Column resolution (normalized name <-> each side's actual casing)
# --------------------------------------------------------------------------- #
def _resolve_actual(actual_names: Iterable[str], norm: Optional[str]) -> Optional[str]:
    if norm is None:
        return None
    for a in actual_names:
        if _norm(a) == norm:
            return a
    return None


def _business_norms(actual_names: Iterable[str], injected: set[str]) -> set[str]:
    # dlt's internal bookkeeping columns (_dlt_id, _dlt_load_id, ...) are stamped
    # in by the pipeline at load time and exist only in the lake. They can't come
    # from the source, so they must never count as business columns -- keeping
    # them out of the row fingerprint *and* the column-drift report.
    return {_norm(a) for a in actual_names if not a.startswith("_dlt")} - injected


# --------------------------------------------------------------------------- #
# Iceberg (lake) side
# --------------------------------------------------------------------------- #
def dataset_root(settings: Settings) -> Path:
    """Local ``<bucket>/<dataset>`` directory for the configured destination."""
    pr = urlparse(settings.destination_bucket_url)
    if pr.scheme not in ("", "file"):
        raise SystemExit(
            f"DQ reads local Iceberg only; destination is {settings.destination_bucket_url!r}")
    base = Path(url2pathname(pr.path)) if pr.scheme == "file" else Path(settings.destination_bucket_url)
    return base / settings.dataset_name


def _latest_metadata(table_dir: Path) -> Optional[Path]:
    metas = list((table_dir / "metadata").glob("*.metadata.json"))
    if not metas:
        return None

    def ver(p: Path) -> int:
        m = re.match(r"(\d+)-", p.name)
        return int(m.group(1)) if m else -1

    return max(metas, key=lambda p: (ver(p), p.stat().st_mtime))


def _iceberg_uri(path: Path) -> str:
    # dlt writes file://<drive>/... (drive as netloc); match it so pyiceberg
    # resolves the path on Windows. On POSIX this is a normal file:///abs path.
    return "file://" + str(path.resolve()).replace("\\", "/")


def open_lake_table(root: Path, table: str):
    """Return a read-only pyiceberg StaticTable for ``table`` (or None if unloaded)."""
    meta = _latest_metadata(root / table)
    if meta is None:
        return None
    from pyiceberg.table import StaticTable

    return StaticTable.from_metadata(_iceberg_uri(meta))


def _lake_scan_batches(static_table, branch: int, columns: list[str]) -> Iterator[pa.Table]:
    """Stream the branch partition's ``columns`` as Arrow tables (partition-pruned).

    ``branch`` is the numeric BRANCH_ID value (see BranchConfig.id)."""
    from pyiceberg.expressions import EqualTo

    branch_field = _resolve_actual(
        (f.name for f in static_table.schema().fields), _norm("BRANCH_ID")) or "branch_id"
    scan = static_table.scan(
        row_filter=EqualTo(branch_field, branch), selected_fields=tuple(columns))
    for rb in scan.to_arrow_batch_reader():
        yield pa.Table.from_batches([rb])


# --------------------------------------------------------------------------- #
# Source side (live Oracle  or  staged parquet for --self-test)
# --------------------------------------------------------------------------- #
def _oracle_select(tdef: TableDef, win: _Window) -> str:
    if tdef.key_is_expression:
        base = f"SELECT t.*, ({tdef.unique_key}) AS {tdef.derived_key_alias} FROM {tdef.table} t"
        where = win.oracle_where().replace(f"{win.date_col} ", f"t.{win.date_col} ") if win.date_col else ""
    else:
        base = f"SELECT * FROM {tdef.table}"
        where = win.oracle_where()
    return base + (f" WHERE {where}" if where else "")


def _oracle_count_sql(tdef: TableDef, win: _Window) -> str:
    where = win.oracle_where()
    return f"SELECT COUNT(*) FROM {tdef.table}" + (f" WHERE {where}" if where else "")


def _oracle_business_norms(conn, query: str, injected: set[str]) -> tuple[set[str], list[str]]:
    """Column names of the source query without fetching data (ROWNUM<1 peek)."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM ({query}) WHERE ROWNUM < 1")
        names = [d.name for d in cur.description]
    finally:
        cur.close()
    return _business_norms(names, injected), names


def _oracle_count(conn, sql: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return int(cur.fetchone()[0])
    finally:
        cur.close()


def _oracle_batches(conn, query: str, fetch_batch_size: int) -> Iterator[pa.Table]:
    from .oracle_extract import (
        _cursor_arrow_stream,
        _is_arrow_unsupported,
        arrow_safe_rewrite,
    )

    yielded = False
    try:
        for odf in conn.fetch_df_batches(query, size=fetch_batch_size):
            batch = pa.table(odf)
            if batch.num_rows:
                yielded = True
                yield batch
        return
    except Exception as exc:  # noqa: BLE001
        # DPY-3030 (e.g. a ROWID column) fires before the first batch; anything
        # else -- or a mid-stream failure -- is a real error for this check unit.
        if yielded or not _is_arrow_unsupported(exc):
            raise

    # Retry the fast path with Arrow-unsupported columns cast server-side
    # (ROWIDTOCHAR), mirroring the extract's fetch_and_stage behavior.
    rewritten = None
    try:
        rewritten = arrow_safe_rewrite(conn, query)
    except Exception:  # noqa: BLE001 - peek is best-effort; cursor path below
        rewritten = None
    if rewritten is not None:
        for odf in conn.fetch_df_batches(rewritten, size=fetch_batch_size):
            batch = pa.table(odf)
            if batch.num_rows:
                yield batch
        return

    # Last resort: row-by-row cursor stream (handles ROWID etc. natively).
    cur = conn.cursor()
    try:
        cur.arraysize = fetch_batch_size
        cur.prefetchrows = fetch_batch_size + 1
        cur.execute(query)
        for batch in _cursor_arrow_stream(cur, fetch_batch_size):
            if batch.num_rows:
                yield batch
    finally:
        cur.close()


def _staged_file(settings: Settings, tdef: TableDef, branch: str) -> Optional[Path]:
    f = settings.staging_dir / tdef.dataset_table_name / f"{branch}.parquet"
    return f if f.exists() else None


def _staged_batches(path: Path, win: _Window, batch_rows: int = 100_000) -> Iterator[pa.Table]:
    pf = pq.ParquetFile(path)
    for rb in pf.iter_batches(batch_size=batch_rows):
        tbl = _apply_window_arrow(pa.Table.from_batches([rb]), win)
        if tbl.num_rows:
            yield tbl


# --------------------------------------------------------------------------- #
# Per-(table, branch) check
# --------------------------------------------------------------------------- #
@dataclass
class DqResult:
    table: str
    source_table: str
    branch: str
    window_start: str = ""
    window_end: str = ""
    date_column: Optional[str] = None
    window_note: Optional[str] = None
    oracle_row_count: Optional[int] = None
    iceberg_row_count: Optional[int] = None
    hash: Optional[HashDelta] = None
    hash_delta_pct: Optional[float] = None
    cols_only_oracle: list[str] = field(default_factory=list)
    cols_only_iceberg: list[str] = field(default_factory=list)
    status: str = "OK"
    error: Optional[str] = None

    @property
    def row_count_delta(self) -> Optional[int]:
        if self.oracle_row_count is None or self.iceberg_row_count is None:
            return None
        return self.oracle_row_count - self.iceberg_row_count


def _accumulate_kh(
    batches: Iterator[pa.Table], key_norm: list[str], common: list[str]
) -> tuple[pa.Table, int]:
    """Hash a stream of windowed batches into one (k, h) table + a row count.

    ``common`` (normalized, sorted) and ``key_norm`` are resolved to each batch's
    actual column names, so the source's UPPER columns and the lake's lower_snake
    columns hash the identical column set in the identical order.
    """
    key_parts, hash_parts, rows = [], [], 0
    for batch in batches:
        names = batch.column_names
        key_actual = [_resolve_actual(names, k) for k in key_norm]
        payload_actual = [_resolve_actual(names, c) for c in common]
        if any(k is None for k in key_actual):
            missing = [k for k, a in zip(key_norm, key_actual) if a is None]
            raise KeyError(f"key column(s) {missing} absent from source/lake batch")
        keys, hashes = _key_and_hash(
            batch, key_actual, [c for c in payload_actual if c is not None])
        key_parts.append(keys)
        hash_parts.append(hashes)
        rows += batch.num_rows
    if not key_parts:
        empty = pa.table({"k": pa.array([], pa.string()), "h": pa.array([], pa.binary(16))})
        return empty, 0
    kh = pa.table({"k": pa.chunked_array(key_parts), "h": pa.chunked_array(hash_parts)})
    return kh, rows


def check_unit(
    tdef: TableDef,
    branch: BranchConfig,
    settings: Settings,
    static_table,
    control_entry: dict,
    since: dt.date,
    until: Optional[dt.date],
    do_hash: bool,
    conn=None,
    self_test: bool = False,
) -> DqResult:
    """Run both checks for one (table, branch) and return a populated DqResult."""
    injected = _injected_norms(settings)
    win = _make_window(tdef, control_entry, since, until)
    lo, hi = win.label()
    res = DqResult(
        table=tdef.dataset_table_name, source_table=tdef.table, branch=branch.key,
        window_start=lo, window_end=hi, date_column=win.date_col, window_note=win.note,
    )

    lake_cols = {f.name for f in static_table.schema().fields} if static_table else set()
    lake_business = _business_norms(lake_cols, injected)

    try:
        # ---- source: column set + windowed COUNT(*) ---------------------------
        if self_test:
            staged = _staged_file(settings, tdef, branch.key)
            if staged is None:
                res.status = "SKIPPED"
                res.error = "no staged parquet for this branch (--self-test)"
                return res
            src_names = pq.read_schema(staged).names
            src_business = _business_norms(src_names, injected)
            if not do_hash:  # the hash path sets this from the rows it pulls
                res.oracle_row_count = sum(b.num_rows for b in _staged_batches(staged, win))
        else:
            query = _oracle_select(tdef, win)
            src_business, _ = _oracle_business_norms(conn, query, injected)
            if not do_hash:  # the hash path derives the count from the rows it pulls
                res.oracle_row_count = _oracle_count(conn, _oracle_count_sql(tdef, win))

        if static_table is None:
            res.iceberg_row_count = 0
        common = sorted(src_business & lake_business)
        res.cols_only_oracle = sorted(src_business - lake_business)
        res.cols_only_iceberg = sorted(lake_business - src_business)

        # ---- hash delta -------------------------------------------------------
        if do_hash:
            key_norm = [_norm(k) for k in tdef.key_columns]
            if self_test:
                src_batches = _staged_batches(_staged_file(settings, tdef, branch.key), win)
            else:
                src_batches = _oracle_batches(
                    conn, _oracle_select(tdef, win), branch.fetch_batch_size)
            src_kh, src_rows = _accumulate_kh(src_batches, key_norm, common)
            # The windowed hash SELECT returns exactly the windowed COUNT(*) row
            # set, so take the count from the rows pulled (one fewer full scan)
            # and keep it consistent with the rows actually hashed.
            res.oracle_row_count = src_rows

            if static_table is not None:
                scan_cols = sorted(set(common) | set(key_norm) | (
                    {win.date_col_norm} if win.date_col_norm else set()))
                scan_actual = [a for a in (
                    _resolve_actual(lake_cols, c) for c in scan_cols) if a]
                ice_batches = (_apply_window_arrow(b, win)
                               for b in _lake_scan_batches(static_table, branch.id, scan_actual))
                ice_kh, ice_rows = _accumulate_kh(ice_batches, key_norm, common)
            else:
                ice_kh, ice_rows = pa.table(
                    {"k": pa.array([], pa.string()), "h": pa.array([], pa.binary(16))}), 0

            res.iceberg_row_count = ice_rows
            delta = _compare(src_kh, ice_kh)
            delta.columns = len(common)
            res.hash = delta
        else:
            # counts-only: count the lake partition in the window without hashing
            if static_table is not None:
                res.iceberg_row_count = _lake_window_count(static_table, branch.id, win)

        # ---- status -----------------------------------------------------------
        res.status, res.hash_delta_pct = classify_status(
            res.row_count_delta, res.hash, settings.dq_hash_delta_tolerance_pct)
    except Exception as exc:  # noqa: BLE001 - isolate per-unit failures
        res.status = "ERROR"
        res.error = f"{type(exc).__name__}: {exc}"
        log.error("[%s/%s] DQ check failed: %s", tdef.dataset_table_name, branch.key, exc)
    return res


def _lake_window_count(static_table, branch: int, win: _Window) -> int:
    """Count rows in the branch partition within the window (counts-only path)."""
    cols = [win.date_col_norm] if win.date_col_norm else [_norm("BRANCH_ID")]
    lake_cols = {f.name for f in static_table.schema().fields}
    actual = [a for a in (_resolve_actual(lake_cols, c) for c in cols) if a] or None
    total = 0
    for b in _lake_scan_batches(static_table, branch, actual or list(lake_cols)[:1]):
        total += _apply_window_arrow(b, win).num_rows
    return total


def _fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 3600:d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


class _DqProgress:
    """Cheap per-unit + heartbeat progress for a DQ run.

    ``DQ-UNIT`` lines are logged as each (table, branch) completes; a background
    daemon thread logs a ``DQ-PROGRESS`` heartbeat every ``interval_s``. Both go
    to the ``etl.dq`` logger so they land in the run log with timestamps; the GUI
    parses them into a live dashboard. All updates are integer counters under a
    short lock -- no per-unit measurement cost.
    """

    def __init__(self, total: int, *, interval_s: float = 5.0,
                 enabled: bool = True, logger: Optional[logging.Logger] = None):
        self.total = total
        self.interval_s = max(1.0, float(interval_s))
        self.enabled = enabled
        self.log = logger or log
        self._lock = threading.Lock()
        self._done = self._ok = self._tol = self._mismatch = self._err = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_t = 0.0

    def start(self) -> "_DqProgress":
        self._start_t = time.perf_counter()
        if self.enabled:
            self._thread = threading.Thread(
                target=self._run, name="dq-progress", daemon=True)
            self._thread.start()
        return self

    def record(self, res: "DqResult") -> None:
        with self._lock:
            self._done += 1
            if res.status == STATUS_OK:
                self._ok += 1
            elif res.status == STATUS_WITHIN_TOLERANCE:
                self._tol += 1
            elif res.status == "ERROR":
                self._err += 1
            elif res.status == STATUS_MISMATCH:
                self._mismatch += 1
        self.log.info(self._unit_line(res))

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 2.0)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self.log.info(self._heartbeat_line(time.perf_counter() - self._start_t))

    @staticmethod
    def _n(v) -> str:
        return "-" if v is None else str(v)

    def _unit_line(self, res: "DqResult") -> str:
        h = res.hash
        pct = "-" if res.hash_delta_pct is None else f"{res.hash_delta_pct:.2f}"
        return (f"DQ-UNIT {res.table}/{res.branch} | "
                f"ora={self._n(res.oracle_row_count)} ice={self._n(res.iceberg_row_count)} "
                f"cnt={self._n(res.row_count_delta)} | "
                f"match={self._n(h.matched if h else None)} "
                f"delta={self._n(h.total_delta if h else None)} pct={pct} | {res.status}")

    def _heartbeat_line(self, elapsed: float) -> str:
        with self._lock:
            done, ok, tol, mm, err = (
                self._done, self._ok, self._tol, self._mismatch, self._err)
        return (f"DQ-PROGRESS {_fmt_elapsed(elapsed)} | units {done}/{self.total} | "
                f"ok {ok} tol {tol} mismatch {mm} err {err}")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run_dq(
    tables: list[TableDef],
    branches: list[BranchConfig],
    settings: Settings,
    control: dict,
    since: dt.date,
    until: Optional[dt.date],
    do_hash: bool = True,
    self_test: bool = False,
    max_workers: Optional[int] = None,
) -> list[DqResult]:
    """Run DQ for every (table, branch), one Oracle connection per branch.

    Branches run in parallel (each on its own connection / staged files); within a
    branch the tables run sequentially. Iceberg StaticTables are opened once up
    front and shared read-only across the branch workers.
    """
    root = dataset_root(settings)
    lake: dict[str, object] = {t.dataset_table_name: open_lake_table(root, t.dataset_table_name)
                               for t in tables}
    for name, st in lake.items():
        if st is None:
            log.warning("[%s] not present in the lake yet; Iceberg side will be 0", name)

    if not self_test:
        from .oracle_extract import ensure_oracle_client
        ensure_oracle_client(settings)

    results: list[DqResult] = []
    lock = threading.Lock()

    progress = _DqProgress(
        total=len(tables) * len(branches),
        interval_s=settings.progress_interval_s,
        enabled=settings.progress_enabled,
    ).start()

    def run_branch(branch: BranchConfig) -> list[DqResult]:
        conn = None
        try:
            if not self_test:
                import oracledb

                conn = oracledb.connect(
                    user=branch.username, password=branch.password,
                    dsn=branch.dsn(settings.dsn_mode),
                    tcp_connect_timeout=settings.pool_acquire_timeout_s)
            out = []
            for tdef in tables:
                entry = (control.get(tdef.dataset_table_name, {}) or {}).get(branch.key, {})
                res_u = check_unit(
                    tdef, branch, settings, lake[tdef.dataset_table_name], entry,
                    since, until, do_hash, conn=conn, self_test=self_test)
                progress.record(res_u)
                out.append(res_u)
            return out
        except Exception as exc:  # noqa: BLE001 - a dead branch fails only its own rows
            log.error("[%s] branch failed: %s", branch.key, exc)
            errs = [DqResult(table=t.dataset_table_name, source_table=t.table,
                             branch=branch.key, status="ERROR",
                             error=f"{type(exc).__name__}: {exc}") for t in tables]
            for r in errs:
                progress.record(r)
            return errs
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass

    workers = max(1, min(max_workers or settings.max_branch_workers, len(branches)))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dq") as pool:
        futs = {pool.submit(run_branch, b): b for b in branches}
        for fut in as_completed(futs):
            with lock:
                results.extend(fut.result())
    progress.stop()
    return results


# --------------------------------------------------------------------------- #
# Output: Iceberg table + console + CSV
# --------------------------------------------------------------------------- #
def _result_rows(results: list[DqResult], settings: Settings, run_id: str) -> list[dict]:
    now = now_local()
    rows = []
    for r in results:
        h = r.hash
        rows.append({
            "pipeline_run_id": run_id,
            "check_time": now,
            "table_name": r.table,
            "source_table": r.source_table,
            "branch_id": r.branch,
            "date_column": r.date_column,
            "window_start": r.window_start,
            "window_end": r.window_end,
            "window_note": r.window_note,
            "oracle_row_count": r.oracle_row_count,
            "iceberg_row_count": r.iceberg_row_count,
            "row_count_delta": r.row_count_delta,
            "hash_columns": h.columns if h else None,
            "oracle_hashed_rows": h.oracle_rows if h else None,
            "iceberg_hashed_rows": h.iceberg_rows if h else None,
            "hash_matched": h.matched if h else None,
            "hash_only_in_oracle": h.only_in_oracle if h else None,
            "hash_only_in_iceberg": h.only_in_iceberg if h else None,
            "hash_mismatch": h.mismatch if h else None,
            "hash_total_delta": h.total_delta if h else None,
            "hash_delta_pct": r.hash_delta_pct,
            "columns_only_in_oracle": ",".join(r.cols_only_oracle) or None,
            "columns_only_in_iceberg": ",".join(r.cols_only_iceberg) or None,
            "status": r.status,
            "error_details": r.error,
        })
    return rows


# Lock the schema with explicit hints: many columns are all-null on a clean run
# (e.g. error_details, the hash_* columns under --no-hash), which dlt otherwise
# can't type, drifting the Iceberg schema run-to-run.
_DQ_HINTS = {
    # Naive local wall-clock, like the pipeline's other generated time columns:
    # timezone=False stops dlt tagging the value UTC (which shifts it for any
    # UTC+offset reader). See iceberg_load._naive_ts_hint.
    "check_time": {"data_type": "timestamp", "timezone": False, "precision": 6},
    "pipeline_run_id": {"data_type": "text"},
    "table_name": {"data_type": "text"},
    "source_table": {"data_type": "text"},
    "branch_id": {"data_type": "text"},
    "date_column": {"data_type": "text"},
    "window_start": {"data_type": "text"},
    "window_end": {"data_type": "text"},
    "window_note": {"data_type": "text"},
    "oracle_row_count": {"data_type": "bigint"},
    "iceberg_row_count": {"data_type": "bigint"},
    "row_count_delta": {"data_type": "bigint"},
    "hash_columns": {"data_type": "bigint"},
    "oracle_hashed_rows": {"data_type": "bigint"},
    "iceberg_hashed_rows": {"data_type": "bigint"},
    "hash_matched": {"data_type": "bigint"},
    "hash_only_in_oracle": {"data_type": "bigint"},
    "hash_only_in_iceberg": {"data_type": "bigint"},
    "hash_mismatch": {"data_type": "bigint"},
    "hash_total_delta": {"data_type": "bigint"},
    "hash_delta_pct": {"data_type": "double"},
    "columns_only_in_oracle": {"data_type": "text"},
    "columns_only_in_iceberg": {"data_type": "text"},
    "status": {"data_type": "text"},
    "error_details": {"data_type": "text"},
}


def write_results_postgres(results: list[DqResult], settings: Settings, run_id: str, store=None) -> str:
    """Append the DQ results to the Postgres ``etl_dq_results`` table."""
    from .metastore import MetaStore

    rows = _result_rows(results, settings, run_id)
    if not rows:
        return _TABLE_NAME
    store = store or MetaStore(settings.postgres)
    store.ensure_schema()
    store.append_dq_results(rows)
    return _TABLE_NAME


def render_summary(results: list[DqResult], do_hash: bool) -> str:
    """A compact, aligned console table of the per-(table, branch) results."""
    results = sorted(results, key=lambda r: (r.table, r.branch))
    headers = ["TABLE", "BRANCH", "ORA_ROWS", "ICE_ROWS", "CNT_DELTA"]
    if do_hash:
        headers += ["MATCH", "ONLY_ORA", "ONLY_ICE", "MISMATCH", "HASH_DELTA", "TOL%"]
    headers += ["STATUS"]

    def cell(v) -> str:
        if v is None:
            return "-"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)

    def pct_cell(v) -> str:
        return "-" if v is None else f"{v:.2f}%"

    table = [headers]
    for r in results:
        row = [r.table, r.branch, cell(r.oracle_row_count), cell(r.iceberg_row_count),
               cell(r.row_count_delta)]
        if do_hash:
            h = r.hash
            row += [cell(h.matched if h else None), cell(h.only_in_oracle if h else None),
                    cell(h.only_in_iceberg if h else None), cell(h.mismatch if h else None),
                    cell(h.total_delta if h else None), pct_cell(r.hash_delta_pct)]
        row += [r.status]
        table.append(row)

    widths = [max(len(row[i]) for row in table) for i in range(len(headers))]
    lines = []
    for j, row in enumerate(table):
        lines.append("  ".join(c.ljust(widths[i]) if i < 2 else c.rjust(widths[i])
                               for i, c in enumerate(row)))
        if j == 0:
            lines.append("  ".join("-" * widths[i] for i in range(len(headers))))

    ok = sum(1 for r in results if r.status == "OK")
    tol = sum(1 for r in results if r.status == STATUS_WITHIN_TOLERANCE)
    mism = sum(1 for r in results if r.status == "MISMATCH")
    err = sum(1 for r in results if r.status == "ERROR")
    skip = sum(1 for r in results if r.status == "SKIPPED")
    lines.append("")
    lines.append(f"{len(results)} unit(s): {ok} OK, {tol} WITHIN_TOLERANCE, "
                 f"{mism} MISMATCH, {err} ERROR, {skip} SKIPPED")
    notes = {(r.table): r.window_note for r in results if r.window_note}
    for tbl, note in sorted(notes.items()):
        lines.append(f"  note [{tbl}]: {note}")
    return "\n".join(lines)


def write_csv(results: list[DqResult], path: Path, run_id: str, settings: Settings) -> None:
    import csv

    rows = _result_rows(results, settings, run_id)
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for row in rows:
            w.writerow({k: ("" if v is None else v) for k, v in row.items()})
