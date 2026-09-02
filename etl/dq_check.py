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
from functools import lru_cache
from hashlib import blake2b
from pathlib import Path
from typing import Iterable, Iterator, Optional
from urllib.parse import urlparse
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)

from .config import (
    HELPER_RESERVED_COLUMNS,
    BranchConfig,
    Settings,
    TableDef,
    now_local,
)

log = logging.getLogger("etl.dq")

# The lake's identifier normalizer. DQ must resolve a source column to the same
# name dlt wrote, so it borrows dlt's convention rather than reimplementing it.
_NAMING = SnakeCaseNamingConvention()

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


@lru_cache(maxsize=8192)
def _norm(name: str) -> str:
    """Lower-snake a column name the same way dlt normalizes lake identifiers.

    Delegates to dlt's own naming convention instead of approximating it. A
    hand-rolled "non-alphanumeric -> _" pass gets digit/letter boundaries wrong:
    dlt writes ``STAFF_NAME_1B`` as ``staff_name_1_b`` and ``COL2A`` as
    ``col2_a``, so the approximation produced a name that exists on neither
    side. Such a column silently dropped out of ``common`` and was never
    compared -- and inside a ``unique_key`` it would have broken the join
    outright, bucketing every row as only-in-oracle plus only-in-iceberg.
    """
    return _NAMING.normalize_identifier(name)


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


def _delta_pct(delta: Optional[int], base: Optional[int]) -> Optional[float]:
    """``|delta|`` as a percent of ``base`` (None when the ratio is undefined).

    ``0.0`` for a clean delta, ``None`` when there is no delta to measure at all
    or when ``base`` is 0 yet a delta exists (an undefined ratio -- treated as a
    mismatch by ``classify_status``).
    """
    if delta is None:
        return None
    if delta == 0:
        return 0.0
    if not base or base <= 0:
        return None
    return 100.0 * abs(delta) / base


def _hash_delta_pct(hash: Optional[HashDelta]) -> Optional[float]:
    """Percent of Oracle hashed rows that diverged (None when undefined)."""
    if hash is None:
        return None
    return _delta_pct(hash.total_delta, hash.oracle_rows)


def classify_status(
    row_count_delta: Optional[int],
    oracle_row_count: Optional[int],
    hash: Optional[HashDelta],
    tolerance_pct: float,
) -> tuple[str, Optional[float], Optional[float]]:
    """Return ``(status, row_count_delta_pct, hash_delta_pct)`` for a unit.

    ERROR is decided by the caller (a check that could not complete). Both drifts
    -- row-count and row-hash -- are measured as a percent of the unit's Oracle
    rows and tolerated up to ``tolerance_pct`` -> WITHIN_TOLERANCE; whichever is
    larger decides, and either one above the tolerance is a MISMATCH. A drift
    whose ratio is undefined (Oracle contributed 0 rows) is always a MISMATCH.
    """
    count_pct = _delta_pct(row_count_delta, oracle_row_count)
    hash_pct = _hash_delta_pct(hash)
    # None/0 deltas are not drift: a missing count leaves that side unmeasured.
    drifts = ([count_pct] if row_count_delta else []) + (
        [hash_pct] if hash is not None and hash.total_delta else [])
    if not drifts:
        return STATUS_OK, count_pct, hash_pct
    if any(p is None for p in drifts):  # base 0 with a delta -> undefined ratio
        return STATUS_MISMATCH, count_pct, hash_pct
    status = (STATUS_WITHIN_TOLERANCE if max(drifts) <= tolerance_pct
              else STATUS_MISMATCH)
    return status, count_pct, hash_pct


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

    def oracle_where(self, qualifier: str = "") -> str:
        """Window predicates on the date column, qualified for the FROM alias."""
        parts = []
        col = f"{qualifier}{self.date_col}"
        if self.oracle_lower is not None:
            parts.append(f"{col} >= {self.oracle_lower}")
        if self.oracle_upper is not None:
            parts.append(f"{col} <= {self.oracle_upper}")
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
        # The watermark belongs to the helper's column, not this one, so it
        # cannot bound *this* date column on either side. The source is bounded
        # instead by the helper predicates (see ``_coverage_predicates``), which
        # reproduce the join the pipeline loads through; the lake needs no upper
        # bound because it only ever received that same subset.
        _apply_upper(win, None, None, ceil_ora, ceil_ice)
        win.note = ("helper-driven: source scoped by the helper join"
                    + (" and the configured ceiling"
                       if win.oracle_upper is not None else ""))
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


def _pad_bound(value, widen: int):
    """Move a window bound outward by one day (or one unit, for a numeric date)."""
    if isinstance(value, dt.datetime):
        return value + dt.timedelta(days=widen)
    if isinstance(value, dt.date):
        return value + dt.timedelta(days=widen)
    if isinstance(value, (int, float)):
        return value + widen
    return None


def _window_row_filter(static_table, win: Optional["_Window"]):
    """A deliberately loose Iceberg predicate for the window, or None.

    Without this the scan's only predicate is the branch, so DQ decoded whole
    branch partitions and then discarded most rows in Arrow -- ``appointments``
    read 129.7M rows per branch to compare 21.1M, and ``docl``/``doc`` read ~6x
    what they compared. Pushing the date bounds into the scan lets Iceberg skip
    files on their column statistics before any of it is decoded.

    The bounds are padded outward by a day so this can only ever be a *pruning
    hint*: it must never drop a row that ``_apply_window_arrow`` would keep, and
    that filter still decides the exact row set. The padding also absorbs the
    timestamp/timezone edge cases the Arrow filter handles explicitly (a
    tz-aware lake column vs the source's naive wall clock).
    """
    if win is None or win.date_col is None:
        return None
    if win.ice_lower is None and win.ice_upper is None:
        return None
    field = _resolve_actual(
        (f.name for f in static_table.schema().fields), win.date_col_norm)
    if field is None:
        return None

    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

    preds = []
    lo = _pad_bound(win.ice_lower, -1) if win.ice_lower is not None else None
    hi = _pad_bound(win.ice_upper, +1) if win.ice_upper is not None else None
    if lo is not None:
        preds.append(GreaterThanOrEqual(field, lo))
    if hi is not None:
        preds.append(LessThanOrEqual(field, hi))
    if not preds:
        return None
    out = preds[0]
    for extra in preds[1:]:
        out = And(out, extra)
    return out


def _lake_scan_batches(static_table, branch: int, columns: list[str],
                       snapshot: Optional["_SnapshotScope"] = None,
                       win: Optional["_Window"] = None) -> Iterator[pa.Table]:
    """Stream the branch partition's ``columns`` as Arrow tables (partition-pruned).

    ``branch`` is the numeric BRANCH_ID value (see BranchConfig.id). ``snapshot``
    pins an append-only snapshot table to one version stamp (see
    ``_SnapshotScope``); ``win`` prunes files to the window (see
    ``_window_row_filter``) -- both are narrowing hints, never the final say.
    """
    from pyiceberg.expressions import And, EqualTo

    branch_field = _resolve_actual(
        (f.name for f in static_table.schema().fields), _norm("BRANCH_ID")) or "branch_id"
    row_filter = EqualTo(branch_field, branch)
    if snapshot is not None:
        # version_date is an identity partition field, so pinning it prunes whole
        # files; the version equality then picks the exact run within that day
        # (a table can be snapshotted more than once a day).
        if snapshot.date_field is not None:
            row_filter = And(row_filter, EqualTo(snapshot.date_field, snapshot.version.date()))
        row_filter = And(row_filter, EqualTo(snapshot.field, snapshot.version))
    try:
        window_pred = _window_row_filter(static_table, win)
    except Exception as exc:  # noqa: BLE001 - pruning is optional; never fail on it
        log.debug("window pushdown unavailable (%s); scanning the branch partition", exc)
        window_pred = None
    if window_pred is not None:
        row_filter = And(row_filter, window_pred)
    scan = static_table.scan(row_filter=row_filter, selected_fields=tuple(columns))
    for rb in scan.to_arrow_batch_reader():
        yield pa.Table.from_batches([rb])


@dataclass
class _SnapshotScope:
    """Which version of an append-only snapshot table the lake side compares.

    Snapshot tables are appended, never merged: every run stamps a full copy of
    the source with a run timestamp (``settings.snapshot_version_column``) and
    adds it to the table, so the lake accumulates one generation per run. The
    source only ever holds the *current* generation, so comparing it against the
    whole lake table is meaningless -- ``product_base`` had grown to 14 daily
    copies (35.2M rows for one branch against 2.5M in Oracle), reporting 100%
    drift every night. Pinning the newest version restores an apples-to-apples
    compare of the last snapshot against the source.
    """

    field: str                    # lake column holding the run stamp ("version")
    version: dt.datetime          # the newest stamp in this branch partition
    date_field: Optional[str]     # partition column holding date(version)


def _latest_snapshot_version(static_table, branch: int,
                             settings: Settings) -> Optional["_SnapshotScope"]:
    """Newest snapshot version present in one branch partition, or None.

    Reads the partition summaries first (metadata only, no data files) to find
    the newest ``version_date`` for the branch, then scans just that day's
    ``version`` column for the exact stamp.
    """
    lake_cols = {f.name for f in static_table.schema().fields}
    field = _resolve_actual(lake_cols, _norm(settings.snapshot_version_column))
    if field is None:
        return None
    date_field = _resolve_actual(lake_cols, _norm(settings.snapshot_date_column))
    branch_field = _resolve_actual(lake_cols, _norm("BRANCH_ID")) or "branch_id"

    scope = None
    if date_field is not None:
        try:
            parts = static_table.inspect.partitions().column("partition").combine_chunks()
            days = [r[date_field] for r in parts.to_pylist()
                    if r.get(branch_field) == branch and r.get(date_field) is not None]
            if days:
                scope = _SnapshotScope(field=field, date_field=date_field,
                                       version=dt.datetime.combine(max(days), dt.time()))
        except Exception as exc:  # noqa: BLE001 - metadata shape is pyiceberg's
            log.debug("partition summary unusable (%s); scanning for max version", exc)

    best = None
    if scope is not None:
        # Narrowed to the newest day: scan that partition only.
        from pyiceberg.expressions import And, EqualTo

        batches = static_table.scan(
            row_filter=And(EqualTo(branch_field, branch),
                           EqualTo(date_field, scope.version.date())),
            selected_fields=(field,)).to_arrow_batch_reader()
        batches = (pa.Table.from_batches([rb]) for rb in batches)
    else:
        batches = _lake_scan_batches(static_table, branch, [field])
    for b in batches:
        if b.num_rows:
            m = pc.max(b.column(field)).as_py()
            if m is not None and (best is None or m > best):
                best = m
    if best is None:
        return None
    return _SnapshotScope(field=field, version=best,
                          date_field=date_field if scope is not None else None)


# --------------------------------------------------------------------------- #
# Source side (live Oracle  or  staged parquet for --self-test)
# --------------------------------------------------------------------------- #
def _coverage_predicates(tdef: TableDef, control_entry: dict) -> tuple[list[str], Optional[str]]:
    """Predicates restricting the source to rows the pipeline can ever load.

    A helper-driven table is not extracted from its own table alone: the
    pipeline inner-joins it to a parent and filters on the *parent's* columns
    (``oracle_extract.build_query`` / ``_query_shape``). Two things follow, and
    DQ has to reproduce both or the unreachable rows read as drift:

    * **the join** -- a child row whose foreign key matches no parent row is
      never extracted. ``_query_shape`` supplies the join, so it is already in
      the FROM clause by the time this runs.
    * **the parent's window** -- the configured initial floor applies to the
      helper's date column, and the branch's CDC watermark (which *is* the
      helper's column) caps how fresh the lake can be.

    Without these, ``authorisations`` compared its whole Oracle table against a
    lake that only ever held rows joining to AUTHORISATIONS_MASTER on or after
    the 2022-01-01 floor -- reporting ~975k rows per branch as missing when the
    pipeline was never asked to load them.

    Returns ``(predicates, note)``; both empty for a plain table.
    """
    if not tdef.is_helper_driven:
        return [], None

    from .oracle_extract import Watermark, format_initial_value, format_watermark

    shape = _source_shape(tdef)
    preds: list[str] = []
    bounds: list[str] = []

    # Lower bound: the same initial-range filter build_query applies, on the
    # helper's date column. Masters are loaded in full and get no floor.
    if not tdef.is_master and shape.date_ref and tdef.where_value_of_initial_run:
        op = tdef.where_operator or ">="
        preds.append(f"{shape.date_ref} {op} "
                     f"{format_initial_value(tdef.where_value_of_initial_run)}")
        bounds.append(f"{shape.date_ref} {op} {tdef.where_value_of_initial_run}")

    # Upper bound: the branch's CDC watermark, which for a helper-driven table
    # is the helper's own column -- the exact edge of what the last load saw.
    wm = (control_entry or {}).get("last_cdc") or {}
    if shape.cdc_ref and wm.get("value") is not None:
        preds.append(f"{shape.cdc_ref} <= {format_watermark(Watermark.from_dict(wm))}")
        bounds.append(f"{shape.cdc_ref} <= {wm['value']}")

    note = ("helper join " + tdef.helper.table
            + (" + " + " + ".join(bounds) if bounds else "")) if preds else None
    return preds, note


def _source_shape(tdef: TableDef):
    """The pipeline's own SELECT/FROM shape, so DQ reads what the loader read."""
    from .oracle_extract import _query_shape

    return _query_shape(tdef)


def _oracle_where_all(tdef: TableDef, win: _Window, coverage: list[str]) -> str:
    """Window predicates (aliased to ``t.``) AND-ed with the coverage bounds."""
    parts = [p for p in (win.oracle_where("t.") if win.date_col else "",) if p]
    parts.extend(coverage)
    return " AND ".join(parts)


def _oracle_select(tdef: TableDef, win: _Window, coverage: list[str] = ()) -> str:
    shape = _source_shape(tdef)
    base = f"SELECT {shape.select} FROM {shape.frm}"
    where = _oracle_where_all(tdef, win, list(coverage))
    return base + (f" WHERE {where}" if where else "")


def _oracle_count_sql(tdef: TableDef, win: _Window, coverage: list[str] = ()) -> str:
    shape = _source_shape(tdef)
    where = _oracle_where_all(tdef, win, list(coverage))
    return f"SELECT COUNT(*) FROM {shape.frm}" + (f" WHERE {where}" if where else "")


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
    row_count_delta_pct: Optional[float] = None
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


def _warn_on_normalizer_drift(res: "DqResult", tdef: TableDef,
                              branch: BranchConfig) -> None:
    """Flag a column that looks present on both sides under two spellings.

    A name that appears in *both* one-sided lists once underscores are removed
    is almost never real schema drift -- it is the source and the lake
    normalizing the same column differently, which silently drops it from the
    comparison. ``_norm`` now defers to dlt, so this should never fire; it is
    here so the next divergence surfaces as a warning instead of as a column
    that quietly stops being checked.
    """
    if not res.cols_only_oracle or not res.cols_only_iceberg:
        return
    squashed = {c.replace("_", ""): c for c in res.cols_only_iceberg}
    pairs = [(o, squashed[k]) for o in res.cols_only_oracle
             if (k := o.replace("_", "")) in squashed]
    if pairs:
        log.warning(
            "[%s/%s] column name(s) normalize differently on the two sides and "
            "are excluded from the comparison: %s", tdef.dataset_table_name,
            branch.key, ", ".join(f"{o!r} vs {i!r}" for o, i in pairs))


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
    coverage, coverage_note = _coverage_predicates(tdef, control_entry)
    if coverage_note:
        win.note = f"{win.note}; {coverage_note}" if win.note else coverage_note
    lo, hi = win.label()
    res = DqResult(
        table=tdef.dataset_table_name, source_table=tdef.table, branch=branch.key,
        window_start=lo, window_end=hi, date_column=win.date_col, window_note=win.note,
    )

    lake_cols = {f.name for f in static_table.schema().fields} if static_table else set()
    lake_business = _business_norms(lake_cols, injected)

    # Append-only snapshot tables accumulate one full copy per run; compare the
    # source against the newest copy only, never the whole accumulation.
    snapshot = None
    if tdef.is_snapshot and static_table is not None:
        snapshot = _latest_snapshot_version(static_table, branch.id, settings)
        if snapshot is not None:
            note = f"snapshot: lake pinned to version {snapshot.version}"
            res.window_note = f"{res.window_note}; {note}" if res.window_note else note
        else:
            log.warning("[%s/%s] snapshot table has no %s column; comparing the "
                        "whole lake table", tdef.dataset_table_name, branch.key,
                        settings.snapshot_version_column)

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
            query = _oracle_select(tdef, win, coverage)
            src_business, _ = _oracle_business_norms(conn, query, injected)
            if not do_hash:  # the hash path derives the count from the rows it pulls
                res.oracle_row_count = _oracle_count(
                    conn, _oracle_count_sql(tdef, win, coverage))

        if static_table is None:
            res.iceberg_row_count = 0
        common = sorted(src_business & lake_business)
        res.cols_only_oracle = sorted(src_business - lake_business)
        res.cols_only_iceberg = sorted(lake_business - src_business)
        _warn_on_normalizer_drift(res, tdef, branch)

        # ---- hash delta -------------------------------------------------------
        if do_hash:
            key_norm = [_norm(k) for k in tdef.key_columns]
            if self_test:
                src_batches = _staged_batches(_staged_file(settings, tdef, branch.key), win)
            else:
                src_batches = _oracle_batches(
                    conn, _oracle_select(tdef, win, coverage), branch.fetch_batch_size)
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
                               for b in _lake_scan_batches(static_table, branch.id,
                                                           scan_actual, snapshot, win))
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
                res.iceberg_row_count = _lake_window_count(
                    static_table, branch.id, win, snapshot)

        # ---- status -----------------------------------------------------------
        res.status, res.row_count_delta_pct, res.hash_delta_pct = classify_status(
            res.row_count_delta, res.oracle_row_count, res.hash,
            settings.dq_hash_delta_tolerance_pct)
    except Exception as exc:  # noqa: BLE001 - isolate per-unit failures
        res.status = "ERROR"
        res.error = f"{type(exc).__name__}: {exc}"
        log.error("[%s/%s] DQ check failed: %s", tdef.dataset_table_name, branch.key, exc)
    return res


def _lake_window_count(static_table, branch: int, win: _Window,
                       snapshot: Optional[_SnapshotScope] = None) -> int:
    """Count rows in the branch partition within the window (counts-only path)."""
    cols = [win.date_col_norm] if win.date_col_norm else [_norm("BRANCH_ID")]
    lake_cols = {f.name for f in static_table.schema().fields}
    actual = [a for a in (_resolve_actual(lake_cols, c) for c in cols) if a] or None
    total = 0
    for b in _lake_scan_batches(static_table, branch,
                                actual or list(lake_cols)[:1], snapshot, win):
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

    @staticmethod
    def _p(v) -> str:
        return "-" if v is None else f"{v:.2f}"

    def _unit_line(self, res: "DqResult") -> str:
        h = res.hash
        return (f"DQ-UNIT {res.table}/{res.branch} | "
                f"ora={self._n(res.oracle_row_count)} ice={self._n(res.iceberg_row_count)} "
                f"cnt={self._n(res.row_count_delta)} "
                f"cntpct={self._p(res.row_count_delta_pct)} | "
                f"match={self._n(h.matched if h else None)} "
                f"delta={self._n(h.total_delta if h else None)} "
                f"pct={self._p(res.hash_delta_pct)} | {res.status}")

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
            "row_count_delta_pct": r.row_count_delta_pct,
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
    "row_count_delta_pct": {"data_type": "double"},
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
    headers = ["TABLE", "BRANCH", "ORA_ROWS", "ICE_ROWS", "CNT_DELTA", "CNT%"]
    if do_hash:
        headers += ["MATCH", "ONLY_ORA", "ONLY_ICE", "MISMATCH", "HASH_DELTA", "HASH%"]
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
               cell(r.row_count_delta), pct_cell(r.row_count_delta_pct)]
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
