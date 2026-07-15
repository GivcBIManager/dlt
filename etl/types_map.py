"""Oracle -> Arrow type mapping and cross-branch schema unification.

Two responsibilities:

1. Map an Oracle column (from ``cursor.description``) to a target Arrow type so
   the Iceberg files get sensible logical types (NUMBER -> DECIMAL,
   VARCHAR2 -> STRING, DATE -> TIMESTAMP, ...).

2. Merge the per-branch Arrow schemas for the same table into one unified schema
   (union of columns, widened types, nullable where a branch is missing/null) and
   cast each branch's table to it so all 7 branches stack cleanly into one
   Iceberg dataset.

Arrow types are used as the intermediate representation; dlt + pyiceberg then map
Arrow -> Iceberg (decimal->decimal, timestamp[us]->timestamp, string->string, ...).
"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import oracledb
import pyarrow as pa

# Return NUMBER values as decimal.Decimal so they slot straight into decimal128
# arrays instead of lossy floats.
oracledb.defaults.fetch_decimals = True

# Fetch (small) LOBs as direct str/bytes instead of LOB locators. Locator
# fetching costs an extra Oracle round trip *per row*, so this is a meaningful
# read speedup for any CLOB/VARCHAR-heavy table on the cursor path.
oracledb.defaults.fetch_lobs = False

_MAX_DECIMAL_PRECISION = 38  # pyarrow decimal128 hard limit


def _dbtype(name: str):
    """Look up an oracledb DB_TYPE_* constant defensively across versions."""
    return getattr(oracledb, name, None)


# Group Oracle DB types by destination family. Unknown/None entries are skipped.
_STRING_TYPES = {
    _dbtype(n)
    for n in (
        "DB_TYPE_VARCHAR", "DB_TYPE_CHAR", "DB_TYPE_NVARCHAR", "DB_TYPE_NCHAR",
        "DB_TYPE_LONG", "DB_TYPE_CLOB", "DB_TYPE_NCLOB", "DB_TYPE_ROWID",
        "DB_TYPE_UROWID", "DB_TYPE_JSON", "DB_TYPE_XMLTYPE",
    )
}
_TIMESTAMP_TYPES = {
    _dbtype(n)
    for n in (
        "DB_TYPE_DATE", "DB_TYPE_TIMESTAMP", "DB_TYPE_TIMESTAMP_TZ",
        "DB_TYPE_TIMESTAMP_LTZ",
    )
}
_BINARY_TYPES = {
    _dbtype(n) for n in ("DB_TYPE_RAW", "DB_TYPE_LONG_RAW", "DB_TYPE_BLOB")
}
_STRING_TYPES.discard(None)
_TIMESTAMP_TYPES.discard(None)
_BINARY_TYPES.discard(None)

# Types python-oracledb's native Arrow fetch cannot convert (raises DPY-3030);
# they must be cast server-side (ROWIDTOCHAR) to keep the fast path.
ARROW_UNSUPPORTED_ROWID_TYPES = {
    t for t in (_dbtype("DB_TYPE_ROWID"), _dbtype("DB_TYPE_UROWID")) if t is not None
}

DB_TYPE_NUMBER = _dbtype("DB_TYPE_NUMBER")
DB_TYPE_BINARY_FLOAT = _dbtype("DB_TYPE_BINARY_FLOAT")
DB_TYPE_BINARY_DOUBLE = _dbtype("DB_TYPE_BINARY_DOUBLE")
DB_TYPE_BOOLEAN = _dbtype("DB_TYPE_BOOLEAN")

# Sentinel meaning "let pyarrow infer the type from the values" -- used for
# unconstrained NUMBER where precision/scale are unknown until we see the data.
INFER = None


def oracle_field_to_arrow(field) -> Optional[pa.DataType]:
    """Map one ``cursor.description`` FetchInfo entry to a target Arrow type.

    Returns ``INFER`` (None) for unconstrained NUMBER so the caller lets pyarrow
    derive a precise decimal from the actual values.
    """
    db_type = getattr(field, "type_code", None)
    precision = getattr(field, "precision", None) or 0
    scale = getattr(field, "scale", None)

    if db_type == DB_TYPE_NUMBER:
        # Unconstrained NUMBER (no precision reported): infer to avoid clipping
        # large integer ids or over-/under-scaling decimals.
        if not precision:
            return INFER
        s = max(0, scale or 0)
        p = min(_MAX_DECIMAL_PRECISION, max(precision, s + 1))
        s = min(s, p)
        return pa.decimal128(p, s)

    if db_type == DB_TYPE_BINARY_DOUBLE:
        return pa.float64()
    if db_type == DB_TYPE_BINARY_FLOAT:
        return pa.float32()
    if db_type == DB_TYPE_BOOLEAN:
        return pa.bool_()
    if db_type in _TIMESTAMP_TYPES:
        return pa.timestamp("us")
    if db_type in _BINARY_TYPES:
        return pa.binary()
    if db_type in _STRING_TYPES:
        return pa.string()
    # Unknown/rare types: stringify so the load never fails on an exotic column.
    return pa.string()


def _to_naive_utc(value):
    """Normalize datetimes to naive UTC so they fit a tz-less timestamp[us]."""
    if isinstance(value, dt.datetime) and value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value


def build_arrow_column(values: list, target_type: Optional[pa.DataType]) -> pa.Array:
    """Build an Arrow array, degrading gracefully if the target type won't fit.

    Order: target type -> pyarrow inference -> stringified. This guarantees a
    column is always produced even for messy/over-scaled source data.
    """
    if target_type is not None and pa.types.is_timestamp(target_type):
        values = [_to_naive_utc(v) for v in values]

    try:
        return pa.array(values, type=target_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError,
            OverflowError, ValueError):
        pass

    # Fallback 1: let pyarrow infer (handles odd decimal scales, mixed ints).
    try:
        return pa.array([_to_naive_utc(v) for v in values])
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError,
            OverflowError, ValueError):
        pass

    # Fallback 2: stringify -- last resort, never lose the column.
    return pa.array([None if v is None else str(v) for v in values], type=pa.string())


# --------------------------------------------------------------------------- #
# Cross-branch schema unification
# --------------------------------------------------------------------------- #
def _is_numeric(t: pa.DataType) -> bool:
    return (
        pa.types.is_integer(t)
        or pa.types.is_floating(t)
        or pa.types.is_decimal(t)
    )


def widen_types(t1: pa.DataType, t2: pa.DataType) -> pa.DataType:
    """Return a type that can hold values of both ``t1`` and ``t2``."""
    if t1.equals(t2):
        return t1
    if pa.types.is_null(t1):
        return t2
    if pa.types.is_null(t2):
        return t1

    # A string column anywhere wins -- it can represent anything safely.
    if pa.types.is_string(t1) or pa.types.is_string(t2):
        return pa.string()

    if _is_numeric(t1) and _is_numeric(t2):
        if pa.types.is_floating(t1) or pa.types.is_floating(t2):
            return pa.float64()
        # decimal vs decimal (or decimal vs int): widen precision & scale.
        s1 = t1.scale if pa.types.is_decimal(t1) else 0
        s2 = t2.scale if pa.types.is_decimal(t2) else 0
        p1 = t1.precision if pa.types.is_decimal(t1) else 18
        p2 = t2.precision if pa.types.is_decimal(t2) else 18
        scale = max(s1, s2)
        int_digits = max(p1 - s1, p2 - s2)
        precision = min(_MAX_DECIMAL_PRECISION, int_digits + scale)
        scale = min(scale, precision)
        return pa.decimal128(precision, scale)

    if pa.types.is_timestamp(t1) and pa.types.is_timestamp(t2):
        return pa.timestamp("us")

    if pa.types.is_binary(t1) and pa.types.is_binary(t2):
        return pa.binary()

    # Incompatible families -> string is the safe common ground.
    return pa.string()


def unify_schemas(schemas: list[pa.Schema]) -> pa.Schema:
    """Union all branch schemas: keep every column, widen types, set nullable.

    A column is nullable if it is nullable in any branch OR absent from any
    branch (so the missing-branch rows can carry nulls).
    """
    order: list[str] = []
    types: dict[str, pa.DataType] = {}
    nullable: dict[str, bool] = {}
    present_count: dict[str, int] = {}

    for schema in schemas:
        for fld in schema:
            if fld.name not in types:
                order.append(fld.name)
                types[fld.name] = fld.type
                nullable[fld.name] = fld.nullable
                present_count[fld.name] = 0
            else:
                types[fld.name] = widen_types(types[fld.name], fld.type)
                nullable[fld.name] = nullable[fld.name] or fld.nullable
            present_count[fld.name] += 1

    total = len(schemas)
    fields = []
    for name in order:
        is_nullable = nullable[name] or present_count[name] < total
        fields.append(pa.field(name, types[name], nullable=is_nullable))
    return pa.schema(fields)


def replace_null_types(
    schema: pa.Schema,
    overrides: Optional[dict[str, pa.DataType]] = None,
    fallback: pa.DataType = pa.string(),
) -> pa.Schema:
    """Replace every ``null``-typed field with a concrete type.

    A column that is entirely null across every branch this run -- an
    unconstrained NUMBER mapped to ``INFER`` with no (or all-null) values, or a
    0-row delta -- is inferred by pyarrow as the ``null`` type, which then
    survives ``unify_schemas`` (``widen_types`` keeps null when both sides are
    null). dlt maps an Arrow ``null`` column to an *incomplete* column, and
    pyiceberg cannot cast an existing typed column to ``null`` -- so handing such
    a column to the load raises ``Unsupported cast from <type> to null using
    function cast_null`` on a table that already has the column typed.

    Coerce to a concrete type instead: the destination's existing type for that
    column when known (``overrides``, keyed by field name) so a merge stays a
    no-op, otherwise ``fallback`` (string -- the universal safe ground, matching
    ``build_arrow_column``'s stringify last resort). Non-null fields are
    untouched.
    """
    overrides = overrides or {}
    fields = [
        pa.field(f.name, overrides.get(f.name, fallback),
                 nullable=f.nullable, metadata=f.metadata)
        if pa.types.is_null(f.type) else f
        for f in schema
    ]
    return pa.schema(fields)


def cast_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Reshape one branch's table to the unified schema.

    Missing columns are added as all-null; present columns are cast to the
    unified type (lenient: ``safe=False`` so widening never aborts a load).
    """
    columns = []
    for fld in schema:
        if fld.name in table.column_names:
            col = table.column(fld.name)
            if not col.type.equals(fld.type):
                try:
                    col = col.cast(fld.type, safe=False)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    col = col.cast(pa.string()) if not pa.types.is_string(fld.type) else col
            columns.append(col)
        else:
            columns.append(pa.nulls(table.num_rows, type=fld.type))
    return pa.Table.from_arrays(
        [c if isinstance(c, pa.ChunkedArray) else pa.chunked_array([c]) for c in columns],
        schema=schema,
    )


def schema_diff(branch_schema: pa.Schema, unified: pa.Schema) -> dict:
    """Describe how one branch deviates from the unified schema (for logging)."""
    unified_names = set(unified.names)
    branch_names = set(branch_schema.names)
    widened = []
    for fld in branch_schema:
        if fld.name in unified_names:
            u = unified.field(fld.name)
            if not u.type.equals(fld.type):
                widened.append({"column": fld.name,
                                "branch_type": str(fld.type),
                                "unified_type": str(u.type)})
    return {
        "missing_in_branch": sorted(unified_names - branch_names),
        "extra_in_branch": sorted(branch_names - unified_names),
        "type_widened": widened,
    }
