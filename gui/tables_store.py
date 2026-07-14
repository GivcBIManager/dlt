"""Load, validate and save ``tables.json``.

Validation mirrors what ``etl/config.py`` expects (``load_table_defs`` /
``_parse_helper``) so a doc that saves cleanly here also loads cleanly in the
pipeline. Saves are atomic and keep a timestamped backup.
"""

from __future__ import annotations

import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import security
from config import STATE_DIR, TABLES_JSON

CATEGORIES = ("masters", "transactions", "snapshots")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")

# These fields are concatenated into Oracle SQL unparameterized, so guard them.
_OPERATORS = {"=", "!=", "<>", "<", ">", "<=", ">="}
# Tokens that have no place even inside a value expression: statement
# terminators and comment markers (TO_DATE(...) etc. remain valid).
_SQL_DANGER = (";", "--", "/*", "*/", "\x00")


def _is_sql_identifier(value: str) -> bool:
    """True for a plain or dotted identifier (``COL`` / ``SCHEMA.TABLE``)."""
    parts = [p.strip() for p in str(value).split(".")]
    return 1 <= len(parts) <= 3 and all(_IDENT_RE.match(p) for p in parts)


def _value_expr_is_safe(value: Any) -> bool:
    """A comparison value/expression must not smuggle in a second statement."""
    s = str(value)
    return not any(tok in s for tok in _SQL_DANGER)


def _is_valid_table_ref(value: str) -> bool:
    """A source ``table`` is either an identifier or an inline-view subquery.

    Subqueries (``(SELECT ... )``) are a deliberate raw-SQL escape hatch; they
    are accepted as long as they carry no statement terminator / comment marker.
    """
    v = str(value).strip()
    if v.startswith("(") and v.endswith(")"):
        return _value_expr_is_safe(v)
    return _is_sql_identifier(v)

# Recognised keys on a table entry (used to warn about typos, not to reject).
KNOWN_KEYS = {
    "table",
    "name",
    "unique_key",
    "cdc_column",
    "where_date_column",
    "where_operator",
    "where_value_of_initial_run",
    "where_value_max",
    "where_operator_max",
    "helper",
}
KNOWN_HELPER_KEYS = {"table", "join", "join_keys", "cdc_column", "where_date_column"}


def load_raw() -> dict[str, Any]:
    """Full tables.json document (creates a minimal skeleton if missing)."""
    if not TABLES_JSON.exists():
        return {"schema": "", "description": "", "masters": [], "transactions": [], "snapshots": []}
    return json.loads(TABLES_JSON.read_text(encoding="utf-8"))


def _validate_entry(entry: dict[str, Any], category: str, idx: int) -> list[str]:
    where = f"{category}[{idx}]"
    errs: list[str] = []
    if not isinstance(entry, dict):
        return [f"{where}: must be an object"]

    table = str(entry.get("table") or "").strip()
    if not table:
        errs.append(f"{where}: 'table' is required (e.g. OASIS.MY_TABLE)")
    is_query = table.startswith("(")
    iceberg_name = str(entry.get("name") or "").strip()
    name = iceberg_name or table or where
    if table and not _is_valid_table_ref(table):
        errs.append(f"{name}: 'table' must be a valid identifier (e.g. OASIS.MY_TABLE)")

    # 'name' is the Iceberg table name for subquery sources. Plain tables derive
    # their name from the identifier, so a stray 'name' there is ambiguous.
    if is_query and not iceberg_name:
        errs.append(f"{name}: subquery sources require 'name' (the Iceberg table name)")
    if iceberg_name and not is_query:
        errs.append(f"{name}: 'name' is only allowed when 'table' is a subquery")
    if iceberg_name and not _IDENT_RE.match(iceberg_name):
        errs.append(f"{name}: 'name' must be a plain identifier (e.g. visits_enriched)")

    unique_key = str(entry.get("unique_key") or "").strip()
    # Snapshot tables are append-only (no merge), so they need no unique_key.
    if not unique_key and category != "snapshots":
        errs.append(f"{name}: 'unique_key' is required")
    if unique_key:
        for col in unique_key.split(","):
            if not _is_sql_identifier(col):
                errs.append(f"{name}: 'unique_key' has an invalid column '{col.strip()}'")

    for col_field in ("cdc_column", "where_date_column"):
        col = str(entry.get(col_field) or "").strip()
        if col and not _is_sql_identifier(col):
            errs.append(f"{name}: '{col_field}' must be a valid column identifier")

    for op_field in ("where_operator", "where_operator_max"):
        op = str(entry.get(op_field) or "").strip()
        if op and op not in _OPERATORS:
            errs.append(f"{name}: '{op_field}' must be one of {' '.join(sorted(_OPERATORS))}")

    for val_field in ("where_value_of_initial_run", "where_value_max"):
        val = entry.get(val_field)
        if val not in (None, "") and not _value_expr_is_safe(val):
            errs.append(f"{name}: '{val_field}' contains a disallowed SQL token (; -- /* */)")

    for k in entry:
        if k not in KNOWN_KEYS:
            errs.append(f"{name}: unknown key '{k}'")

    # If a lower bound is declared, both operator and value must be present.
    op = entry.get("where_operator")
    val = entry.get("where_value_of_initial_run")
    if bool(op) ^ bool(val):
        errs.append(
            f"{name}: 'where_operator' and 'where_value_of_initial_run' must be "
            f"set together (or both empty)"
        )

    op_max = entry.get("where_operator_max")
    val_max = entry.get("where_value_max")
    if bool(op_max) and not bool(val_max):
        errs.append(f"{name}: 'where_operator_max' set without 'where_value_max'")

    helper = entry.get("helper")
    if helper not in (None, "", {}):
        errs.extend(_validate_helper(helper, name))
    return errs


def _validate_helper(helper: Any, name: str) -> list[str]:
    errs: list[str] = []
    if not isinstance(helper, dict):
        return [f"{name}: 'helper' must be an object"]
    for k in helper:
        if k not in KNOWN_HELPER_KEYS:
            errs.append(f"{name}: helper has unknown key '{k}'")
    h_table = str(helper.get("table") or "").strip()
    if not h_table:
        errs.append(f"{name}: helper is missing 'table'")
    elif not _is_sql_identifier(h_table):
        errs.append(f"{name}: helper 'table' must be a valid identifier")
    h_cdc = str(helper.get("cdc_column") or "").strip()
    if not h_cdc:
        errs.append(f"{name}: helper is missing 'cdc_column'")
    elif not _is_sql_identifier(h_cdc):
        errs.append(f"{name}: helper 'cdc_column' must be a valid column identifier")
    h_wdc = str(helper.get("where_date_column") or "").strip()
    if h_wdc and not _is_sql_identifier(h_wdc):
        errs.append(f"{name}: helper 'where_date_column' must be a valid column identifier")
    pairs = helper.get("join") or helper.get("join_keys") or []
    if not pairs:
        errs.append(f"{name}: helper is missing 'join' key pairs")
    else:
        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                errs.append(
                    f"{name}: each helper 'join' entry must be a "
                    f"[child_column, helper_column] pair, got {pair!r}"
                )
            elif not str(pair[0]).strip() or not str(pair[1]).strip():
                errs.append(f"{name}: empty column in helper join pair {pair!r}")
            elif not (_is_sql_identifier(pair[0]) and _is_sql_identifier(pair[1])):
                errs.append(f"{name}: helper join pair {pair!r} must be column identifiers")
    return errs


def validate(doc: dict[str, Any]) -> list[str]:
    """Return a list of human-readable problems ([] means the doc is valid)."""
    errs: list[str] = []
    if not isinstance(doc, dict):
        return ["Document must be a JSON object"]

    seen: set[str] = set()
    total = 0
    for category in CATEGORIES:
        items = doc.get(category, [])
        if items in (None, ""):
            continue
        if not isinstance(items, list):
            errs.append(f"'{category}' must be a list")
            continue
        for idx, entry in enumerate(items):
            total += 1
            errs.extend(_validate_entry(entry, category, idx))
            t = str((entry or {}).get("name")
                    or (entry or {}).get("table") or "").strip().upper()
            if t:
                if t in seen:
                    errs.append(f"Duplicate table '{t}' (appears more than once)")
                seen.add(t)
    if total == 0:
        errs.append("No tables defined (need at least one master or transaction)")
    return errs


def save_raw(doc: dict[str, Any]) -> dict[str, Any]:
    """Validate then atomically write tables.json, keeping a backup.

    Returns ``{"backup": <path or None>}``. Raises ``ValueError`` if invalid.
    """
    errs = validate(doc)
    if errs:
        raise ValueError("; ".join(errs))

    # Preserve top-level metadata keys (schema/description) and known sections,
    # in a stable order.
    out: dict[str, Any] = {}
    for k in ("schema", "description"):
        if k in doc:
            out[k] = doc[k]
    for category in CATEGORIES:
        out[category] = doc.get(category, []) or []

    backup_path: Path | None = None
    if TABLES_JSON.exists():
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = STATE_DIR / f"tables.json.{stamp}.bak"
        shutil.copy2(TABLES_JSON, backup_path)

    tmp = TABLES_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(TABLES_JSON)
    security.prune_backups(STATE_DIR, "tables.json.*.bak")
    return {"backup": str(backup_path) if backup_path else None}
