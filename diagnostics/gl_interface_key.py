"""Discover a usable unique / primary key for DEVDBA.GL_INTERFACE.

GL_INTERFACE is an ERP-style *interface* table and frequently ships with no
declared primary key, so the pipeline currently carries ``unique_key: null``
for it. This script inspects the live table on one or more branches to decide
what (if anything) can serve as a natural unique key:

    1. declared PRIMARY KEY / UNIQUE constraints        (ALL_CONSTRAINTS)
    2. UNIQUE indexes                                   (ALL_INDEXES/_IND_COLUMNS)
    3. the full column list                             (ALL_TAB_COLUMNS)
    4. row count + NULL/DISTINCT profile of candidate id-ish columns and of a
       few composite combinations, so we can see which are actually unique.

It only runs read-only dictionary + COUNT queries. Nothing is written.

Usage:  python diagnostics/gl_interface_key.py [branch_key ...]   (default: jazan)
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

import oracledb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script
from etl.config import resolve_oracle_client_lib_dir  # noqa: E402

SECRETS = ROOT / ".dlt" / "secrets.toml"
CONFIG = ROOT / ".dlt" / "config.toml"

OWNER = "DEVDBA"
TABLE = "GL_INTERFACE"

# Composite combinations to test for uniqueness (only used when every column
# in the combination exists on the table). Every actual column is also profiled
# individually, so single-column candidates don't need to be pre-listed.
COMPOSITE_CANDIDATES = [
    ["BATCH_ID", "DOC_ID", "GL_DOC_ID", "LINE_ID"]
]


def load():
    secrets = tomllib.loads(SECRETS.read_text(encoding="utf-8"))
    config = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    return secrets["oracle_branches"], config.get("etl", {})


def connect(sec: dict, dsn_mode: str):
    host, port = sec["host"], int(sec["port"])
    dsn = (oracledb.makedsn(host, port, sid=sec["database"]) if dsn_mode == "sid"
           else oracledb.makedsn(host, port, service_name=sec["database"]))
    return oracledb.connect(user=sec["username"], password=sec["password"],
                            dsn=dsn, tcp_connect_timeout=30)


def constraints(cur) -> None:
    cur.execute(
        "SELECT c.constraint_name, c.constraint_type, cc.column_name, cc.position "
        "FROM all_constraints c "
        "JOIN all_cons_columns cc "
        "  ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name "
        "WHERE c.owner = :o AND c.table_name = :t "
        "  AND c.constraint_type IN ('P', 'U') "
        "ORDER BY c.constraint_type, c.constraint_name, cc.position",
        {"o": OWNER, "t": TABLE},
    )
    rows = cur.fetchall()
    print("  -- declared PRIMARY KEY / UNIQUE constraints --")
    if not rows:
        print("     (none)")
        return
    grouped: dict[tuple[str, str], list[str]] = {}
    for name, ctype, col, _pos in rows:
        grouped.setdefault((ctype, name), []).append(col)
    for (ctype, name), cols in grouped.items():
        kind = "PK" if ctype == "P" else "UNIQUE"
        print(f"     {kind:6s} {name}: ({', '.join(cols)})")


def unique_indexes(cur) -> None:
    cur.execute(
        "SELECT i.index_name, ic.column_name, ic.column_position "
        "FROM all_indexes i "
        "JOIN all_ind_columns ic "
        "  ON ic.index_owner = i.owner AND ic.index_name = i.index_name "
        "WHERE i.table_owner = :o AND i.table_name = :t AND i.uniqueness = 'UNIQUE' "
        "ORDER BY i.index_name, ic.column_position",
        {"o": OWNER, "t": TABLE},
    )
    rows = cur.fetchall()
    print("  -- UNIQUE indexes --")
    if not rows:
        print("     (none)")
        return
    grouped: dict[str, list[str]] = {}
    for name, col, _pos in rows:
        grouped.setdefault(name, []).append(col)
    for name, cols in grouped.items():
        print(f"     {name}: ({', '.join(cols)})")


def columns(cur) -> list[str]:
    cur.execute(
        "SELECT column_name, data_type, nullable "
        "FROM all_tab_columns WHERE owner = :o AND table_name = :t "
        "ORDER BY column_id",
        {"o": OWNER, "t": TABLE},
    )
    rows = cur.fetchall()
    print(f"  -- columns ({len(rows)}) --")
    names = []
    for name, dtype, nullable in rows:
        names.append(name)
        null_tag = "NULL" if nullable == "Y" else "NOT NULL"
        print(f"     {name:28s} {dtype:14s} {null_tag}")
    return names


def profile(cur, present: set[str]) -> None:
    cur.execute(f"SELECT COUNT(*) FROM {OWNER}.{TABLE}")
    total = cur.fetchone()[0]
    print(f"  -- profile (total rows = {total:,}) --")
    if total == 0:
        print("     (empty table; cannot profile uniqueness)")
        return

    def report(label: str, cols: list[str]) -> None:
        null_pred = " OR ".join(f"{c} IS NULL" for c in cols)
        # Oracle has no COUNT(DISTINCT a, b); count distinct rows via a subquery.
        if len(cols) == 1:
            distinct_expr = f"COUNT(DISTINCT {cols[0]})"
            cur.execute(
                f"SELECT COUNT(*), {distinct_expr}, "
                f"SUM(CASE WHEN {null_pred} THEN 1 ELSE 0 END) "
                f"FROM {OWNER}.{TABLE}"
            )
            cnt, distinct, nulls = cur.fetchone()
        else:
            col_list = ", ".join(cols)
            cur.execute(
                f"SELECT COUNT(*), "
                f"SUM(CASE WHEN {null_pred} THEN 1 ELSE 0 END) "
                f"FROM {OWNER}.{TABLE}"
            )
            cnt, nulls = cur.fetchone()
            cur.execute(
                f"SELECT COUNT(*) FROM "
                f"(SELECT DISTINCT {col_list} FROM {OWNER}.{TABLE})"
            )
            distinct = cur.fetchone()[0]
        nulls = nulls or 0
        unique = distinct == cnt and nulls == 0
        verdict = "UNIQUE + NOT NULL -> usable key" if unique else (
            "distinct but has NULL parts" if distinct == cnt
            else "NOT unique (has duplicates)")
        print(f"     {label:34s} distinct={distinct:>12,}  "
              f"nulls={nulls:>12,}  {verdict}")

    for col in sorted(present):
        report(col, [col])
    for combo in COMPOSITE_CANDIDATES:
        if all(c in present for c in combo):
            report("(" + ",".join(combo) + ")", combo)


def run(key: str, sec: dict, dsn_mode: str) -> None:
    print(f"\n=== {key}  ({sec['host']})  {OWNER}.{TABLE} ===")
    conn = connect(sec, dsn_mode)
    try:
        cur = conn.cursor()
        constraints(cur)
        unique_indexes(cur)
        present = set(columns(cur))
        profile(cur, present)
    finally:
        conn.close()


def main() -> None:
    branches, etl = load()
    dsn_mode = etl.get("dsn_mode", "service_name")
    if etl.get("thick_mode", True):
        try:
            oracledb.init_oracle_client(
                lib_dir=resolve_oracle_client_lib_dir(etl.get("oracle_client_lib_dir")))
        except Exception as e:
            print(f"[init] {e!r}")
    for key in (sys.argv[1:] or ["abha"]):
        if key not in branches:
            print(f"\n=== {key}: NOT FOUND in secrets.toml ===")
            continue
        try:
            run(key, branches[key], dsn_mode)
        except Exception as e:
            print(f"\n=== {key}: FAILED {e!r} ===")


if __name__ == "__main__":
    main()
