"""Compare the APPOINTMENTS extraction plan across branches.

APPOINTMENTS is the only transaction table filtered on a *numeric* Julian date
(``JULIAN_DATE``) via a ``TO_NUMBER(TO_CHAR(TO_DATE(...),'J'))`` expression, and
it has no recorded watermark, so every run executes the INITIAL-style query:

    SELECT t.* FROM OASIS.APPOINTMENTS t
    WHERE t.JULIAN_DATE >= TO_NUMBER(TO_CHAR(TO_DATE('2026-06-20','YYYY-MM-DD'),'J'))

If one branch index-range-scans JULIAN_DATE while another full-scans a 100M+ row
table, that branch is dramatically slower for the *same* query. This tool pulls,
for each branch, the **exact** query the pipeline builds (from tables.json +
control_state.json), the optimizer EXPLAIN PLAN for it (read-only: the query is
NOT executed), and the index / column-stats facts that explain the chosen plan.

Usage:  python diagnostics/explain_appointments.py [branch_key ...]
        (default: jazan alrabwah  -- the slow one vs the fast reference)
"""
from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

import oracledb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script

from etl import oracle_extract  # noqa: E402
from etl.config import (  # noqa: E402
    MODE_INCREMENTAL,
    Settings,
    load_table_defs,
    resolve_oracle_client_lib_dir,
)

OWNER = "OASIS"
TABLE = "APPOINTMENTS"
TARGET = f"{OWNER}.{TABLE}"


def load_cfg():
    s = tomllib.loads((ROOT / ".dlt" / "secrets.toml").read_text("utf-8"))
    c = tomllib.loads((ROOT / ".dlt" / "config.toml").read_text("utf-8"))
    state_path = ROOT / "control_state.json"
    state = json.loads(state_path.read_text("utf-8")) if state_path.exists() else {}
    return s["oracle_branches"], c.get("etl", {}), state


def appointments_def():
    for t in load_table_defs(ROOT / "tables.json"):
        if t.table.upper() == TARGET:
            return t
    raise SystemExit(f"{TARGET} not found in tables.json")


def build_branch_query(tdef, state: dict) -> str:
    """Reproduce exactly what etl.oracle_extract.build_query would emit.

    Uses INCREMENTAL mode + whatever watermark the branch has in
    control_state.json (APPOINTMENTS currently has none -> INITIAL fallback).
    The watermark is keyed by dataset_table_name then branch; for the plan it is
    identical across branches when no watermark exists, so we build it once.
    """
    settings = Settings(mode=MODE_INCREMENTAL)
    tbl_state = state.get(tdef.dataset_table_name, {})
    # Branch watermarks would normally differ; APPOINTMENTS has none, so any
    # branch yields the same query. Use an empty per-branch slot.
    wm = next(iter(tbl_state.values()), {}) if tbl_state else {}
    cdc = oracle_extract.Watermark.from_dict(wm.get("last_cdc"))
    date = oracle_extract.Watermark.from_dict(wm.get("last_date"))
    return oracle_extract.build_query(tdef, settings, cdc, date)


def connect(sec, dsn_mode):
    host, port = sec["host"], int(sec["port"])
    dsn = (oracledb.makedsn(host, port, sid=sec["database"]) if dsn_mode == "sid"
           else oracledb.makedsn(host, port, service_name=sec["database"]))
    return oracledb.connect(user=sec["username"], password=sec["password"],
                            dsn=dsn, tcp_connect_timeout=30)


def fetch_all(cur, sql, params=None):
    cur.execute(sql, params or {})
    return cur.fetchall()


def explain(cur, query: str) -> str:
    # Unique statement id so re-runs don't read a stale plan.
    sid = f"appt_{id(query) & 0xffffff}"
    cur.execute(f"EXPLAIN PLAN SET STATEMENT_ID = '{sid}' FOR {query}")
    rows = fetch_all(
        cur,
        "SELECT plan_table_output FROM "
        "TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', :sid, 'TYPICAL'))",
        {"sid": sid},
    )
    return "\n".join(r[0] for r in rows)


def report(key, sec, dsn_mode, query):
    print(f"\n{'='*78}\n=== {key}  ({sec['host']}  {dsn_mode}={sec['database']}) ===\n{'='*78}")
    conn = connect(sec, dsn_mode)
    cur = conn.cursor()

    # --- table-level optimizer stats (drives the plan) ---
    trows = fetch_all(cur,
        "SELECT num_rows, blocks, last_analyzed FROM all_tables "
        "WHERE owner=:o AND table_name=:t", {"o": OWNER, "t": TABLE})
    if trows:
        nr, blocks, la = trows[0]
        print(f"\n[table stats] num_rows={nr:,}  blocks={blocks}  "
              f"last_analyzed={la.date() if la else 'NEVER'}"
              if nr is not None else
              f"\n[table stats] NO STATS GATHERED (num_rows is NULL) "
              f"last_analyzed={la.date() if la else 'NEVER'}")
    else:
        print("\n[table stats] table not visible to this user")

    # --- column stats for the filter column ---
    crows = fetch_all(cur,
        "SELECT data_type, num_distinct, density, num_nulls, last_analyzed "
        "FROM all_tab_columns WHERE owner=:o AND table_name=:t AND column_name=:c",
        {"o": OWNER, "t": TABLE, "c": "JULIAN_DATE"})
    if crows:
        dtype, ndv, dens, nulls, la = crows[0]
        print(f"[JULIAN_DATE col] type={dtype} num_distinct={ndv} density={dens} "
              f"num_nulls={nulls} last_analyzed={la.date() if la else 'NEVER'}")

    # --- indexes that lead with the filter column ---
    irows = fetch_all(cur,
        "SELECT ic.index_name, ic.column_position, i.index_type, i.uniqueness, "
        "       i.status, i.num_rows, i.last_analyzed "
        "FROM all_ind_columns ic "
        "JOIN all_indexes i ON i.owner=ic.index_owner AND i.index_name=ic.index_name "
        "WHERE ic.table_owner=:o AND ic.table_name=:t AND ic.column_name=:c "
        "ORDER BY ic.index_name", {"o": OWNER, "t": TABLE, "c": "JULIAN_DATE"})
    if irows:
        print("[indexes on JULIAN_DATE]")
        for iname, pos, itype, uniq, status, inr, ila in irows:
            lead = "LEADING" if pos == 1 else f"position {pos}"
            print(f"   {iname:30s} {lead:12s} {itype} {uniq} status={status} "
                  f"analyzed={ila.date() if ila else 'NEVER'}")
    else:
        print("[indexes on JULIAN_DATE] *** NONE -> forced full table scan ***")

    # --- clustering factor: the real cost driver of TABLE ACCESS BY INDEX ROWID.
    # A high clustering_factor (-> num_rows) means rows for a JULIAN_DATE range are
    # scattered across many blocks, so each matching row can cost a separate
    # single-block read; a low one (~blocks) means they are packed together.
    print("[index clustering / size]")
    cf = fetch_all(cur,
        "SELECT index_name, clustering_factor, leaf_blocks, distinct_keys, "
        "       blevel, num_rows FROM all_indexes "
        "WHERE table_owner=:o AND table_name=:t ORDER BY index_name",
        {"o": OWNER, "t": TABLE})
    for iname, clf, leaf, dk, bl, inr in cf:
        print(f"   {iname:30s} clustering_factor={clf:>12,}  leaf_blocks={leaf:>9,}  "
              f"distinct_keys={dk:>10,}  blevel={bl}")

    # --- JULIAN_DATE high value vs the predicate value: confirms the out-of-range
    # (ascending-key) estimate problem when the query floor is above the stats max.
    hv = fetch_all(cur,
        "SELECT low_value, high_value FROM all_tab_columns "
        "WHERE owner=:o AND table_name=:t AND column_name=:c",
        {"o": OWNER, "t": TABLE, "c": "JULIAN_DATE"})
    if hv:
        lo_raw, hi_raw = hv[0]
        try:
            lo = cur.callfunc("utl_raw.cast_to_number", oracledb.DB_TYPE_NUMBER, [lo_raw])
            hi = cur.callfunc("utl_raw.cast_to_number", oracledb.DB_TYPE_NUMBER, [hi_raw])
            print(f"[JULIAN_DATE stats range] low={lo:.0f}  high={hi:.0f}  "
                  f"(predicate floor = 2461212; rows above 'high' are invisible to "
                  f"the optimizer -> under-estimate)")
        except oracledb.Error:
            pass

    print(f"\n[query]\n{query}")
    print("\n[EXPLAIN PLAN]")
    try:
        print(explain(cur, query))
    except oracledb.Error as e:
        print(f"   EXPLAIN failed: {e!r}")

    # --- ACTUALS: force the row source (COUNT wrapper ships no row data) and read
    # the real cardinality + timing + buffer gets back via DISPLAY_CURSOR.
    print("\n[ACTUAL row-source stats]  (COUNT(*) over the same predicate)")
    try:
        count_sql = (f"SELECT /*+ gather_plan_statistics */ COUNT(*) "
                     f"FROM ({query})")
        cur.execute(count_sql)
        matched = cur.fetchone()[0]
        print(f"   actual matching rows = {matched:,}")
        stats = fetch_all(cur,
            "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR("
            "format=>'ALLSTATS LAST +COST'))")
        print("\n".join(r[0] for r in stats))
    except oracledb.Error as e:
        print(f"   actuals failed: {e!r}")

    conn.close()


def main():
    branches, etl, state = load_cfg()
    dsn_mode = etl.get("dsn_mode", "service_name")
    if etl.get("thick_mode", True):
        try:
            oracledb.init_oracle_client(
                lib_dir=resolve_oracle_client_lib_dir(etl.get("oracle_client_lib_dir")))
        except Exception as e:  # noqa: BLE001
            print(f"[init] {e!r}")

    tdef = appointments_def()
    query = build_branch_query(tdef, state)

    for key in (sys.argv[1:] or ["jazan", "alrabwah"]):
        if key not in branches:
            print(f"\n=== {key}: NOT FOUND in secrets.toml ===")
            continue
        try:
            report(key, branches[key], dsn_mode, query)
        except Exception as e:  # noqa: BLE001
            print(f"\n=== {key}: FAILED {e!r} ===")


if __name__ == "__main__":
    main()
