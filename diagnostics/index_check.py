"""Check whether the filter/CDC columns are indexed on each branch.

The INITIAL/INCREMENTAL queries filter on date/CDC columns. If a branch lacks
an index on the column used to filter a huge table (e.g. APPOINTMENTS.JULIAN_DATE,
115M rows), that branch does a full table scan server-side -> slow extraction,
even though the connection itself is fast. Pure metadata (ALL_IND_COLUMNS).

Usage:  python diagnostics/index_check.py [branch_key ...]
"""
from __future__ import annotations
import sys, tomllib
from pathlib import Path
import oracledb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script
from etl.config import resolve_oracle_client_lib_dir  # noqa: E402

OWNER = "OASIS"
# table -> columns the pipeline filters on (date + cdc)
WANTED = {
    "APPOINTMENTS": ["JULIAN_DATE", "AMEND_LAST_DATE"],
    "MASTER_DELIVERIES": ["DELIVERY_DATE", "AMEND_LAST_DATE"],
    "PATIENT_MASTER_DATA": ["AMEND_LAST_DATE"],
    "STAFF_MASTER_DATA": ["AMEND_LAST_DATE"],
}


def load():
    s = tomllib.loads((ROOT / ".dlt" / "secrets.toml").read_text("utf-8"))
    c = tomllib.loads((ROOT / ".dlt" / "config.toml").read_text("utf-8"))
    return s["oracle_branches"], c.get("etl", {})


def run(key, sec, dsn_mode):
    host, port = sec["host"], int(sec["port"])
    dsn = (oracledb.makedsn(host, port, sid=sec["database"]) if dsn_mode == "sid"
           else oracledb.makedsn(host, port, service_name=sec["database"]))
    conn = oracledb.connect(user=sec["username"], password=sec["password"],
                            dsn=dsn, tcp_connect_timeout=30)
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name, column_name, index_name, column_position "
        "FROM all_ind_columns WHERE table_owner = :o "
        "AND table_name IN (" +
        ",".join(f":t{i}" for i in range(len(WANTED))) + ")",
        [OWNER, *WANTED.keys()],
    )
    idx: dict[tuple[str, str], list] = {}
    for tn, cn, iname, pos in cur.fetchall():
        idx.setdefault((tn, cn), []).append((iname, pos))
    conn.close()
    print(f"\n=== {key} ({host}) ===")
    for tbl, cols in WANTED.items():
        for col in cols:
            hits = idx.get((tbl, col))
            if hits:
                lead = [i for i, p in hits if p == 1]
                tag = "INDEXED(leading)" if lead else "indexed(non-leading)"
            else:
                tag = "*** NO INDEX -> full scan ***"
            print(f"  {tbl:20s}.{col:18s} {tag}")


def main():
    branches, etl = load()
    dsn_mode = etl.get("dsn_mode", "service_name")
    if etl.get("thick_mode", True):
        try:
            oracledb.init_oracle_client(
                lib_dir=resolve_oracle_client_lib_dir(etl.get("oracle_client_lib_dir")))
        except Exception as e:
            print(f"[init] {e!r}")
    for key in (sys.argv[1:] or ["jazan", "alrabwah", "khamis"]):
        if key not in branches:
            print(f"\n=== {key}: NOT FOUND ===")
            continue
        try:
            run(key, branches[key], dsn_mode)
        except Exception as e:
            print(f"\n=== {key}: FAILED {e!r} ===")


if __name__ == "__main__":
    main()
