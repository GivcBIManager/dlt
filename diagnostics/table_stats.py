"""Compare table sizes per branch via optimizer stats (no table scans).

Reads NUM_ROWS / LAST_ANALYZED from ALL_TABLES (cheap metadata) for the OASIS
tables, so we can see whether a branch is slow simply because it has far more
rows to pull on a full (un-watermarked) load.

Usage:  python diagnostics/table_stats.py [branch_key ...]
"""
from __future__ import annotations
import sys, tomllib
from pathlib import Path
import oracledb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script
from etl.config import resolve_oracle_client_lib_dir  # noqa: E402

TABLES = ["STAFF_MASTER_DATA", "PATIENT_MASTER_DATA", "CODES_DATA",
          "MASTER_DELIVERIES", "APPOINTMENTS"]
OWNER = "OASIS"


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
        "SELECT table_name, num_rows, last_analyzed FROM all_tables "
        "WHERE owner = :o AND table_name IN "
        "(" + ",".join(f":t{i}" for i in range(len(TABLES))) + ")",
        [OWNER, *TABLES],
    )
    rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    conn.close()
    print(f"\n=== {key} ({host}) ===")
    for t in TABLES:
        nr, la = rows.get(t, (None, None))
        nr_s = f"{nr:>12,}" if nr is not None else "    (no stats)"
        la_s = la.date().isoformat() if la else "n/a"
        print(f"  {t:22s} {nr_s} rows   analyzed:{la_s}")


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
