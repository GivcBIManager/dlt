"""How are jazan's APPOINTMENTS.JULIAN_DATE values distributed above the floor?

22.4M rows match JULIAN_DATE >= 2026-06-20 on jazan vs 0.94M on alrabwah. This
shows whether that mass is real near-future volume or a tail of garbage far-future
dates (which would argue for an upper bound / data fix vs a different watermark
column). Buckets the matching rows by appointment YEAR. One index-driven scan.

Usage:  python diagnostics/appt_year_dist.py [branch_key ...]   (default: jazan alrabwah)
"""
from __future__ import annotations

import sys
import tomllib
from pathlib import Path

import oracledb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from etl.config import resolve_oracle_client_lib_dir  # noqa: E402

FLOOR = "TO_NUMBER(TO_CHAR(TO_DATE('2026-06-20','YYYY-MM-DD'),'J'))"
SQL = f"""
SELECT EXTRACT(YEAR FROM TO_DATE(julian_date,'J')) AS yr, COUNT(*) AS n
FROM oasis.appointments
WHERE julian_date >= {FLOOR}
GROUP BY EXTRACT(YEAR FROM TO_DATE(julian_date,'J'))
ORDER BY yr
"""


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
    cur.execute(SQL)
    rows = cur.fetchall()
    conn.close()
    total = sum(n for _, n in rows)
    print(f"\n=== {key} ({host})  total matching = {total:,} ===")
    for yr, n in rows:
        bar = "#" * min(60, int(60 * n / max(total, 1)))
        print(f"  {int(yr)}  {n:>12,}  {bar}")


def main():
    branches, etl = load()
    dsn_mode = etl.get("dsn_mode", "service_name")
    if etl.get("thick_mode", True):
        try:
            oracledb.init_oracle_client(
                lib_dir=resolve_oracle_client_lib_dir(etl.get("oracle_client_lib_dir")))
        except Exception as e:  # noqa: BLE001
            print(f"[init] {e!r}")
    for key in (sys.argv[1:] or ["jazan", "alrabwah"]):
        if key not in branches:
            print(f"\n=== {key}: NOT FOUND ===")
            continue
        try:
            run(key, branches[key], dsn_mode)
        except Exception as e:  # noqa: BLE001
            print(f"\n=== {key}: FAILED {e!r} ===")


if __name__ == "__main__":
    main()
