"""Quantify per-branch fetch throughput using the pipeline's fetch settings.

Generates synthetic rows server-side (CONNECT BY on dual) so NO business table
is touched, then times pulling them with the same arraysize/prefetchrows the
real extractor uses. Shows whether high network latency makes a branch's data
pull slow even when login is fast.

Usage:  python diagnostics/throughput_test.py [branch_key ...]
"""

from __future__ import annotations

import sys
import time
import tomllib
from pathlib import Path

import oracledb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script
from etl.config import resolve_oracle_client_lib_dir  # noqa: E402

ROWS = 50_000
ROW_BYTES = 200
DEFAULT_BATCH = 10_000  # fallback when a branch sets no fetch_batch_size

GEN_SQL = (
    f"SELECT level AS n, RPAD('x', {ROW_BYTES}, 'x') AS payload "
    f"FROM dual CONNECT BY level <= {ROWS}"
)


def load():
    secrets = tomllib.loads((ROOT / ".dlt" / "secrets.toml").read_text("utf-8"))
    config = tomllib.loads((ROOT / ".dlt" / "config.toml").read_text("utf-8"))
    return secrets["oracle_branches"], config.get("etl", {})


def run(key, sec, dsn_mode):
    host, port = sec["host"], int(sec["port"])
    batch = int(sec.get("fetch_batch_size", DEFAULT_BATCH))  # the branch's own size
    dsn = (oracledb.makedsn(host, port, sid=sec["database"]) if dsn_mode == "sid"
           else oracledb.makedsn(host, port, service_name=sec["database"]))
    conn = oracledb.connect(user=sec["username"], password=sec["password"],
                            dsn=dsn, tcp_connect_timeout=30)
    cur = conn.cursor()
    cur.arraysize = batch
    cur.prefetchrows = batch + 1
    t = time.perf_counter()
    cur.execute(GEN_SQL)
    n = 0
    while True:
        rows = cur.fetchmany(batch)
        if not rows:
            break
        n += len(rows)
    dur = time.perf_counter() - t
    conn.close()
    print(f"  {key:10s}: {n:,} rows in {dur:6.2f}s  "
          f"({n/dur:,.0f} rows/s, {n*ROW_BYTES/1e6/dur:5.2f} MB/s, batch={batch:,})")


def main():
    branches, etl = load()
    dsn_mode = etl.get("dsn_mode", "service_name")
    if etl.get("thick_mode", True):
        try:
            oracledb.init_oracle_client(
                lib_dir=resolve_oracle_client_lib_dir(etl.get("oracle_client_lib_dir")))
        except Exception as e:
            print(f"[init] {e!r}")
    print(f"Pulling {ROWS:,} synthetic rows (~{ROW_BYTES}B each); "
          f"batch is each branch's own fetch_batch_size\n")
    for key in (sys.argv[1:] or ["jazan", "alrabwah", "khamis"]):
        if key not in branches:
            print(f"  {key:10s}: NOT FOUND")
            continue
        try:
            run(key, branches[key], dsn_mode)
        except Exception as e:
            print(f"  {key:10s}: FAILED  {e!r}")


if __name__ == "__main__":
    main()
