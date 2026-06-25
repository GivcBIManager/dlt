"""Diagnose Jazan (or any) branch connection latency.

Reads credentials from .dlt/secrets.toml (nothing hard-coded / printed) and
times each phase separately so we can see *where* a slow connection spends its
time:

    1. raw TCP connect to host:port      (network / listener reachability)
    2. Oracle login                       (service handoff + authentication)
    3. a trivial round-trip query         (session usability)

Usage:  python diagnostics/diagnose_jazan.py [branch_key ...]   (default: jazan)
"""

from __future__ import annotations

import socket
import sys
import time
import tomllib
from pathlib import Path

import oracledb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script
from etl.config import resolve_oracle_client_lib_dir  # noqa: E402

SECRETS = ROOT / ".dlt" / "secrets.toml"
CONFIG = ROOT / ".dlt" / "config.toml"
TCP_TIMEOUT = 10
LOGIN_TIMEOUT = 30


def load():
    secrets = tomllib.loads(SECRETS.read_text(encoding="utf-8"))
    config = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    etl = config.get("etl", {})
    return secrets["oracle_branches"], etl


def time_branch(key: str, sec: dict, etl: dict) -> None:
    host, port = sec["host"], int(sec["port"])
    dsn_mode = etl.get("dsn_mode", "service_name")
    if dsn_mode == "sid":
        dsn = oracledb.makedsn(host, port, sid=sec["database"])
    else:
        dsn = oracledb.makedsn(host, port, service_name=sec["database"])

    print(f"\n=== {key}  ({host}:{port}  {dsn_mode}={sec['database']}) ===")

    # 1) raw TCP
    t = time.perf_counter()
    try:
        socket.create_connection((host, port), timeout=TCP_TIMEOUT).close()
        print(f"  [1] TCP connect     : OK   {time.perf_counter()-t:6.2f}s")
    except Exception as e:
        print(f"  [1] TCP connect     : FAIL {time.perf_counter()-t:6.2f}s  {e!r}")
        return

    # 2) Oracle login
    t = time.perf_counter()
    try:
        conn = oracledb.connect(
            user=sec["username"], password=sec["password"], dsn=dsn,
            tcp_connect_timeout=LOGIN_TIMEOUT,
        )
        print(f"  [2] Oracle login    : OK   {time.perf_counter()-t:6.2f}s")
    except Exception as e:
        print(f"  [2] Oracle login    : FAIL {time.perf_counter()-t:6.2f}s  {e!r}")
        return

    # 3) trivial query
    t = time.perf_counter()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dual")
        cur.fetchone()
        print(f"  [3] SELECT 1 query  : OK   {time.perf_counter()-t:6.2f}s")
    except Exception as e:
        print(f"  [3] SELECT 1 query  : FAIL {time.perf_counter()-t:6.2f}s  {e!r}")
    finally:
        conn.close()


def main() -> None:
    branches, etl = load()
    if etl.get("thick_mode", True):
        lib = resolve_oracle_client_lib_dir(etl.get("oracle_client_lib_dir"))
        try:
            oracledb.init_oracle_client(lib_dir=lib)
            print(f"[init] thick client OK (lib_dir={lib or 'system path'})")
        except Exception as e:
            print(f"[init] thick init failed: {e!r}")

    keys = sys.argv[1:] or ["jazan"]
    for key in keys:
        if key not in branches:
            print(f"\n=== {key} : NOT FOUND in secrets.toml ===")
            continue
        time_branch(key, branches[key], etl)


if __name__ == "__main__":
    main()
