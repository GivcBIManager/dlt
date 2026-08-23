#!/usr/bin/env python
"""Bootstrap the two Postgres databases this app needs (setup step).

Postgres itself is an EXTERNAL prerequisite -- a server that is already running
somewhere. What this script owns is everything *inside* it:

- ``oasis_catalog`` -- the Iceberg SQL catalog database (pyiceberg ``SqlCatalog``).
  Created if missing; its ``iceberg_tables`` / ``iceberg_namespace_properties``
  tables are left to pyiceberg, which creates them on first use.
- ``oasis_meta``    -- the app metastore database, plus the ``etl_meta`` schema
  and its four tables (``control_state``, ``etl_control``, ``etl_run_log``,
  ``etl_dq_results``) via ``etl.metastore.MetaStore.ensure_schema()``.

Both targets come from ``.dlt/secrets.toml`` (``[postgres]`` and
``[iceberg_catalog.iceberg_catalog_config].uri``) -- the same per-host file the
pipeline reads, so there is nothing extra to configure for this step.
``CREATE DATABASE`` is issued against the server's ``postgres`` maintenance
database using those same credentials.

Everything here is idempotent: re-running against a fully provisioned server
reports "ok" and changes nothing.

    python setup_postgres.py            # create what's missing, then verify
    python setup_postgres.py --check    # verify only; never creates a database

Exit codes:  0 = ready   1 = failed   2 = not configured (nothing to do)
"""
from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit

REPO_ROOT = Path(__file__).resolve().parent
SECRETS_TOML = REPO_ROOT / ".dlt" / "secrets.toml"

# Database that always exists on a stock server -- CREATE DATABASE has to be
# issued from *some* other database than the one being created.
MAINTENANCE_DB = "postgres"

NOT_CONFIGURED = 2


def _load_toml(path: Path) -> dict:
    try:
        import tomllib
    except ModuleNotFoundError:          # Python 3.10 -- backport from requirements-gui.txt
        import tomli as tomllib          # type: ignore[no-redef]
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _target_from_postgres_section(secrets: dict) -> dict | None:
    """The app metastore (``oasis_meta``) from the ``[postgres]`` block."""
    sec = secrets.get("postgres") or {}
    if not sec:
        return None
    return {
        "label": "app metastore",
        "host": str(sec["host"]),
        "port": int(sec.get("port", 5432)),
        "database": str(sec["database"]),
        "username": str(sec["username"]),
        "password": str(sec["password"]),
        "schema": str(sec.get("schema", "etl_meta")),
    }


def _target_from_catalog_uri(secrets: dict) -> dict | None:
    """The Iceberg SQL catalog (``oasis_catalog``) from the catalog URI.

    The URI is a SQLAlchemy URL (``postgresql+psycopg2://user:pass@host:port/db``);
    only Postgres catalogs are ours to create -- a sqlite/other-backend catalog
    is left alone.
    """
    uri = ((secrets.get("iceberg_catalog") or {})
           .get("iceberg_catalog_config") or {}).get("uri")
    if not uri:
        return None
    parts = urlsplit(str(uri))
    if not parts.scheme.split("+")[0].startswith("postgres"):
        return None
    database = parts.path.lstrip("/")
    if not database:
        return None
    return {
        "label": "Iceberg SQL catalog",
        "host": parts.hostname or "localhost",
        "port": int(parts.port or 5432),
        "database": database,
        # Credentials are percent-encoded in the URL (a password may contain @).
        "username": unquote(parts.username or ""),
        "password": unquote(parts.password or ""),
        "schema": None,                  # pyiceberg owns this database's tables
    }


@contextmanager
def _connect(target: dict, database: str):
    """Open an autocommit connection to ``database`` on the target's server.

    Autocommit because CREATE DATABASE cannot run inside a transaction block.
    Note this closes the connection but deliberately does NOT use psycopg2's own
    ``with connection`` block: that one opens a transaction (leaving the session
    INTRANS after the very first SELECT) and CREATE DATABASE would then fail
    even with autocommit set.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=target["host"], port=target["port"], dbname=database,
        user=target["username"], password=target["password"],
        connect_timeout=10,
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def _ensure_database(target: dict, *, check_only: bool) -> bool:
    """Create the target database if the server does not have it yet.

    Returns True if it exists (or was just created), False if it is missing and
    ``--check`` forbade creating it.
    """
    from psycopg2 import sql

    with _connect(target, MAINTENANCE_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s",
                    (target["database"],))
        if cur.fetchone():
            print(f"    database '{target['database']}' already exists")
            return True
        if check_only:
            print(f"    MISSING: database '{target['database']}' "
                  f"(re-run without --check to create it)")
            return False
        cur.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(target["database"])))
        print(f"    created database '{target['database']}'")
        return True


def _ensure_metastore_schema(target: dict) -> None:
    """Create ``etl_meta`` + its tables via the app's own idempotent DDL."""
    from etl.config import PostgresConfig
    from etl.metastore import MetaStore

    cfg = PostgresConfig(
        host=target["host"], port=target["port"], database=target["database"],
        username=target["username"], password=target["password"],
        schema=target["schema"],
    )
    MetaStore(cfg).ensure_schema()
    print(f"    schema '{target['schema']}' ready "
          f"(control_state, etl_control, etl_run_log, etl_dq_results)")


def _verify(target: dict) -> None:
    with _connect(target, target["database"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT version()")
        version = cur.fetchone()[0].split(",")[0]
    print(f"    connected: {version}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="verify connectivity/existence only; never CREATE DATABASE")
    args = ap.parse_args()

    if not SECRETS_TOML.is_file():
        print(f"Postgres: no {SECRETS_TOML} on this host — "
              f"skipping (see README.md 'Postgres')")
        return NOT_CONFIGURED
    secrets = _load_toml(SECRETS_TOML)

    targets = [t for t in (_target_from_catalog_uri(secrets),
                           _target_from_postgres_section(secrets)) if t]
    if not targets:
        print("Postgres: neither [postgres] nor "
              "[iceberg_catalog.iceberg_catalog_config].uri is set in "
              ".dlt/secrets.toml — skipping (see README.md 'Postgres')")
        return NOT_CONFIGURED

    incomplete = False
    for target in targets:
        print(f"--> {target['label']}: "
              f"{target['username']}@{target['host']}:{target['port']}/{target['database']}")
        if not _ensure_database(target, check_only=args.check):
            incomplete = True
            continue
        _verify(target)
        if target["schema"]:
            if args.check:
                print(f"    (skipping '{target['schema']}' schema DDL under --check)")
            else:
                _ensure_metastore_schema(target)

    if incomplete:
        print("Postgres: incomplete — see MISSING above.")
        return 1
    print("Postgres: ready.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:             # noqa: BLE001 - a setup step reports, it doesn't traceback
        print(f"Postgres setup FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        print("Check that the server is running and that the credentials in "
              ".dlt/secrets.toml can connect and CREATE DATABASE.", file=sys.stderr)
        sys.exit(1)
