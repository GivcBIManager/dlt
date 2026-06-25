#!/usr/bin/env python
"""Oracle 11g multi-branch -> Iceberg ETL pipeline (dlt).

Reads table definitions from ``tables.json`` and branch connections from
``.dlt/secrets.toml`` (``[oracle_branches.*]``), extracts every table from all
branches in parallel, unifies their schemas, and writes one Iceberg dataset per
table plus ``etl_control`` and ``etl_run_log`` Iceberg tables.

Examples
--------
    # full initial load of every table from every branch
    python oracle_to_iceberg.py --mode INITIAL

    # incremental (CDC) load, limited to two branches and one table
    python oracle_to_iceberg.py --mode INCREMENTAL --branch alrabwah,khamis --tables APPOINTMENTS

    # exercise the whole chain offline (no Oracle), synthetic data
    python oracle_to_iceberg.py --mode INITIAL --self-test
"""

from __future__ import annotations

import argparse
import copy
import logging
import sys
import uuid
from datetime import datetime, timezone

from etl import config, iceberg_load, oracle_extract


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [p.strip() for p in value.replace(",", " ").split() if p.strip()]


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=[config.MODE_INITIAL, config.MODE_INCREMENTAL],
                   default=config.MODE_INCREMENTAL, help="load mode")
    p.add_argument("--branch", help="comma/space separated branch keys to load "
                                    "(default: all branches in secrets.toml)")
    p.add_argument("--tables", help="comma/space separated table object names to load "
                                    "(default: all tables in tables.json)")
    p.add_argument("--tables-file", default="tables.json", help="path to tables.json")
    p.add_argument("--category", choices=["masters", "transactions", "both"],
                   default="both",
                   help="which group to run; 'both' runs masters first, then "
                        "transactions as a separate phase (never together)")

    p.add_argument("--max-branch-workers", type=int, help="outer pool size (branches)")
    p.add_argument("--max-table-workers", type=int, help="inner pool size (tables/branch)")
    p.add_argument("--pool-max", type=int, help="max Oracle connections per branch")
    p.add_argument("--max-retries", type=int, help="connection-failure retries")
    p.add_argument("--retry-interval", type=int, dest="retry_interval_s",
                   help="seconds between connection-failure retries (default 300)")
    p.add_argument("--dsn-mode", choices=["service_name", "sid"],
                   help="how 'database' is interpreted in the DSN")
    p.add_argument("--thin", action="store_true",
                   help="use python-oracledb thin mode (Oracle 12.1+ only; "
                        "11g requires the default thick mode)")
    p.add_argument("--oracle-client-lib-dir",
                   help="path to Oracle Instant Client libs (thick mode)")

    p.add_argument("--staging-dir", help="local staging dir for extracted parquet")
    p.add_argument("--control-state", help="path to the local control_state.json")
    p.add_argument("--no-progress", action="store_true",
                   help="disable the live progress/memory heartbeat")
    p.add_argument("--progress-interval", type=float, dest="progress_interval_s",
                   help="seconds between progress heartbeats (default 5)")
    p.add_argument("--self-test", action="store_true",
                   help="generate synthetic data instead of querying Oracle")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


def build_overrides(args: argparse.Namespace) -> dict:
    """Translate CLI flags into Settings overrides (None -> keep config default)."""
    overrides: dict = {
        "mode": args.mode,
        "max_branch_workers": args.max_branch_workers,
        "max_table_workers": args.max_table_workers,
        "pool_max": args.pool_max,
        "max_retries": args.max_retries,
        "retry_interval_s": args.retry_interval_s,
        "dsn_mode": args.dsn_mode,
        "self_test": args.self_test,
        "oracle_client_lib_dir": args.oracle_client_lib_dir,
        "progress_interval_s": args.progress_interval_s,
    }
    if args.thin:
        overrides["thick_mode"] = False
    if args.no_progress:
        overrides["progress_enabled"] = False
    if args.staging_dir:
        overrides["staging_dir"] = args.staging_dir
    if args.control_state:
        overrides["control_state_path"] = args.control_state
    return overrides


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    log = logging.getLogger("etl")

    settings = config.load_settings(build_overrides(args))
    all_branches = config.load_branches()
    all_tables = config.load_table_defs(args.tables_file)

    # apply filters
    branch_filter = _parse_list(args.branch)
    if branch_filter:
        missing = [b for b in branch_filter if b not in all_branches]
        if missing:
            log.error("Unknown branch(es): %s; known: %s",
                      missing, sorted(all_branches))
            return 2
        branches = [all_branches[b] for b in branch_filter]
    else:
        branches = list(all_branches.values())

    table_filter = {t.upper() for t in _parse_list(args.tables)}
    if table_filter:
        tables = [t for t in all_tables if t.object_name.upper() in table_filter]
        if not tables:
            log.error("No tables matched %s", sorted(table_filter))
            return 2
    else:
        tables = all_tables

    # Split into masters and transactions; they run as separate, sequential
    # phases (masters first) and are never extracted/loaded together.
    masters = [t for t in tables if t.is_master]
    transactions = [t for t in tables if not t.is_master]
    phases: list[tuple[str, list]] = []
    if args.category in ("masters", "both"):
        phases.append(("masters", masters))
    if args.category in ("transactions", "both"):
        phases.append(("transactions", transactions))

    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    log.info("Run %s | mode=%s | branches=%s | phases=%s | dest=%s",
             run_id, settings.mode, [b.key for b in branches],
             [f"{n}({len(t)})" for n, t in phases], settings.destination_bucket_url)

    control = iceberg_load.ControlStore(settings.control_state_path).load()

    exit_code = 0
    for phase_name, phase_tables in phases:
        if not phase_tables:
            log.info("Phase '%s': no tables, skipping", phase_name)
            continue
        log.info("==== Phase '%s': %d table(s) ====", phase_name,
                 len(phase_tables))

        # Streaming extract + load for this phase only. Extraction reads an
        # isolated snapshot of the control state; the live store is mutated only
        # by the (serialized) load executor as tables complete.
        control_snapshot = copy.deepcopy(control.as_dict())

        def run_extraction_fn(on_table_done, _tables=phase_tables, _snap=control_snapshot):
            return oracle_extract.run_extraction(
                branches, _tables, settings, _snap, on_table_done=on_table_done)

        summary = iceberg_load.load_and_record(
            run_extraction_fn, phase_tables, settings, control, run_id,
            total_branches=len(all_branches), branches_in_run=len(branches))
        print(f"[{phase_name}] " + summary.render())

        results = summary.results
        ok = sum(1 for r in results if r.status == "SUCCESS")
        failed = [r for r in results if r.status != "SUCCESS"]
        log.info("Phase '%s' extraction: %d/%d units succeeded, %d failed",
                 phase_name, ok, len(results), len(failed))
        for r in failed:
            log.warning("  FAILED %s/%s after %d attempt(s): %s",
                        r.branch, r.table, r.attempts, r.error)

        if summary.extraction_error is not None:
            log.error("Phase '%s' aborted by a non-connection error: %s; "
                      "later phases will not run", phase_name,
                      summary.extraction_error)
            return 2
        if failed or any(p.load_status == "FAILED" for p in summary.plans):
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
