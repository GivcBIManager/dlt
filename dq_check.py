#!/usr/bin/env python
"""Data-quality check: Oracle (source) vs Iceberg (lake) reconciliation per branch.

Runs two checks for every ``(table, branch)`` over one shared window
(**YTD .. last run** by default), then writes the results to the Iceberg table
``etl_dq_results`` in the same dataset as the pipeline output and prints a
summary:

* **row-count comparison** -- windowed ``COUNT(*)`` on Oracle vs the Iceberg
  branch partition, and their delta.
* **row-hash delta** -- a per-row content hash on both sides, joined on the
  table's unique key, bucketed into matched / only-in-oracle / only-in-iceberg /
  hash-mismatch.

The window is the same for both checks: from ``--since`` (default Jan 1 of the
current year) up to each ``(table, branch)``'s last-run watermark from
``control_state.json`` (override with ``--until``).

Examples
--------
    # all tables, all branches; write etl_dq_results + print summary
    python dq_check.py

    # scope to branches/tables, counts only (skip the hash pull)
    python dq_check.py --branch jazan,khamis --tables APPOINTMENTS --no-hash

    # explicit window, also dump a CSV, don't write the Iceberg table
    python dq_check.py --since 2026-06-01 --until 2026-06-23 --csv exports --no-write

    # offline: reconcile the lake against the staged parquet (no Oracle)
    python dq_check.py --self-test
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import uuid
from datetime import datetime, timezone

from etl import config, dq_check
from etl.iceberg_load import ControlStore


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [p.strip() for p in value.replace(",", " ").split() if p.strip()]


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--branch", help="comma/space separated branch keys (default: all)")
    p.add_argument("--tables", help="comma/space separated table object names (default: all)")
    p.add_argument("--tables-file", default="tables.json", help="path to tables.json")

    p.add_argument("--since", help="window lower bound YYYY-MM-DD "
                                   "(default: Jan 1 of the current year)")
    p.add_argument("--until", help="window upper bound YYYY-MM-DD "
                                   "(default: each table+branch's last-run watermark)")
    p.add_argument("--year", type=int, help="year whose Jan 1 is the default --since")

    p.add_argument("--no-hash", action="store_true",
                   help="row-count comparison only; skip the (heavier) hash pull")
    p.add_argument("--self-test", action="store_true",
                   help="reconcile the lake against _staging parquet instead of Oracle")

    p.add_argument("--no-write", action="store_true",
                   help="don't write the etl_dq_results Iceberg table (print only)")
    p.add_argument("--csv", metavar="DIR", help="also write the full results as CSV into DIR")
    p.add_argument("--max-workers", type=int, help="parallel branch workers (default: etl setting)")

    p.add_argument("--dsn-mode", choices=["service_name", "sid"],
                   help="how 'database' is interpreted in the DSN")
    p.add_argument("--oracle-client-lib-dir", help="Oracle Instant Client libs (thick mode)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    log = logging.getLogger("etl.dq")

    overrides = {"dsn_mode": args.dsn_mode,
                 "oracle_client_lib_dir": args.oracle_client_lib_dir}
    settings = config.load_settings({k: v for k, v in overrides.items() if v})
    all_branches = config.load_branches()
    all_tables = config.load_table_defs(args.tables_file)

    branch_filter = _parse_list(args.branch)
    if branch_filter:
        missing = [b for b in branch_filter if b not in all_branches]
        if missing:
            log.error("Unknown branch(es): %s; known: %s", missing, sorted(all_branches))
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

    year = args.year or datetime.now().year
    since = _parse_date(args.since) or dt.date(year, 1, 1)
    until = _parse_date(args.until)

    control = ControlStore(settings.control_state_path).load().as_dict()
    run_id = f"dq-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"

    log.info("DQ run %s | %s | branches=%s | tables=%d | window=%s..%s | hash=%s",
             run_id, "SELF-TEST" if args.self_test else settings.destination_bucket_url,
             [b.key for b in branches], len(tables), since,
             until or "(per-branch watermark)", not args.no_hash)

    results = dq_check.run_dq(
        tables, branches, settings, control, since, until,
        do_hash=not args.no_hash, self_test=args.self_test, max_workers=args.max_workers)

    print(dq_check.render_summary(results, do_hash=not args.no_hash))

    if args.csv:
        from pathlib import Path
        out = Path(args.csv) / f"{run_id}.csv"
        dq_check.write_csv(results, out, run_id, settings)
        print(f"\n-> wrote CSV {out}")

    if not args.no_write:
        name = dq_check.write_results_iceberg(results, settings, run_id)
        print(f"-> wrote {len(results)} row(s) to Iceberg table "
              f"'{settings.dataset_name}.{name}'")

    # A MISMATCH is a *successful* check that found drift -- it's recorded in
    # etl_dq_results and the summary, not a run failure, so it still exits 0.
    # Only a genuine ERROR (the check itself could not complete) exits non-zero,
    # so the run shows "finished" whenever the reconciliation actually ran.
    errored = [r for r in results if r.status == "ERROR"]
    if errored:
        log.error("%d unit(s) errored: %s", len(errored),
                  ", ".join(f"{r.table}/{r.branch}" for r in errored))
    return 1 if errored else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
