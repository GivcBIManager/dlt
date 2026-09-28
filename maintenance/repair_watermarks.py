#!/usr/bin/env python
"""Repair future-dated control_state watermarks and seed insert-key watermarks.

Thin CLI over ``etl.watermark_repair``. Dry run by default; ``--apply`` writes.
Never ``--apply`` while a pipeline run is in flight: the run's end-of-run save
would write the old values back.

    python maintenance/repair_watermarks.py future                     # report all
    python maintenance/repair_watermarks.py future --field last_cdc \\
        --only staff_contracts/unaizah --only item_barcode/jazan --apply
    python maintenance/repair_watermarks.py seed-key -t delivery_charge --since 2026-09-01
    python maintenance/repair_watermarks.py seed-key -t delivery_charge --since 2026-09-01 \\
        --branch Muhayil --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# maintenance/ is a subdirectory of the repo root, where the etl package lives.
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from etl import config
from etl import watermark_repair as wr
from etl.iceberg_load import ControlStore
from etl.metastore import MetaStore


def _context():
    settings = config.load_settings({})
    if settings.postgres is None:
        raise SystemExit("no [postgres] config in .dlt/secrets.toml")
    branches = config.load_branches()
    tdefs = {t.dataset_table_name: t
             for t in config.load_table_defs(REPO / "tables.json")}
    store = ControlStore(MetaStore(settings.postgres)).load()
    return settings, branches, tdefs, store


def cmd_future(args) -> int:
    settings, branches, tdefs, store = _context()
    now = config.now_local()
    marks = wr.find_future_marks(store.as_dict(), tdefs, now)
    if args.field:
        marks = [m for m in marks if m.field == args.field]
    if args.only:
        wanted = set(args.only)
        marks = [m for m in marks if f"{m.table}/{m.branch}" in wanted]
    if not marks:
        print("no future-dated watermarks match")
        return 0

    lake_max = wr.lake_max_real(settings, branches)
    updates = []
    for m in marks:
        new = wr.repair_value(m, lake_max)
        verdict = new or ("manual: helper-driven" if m.column is None else "manual: no lake rows")
        print(f"{m.table:30s} {m.branch:9s} {m.field:9s} {m.value:28s} -> {verdict}")
        if new:
            updates.append((m.table, m.branch, m.field, {"value": new, "kind": "datetime"}))

    if not args.apply:
        print(f"\ndry run: {len(updates)} repairable mark(s); re-run with --apply to write")
        return 0
    wr.apply_marks(store, updates)
    print(f"\nwrote {len(updates)} mark(s) to control_state")
    return 0


def cmd_seed_key(args) -> int:
    import datetime as dt

    settings, branches, tdefs, store = _context()
    tdef = tdefs.get(args.table)
    if tdef is None or not tdef.insert_key_column:
        raise SystemExit(f"{args.table}: no 'insert_key_column' in tables.json")
    if not tdef.where_date_column:
        raise SystemExit(f"{args.table}: needs a 'where_date_column' to pick the floor by date")
    since = dt.datetime.strptime(args.since, "%Y-%m-%d")
    wanted = args.branch or sorted(branches)
    from etl.dq_check import _norm

    updates = []
    for b in wanted:
        tbl = wr.lake_column(settings, branches, args.table, b,
                             [tdef.insert_key_column, tdef.where_date_column])
        floor = (wr.min_key_since(tbl.column(_norm(tdef.insert_key_column)),
                                  tbl.column(_norm(tdef.where_date_column)), since)
                 if tbl is not None and tbl.num_rows else None)
        current = (store.entry(args.table, b).get("last_key") or {}).get("value")
        print(f"{args.table:20s} {b:9s} last_key {str(current):14s} -> {floor or 'skip: no lake rows'}")
        if floor:
            updates.append((args.table, b, "last_key", wr.seed_key_mark(floor)))

    if not args.apply:
        print(f"\ndry run: {len(updates)} branch(es) would be seeded; re-run with --apply")
        return 0
    wr.apply_marks(store, updates)
    print(f"\nseeded {len(updates)} branch(es)")
    return 0


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("future", help="reset future-dated watermarks to the lake's max")
    f.add_argument("--only", action="append", metavar="TABLE/BRANCH",
                   help="restrict to these units (repeatable)")
    f.add_argument("--field", choices=["last_cdc", "last_date"])
    f.add_argument("--apply", action="store_true", help="write (default: dry run)")
    f.set_defaults(func=cmd_future)
    s = sub.add_parser("seed-key", help="seed last_key to the lake's first key since a date")
    s.add_argument("-t", "--table", required=True, help="dataset table name, e.g. delivery_charge")
    s.add_argument("--since", required=True, metavar="YYYY-MM-DD",
                   help="re-read every row inserted since this date that the lake lacks")
    s.add_argument("--branch", action="append", help="restrict to these branches (repeatable)")
    s.add_argument("--apply", action="store_true", help="write (default: dry run)")
    s.set_defaults(func=cmd_seed_key)
    return p.parse_args(argv)


def main(argv) -> int:
    logging.basicConfig(level=logging.WARNING)
    args = parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
