#!/usr/bin/env python
"""Delete Iceberg data files no longer referenced by any live snapshot.

pyiceberg 0.11 expires snapshots but never deletes the data files they orphan,
so the lake accumulates dead parquet indefinitely. This reclaims it.

SAFETY: only files older than --min-age-hours are considered, so a file being
written by an in-flight commit is never touched. Dry-run by default.

    python maintenance/remove_orphan_files.py                 # report only
    python maintenance/remove_orphan_files.py --apply         # delete
    python maintenance/remove_orphan_files.py --apply -t doc  # one table
"""
from __future__ import annotations
import argparse, glob, json, os, sys, time
from pyiceberg.table import StaticTable

ROOT = os.environ.get("OASIS_LAKE", "/var/lib/clickhouse/user_files/iceberg_output/oasis")


def latest_metadata(tdir: str) -> str | None:
    mds = glob.glob(os.path.join(tdir, "metadata", "*.metadata.json"))
    if not mds:
        return None
    return max(mds, key=lambda p: json.load(open(p)).get("last-updated-ms", 0))


def live_paths(tbl) -> set[str]:
    """Every data file reachable from ANY retained snapshot, not just current."""
    live: set[str] = set()
    for snap in tbl.metadata.snapshots:
        for mf in snap.manifests(tbl.io):
            for entry in mf.fetch_manifest_entry(tbl.io, discard_deleted=False):
                live.add(os.path.realpath(entry.data_file.file_path.replace("file://", "")))
    return live


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry run)")
    ap.add_argument("-t", "--table", action="append", help="limit to these tables")
    ap.add_argument("--min-age-hours", type=float, default=24.0,
                    help="never touch files younger than this (default 24)")
    args = ap.parse_args()

    cutoff = time.time() - args.min_age_hours * 3600
    tables = args.table or sorted(os.listdir(ROOT))
    tot_n = tot_b = 0

    for t in tables:
        tdir = os.path.join(ROOT, t)
        md = latest_metadata(tdir)
        if not md:
            continue
        try:
            live = live_paths(StaticTable.from_metadata(f"file://{md}"))
        except Exception as e:
            print(f"  !! {t}: cannot read metadata ({type(e).__name__}) -- SKIPPED", file=sys.stderr)
            continue

        n = b = skipped = 0
        for p in glob.glob(os.path.join(tdir, "data", "**", "*.parquet"), recursive=True):
            if os.path.realpath(p) in live:
                continue
            st = os.stat(p)
            if st.st_mtime > cutoff:      # too young: may be an uncommitted write
                skipped += 1
                continue
            n += 1
            b += st.st_size
            if args.apply:
                os.remove(p)
        if n or skipped:
            note = f"  ({skipped} too young, kept)" if skipped else ""
            print(f"{t:<30} {n:>6} orphans  {b/2**30:>7.2f} GB{note}")
        tot_n += n
        tot_b += b

    verb = "DELETED" if args.apply else "would delete"
    print(f"\n{verb}: {tot_n} files, {tot_b/2**30:.1f} GB")
    if not args.apply and tot_n:
        print("re-run with --apply to reclaim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
