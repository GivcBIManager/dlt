#!/usr/bin/env python
"""Compact fragmented Iceberg partitions into few large, sorted files.

Thin CLI over ``etl.iceberg_load.apply_compaction`` -- the same code path the
pipeline runs when ``[etl] compaction = true``, so the CLI and the scheduled
maintenance can never drift apart.

Each rewrite is a single atomic ``overwrite`` scoped to one partition, and the
row count is verified per partition and again for the whole table, so a failed
or interrupted run leaves the table on its previous snapshot rather than
half-written.

    python maintenance/compact.py                        # dry run, whole dataset
    python maintenance/compact.py -t patient_ad          # dry run, one table
    python maintenance/compact.py -t patient_ad --apply
    python maintenance/compact.py --all --apply          # everything
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

# maintenance/ is a subdirectory of the repo root, where the etl package lives.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.config import Settings
from etl.iceberg_load import apply_compaction


def _report(settings: Settings, tables: list[str] | None) -> int:
    """Print what --apply would rewrite, without touching anything."""
    from dlt.common.libs.pyiceberg import get_catalog

    from etl.iceberg_load import COMPACT_PEAK_MULTIPLIER

    catalog = get_catalog()
    wanted = set(tables) if tables else None
    tot_files = tot_saved = 0

    for ident in catalog.list_tables(settings.dataset_name):
        name = ident[-1] if isinstance(ident, tuple) else str(ident).rsplit(".", 1)[-1]
        if name.startswith("_dlt") or (wanted is not None and name not in wanted):
            continue
        try:
            tbl = catalog.load_table(ident)
            if not tbl.spec().fields:
                continue
            tasks = list(tbl.scan().plan_files())
            tot_files += len(tasks)
            sample = tbl.scan(limit=settings.compact_sample_rows).to_arrow()
            if not sample.num_rows:
                continue
            bytes_per_row = sample.nbytes / sample.num_rows
            budget_rows = max(1, int(settings.compact_max_memory_bytes
                                     / COMPACT_PEAK_MULTIPLIER / max(bytes_per_row, 1)))

            by_part: dict = {}
            for task in tasks:
                by_part.setdefault(tuple(task.file.partition), []).append(task)

            rows = []
            for key, part in by_part.items():
                small = [t for t in part
                         if t.file.file_size_in_bytes < settings.compact_small_file_bytes]
                if len(small) < settings.compact_min_files:
                    continue
                nrows = sum(t.file.record_count for t in small)
                nbytes = sum(t.file.file_size_in_bytes for t in small)
                # output files are sized by DECODED bytes against the write
                # target; batching only bounds memory, it does not change the
                # result. A partition that would not shrink is not listed.
                target = int(tbl.properties.get(
                    "write.target-file-size-bytes",
                    settings.write_target_file_size_bytes))
                est_out = max(1, round(nrows * bytes_per_row / target))
                if est_out > len(small) * settings.compact_min_gain_ratio:
                    continue
                rows.append((key, len(small), nbytes, est_out,
                             max(1, -(-nrows // budget_rows))))
            if not rows:
                continue
            print(f"\n{name}: {len(tasks)} files "
                  f"({bytes_per_row * 1.0:.0f} B/row decoded, "
                  f"{budget_rows:,} rows/batch)")
            for key, n, nbytes, est_out, nbatch in sorted(rows, key=lambda r: -r[1]):
                print(f"  {str(key):<12} {n:>5} small files "
                      f"({nbytes/2**30:6.2f} GB) -> ~{est_out} file(s) "
                      f"in {nbatch} batch(es)")
                tot_saved += n - est_out
        except Exception as exc:  # noqa: BLE001
            print(f"{name}: FAILED {type(exc).__name__}: {exc}", file=sys.stderr)

    print(f"\nlive files now: {tot_files}; ~{tot_saved} removable by compaction")
    print("re-run with --apply to rewrite")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--table", action="append", help="limit to these tables")
    ap.add_argument("--all", action="store_true", help="every table in the dataset")
    ap.add_argument("--apply", action="store_true", help="actually rewrite")
    ap.add_argument("--min-files", type=int,
                    help="override etl.compact_min_files")
    ap.add_argument("--max-memory-gb", type=float,
                    help="override etl.compact_max_memory_bytes (PEAK RSS budget per "
                         "partition; raise it when nothing else is running)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    settings = Settings()
    if args.min_files is not None:
        settings.compact_min_files = args.min_files
    if args.max_memory_gb is not None:
        settings.compact_max_memory_bytes = int(args.max_memory_gb * 2**30)

    tables = args.table
    if not tables and not args.all and args.apply:
        ap.error("--apply needs -t <table> or --all")

    if not args.apply:
        return _report(settings, tables)

    apply_compaction(settings, tables=tables or [])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
