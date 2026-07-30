"""Merge already-staged parquet into Iceberg, without re-extracting from Oracle.

Recovery driver for tables whose EXTRACTION succeeded but whose LOAD failed --
their staged parquet is still on disk under ``_staging/<table>/<branch>.parquet``
(``cleanup_staging_after_load`` only deletes it once the rows are committed).
Re-running the normal pipeline would pull every row from Oracle again; this
replays the staged files straight into the existing load path.

It reuses ``_load_one_table`` rather than reimplementing the merge, so the
recovery gets the same insert_at carry-forward, destination-type widening,
merge-hash handling, single-commit merge and snapshot squashing as a real run.

Watermarks are deliberately NOT advanced: every ExtractResult carries an empty
Watermark, and ``_wm_advance`` returns the stored value unchanged when the new
one is None. The next scheduled incremental therefore re-reads the same window
and merges it idempotently -- a recovery load must not claim CDC progress it
did not observe.

Usage
-----
    python load_staged.py --list                # what is staged, and how big
    python load_staged.py --tables bintran --dry-run
    python load_staged.py --tables bintran      # merge one table
    python load_staged.py                       # merge everything staged
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

from etl.config import load_branches, load_settings, load_table_defs
from etl.iceberg_load import (
    ControlStore,
    _install_single_commit_merge,
    _load_one_table,
    _table_pipeline_name,
    build_pipeline,
)
from etl.metastore import MetaStore
from etl.oracle_extract import ExtractResult
from etl.progress import PipelineMonitor

log = logging.getLogger("load_staged")


def _staged_tables(staging_dir: Path) -> dict[str, list[Path]]:
    """``{table_dir_name: [branch parquet, ...]}`` for every non-empty dir."""
    found = {}
    for table_dir in sorted(p for p in staging_dir.iterdir() if p.is_dir()):
        files = sorted(table_dir.glob("*.parquet"))
        if files:
            found[table_dir.name] = files
    return found


def _results_for(tdef, files: list[Path], branches_by_key) -> list[ExtractResult]:
    """One SUCCESS ExtractResult per staged branch file.

    ``row_count`` and ``schema`` come from the parquet footer -- metadata only,
    so this stays cheap no matter how large the staged file is. The loader reads
    the rows themselves in ``load_batch_rows`` batches later.
    """
    results = []
    for path in files:
        branch_key = path.stem
        branch = branches_by_key.get(branch_key)
        if branch is None:
            log.warning("[%s] no [oracle_branches.%s] section; skipping %s",
                        tdef.dataset_table_name, branch_key, path.name)
            continue
        meta = pq.read_metadata(path)
        results.append(ExtractResult(
            table_def=tdef,
            branch=branch_key,
            branch_id=branch.id,
            status="SUCCESS",
            row_count=meta.num_rows,
            staged_path=path,
            schema=pq.read_schema(path),
            # Watermarks left empty on purpose -- see the module docstring.
        ))
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tables", help="comma separated table dir names (default: all staged)")
    parser.add_argument("--staging-dir", help="override the configured staging dir")
    parser.add_argument("--tables-file", default="tables.json", help="path to tables.json")
    parser.add_argument("--list", action="store_true", help="show what is staged and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="plan and report, but run no load")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )

    settings = load_settings()
    staging_dir = Path(args.staging_dir) if args.staging_dir else Path(settings.staging_dir)
    if not staging_dir.is_dir():
        log.error("staging dir does not exist: %s", staging_dir)
        return 2

    staged = _staged_tables(staging_dir)
    if not staged:
        log.error("nothing staged under %s", staging_dir)
        return 2

    if args.tables:
        wanted = {t.strip() for t in args.tables.replace(",", " ").split() if t.strip()}
        missing = wanted - set(staged)
        if missing:
            log.error("not staged: %s", ", ".join(sorted(missing)))
            return 2
        staged = {k: v for k, v in staged.items() if k in wanted}

    tdefs = {t.dataset_table_name: t for t in load_table_defs(Path(args.tables_file))}
    branches_by_key = load_branches()

    if args.list:
        print(f"{'TABLE':<32}{'BRANCHES':>9}{'ROWS':>14}{'STAGED':>12}")
        for name, files in staged.items():
            rows = sum(pq.read_metadata(f).num_rows for f in files)
            mb = sum(f.stat().st_size for f in files) / 1e6
            print(f"{name:<32}{len(files):>9}{rows:>14,}{mb:>11.0f}M")
        return 0

    unknown = [n for n in staged if n not in tdefs]
    if unknown:
        log.error("staged dirs with no tables.json entry: %s", ", ".join(unknown))
        return 2

    # Same install the real pipeline does: one Iceberg snapshot per merge
    # instead of one per 1,000 rows. Must precede any load.
    _install_single_commit_merge()

    control = ControlStore(MetaStore(settings.postgres)).load()

    total_branches = len(branches_by_key)
    failures, successes = [], []

    for name, files in staged.items():
        tdef = tdefs[name]
        results = _results_for(tdef, files, branches_by_key)
        if not results:
            log.warning("[%s] no usable staged branches; skipping", name)
            continue

        rows = sum(r.row_count for r in results)
        log.info("[%s] %d staged branch(es), %s row(s)", name, len(results), f"{rows:,}")
        if args.dry_run:
            continue

        # branches_in_run == len(results): the plan must consider this table
        # complete from the branches actually staged, or _plan_table treats it
        # as a partial run and refuses to merge.
        monitor = PipelineMonitor(total_units=len(results), total_tables=1,
                                  interval_s=settings.progress_interval_s,
                                  enabled=settings.progress_enabled).start()
        started = time.time()
        try:
            pipeline = build_pipeline(settings, pipeline_name=_table_pipeline_name(settings, tdef))
            plan = _load_one_table(pipeline, tdef, results, settings, control,
                                   total_branches, len(results), monitor)
            elapsed = time.time() - started
            if plan.load_status == "SUCCESS":
                successes.append((name, rows, elapsed))
                log.info("[%s] SUCCESS in %.1fs", name, elapsed)
            else:
                failures.append((name, plan.load_status, plan.load_error))
                log.error("[%s] %s: %s", name, plan.load_status, plan.load_error)
        except Exception as exc:  # noqa: BLE001 - one bad table must not stop the rest
            failures.append((name, "EXCEPTION", f"{type(exc).__name__}: {exc}"))
            log.exception("[%s] load raised", name)
        finally:
            monitor.stop()
            control.save()

    if args.dry_run:
        log.info("dry run: nothing loaded")
        return 0

    print("\n" + "-" * 68)
    for name, rows, elapsed in successes:
        print(f"  {name:<32} SUCCESS  {rows:>12,} rows  {elapsed:>8.1f}s")
    for name, status, err in failures:
        print(f"  {name:<32} {status:<8} {err}")
    print(f"  {len(successes)} succeeded, {len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
