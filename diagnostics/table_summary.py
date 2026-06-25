"""Summarize a loaded table's record counts by branch x time-bucket.

For any selected table this prints two summaries of **record counts**:

  1. by BRANCH_ID and the *insert time* truncated to the hour, and
  2. by BRANCH_ID and the *update time* truncated to the hour.

"Insert time" defaults to the injected ``insert_at`` (when the row was first
loaded into the lake; preserved across updates); "update time" defaults to the
table's ``cdc_column`` from tables.json (``AMEND_LAST_DATE`` for the OASIS
tables). Either can be overridden
with ``--insert-col`` / ``--update-col`` -- e.g. pass the source transaction date
(``ORDER_DATE`` / ``DELIVERY_DATE``) as the insert column. Column matching is
case-insensitive, so the same names work whether the data lives in the
normalized Iceberg table (lower_snake) or the raw staged parquet (UPPER case).

Data is read from the **live Iceberg snapshot** (via pyiceberg, so superseded
files from merges/replaces are never double-counted), or from ``_staging`` with
``--source staging``.

Usage:
    python diagnostics/table_summary.py --list
    python diagnostics/table_summary.py APPOINTMENTS
    python diagnostics/table_summary.py orders_master --grain day --csv out
    python diagnostics/table_summary.py MASTER_DELIVERIES --insert-col DELIVERY_DATE
    python diagnostics/table_summary.py CODES_DATA --branch jazan,khamis --which update
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import tomllib
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.compute as pc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script
from etl import config  # noqa: E402

GRAIN_FORMATS = {
    "minute": "%Y-%m-%d %H:%M",
    "hour": "%Y-%m-%d %H:00",
    "day": "%Y-%m-%d",
    "month": "%Y-%m",
}


# --------------------------------------------------------------------------- #
# Config / path resolution (reuses the pipeline's own portable resolver)
# --------------------------------------------------------------------------- #
def _read_config() -> tuple[str, str]:
    """Return (dataset_name, local dataset root path) from .dlt/config.toml."""
    cfg = tomllib.loads((ROOT / ".dlt" / "config.toml").read_text("utf-8"))
    raw_bucket = cfg.get("destination", {}).get("filesystem", {}).get("bucket_url")
    dataset = cfg.get("etl", {}).get("dataset_name", "oasis")
    bucket_uri = config.resolve_bucket_url(raw_bucket)  # -> file:// URI (or s3://...)
    pr = urlparse(bucket_uri)
    if pr.scheme not in ("", "file"):
        sys.exit(f"non-local destination {bucket_uri!r}; this tool reads local files only")
    root = Path(url2pathname(pr.path)) if pr.scheme == "file" else Path(bucket_uri)
    return dataset, str(root / dataset)


def _resolve_table_name(name: str) -> str:
    """Map APPOINTMENTS / OASIS.APPOINTMENTS / appointments -> dataset table id."""
    obj = name.split(".", 1)[1] if "." in name else name
    return re.sub(r"[^0-9a-zA-Z]+", "_", obj).strip("_").lower()


def _table_defs() -> dict:
    """dataset_table_name -> TableDef (cdc/date columns live here)."""
    try:
        defs = config.load_table_defs(ROOT / "tables.json")
    except Exception as exc:  # noqa: BLE001 - tool should still run on unknown tables
        print(f"[warn] could not parse tables.json ({exc}); column defaults limited")
        return {}
    return {t.dataset_table_name: t for t in defs}


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #
def _latest_metadata(table_dir: Path) -> Path:
    metas = list((table_dir / "metadata").glob("*.metadata.json"))
    if not metas:
        sys.exit(f"no Iceberg metadata under {table_dir / 'metadata'} "
                 f"(has this table been loaded? try --source staging)")

    def ver(p: Path) -> int:
        m = re.match(r"(\d+)-", p.name)
        return int(m.group(1)) if m else -1

    return max(metas, key=lambda p: (ver(p), p.stat().st_mtime))


def _iceberg_uri(path: Path) -> str:
    """A file URI in dlt's own ``file://<drive>/...`` form.

    ``Path.as_uri()`` yields ``file:///D:/...`` (empty netloc), which pyiceberg
    mis-reads on Windows; dlt writes ``file://D:/...`` (drive as netloc), so we
    match that. On POSIX the absolute path already starts with ``/`` -> a normal
    ``file:///abs/path``.
    """
    return "file://" + str(path.resolve()).replace("\\", "/")


def load_iceberg(dataset_root: str, table: str, columns: list[str]) -> pa.Table:
    from pyiceberg.table import StaticTable

    meta = _latest_metadata(Path(dataset_root) / table)
    tbl = StaticTable.from_metadata(_iceberg_uri(meta))
    # Only scan the columns we actually summarize (cheap on wide/large tables).
    return tbl.scan(selected_fields=tuple(columns)).to_arrow()


def load_staging(table: str, columns: list[str]) -> pa.Table:
    import pyarrow.parquet as pq

    files = sorted((ROOT / "_staging" / table).glob("*.parquet"))
    if not files:
        sys.exit(f"no staged parquet under {ROOT / '_staging' / table}")
    # Read each branch file selecting only present columns, then concat.
    parts = []
    for f in files:
        present = [c for c in columns if c in set(pq.read_schema(f).names)]
        parts.append(pq.read_table(f, columns=present))
    return pa.concat_tables(parts, promote_options="default")


# --------------------------------------------------------------------------- #
# Summary
# --------------------------------------------------------------------------- #
def _resolve_col(names: list[str], wanted: str | None) -> str | None:
    """Case-insensitive column lookup (iceberg=lower_snake, staging=UPPER)."""
    if not wanted:
        return None
    lower = {n.lower(): n for n in names}
    return lower.get(wanted.lower())


def summarize(at: pa.Table, branch_col: str, time_col: str, grain: str):
    """Return sorted rows [(branch, bucket_label, count)] grouped to ``grain``."""
    col = at.column(time_col)
    is_ts = pa.types.is_timestamp(col.type) or pa.types.is_date(col.type)
    if is_ts:
        bucket = pc.floor_temporal(col, unit=grain)
    else:
        bucket = col  # non-temporal column: count by raw value, grain ignored

    work = pa.table({branch_col: at.column(branch_col), "bucket": bucket})
    grouped = work.group_by([branch_col, "bucket"]).aggregate([(branch_col, "count")])

    branches = grouped.column(branch_col).to_pylist()
    buckets = grouped.column("bucket").to_pylist()
    counts = grouped.column(f"{branch_col}_count").to_pylist()

    fmt = GRAIN_FORMATS.get(grain, "%Y-%m-%d %H:00")

    def label(v) -> str:
        if isinstance(v, (datetime, date)):
            return v.strftime(fmt)
        return "(null)" if v is None else str(v)

    rows = [(b if b is not None else "(null)", label(t), c)
            for b, t, c in zip(branches, buckets, counts)]
    rows.sort(key=lambda r: (r[0], r[1]))
    return rows, is_ts


def render(title: str, col_used: str, rows: list, is_ts: bool, grain: str,
           max_rows: int) -> None:
    print(f"\n{title}")
    print(f"  column = {col_used}" + ("" if is_ts else "  (not a timestamp; "
          f"grain '{grain}' not applied -- counting by raw value)"))
    if not rows:
        print("  (no rows)")
        return

    per_branch: dict[str, int] = {}
    for b, _, c in rows:
        per_branch[b] = per_branch.get(b, 0) + c
    total = sum(per_branch.values())

    bw = max(len("BRANCH"), *(len(b) for b, _, _ in rows))
    tw = max(len("TIME_BUCKET"), *(len(t) for _, t, _ in rows))
    print(f"  {'BRANCH':<{bw}}  {'TIME_BUCKET':<{tw}}  {'RECORDS':>12}")
    print(f"  {'-'*bw}  {'-'*tw}  {'-'*12}")
    for b, t, c in rows[:max_rows]:
        print(f"  {b:<{bw}}  {t:<{tw}}  {c:>12,}")
    if len(rows) > max_rows:
        print(f"  ... {len(rows) - max_rows:,} more bucket(s) "
              f"(use --max-rows N or --csv for the full breakdown)")

    print(f"  {'-'*bw}  {'-'*tw}  {'-'*12}")
    for b in sorted(per_branch):
        print(f"  {b:<{bw}}  {'(all buckets)':<{tw}}  {per_branch[b]:>12,}")
    print(f"  {'TOTAL':<{bw}}  {len(rows):>{tw},} bucket(s){'':<0}  {total:>12,}")


def write_csv(path: Path, summary_name: str, col_used: str, rows: list) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["summary", "time_column", "branch_id", "time_bucket", "records"])
        for b, t, c in rows:
            w.writerow([summary_name, col_used, b, t, c])
    print(f"  -> wrote {len(rows):,} rows to {path}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("table", nargs="?", help="table to summarize (APPOINTMENTS, "
                   "OASIS.APPOINTMENTS, or orders_master)")
    p.add_argument("--source", choices=["iceberg", "staging"], default="iceberg",
                   help="read the live Iceberg snapshot (default) or raw _staging parquet")
    p.add_argument("--grain", choices=list(GRAIN_FORMATS), default="hour",
                   help="time bucket size (default: hour)")
    p.add_argument("--which", choices=["insert", "update", "both"], default="both",
                   help="which summary to produce (default: both)")
    p.add_argument("--insert-col", help="insert-time column "
                   "(default: insert_at)")
    p.add_argument("--update-col", help="update-time column "
                   "(default: the table's cdc_column, e.g. AMEND_LAST_DATE)")
    p.add_argument("--branch", help="comma/space separated branch keys to include "
                   "(default: all)")
    p.add_argument("--max-rows", type=int, default=40,
                   help="max bucket rows printed per summary (default: 40)")
    p.add_argument("--csv", metavar="DIR",
                   help="also write the full breakdown(s) as CSV into DIR")
    p.add_argument("--list", action="store_true",
                   help="list loaded tables and exit")
    args = p.parse_args(argv)

    dataset, dataset_root = _read_config()
    defs = _table_defs()

    if args.list:
        root = Path(dataset_root)
        loaded = sorted(d.name for d in root.iterdir()
                        if d.is_dir() and not d.name.startswith("_")) if root.exists() else []
        print(f"dataset '{dataset}' at {dataset_root}")
        print("loaded tables:", ", ".join(loaded) or "(none)")
        staged = sorted(d.name for d in (ROOT / "_staging").iterdir()
                        if d.is_dir()) if (ROOT / "_staging").exists() else []
        print("staged tables:", ", ".join(staged) or "(none)")
        return 0

    if not args.table:
        p.error("a table name is required (or use --list)")

    table = _resolve_table_name(args.table)
    tdef = defs.get(table)

    # Default column names: branch + injected load ts come from Settings; the
    # update column defaults to the table's configured cdc_column.
    settings = config.Settings()
    branch_want = settings.branch_id_column                 # BRANCH_ID
    insert_want = args.insert_col or settings.inserted_ts_column  # insert_at
    update_want = args.update_col or (tdef.cdc_column if tdef else None)
    if args.which in ("update", "both") and not update_want:
        sys.exit("no update column: table not in tables.json and --update-col not given")

    wanted = [branch_want]
    if args.which in ("insert", "both"):
        wanted.append(insert_want)
    if args.which in ("update", "both"):
        wanted.append(update_want)

    if args.source == "iceberg":
        # scan needs the *actual* (normalized) names; discover them from schema
        meta_names = _peek_columns(dataset_root, table)
        scan_cols = [_resolve_col(meta_names, w) for w in dict.fromkeys(wanted)]
        missing = [w for w, r in zip(dict.fromkeys(wanted), scan_cols) if r is None]
        if missing:
            sys.exit(f"column(s) {missing} not found in {table}; "
                     f"available: {', '.join(sorted(meta_names))}")
        at = load_iceberg(dataset_root, table, [c for c in scan_cols if c])
    else:
        at = load_staging(table, list(dict.fromkeys(wanted)))

    names = at.column_names
    branch_col = _resolve_col(names, branch_want)
    if not branch_col:
        sys.exit(f"branch column {branch_want!r} not found in {table} "
                 f"(have: {', '.join(names)})")

    # optional branch filter
    if args.branch:
        keep = {b.strip() for b in args.branch.replace(",", " ").split() if b.strip()}
        at = at.filter(pc.is_in(at.column(branch_col), value_set=pa.array(list(keep))))

    print(f"table '{table}'  source={args.source}  grain={args.grain}  "
          f"rows={at.num_rows:,}  branches={len(pc.unique(at.column(branch_col)))}")

    csv_dir = Path(args.csv) if args.csv else None
    if csv_dir:
        csv_dir.mkdir(parents=True, exist_ok=True)

    jobs = []
    if args.which in ("insert", "both"):
        jobs.append(("insert", _resolve_col(names, insert_want), insert_want))
    if args.which in ("update", "both"):
        jobs.append(("update", _resolve_col(names, update_want), update_want))

    for kind, actual, want in jobs:
        if not actual:
            print(f"\n[{kind}] column {want!r} not present in {table}; skipped")
            continue
        rows, is_ts = summarize(at, branch_col, actual, args.grain)
        render(f"== {kind.upper()} summary: records by {branch_col} x {args.grain} ==",
               actual, rows, is_ts, args.grain, args.max_rows)
        if csv_dir:
            write_csv(csv_dir / f"{table}_{kind}_by_{args.grain}.csv", kind, actual, rows)

    return 0


def _peek_columns(dataset_root: str, table: str) -> list[str]:
    """Read the Iceberg schema column names without scanning data."""
    from pyiceberg.table import StaticTable

    meta = _latest_metadata(Path(dataset_root) / table)
    return [f.name for f in StaticTable.from_metadata(_iceberg_uri(meta)).schema().fields]


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
