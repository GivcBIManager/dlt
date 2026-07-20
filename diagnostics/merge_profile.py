"""Attribute a composite-key Iceberg merge's cost to its individual stages.

The incremental load path (``etl.iceberg_load._merge_iceberg_single_commit`` ->
pyiceberg ``table.upsert``) is slow on large composite-key tables, but the cost
is split across four very different stages -- and which one dominates decides
which optimization is worth doing. This tool runs the *exact* pyiceberg upsert
stages against the **live Iceberg snapshot** and times each one, so the guess
comes out of the numbers instead of the head:

    1. match-filter build   create_match_filter(delta, join_cols)
                            -- for a composite key this is Or(AND(EqualTo...)),
                               ONE clause per delta row (vs a single cheap In for
                               a one-column key). Reports clause count + shape.
    2. matched scan          scan(row_filter=match_filter).plan_files() + to_arrow
                            -- manifest pruning of that giant Or, then reading the
                               matched target rows. Reports files/bytes touched.
    3. change diff           upsert_util.get_rows_to_update(...)
                            -- pyiceberg's row-by-row, per-cell Python compare of
                               every non-key column. O(rows x cols) pure Python.
    4. insert-side filter    create_match_filter + expression_to_pyarrow + filter
                            -- splitting new rows from matched rows.

It also *estimates* the copy-on-write rewrite (stage 5) that the real upsert
would do -- the count/bytes of data files the overwrite would rewrite -- from the
matched scan's file plan, WITHOUT writing anything.

NOTHING IS WRITTEN. The table is opened as a pyiceberg ``StaticTable`` (read
only) and the delta is synthesized by sampling real rows out of a spread of the
table's own data files, so the sampled keys land across many files exactly like a
real delta. Safe to run against production.

Usage:
    python diagnostics/merge_profile.py --list
    python diagnostics/merge_profile.py contract_rules
    python diagnostics/merge_profile.py gl_distribution --delta-rows 200000
    python diagnostics/merge_profile.py CLAIM_VISIT_DETAIL --branch 12 --files 8
    python diagnostics/merge_profile.py orders_master --changed-frac 0.3
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import sys
import time
import tomllib
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # so `import etl` works when run as a script
from etl import config  # noqa: E402


# --------------------------------------------------------------------------- #
# Config / table resolution (same portable resolver the pipeline uses)
# --------------------------------------------------------------------------- #
def _read_config() -> tuple[str, str]:
    """Return (dataset_name, local dataset root path) from .dlt/config.toml."""
    cfg = tomllib.loads((ROOT / ".dlt" / "config.toml").read_text("utf-8"))
    raw_bucket = cfg.get("destination", {}).get("filesystem", {}).get("bucket_url")
    dataset = cfg.get("etl", {}).get("dataset_name", "oasis")
    bucket_uri = config.resolve_bucket_url(raw_bucket)
    pr = urlparse(bucket_uri)
    if pr.scheme not in ("", "file"):
        sys.exit(f"non-local destination {bucket_uri!r}; this tool reads local files only")
    root = Path(url2pathname(pr.path)) if pr.scheme == "file" else Path(bucket_uri)
    return dataset, str(root / dataset)


def _resolve_table_name(name: str) -> str:
    obj = name.split(".", 1)[1] if "." in name else name
    return re.sub(r"[^0-9a-zA-Z]+", "_", obj).strip("_").lower()


def _table_defs() -> dict:
    try:
        defs = config.load_table_defs(ROOT / "tables.json")
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] could not parse tables.json ({exc}); key defaults limited")
        return {}
    return {t.dataset_table_name: t for t in defs}


def _iceberg_uri(path: Path) -> str:
    """dlt's ``file://<drive>/...`` form (pyiceberg mis-reads Path.as_uri on Windows)."""
    return "file://" + str(path.resolve()).replace("\\", "/")


def _uri_to_path(uri: str) -> Path:
    """Reverse of ``_iceberg_uri``: an Iceberg data-file URI -> local Path.

    dlt writes ``file://D:/...`` (drive as netloc) on Windows and ``file:///abs``
    on POSIX; handle both plus bare paths.
    """
    if not uri.startswith("file://"):
        return Path(uri)
    rest = uri[len("file://"):]
    pr = urlparse(uri)
    if pr.netloc and re.match(r"^[A-Za-z]:$", pr.netloc):  # file://D:/...
        return Path(pr.netloc + url2pathname(pr.path))
    return Path(url2pathname(pr.path)) if rest.startswith("/") else Path(rest)


def _latest_metadata(table_dir: Path) -> Path:
    metas = list((table_dir / "metadata").glob("*.metadata.json"))
    if not metas:
        sys.exit(f"no Iceberg metadata under {table_dir / 'metadata'} "
                 f"(has this table been loaded?)")

    def ver(p: Path) -> int:
        m = re.match(r"(\d+)-", p.name)
        return int(m.group(1)) if m else -1

    return max(metas, key=lambda p: (ver(p), p.stat().st_mtime))


def _open_table(dataset_root: str, table: str):
    from pyiceberg.table import StaticTable

    meta = _latest_metadata(Path(dataset_root) / table)
    return StaticTable.from_metadata(_iceberg_uri(meta))


# --------------------------------------------------------------------------- #
# Merge key (mirrors iceberg_load._iceberg_resource: key_columns + BRANCH_ID)
# --------------------------------------------------------------------------- #
def _join_cols(tdef, settings, schema_names: set[str]) -> list[str]:
    """Normalized merge key columns exactly as the loader builds them.

    ``primary_key = key_columns + BRANCH_ID``, and dlt normalizes identifiers to
    lower-case (for these clean UPPER_SNAKE names that is just lower-casing --
    see iceberg_load._existing_insert_at). Validated against the live schema so a
    stale tables.json key fails loudly with the available columns.
    """
    if tdef is None or not tdef.key_columns:
        sys.exit("table has no unique_key in tables.json; a merge needs a key to "
                 "profile (snapshot/append tables are never merged)")
    raw = list(tdef.key_columns) + [settings.branch_id_column]
    cols = [c.lower() for c in raw]
    missing = [c for c in cols if c not in schema_names]
    if missing:
        sys.exit(f"merge key column(s) {missing} not in the Iceberg schema; "
                 f"available: {', '.join(sorted(schema_names))}. "
                 f"(tables.json unique_key may be stale for this table.)")
    return cols


# --------------------------------------------------------------------------- #
# Delta synthesis: sample real rows spread across the table's data files
# --------------------------------------------------------------------------- #
def _read_parts(
    tbl, n_files: int, branch_filter: set[int] | None, branch_col: str,
) -> list[pa.Table]:
    """Read a strided spread of ``n_files`` data files, each aligned to the table
    schema. Returns one Arrow table per file (the benchmark appends them
    separately so the temp table keeps a realistic multi-file layout)."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    tasks = list(tbl.scan().plan_files())
    if not tasks:
        sys.exit("table has no data files (empty table); nothing to profile")

    files = [_uri_to_path(t.file.file_path) for t in tasks]
    files = [f for f in files if f.exists()]
    if not files:
        sys.exit("could not resolve any Iceberg data-file paths on this host")

    # Strided pick so the chosen files span the whole table, not one corner.
    step = max(1, len(files) // max(1, n_files))
    picked = files[::step][:n_files] or files[:n_files]

    # Canonical target schema: data files drift in Arrow encoding across runs
    # (string vs large_string, dict vs plain), so align every sampled file to the
    # table's own schema before concat -- which is also the schema the merge
    # stages compare against.
    target_schema = schema_to_pyarrow(tbl.schema())
    parts = []
    for f in picked:
        # Read the one file directly: these data files live under Hive-style
        # ``branch_id=N/`` dirs, so pq.read_table would infer branch_id as a
        # partition column and clash with the in-file branch_id column.
        t = _align_to_schema(pq.ParquetFile(str(f)).read(), target_schema)
        if branch_filter is not None and branch_col in t.column_names:
            t = t.filter(pc.is_in(t.column(branch_col), value_set=pa.array(list(branch_filter))))
        if t.num_rows:
            parts.append(t)
    if not parts:
        sys.exit("no rows read from sampled files (branch filter too narrow?)")
    return parts


def _read_pool(
    tbl, n_files: int, branch_filter: set[int] | None, branch_col: str,
) -> pa.Table:
    """Read a spread of ``n_files`` data files into one aligned Arrow pool."""
    return pa.concat_tables(
        _read_parts(tbl, n_files, branch_filter, branch_col),
        promote_options="default")


def _sample_from_pool(
    pool: pa.Table, n_rows: int, changed_frac: float, rng: random.Random,
) -> pa.Table:
    """Sample an ``n_rows`` delta out of ``pool`` (with ``changed_frac`` perturbed).

    ``changed_frac`` of the rows get a non-key column perturbed so the change-diff
    stage sees genuine updates; the rest are left identical (the diff's worst
    case -- it compares every non-key column before concluding "unchanged").
    """
    take = min(n_rows, pool.num_rows)
    delta = pool.take(rng.sample(range(pool.num_rows), take))
    if take < n_rows:
        print(f"[note] only {take:,} rows available in the sampled file(s); "
              f"profiling with that (raise --files for a bigger delta)")
    return _perturb(delta, changed_frac, rng)


def _sample_delta(
    tbl, dataset_root: str, table: str, n_rows: int, n_files: int,
    branch_filter: set[int] | None, branch_col: str, changed_frac: float,
    seed: int,
) -> pa.Table:
    """Convenience: read a pool of ``n_files`` files and sample an ``n_rows`` delta."""
    pool = _read_pool(tbl, n_files, branch_filter, branch_col)
    return _sample_from_pool(pool, n_rows, changed_frac, random.Random(seed))


def _align_to_schema(t: pa.Table, target: pa.Schema) -> pa.Table:
    """Reorder + cast ``t`` to ``target`` (fields absent in ``t`` filled null).

    Iceberg data files written on different runs drift in Arrow encoding; casting
    each to the table's canonical schema makes them concatenable and matches the
    schema the pyiceberg merge stages expect.
    """
    cols = {}
    for field in target:
        if field.name in t.column_names:
            col = t.column(field.name)
            if not col.type.equals(field.type):
                try:
                    col = col.cast(field.type, safe=False)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
                    col = pc.cast(pc.cast(col, pa.string()), field.type, safe=False)
            cols[field.name] = col
        else:
            cols[field.name] = pa.nulls(t.num_rows, field.type)
    return pa.table(cols, schema=target)


def _perturb(delta: pa.Table, frac: float, rng: random.Random) -> pa.Table:
    """Flip a non-key numeric/string column on ``frac`` of rows so they read as
    genuine updates. Best-effort: if no perturbable column exists, leave as-is."""
    if frac <= 0 or delta.num_rows == 0:
        return delta
    # Pick the first non-partition, non-timestamp column that's easy to bump.
    for name in delta.column_names:
        col = delta.column(name)
        if pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
            n = int(delta.num_rows * frac)
            rows = set(rng.sample(range(delta.num_rows), min(n, delta.num_rows)))
            vals = col.to_pylist()
            for i in rows:
                vals[i] = (vals[i] or 0) + 1
            idx = delta.schema.get_field_index(name)
            return delta.set_column(idx, name, pa.array(vals, type=col.type))
    return delta


# --------------------------------------------------------------------------- #
# The profiled stages (real pyiceberg code)
# --------------------------------------------------------------------------- #
class _Timer:
    def __init__(self):
        self.stages: list[tuple[str, float, str]] = []

    def run(self, label: str, fn):
        t0 = time.perf_counter()
        out = fn()
        dt = time.perf_counter() - t0
        note = ""
        if isinstance(out, tuple):
            out, note = out
        self.stages.append((label, dt, note))
        print(f"  [{dt:8.3f}s] {label}" + (f"  -- {note}" if note else ""), flush=True)
        return out


def _dedup_delta(delta: pa.Table, join_cols: list[str]) -> pa.Table:
    """Report + drop duplicate merge keys (pyiceberg aborts the upsert on them)."""
    from pyiceberg.table import upsert_util

    dup = upsert_util.has_duplicate_rows(delta, join_cols)
    print(f"delta has duplicate merge keys: {dup}"
          + ("   <-- upsert would RAISE 'Duplicate rows found'" if dup else ""))
    if not dup:
        return delta
    print("  (de-duplicating to profile the remaining stages)")
    seen, rows = set(), []
    keycols = [delta.column(c).to_pylist() for c in join_cols]
    for i in range(delta.num_rows):
        k = tuple(col[i] for col in keycols)
        if k not in seen:
            seen.add(k)
            rows.append(i)
    out = delta.take(rows)
    print(f"  deduped delta rows = {out.num_rows:,}")
    return out


def _run_stages(tbl, delta: pa.Table, join_cols: list[str]):
    """Time the four real pyiceberg upsert stages for one (table, delta, key).

    Returns ``(timer, matched_files, matched_bytes, updated)``. Prints each stage
    line as it completes. Does NOT dedup (call _dedup_delta first) or print the
    summary (call _summary), so the same stages can be timed for two different
    key choices over the same data.
    """
    from pyiceberg.table import upsert_util
    from pyiceberg.expressions.visitors import bind
    from pyiceberg.io.pyarrow import expression_to_pyarrow

    schema = tbl.schema()
    n = delta.num_rows
    t = _Timer()

    def stage_match():
        f = upsert_util.create_match_filter(delta, join_cols)
        return f, f"{type(f).__name__}; {n:,} clause(s)"
    match_filter = t.run("1. build match filter", stage_match)

    def stage_plan():
        tasks = list(tbl.scan(row_filter=match_filter, case_sensitive=True).plan_files())
        nbytes = sum(getattr(task.file, "file_size_in_bytes", 0) or 0 for task in tasks)
        return (tasks, nbytes), f"{len(tasks)} file(s) matched, {nbytes/1e6:,.1f} MB to scan/rewrite"
    tasks, matched_bytes = t.run("2a. plan matched files (prune)", stage_plan)

    def stage_scan():
        tgt = tbl.scan(row_filter=match_filter, case_sensitive=True).to_arrow()
        return tgt, f"{tgt.num_rows:,} target rows read"
    target = t.run("2b. read matched target rows", stage_scan)

    def stage_diff():
        try:
            upd = upsert_util.get_rows_to_update(delta, target, join_cols)
            return upd, f"{len(upd):,} row(s) actually changed"
        except ValueError as exc:  # target has duplicate merge keys -> real upsert aborts
            return target.schema.empty_table(), f"ABORTED: {exc} (non-unique key in stored data)"
    updated = t.run("3. change diff (per-cell Python)", stage_diff)

    def stage_insert():
        if target.num_rows == 0:
            return delta, "target empty; all delta rows are inserts"
        expr = upsert_util.create_match_filter(target, join_cols)
        arrow = expression_to_pyarrow(bind(schema, expr, case_sensitive=True))
        ins = delta.filter(~arrow)
        return ins, f"{ins.num_rows:,} new row(s) to insert"
    t.run("4. insert-side filter", stage_insert)

    return t, len(tasks), matched_bytes, updated


def profile(tbl, delta: pa.Table, join_cols: list[str]) -> None:
    n = delta.num_rows
    single = len(join_cols) == 1
    print(f"\ndelta rows = {n:,}   join_cols = {join_cols}   "
          f"filter path = {'In (fast)' if single else 'Or(AND EqualTo) (composite)'}")
    delta = _dedup_delta(delta, join_cols)
    t, matched_files, matched_bytes, updated = _run_stages(tbl, delta, join_cols)
    _summary(t, tbl, matched_files, matched_bytes, updated)


def _table_file_stats(tbl) -> tuple[int, int]:
    """(total data files, total bytes) across the current snapshot."""
    tasks = list(tbl.scan().plan_files())
    return len(tasks), sum(getattr(x.file, "file_size_in_bytes", 0) or 0 for x in tasks)


def _summary(t: _Timer, tbl, matched_files: int, matched_bytes: int, updated) -> None:
    total = sum(dt for _, dt, _ in t.stages)
    tot_files, tot_bytes = _table_file_stats(tbl)
    print("\n" + "=" * 68)
    print(f"{'STAGE':<34}{'SECONDS':>10}{'% OF PROFILED':>16}")
    print("-" * 68)
    for label, dt, _ in t.stages:
        print(f"{label:<34}{dt:>10.3f}{(dt/total*100 if total else 0):>15.1f}%")
    print("-" * 68)
    print(f"{'profiled total':<34}{total:>10.3f}{100.0:>15.1f}%")
    print("=" * 68)

    # Stage 5 (not executed): copy-on-write would rewrite every matched file.
    pct_files = matched_files / tot_files * 100 if tot_files else 0
    pct_bytes = matched_bytes / tot_bytes * 100 if tot_bytes else 0
    print("\nESTIMATED copy-on-write rewrite (stage 5, NOT executed):")
    print(f"  would rewrite {matched_files:,}/{tot_files:,} data files "
          f"({pct_files:.1f}%), {matched_bytes/1e6:,.1f}/{tot_bytes/1e6:,.1f} MB "
          f"({pct_bytes:.1f}% of table) to change {len(updated):,} row(s)")

    # Verdict heuristic: which lever the numbers point to.
    by = {label: dt for label, dt, _ in t.stages}
    build = by.get("1. build match filter", 0)
    scan = by.get("2a. plan matched files (prune)", 0) + by.get("2b. read matched target rows", 0)
    diff = by.get("3. change diff (per-cell Python)", 0)
    print("\nVERDICT:")
    if diff >= max(build, scan):
        print("  CPU-bound on the per-cell change diff -> lever #1 (skip the diff for")
        print("  CDC tables) is the cheapest, highest-return fix.")
    elif build >= scan:
        print("  CPU-bound building/binding the composite Or filter -> lever #2")
        print("  (single-column merge key + per-branch scope for the In fast path).")
    elif pct_bytes >= 40:
        print("  I/O-bound: copy-on-write rewrites a large fraction of the table ->")
        print("  lever #3 (cluster/sort by key, or partition-scoped overwrite).")
    else:
        print("  Scan-bound reading matched target rows -> lever #2/#3 (better file")
        print("  pruning via a single-column key and/or key-sorted data files).")
    print("\n(read-only profile; nothing was written to the lake)")


# --------------------------------------------------------------------------- #
# Row-hash benchmark: single-column In key vs composite Or key, same data
# --------------------------------------------------------------------------- #
def _hash_array(t: pa.Table, cols: list[str]) -> pa.Array:
    """Deterministic-within-process int64 hash of ``cols`` (the "row hash" idea).

    Stringifies + null-fills each key column and concatenates them (vectorized in
    pyarrow), then hashes each row. Both the stored pool and the delta are hashed
    by this same function in the same process, so their hashes align (the value
    only needs to be consistent within one run, not across processes). Masked to a
    non-negative 63-bit value so it fits an Iceberg ``long`` (a production version
    would use a stable 64-bit hash such as xxhash/blake2b).
    """
    parts = [pc.fill_null(pc.cast(t.column(c), pa.string()), "\x00") for c in cols]
    joined = parts[0] if len(parts) == 1 else pc.binary_join_element_wise(*parts, "\x1f")
    return pa.array([hash(s) & 0x7FFFFFFFFFFFFFFF for s in joined.to_pylist()],
                    pa.int64())


def _with_hash(t: pa.Table, cols: list[str], name: str) -> pa.Table:
    return t.append_column(name, _hash_array(t, cols))


def _distinct_count(t: pa.Table, cols: list[str]) -> int:
    return t.select(cols).group_by(cols).aggregate([]).num_rows


def _create_bench_table(schema: pa.Schema, tmp_dir: Path):
    from pyiceberg.catalog.sql import SqlCatalog

    cat = SqlCatalog(
        "mp", uri=f"sqlite:///{(tmp_dir / 'cat.db').as_posix()}",
        warehouse=tmp_dir.as_uri(),
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("mp")
    return cat, cat.create_table("mp.bench", schema=schema)


def _compare_runs(comp, hash_) -> None:
    tc = {l: dt for l, dt, _ in comp[0].stages}
    th = {l: dt for l, dt, _ in hash_[0].stages}
    print("\n" + "=" * 72)
    print(f"{'STAGE':<34}{'COMPOSITE s':>13}{'HASH s':>11}{'SPEEDUP':>10}")
    print("-" * 72)
    for label, _, _ in comp[0].stages:
        a, b = tc.get(label, 0.0), th.get(label, 0.0)
        sp = f"{a / b:>8.1f}x" if b > 1e-9 else "     inf"
        print(f"{label:<34}{a:>13.3f}{b:>11.3f}{sp:>10}")
    ta, tb = sum(tc.values()), sum(th.values())
    tot_sp = f"{ta / tb:>8.1f}x" if tb > 1e-9 else "     inf"
    print("-" * 72)
    print(f"{'TOTAL':<34}{ta:>13.3f}{tb:>11.3f}{tot_sp:>10}")
    print("=" * 72)
    print(f"matched files -> composite {comp[1]}, hash {hash_[1]}  "
          f"(fewer = better pruning + less copy-on-write rewrite)")


def benchmark_hash(tbl, join_cols, branch_col, branch_filter, n_rows, n_files,
                   seed, sorted_by_hash) -> None:
    """Compare a single-column integer row-hash key (In) vs the composite key (Or).

    Builds a THROWAWAY Iceberg table (temp dir, sqlite catalog) from the sampled
    data files with an added ``merge_hash`` column, then times the same four merge
    stages twice over that identical data -- once joining on the composite key
    (Or path), once on the single hash column (In path). Nothing is written to the
    lake. ``sorted_by_hash`` physically clusters the temp table by the hash so its
    per-file min/max can prune an ``In`` scan (the "does it help pruning too?" test).
    """
    import shutil
    import tempfile

    HASH = "merge_hash"
    print(f"\n=== HASH-KEY BENCHMARK ===")
    print(f"composite join_cols = {join_cols}")
    print("building throwaway hashed Iceberg table from sampled files "
          + ("(hash-SORTED)" if sorted_by_hash else "(natural order)") + " ...", flush=True)

    parts = [_with_hash(p, join_cols, HASH)
             for p in _read_parts(tbl, n_files, branch_filter, branch_col)]
    pool = pa.concat_tables(parts, promote_options="default")

    tmp = Path(tempfile.mkdtemp(prefix="merge_bench_"))
    try:
        cat, bt = _create_bench_table(pool.schema, tmp)
        if sorted_by_hash:
            sp = pool.sort_by(HASH)
            k = max(1, len(parts))
            size = max(1, -(-sp.num_rows // k))   # ceil division into k files
            for i in range(0, sp.num_rows, size):
                bt.append(sp.slice(i, min(size, sp.num_rows - i)))
        else:
            for p in parts:
                bt.append(p)
        bt = cat.load_table("mp.bench")  # fresh metadata after the appends

        rng = random.Random(seed)
        delta = _sample_from_pool(pool, n_rows, 0.0, rng)  # matched rows, no perturb
        n_key = _distinct_count(delta, join_cols)
        n_hash = _distinct_count(delta, [HASH])
        collide = n_hash < n_key
        print(f"delta rows = {delta.num_rows:,}   distinct composite keys = {n_key:,}"
              f"   distinct hashes = {n_hash:,}"
              + ("   <-- HASH COLLISIONS (needs exact-key backstop)" if collide
                 else "   (no collisions -> hash In is a safe prefilter)"))
        delta = _dedup_delta(delta, join_cols)

        print("\n--- COMPOSITE key  (create_match_filter -> Or) ---", flush=True)
        comp = _run_stages(bt, delta, join_cols)
        print("\n--- HASH key  (create_match_filter -> In) ---", flush=True)
        hsh = _run_stages(bt, delta, [HASH])
        _compare_runs(comp, hsh)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\n(benchmark used a throwaway temp table; the lake was not touched)")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("table", nargs="?", help="table to profile (contract_rules, "
                   "OASIS.CONTRACT_RULES, ...)")
    p.add_argument("--delta-rows", type=int, default=10_000,
                   help="synthetic delta size to profile (default: 10000). NOTE: "
                        "plan+scan time grows super-linearly with delta size for a "
                        "composite key -- sweep 10k/50k/200k to see the curve.")
    p.add_argument("--files", type=int, default=6,
                   help="number of data files to sample the delta from, spread "
                        "across the table (default: 6)")
    p.add_argument("--changed-frac", type=float, default=0.0,
                   help="fraction of delta rows to perturb so they count as real "
                        "updates in the diff (default: 0.0 = diff worst case)")
    p.add_argument("--branch", help="restrict the sampled delta to these BRANCH_ID "
                   "value(s), comma separated (numeric ids)")
    p.add_argument("--seed", type=int, default=1234, help="sampling seed")
    p.add_argument("--hash-key", action="store_true",
                   help="benchmark a single-column integer row-hash merge key (In) "
                        "against the composite key (Or) on identical data, using a "
                        "throwaway temp Iceberg table (the lake is not touched)")
    p.add_argument("--hash-sorted", action="store_true",
                   help="with --hash-key, physically sort the temp table by the hash "
                        "so its per-file min/max can prune the In scan (pruning test)")
    p.add_argument("--list", action="store_true", help="list loaded tables and exit")
    args = p.parse_args(argv)

    # Quiet tables.json parse warnings (helper/cdc notices) -- irrelevant here.
    logging.disable(logging.WARNING)
    dataset, dataset_root = _read_config()

    if args.list:
        root = Path(dataset_root)
        loaded = sorted(d.name for d in root.iterdir()
                        if d.is_dir() and not d.name.startswith("_")) if root.exists() else []
        print(f"dataset '{dataset}' at {dataset_root}")
        print("loaded tables:", ", ".join(loaded) or "(none)")
        return 0

    if not args.table:
        p.error("a table name is required (or use --list)")

    table = _resolve_table_name(args.table)
    defs = _table_defs()
    tdef = defs.get(table)
    settings = config.Settings()

    tbl = _open_table(dataset_root, table)
    schema_names = {f.name for f in tbl.schema().fields}
    join_cols = _join_cols(tdef, settings, schema_names)
    branch_col = settings.branch_id_column.lower()

    branch_filter = None
    if args.branch:
        branch_filter = {int(b) for b in args.branch.replace(",", " ").split() if b.strip()}

    print(f"table '{table}'   dataset '{dataset}'")
    if args.hash_key:
        benchmark_hash(tbl, join_cols, branch_col, branch_filter, args.delta_rows,
                       args.files, args.seed, args.hash_sorted)
        return 0
    delta = _sample_delta(tbl, dataset_root, table, args.delta_rows, args.files,
                          branch_filter, branch_col, args.changed_frac, args.seed)
    profile(tbl, delta, join_cols)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
