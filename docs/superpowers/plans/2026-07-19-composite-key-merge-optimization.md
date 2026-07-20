# Composite-Key Merge Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the wall-clock cost of incremental `merge` upserts on large composite-key Iceberg tables, without changing merge semantics, naming, or typing.

**Architecture:** The incremental path is `_merge_iceberg_single_commit` (etl/iceberg_load.py) → pyiceberg `table.upsert`. Profiling (Task 0, tool already built at `diagnostics/merge_profile.py`) attributes the cost to four stages; the two optimization tasks each remove one dominant cost and are **independently landable**. Task 1 (vectorized change-split) removes pyiceberg's per-cell Python diff. Task 2 (key-sorted writes) shrinks copy-on-write rewrite amplification. **Descoped:** replacing the `Or(AND EqualTo…)`-per-row composite match filter with a per-branch single-column `In` prefilter — the biggest structural lever, but the most invasive (a per-branch merge driver plus threading branch scope through the installed merge) — is intentionally out of scope for this plan. If profiling shows that filter dominates, see the Task 0 decision rule for the interim mitigation.

**Tech Stack:** Python, dlt (filesystem + Iceberg), pyiceberg 0.11.1, pyarrow, pytest with `pyiceberg.catalog.sql.SqlCatalog` over sqlite + `FsspecFileIO` for tests.

## Global Constraints

- pyiceberg is pinned at **0.11.1** — `create_match_filter` uses the fast `In(col, values)` path **only when `len(join_cols) == 1`**; ≥2 columns build `Or(AND EqualTo…)`, one clause per delta row (`.venv/Lib/site-packages/pyiceberg/table/upsert_util.py:33-48`).
- The loader always appends `BRANCH_ID` to the merge key (`etl/iceberg_load.py:421`), so **every** non-snapshot merge is composite today and never hits the `In` fast path.
- Merge semantics, column naming, and typing must be **unchanged** — this is a performance change only. Preserve the single-commit-per-merge property already asserted by `tests/test_merge_single_commit.py`.
- Tables are partitioned by `BRANCH_ID` (identity transform) — `etl/iceberg_load.py:439`. `BRANCH_ID` equality prunes to one partition's files.
- dlt normalizes identifiers to lower-case; for the clean UPPER_SNAKE source names that is just lower-casing (see `etl/iceberg_load.py:353`).
- Do not introduce a new **stored** Iceberg column on existing tables (avoids a backfill/migration). Any derived merge key used for matching must be computed in-memory at merge time, not persisted.
- Tests must use the existing `SqlCatalog` + sqlite + `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO` pattern from `tests/test_merge_single_commit.py` (pyarrow's IO chokes on `file:///D:/` URIs on Windows).

---

## File Structure

- `diagnostics/merge_profile.py` — **already created.** Read-only per-stage profiler + copy-on-write estimator. Task 0 runs it; no code change expected.
- `etl/iceberg_load.py` — the merge path. Task 1 rewrites `_merge_iceberg_single_commit` and adds the split helper; Task 2 adds a key-sort to `_iceberg_resource`.
- `tests/test_merge_single_commit.py` — existing merge tests (must stay green).
- `tests/test_merge_vectorized_split.py` — **new** (Task 1).
- `tests/test_merge_key_sorted_write.py` — **new** (Task 2).

---

## Task 0: Establish the per-stage baseline (measurement gate)

**No code.** This task decides which of Tasks 1–3 are worth doing, using the already-built read-only profiler. Nothing is written to the lake.

**Files:**
- Use: `diagnostics/merge_profile.py`

- [ ] **Step 1: Identify the real problem tables**

List the largest composite-key transaction tables that actually take the `merge` path (i.e. NOT a full-branch INITIAL and NOT no-CDC → those already `replace`; see `_plan_table`, `etl/iceberg_load.py:208-240`). Candidates from history: `gl_distribution` (no-CDC — verify it isn't already replacing), `contract_rules`, `claim_visit_detail`, and any large `--branch`-subset incremental.

- [ ] **Step 2: Sweep delta sizes on each table**

Run, escalating delta size to see the curve (plan+scan time is super-linear in clause count for a composite key):

```bash
.venv/Scripts/python.exe diagnostics/merge_profile.py <table> --delta-rows 10000
.venv/Scripts/python.exe diagnostics/merge_profile.py <table> --delta-rows 50000
.venv/Scripts/python.exe diagnostics/merge_profile.py <table> --delta-rows 200000 --files 12
```

- [ ] **Step 3: Record the numbers and read the verdict**

For each table capture: the per-stage seconds table, "`N row(s) actually changed`" (stage 3) vs delta size, and the "ESTIMATED copy-on-write rewrite" `% of table`. The tool prints a VERDICT line pointing at a lever.

- [ ] **Step 4: Apply the decision rule**

| Profiler signal | Do |
|---|---|
| Stages 2a+2b (Or plan/scan) dominate | **Descoped in this plan** (the composite-`Or` fix was omitted). Interim mitigation: shrink the delta upstream — tighten CDC granularity and/or correct the `unique_key` so fewer rows re-pull each run (a smaller delta = fewer `Or` clauses). If it stays the bottleneck after Tasks 1–2, revisit the per-branch single-column `In` approach as a follow-up plan. |
| Stage 3 (change diff) dominates **and** `actually changed` ≈ delta size (real CDC) | **Task 1** is a clean win. |
| Stage 3 dominates but `actually changed` ≪ delta size | The diff is *saving* rewrites (over-pull). Prefer shrinking the delta upstream (tighten CDC) over Task 1; note it and stop. |
| copy-on-write `% of table` ≥ ~40% | **Task 2** (after Task 1). |
| `delta has duplicate merge keys: True` or `Target table has duplicate rows` | **Stop — key bug, not a perf bug.** Fix the `unique_key` in tables.json first (see contract_rules history); merge cannot be correct until the key is unique. |

- [ ] **Step 5: Write the decision down**

Record, in the PR/issue, which tasks you're doing and the baseline numbers they must beat. This is the acceptance target for Tasks 1–2.

### Measured baseline (2026-07-19, delta = 1,000 rows each)

| Table | size | key cols | 2a plan | 2b scan | 2a+2b share | change diff | copy-on-write rewrite | matched files |
|---|---|---|---|---|---|---|---|---|
| purchaser_ios_prices | 28 MB | 6 | 7.0 s | 37.8 s | **96%** | 0.2 s | 49.3% of table | 5/12 |
| external_accounts_data | 101 MB | 4 | 4.2 s | 18.3 s | **92%** | 1.3 s | 37.1% of table | 5/23 |
| patient_episodes | 205 MB | 3 | 4.5 s | 12.6 s | **96%** | 0.2 s | 48.4% of table | 9/25 |
| patient_master_data | 623 MB | 2 | 5.9 s | 12.6 s | **90%** | 1.7 s | 81.8% of table | 18/35 |

Scaling (purchaser_ios_prices): build-filter 0.16 s (1k) → 0.83 s (5k) → **10.5 s (30k)**; 2a plan 7 s (1k) → **50 s (5k)** — both super-linear in delta rows.

**Reading of the numbers:**
- The composite `Or` plan+scan (stages 2a+2b) is **90–96%** of the cost on every table — i.e. the descoped lever is the real bottleneck. The per-cell diff (Task 1's target) is **<8%** here, so Task 1 barely moves these tables.
- The **matched-files set is the lever that is in scope**: stage 2b reads exactly the files copy-on-write would rewrite (the "matched files" column). For 1,000 *scattered* keys, patient_master_data touches 18/35 files (274 MB). **Task 2 (key-sorted writes) clusters a delta's keys into far fewer files**, shrinking *both* the 2b scan and the rewrite — the two dominant costs — even though it doesn't change the `Or` filter itself. That makes Task 2, not Task 1, the high-value in-scope work for these tables.
- If the `Or` plan/scan stays dominant after Task 2, the only further win is the descoped per-branch single-column `In` (a follow-up plan) or a smaller delta upstream (build/plan scale super-linearly with delta rows, so tighter CDC helps).

---

## Task 1: Vectorized change-split (remove the per-cell Python diff)

**What & why:** pyiceberg's `get_rows_to_update` compares every non-key column **cell-by-cell in Python** (`upsert_util.py:104-118`), O(rows × columns). For a CDC delta (rows are already the changed rows) this work is largely wasted. Replace `table.upsert` with a merge that splits the delta into "key already exists → overwrite" vs "new → append" using a **vectorized Arrow semi-join**, and overwrites all matched rows. Keeps the composite `Or` match filter as-is (replacing it is descoped — see Architecture), so this task is an isolated, testable change.

**Trade-off (already surfaced by Task 0):** overwriting all matched rows rewrites files for matched-but-unchanged rows too. Only worth it when `actually changed ≈ delta size`. Task 0's decision rule gates this.

**Files:**
- Modify: `etl/iceberg_load.py` — `_merge_iceberg_single_commit` (currently ~lines 758-799); add helper `_vectorized_matched_split`.
- Test: `tests/test_merge_vectorized_split.py` (new)

**Interfaces:**
- Consumes: `table` (pyiceberg Table), `data` (pa.Table, dlt-normalized), `schema` (dlt table schema dict), `load_table_name` (str) — unchanged signature of `_merge_iceberg_single_commit`.
- Produces: `_vectorized_matched_split(data: pa.Table, target_keys: pa.Table, join_cols: list[str]) -> tuple[pa.Table, pa.Table]` returning `(rows_to_update, rows_to_insert)`.

- [ ] **Step 1: Write the failing test for the split helper**

```python
# tests/test_merge_vectorized_split.py
from __future__ import annotations
import pyarrow as pa
from etl.iceberg_load import _vectorized_matched_split


def _delta(ids, branch=1):
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "branch_id": pa.array([branch] * len(ids), pa.int64()),
        "val": pa.array([f"v{i}" for i in ids]),
    })


def test_split_partitions_delta_into_update_and_insert():
    delta = _delta([1, 2, 3, 4])            # keys present in target: 2, 4
    target_keys = pa.table({"id": pa.array([2, 4], pa.int64()),
                            "branch_id": pa.array([1, 1], pa.int64())})
    upd, ins = _vectorized_matched_split(delta, target_keys, ["id", "branch_id"])
    assert sorted(upd.column("id").to_pylist()) == [2, 4]     # matched -> update
    assert sorted(ins.column("id").to_pylist()) == [1, 3]     # unmatched -> insert
    assert upd.num_rows + ins.num_rows == delta.num_rows       # partition, no loss


def test_split_empty_target_is_all_inserts():
    delta = _delta([1, 2])
    empty = pa.table({"id": pa.array([], pa.int64()),
                      "branch_id": pa.array([], pa.int64())})
    upd, ins = _vectorized_matched_split(delta, empty, ["id", "branch_id"])
    assert upd.num_rows == 0 and ins.num_rows == 2
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_vectorized_split.py::test_split_partitions_delta_into_update_and_insert -v`
Expected: FAIL with `ImportError: cannot import name '_vectorized_matched_split'`.

- [ ] **Step 3: Implement the split helper**

Add to `etl/iceberg_load.py` (near the other merge helpers):

```python
def _vectorized_matched_split(
    data: pa.Table, target_keys: pa.Table, join_cols: list[str]
) -> tuple[pa.Table, pa.Table]:
    """Split ``data`` into (rows whose key exists in ``target_keys``, the rest).

    Replaces pyiceberg's per-cell Python change-diff with a single vectorized
    Arrow inner-join on the key columns. ``target_keys`` holds only the key
    columns of the rows already in the table that could match this delta.
    """
    if target_keys.num_rows == 0:
        return data.schema.empty_table(), data
    keys = [k for k in join_cols]
    # Align key column types so the join matches exactly.
    tk = target_keys.select(keys)
    for k in keys:
        if not data.column(k).type.equals(tk.column(k).type):
            idx = tk.schema.get_field_index(k)
            tk = tk.set_column(idx, k, tk.column(k).cast(data.column(k).type, safe=False))
    tagged = data.select(keys).append_column(
        "__row", pa.array(range(data.num_rows), pa.int64()))
    matched = tagged.join(tk.group_by(keys).aggregate([]), keys=keys,
                          join_type="inner")
    matched_rows = sorted(set(matched.column("__row").to_pylist()))
    if not matched_rows:
        return data.schema.empty_table(), data
    matched_set = set(matched_rows)
    insert_rows = [i for i in range(data.num_rows) if i not in matched_set]
    return data.take(matched_rows), data.take(insert_rows)
```

- [ ] **Step 4: Run the helper tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_vectorized_split.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Write the failing integration test (merge correctness via the new path)**

Add to `tests/test_merge_vectorized_split.py` (reuses the SqlCatalog pattern from `tests/test_merge_single_commit.py`):

```python
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from etl.iceberg_load import _merge_iceberg_single_commit


def _rows(ids, names, branch=1):
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "name": pa.array(names),
        "branch_id": pa.array([branch] * len(ids), pa.int64()),
    })


def _schema():
    return {
        "x-merge-strategy": "upsert",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True},
            "branch_id": {"name": "branch_id", "data_type": "bigint", "primary_key": True},
            "name": {"name": "name", "data_type": "text"},
        },
    }


def _table(tmp_path, tag):
    cat = SqlCatalog("t", uri=f"sqlite:///{(tmp_path/f'c_{tag}.db').as_posix()}",
                     warehouse=(tmp_path/f"w_{tag}").as_uri(),
                     **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    t = cat.create_table(f"oasis.m_{tag}", schema=_rows([0], ["seed"]).schema)
    t.append(_rows([0], ["seed"]))
    return t


def test_merge_updates_existing_and_inserts_new(tmp_path):
    t = _table(tmp_path, "vec")
    _merge_iceberg_single_commit(
        t, _rows([0, 1, 2], ["u0", "n1", "n2"]), _schema(), "m")
    t.refresh()
    got = t.scan().to_arrow()
    assert got.num_rows == 3
    assert got.filter(pc.equal(got["id"], 0)).to_pydict()["name"] == ["u0"]


def test_merge_snapshot_count_stays_bounded(tmp_path):
    t = _table(tmp_path, "vecsnap")
    before = len(list(t.metadata.snapshots))
    _merge_iceberg_single_commit(
        t, _rows(list(range(2500)), [f"v{i}" for i in range(2500)]), _schema(), "m")
    t.refresh()
    assert len(list(t.metadata.snapshots)) - before <= 3   # O(1) in delta size
```

- [ ] **Step 6: Run to verify the correctness test fails on the current path where expected, then wire the new path**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_vectorized_split.py -v`
Expected: the two integration tests may PASS against the *current* `table.upsert` body — that's fine; they lock in behavior. Now change the body so the vectorized split is what runs.

Replace the `table.upsert(...)` call at the end of `_merge_iceberg_single_commit` with:

```python
    from pyiceberg.table import upsert_util

    normalized = ensure_iceberg_compatible_arrow_data(data)
    if strategy == "insert-only":
        table.append(normalized)
        return

    match_filter = upsert_util.create_match_filter(normalized, join_cols)
    target_keys = table.scan(
        row_filter=match_filter, selected_fields=tuple(join_cols),
        case_sensitive=True,
    ).to_arrow()
    to_update, to_insert = _vectorized_matched_split(normalized, target_keys, join_cols)

    with table.transaction() as txn:
        if to_update.num_rows:
            txn.overwrite(
                to_update,
                overwrite_filter=upsert_util.create_match_filter(to_update, join_cols),
            )
        if to_insert.num_rows:
            txn.append(to_insert)
```

Keep the existing `update_schema().union_by_name(...)` block above it (schema evolution) and the `strategy` validation. Remove the old `table.upsert(...)` call.

- [ ] **Step 7: Run the full merge suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_vectorized_split.py tests/test_merge_single_commit.py -v`
Expected: PASS. (`test_merge_single_commit.py` still green ⇒ semantics + bounded-commit preserved.)

- [ ] **Step 8: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_vectorized_split.py
git commit -m "perf(etl): vectorized merge change-split, drop per-cell Python diff"
```

---

## Task 2: Key-sorted writes (shrink copy-on-write rewrite)

**What & why:** Only do this if Task 0 showed copy-on-write rewriting a large `% of table` (I/O-bound). Copy-on-write rewrites every data file containing a matched row; if matched rows are scattered, nearly every file is rewritten. Sorting each written batch by the merge key clusters keys into fewer files, so a delta's matched rows hit fewer files (less rewrite) and per-file min/max stats on the key actually prune the scan.

**Files:**
- Modify: `etl/iceberg_load.py` — `_iceberg_resource._finish` (sort the yielded batch by the merge key).
- Test: `tests/test_merge_key_sorted_write.py` (new)

**Interfaces:**
- Consumes: the resource's `primary_key` (already `key_columns + BRANCH_ID`).
- Produces: no new symbol; `_finish` yields key-sorted tables.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_merge_key_sorted_write.py
from __future__ import annotations
import pyarrow as pa
from etl.iceberg_load import _sort_by_key


def test_sort_by_key_orders_rows_by_merge_key():
    t = pa.table({"id": pa.array([3, 1, 2], pa.int64()),
                  "branch_id": pa.array([1, 1, 1], pa.int64()),
                  "v": pa.array(["c", "a", "b"])})
    out = _sort_by_key(t, ["id", "branch_id"])
    assert out.column("id").to_pylist() == [1, 2, 3]


def test_sort_by_key_noop_when_no_key():
    t = pa.table({"v": pa.array([1, 2])})
    assert _sort_by_key(t, []).equals(t)
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_key_sorted_write.py -v`
Expected: FAIL with `ImportError: cannot import name '_sort_by_key'`.

- [ ] **Step 3: Implement `_sort_by_key` and call it in `_finish`**

```python
def _sort_by_key(tbl: pa.Table, key_cols: list[str]) -> pa.Table:
    """Sort a batch by the merge key so writes cluster keys into fewer files
    (less copy-on-write rewrite + prunable per-file key min/max). No-op if the
    table lacks the key columns or has none."""
    keys = [c for c in key_cols if c in tbl.column_names]
    if not keys or tbl.num_rows == 0:
        return tbl
    return tbl.sort_by([(k, "ascending") for k in keys])
```

In `_iceberg_resource._finish`, after the cast/carry-forward, return `_sort_by_key(tbl, primary_key)`.

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_key_sorted_write.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full merge suite (semantics unchanged; row order is irrelevant to a merge)**

Run: `.venv/Scripts/python.exe -m pytest tests/ -k "merge" -v`
Expected: PASS.

- [ ] **Step 6: Re-profile to confirm the rewrite % dropped**

Run `diagnostics/merge_profile.py` on the I/O-bound table after a fresh load and confirm the "ESTIMATED copy-on-write rewrite" `% of table` fell for the same delta. Record before/after.

- [ ] **Step 7: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_key_sorted_write.py
git commit -m "perf(etl): sort merge batches by key to cut copy-on-write rewrite"
```

---

## Self-Review

**Spec coverage:**
- Profile-first (user's chosen scope) → Task 0 (uses the built `diagnostics/merge_profile.py`), no behavior change.
- Per-cell diff cost → Task 1.
- Copy-on-write amplification → Task 2.
- Composite `Or` filter cost (the structural killer) → **descoped** (see Architecture + Task 0 decision rule); interim mitigation is to shrink the delta upstream.
- Non-unique-key discovery (a correctness, not perf, issue) → Task 0 Step 4 decision rule (stop + fix tables.json).

**Type consistency:** `_vectorized_matched_split(data, target_keys, join_cols) -> (pa.Table, pa.Table)` is defined and consumed in Task 1. `_sort_by_key(tbl, key_cols)` is self-contained (Task 2).

**Placeholder scan:** none — every code/test step carries real content.

**Ordering constraint:** Task 1 and Task 2 are independent; Task 2 is only worthwhile after Task 0's I/O verdict. Task 0 gates both.
