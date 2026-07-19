# Hash-Keyed Composite Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Derive one stored 128-bit `merge_hash` column from each table's `(PK…, BRANCH_ID)` key so the incremental Iceberg merge joins on a single column (pyiceberg's fast `In` path) instead of the per-row `Or(AND EqualTo…)` composite filter that is 90–96% of merge cost.

**Architecture:** Both the initial-load and incremental paths flow every batch through `_iceberg_resource._finish` (`etl/iceberg_load.py:429`), the single place the hash is computed and the batch sorted. The merge's join-column choice is overridden locally in `_merge_iceberg_single_commit` (`etl/iceberg_load.py:758`). Rollout is reload-gated: a table is "hash-ready" iff a prior full `replace` wrote the column; incremental merges write the hash only on an already-ready table (so `union_by_name` never half-populates it) and otherwise fall back to today's composite merge unchanged.

**Tech Stack:** Python, dlt (filesystem + Iceberg), pyiceberg 0.11.1, pyarrow, stdlib `hashlib.blake2b`, pytest with `pyiceberg.catalog.sql.SqlCatalog` over sqlite + `FsspecFileIO`.

## Global Constraints

- pyiceberg pinned at **0.11.1** — `create_match_filter` takes the fast `In(col, values)` path **only when `len(join_cols) == 1`** (`.venv/Lib/site-packages/pyiceberg/table/upsert_util.py`).
- The merge is the **sole** join on `merge_hash` (no in-memory exact-key backstop); safety comes from the **128-bit** width (collision ≈ 1e-23 at 100M rows), which also gives equal-hash ⟺ equal-composite-key so duplicate-key detection is unchanged.
- `merge_hash` is Iceberg **`binary`** (Arrow `pa.binary()`), every value exactly **16 bytes**; NOT declared a dlt `primary_key`.
- Hash = stdlib **`hashlib.blake2b(digest_size=16)`** over a **typed, length-prefixed canonical serialization** of the key columns. **No new third-party dependency.** Python's built-in `hash()` is per-process salted — never use it.
- Merge semantics, existing column naming, and typing must be **unchanged**. Preserve the single-commit-per-merge property (`tests/test_merge_single_commit.py`).
- Tables are partitioned by `BRANCH_ID` (identity, `etl/iceberg_load.py:440`); `merge_hash` is **not** a partition column.
- Key columns keep their original (unified-schema) casing in the batch — the hash key columns are `list(tdef.key_columns) + [settings.branch_id_column]`, identical to `primary_key` at `etl/iceberg_load.py:421`.
- Tests use the `SqlCatalog` + sqlite + `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO` pattern from `tests/test_merge_single_commit.py` (pyarrow's IO chokes on `file:///D:/` URIs on Windows). Run tests with `.venv/Scripts/python.exe -m pytest`.

---

## File Structure

- `etl/config.py` — add `merge_hash_column` setting (Task 2).
- `etl/iceberg_load.py` — all logic: hash helpers (`_serialize_keys`, `_merge_hash_array`), write helpers (`_append_merge_hash`, `_sort_by_hash`), the extracted `_finish_batch`, the readiness helper (`_table_is_hash_ready`), the merge join-column override (`_merge_join_cols`), and the wiring in `_iceberg_resource` + its three call sites + the merge path.
- `tests/test_merge_hash.py` — **new**, hashing/serialization unit tests (Tasks 1–2).
- `tests/test_merge_hash_write.py` — **new**, write-path `_finish_batch` tests (Task 3).
- `tests/test_merge_hash_merge.py` — **new**, merge + readiness + carry-forward integration tests (Tasks 4–6).
- `tests/test_merge_single_commit.py` — existing; must stay green.

---

## Task 0: Profile-first measurement gate (no code)

**No code.** Establishes the acceptance target using the already-built read-only profiler. Nothing is written to the lake.

**Files:**
- Use: `diagnostics/merge_profile.py`

- [ ] **Step 1: Benchmark hash-`In` vs composite-`Or` on the real problem tables**

Run (read-only; builds a throwaway temp table, the lake is untouched):

```bash
.venv/Scripts/python.exe diagnostics/merge_profile.py patient_master_data --hash-key --delta-rows 10000
.venv/Scripts/python.exe diagnostics/merge_profile.py patient_episodes --hash-key --delta-rows 10000
.venv/Scripts/python.exe diagnostics/merge_profile.py external_accounts_data --hash-key --delta-rows 10000
.venv/Scripts/python.exe diagnostics/merge_profile.py purchaser_ios_prices --hash-key --delta-rows 10000 --hash-sorted
```

- [ ] **Step 2: Record the target**

From each run's `HASH-KEY BENCHMARK` table, record the per-stage `SPEEDUP` and the `matched files -> composite N, hash M` line. Note the `HASH COLLISIONS` / `no collisions` line (the benchmark uses a within-process int hash; our real 128-bit hash cannot collide in practice). Write the composite→hash total speedup down in the PR description as the number Task 7 must reproduce with the real `binary` column.

> **Note for Task 7:** the profiler's `--hash-key` uses an int64 hash column; the implementation uses a 16-byte `binary` column. Task 7 re-validates on the real binary column (via a fresh hash-ready load), not only this int benchmark.

---

## Task 1: Deterministic 128-bit key hash

**What & why:** The correctness core. `_serialize_keys` turns each row's key columns into canonical, run-stable, injective bytes; `_merge_hash_array` blake2b-hashes them to a 16-byte `binary` array. Must be identical across processes (unlike `hash()`).

**Files:**
- Modify: `etl/iceberg_load.py` — add `_serialize_keys`, `_merge_hash_array` near the other merge helpers (before `_merge_iceberg_single_commit`, ~line 750).
- Test: `tests/test_merge_hash.py` (new)

**Interfaces:**
- Produces: `_serialize_keys(table: pa.Table, key_cols: list[str]) -> list[bytes]` — one canonical byte string per row.
- Produces: `_merge_hash_array(table: pa.Table, key_cols: list[str]) -> pa.Array` — `pa.binary()` array, one 16-byte digest per row, row-aligned to `table`.

- [ ] **Step 1: Write the failing unit tests**

```python
# tests/test_merge_hash.py
from __future__ import annotations
import subprocess
import sys
import pyarrow as pa
from etl.iceberg_load import _serialize_keys, _merge_hash_array


def _t(ids, branch, codes=None):
    cols = {"id": pa.array(ids, pa.int64()),
            "branch_id": pa.array(branch, pa.int64())}
    if codes is not None:
        cols["code"] = pa.array(codes, pa.string())
    return pa.table(cols)


def test_hash_is_16_byte_binary_row_aligned():
    t = _t([1, 2, 3], [7, 7, 7])
    arr = _merge_hash_array(t, ["id", "branch_id"])
    assert arr.type == pa.binary()
    assert len(arr) == 3
    assert all(len(v.as_py()) == 16 for v in arr)


def test_equal_keys_hash_equal_distinct_keys_differ():
    a = _merge_hash_array(_t([1], [7]), ["id", "branch_id"])
    b = _merge_hash_array(_t([1], [7]), ["id", "branch_id"])
    c = _merge_hash_array(_t([1], [8]), ["id", "branch_id"])   # different branch
    assert a[0].as_py() == b[0].as_py()
    assert a[0].as_py() != c[0].as_py()


def test_serialize_is_injective_across_column_boundary():
    # ("a","bc") must not serialize the same as ("ab","c")
    s1 = _serialize_keys(pa.table({"x": pa.array(["a"]), "y": pa.array(["bc"])}), ["x", "y"])
    s2 = _serialize_keys(pa.table({"x": pa.array(["ab"]), "y": pa.array(["c"])}), ["x", "y"])
    assert s1[0] != s2[0]


def test_serialize_null_differs_from_empty_string():
    s_null = _serialize_keys(pa.table({"x": pa.array([None], pa.string())}), ["x"])
    s_empty = _serialize_keys(pa.table({"x": pa.array([""], pa.string())}), ["x"])
    assert s_null[0] != s_empty[0]


def test_hash_stable_across_a_fresh_process():
    # A salted/nondeterministic hash would change between interpreter runs.
    code = (
        "import pyarrow as pa;"
        "from etl.iceberg_load import _merge_hash_array;"
        "t=pa.table({'id':pa.array([12345],pa.int64()),"
        "'branch_id':pa.array([7],pa.int64())});"
        "print(_merge_hash_array(t,['id','branch_id'])[0].as_py().hex())"
    )
    out1 = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    out2 = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out1.stdout.strip() == out2.stdout.strip()
    assert len(out1.stdout.strip()) == 32   # 16 bytes hex
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash.py -v`
Expected: FAIL with `ImportError: cannot import name '_serialize_keys'`.

- [ ] **Step 3: Implement the helpers**

Add to `etl/iceberg_load.py` (with `import hashlib` and `import struct` at the top of the module, and `import pyarrow.compute as pc` if not already imported):

```python
def _serialize_keys(table: pa.Table, key_cols: list[str]) -> list[bytes]:
    """Canonical, injective, run-stable byte encoding of each row's key.

    Per column, per row: a 1-byte null flag; for a present value, a 4-byte
    big-endian length prefix then the value's canonical bytes -- integers as
    8-byte big-endian, everything else as its UTF-8 string cast. Length-prefixing
    makes the concatenation injective across column boundaries; the null flag
    distinguishes null from an empty string. Computed after cast_table_to_schema,
    so the column types (hence the bytes) are identical every run.
    """
    col_encodings: list[list[bytes | None]] = []
    for name in key_cols:
        col = table.column(name)
        if pa.types.is_integer(col.type):
            col_encodings.append(
                [None if v is None else struct.pack(">q", int(v)) for v in col.to_pylist()])
        else:
            strs = pc.cast(col, pa.string()).to_pylist()
            col_encodings.append(
                [None if v is None else v.encode("utf-8") for v in strs])
    out: list[bytes] = []
    for i in range(table.num_rows):
        parts = bytearray()
        for enc in col_encodings:
            v = enc[i]
            if v is None:
                parts += b"\x01"
            else:
                parts += b"\x00" + struct.pack(">I", len(v)) + v
        out.append(bytes(parts))
    return out


def _merge_hash_array(table: pa.Table, key_cols: list[str]) -> pa.Array:
    """128-bit blake2b of each row's canonical key serialization -> pa.binary().

    Deterministic across processes and library versions (unlike the salted
    built-in hash()). Every value is exactly 16 bytes.
    """
    digests = [hashlib.blake2b(b, digest_size=16).digest()
               for b in _serialize_keys(table, key_cols)]
    return pa.array(digests, type=pa.binary())
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash.py -v`
Expected: PASS (all five tests).

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_hash.py
git commit -m "feat(etl): deterministic 128-bit merge-key hash helper"
```

---

## Task 2: Write helpers + `merge_hash_column` setting

**What & why:** Small, isolated helpers the write path composes, plus the config knob. `_append_merge_hash` adds the column; `_sort_by_hash` clusters the batch (your step 1) and partly recovers the lost `BRANCH_ID` partition pruning. Kept separate so carry-forward can sit between them (Task 6).

**Files:**
- Modify: `etl/config.py:275-278` — add the setting.
- Modify: `etl/iceberg_load.py` — add `_append_merge_hash`, `_sort_by_hash` beside the Task 1 helpers.
- Test: `tests/test_merge_hash.py` (extend)

**Interfaces:**
- Consumes: `_merge_hash_array` (Task 1).
- Produces: `_append_merge_hash(tbl: pa.Table, key_cols: list[str], hash_col: str) -> pa.Table` — `tbl` with the hash column appended (no sort).
- Produces: `_sort_by_hash(tbl: pa.Table, hash_col: str) -> pa.Table` — `tbl` sorted ascending by `hash_col`; no-op if empty or the column is absent.
- Produces: `Settings.merge_hash_column: str = "merge_hash"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_merge_hash.py`:

```python
from etl.iceberg_load import _append_merge_hash, _sort_by_hash
from etl.config import Settings


def test_append_adds_binary_hash_column():
    t = _t([1, 2], [7, 7])
    out = _append_merge_hash(t, ["id", "branch_id"], "merge_hash")
    assert "merge_hash" in out.column_names
    assert out.schema.field("merge_hash").type == pa.binary()
    assert out.num_rows == 2


def test_sort_by_hash_orders_rows_and_is_stable():
    t = _append_merge_hash(_t([3, 1, 2], [7, 7, 7]), ["id", "branch_id"], "merge_hash")
    out = _sort_by_hash(t, "merge_hash")
    hashes = [v.as_py() for v in out.column("merge_hash")]
    assert hashes == sorted(hashes)


def test_sort_by_hash_noop_when_missing_or_empty():
    t = _t([1], [7])
    assert _sort_by_hash(t, "merge_hash").equals(t)        # column absent
    empty = t.slice(0, 0)
    assert _sort_by_hash(_append_merge_hash(empty, ["id", "branch_id"], "merge_hash"),
                         "merge_hash").num_rows == 0


def test_settings_has_merge_hash_column_default():
    assert Settings().merge_hash_column == "merge_hash"
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash.py -k "append or sort_by_hash or merge_hash_column" -v`
Expected: FAIL with `ImportError: cannot import name '_append_merge_hash'`.

- [ ] **Step 3: Add the setting**

In `etl/config.py`, after line 278 (`inserted_ts_column`), add:

```python
    merge_hash_column: str = "merge_hash"             # single-column merge key derived from PK+BRANCH_ID
```

- [ ] **Step 4: Implement the write helpers**

Add to `etl/iceberg_load.py` beside the Task 1 helpers:

```python
def _append_merge_hash(tbl: pa.Table, key_cols: list[str], hash_col: str) -> pa.Table:
    """Append the derived merge-hash column (no sort). Row-aligned to ``tbl``."""
    return tbl.append_column(hash_col, _merge_hash_array(tbl, key_cols))


def _sort_by_hash(tbl: pa.Table, hash_col: str) -> pa.Table:
    """Cluster a batch by the merge hash so per-file min/max can prune the In
    scan (and less copy-on-write rewrite). No-op if the column is absent or the
    table is empty; row content/count is unchanged either way."""
    if hash_col not in tbl.column_names or tbl.num_rows == 0:
        return tbl
    return tbl.sort_by([(hash_col, "ascending")])
```

- [ ] **Step 5: Run to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash.py -v`
Expected: PASS (all tests, old and new).

- [ ] **Step 6: Commit**

```bash
git add etl/config.py etl/iceberg_load.py tests/test_merge_hash.py
git commit -m "feat(etl): merge-hash write helpers + merge_hash_column setting"
```

---

## Task 3: Write path — hash + sort in `_finish`, gated by `write_hash`

**What & why:** Extract the `_finish` closure body into a testable module-level `_finish_batch`, add hashing+sorting gated by a `write_hash` flag, thread the flag through `_iceberg_resource` and its three call sites. Rebuild writes the hash for every branch; snapshot append never does; the merge call site is wired in Task 4.

**Files:**
- Modify: `etl/iceberg_load.py` — add `_finish_batch`; rewrite `_iceberg_resource._finish` to delegate; add `write_hash` param to `_iceberg_resource`; add the `merge_hash` `columns` hint; update the two call sites in `_run_per_branch_rebuild` (`:495`) and `_run_per_branch_append` (`:517`).
- Test: `tests/test_merge_hash_write.py` (new)

**Interfaces:**
- Consumes: `_append_merge_hash`, `_sort_by_hash` (Task 2), `_carry_forward_insert_at` (`:293`), `types_map.cast_table_to_schema`.
- Produces: `_finish_batch(tbl, schema, existing_insert_at, insert_col, write_hash, hash_key_cols, hash_col, carry_keys) -> pa.Table`.
- Produces: `_iceberg_resource(plan, settings, paths, disposition, existing_insert_at=None, write_hash=False)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_merge_hash_write.py
from __future__ import annotations
import pyarrow as pa
from etl.iceberg_load import _finish_batch


def _schema():
    return pa.schema([("id", pa.int64()), ("branch_id", pa.int64()), ("v", pa.string())])


def _batch(ids, vs, branch=7):
    return pa.table({"id": pa.array(ids, pa.int64()),
                     "branch_id": pa.array([branch] * len(ids), pa.int64()),
                     "v": pa.array(vs)})


def test_finish_batch_appends_and_sorts_hash_when_enabled():
    out = _finish_batch(_batch([3, 1, 2], ["c", "a", "b"]), _schema(),
                        existing_insert_at=None, insert_col="insert_at",
                        write_hash=True, hash_key_cols=["id", "branch_id"],
                        hash_col="merge_hash", carry_keys=["id", "branch_id"])
    assert "merge_hash" in out.column_names
    assert out.schema.field("merge_hash").type == pa.binary()
    hashes = [h.as_py() for h in out.column("merge_hash")]
    assert hashes == sorted(hashes)                       # sorted by hash
    assert out.num_rows == 3                              # no rows lost


def test_finish_batch_no_hash_when_disabled():
    out = _finish_batch(_batch([1, 2], ["a", "b"]), _schema(),
                        existing_insert_at=None, insert_col="insert_at",
                        write_hash=False, hash_key_cols=["id", "branch_id"],
                        hash_col="merge_hash", carry_keys=["id", "branch_id"])
    assert "merge_hash" not in out.column_names
    assert out.column_names == ["id", "branch_id", "v"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_write.py -v`
Expected: FAIL with `ImportError: cannot import name '_finish_batch'`.

- [ ] **Step 3: Implement `_finish_batch` and rewire `_iceberg_resource`**

Add `_finish_batch` near `_iceberg_resource` in `etl/iceberg_load.py`:

```python
def _finish_batch(
    tbl: pa.Table, schema: pa.Schema, existing_insert_at: Optional[pa.Table],
    insert_col: str, write_hash: bool, hash_key_cols: list[str], hash_col: str,
    carry_keys: list[str],
) -> pa.Table:
    """Reshape one streamed batch: cast to the unified schema, (optionally)
    derive the merge hash, carry forward insert_at for existing rows, and
    (when hashing) leave the batch clustered by the hash.

    The hash is appended BEFORE carry-forward so the carry-forward join can key
    on it (``carry_keys`` may be the hash column). The final sort-by-hash runs
    after the join, since a join does not preserve row order.
    """
    tbl = types_map.cast_table_to_schema(tbl, schema)
    if write_hash:
        tbl = _append_merge_hash(tbl, hash_key_cols, hash_col)
    if existing_insert_at is not None and tbl.num_rows:
        tbl = _carry_forward_insert_at(tbl, existing_insert_at, carry_keys, insert_col)
    if write_hash:
        tbl = _sort_by_hash(tbl, hash_col)
    return tbl
```

Change `_iceberg_resource`'s signature and body. Replace the current signature (`:390-396`) and `_finish` (`:429-434`) with:

```python
def _iceberg_resource(
    plan: TableLoadPlan,
    settings: Settings,
    paths: list,
    disposition: str,
    existing_insert_at: Optional[pa.Table] = None,
    write_hash: bool = False,
):
```

Inside, after `primary_key = ...` (`:421`), add the hash-key columns and column hint:

```python
    hash_col = settings.merge_hash_column
    hash_key_cols = primary_key   # PK + BRANCH_ID, same list, original casing
```

Replace the `_finish` closure with a delegator:

```python
    def _finish(tbl: pa.Table) -> pa.Table:
        return _finish_batch(
            tbl, schema, existing_insert_at, insert_col,
            write_hash, hash_key_cols, hash_col, carry_keys=primary_key)
```

In the `columns = { ... }` dict (`:439-443`), add the hash column hint only when writing it:

```python
    if write_hash:
        columns[hash_col] = {"data_type": "binary"}
```

(Place this after the `columns` dict literal and before the `if is_snapshot:` block.)

- [ ] **Step 4: Wire the rebuild/append call sites**

In `_run_per_branch_rebuild` (`:494-496`), pass `write_hash` for a real (non-snapshot) rebuild — a full replace always writes the hash so the table becomes hash-ready:

```python
        _run_pipeline(
            pipeline, [_iceberg_resource(plan, settings, [r.staged_path], disposition,
                                         write_hash=not plan.tdef.is_snapshot)],
            settings, f"{plan.tdef.dataset_table_name}:branch={r.branch_id}:{disposition}")
```

`_run_per_branch_append` (`:516-518`) is snapshot-only — leave it as-is (`write_hash` defaults to `False`).

- [ ] **Step 5: Run the write tests + the existing suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_write.py tests/test_merge_single_commit.py -v`
Expected: PASS (new write tests pass; single-commit suite still green).

- [ ] **Step 6: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_hash_write.py
git commit -m "feat(etl): compute+sort merge_hash on hash-enabled writes"
```

---

## Task 4: Reload-gated readiness + wire the merge write path

**What & why:** A table is hash-ready iff a prior full `replace` already wrote `merge_hash`. Detect it from the stored Iceberg schema and pass it as the merge path's `write_hash`, so an incremental delta carries the hash only when the stored side already has it (never half-populating via `union_by_name`).

**Files:**
- Modify: `etl/iceberg_load.py` — add `_table_is_hash_ready`; set `write_hash` at the merge call site (`:964-966`).
- Test: `tests/test_merge_hash_merge.py` (new)

**Interfaces:**
- Produces: `_table_is_hash_ready(pipeline, tdef, hash_col: str) -> bool`.
- Consumes: `dlt.common.libs.pyiceberg.get_iceberg_tables` (already used by `_existing_insert_at`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_merge_hash_merge.py
from __future__ import annotations
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from etl.iceberg_load import _table_is_hash_ready


class _Tdef:
    dataset_table_name = "m_ready"


def _cat(tmp_path, tag):
    cat = SqlCatalog("t", uri=f"sqlite:///{(tmp_path/f'c_{tag}.db').as_posix()}",
                     warehouse=(tmp_path/f"w_{tag}").as_uri(),
                     **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    return cat


def test_hash_ready_true_only_when_column_present(tmp_path, monkeypatch):
    cat = _cat(tmp_path, "r")
    with_hash = pa.table({"id": pa.array([1], pa.int64()),
                          "merge_hash": pa.array([b"x" * 16], pa.binary())})
    without = pa.table({"id": pa.array([1], pa.int64())})
    t_ready = cat.create_table("oasis.ready", schema=with_hash.schema)
    t_ready.append(with_hash)
    t_plain = cat.create_table("oasis.plain", schema=without.schema)
    t_plain.append(without)

    # The function does a call-time `from dlt.common.libs.pyiceberg import
    # get_iceberg_tables`, so patch the attribute on that source module.
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_iceberg_tables",
                        lambda pipeline: pipeline)   # pipeline IS the {name: table} map

    assert _table_is_hash_ready({_Tdef.dataset_table_name: t_ready}, _Tdef, "merge_hash") is True
    assert _table_is_hash_ready({_Tdef.dataset_table_name: t_plain}, _Tdef, "merge_hash") is False
    assert _table_is_hash_ready({}, _Tdef, "merge_hash") is False   # table missing
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_merge.py::test_hash_ready_true_only_when_column_present -v`
Expected: FAIL with `ImportError: cannot import name '_table_is_hash_ready'`.

- [ ] **Step 3: Implement `_table_is_hash_ready`**

The test monkeypatches a module-level `get_iceberg_tables`, so import it at module scope. Near `_existing_insert_at`, add:

```python
def _table_is_hash_ready(pipeline, tdef, hash_col: str) -> bool:
    """True iff the stored Iceberg table already carries ``hash_col`` -- i.e. a
    prior full replace wrote it for every row. Missing table or any read error
    -> not ready (the merge falls back to the composite key). Best-effort: never
    fails a load."""
    try:
        from dlt.common.libs.pyiceberg import get_iceberg_tables
        tbl = get_iceberg_tables(pipeline).get(tdef.dataset_table_name)
    except Exception:  # noqa: BLE001 - best effort
        return False
    if tbl is None:
        return False
    return hash_col.lower() in {f.name for f in tbl.schema().fields}
```

> The `except Exception` import-and-call keeps the same best-effort contract as `_existing_insert_at`. The test patches `etl.iceberg_load.get_iceberg_tables`; add a module-level `from dlt.common.libs.pyiceberg import get_iceberg_tables` **or** keep the local import and have the test patch it there. Simplest: add the module-level import at the top of `etl/iceberg_load.py` and call the bare name here (drop the local import).

- [ ] **Step 4: Wire the merge call site**

In the merge branch (`:959-967`), compute readiness and pass it:

```python
            existing = _existing_insert_at(
                pipeline, tdef, settings,
                [r.branch_id for r in plan.success], plan.unified_schema)
            hash_ready = _table_is_hash_ready(pipeline, tdef, settings.merge_hash_column)
            _run_pipeline(
                pipeline,
                [_iceberg_resource(
                    plan, settings, [r.staged_path for r in plan.success],
                    plan.disposition, existing_insert_at=existing,
                    write_hash=hash_ready)],
                settings, f"{tdef.dataset_table_name}:{plan.disposition}")
```

- [ ] **Step 5: Run to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_merge.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_hash_merge.py
git commit -m "feat(etl): reload-gated hash readiness wired into the merge path"
```

---

## Task 5: Merge join-column override

**What & why:** Make the single-commit merge join on `merge_hash` when both the stored table and the delta carry it, else the composite key (today's behavior). This is where the `Or`→`In` win actually lands.

**Files:**
- Modify: `etl/iceberg_load.py` — add `_merge_join_cols`; use it in `_merge_iceberg_single_commit` (`:788-799`).
- Test: `tests/test_merge_hash_merge.py` (extend)

**Interfaces:**
- Consumes: the pyiceberg `table`, the delta `data` (pa.Table), `settings.merge_hash_column`.
- Produces: `_merge_join_cols(table, data, composite: list[str], hash_col: str) -> list[str]`.

- [ ] **Step 1: Write the failing integration tests**

Append to `tests/test_merge_hash_merge.py`:

```python
import pyarrow.compute as pc
from etl.iceberg_load import (_merge_join_cols, _merge_iceberg_single_commit,
                              _append_merge_hash)


def _schema_dict():
    return {
        "x-merge-strategy": "upsert",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True},
            "branch_id": {"name": "branch_id", "data_type": "bigint", "primary_key": True},
            "name": {"name": "name", "data_type": "text"},
        },
    }


def _rows(ids, names, branch=1, with_hash=False):
    t = pa.table({"id": pa.array(ids, pa.int64()),
                  "name": pa.array(names),
                  "branch_id": pa.array([branch] * len(ids), pa.int64())})
    return _append_merge_hash(t, ["id", "branch_id"], "merge_hash") if with_hash else t


def test_join_cols_picks_hash_only_when_both_sides_have_it(tmp_path):
    cat = _cat(tmp_path, "jc")
    ready = _rows([1], ["a"], with_hash=True)
    t = cat.create_table("oasis.jc", schema=ready.schema)
    t.append(ready)
    assert _merge_join_cols(t, _rows([2], ["b"], with_hash=True),
                            ["id", "branch_id"], "merge_hash") == ["merge_hash"]
    assert _merge_join_cols(t, _rows([2], ["b"], with_hash=False),
                            ["id", "branch_id"], "merge_hash") == ["id", "branch_id"]


def _seed(tmp_path, tag, with_hash):
    cat = _cat(tmp_path, tag)
    seed = _rows([0], ["seed"], with_hash=with_hash)
    t = cat.create_table(f"oasis.m_{tag}", schema=seed.schema)
    t.append(seed)
    return t


def test_hash_ready_merge_updates_and_inserts(tmp_path):
    t = _seed(tmp_path, "hm", with_hash=True)
    before = len(list(t.metadata.snapshots))
    _merge_iceberg_single_commit(t, _rows([0, 1], ["u0", "n1"], with_hash=True),
                                 _schema_dict(), "m")
    t.refresh()
    got = t.scan().to_arrow()
    assert got.num_rows == 2
    assert got.filter(pc.equal(got["id"], 0)).to_pydict()["name"] == ["u0"]   # updated
    assert len(list(t.metadata.snapshots)) - before == 1                      # single commit


def test_not_ready_merge_falls_back_and_adds_no_hash(tmp_path):
    t = _seed(tmp_path, "cm", with_hash=False)
    _merge_iceberg_single_commit(t, _rows([0, 1], ["u0", "n1"], with_hash=False),
                                 _schema_dict(), "m")
    t.refresh()
    assert "merge_hash" not in {f.name for f in t.schema().fields}   # stayed not-ready
    assert t.scan().to_arrow().num_rows == 2
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_merge.py -k "join_cols or ready_merge or falls_back" -v`
Expected: FAIL with `ImportError: cannot import name '_merge_join_cols'`.

- [ ] **Step 3: Implement the override**

Add near `_merge_iceberg_single_commit`:

```python
def _merge_join_cols(table, data, composite: list[str], hash_col: str) -> list[str]:
    """Join on the single hash column when BOTH the stored table and the delta
    carry it (hash-ready) -- pyiceberg then takes the fast In path. Otherwise the
    composite key, unchanged. 128-bit width makes equal-hash <=> equal-key, so
    match/insert and duplicate detection are semantically identical either way."""
    stored = {f.name for f in table.schema().fields}
    if hash_col in stored and hash_col in data.column_names:
        return [hash_col]
    return composite
```

In `_merge_iceberg_single_commit`, replace the join-cols block (`:788-799`) so the composite result is overridden and used as the upsert key:

```python
    if "parent" in schema:
        join_cols = [get_first_column_name_with_prop(schema, "unique")]
    else:
        join_cols = get_columns_names_with_prop(schema, "primary_key")

    normalized = ensure_iceberg_compatible_arrow_data(data)
    from etl.config import Settings
    join_cols = _merge_join_cols(table, normalized, join_cols, Settings().merge_hash_column)

    table.upsert(
        df=normalized,
        join_cols=join_cols,
        when_matched_update_all=(strategy == "upsert"),
        when_not_matched_insert_all=True,
        case_sensitive=True,
    )
```

(`ensure_iceberg_compatible_arrow_data` is already imported at the top of the function; move its call up as shown so `_merge_join_cols` checks the same frame the upsert uses. `Settings().merge_hash_column` reuses the default — no signature change to the installed-merge hook.)

- [ ] **Step 4: Run the merge tests + the existing suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_merge.py tests/test_merge_single_commit.py -v`
Expected: PASS (hash-ready merge joins on the hash; not-ready falls back and adds no column; single commit preserved).

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_hash_merge.py
git commit -m "feat(etl): merge joins on merge_hash In path when hash-ready"
```

---

## Task 6: Carry-forward insert_at on the hash key

**What & why:** "Any operation involving the composite key" also uses the hash when ready. On a hash-ready table the `insert_at` carry-forward reads the stored `merge_hash` and joins on it — a cheaper single-column Arrow join, same collision-proof guarantee. Not ready → composite (unchanged).

**Files:**
- Modify: `etl/iceberg_load.py` — `_existing_insert_at` selects `merge_hash` when ready; the merge call site passes hash-based `carry_keys`; `_finish_batch` uses them.
- Test: `tests/test_merge_hash_merge.py` (extend)

**Interfaces:**
- Modifies: `_existing_insert_at(pipeline, tdef, settings, branches, unified_schema, hash_ready: bool = False)` — when `hash_ready`, returns rows keyed by `merge_hash` (+ `insert_at`) instead of the composite key.
- The merge call site passes `carry_keys = [settings.merge_hash_column] if hash_ready else composite` into `_iceberg_resource` (new `carry_keys` param) → `_finish_batch`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_merge_hash_merge.py`:

```python
def test_carry_forward_preserves_insert_at_via_hash(tmp_path):
    # existing row keyed by hash with an OLD insert_at; batch re-loads same key
    from etl.iceberg_load import _finish_batch
    existing = _append_merge_hash(
        pa.table({"id": pa.array([5], pa.int64()),
                  "branch_id": pa.array([1], pa.int64())}),
        ["id", "branch_id"], "merge_hash").append_column(
            "insert_at", pa.array([pa.scalar("2020-01-01")], pa.string()))
    existing = existing.select(["merge_hash", "insert_at"])

    schema = pa.schema([("id", pa.int64()), ("branch_id", pa.int64()),
                        ("insert_at", pa.string())])
    batch = pa.table({"id": pa.array([5], pa.int64()),
                      "branch_id": pa.array([1], pa.int64()),
                      "insert_at": pa.array(["2026-07-19"], pa.string())})
    out = _finish_batch(batch, schema, existing_insert_at=existing,
                        insert_col="insert_at", write_hash=True,
                        hash_key_cols=["id", "branch_id"], hash_col="merge_hash",
                        carry_keys=["merge_hash"])
    assert out.column("insert_at").to_pylist() == ["2020-01-01"]   # old value kept
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_merge.py::test_carry_forward_preserves_insert_at_via_hash -v`
Expected: FAIL — `_finish_batch` appends `merge_hash` after cast, but the carry-forward join on `["merge_hash"]` must find it; it fails only if the existing/existing-column wiring is wrong. (If it already passes, the join wiring from Task 3 is correct; proceed to wire `_existing_insert_at`.)

> This test exercises `_finish_batch` directly (Task 3 already appends the hash before carry-forward), so it verifies the batch side. Steps 3–4 wire the *existing* side and the call site.

- [ ] **Step 3: Make `_existing_insert_at` return hash-keyed rows when ready**

Add a `hash_ready: bool = False` parameter to `_existing_insert_at` (`:320-323`). When `hash_ready`, project `merge_hash` (+ `insert_at`) instead of the composite key and skip the composite rename/retype block:

```python
def _existing_insert_at(
    pipeline, tdef: TableDef, settings: Settings, branches: list[int],
    unified_schema: pa.Schema, hash_ready: bool = False,
) -> Optional[pa.Table]:
    ...
    insert_col = settings.inserted_ts_column
    hash_col = settings.merge_hash_column
    ...
    insert_norm = insert_col.lower()
    branch_norm = settings.branch_id_column.lower()
    iceberg_cols = {f.name for f in tbl.schema().fields}
    if insert_norm not in iceberg_cols:
        return None
    if hash_ready:
        if hash_col not in iceberg_cols:
            return None
        try:
            existing = tbl.scan(
                row_filter=In(branch_norm, set(branches)),
                selected_fields=(hash_col, insert_norm),
            ).to_arrow()
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] insert_at carry-forward scan failed: %s",
                        tdef.dataset_table_name, exc)
            return None
        if existing.num_rows == 0:
            return None
        return existing.rename_columns(
            [insert_col if n == insert_norm else n for n in existing.column_names])
    # --- composite path (unchanged from today) ---
    key_norms = [k.lower() for k in (list(tdef.key_columns) + [settings.branch_id_column])]
    if any(k not in iceberg_cols for k in key_norms):
        return None
    ...
```

(Keep the existing composite body verbatim below the `# --- composite path` marker.)

- [ ] **Step 4: Pass hash-based carry_keys from the merge call site**

Add a `carry_keys` param to `_iceberg_resource` (default = `primary_key`) and thread it into `_finish_batch`:

```python
def _iceberg_resource(plan, settings, paths, disposition,
                      existing_insert_at=None, write_hash=False, carry_keys=None):
    ...
    def _finish(tbl: pa.Table) -> pa.Table:
        return _finish_batch(
            tbl, schema, existing_insert_at, insert_col,
            write_hash, hash_key_cols, hash_col,
            carry_keys=carry_keys if carry_keys is not None else primary_key)
```

At the merge call site (Task 4 edit), compute both from `hash_ready`:

```python
            hash_ready = _table_is_hash_ready(pipeline, tdef, settings.merge_hash_column)
            existing = _existing_insert_at(
                pipeline, tdef, settings,
                [r.branch_id for r in plan.success], plan.unified_schema,
                hash_ready=hash_ready)
            carry_keys = ([settings.merge_hash_column] if hash_ready
                          else list(tdef.key_columns) + [settings.branch_id_column])
            _run_pipeline(
                pipeline,
                [_iceberg_resource(
                    plan, settings, [r.staged_path for r in plan.success],
                    plan.disposition, existing_insert_at=existing,
                    write_hash=hash_ready, carry_keys=carry_keys)],
                settings, f"{tdef.dataset_table_name}:{plan.disposition}")
```

- [ ] **Step 5: Run the full hash + merge suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash.py tests/test_merge_hash_write.py tests/test_merge_hash_merge.py tests/test_merge_single_commit.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_hash_merge.py
git commit -m "feat(etl): carry insert_at forward on the hash key when hash-ready"
```

---

## Task 7: Full-suite regression + real-column perf validation

**What & why:** Confirm nothing regressed and that the real 16-byte `binary` merge reproduces the Task 0 hash-`In` win (not just the profiler's int benchmark).

**Files:**
- Use: `diagnostics/merge_profile.py`, the full test suite.

- [ ] **Step 1: Run the entire test suite**

Run: `.venv/Scripts/python.exe -m pytest tests/ -v`
Expected: PASS (all green; no regression in non-merge tests).

- [ ] **Step 2: Produce a hash-ready copy of a real problem table and re-profile**

On a scratch dataset (never production), run a full `INITIAL` load of one problem table (e.g. `patient_master_data`) with the new code so it is written hash-ready, then profile a real incremental merge:

```bash
.venv/Scripts/python.exe diagnostics/merge_profile.py patient_master_data --delta-rows 10000
```

Confirm the `filter path` line now reports `In (fast)` for the hash-ready table's merge and that stage `1. build match filter` + `2a. plan matched files` collapsed versus the composite baseline in `docs/superpowers/plans/2026-07-19-composite-key-merge-optimization.md` (the 90–96% `Or` cost). Record before/after.

> If the profiler still reports composite for the table (its `_join_cols` derives the key from tables.json, not the stored `merge_hash`), profile via the `--hash-key` benchmark instead and additionally verify by hand that a real pipeline incremental run on the hash-ready table logs a single-column merge. Note which validation you used.

- [ ] **Step 3: Record the result and finish the branch**

Write the measured speedup into the PR description against the Task 0 target. Then use the `superpowers:finishing-a-development-branch` skill to choose merge/PR/cleanup.

---

## Self-Review

**Spec coverage:**
- Component 1 (`merge_hash` column, blake2b, canonical serialization) → Tasks 1–2.
- Component 2 (write path: hash + sort, `write_hash` rule for rebuild/append) → Tasks 2–3.
- Component 3 (merge join override; carry-forward on hash) → Tasks 5–6.
- Component 4 (reload-gated readiness; no half-populated column) → Task 4 (readiness) + Task 5 test `test_not_ready_merge_falls_back_and_adds_no_hash`.
- Validation & tests (profile-first; cross-process stability; injectivity; merge correctness/fallback; single-commit; carry-forward) → Task 0 + Tasks 1–7.
- Scope/non-goals (snapshot untouched; no backfill; additive only) → snapshot path left as-is (Task 3 Step 4), no backfill task exists by design.

**Placeholder scan:** none — every code/test step carries real content and exact commands.

**Type consistency:** `_serialize_keys -> list[bytes]`, `_merge_hash_array -> pa.Array (binary)`, `_append_merge_hash`/`_sort_by_hash -> pa.Table`, `_finish_batch(...) -> pa.Table`, `_table_is_hash_ready(...) -> bool`, `_merge_join_cols(...) -> list[str]` are defined once and consumed with matching signatures. `write_hash` and `carry_keys` thread consistently through `_iceberg_resource` → `_finish_batch`. `hash_col` / `settings.merge_hash_column` is `"merge_hash"` throughout (already lower-case, no normalization mismatch).

**Ordering constraint:** Task 1 → 2 (helpers) → 3 (write path uses them) → 4 (readiness gates the merge delta) → 5 (join override consumes the hashed delta) → 6 (carry-forward on hash) → 7 (regression + profile). Task 4 must precede 5 because the override only fires on a hashed delta, which requires `write_hash=hash_ready`.
