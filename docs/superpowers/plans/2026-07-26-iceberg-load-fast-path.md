# Iceberg Load Fast Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut Iceberg load wall-clock — measured to be 92–98% of an INITIAL run — by eliminating per-branch commit overhead, no-op maintenance commits, and per-row Python on the serial load thread.

**Architecture:** Four independent, individually-shippable optimizations inside the existing load module: (1) pack full-rebuild/append branches into size-budgeted groups so a small table commits once instead of once per branch; (2) skip snapshot-retention commits when there is nothing to do; (3) replace the per-row Python merge-hash serializer with zero-copy Arrow-buffer slicing (byte-identical digests, verified); (4) shrink the carry-forward `existing` table to only the rows the merge delta can touch. No new dependencies, no schema or hash-format changes, no dlt/pyiceberg patching beyond what already exists.

**Tech Stack:** Python 3.13, pyarrow, pyiceberg 0.11.1, dlt (filesystem destination, Iceberg table format), pytest.

**Measured baseline (2026-07-20 run_logs):** serial load thread busy 92–98% on INITIAL; a 23,587-row table took 2:50 across 8 per-branch commits (~13–32s each); small tables are 39–54% of total load time in pure commit overhead; collapsing them is estimated to save ~12–14 min per INITIAL run.

## Global Constraints

- **No new third-party dependencies.** The venv has NO numpy. Fast paths must use only stdlib + pyarrow.
- **The merge-hash bytes are FROZEN.** Stored tables already carry `merge_hash` values; any drift in framing or digest silently duplicates rows on later merges. Every hash change must be byte-identical to `_serialize_keys` framing + `blake2b(digest_size=16)`, enforced by differential tests. (Verified 2026-07-26: the Task 3 implementation is byte-identical on ints/decimals/strings/nulls/unicode/chunked/sliced/empty inputs, 2.2× faster on 1M rows.)
- **Best-effort rule:** new reads of the destination or staged files must never fail a load — log a warning and degrade to current behavior (matches `_existing_insert_at`, `_coerce_unified_nulls`, etc.).
- **Do not modify anything under `.venv/`.**
- Windows dev host. Run tests with `.venv/Scripts/python.exe -m pytest`.
- Work on branch `perf/iceberg-load-fast-path` off `main`.
- Function names `_run_per_branch_rebuild` / `_run_per_branch_append` are kept (existing tests and callers reference them) even though they now operate on groups.
- `tables.json` is gitignored and untouched by this plan.

**Explicit non-goals (separate future plans):** direct-pyiceberg single-transaction rebuild; parallel load slots (2–3 isolated pipelines — needs per-slot pipeline_name/pipelines_dir, ControlStore lock, sticky table→slot assignment); `PYICEBERG_MAX_WORKERS` deployment tuning (only pays off once a run writes multiple partitions, i.e. after Task 1 — note it in the ops runbook when deploying).

---

### Task 1: Size-budgeted branch grouping for full rebuilds and snapshot appends

The per-branch loop in `_run_per_branch_rebuild` exists only to bound memory (dlt's filesystem-Iceberg loader materializes a whole load package via `arrow_dataset.to_table()` before writing). Small tables don't need that bound: pack consecutive branches into groups whose **staged parquet bytes** fit a budget, one dlt run (= one Iceberg commit) per group. A branch bigger than the budget gets its own group — exactly today's behavior. First group `replace`, later groups `append`; snapshot tables append every group.

**Files:**
- Modify: `etl/config.py` (Settings dataclass ~line 322, `load_settings()` ~line 556)
- Modify: `etl/iceberg_load.py:683-736` (`_run_per_branch_rebuild`, `_run_per_branch_append`; add `_group_by_staged_bytes` above them)
- Test: `tests/test_load_grouping.py` (new)

**Interfaces:**
- Consumes: `ExtractResult.staged_path: Path`, `TableLoadPlan.success: list[ExtractResult]`, `Settings`, `_iceberg_resource(plan, settings, paths, disposition, **kw)`, `_run_pipeline(pipeline, resources, settings, label)` — all existing.
- Produces: `Settings.load_group_max_bytes: int` (default `256 * 1024 * 1024`); `_group_by_staged_bytes(results: list[ExtractResult], max_bytes: int) -> list[list[ExtractResult]]`. `_run_per_branch_rebuild` / `_run_per_branch_append` keep their exact signatures.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_load_grouping.py`:

```python
"""Full rebuilds/appends pack branches into size-budgeted groups: one commit each."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, Settings, TableDef
from etl.oracle_extract import ExtractResult


def _tdef():
    return TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _staged_parquet(base, tdef, branch, rows=2):
    d = base / tdef.dataset_table_name
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{branch}.parquet"
    pq.write_table(pa.table({"ID": pa.array(list(range(rows)), pa.int64())}), p)
    return p


def _result(tdef, branch, branch_id, staged, rows=2):
    return ExtractResult(table_def=tdef, branch=branch, branch_id=branch_id,
                         status="SUCCESS", row_count=rows, staged_path=staged)


def _three_results(tmp_path):
    tdef = _tdef()
    return tdef, [
        _result(tdef, f"b{i}", i, _staged_parquet(tmp_path, tdef, f"b{i}"))
        for i in (1, 2, 3)
    ]


class _FakeControl:
    def __init__(self):
        self.advanced = []

    def advance(self, r):
        self.advanced.append(r)

    def save(self):
        pass


def _record_runs(monkeypatch):
    """Stub the dlt run; record (paths, disposition) per _iceberg_resource call."""
    calls = []

    def fake_resource(plan, settings, paths, disposition, **kw):
        calls.append((list(paths), disposition))
        return None

    monkeypatch.setattr(iceberg_load, "_iceberg_resource", fake_resource)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    return calls


# ------------------------- _group_by_staged_bytes ------------------------- #

def test_grouping_collapses_small_branches_into_one_group(tmp_path):
    _, results = _three_results(tmp_path)
    groups = iceberg_load._group_by_staged_bytes(results, max_bytes=10**9)
    assert groups == [results]


def test_grouping_splits_when_budget_exceeded(tmp_path):
    _, results = _three_results(tmp_path)
    groups = iceberg_load._group_by_staged_bytes(results, max_bytes=1)
    assert groups == [[r] for r in results]      # every branch oversized: solo


def test_grouping_preserves_branch_order(tmp_path):
    _, results = _three_results(tmp_path)
    size = results[0].staged_path.stat().st_size  # all three files identical
    groups = iceberg_load._group_by_staged_bytes(results, max_bytes=2 * size)
    assert [r for g in groups for r in g] == results
    assert groups == [results[:2], results[2:]]   # packed 2 + 1


def test_grouping_isolates_missing_staged_file(tmp_path):
    tdef = _tdef()
    ok1 = _result(tdef, "b1", 1, _staged_parquet(tmp_path, tdef, "b1"))
    gone = _result(tdef, "b2", 2, tmp_path / tdef.dataset_table_name / "gone.parquet")
    ok2 = _result(tdef, "b3", 3, _staged_parquet(tmp_path, tdef, "b3"))
    groups = iceberg_load._group_by_staged_bytes([ok1, gone, ok2], max_bytes=10**9)
    assert groups == [[ok1], [gone], [ok2]]       # unknown size: quarantined


# --------------------------- grouped run loops ---------------------------- #

def test_rebuild_small_table_is_single_replace_run(tmp_path, monkeypatch):
    calls = _record_runs(monkeypatch)
    tdef, results = _three_results(tmp_path)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=results, failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_rebuild(None, plan, Settings(), control)

    assert len(calls) == 1                        # ONE commit, not 3
    paths, disposition = calls[0]
    assert paths == [r.staged_path for r in results]
    assert disposition == "replace"
    assert control.advanced == results
    assert all(not r.staged_path.exists() for r in results)


def test_rebuild_over_budget_is_replace_then_appends(tmp_path, monkeypatch):
    calls = _record_runs(monkeypatch)
    tdef, results = _three_results(tmp_path)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=results, failed=[])

    iceberg_load._run_per_branch_rebuild(
        None, plan, Settings(load_group_max_bytes=1), _FakeControl())

    assert [d for _, d in calls] == ["replace", "append", "append"]
    assert [p for p, _ in calls] == [[r.staged_path] for r in results]


def test_snapshot_append_small_table_is_single_append_run(tmp_path, monkeypatch):
    calls = _record_runs(monkeypatch)
    tdef, results = _three_results(tmp_path)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=results, failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_append(None, plan, Settings(), control)

    assert len(calls) == 1
    assert calls[0][1] == "append"                # never replace: history kept
    assert control.advanced == results


def test_settings_default_group_budget():
    assert Settings().load_group_max_bytes == 256 * 1024 * 1024
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_load_grouping.py -v`
Expected: FAIL — `AttributeError: module 'etl.iceberg_load' has no attribute '_group_by_staged_bytes'` and `TypeError: Settings.__init__() got an unexpected keyword argument 'load_group_max_bytes'`.

- [ ] **Step 3: Add the setting**

In `etl/config.py`, directly below the `load_batch_rows` field (~line 322):

```python
    # Full-rebuild/append branch grouping: pack consecutive branches into one
    # dlt run while their staged parquet bytes fit this budget. One run = one
    # Iceberg commit, so a small table collapses from one commit per branch to
    # a single commit (measured: 39-54% of INITIAL load time was pure
    # per-branch commit overhead). A branch bigger than the budget still runs
    # alone -- the pre-grouping behavior. Staged bytes are compressed; the
    # loader's in-memory Arrow peak is a few times larger, so keep this well
    # under the available RAM headroom.
    load_group_max_bytes: int = 256 * 1024 * 1024
```

In `load_settings()` (~line 556), next to the `load_batch_rows` line:

```python
        load_group_max_bytes=int(_cfg("etl.load_group_max_bytes", 256 * 1024 * 1024)),
```

- [ ] **Step 4: Implement grouping in `etl/iceberg_load.py`**

Insert above `_run_per_branch_rebuild` (~line 683):

```python
def _group_by_staged_bytes(
    results: list[ExtractResult], max_bytes: int
) -> list[list[ExtractResult]]:
    """Greedy, order-preserving packing of branch results into size-budgeted groups.

    Each group's staged parquet bytes total at most ``max_bytes``, except a
    single branch that alone exceeds the budget, which gets its own group (the
    pre-grouping behavior). One group = one dlt run = one Iceberg commit, so
    small tables collapse to a single commit while the loader's in-memory peak
    (dlt materializes each load package whole) stays bounded by the budget
    times the parquet->Arrow expansion factor. A branch whose staged file
    cannot be stat'ed is treated as budget-sized so it runs alone.
    """
    groups: list[list[ExtractResult]] = []
    cur: list[ExtractResult] = []
    cur_bytes = 0
    for r in results:
        try:
            size = r.staged_path.stat().st_size
        except OSError:
            size = max_bytes  # unknown size: quarantine in its own group
        if cur and cur_bytes + size > max_bytes:
            groups.append(cur)
            cur, cur_bytes = [], 0
        cur.append(r)
        cur_bytes += size
    if cur:
        groups.append(cur)
    return groups
```

Replace the bodies of `_run_per_branch_rebuild` and `_run_per_branch_append` (keep names and signatures):

```python
def _run_per_branch_rebuild(
    pipeline,
    plan: TableLoadPlan,
    settings: Settings,
    control: ControlStore,
) -> None:
    """Load a full-rebuild (``replace``) table in size-budgeted branch groups.

    The dlt filesystem-Iceberg loader materializes a whole load package into one
    Arrow table (``arrow_dataset.to_table()``) before writing -- regardless of
    write disposition -- so each run's package must fit in memory. Branches are
    greedily packed into groups whose staged bytes fit
    ``settings.load_group_max_bytes``: a small table becomes ONE run (one
    Iceberg commit instead of one per branch), while an oversized branch still
    runs alone, keeping the peak bounded exactly as before grouping.

    The first group is written with ``replace`` (which truncates any prior
    table via pyiceberg ``overwrite``); every later group ``append``s.
    Watermarks advance per group as it commits, so a mid-stream failure still
    leaves the already-committed groups' branches (and their watermarks)
    correct; the failed and not-yet-attempted branches keep their old watermark
    and are re-pulled next run. The caller persists ``control`` and marks the
    table FAILED if this raises.
    """
    disposition = "replace"  # first group truncates the prior table
    for group in _group_by_staged_bytes(plan.success, settings.load_group_max_bytes):
        branch_ids = ",".join(str(r.branch_id) for r in group)
        _run_pipeline(
            pipeline,
            [_iceberg_resource(plan, settings, [r.staged_path for r in group],
                               disposition, write_hash=not plan.tdef.is_snapshot)],
            settings,
            f"{plan.tdef.dataset_table_name}:branches={branch_ids}:{disposition}")
        for r in group:
            control.advance(r)
            _cleanup_staged(r, settings)
        disposition = "append"  # everything after the first group adds on


def _run_per_branch_append(
    pipeline,
    plan: TableLoadPlan,
    settings: Settings,
    control: ControlStore,
) -> None:
    """Append a snapshot table in size-budgeted branch groups (memory-bounded).

    Like ``_run_per_branch_rebuild`` this packs branches into
    ``load_group_max_bytes`` groups, but *every* group appends -- including the
    first -- so snapshots stored by earlier runs are preserved (the whole point
    of a snapshot table). Watermarks advance per group as it commits so a
    mid-stream failure leaves the already-appended branches correct.
    """
    for group in _group_by_staged_bytes(plan.success, settings.load_group_max_bytes):
        branch_ids = ",".join(str(r.branch_id) for r in group)
        _run_pipeline(
            pipeline,
            [_iceberg_resource(plan, settings, [r.staged_path for r in group],
                               "append")],
            settings,
            f"{plan.tdef.dataset_table_name}:branches={branch_ids}:append")
        for r in group:
            control.advance(r)
            _cleanup_staged(r, settings)
```

Note: `_squash_table_run_snapshots` needs no change — with one commit it finds zero extra snapshots and its existing `if ids:` guard skips the expire commit.

- [ ] **Step 5: Run the new tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_load_grouping.py -v`
Expected: all PASS.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass — in particular `tests/test_cleanup_staged.py` (its single-branch rebuild/append tests now go through a 1-element group and must behave identically).

- [ ] **Step 6: Commit**

```bash
git add etl/config.py etl/iceberg_load.py tests/test_load_grouping.py
git commit -m "perf(load): group full-rebuild branches into size-budgeted single commits" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Skip no-op snapshot-retention commits; bound metadata growth

`apply_snapshot_retention` currently calls `txn.set_properties(props)` on **every table every run** — verified against pyiceberg 0.11.1: `set_properties` unconditionally applies a `SetPropertiesUpdate`, i.e. a new metadata.json commit even when nothing changed — and runs `expire_snapshots` even when no snapshot is old enough. Guard both so a steady-state run makes **zero** maintenance commits. Also add the metadata-housekeeping properties (both are supported `TableProperties` in pyiceberg 0.11.1) so metadata.json files stop accumulating forever.

**Files:**
- Modify: `etl/iceberg_load.py:872-909` (`apply_snapshot_retention`)
- Test: `tests/test_snapshot_retention_guards.py` (new)

**Interfaces:**
- Consumes: `Settings.snapshot_maintenance / snapshot_expire_days / snapshot_min_to_keep`, `get_iceberg_tables(pipeline)` (imported inside the function — tests monkeypatch `dlt.common.libs.pyiceberg.get_iceberg_tables`).
- Produces: no signature changes; two new table properties (`write.metadata.delete-after-commit.enabled`, `write.metadata.previous-versions-max`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_snapshot_retention_guards.py`:

```python
"""apply_snapshot_retention makes zero commits when there is nothing to do."""
from __future__ import annotations

import time

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from etl import iceberg_load
from etl.config import Settings


def _rows(offset: int) -> pa.Table:
    return pa.table({
        "id": pa.array([offset, offset + 1], pa.int64()),
        "name": pa.array([f"a{offset}", f"b{offset}"]),
    })


@pytest.fixture
def table(tmp_path):
    """Real Iceberg table: 2 appends + 1 overwrite -> 3 snapshots."""
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("oasis")
    tbl = catalog.create_table(
        "oasis.foo", schema=_rows(0).schema,
        location=(tmp_path / "lake" / "foo").as_uri(),
    )
    tbl.append(_rows(0))
    tbl.append(_rows(10))
    tbl.overwrite(_rows(20))
    return tbl


def _retention(monkeypatch, tbl, **settings_kw):
    import dlt.common.libs.pyiceberg as ice

    monkeypatch.setattr(ice, "get_iceberg_tables", lambda pipeline: {"foo": tbl})
    iceberg_load.apply_snapshot_retention(object(), Settings(**settings_kw))
    tbl.refresh()


def _metadata_files(tmp_path) -> int:
    """Every commit writes a new metadata.json; the count is the commit count."""
    return len(list((tmp_path / "lake" / "foo" / "metadata").glob("*.metadata.json")))


def test_first_run_sets_properties_once(tmp_path, table, monkeypatch):
    _retention(monkeypatch, table)
    assert table.properties["history.expire.max-snapshot-age-ms"] == str(
        7 * 24 * 60 * 60 * 1000)
    assert table.properties["history.expire.min-snapshots-to-keep"] == "1"
    assert table.properties["write.metadata.delete-after-commit.enabled"] == "true"
    assert table.properties["write.metadata.previous-versions-max"] == "25"


def test_steady_state_run_commits_nothing(tmp_path, table, monkeypatch):
    _retention(monkeypatch, table)               # first run: property commit
    before = _metadata_files(tmp_path)
    _retention(monkeypatch, table)               # steady state: all guards hit
    assert _metadata_files(tmp_path) == before   # ZERO new commits


def test_old_snapshots_still_expire(tmp_path, table, monkeypatch):
    assert len(table.metadata.snapshots) > 1
    time.sleep(0.05)      # make existing snapshots strictly older than cutoff=now
    _retention(monkeypatch, table, snapshot_expire_days=0)
    assert len(table.metadata.snapshots) == 1    # only the current ref survives


def test_noop_when_maintenance_disabled(tmp_path, table, monkeypatch):
    before = _metadata_files(tmp_path)
    _retention(monkeypatch, table, snapshot_maintenance=False)
    assert _metadata_files(tmp_path) == before
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_snapshot_retention_guards.py -v`
Expected: `test_first_run_sets_properties_once` FAILS with `KeyError: 'write.metadata.delete-after-commit.enabled'`; `test_steady_state_run_commits_nothing` FAILS (second run writes a new metadata.json). The other two may already pass — that's fine.

- [ ] **Step 3: Implement the guards**

In `apply_snapshot_retention` (`etl/iceberg_load.py:872`), replace the `props = {...}` dict and the per-table loop:

```python
    props = {
        "history.expire.max-snapshot-age-ms": str(max_age_ms),
        "history.expire.min-snapshots-to-keep": str(settings.snapshot_min_to_keep),
        # Every commit rewrites table metadata and keeps the previous
        # metadata.json around; without a cap the metadata dir grows forever
        # and each commit's metadata-log grows with it. Both keys are
        # honored by pyiceberg (TableProperties, 0.11.1).
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "25",
    }

    try:
        tables = get_iceberg_tables(pipeline)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not open Iceberg tables for retention: %s", exc)
        return

    for name, tbl in tables.items():
        try:
            # Property writes and no-op expiry are real catalog commits (a new
            # metadata.json per table per run) -- pyiceberg's set_properties
            # commits even when the values are unchanged. Guard both so a
            # steady-state run makes ZERO maintenance commits.
            current = tbl.properties
            if any(current.get(k) != v for k, v in props.items()):
                with tbl.transaction() as txn:
                    txn.set_properties(props)
            cutoff_ms = int(cutoff.timestamp() * 1000)
            protected = {ref.snapshot_id for ref in tbl.metadata.refs.values()}
            expirable = [
                s for s in tbl.metadata.snapshots
                if s.timestamp_ms < cutoff_ms and s.snapshot_id not in protected
            ]
            if expirable:
                tbl.maintenance.expire_snapshots().older_than(cutoff).commit()
                log.info("[%s] retention applied: keep %dd, %d snapshot(s) remain",
                         name, settings.snapshot_expire_days,
                         len(list(tbl.snapshots())))
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] snapshot retention failed: %s", name, exc)
```

(The function's opening — the `settings.snapshot_maintenance` early-return, the `pyiceberg` import guard, and the `max_age_ms` / `cutoff` computation — is unchanged.)

- [ ] **Step 4: Run the new tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_snapshot_retention_guards.py -v`
Expected: all 4 PASS.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_snapshot_retention_guards.py
git commit -m "perf(load): skip no-op retention commits; bound metadata.json growth" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Zero-copy merge-hash fast path (byte-identical digests)

`_merge_hash_array` currently goes through `_serialize_keys`: `to_pylist()` materializes every key value as a Python string, then a per-row Python loop builds framed bytearrays. Replace it with zero-copy slicing of the Arrow string buffers — the UTF-8 payload already sits contiguous in the array's data buffer, so per row we only feed `blake2b` a cached length-prefix plus a buffer slice. **Byte-identical output** (validated 2026-07-26 against the reference on ints, decimals, strings with nulls/unicode/embedded-NUL/>128-byte values, chunked, sliced, and empty inputs; 2.2× on 1M rows). `_serialize_keys` stays exactly as-is: it is the frozen reference implementation, the fallback, and is imported by existing tests.

**Files:**
- Modify: `etl/iceberg_load.py:960-1012` (extract `_reject_unstable_key_types`; add `_LEN_PREFIX`, `_key_column_view`; rewrite `_merge_hash_array`)
- Test: `tests/test_merge_hash_fastpath.py` (new); `tests/test_merge_hash.py` must keep passing unmodified

**Interfaces:**
- Consumes: `hashlib`, `struct`, `pyarrow` / `pyarrow.compute` (`pc`) — all already imported at module top.
- Produces: `_reject_unstable_key_types(table: pa.Table, key_cols: list[str]) -> None` (raises `ValueError` matching "not run-stable"); `_key_column_view(col) -> tuple[memoryview, memoryview, Optional[list]]`; `_merge_hash_array(table, key_cols) -> pa.Array` — signature and output bytes unchanged.

- [ ] **Step 1: Write the failing differential tests**

Create `tests/test_merge_hash_fastpath.py`:

```python
"""The fast merge hash must be byte-identical to the reference serializer.

_serialize_keys is the FROZEN reference: stored tables already carry its
digests, so any drift silently duplicates rows on later merges. Every case
here hashes both ways and demands equality.
"""
from __future__ import annotations

import hashlib
from decimal import Decimal

import pyarrow as pa
import pytest

from etl.iceberg_load import _merge_hash_array, _reject_unstable_key_types, _serialize_keys


def _reference_digests(table, key_cols):
    return [hashlib.blake2b(b, digest_size=16).digest()
            for b in _serialize_keys(table, key_cols)]


CASES = {
    "ints": pa.table({"id": pa.array([1, 2, 3, None], pa.int64()),
                      "branch_id": pa.array([7, 7, 8, 8], pa.int64())}),
    "decimals": pa.table({"id": pa.array([Decimal(1), None, Decimal(123456789)],
                                         pa.decimal128(38, 0)),
                          "branch_id": pa.array([1, 2, 3], pa.int64())}),
    "strings": pa.table({"id": pa.array(["", None, "abc", "münchen\U0001F600",
                                         "a\x00b", "x" * 300],  # > prefix cache
                                        pa.string()),
                         "branch_id": pa.array([1, 1, 2, 2, 3, 3], pa.int64())}),
    "single_col": pa.table({"id": pa.array(list(range(100)), pa.int64())}),
    "all_null": pa.table({"id": pa.array([None, None], pa.string()),
                          "branch_id": pa.array([1, 2], pa.int64())}),
    "empty": pa.table({"id": pa.array([], pa.int64()),
                       "branch_id": pa.array([], pa.int64())}),
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_fast_hash_matches_reference(name):
    t = CASES[name]
    got = [v.as_py() for v in _merge_hash_array(t, t.column_names)]
    assert got == _reference_digests(t, t.column_names)


def test_fast_hash_matches_reference_on_chunked_input():
    t = pa.concat_tables([
        pa.table({"id": pa.array([1, 2], pa.int64()),
                  "branch_id": pa.array([7, 7], pa.int64())}),
        pa.table({"id": pa.array([3, None], pa.int64()),
                  "branch_id": pa.array([8, 8], pa.int64())}),
    ])                                            # 2 chunks per column
    got = [v.as_py() for v in _merge_hash_array(t, t.column_names)]
    assert got == _reference_digests(t, t.column_names)


def test_fast_hash_matches_reference_on_sliced_input():
    t = pa.table({"id": pa.array(list(range(10)), pa.int64()),
                  "branch_id": pa.array([7] * 10, pa.int64())}).slice(3, 4)
    got = [v.as_py() for v in _merge_hash_array(t, t.column_names)]
    assert got == _reference_digests(t, t.column_names)


def test_reject_helper_raises_on_float_and_fractional_decimal():
    bad_float = pa.table({"id": pa.array([1.5], pa.float64())})
    bad_dec = pa.table({"id": pa.array([Decimal("1.50")], pa.decimal128(18, 2))})
    for t in (bad_float, bad_dec):
        with pytest.raises(ValueError, match="not run-stable"):
            _reject_unstable_key_types(t, ["id"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_fastpath.py -v`
Expected: FAIL — `ImportError: cannot import name '_reject_unstable_key_types'`.

- [ ] **Step 3: Implement the fast path**

In `etl/iceberg_load.py`, extract the validation loop from `_serialize_keys` (lines 977–986) into a shared helper placed directly above it, and make `_serialize_keys` call it (its docstring and the rest of its body are unchanged):

```python
def _reject_unstable_key_types(table: pa.Table, key_cols: list[str]) -> None:
    """Refuse float / fractional-decimal merge-key columns.

    Their string cast can drift across runs, so hashing them risks silent
    duplicate rows. Shared by the reference serializer and the fast hasher so
    both reject identically.
    """
    for name in key_cols:
        t = table.column(name).type
        if pa.types.is_floating(t) or (pa.types.is_decimal(t) and t.scale > 0):
            raise ValueError(
                f"merge-key column {name!r} has type {t}, which is not "
                f"run-stable: hashing a floating or fractional-decimal key is "
                f"not run-stable across runs (its string cast can vary -> "
                f"silent duplicate rows). Merge keys must be integer, "
                f"scale-0 decimal, or string."
            )
```

In `_serialize_keys`, replace the inline `for name in key_cols: ... raise ValueError(...)` block with a single call:

```python
    _reject_unstable_key_types(table, key_cols)
```

Add below `_serialize_keys`:

```python
# Cached ``b"\x00" + 4-byte big-endian length`` frames for short values --
# stringified numeric keys are almost always < 128 bytes, so the per-value
# prefix is a list lookup instead of a struct.pack call.
_LEN_PREFIX = [b"\x00" + struct.pack(">I", n) for n in range(128)]


def _key_column_view(col) -> "tuple[memoryview, memoryview, Optional[list]]":
    """Zero-copy per-row view of one stringified key column.

    Returns ``(payload, offsets, valid)``: ``payload[offsets[i]:offsets[i+1]]``
    is row i's UTF-8 bytes straight from the Arrow data buffer (no Python
    string is ever materialized), ``offsets`` is the int64 offsets buffer, and
    ``valid`` is a per-row validity list or ``None`` when no row is null.
    Raises when the cast/combine yields a non-zero array offset -- the caller
    falls back to the reference serializer.
    """
    arr = pc.cast(col, pa.large_string())
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if isinstance(arr, pa.ChunkedArray):  # older pyarrow: still chunked
        arr = arr.chunk(0) if arr.num_chunks else pa.array([], pa.large_string())
    if arr.offset != 0:
        raise ValueError("expected offset-0 array after cast/combine")
    if len(arr) == 0:
        return memoryview(b""), memoryview(struct.pack("q", 0)).cast("q"), None
    offsets = memoryview(arr.buffers()[1]).cast("q")  # int64 offsets, zero-copy
    payload_buf = arr.buffers()[2]
    payload = memoryview(payload_buf) if payload_buf is not None else memoryview(b"")
    valid = None if arr.null_count == 0 else arr.is_valid().to_pylist()
    return payload, offsets, valid


def _merge_hash_array(table: pa.Table, key_cols: list[str]) -> pa.Array:
    """128-bit blake2b of each row's canonical key serialization -> pa.binary().

    Deterministic across processes and library versions (unlike the salted
    built-in hash()). Every value is exactly 16 bytes. Fast path: instead of
    materializing every key value as a Python string (``_serialize_keys``),
    each column is cast to ``large_string`` once and rows are fed to blake2b
    as slices of the Arrow data buffer -- byte-identical framing (null flag /
    ``\\x00`` + 4-byte BE length + UTF-8), no per-row Python objects. Any
    input the fast path cannot view falls back to the reference serializer,
    which stays the canonical definition of the hash bytes.
    """
    _reject_unstable_key_types(table, key_cols)
    try:
        views = [_key_column_view(table.column(name)) for name in key_cols]
    except Exception as exc:  # noqa: BLE001 - reference path is always correct
        log.warning("fast merge-hash framing unavailable (%s); "
                    "using reference serializer", exc)
        digests = [hashlib.blake2b(b, digest_size=16).digest()
                   for b in _serialize_keys(table, key_cols)]
        return pa.array(digests, type=pa.binary())

    out = []
    for i in range(table.num_rows):
        h = hashlib.blake2b(digest_size=16)
        for payload, offsets, valid in views:
            if valid is not None and not valid[i]:
                h.update(b"\x01")
                continue
            a, b = offsets[i], offsets[i + 1]
            ln = b - a
            h.update(_LEN_PREFIX[ln] if ln < 128 else
                     b"\x00" + struct.pack(">I", ln))
            h.update(payload[a:b])
        out.append(h.digest())
    return pa.array(out, type=pa.binary())
```

- [ ] **Step 4: Run the new tests, the frozen hash suite, then everything**

Run: `.venv/Scripts/python.exe -m pytest tests/test_merge_hash_fastpath.py tests/test_merge_hash.py -v`
Expected: all PASS — including the pre-existing cross-process stability and int-vs-decimal equivalence tests, which pin the digest bytes.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_merge_hash_fastpath.py
git commit -m "perf(load): zero-copy merge-hash fast path, byte-identical digests" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Shrink carry-forward `existing` to the delta's hashes

On a hash-ready merge, `_finish_batch` joins **every** streamed 50k-row batch against the full `existing_insert_at` table — for a 20M-row stored table that hash table is rebuilt per batch. Since the delta's keys are knowable up front (staged parquet, key columns only), compute the delta's merge hashes once and filter `existing` down to only rows the delta can touch. Per-batch joins then cost O(delta) instead of O(stored table). Best-effort: on any failure the prefilter is skipped and behavior is exactly today's.

**Files:**
- Modify: `etl/iceberg_load.py` (add `_staged_delta_hashes` below `_existing_insert_at`; wire into the merge branch of `_load_one_table`, ~lines 1257–1263)
- Test: `tests/test_carry_forward_prefilter.py` (new)

**Interfaces:**
- Consumes: `_merge_hash_array(table, key_cols)` (Task 3's — but works identically against pre-Task-3 code), `types_map.cast_table_to_schema(table, schema)`, `_existing_insert_at(...)` (unchanged), `Settings.merge_hash_column` (always lowercase — matches the column name `_existing_insert_at` returns on the hash-ready path).
- Produces: `_staged_delta_hashes(paths: list, key_cols: list[str], unified_schema: pa.Schema) -> Optional[pa.Array]` — distinct 16-byte binary hashes, or `None` when unavailable.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_carry_forward_prefilter.py`:

```python
"""Carry-forward prefilter: `existing` shrinks to the rows the delta can touch."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef
from etl.iceberg_load import _merge_hash_array, _staged_delta_hashes
from etl.oracle_extract import ExtractResult
from etl.progress import PipelineMonitor


def _tdef():
    return TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _write_staged(dir_, name, ids, branches):
    p = dir_ / name
    pq.write_table(pa.table({"ID": pa.array(ids, pa.int64()),
                             "BRANCH_ID": pa.array(branches, pa.int64()),
                             "VAL": pa.array([f"v{i}" for i in ids])}), p)
    return p


def _unified_schema():
    return pa.schema([pa.field("ID", pa.int64()),
                      pa.field("BRANCH_ID", pa.int64()),
                      pa.field("VAL", pa.string())])


def _hashes_of(ids, branches):
    keys = pa.table({"ID": pa.array(ids, pa.int64()),
                     "BRANCH_ID": pa.array(branches, pa.int64())})
    return _merge_hash_array(keys, ["ID", "BRANCH_ID"])


def test_delta_hashes_match_finish_batch_hashes(tmp_path):
    p = _write_staged(tmp_path, "b1.parquet", [1, 2, 2], [7, 7, 7])
    got = _staged_delta_hashes([p], ["ID", "BRANCH_ID"], _unified_schema())
    want = {v.as_py() for v in _hashes_of([1, 2], [7, 7])}
    assert {v.as_py() for v in got} == want       # deduped, digest-identical


def test_delta_hashes_none_on_unreadable_file(tmp_path):
    got = _staged_delta_hashes([tmp_path / "gone.parquet"],
                               ["ID", "BRANCH_ID"], _unified_schema())
    assert got is None                            # best-effort: no prefilter


def test_merge_shrinks_existing_to_delta_rows(tmp_path, monkeypatch):
    tdef = _tdef()
    staged_dir = tmp_path / tdef.dataset_table_name
    staged_dir.mkdir(parents=True)
    p = _write_staged(staged_dir, "b1.parquet", [1, 2], [7, 7])
    result = ExtractResult(table_def=tdef, branch="b1", branch_id=7,
                           status="SUCCESS", row_count=2, staged_path=p)

    # Stored carry-forward rows for ids 1..4; only 1 and 2 are in the delta.
    existing = pa.table({
        "merge_hash": _hashes_of([1, 2, 3, 4], [7, 7, 7, 7]),
        "insert_at": pa.array([None] * 4, pa.timestamp("us")),
    })

    captured = {}

    def fake_resource(plan, s, paths, disposition, existing_insert_at=None,
                      write_hash=False, carry_keys=None):
        captured["existing"] = existing_insert_at
        return None

    monkeypatch.setattr(iceberg_load, "_coerce_unified_nulls", lambda p_, t, s: s)
    monkeypatch.setattr(iceberg_load, "_widen_schema_to_destination",
                        lambda p_, t, s: s)
    monkeypatch.setattr(iceberg_load, "_table_is_hash_ready", lambda *a, **k: True)
    monkeypatch.setattr(iceberg_load, "_existing_insert_at",
                        lambda *a, **k: existing)
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", fake_resource)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)

    class _FakeControl:
        def advance(self, r):
            pass

        def save(self):
            pass

    monitor = PipelineMonitor(total_units=1, total_tables=1, enabled=False)
    # total_branches=2, branches_in_run=1 -> branch-subset -> merge disposition.
    plan = iceberg_load._load_one_table(
        None, tdef, [result],
        Settings(mode=MODE_INCREMENTAL, snapshot_maintenance=False),
        _FakeControl(), 2, 1, monitor)

    assert plan.disposition == "merge"
    assert plan.load_status == "SUCCESS"
    kept = captured["existing"]
    assert kept.num_rows == 2                     # ids 3,4 filtered out
    want = {v.as_py() for v in _hashes_of([1, 2], [7, 7])}
    assert {v.as_py() for v in kept.column("merge_hash")} == want
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_carry_forward_prefilter.py -v`
Expected: FAIL — `ImportError: cannot import name '_staged_delta_hashes'`.

- [ ] **Step 3: Implement the prefilter**

In `etl/iceberg_load.py`, add below `_existing_insert_at`:

```python
def _staged_delta_hashes(
    paths: list, key_cols: list[str], unified_schema: pa.Schema
) -> Optional[pa.Array]:
    """Distinct merge-hash values present in a merge delta's staged parquets.

    Reads ONLY the key columns (columnar), casts them to the unified schema's
    types -- the same cast ``_finish_batch`` applies before hashing, so the
    digests are byte-identical to the ones the batches will carry -- and
    hashes. Used to pre-shrink the carry-forward ``existing`` table to just
    the rows the delta can actually touch: ``_finish_batch`` joins ``existing``
    once per streamed batch, so an unfiltered multi-million-row table would be
    re-hashed for the join on every 50k-row batch. Best-effort: returns
    ``None`` (no prefilter, today's behavior) on any failure.
    """
    try:
        key_schema = pa.schema([unified_schema.field(c) for c in key_cols])
        chunks = []
        for path in paths:
            t = pq.read_table(path, columns=key_cols)
            t = types_map.cast_table_to_schema(t, key_schema)
            chunks.append(_merge_hash_array(t, key_cols))
        if not chunks:
            return None
        return pc.unique(pa.chunked_array(chunks, type=pa.binary()))
    except Exception as exc:  # noqa: BLE001 - prefilter is an optimization only
        log.warning("delta hash prefilter unavailable: %s", exc)
        return None
```

In `_load_one_table`'s merge branch, directly after the `existing = _existing_insert_at(...)` call (~line 1258), insert:

```python
            if hash_ready and existing is not None:
                # Shrink carry-forward to the rows the delta can touch: the
                # batch loop joins `existing` once per streamed batch, so an
                # unfiltered multi-million-row table would be re-hashed for
                # the join on every 50k-row batch.
                delta_hashes = _staged_delta_hashes(
                    [r.staged_path for r in plan.success],
                    list(tdef.key_columns) + [settings.branch_id_column],
                    plan.unified_schema)
                if delta_hashes is not None:
                    existing = existing.filter(pc.is_in(
                        existing.column(settings.merge_hash_column),
                        value_set=delta_hashes))
                    if existing.num_rows == 0:
                        existing = None   # nothing to carry: skip joins entirely
```

Note the key-column order `list(tdef.key_columns) + [settings.branch_id_column]` matches `hash_key_cols` in `_iceberg_resource` exactly — framing order is part of the hash bytes.

- [ ] **Step 4: Run the new tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_carry_forward_prefilter.py -v`
Expected: all 3 PASS.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass — in particular `tests/test_cleanup_staged.py::test_load_one_table_merge_deletes_staged` (non-hash-ready merge: the new block must not fire when `hash_ready` is False).

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_carry_forward_prefilter.py
git commit -m "perf(load): prefilter carry-forward rows to the merge delta's hashes" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Validation after merge (manual, on the server)

Not part of the automated tasks — record results in the run log / memory:

1. Deploy to `/home/bi/workspace/dlt` (copy branch, `pip install` not needed — no new deps) and run one INITIAL with `--progress` on.
2. Compare against the 2026-07-20 baseline: expect small tables (<100k rows) to drop from ~8 commits to 1 (est. −12–14 min per INITIAL run) and zero `set_properties`/expire commits on steady-state tables.
3. While there, run the still-pending hash-merge validation: `diagnostics/merge_profile.py <table> --hash-key` on a hash-ready table.
4. If RSS peaks rise (groups materialize more than one branch), lower `etl.load_group_max_bytes` in `.dlt/config.toml` — no code change needed.
