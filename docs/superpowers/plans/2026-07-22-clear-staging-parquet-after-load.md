# Clear Staged Parquet After Load — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After each branch's rows are durably committed to Iceberg, delete that branch's staged parquet (`_staging/<table>/<branch>.parquet`) to reclaim local disk — gated by a new setting (default on) with a `--keep-staging` opt-out.

**Architecture:** A new best-effort helper `_cleanup_staged(result, settings)` in `etl/iceberg_load.py` deletes a branch's staged parquet. It is called at the per-branch commit point — immediately after `control.advance(r)` — in the four load-success paths. A new `Settings.cleanup_staging_after_load` field (default `True`) gates it; `oracle_to_iceberg.py --keep-staging` disables it for a run so `dq_check --self-test` can still reconcile against the staged files.

**Tech Stack:** Python 3.13, pytest, dlt/pyiceberg (not exercised by these tests), pyarrow/parquet.

## Global Constraints

- Cleanup is **best-effort**: a failed delete is logged at WARNING and never fails the load (mirrors the existing `_cleanup_tmp` tolerance).
- Deletion is tied to `control.advance(r)` (the durable-commit point), **not** to whole-table success — earlier committed branches in a table that later partially fails are still cleaned.
- The setting defaults to **on**. The escape hatch is the CLI flag only; no GUI change.
- `result.staged_path` is already a `pathlib.Path` — call methods on it directly; do **not** add a `Path` import to `etl/iceberg_load.py`.
- `_cleanup_staged` is duck-typed on `result.staged_path` (a `Path` or `None`) and `result.table` (a `str`, used only in the error log line).

---

### Task 1: Config setting `cleanup_staging_after_load`

**Files:**
- Modify: `etl/config.py` (the `Settings` dataclass "local working state" block; `load_settings`'s `Settings(...)` constructor)
- Test: `tests/test_staging_cleanup_settings.py` (create)

**Interfaces:**
- Produces: `Settings.cleanup_staging_after_load: bool` (default `True`); `load_settings` reads it from `[etl] cleanup_staging_after_load` and honors it as an override key.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_staging_cleanup_settings.py`:

```python
"""Config + CLI plumbing for the staged-parquet cleanup setting."""
from __future__ import annotations

from etl import config
from etl.config import Settings


def test_settings_defaults_cleanup_on():
    assert Settings().cleanup_staging_after_load is True


def test_load_settings_override_cleanup():
    s = config.load_settings({"cleanup_staging_after_load": False})
    assert s.cleanup_staging_after_load is False


def test_load_settings_reads_etl_key(monkeypatch):
    orig = config._cfg
    monkeypatch.setattr(
        config, "_cfg",
        lambda key, default: False if key == "etl.cleanup_staging_after_load" else orig(key, default),
    )
    s = config.load_settings()
    assert s.cleanup_staging_after_load is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_staging_cleanup_settings.py -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'cleanup_staging_after_load'` (and the override test raises `AttributeError: Unknown setting override`).

- [ ] **Step 3: Add the `Settings` field**

In `etl/config.py`, in the `Settings` dataclass, find the "local working state" block:

```python
    # local working state
    staging_dir: Path = field(default_factory=lambda: Path("_staging"))

    self_test: bool = False
```

Insert the new field between `staging_dir` and `self_test`:

```python
    # local working state
    staging_dir: Path = field(default_factory=lambda: Path("_staging"))

    # Delete a branch's staged parquet once its rows are committed to Iceberg,
    # to reclaim local disk. Turn off (--keep-staging) to retain the files for
    # an offline `dq_check --self-test` reconciliation afterward.
    cleanup_staging_after_load: bool = True

    self_test: bool = False
```

- [ ] **Step 4: Read it in `load_settings`**

In `etl/config.py`, in `load_settings`, find the last kwarg of the `Settings(...)` constructor:

```python
        dq_hash_delta_tolerance_pct=float(_cfg("etl.dq_hash_delta_tolerance_pct", 10.0)),
    )
```

Add the new read before the closing `)`:

```python
        dq_hash_delta_tolerance_pct=float(_cfg("etl.dq_hash_delta_tolerance_pct", 10.0)),
        cleanup_staging_after_load=bool(_cfg("etl.cleanup_staging_after_load", True)),
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_staging_cleanup_settings.py -v`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
git add etl/config.py tests/test_staging_cleanup_settings.py
git commit -m "feat(etl): add cleanup_staging_after_load setting (default on)"
```

---

### Task 2: CLI flag `--keep-staging`

**Files:**
- Modify: `oracle_to_iceberg.py` (`parse_args` and `build_overrides`)
- Test: `tests/test_staging_cleanup_settings.py` (append)

**Interfaces:**
- Consumes: `Settings.cleanup_staging_after_load` (Task 1).
- Produces: `oracle_to_iceberg.parse_args` accepts `--keep-staging` (dest `keep_staging`, default `False`); `build_overrides` emits `{"cleanup_staging_after_load": False}` **only** when the flag is set (absent otherwise, so the config default wins).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_staging_cleanup_settings.py`:

```python
import oracle_to_iceberg as o2i


def test_keep_staging_flag_disables_cleanup():
    args = o2i.parse_args(["--keep-staging"])
    assert o2i.build_overrides(args)["cleanup_staging_after_load"] is False


def test_no_keep_staging_flag_leaves_default():
    args = o2i.parse_args([])
    # Absent from overrides -> the config default (on) is used.
    assert "cleanup_staging_after_load" not in o2i.build_overrides(args)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_staging_cleanup_settings.py -k keep_staging -v`
Expected: FAIL — `AttributeError: 'Namespace' object has no attribute 'keep_staging'`.

- [ ] **Step 3: Add the CLI argument**

In `oracle_to_iceberg.py`, in `parse_args`, find the `--staging-dir` argument:

```python
    p.add_argument("--staging-dir", help="local staging dir for extracted parquet")
```

Add the new flag right after it:

```python
    p.add_argument("--staging-dir", help="local staging dir for extracted parquet")
    p.add_argument("--keep-staging", action="store_true",
                   help="keep staged parquet after load (default: delete it to "
                        "reclaim disk; keep it to run dq_check --self-test)")
```

- [ ] **Step 4: Translate it in `build_overrides`**

In `oracle_to_iceberg.py`, in `build_overrides`, find the tail of the function:

```python
    if args.staging_dir:
        overrides["staging_dir"] = args.staging_dir
    return overrides
```

Insert the keep-staging translation before `return`:

```python
    if args.staging_dir:
        overrides["staging_dir"] = args.staging_dir
    if args.keep_staging:
        overrides["cleanup_staging_after_load"] = False
    return overrides
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_staging_cleanup_settings.py -v`
Expected: PASS (5 passed).

- [ ] **Step 6: Commit**

```bash
git add oracle_to_iceberg.py tests/test_staging_cleanup_settings.py
git commit -m "feat(etl): --keep-staging flag to retain staged parquet for a run"
```

---

### Task 3: `_cleanup_staged` helper

**Files:**
- Modify: `etl/iceberg_load.py` (add helper after `_iceberg_resource`, before `_run_per_branch_rebuild`)
- Test: `tests/test_cleanup_staged.py` (create)

**Interfaces:**
- Consumes: `Settings.cleanup_staging_after_load` (Task 1).
- Produces: `iceberg_load._cleanup_staged(result, settings) -> None` — deletes `result.staged_path` (best-effort) and removes the now-empty parent dir; no-op when cleanup is disabled or `staged_path is None`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_cleanup_staged.py`:

```python
"""Best-effort deletion of a branch's staged parquet after it is committed."""
from __future__ import annotations

from types import SimpleNamespace

from etl import iceberg_load
from etl.config import Settings


def _result(path):
    # _cleanup_staged is duck-typed on .staged_path and .table.
    return SimpleNamespace(staged_path=path, table="FOO")


def _staged(dir_, branch="b1"):
    tbl_dir = dir_ / "FOO"
    tbl_dir.mkdir(parents=True, exist_ok=True)
    p = tbl_dir / f"{branch}.parquet"
    p.write_bytes(b"parquet")
    return p


def test_deletes_file_when_enabled(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings())
    assert not p.exists()


def test_removes_empty_table_dir(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings())
    assert not p.parent.exists()


def test_keeps_dir_with_other_branch(tmp_path):
    p1 = _staged(tmp_path, "b1")
    p2 = _staged(tmp_path, "b2")
    iceberg_load._cleanup_staged(_result(p1), Settings())
    assert not p1.exists()
    assert p2.exists()            # sibling untouched
    assert p2.parent.exists()     # dir kept — still has b2


def test_noop_when_disabled(tmp_path):
    p = _staged(tmp_path)
    iceberg_load._cleanup_staged(_result(p), Settings(cleanup_staging_after_load=False))
    assert p.exists()


def test_tolerates_missing_file(tmp_path):
    p = tmp_path / "FOO" / "gone.parquet"   # never created
    iceberg_load._cleanup_staged(_result(p), Settings())  # must not raise


def test_noop_when_path_none():
    iceberg_load._cleanup_staged(_result(None), Settings())  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_cleanup_staged.py -v`
Expected: FAIL — `AttributeError: module 'etl.iceberg_load' has no attribute '_cleanup_staged'`.

- [ ] **Step 3: Add the helper**

In `etl/iceberg_load.py`, find the end of `_iceberg_resource` (it ends with `return _resource()`), immediately followed by:

```python
def _run_per_branch_rebuild(
```

Insert the helper between them:

```python
def _cleanup_staged(result: ExtractResult, settings: Settings) -> None:
    """Delete a branch's staged parquet once its rows are durably in Iceberg.

    The staged parquet exists only to feed the load; after the branch's
    watermark advances it is dead weight, so we reclaim the disk. Best-effort:
    a failed unlink is logged and never fails the load (mirrors _cleanup_tmp).
    No-op when cleanup is disabled -- e.g. to run ``dq_check --self-test``
    against the staged files afterward.
    """
    if not settings.cleanup_staging_after_load:
        return
    path = result.staged_path
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
        # Drop the now-empty table dir when this was the last branch; an OSError
        # just means other branches' files remain (or it's already gone) -- leave it.
        try:
            path.parent.rmdir()
        except OSError:
            pass
    except OSError as exc:
        log.warning("[%s] could not delete staged parquet %s: %s",
                    result.table, path, exc)


```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_cleanup_staged.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_cleanup_staged.py
git commit -m "feat(etl): _cleanup_staged helper to delete a committed branch's parquet"
```

---

### Task 4: Wire cleanup into the four load-success paths

**Files:**
- Modify: `etl/iceberg_load.py` (`_run_per_branch_rebuild`, `_run_per_branch_append`, and two loops in `_load_one_table`)
- Test: `tests/test_cleanup_staged.py` (append)

**Interfaces:**
- Consumes: `iceberg_load._cleanup_staged` (Task 3); `iceberg_load.TableLoadPlan(tdef, success, failed)`; `ExtractResult`; `Settings`.
- Produces: staged parquet for each `r in plan.success` is deleted right after `control.advance(r)` in every success path.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cleanup_staged.py`:

```python
import pyarrow as pa
import pyarrow.parquet as pq

from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, TableDef
from etl.oracle_extract import ExtractResult
from etl.progress import PipelineMonitor


class _FakeControl:
    """Records advance() calls; save() is a no-op (no Postgres needed)."""
    def __init__(self):
        self.advanced = []

    def advance(self, r):
        self.advanced.append(r)

    def save(self):
        pass


def _merge_tdef():
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


def test_per_branch_rebuild_deletes_staged(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _result(tdef, "b1", 1, staged)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=[result], failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_rebuild(None, plan, Settings(), control)

    assert control.advanced == [result]
    assert not staged.exists()


def test_per_branch_rebuild_keeps_staged_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _result(tdef, "b1", 1, staged)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=[result], failed=[])

    iceberg_load._run_per_branch_rebuild(
        None, plan, Settings(cleanup_staging_after_load=False), _FakeControl())

    assert staged.exists()   # retained for dq_check --self-test


def test_per_branch_append_deletes_staged(tmp_path, monkeypatch):
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _result(tdef, "b1", 1, staged)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=[result], failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_append(None, plan, Settings(), control)

    assert control.advanced == [result]
    assert not staged.exists()


def test_load_one_table_zero_row_deletes_staged(tmp_path):
    # 0-row load: early-return SUCCESS path, no dlt run, no pipeline touched.
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1", rows=0)
    result = _result(tdef, "b1", 1, staged, rows=0)
    monitor = PipelineMonitor(total_units=1, total_tables=1, enabled=False)

    plan = iceberg_load._load_one_table(
        None, tdef, [result], Settings(mode=MODE_INCREMENTAL),
        _FakeControl(), 1, 1, monitor)

    assert plan.load_status == "SUCCESS"
    assert not staged.exists()


def test_load_one_table_merge_deletes_staged(tmp_path, monkeypatch):
    # Merge branch: stub the dlt run + destination reads so no real commit
    # happens, then assert the advance-loop cleanup deleted the parquet.
    monkeypatch.setattr(iceberg_load, "_coerce_unified_nulls", lambda p, t, s: s)
    monkeypatch.setattr(iceberg_load, "_widen_schema_to_destination", lambda p, t, s: s)
    monkeypatch.setattr(iceberg_load, "_table_is_hash_ready", lambda *a, **k: False)
    monkeypatch.setattr(iceberg_load, "_existing_insert_at", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_iceberg_resource", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    tdef = _merge_tdef()
    staged = _staged_parquet(tmp_path, tdef, "b1")
    result = _result(tdef, "b1", 1, staged)
    monitor = PipelineMonitor(total_units=1, total_tables=1, enabled=False)

    # total_branches=2, branches_in_run=1 -> branch-subset INCREMENTAL -> merge.
    plan = iceberg_load._load_one_table(
        None, tdef, [result],
        Settings(mode=MODE_INCREMENTAL, snapshot_maintenance=False),
        _FakeControl(), 2, 1, monitor)

    assert plan.disposition == "merge"
    assert plan.load_status == "SUCCESS"
    assert not staged.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_cleanup_staged.py -k "rebuild or append or load_one_table" -v`
Expected: FAIL — the `deletes_staged` / `SUCCESS` assertions fail because the staged files still exist (cleanup not wired in yet).

- [ ] **Step 3: Wire site 1 — `_run_per_branch_rebuild`**

In `etl/iceberg_load.py`, in `_run_per_branch_rebuild`, find the loop body:

```python
        control.advance(r)
        disposition = "append"  # everything after the first adds on
```

Add the cleanup call after `control.advance(r)`:

```python
        control.advance(r)
        _cleanup_staged(r, settings)
        disposition = "append"  # everything after the first adds on
```

- [ ] **Step 4: Wire site 2 — `_run_per_branch_append`**

In `etl/iceberg_load.py`, in `_run_per_branch_append`, find the loop:

```python
    for r in plan.success:
        _run_pipeline(
            pipeline, [_iceberg_resource(plan, settings, [r.staged_path], "append")],
            settings, f"{plan.tdef.dataset_table_name}:branch={r.branch_id}:append")
        control.advance(r)
```

Add the cleanup call after `control.advance(r)`:

```python
    for r in plan.success:
        _run_pipeline(
            pipeline, [_iceberg_resource(plan, settings, [r.staged_path], "append")],
            settings, f"{plan.tdef.dataset_table_name}:branch={r.branch_id}:append")
        control.advance(r)
        _cleanup_staged(r, settings)
```

- [ ] **Step 5: Wire site 3 — merge loop in `_load_one_table`**

In `etl/iceberg_load.py`, in `_load_one_table`'s merge branch, find:

```python
            # Advance watermarks only for a table that actually loaded.
            for r in plan.success:
                control.advance(r)
```

Add the cleanup call inside the loop:

```python
            # Advance watermarks only for a table that actually loaded.
            for r in plan.success:
                control.advance(r)
                _cleanup_staged(r, settings)
```

- [ ] **Step 6: Wire site 4 — 0-row early return in `_load_one_table`**

In `etl/iceberg_load.py`, in `_load_one_table`'s 0-row early-return block, find:

```python
    if sum(r.row_count for r in plan.success) == 0:
        for r in plan.success:
            control.advance(r)
        control.save()
```

Add the cleanup call inside the loop:

```python
    if sum(r.row_count for r in plan.success) == 0:
        for r in plan.success:
            control.advance(r)
            _cleanup_staged(r, settings)
        control.save()
```

- [ ] **Step 7: Run the wiring tests to verify they pass**

Run: `python -m pytest tests/test_cleanup_staged.py -v`
Expected: PASS (all tests, including the 6 helper tests from Task 3).

- [ ] **Step 8: Run the full cleanup + settings suites**

Run: `python -m pytest tests/test_cleanup_staged.py tests/test_staging_cleanup_settings.py -v`
Expected: PASS (all).

- [ ] **Step 9: Commit**

```bash
git add etl/iceberg_load.py tests/test_cleanup_staged.py
git commit -m "feat(etl): delete staged parquet after each branch commits"
```

---

### Task 5: Regression check + docs pointer

**Files:**
- Modify: `README.md` (document the setting/flag, if a relevant settings/flags section exists)

- [ ] **Step 1: Run the load-related test suite for regressions**

Run: `python -m pytest tests/test_load_timeout.py tests/test_pending_packages.py tests/test_coerce_nulls_isolation.py -v`
Expected: PASS (no regressions from the `_load_one_table` / per-branch edits). Tests that require `OASIS_TEST_PG_DSN` will `SKIP` — that is expected, not a failure.

- [ ] **Step 2: Document the setting**

In `README.md`, find where `[etl]` settings and/or `oracle_to_iceberg.py` flags are documented (search for `staging` or `--staging-dir`). Add a short entry, e.g.:

```markdown
- `cleanup_staging_after_load` (default `true`) — delete each branch's
  `_staging/<table>/<branch>.parquet` after it is committed to Iceberg, to
  reclaim local disk. Pass `--keep-staging` to retain the files for an
  offline `dq_check --self-test` run.
```

If no such section exists, skip this step (do not invent a new doc structure).

- [ ] **Step 3: Commit (only if README changed)**

```bash
git add README.md
git commit -m "docs: document cleanup_staging_after_load setting and --keep-staging"
```

---

## Self-Review

**Spec coverage:**
- §3.1 config setting → Task 1. ✔
- §3.2 CLI `--keep-staging` → Task 2. ✔
- §3.3 `_cleanup_staged` helper → Task 3. ✔
- §3.3 four call sites (rebuild/append/merge/0-row) → Task 4 (sites 1–4). ✔
- §4 edge cases (failure logged, 0-row, `--keep-staging` retains, branch-subset keeps dir) → Task 3 (`test_tolerates_missing_file`, `test_keeps_dir_with_other_branch`, `test_noop_when_disabled`) + Task 4 (`zero_row`, `keeps_staged_when_disabled`). ✔
- §5 testing (helper unit tests + behavioral default-on/off) → Tasks 3 & 4. ✔
- §6 files touched (config.py, oracle_to_iceberg.py, iceberg_load.py, tests) → Tasks 1–4. ✔

**Placeholder scan:** No TBD/TODO; every code step shows full code. ✔

**Type consistency:** `_cleanup_staged(result, settings)` signature is identical across the helper definition (Task 3) and all four call sites (Task 4). `TableLoadPlan(tdef=, success=, failed=)`, `ExtractResult(table_def=, branch=, branch_id=, status=, row_count=, staged_path=)`, and `_merge_tdef()` match the constructors used in `tests/test_load_timeout.py`. `Settings(cleanup_staging_after_load=...)` matches the field added in Task 1. ✔
