# DQ tolerance + per-command-type run/log observability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a configurable DQ hash-delta tolerance (default 10%), give `dq_check` live progress in the Run panel, and render a per-command-type summary for every log file on the Logs page.

**Architecture:** Backend changes live in `etl/dq_check.py` + `etl/config.py` (a pure `classify_status` function for the tolerance decision, and a small `_DqProgress` heartbeat emitter). Frontend changes turn the single hardwired dashboard parser (`gui/static/runparse.js` + `gui/templates/_dash.html`) into a **log-type dispatcher** with pluggable per-type views (pipeline / dq / generic), shared by the Run page (live tail) and Logs page (whole file).

**Tech Stack:** Python 3.13, PyArrow, dlt, Flask; vanilla browser JS + Jinja templates; pytest for backend tests.

## Global Constraints

- **OS:** Windows. Shell is Git Bash (POSIX) for the Bash tool.
- **Python interpreter for tests:** `.venv/Scripts/python.exe` (the repo venv; system `python` lacks pytest).
- **Test command:** `.venv/Scripts/python.exe -m pytest <path> -v` (config: `pytest.ini`, `testpaths = tests`, `--basetemp=.pytest_tmp`). `tests/conftest.py` already puts `gui/`, `orchestrator/src/`, and repo root on `sys.path`.
- **No JS test harness** (Node is not installed). JS changes are verified manually against sample log fixtures; the log-line *formats* are pinned by Python tests so the JS regexes have a fixed contract.
- **New DQ status string:** `WITHIN_TOLERANCE` (exact spelling — the GUI lowercases it to the CSS class `within_tolerance`).
- **Tolerance semantics:** percent of **Oracle hashed rows**; row-count delta stays a hard `MISMATCH` (zero tolerance). Default `10.0`.
- **Exit codes unchanged:** `WITHIN_TOLERANCE` and `MISMATCH` exit 0; only `ERROR` exits non-zero.
- DRY, YAGNI, TDD, frequent commits. Match existing file style (module docstrings, comment density).

## File Structure

- `etl/config.py` — MODIFY: add `Settings.dq_hash_delta_tolerance_pct`; load it in `load_settings`.
- `.dlt/config.toml` — MODIFY: add the `[etl]` key (with comment).
- `etl/dq_check.py` — MODIFY: status constants, `_hash_delta_pct`, `classify_status`, `DqResult.hash_delta_pct`, wire into `check_unit`, `render_summary` column+tally, `_result_rows`/`_DQ_HINTS` column, `_DqProgress` + `run_dq` wiring, `_fmt_elapsed`.
- `dq_check.py` — MODIFY: `--no-progress` flag; `if v is not None` override filter.
- `gui/workspace.py` — MODIFY: add key to `EDITABLE_ETL_KEYS`.
- `gui/templates/run.html` — MODIFY: settings panel key lists; optional dq `--no-progress` checkbox.
- `gui/templates/logs.html` — MODIFY: `hash_delta_pct` numCol; summary-first collapse in `openFile`.
- `gui/static/runparse.js` — REWRITE: dispatcher + `makePipelineView` (ported) + `makeDqView` + `makeGenericView`.
- `gui/templates/_dash.html` — REWRITE: split into `#rd-pipeline` / `#rd-dq` / `#rd-generic`.
- `gui/static/style.css` — MODIFY: `.pill.within_tolerance` + a few `rd-*` helper classes.
- `tests/test_dq_tolerance.py` — CREATE.
- `tests/test_dq_progress.py` — CREATE.
- `tests/test_dq_tolerance_settings.py` — CREATE (config + workspace).

---

## Task 1: DQ tolerance setting (config plumbing)

**Files:**
- Modify: `etl/config.py` (Settings dataclass + `load_settings`)
- Modify: `.dlt/config.toml` (`[etl]` block)
- Test: `tests/test_dq_tolerance_settings.py`

**Interfaces:**
- Produces: `Settings.dq_hash_delta_tolerance_pct: float` (default `10.0`), read from `etl.dq_hash_delta_tolerance_pct`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_dq_tolerance_settings.py`:

```python
"""Config + GUI plumbing for the DQ hash-delta tolerance setting."""
from __future__ import annotations

from etl import config
from etl.config import Settings


def test_settings_has_default_tolerance():
    assert Settings().dq_hash_delta_tolerance_pct == 10.0


def test_load_settings_override_tolerance():
    s = config.load_settings({"dq_hash_delta_tolerance_pct": 5.0})
    assert s.dq_hash_delta_tolerance_pct == 5.0


def test_load_settings_reads_etl_key(monkeypatch):
    orig = config._cfg
    monkeypatch.setattr(
        config, "_cfg",
        lambda key, default: 7.5 if key == "etl.dq_hash_delta_tolerance_pct" else orig(key, default),
    )
    s = config.load_settings()
    assert s.dq_hash_delta_tolerance_pct == 7.5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance_settings.py -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'dq_hash_delta_tolerance_pct'`.

- [ ] **Step 3: Add the Settings field**

In `etl/config.py`, inside the `Settings` dataclass, add after the `load_batch_rows` field (around line 286):

```python
    # DQ: tolerate row-hash drift up to this percent of a (table, branch)'s
    # Oracle hashed rows before flagging MISMATCH; at or below it the status is
    # WITHIN_TOLERANCE. Row-count drift is always a hard MISMATCH.
    dq_hash_delta_tolerance_pct: float = 10.0
```

- [ ] **Step 4: Load it in `load_settings`**

In `etl/config.py` `load_settings`, add to the `Settings(...)` constructor call (after `load_batch_rows=...`, around line 483):

```python
        dq_hash_delta_tolerance_pct=float(_cfg("etl.dq_hash_delta_tolerance_pct", 10.0)),
```

- [ ] **Step 5: Add the key to `.dlt/config.toml`**

In `.dlt/config.toml`, inside the `[etl]` block (after the `dsn_mode` line, around line 69), add:

```toml

# DQ hash-delta tolerance: a (table, branch) whose row-hash drift is within this
# percent of its Oracle hashed rows is reported WITHIN_TOLERANCE instead of
# MISMATCH. Row-count drift is always a hard MISMATCH. (0 = strict / no tolerance.)
dq_hash_delta_tolerance_pct = 10.0
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance_settings.py -v`
Expected: PASS (3 passed).

- [ ] **Step 7: Commit**

```bash
git add etl/config.py .dlt/config.toml tests/test_dq_tolerance_settings.py
git commit -m "feat(dq): add dq_hash_delta_tolerance_pct setting (default 10%)"
```

---

## Task 2: Tolerance status classification

**Files:**
- Modify: `etl/dq_check.py` (status constants, `_hash_delta_pct`, `classify_status`, `DqResult.hash_delta_pct`, `check_unit`)
- Test: `tests/test_dq_tolerance.py`

**Interfaces:**
- Consumes: `HashDelta` (existing dataclass; `.total_delta`, `.oracle_rows`, `.matched`).
- Produces:
  - `STATUS_OK = "OK"`, `STATUS_WITHIN_TOLERANCE = "WITHIN_TOLERANCE"`, `STATUS_MISMATCH = "MISMATCH"`.
  - `classify_status(row_count_delta: Optional[int], hash: Optional[HashDelta], tolerance_pct: float) -> tuple[str, Optional[float]]` returning `(status, hash_delta_pct)`.
  - `DqResult.hash_delta_pct: Optional[float]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_dq_tolerance.py`:

```python
"""DQ hash-delta tolerance: status classification + reporting surfaces."""
from __future__ import annotations

from etl import dq_check
from etl.dq_check import HashDelta, DqResult, classify_status


def _hash(matched=0, oo=0, oi=0, mm=0, ora=0, ice=0):
    return HashDelta(matched=matched, only_in_oracle=oo, only_in_iceberg=oi,
                     mismatch=mm, oracle_rows=ora, iceberg_rows=ice)


def test_zero_delta_is_ok():
    assert classify_status(0, _hash(matched=100, ora=100, ice=100), 10.0) == ("OK", 0.0)


def test_within_tolerance():
    status, pct = classify_status(0, _hash(matched=992, oo=8, ora=1000, ice=1000), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert round(pct, 4) == 0.8


def test_boundary_exactly_at_tolerance_is_within():
    # delta 100 / 1000 = 10.0% == tolerance -> WITHIN_TOLERANCE (<=)
    status, pct = classify_status(0, _hash(matched=900, oo=100, ora=1000, ice=1000), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert round(pct, 2) == 10.0


def test_over_tolerance_is_mismatch():
    status, pct = classify_status(0, _hash(matched=850, oo=150, ora=1000, ice=1000), 10.0)
    assert status == "MISMATCH"
    assert round(pct, 2) == 15.0


def test_row_count_delta_is_hard_mismatch():
    # hash is clean but the row-count delta is nonzero -> MISMATCH regardless
    status, pct = classify_status(5, _hash(matched=1000, ora=1000, ice=1000), 10.0)
    assert status == "MISMATCH"
    assert pct == 0.0


def test_zero_oracle_rows_with_delta_is_mismatch():
    status, pct = classify_status(0, _hash(oi=50, ora=0, ice=50), 10.0)
    assert status == "MISMATCH"
    assert pct is None


def test_no_hash_is_ok_when_count_clean():
    assert classify_status(0, None, 10.0) == ("OK", None)
    assert classify_status(None, None, 10.0) == ("OK", None)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance.py -v`
Expected: FAIL — `ImportError: cannot import name 'classify_status'`.

- [ ] **Step 3: Add constants + classification, extend `DqResult`**

In `etl/dq_check.py`, add module-level constants near the top (after `_TABLE_NAME`, around line 78):

```python
STATUS_OK = "OK"
STATUS_WITHIN_TOLERANCE = "WITHIN_TOLERANCE"
STATUS_MISMATCH = "MISMATCH"
```

Add these functions just after the `HashDelta` dataclass (after its `total_delta` property, around line 221):

```python
def _hash_delta_pct(hash: Optional[HashDelta]) -> Optional[float]:
    """Percent of Oracle hashed rows that diverged (None when undefined).

    ``0.0`` for a clean hash, ``None`` when no hash ran or when Oracle hashed 0
    rows yet a delta exists (an undefined ratio -- treated as a mismatch upstream).
    """
    if hash is None:
        return None
    if hash.total_delta == 0:
        return 0.0
    if hash.oracle_rows <= 0:
        return None
    return 100.0 * hash.total_delta / hash.oracle_rows


def classify_status(
    row_count_delta: Optional[int],
    hash: Optional[HashDelta],
    tolerance_pct: float,
) -> tuple[str, Optional[float]]:
    """Return ``(status, hash_delta_pct)`` for a completed unit.

    ERROR is decided by the caller (a check that could not complete). Row-count
    drift is a hard MISMATCH (zero tolerance). Hash drift is tolerated up to
    ``tolerance_pct`` percent of the Oracle hashed rows -> WITHIN_TOLERANCE.
    """
    pct = _hash_delta_pct(hash)
    if row_count_delta not in (None, 0):
        return STATUS_MISMATCH, pct
    if hash is None or hash.total_delta == 0:
        return STATUS_OK, pct
    if pct is None:  # oracle_rows == 0 with delta > 0 -> undefined ratio
        return STATUS_MISMATCH, None
    return (STATUS_WITHIN_TOLERANCE if pct <= tolerance_pct else STATUS_MISMATCH), pct
```

In the `DqResult` dataclass, add a field after `hash: Optional[HashDelta] = None` (around line 584):

```python
    hash_delta_pct: Optional[float] = None
```

- [ ] **Step 4: Wire it into `check_unit`**

In `etl/dq_check.py` `check_unit`, replace the status block (currently around lines 706-709):

```python
        # ---- status -----------------------------------------------------------
        bad_count = res.row_count_delta not in (None, 0)
        bad_hash = res.hash is not None and res.hash.total_delta > 0
        res.status = "MISMATCH" if (bad_count or bad_hash) else "OK"
```

with:

```python
        # ---- status -----------------------------------------------------------
        res.status, res.hash_delta_pct = classify_status(
            res.row_count_delta, res.hash, settings.dq_hash_delta_tolerance_pct)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance.py -v`
Expected: PASS (7 passed).

- [ ] **Step 6: Commit**

```bash
git add etl/dq_check.py tests/test_dq_tolerance.py
git commit -m "feat(dq): WITHIN_TOLERANCE status via classify_status"
```

---

## Task 3: DQ reporting surfaces (summary, Iceberg row, hints)

**Files:**
- Modify: `etl/dq_check.py` (`render_summary`, `_result_rows`, `_DQ_HINTS`)
- Test: `tests/test_dq_tolerance.py` (append)

**Interfaces:**
- Consumes: `DqResult.hash_delta_pct` (Task 2), `STATUS_WITHIN_TOLERANCE`.
- Produces: `etl_dq_results` gains a `hash_delta_pct` double column; `render_summary` gains a `TOL%` column and a `WITHIN_TOLERANCE` tally.

- [ ] **Step 1: Write the failing test (append to `tests/test_dq_tolerance.py`)**

```python
def _res(status, pct, table="t", branch="b"):
    return DqResult(
        table=table, source_table="OASIS.T", branch=branch,
        oracle_row_count=1000, iceberg_row_count=1000,
        hash=_hash(matched=992, oo=8, ora=1000, ice=1000),
        hash_delta_pct=pct, status=status)


def test_render_summary_has_tol_column_and_tally():
    out = dq_check.render_summary(
        [_res("WITHIN_TOLERANCE", 0.8), _res("OK", 0.0, table="u")], do_hash=True)
    assert "TOL%" in out
    assert "0.80%" in out
    assert "1 WITHIN_TOLERANCE" in out


def test_render_summary_tol_dash_without_hash():
    out = dq_check.render_summary([_res("OK", None)], do_hash=False)
    assert "TOL%" not in out  # TOL% only shown with the hash columns


def test_result_rows_includes_hash_delta_pct():
    from etl.config import Settings
    rows = dq_check._result_rows([_res("WITHIN_TOLERANCE", 0.8)], Settings(), "run1")
    assert rows[0]["hash_delta_pct"] == 0.8
    assert rows[0]["status"] == "WITHIN_TOLERANCE"


def test_dq_hints_has_hash_delta_pct_double():
    assert dq_check._DQ_HINTS["hash_delta_pct"] == {"data_type": "double"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance.py -k "summary or result_rows or hints" -v`
Expected: FAIL — `KeyError: 'hash_delta_pct'` / missing `TOL%`.

- [ ] **Step 3: Add the `_DQ_HINTS` column**

In `etl/dq_check.py` `_DQ_HINTS`, add after the `"hash_total_delta"` entry (around line 863):

```python
    "hash_delta_pct": {"data_type": "double"},
```

- [ ] **Step 4: Add the `_result_rows` field**

In `_result_rows`, add to the row dict after `"hash_total_delta": h.total_delta if h else None,` (around line 828):

```python
            "hash_delta_pct": r.hash_delta_pct,
```

- [ ] **Step 5: Add the `TOL%` column + tally to `render_summary`**

In `render_summary`, change the header build (around line 895) to append `"TOL%"`:

```python
    if do_hash:
        headers += ["MATCH", "ONLY_ORA", "ONLY_ICE", "MISMATCH", "HASH_DELTA", "TOL%"]
```

Add a percent formatter next to the existing `cell` helper (around line 899):

```python
    def pct_cell(v) -> str:
        return "-" if v is None else f"{v:.2f}%"
```

In the per-row build, extend the `if do_hash:` block (around line 910) to append the pct:

```python
        if do_hash:
            h = r.hash
            row += [cell(h.matched if h else None), cell(h.only_in_oracle if h else None),
                    cell(h.only_in_iceberg if h else None), cell(h.mismatch if h else None),
                    cell(h.total_delta if h else None), pct_cell(r.hash_delta_pct)]
```

Update the tally (around lines 926-932) to count and print WITHIN_TOLERANCE:

```python
    ok = sum(1 for r in results if r.status == "OK")
    tol = sum(1 for r in results if r.status == STATUS_WITHIN_TOLERANCE)
    mism = sum(1 for r in results if r.status == "MISMATCH")
    err = sum(1 for r in results if r.status == "ERROR")
    skip = sum(1 for r in results if r.status == "SKIPPED")
    lines.append("")
    lines.append(f"{len(results)} unit(s): {ok} OK, {tol} WITHIN_TOLERANCE, "
                 f"{mism} MISMATCH, {err} ERROR, {skip} SKIPPED")
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance.py -v`
Expected: PASS (all).

- [ ] **Step 7: Commit**

```bash
git add etl/dq_check.py tests/test_dq_tolerance.py
git commit -m "feat(dq): TOL% column + hash_delta_pct in summary and etl_dq_results"
```

---

## Task 4: GUI settings surface + DQ results/pill wiring

**Files:**
- Modify: `gui/workspace.py` (`EDITABLE_ETL_KEYS`)
- Modify: `gui/templates/run.html` (`SET_EDITABLE` / `SET_NUMERIC` / `SET_SHOWN`)
- Modify: `gui/templates/logs.html` (`dqView` numCols)
- Modify: `gui/static/style.css` (`.pill.within_tolerance`)
- Test: `tests/test_dq_tolerance_settings.py` (append)

**Interfaces:**
- Consumes: the `dq_hash_delta_tolerance_pct` key (Task 1).
- Produces: the key is editable via `workspace.update_etl_settings` and shown in the Run settings panel.

- [ ] **Step 1: Write the failing test (append to `tests/test_dq_tolerance_settings.py`)**

```python
def test_tolerance_key_is_editable(tmp_path, monkeypatch):
    import workspace
    cfg = tmp_path / "config.toml"
    cfg.write_text("[etl]\ndq_hash_delta_tolerance_pct = 10.0\n", encoding="utf-8")
    monkeypatch.setattr(workspace, "CONFIG_TOML", cfg)
    monkeypatch.setattr(workspace, "STATE_DIR", tmp_path)
    assert "dq_hash_delta_tolerance_pct" in workspace.EDITABLE_ETL_KEYS
    res = workspace.update_etl_settings({"dq_hash_delta_tolerance_pct": 5.0})
    assert res["applied"]["dq_hash_delta_tolerance_pct"] == 5.0
    assert "dq_hash_delta_tolerance_pct = 5.0" in cfg.read_text(encoding="utf-8")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance_settings.py::test_tolerance_key_is_editable -v`
Expected: FAIL — `ValueError: Not editable: dq_hash_delta_tolerance_pct`.

- [ ] **Step 3: Add the key to the workspace allowlist**

In `gui/workspace.py` `EDITABLE_ETL_KEYS` (around lines 83-88), add `"dq_hash_delta_tolerance_pct"`:

```python
EDITABLE_ETL_KEYS = {
    "dataset_name", "pipeline_name", "max_branch_workers", "max_table_workers",
    "pool_min", "pool_max", "pool_increment", "pool_acquire_timeout_s",
    "pool_acquire_attempts", "max_retries", "retry_interval_s",
    "snapshot_expire_days", "snapshot_min_to_keep", "dsn_mode",
    "dq_hash_delta_tolerance_pct",
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance_settings.py::test_tolerance_key_is_editable -v`
Expected: PASS.

- [ ] **Step 5: Show + allow editing in the Run settings panel**

In `gui/templates/run.html`, update the three JS sets (around lines 162-169). Add the key to `SET_EDITABLE`, `SET_NUMERIC`, and `SET_SHOWN`:

```javascript
const SET_EDITABLE = new Set(["dataset_name","pipeline_name","max_branch_workers","max_table_workers",
  "pool_min","pool_max","pool_increment","pool_acquire_timeout_s","pool_acquire_attempts",
  "max_retries","retry_interval_s","snapshot_expire_days","snapshot_min_to_keep","dsn_mode",
  "dq_hash_delta_tolerance_pct"]);
const SET_NUMERIC = new Set(["max_branch_workers","max_table_workers","pool_min","pool_max","pool_increment",
  "pool_acquire_timeout_s","pool_acquire_attempts","max_retries","retry_interval_s",
  "snapshot_expire_days","snapshot_min_to_keep","dq_hash_delta_tolerance_pct"]);
const SET_SHOWN = ["dataset_name","max_branch_workers","max_table_workers","pool_max",
  "max_retries","retry_interval_s","snapshot_expire_days","dq_hash_delta_tolerance_pct"];
```

- [ ] **Step 6: Show the % in the DQ results table**

In `gui/templates/logs.html`, in the `dqView` definition (around line 208), add `"hash_delta_pct"` to `numCols`:

```javascript
  numCols: ["oracle_row_count","iceberg_row_count","row_count_delta","hash_matched","hash_mismatch","hash_total_delta","hash_delta_pct"],
```

- [ ] **Step 7: Add the amber pill CSS**

In `gui/static/style.css`, after the `.pill.failed, .pill.error, .pill.mismatch` rule (line 377), add:

```css
.pill.within_tolerance { background: var(--warning-light); color: #b45309; }
```

- [ ] **Step 8: Manual verify**

Start the GUI: `.venv/Scripts/python.exe gui/app.py` then open `http://127.0.0.1:8765/run`.
Expected: the ETL settings panel shows a `dq_hash_delta_tolerance_pct` card = `10.0`; click **Edit**, it becomes a number input; change to `5`, **Save** → toast "Saved 1 setting(s)"; confirm `.dlt/config.toml` now reads `dq_hash_delta_tolerance_pct = 5.0`. (Restore to `10.0` after.)

- [ ] **Step 9: Commit**

```bash
git add gui/workspace.py gui/templates/run.html gui/templates/logs.html gui/static/style.css tests/test_dq_tolerance_settings.py
git commit -m "feat(gui): expose DQ tolerance setting + WITHIN_TOLERANCE pill/column"
```

---

## Task 5: DQ live-progress emitter

**Files:**
- Modify: `etl/dq_check.py` (`_fmt_elapsed`, `_DqProgress`, `run_dq` wiring)
- Modify: `dq_check.py` (`--no-progress` flag; `if v is not None` override filter)
- Test: `tests/test_dq_progress.py`

**Interfaces:**
- Consumes: `DqResult` (with `.status`, `.hash`, `.hash_delta_pct`, `.row_count_delta`), status constants (Task 2), `Settings.progress_enabled` / `.progress_interval_s`.
- Produces:
  - `_fmt_elapsed(seconds: float) -> str`.
  - `_DqProgress(total: int, *, interval_s: float = 5.0, enabled: bool = True, logger=None)` with `.start()`, `.record(res: DqResult)`, `.stop()`, `._unit_line(res)`, `._heartbeat_line(elapsed)`.
  - Log lines `DQ-UNIT …` (per unit) and `DQ-PROGRESS …` (heartbeat) on the `etl.dq` logger.

- [ ] **Step 1: Write the failing test**

Create `tests/test_dq_progress.py`:

```python
"""DQ live-progress emitter: line formats + counters."""
from __future__ import annotations

import logging

from etl.dq_check import DqResult, HashDelta, _DqProgress, _fmt_elapsed


def _unit(status, table="appointments", branch="jazan", ora=2000, ice=2000,
          matched=1992, oo=8, pct=0.40):
    return DqResult(
        table=table, source_table="OASIS.APPT", branch=branch,
        oracle_row_count=ora, iceberg_row_count=ice,
        hash=HashDelta(matched=matched, only_in_oracle=oo, oracle_rows=ora, iceberg_rows=ice),
        hash_delta_pct=pct, status=status)


def test_fmt_elapsed():
    assert _fmt_elapsed(72) == "0:01:12"
    assert _fmt_elapsed(3661) == "1:01:01"


def test_unit_line_format():
    p = _DqProgress(total=3, enabled=False)
    line = p._unit_line(_unit("WITHIN_TOLERANCE"))
    assert line == ("DQ-UNIT appointments/jazan | ora=2000 ice=2000 cnt=0 | "
                    "match=1992 delta=8 pct=0.40 | WITHIN_TOLERANCE")


def test_unit_line_handles_missing_hash():
    p = _DqProgress(total=1, enabled=False)
    res = DqResult(table="m", source_table="OASIS.M", branch="b",
                   oracle_row_count=10, iceberg_row_count=10, hash=None,
                   hash_delta_pct=None, status="OK")
    assert p._unit_line(res) == "DQ-UNIT m/b | ora=10 ice=10 cnt=0 | match=- delta=- pct=- | OK"


def test_record_counts_and_heartbeat(caplog):
    p = _DqProgress(total=3, enabled=False)
    p.start()
    with caplog.at_level(logging.INFO, logger="etl.dq"):
        p.record(_unit("WITHIN_TOLERANCE"))
        p.record(_unit("OK", table="staff", ora=500, ice=500, matched=500, oo=0, pct=0.0))
    assert "DQ-UNIT appointments/jazan" in caplog.text
    assert p._heartbeat_line(10) == "DQ-PROGRESS 0:00:10 | units 2/3 | ok 1 tol 1 mismatch 0 err 0"
    p.stop()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_progress.py -v`
Expected: FAIL — `ImportError: cannot import name '_DqProgress'`.

- [ ] **Step 3: Add `_fmt_elapsed` + `_DqProgress`**

In `etl/dq_check.py`, add `import time` to the imports (near `import threading`, line 40). Then add, just above the `# Orchestration` section (before `def run_dq`, around line 728):

```python
def _fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 3600:d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


class _DqProgress:
    """Cheap per-unit + heartbeat progress for a DQ run.

    ``DQ-UNIT`` lines are logged as each (table, branch) completes; a background
    daemon thread logs a ``DQ-PROGRESS`` heartbeat every ``interval_s``. Both go
    to the ``etl.dq`` logger so they land in the run log with timestamps; the GUI
    parses them into a live dashboard. All updates are integer counters under a
    short lock -- no per-unit measurement cost.
    """

    def __init__(self, total: int, *, interval_s: float = 5.0,
                 enabled: bool = True, logger: Optional[logging.Logger] = None):
        self.total = total
        self.interval_s = max(1.0, float(interval_s))
        self.enabled = enabled
        self.log = logger or log
        self._lock = threading.Lock()
        self._done = self._ok = self._tol = self._mismatch = self._err = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_t = 0.0

    def start(self) -> "_DqProgress":
        self._start_t = time.perf_counter()
        if self.enabled:
            self._thread = threading.Thread(
                target=self._run, name="dq-progress", daemon=True)
            self._thread.start()
        return self

    def record(self, res: "DqResult") -> None:
        with self._lock:
            self._done += 1
            if res.status == STATUS_OK:
                self._ok += 1
            elif res.status == STATUS_WITHIN_TOLERANCE:
                self._tol += 1
            elif res.status == "ERROR":
                self._err += 1
            elif res.status == STATUS_MISMATCH:
                self._mismatch += 1
        self.log.info(self._unit_line(res))

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 2.0)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self.log.info(self._heartbeat_line(time.perf_counter() - self._start_t))

    @staticmethod
    def _n(v) -> str:
        return "-" if v is None else str(v)

    def _unit_line(self, res: "DqResult") -> str:
        h = res.hash
        pct = "-" if res.hash_delta_pct is None else f"{res.hash_delta_pct:.2f}"
        return (f"DQ-UNIT {res.table}/{res.branch} | "
                f"ora={self._n(res.oracle_row_count)} ice={self._n(res.iceberg_row_count)} "
                f"cnt={self._n(res.row_count_delta)} | "
                f"match={self._n(h.matched if h else None)} "
                f"delta={self._n(h.total_delta if h else None)} pct={pct} | {res.status}")

    def _heartbeat_line(self, elapsed: float) -> str:
        with self._lock:
            done, ok, tol, mm, err = (
                self._done, self._ok, self._tol, self._mismatch, self._err)
        return (f"DQ-PROGRESS {_fmt_elapsed(elapsed)} | units {done}/{self.total} | "
                f"ok {ok} tol {tol} mismatch {mm} err {err}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_progress.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Wire the emitter into `run_dq`**

In `etl/dq_check.py` `run_dq`, create the progress object after `lock = threading.Lock()` (around line 760):

```python
    progress = _DqProgress(
        total=len(tables) * len(branches),
        interval_s=settings.progress_interval_s,
        enabled=settings.progress_enabled,
    ).start()
```

Inside the nested `run_branch`, record each unit as it completes. Replace the success loop (around lines 773-778):

```python
            out = []
            for tdef in tables:
                entry = (control.get(tdef.dataset_table_name, {}) or {}).get(branch.key, {})
                res_u = check_unit(
                    tdef, branch, settings, lake[tdef.dataset_table_name], entry,
                    since, until, do_hash, conn=conn, self_test=self_test)
                progress.record(res_u)
                out.append(res_u)
            return out
```

And in `run_branch`'s `except` (the branch-level failure path, around lines 779-783), record the synthesized ERROR results so the unit counter still reaches `total`:

```python
        except Exception as exc:  # noqa: BLE001 - a dead branch fails only its own rows
            log.error("[%s] branch failed: %s", branch.key, exc)
            errs = [DqResult(table=t.dataset_table_name, source_table=t.table,
                             branch=branch.key, status="ERROR",
                             error=f"{type(exc).__name__}: {exc}") for t in tables]
            for r in errs:
                progress.record(r)
            return errs
```

After the `ThreadPoolExecutor` block returns (after the `for fut in as_completed(...)` loop, before `return results`, around line 797), stop the heartbeat:

```python
    progress.stop()
    return results
```

- [ ] **Step 6: Add `--no-progress` to the CLI**

In `dq_check.py` (CLI), add the argument in `parse_args` (after `--self-test`, around line 76):

```python
    p.add_argument("--no-progress", action="store_true",
                   help="suppress DQ progress heartbeat lines")
```

In `main`, change the overrides block (around lines 97-99) so `False` survives the filter and `--no-progress` is applied:

```python
    overrides = {"dsn_mode": args.dsn_mode,
                 "oracle_client_lib_dir": args.oracle_client_lib_dir}
    if args.no_progress:
        overrides["progress_enabled"] = False
    settings = config.load_settings({k: v for k, v in overrides.items() if v is not None})
```

- [ ] **Step 7: Manual verify (self-test run emits progress lines)**

Run a DQ self-test (works offline against staged parquet; harmless if no staging — it will just SKIP but still emit the opening line and heartbeat):

Run: `.venv/Scripts/python.exe dq_check.py --self-test --no-write 2>&1 | grep -E "DQ-UNIT|DQ-PROGRESS" | head`
Expected: at least one `DQ-UNIT …` line (or, with no staged data, the run still completes; re-run without `--self-test` against a real branch to see `DQ-UNIT`/`DQ-PROGRESS`). The full run also still prints the summary table with the new `TOL%` column and the `WITHIN_TOLERANCE` tally.

- [ ] **Step 8: Run the full backend test suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dq_tolerance.py tests/test_dq_tolerance_settings.py tests/test_dq_progress.py -v`
Expected: PASS (all).

- [ ] **Step 9: Commit**

```bash
git add etl/dq_check.py dq_check.py tests/test_dq_progress.py
git commit -m "feat(dq): live DQ-UNIT/DQ-PROGRESS emitter + --no-progress"
```

---

## Task 6: Frontend — log-type dispatcher + pipeline & generic views

**Files:**
- Rewrite: `gui/static/runparse.js`
- Rewrite: `gui/templates/_dash.html`
- Modify: `gui/static/style.css` (rd-* helper classes)

**Interfaces:**
- Consumes: DOM ids from `_dash.html`; globals `el`, `esc`, `fmtNum`, `pill` (from `app.js`).
- Produces: `createLogDash(opts) -> { reset, feed, flush, render, load, get dash() }` (unchanged signature), now type-aware. Registers views `pipeline`, `dq`, `snapshot`, `generic`.

This task delivers the dispatcher, the ported **pipeline** view (no behavior change for `oracle_to_iceberg` logs), and a **generic** fallback view. The DQ and snapshot specifics come in Tasks 7-8, but the file is written complete here (all view factories present).

- [ ] **Step 1: Rewrite `gui/templates/_dash.html`**

Replace the entire file with:

```html
{# Parsed progress/summary dashboard. Rendered by static/runparse.js
   (createLogDash), which detects the log's command type and shows one of the
   sections below. Included by run.html (live tail) and logs.html (whole-file). #}
<div id="run-dash" class="run-dash" hidden>

  {# ---- oracle_to_iceberg pipeline ---- #}
  <div id="rd-pipeline" hidden>
    <div class="rd-overall">
      <div class="rd-overall-top">
        <span id="rd-stage" class="rd-stage">starting</span>
        <span class="rd-metrics">
          <i class="fa-regular fa-clock"></i> <span id="rd-elapsed">0:00:00</span>
          <span class="rd-sep">·</span> <span id="rd-rows">0 rows</span>
          <span class="rd-sep">·</span> <span id="rd-mem">rss —</span>
          <span id="rd-fail" class="rd-fail" hidden></span>
        </span>
      </div>
      <div class="rd-bar"><div id="rd-bar-fill" class="rd-bar-fill"></div><span id="rd-bar-label" class="rd-bar-label"></span></div>
    </div>
    <div class="rd-section-head"><span>Tables</span><span id="rd-branch-strip" class="rd-branch-strip"></span></div>
    <div class="table-wrap rd-tablewrap">
      <table class="rd-table">
        <thead><tr><th>Table</th><th>Extract (branches)</th><th>Load</th><th class="num">Rows</th><th>Issue</th></tr></thead>
        <tbody id="rd-tbody"></tbody>
      </table>
    </div>
    <details id="rd-issues-box" class="rd-issues" hidden>
      <summary><i class="fa-solid fa-triangle-exclamation"></i> Issues (<span id="rd-issue-count">0</span>)</summary>
      <div id="rd-issues"></div>
    </details>
  </div>

  {# ---- dq_check reconciliation ---- #}
  <div id="rd-dq" hidden>
    <div class="rd-overall">
      <div class="rd-overall-top">
        <span id="dq-title" class="rd-stage">DQ reconciliation</span>
        <span class="rd-metrics">
          <i class="fa-regular fa-clock"></i> <span id="dq-elapsed">0:00:00</span>
          <span class="rd-sep">·</span> <span id="dq-tallies"></span>
        </span>
      </div>
      <div class="rd-bar"><div id="dq-bar-fill" class="rd-bar-fill"></div><span id="dq-bar-label" class="rd-bar-label"></span></div>
      <div id="dq-scope" class="rd-scope muted"></div>
    </div>
    <div class="table-wrap rd-tablewrap">
      <table class="rd-table">
        <thead><tr><th>Table</th><th>Branch</th><th class="num">Ora</th><th class="num">Ice</th><th class="num">Cnt Δ</th><th class="num">Hash Δ</th><th class="num">%</th><th>Status</th></tr></thead>
        <tbody id="dq-tbody"></tbody>
      </table>
    </div>
    <details id="dq-issues-box" class="rd-issues" hidden>
      <summary><i class="fa-solid fa-triangle-exclamation"></i> Issues (<span id="dq-issue-count">0</span>)</summary>
      <div id="dq-issues"></div>
    </details>
  </div>

  {# ---- snapshot_diff / fresh_run / custom / unknown ---- #}
  <div id="rd-generic" hidden>
    <div class="rd-overall">
      <div class="rd-overall-top">
        <span id="gen-title" class="rd-stage">Log summary</span>
        <span class="rd-metrics">
          <i class="fa-regular fa-clock"></i> <span id="gen-elapsed">—</span>
          <span class="rd-sep">·</span> <span id="gen-status"></span>
        </span>
      </div>
    </div>
    <div id="gen-body" class="rd-genbody"></div>
    <details id="gen-issues-box" class="rd-issues" hidden>
      <summary><i class="fa-solid fa-triangle-exclamation"></i> Issues (<span id="gen-issue-count">0</span>)</summary>
      <div id="gen-issues"></div>
    </details>
  </div>

</div>
```

- [ ] **Step 2: Rewrite `gui/static/runparse.js`**

Replace the entire file with:

```javascript
/* Shared log -> progress/summary dashboard, used by the Run page (live tail) and
 * the Monitor "Log files" tab (whole-file). Detects the log's command type from
 * the runner header ("# command : ...", falling back to characteristic lines) and
 * routes parsing + rendering to a per-type view. Public API is unchanged:
 *   createLogDash(opts) -> { reset(), feed(chunk), flush(), render(), load(text), get dash() }
 *   opts.branchHint : () => number  best guess of total branches (Run page only)
 */
function createLogDash(opts = {}) {
  const branchHint = opts.branchHint || (() => 0);

  const HDR = {
    command: /^#\s*command\s*:\s*(.+)$/,
    started: /^#\s*started\s*:\s*(.+)$/,
    exit: /\[runner\] exited with code (-?\d+)/,
    ts: /^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d{3}/,
  };

  function classifyCommand(cmd) {
    if (/dq_check\.py/.test(cmd)) return "dq";
    if (/oracle_to_iceberg\.py/.test(cmd)) return "pipeline";
    if (/snapshot_diff\.py/.test(cmd)) return "snapshot";
    return "generic";               // fresh_run / custom / unknown
  }
  function sniff(line) {
    if (/DQ-PROGRESS|DQ-UNIT|DQ run /.test(line)) return "dq";
    if (/\bPROGRESS\s+\d/.test(line)) return "pipeline";
    if (/^Baseline \(as-of|^Updated\s*:/.test(line)) return "snapshot";
    return null;
  }
  function freshMeta() {
    return { command: "", started: "", exit: null, firstTs: null, lastTs: null };
  }
  function elapsedFromTs(meta) {
    if (!meta.firstTs || !meta.lastTs) return "";
    const a = Date.parse(meta.firstTs.replace(" ", "T")), b = Date.parse(meta.lastTs.replace(" ", "T"));
    if (isNaN(a) || isNaN(b) || b < a) return "";
    const s = Math.floor((b - a) / 1000);
    return `${Math.floor(s / 3600)}:${String(Math.floor(s % 3600 / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;
  }
  function stripPrefix(line) {
    return line.replace(/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}\s*\|?\s*(?:\w+\s*\|\s*[\w.]+\s*\|\s*)?/, "").trim();
  }

  const views = {
    pipeline: makePipelineView({ branchHint }),
    dq: makeDqView(),
    snapshot: makeGenericView("snapshot"),
    generic: makeGenericView("generic"),
  };

  let type = null, active = null, meta = freshMeta(), lineBuf = "";

  function setType(t) {
    if (!t || t === type) return;
    type = t;
    active = views[t] || views.generic;
  }

  function feedLine(line) {
    if (line == null) return;
    let m;
    if ((m = HDR.command.exec(line))) { meta.command = m[1].trim(); setType(classifyCommand(meta.command)); return; }
    if ((m = HDR.started.exec(line))) { meta.started = m[1].trim(); return; }
    if ((m = HDR.ts.exec(line))) { if (!meta.firstTs) meta.firstTs = m[1]; meta.lastTs = m[1]; }
    if ((m = HDR.exit.exec(line))) { meta.exit = +m[1]; }
    if (!type) { const s = sniff(line); if (s) setType(s); }
    if (active) active.feedLine(line, meta);
  }

  function feed(chunk) {
    lineBuf += chunk;
    const parts = lineBuf.split("\n");
    lineBuf = parts.pop();
    for (const ln of parts) feedLine(ln);
  }
  function flush() { if (lineBuf) { feedLine(lineBuf); lineBuf = ""; } }

  function render() {
    const box = el("run-dash");
    if (!box) return;
    for (const id of ["rd-pipeline", "rd-dq", "rd-generic"]) { const s = el(id); if (s) s.hidden = true; }
    if (!active || !active.hasContent()) { box.hidden = true; return; }
    box.hidden = false;
    active.render(meta, { elapsedFromTs, stripPrefix });
  }

  function reset() {
    type = null; active = null; meta = freshMeta(); lineBuf = "";
    for (const v of Object.values(views)) v.reset();
    render();
  }
  function load(text) { reset(); feed(text || ""); flush(); render(); }

  reset();
  return { reset, feed, flush, render, load, get dash() { return active && active.model ? active.model() : null; } };
}

/* ------------------------------------------------------------------ pipeline */
/* oracle_to_iceberg: PROGRESS heartbeats, per-unit extract lines, table loads,
 * the final summary rows. Renders into the #rd-pipeline section. */
function makePipelineView(opts = {}) {
  const branchHint = opts.branchHint || (() => 0);
  const RE = {
    ts: /^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d{3}/,
    prog: /PROGRESS\s+(\d+:\d\d:\d\d)\s+\|\s+([^|]+?)\s+\|\s+tables\s+(\d+)\/(\d+)\s+\|\s+extract\s+(\d+)\/(\d+)(?:\s+(\d+)\s+failed)?\s+\|\s+rows=([\d,]+)\s+\|\s+rss=([^( ]+)\(peak\s+([^)]+)\)\s+arrow=(\S+)/,
    unit: /\[([^/\]]+)\/([^\]]+)\]\s+([\d,]+)\s+rows\s+\(attempt\s+(\d+)\)/,
    unitErr: /\[([^/\]]+)\/([^\]]+)\]\s+(?:non-connection error during read|connection error[^:]*):\s*(.+)/,
    loaded: /\[([^/\]]+)\]\s+loaded:\s+disp=(\S+)\s+ok=(\d+)\s+fail=(\d+)\s+rows=([\d,]+)/,
    loadFail: /\[([^/\]]+)\]\s+load failed:\s*(.+)/,
    skipped: /\[([^/\]]+)\]\s+skipped:\s*(.+)/,
    summaryRow: /^\s{2,}(\S+)\s+(SUCCESS|FAILED)\s+disp=(\S+)\s+ok=(\d+)\s+fail=(\d+)\s+rows=(\d+)/,
  };
  let dash;
  function fresh() {
    return {
      stage: "", elapsed: "", rows: 0,
      unitsDone: 0, unitsTotal: 0, unitsFailed: 0, tablesDone: 0, tablesTotal: 0,
      rss: "", rssPeak: "", arrow: "", branchesTotal: 0,
      tables: new Map(), branches: new Map(),
      issues: [], started: false, firstTs: null, lastTs: null,
    };
  }
  function tdef(name) {
    let t = dash.tables.get(name);
    if (!t) { t = { ok: 0, fail: 0, branches: new Set(), load: "pending", disp: "", rows: 0, err: "", final: null }; dash.tables.set(name, t); }
    return t;
  }
  function bdef(key) {
    let b = dash.branches.get(key);
    if (!b) { b = { ok: 0, fail: 0 }; dash.branches.set(key, b); }
    return b;
  }
  function pushIssue(line, level) {
    const text = line.replace(/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}\s*\|?\s*/, "").trim();
    dash.issues.push({ level, text: text.slice(0, 240), ts: (dash.lastTs || "").slice(11) });
    if (dash.issues.length > 200) dash.issues.shift();
  }
  function feedLine(line) {
    if (!line) return;
    const tm = RE.ts.exec(line);
    if (tm) { if (!dash.firstTs) dash.firstTs = tm[1]; dash.lastTs = tm[1]; }
    let m;
    if ((m = RE.prog.exec(line))) {
      dash.started = true;
      dash.elapsed = m[1]; dash.stage = m[2].trim();
      dash.tablesDone = +m[3]; dash.tablesTotal = +m[4];
      dash.unitsDone = +m[5]; dash.unitsTotal = +m[6];
      if (m[7]) dash.unitsFailed = +m[7];
      dash.rows = +m[8].replace(/,/g, "");
      dash.rss = m[9]; dash.rssPeak = m[10]; dash.arrow = m[11];
      if (dash.tablesTotal && dash.unitsTotal) dash.branchesTotal = Math.round(dash.unitsTotal / dash.tablesTotal);
      if (dash.stage.startsWith("load:")) { const t = dash.tables.get(dash.stage.slice(5)); if (t && t.load === "pending") t.load = "loading"; }
      return;
    }
    if ((m = RE.unit.exec(line))) { dash.started = true; const t = tdef(m[2]); t.ok++; t.branches.add(m[1]); t.rows += +m[3].replace(/,/g, ""); bdef(m[1]).ok++; return; }
    if ((m = RE.unitErr.exec(line))) { const t = tdef(m[2]); t.fail++; t.err = m[3].slice(0, 160); bdef(m[1]).fail++; pushIssue(line, "error"); return; }
    if ((m = RE.loaded.exec(line))) { const t = tdef(m[1]); t.disp = m[2]; t.load = +m[4] > 0 ? "failed" : "loaded"; t.rows = +m[5].replace(/,/g, ""); return; }
    if ((m = RE.loadFail.exec(line))) { const t = tdef(m[1]); t.load = "failed"; t.err = m[2].slice(0, 160); pushIssue(line, "error"); return; }
    if ((m = RE.skipped.exec(line))) { const t = tdef(m[1]); t.load = "skipped"; t.err = m[2].slice(0, 160); return; }
    if ((m = RE.summaryRow.exec(line))) { const t = tdef(m[1]); t.final = m[2]; t.disp = m[3]; if (t.load === "pending" || t.load === "loading") t.load = m[2] === "SUCCESS" ? "loaded" : "failed"; return; }
    if (/\|\s*(WARNING|ERROR)\s*\||\|\[(WARNING|ERROR)\]\||UserWarning|Traceback/.test(line)) pushIssue(line, /ERROR|Traceback/.test(line) ? "error" : "warn");
  }
  function branchTotal() { return dash.branchesTotal || dash.branches.size || branchHint() || 0; }
  function elapsedFromTs() {
    if (!dash.firstTs || !dash.lastTs) return "0:00:00";
    const a = Date.parse(dash.firstTs.replace(" ", "T")), b = Date.parse(dash.lastTs.replace(" ", "T"));
    if (isNaN(a) || isNaN(b) || b < a) return "0:00:00";
    const s = Math.floor((b - a) / 1000);
    return `${Math.floor(s / 3600)}:${String(Math.floor(s % 3600 / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;
  }
  function stageClass(stage) {
    if (!stage) return "";
    if (stage.startsWith("load:") || stage.startsWith("draining")) return "st-load";
    if (stage === "finalize" || stage === "done") return "st-final";
    return "";
  }
  function loadPill(s) {
    const cls = { pending: "gray", loading: "running", loaded: "ok", failed: "failed", skipped: "skipped" }[s] || "gray";
    return `<span class="pill ${cls}">${esc(s)}</span>`;
  }
  function hasContent() { return !!dash && (dash.started || dash.tables.size > 0 || dash.issues.length > 0); }
  function render(meta) {
    el("rd-pipeline").hidden = false;
    const bt = branchTotal();
    const done = dash.unitsDone || [...dash.tables.values()].reduce((a, t) => a + t.branches.size, 0);
    const total = dash.unitsTotal || (bt * (dash.tablesTotal || dash.tables.size)) || 0;
    const exited = meta && meta.exit != null;
    const pct = total ? Math.min(100, Math.round(done / total * 100)) : (exited ? 100 : 0);
    const failTotal = dash.unitsFailed || [...dash.tables.values()].reduce((a, t) => a + t.fail, 0);

    el("rd-stage").textContent = dash.stage || (exited ? "done" : "starting");
    el("rd-stage").className = "rd-stage " + stageClass(dash.stage || (exited ? "done" : ""));
    el("rd-elapsed").textContent = dash.elapsed || elapsedFromTs();
    el("rd-rows").textContent = fmtNum(dash.rows) + " rows";
    el("rd-mem").textContent = dash.rss ? `rss ${dash.rss} (peak ${dash.rssPeak})` : "rss —";
    const fEl = el("rd-fail"); fEl.hidden = !failTotal; fEl.textContent = `${failTotal} failed`;
    el("rd-bar-fill").style.width = pct + "%";
    el("rd-bar-fill").className = "rd-bar-fill" + (exited && meta.exit ? " err" : (failTotal ? " has-fail" : ""));
    el("rd-bar-label").textContent = `${done}/${total || "?"} units · ${pct}%` + (dash.tablesTotal ? ` · tables ${dash.tablesDone}/${dash.tablesTotal}` : "");

    el("rd-branch-strip").innerHTML = [...dash.branches.entries()].sort().map(([k, b]) => {
      const cls = b.fail ? "err" : (bt && b.ok >= bt ? "done" : "");
      return `<span class="rd-bchip ${cls}" title="${esc(k)}: ${b.ok} ok${b.fail ? `, ${b.fail} failed` : ""}">${esc(k)} ${b.ok}${bt ? `/${bt}` : ""}</span>`;
    }).join("");

    el("rd-tbody").innerHTML = [...dash.tables.entries()].map(([name, t]) => {
      const ebTot = bt || t.branches.size || 0;
      const eCount = t.branches.size;
      const ePct = ebTot ? Math.round(Math.min(eCount, ebTot) / ebTot * 100) : (t.final ? 100 : 0);
      const issue = t.err ? `<span class="rd-err" title="${esc(t.err)}">${esc(t.err.slice(0, 64))}</span>` : "";
      return `<tr>
        <td class="mono">${esc(name)}</td>
        <td><span class="rd-mini"><span class="rd-mini-fill${t.fail ? " err" : ""}" style="width:${ePct}%"></span></span>
            <span class="rd-mini-lbl">${eCount}${ebTot ? `/${ebTot}` : ""}${t.fail ? ` · ${t.fail}✕` : ""}</span></td>
        <td>${loadPill(t.load)}${t.disp ? ` <span class="rd-disp">${esc(t.disp)}</span>` : ""}</td>
        <td class="num">${fmtNum(t.rows)}</td>
        <td>${issue}</td></tr>`;
    }).join("") || `<tr><td colspan="5" class="muted">Waiting for table activity…</td></tr>`;

    const ibox = el("rd-issues-box");
    ibox.hidden = dash.issues.length === 0;
    el("rd-issue-count").textContent = dash.issues.length;
    el("rd-issues").innerHTML = dash.issues.slice(-60).reverse().map(i =>
      `<div class="rd-issue ${i.level}"><span class="rd-itime">${esc(i.ts)}</span> ${esc(i.text)}</div>`).join("");
  }
  function reset() { dash = fresh(); }
  reset();
  return { reset, feedLine, render, hasContent, model: () => dash };
}

/* ------------------------------------------------------------------------ dq */
/* Filled in Task 7. */
function makeDqView() {
  let m;
  function reset() { m = { started: false }; }
  function feedLine() {}
  function hasContent() { return false; }
  function render() {}
  reset();
  return { reset, feedLine, render, hasContent, model: () => m };
}

/* ------------------------------------------------------------------- generic */
/* snapshot_diff / fresh_run / custom / unknown: a meta strip + key lines +
 * (snapshot mode) the Updated/Inserted/Deleted counts + an issues feed. */
function makeGenericView(mode) {
  let m;
  function reset() {
    m = { keyLines: [], issues: [], snap: {}, hasSnap: false };
  }
  function feedLine(line, meta) {
    if (!line) return;
    let g;
    if ((g = /(?:->|→)\s*wrote\s+(.+)$/.exec(line))) { m.keyLines.push("wrote " + g[1].trim()); return; }
    if (mode === "snapshot") {
      if ((g = /^Table\s*:\s*(.+)$/.exec(line))) { m.snap.table = g[1].trim(); m.hasSnap = true; return; }
      if ((g = /^Baseline \(as-of ([^)]+)\)\s*:\s*snapshot (\d+) @ (.+)$/.exec(line))) { m.snap.baseline = { asOf: g[1], id: g[2], ts: g[3].trim() }; m.hasSnap = true; return; }
      if ((g = /^Latest\s*:\s*snapshot (\d+) @ (.+)$/.exec(line))) { m.snap.latest = { id: g[1], ts: g[2].trim() }; m.hasSnap = true; return; }
      if ((g = /^Identity\s*:\s*(.+)$/.exec(line))) { m.snap.identity = g[1].trim(); m.hasSnap = true; return; }
      if ((g = /^Updated\s*:\s*([\d,]+)\s+Inserted\s*:\s*([\d,]+)\s+Deleted\s*:\s*([\d,]+)/.exec(line))) {
        m.snap.updated = +g[1].replace(/,/g, ""); m.snap.inserted = +g[2].replace(/,/g, ""); m.snap.deleted = +g[3].replace(/,/g, ""); m.hasSnap = true; return;
      }
      if (/^No updated records/.test(line)) { m.snap.updated = 0; m.snap.inserted = 0; m.snap.deleted = 0; m.hasSnap = true; return; }
    }
    if (/\|\s*(WARNING|ERROR)\s*\||WARNING:|Traceback|Error/.test(line)) {
      const text = line.replace(/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}\s*\|?\s*/, "").trim();
      m.issues.push({ level: /ERROR|Traceback|Error/.test(line) ? "error" : "warn", text: text.slice(0, 240) });
      if (m.issues.length > 200) m.issues.shift();
    }
  }
  function hasContent() {
    return !!m && (m.hasSnap || m.keyLines.length > 0 || m.issues.length > 0);
  }
  function render(meta, helpers) {
    el("rd-generic").hidden = false;
    el("gen-title").textContent = meta.command
      ? meta.command.replace(/^.*?([\w.]+\.py|fresh_run\S*)/, "$1").split(" ")[0] || "Log summary"
      : "Log summary";
    const elapsed = helpers.elapsedFromTs(meta);
    el("gen-elapsed").textContent = elapsed || "—";
    el("gen-status").innerHTML = meta.exit == null ? `<span class="pill running">running</span>`
      : pill(meta.exit === 0 ? "finished" : "failed") + ` <small>rc=${meta.exit}</small>`;

    let body = "";
    if (m.hasSnap && (m.snap.table || m.snap.updated != null)) {
      const s = m.snap;
      const chip = (label, v, cls) => `<span class="rd-tally ${cls || ""}">${fmtNum(v)} ${label}</span>`;
      body += `<div class="rd-kv">`;
      if (s.table) body += `<div><span class="k">table</span><span class="v mono">${esc(s.table)}</span></div>`;
      if (s.baseline) body += `<div><span class="k">baseline</span><span class="v mono">snap ${esc(s.baseline.id)} · ${esc(s.baseline.asOf)} @ ${esc(s.baseline.ts)}</span></div>`;
      if (s.latest) body += `<div><span class="k">latest</span><span class="v mono">snap ${esc(s.latest.id)} @ ${esc(s.latest.ts)}</span></div>`;
      if (s.identity) body += `<div><span class="k">identity</span><span class="v mono">${esc(s.identity)}</span></div>`;
      body += `</div>`;
      if (s.updated != null) body += `<div class="rd-tallies">${chip("updated", s.updated, "warn")}${chip("inserted", s.inserted, "ok")}${chip("deleted", s.deleted, "err")}</div>`;
    }
    if (m.keyLines.length) {
      body += `<div class="rd-keylines">` + m.keyLines.slice(-12).map(k =>
        `<div class="mono">${esc(k)}</div>`).join("") + `</div>`;
    }
    if (!body) body = `<div class="muted">No structured summary for this log — see the raw log.</div>`;
    el("gen-body").innerHTML = body;

    const ibox = el("gen-issues-box");
    ibox.hidden = m.issues.length === 0;
    el("gen-issue-count").textContent = m.issues.length;
    el("gen-issues").innerHTML = m.issues.slice(-60).reverse().map(i =>
      `<div class="rd-issue ${i.level}">${esc(i.text)}</div>`).join("");
  }
  reset();
  return { reset, feedLine, render, hasContent, model: () => m };
}
```

- [ ] **Step 3: Add supporting CSS**

In `gui/static/style.css`, after the `.rd-bar-fill.err` rule (line 406), add:

```css
.rd-scope { font-size: 12px; margin-top: 6px; }
.rd-genbody { display: flex; flex-direction: column; gap: 12px; }
.rd-kv { display: grid; gap: 4px; }
.rd-kv > div { display: flex; gap: 8px; font-size: 13px; }
.rd-kv .k { min-width: 84px; color: var(--text-muted); }
.rd-tallies { display: flex; flex-wrap: wrap; gap: 8px; }
.rd-tally { display: inline-block; padding: 4px 10px; border-radius: var(--radius-full); font-size: 12px; font-weight: 700; background: var(--surface-container-highest); }
.rd-tally.ok { background: var(--success-light); color: #047857; }
.rd-tally.warn { background: var(--warning-light); color: #b45309; }
.rd-tally.err { background: var(--error-light); color: #b91c1c; }
.rd-keylines { display: flex; flex-direction: column; gap: 2px; font-size: 12px; }
```

- [ ] **Step 4: Manual verify — oracle_to_iceberg regression**

Start the GUI (`.venv/Scripts/python.exe gui/app.py`), open `http://127.0.0.1:8765/logs`, pick an existing `run-*.log` from an `oracle_to_iceberg` run.
Expected: the same pipeline dashboard as before (overall bar, Tables table, Issues) renders; the raw log still shows below. No console errors (F12).

- [ ] **Step 5: Manual verify — generic fallback**

Create `run_logs/run-sample-custom.log` with this content, then select it on the Logs page:

```text
# OASIS run sample-custom
# label   : custom probe
# command : D:\dlt\.venv\Scripts\python.exe diagnostics/table_stats.py --branch jazan
# started : 2026-07-07T09:00:00
----------------------------------------------------------------------
2026-07-07 09:00:01,000 | INFO | diag | scanning...
-> wrote exports/table_stats_jazan.csv
----------------------------------------------------------------------
[runner] exited with code 0
```

Expected: a "Log summary" card with title `table_stats.py`, status pill `finished rc=0`, and a key line `wrote exports/table_stats_jazan.csv`.

- [ ] **Step 6: Commit**

```bash
git add gui/static/runparse.js gui/templates/_dash.html gui/static/style.css
git commit -m "refactor(gui): log-type dispatcher with pipeline + generic views"
```

---

## Task 7: DQ dashboard view

**Files:**
- Modify: `gui/static/runparse.js` (`makeDqView`)

**Interfaces:**
- Consumes: `DQ-UNIT`, `DQ-PROGRESS`, and `DQ run` log lines (Task 5 formats); DOM ids from `#rd-dq` (Task 6); globals `el`, `esc`, `fmtNum`, `pill`.
- Produces: a live-filling DQ reconciliation view.

- [ ] **Step 1: Replace the stub `makeDqView`**

In `gui/static/runparse.js`, replace the placeholder `makeDqView()` (the "Filled in Task 7" stub) with:

```javascript
/* ------------------------------------------------------------------------ dq */
/* dq_check: DQ run (scope), DQ-PROGRESS (overall), DQ-UNIT (per table×branch),
 * WARNING/ERROR (issues). Renders into the #rd-dq section. */
function makeDqView() {
  const RE = {
    scope: /DQ run\s+(\S+).*?branches=(\[[^\]]*\]).*?tables=(\d+).*?window=(.+?)\s+\|\s+hash=(\w+)/,
    prog: /DQ-PROGRESS\s+(\d+:\d\d:\d\d)\s+\|\s+units\s+(\d+)\/(\d+)\s+\|\s+ok\s+(\d+)\s+tol\s+(\d+)\s+mismatch\s+(\d+)\s+err\s+(\d+)/,
    unit: /DQ-UNIT\s+([^/\s]+)\/([^\s|]+)\s+\|\s+ora=(\S+)\s+ice=(\S+)\s+cnt=(\S+)\s+\|\s+match=(\S+)\s+delta=(\S+)\s+pct=(\S+)\s+\|\s+(\S+)/,
    ts: /^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d{3}/,
  };
  const SEV = { ERROR: 0, MISMATCH: 1, WITHIN_TOLERANCE: 2, OK: 3, SKIPPED: 4 };
  let m;
  function reset() {
    m = {
      started: false, elapsed: "", window: "", branches: [], tablesTotal: 0, hash: "",
      done: 0, total: 0, ok: 0, tol: 0, mismatch: 0, err: 0,
      units: new Map(), issues: [], firstTs: null, lastTs: null,
    };
  }
  function num(v) { return v === "-" || v === undefined ? null : Number(String(v).replace(/,/g, "")); }
  function feedLine(line) {
    if (!line) return;
    const tm = RE.ts.exec(line);
    if (tm) { if (!m.firstTs) m.firstTs = tm[1]; m.lastTs = tm[1]; }
    let g;
    if ((g = RE.scope.exec(line))) {
      m.started = true;
      try { m.branches = JSON.parse(g[2].replace(/'/g, '"')); } catch (e) { m.branches = []; }
      m.tablesTotal = +g[3]; m.window = g[4].trim(); m.hash = g[5];
      return;
    }
    if ((g = RE.prog.exec(line))) {
      m.started = true; m.elapsed = g[1];
      m.done = +g[2]; m.total = +g[3];
      m.ok = +g[4]; m.tol = +g[5]; m.mismatch = +g[6]; m.err = +g[7];
      return;
    }
    if ((g = RE.unit.exec(line))) {
      m.started = true;
      const key = g[1] + "/" + g[2];
      m.units.set(key, {
        table: g[1], branch: g[2],
        ora: num(g[3]), ice: num(g[4]), cnt: num(g[5]),
        match: num(g[6]), delta: num(g[7]),
        pct: g[8] === "-" ? null : Number(g[8]), status: g[9],
      });
      return;
    }
    if (/\|\s*(WARNING|ERROR)\s*\||Traceback/.test(line)) {
      const text = line.replace(/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}\s*\|?\s*/, "").trim();
      m.issues.push({ level: /ERROR|Traceback/.test(line) ? "error" : "warn", text: text.slice(0, 240) });
      if (m.issues.length > 200) m.issues.shift();
    }
  }
  function elapsedFromTs() {
    if (!m.firstTs || !m.lastTs) return "0:00:00";
    const a = Date.parse(m.firstTs.replace(" ", "T")), b = Date.parse(m.lastTs.replace(" ", "T"));
    if (isNaN(a) || isNaN(b) || b < a) return "0:00:00";
    const s = Math.floor((b - a) / 1000);
    return `${Math.floor(s / 3600)}:${String(Math.floor(s % 3600 / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;
  }
  function hasContent() { return !!m && (m.started || m.units.size > 0 || m.issues.length > 0); }
  function render(meta) {
    el("rd-dq").hidden = false;
    const units = [...m.units.values()];
    const done = m.done || units.length;
    const total = m.total || (m.tablesTotal * (m.branches.length || 1)) || units.length;
    const exited = meta && meta.exit != null;
    const pct = total ? Math.min(100, Math.round(done / total * 100)) : (exited ? 100 : 0);

    el("dq-elapsed").textContent = m.elapsed || elapsedFromTs();
    const chip = (n, label, cls) => n ? `<span class="rd-tally ${cls}">${n} ${label}</span>` : "";
    el("dq-tallies").innerHTML =
      chip(m.ok || units.filter(u => u.status === "OK").length, "ok", "ok") +
      chip(m.tol || units.filter(u => u.status === "WITHIN_TOLERANCE").length, "tol", "warn") +
      chip(m.mismatch || units.filter(u => u.status === "MISMATCH").length, "mismatch", "err") +
      chip(m.err || units.filter(u => u.status === "ERROR").length, "err", "err");
    el("dq-bar-fill").style.width = pct + "%";
    const anyBad = (m.mismatch || units.some(u => u.status === "MISMATCH")) || (m.err || units.some(u => u.status === "ERROR"));
    el("dq-bar-fill").className = "rd-bar-fill" + (anyBad ? " has-fail" : "");
    el("dq-bar-label").textContent = `${done}/${total || "?"} units · ${pct}%`;
    el("dq-scope").textContent = m.started
      ? `branches: ${m.branches.join(", ") || "all"} · tables: ${m.tablesTotal || "?"} · hash: ${m.hash || "?"} · window: ${m.window || "?"}`
      : "";

    units.sort((a, b) => (SEV[a.status] ?? 9) - (SEV[b.status] ?? 9) ||
      (b.delta || 0) - (a.delta || 0) || a.table.localeCompare(b.table));
    const cell = (v) => v == null ? "—" : fmtNum(v);
    el("dq-tbody").innerHTML = units.map(u => `<tr>
      <td class="mono">${esc(u.table)}</td>
      <td class="mono">${esc(u.branch)}</td>
      <td class="num">${cell(u.ora)}</td>
      <td class="num">${cell(u.ice)}</td>
      <td class="num">${cell(u.cnt)}</td>
      <td class="num">${cell(u.delta)}</td>
      <td class="num">${u.pct == null ? "—" : u.pct.toFixed(2) + "%"}</td>
      <td>${pill(u.status)}</td></tr>`).join("") ||
      `<tr><td colspan="8" class="muted">Waiting for DQ units…</td></tr>`;

    const ibox = el("dq-issues-box");
    ibox.hidden = m.issues.length === 0;
    el("dq-issue-count").textContent = m.issues.length;
    el("dq-issues").innerHTML = m.issues.slice(-60).reverse().map(i =>
      `<div class="rd-issue ${i.level}">${esc(i.text)}</div>`).join("");
  }
  reset();
  return { reset, feedLine, render, hasContent, model: () => m };
}
```

- [ ] **Step 2: Manual verify — DQ whole-file summary**

Create `run_logs/run-sample-dq.log` with this content, then select it on the Logs page:

```text
# OASIS run sample-dq
# label   : dq_check jazan
# command : D:\dlt\.venv\Scripts\python.exe dq_check.py --branch jazan --self-test
# started : 2026-07-07T10:00:00
----------------------------------------------------------------------
2026-07-07 10:00:00,100 | INFO    | etl.dq | DQ run dq-x | SELF-TEST | branches=['jazan'] | tables=3 | window=2026-01-01..(per-branch watermark) | hash=True
2026-07-07 10:00:03,200 | INFO    | etl.dq | DQ-UNIT appointments/jazan | ora=2000 ice=2000 cnt=0 | match=1992 delta=8 pct=0.40 | WITHIN_TOLERANCE
2026-07-07 10:00:05,000 | INFO    | etl.dq | DQ-PROGRESS 0:00:05 | units 1/3 | ok 0 tol 1 mismatch 0 err 0
2026-07-07 10:00:06,300 | INFO    | etl.dq | DQ-UNIT staff_master/jazan | ora=500 ice=500 cnt=0 | match=500 delta=0 pct=0.00 | OK
2026-07-07 10:00:08,900 | INFO    | etl.dq | DQ-UNIT orders/jazan | ora=1000 ice=850 cnt=150 | match=850 delta=150 pct=15.00 | MISMATCH
2026-07-07 10:00:10,000 | INFO    | etl.dq | DQ-PROGRESS 0:00:10 | units 3/3 | ok 1 tol 1 mismatch 1 err 0
-> wrote 3 row(s) to Iceberg table 'oasis.etl_dq_results'
----------------------------------------------------------------------
[runner] exited with code 0
```

Expected: a "DQ reconciliation" card — bar at 3/3 · 100%; tallies `1 ok`, `1 tol` (amber), `1 mismatch` (red); scope line `branches: jazan · tables: 3 · hash: True · window: …`; a 3-row units table sorted MISMATCH → WITHIN_TOLERANCE → OK, with `orders` showing `15.00%` and a red `MISMATCH` pill, `appointments` an amber `WITHIN_TOLERANCE` pill.

- [ ] **Step 3: Manual verify — live DQ run (optional, needs Oracle)**

On the Run page, build a `dq_check` command for one branch and **Run now**. Expected: the DQ card fills as `DQ-UNIT` lines stream; the bar and tallies advance on each `DQ-PROGRESS`. (Skip if no Oracle connectivity — Step 2 covers parsing.)

- [ ] **Step 4: Commit**

```bash
git add gui/static/runparse.js
git commit -m "feat(gui): live dq_check reconciliation dashboard"
```

---

## Task 8: Snapshot summary polish + Logs page summary-first

**Files:**
- Modify: `gui/templates/logs.html` (`openFile` summary-first collapse)
- Modify: `gui/templates/run.html` (optional dq `--no-progress` checkbox)

**Interfaces:**
- Consumes: `createLogDash` (via `fileDash`, already wired in `logs.html`); the generic snapshot view (Task 6).

The snapshot parser already lives in `makeGenericView("snapshot")` (Task 6). This task verifies it end-to-end and makes the Logs page summary-first.

- [ ] **Step 1: Summary-first collapse on file open**

In `gui/templates/logs.html`, replace the `openFile` function (around lines 240-245) with:

```javascript
async function openFile(name) {
  curFile = name;
  el("file-title").textContent = name;
  $$("#files-table tr.clickable").forEach(tr => tr.classList.toggle("selected", tr.dataset.f === name));
  await refreshFile();
  // Summary-first: when a parsed summary is available, collapse the raw log by
  // default (the "Raw log" button reveals it). Fall back to raw when there's none.
  el("file-panel").classList.toggle("raw-hidden", !el("run-dash").hidden);
}
```

(Leave `refreshFile` unchanged so auto-refresh does not fight the user's Raw-log toggle.)

- [ ] **Step 2: Optional — a dq `--no-progress` checkbox in the builder**

In `gui/templates/run.html`, inside the `data-grp="dq_check"` block (after the `no_write` checkbox, around line 52), add:

```html
      <div class="checkbox"><input type="checkbox" id="no_progress_d"><label for="no_progress_d">--no-progress (suppress DQ heartbeat)</label></div>
```

In `buildSpec()` (the `dq_check` branch, around line 195) add:

```javascript
    spec.no_progress = el("no_progress_d").checked;
```

In `loadSpec()` (the `dq_check` branch, around line 466) add:

```javascript
    el("no_progress_d").checked = !!spec.no_progress;
```

In `gui/commands.py`, in the `elif script == "dq_check":` block (around line 73), add:

```python
        if spec.get("no_progress"):
            argv.append("--no-progress")
```

- [ ] **Step 3: Manual verify — snapshot summary**

Create `run_logs/run-sample-snap.log` with this content and select it on the Logs page:

```text
# OASIS run sample-snap
# label   : snapshot_diff delivery_charge
# command : D:\dlt\.venv\Scripts\python.exe snapshot_diff.py --table delivery_charge
# started : 2026-07-07T06:00:00
----------------------------------------------------------------------
Table         : delivery_charge
Baseline (as-of 2026-07-06) : snapshot 111 @ 2026-07-06 23:00:00
Latest                   : snapshot 222 @ 2026-07-07 06:00:00
Identity      : (branch_id, delivery_charge_id) | 12 business columns compared
Updated : 5   Inserted : 2   Deleted : 1
-> wrote exports/delivery_charge_changes_2026-07-06_vs_222.xlsx
----------------------------------------------------------------------
[runner] exited with code 0
```

Expected: a "Log summary" card titled `snapshot_diff.py`, status `finished rc=0`, a key/value block (table, baseline, latest, identity), tally chips `5 updated` (amber) / `2 inserted` (green) / `1 deleted` (red), and the `wrote …xlsx` key line. The **raw log is collapsed by default**; clicking **Raw log** reveals it.

- [ ] **Step 4: Manual verify — dq `--no-progress` preview**

On the Run page, pick script `dq_check`, tick **--no-progress**. Expected: the command preview ends with `--no-progress`.

- [ ] **Step 5: Commit**

```bash
git add gui/templates/logs.html gui/templates/run.html gui/commands.py
git commit -m "feat(gui): summary-first logs + snapshot summary + dq --no-progress toggle"
```

---

## Task 9: Full-suite regression + cleanup

**Files:** none (verification only)

- [ ] **Step 1: Run the whole backend test suite**

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass (existing + the three new files). No regressions in `test_snapshot_tables.py`, `test_app_runs_endpoint.py`, etc.

- [ ] **Step 2: Remove the sample log fixtures**

The sample files under `run_logs/` were manual-verification scratch, not committed artifacts. Delete them:

Run: `rm -f run_logs/run-sample-custom.log run_logs/run-sample-dq.log run_logs/run-sample-snap.log`
Expected: gone. (`run_logs/` is runtime output; confirm it is not tracked with `git status`.)

- [ ] **Step 3: Sanity-check the live Run page once more**

Start the GUI, run any quick command (e.g. `oracle_to_iceberg --self-test` if available, or a `custom` echo). Expected: the correct per-type card appears live; the run history and raw log still work; no console errors.

- [ ] **Step 4: Final commit (if anything staged)**

```bash
git add -A
git commit -m "chore(dq): finalize DQ tolerance + observability feature" || echo "nothing to commit"
```

---

## Self-review notes

- **Spec coverage:** Part 1 → Tasks 1-4; shared refactor → Task 6; Part 2 (backend progress) → Task 5, (frontend dq view) → Task 7; Part 3 (per-type summaries) → Tasks 6 (generic/pipeline) + 7 (dq) + 8 (snapshot + summary-first). `hash_delta_pct` column → Task 3; GUI pill/column → Task 4.
- **Line-format contract:** `_DqProgress._unit_line` / `_heartbeat_line` (Task 5, pinned by `tests/test_dq_progress.py`) exactly match the `makeDqView` regexes (Task 7): `DQ-UNIT <t>/<b> | ora= ice= cnt= | match= delta= pct= | STATUS` and `DQ-PROGRESS H:MM:SS | units d/t | ok tol mismatch err`.
- **Type consistency:** `classify_status` returns `(status, hash_delta_pct)` and is the only status writer; `STATUS_WITHIN_TOLERANCE == "WITHIN_TOLERANCE"` lowercases to CSS `within_tolerance` (Task 4). `DqResult.hash_delta_pct` is set in Task 2, read in Tasks 3 & 5.
