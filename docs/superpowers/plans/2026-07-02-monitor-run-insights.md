# Monitor "Runs" per-run insights — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only "Runs" tab to the Monitor page that rolls `etl_run_log` up by `pipeline_run_id` into per-run summary cards with drill-down detail, folding in each unit's current `etl_control` watermark.

**Architecture:** Group `etl_run_log` server-side in `gui/iceberg_browser.py`. Each data-access function splits into a **pure transform** (grouping / join over plain dicts — unit-testable without pyiceberg) and a thin **I/O wrapper** that scans the Iceberg table and delegates. Two Flask routes expose the data; a new tab in `gui/templates/logs.html` renders rollup cards (`<details>`) that lazy-load detail.

**Tech Stack:** Python 3.10+ (pyarrow via existing lazy `pyiceberg` access), Flask, vanilla JS templates (Jinja), existing shared helpers in `gui/static/app.js`.

## Global Constraints

- **Read-only.** No write/delete/mutation of any Iceberg table or state.
- **No new dependencies.** Reuse the lazy `pyiceberg` access already in `iceberg_browser.py` and the JS helpers in `app.js` (`apiGet`, `esc`, `fmtNum`, `fmtDate`, `renderTable`, `pill`).
- **Modern typing**, matching the module: `from __future__ import annotations`, `dict[str, Any]`, `int | None`.
- **Follow existing patterns:** mirror `read_system_table` (data access), `api_ib_system` + the `@api` decorator (routes), and the `SysTable` tab wiring (template).
- **Missing-table friendliness:** a not-yet-created `etl_run_log` / `etl_control` yields an empty, well-formed payload (`{"runs": []}` / empty `rows`) — never a 500. (`etl_control` missing during detail = all-null control columns.)
- **etl_run_log columns** (source of truth, from `etl/iceberg_load.py:_log_rows`): `pipeline_run_id, table_name, branch_id, load_mode, row_count, start_time, end_time, duration_ms, status, attempts, write_disposition, load_status, error_details, schema_discrepancy, recorded_at`. `status == "SUCCESS"` means the unit succeeded.
- **etl_control columns** (from `etl/iceberg_load.py:_control_rows`, latest per `(table_name, branch_id)`): `table_name, branch_id, status, last_cdc_value, last_date_value, updated_at, ...`.

---

## File Structure

- **Modify** `gui/iceberg_browser.py` — add pure transforms `_summarize_runs`, `_control_index`, `_run_detail_rows`, the `RUN_DETAIL_COLUMNS` constant, and I/O wrappers `_scan_pylist`, `read_run_summary`, `read_run_detail`.
- **Modify** `gui/app.py` — add routes `GET /api/iceberg/runs` and `GET /api/iceberg/runs/<run_id>`.
- **Modify** `gui/templates/logs.html` — add the "Runs" tab button, panel, and `runsView` script; wire into `showTab`.
- **Modify** `gui/static/style.css` — add `.run-card` / `.run-badge` styles.
- **Create** `tests/test_iceberg_run_rollup.py` — unit tests for the transforms + wrappers.
- **Create** `tests/test_app_runs_endpoint.py` — Flask route smoke tests.

---

## Task 1: Per-run rollup transform (`_summarize_runs`)

**Files:**
- Modify: `gui/iceberg_browser.py` (add function near the other data-access helpers, after `read_system_table`)
- Test: `tests/test_iceberg_run_rollup.py`

**Interfaces:**
- Consumes: `_jsonable(v)` (existing, `gui/iceberg_browser.py:210`).
- Produces: `_summarize_runs(rows: list[dict], limit_runs: int = 100) -> list[dict]`. Each output dict has keys: `run_id, load_mode, start_time, end_time, duration_wall_ms, rows_total, units, ok, failed, schema_drift, errors, tables`. `start_time`/`end_time` are ISO strings (or `None`); `duration_wall_ms` is `int` ms or `None`. Newest run first.

- [ ] **Step 1: Write the failing test**

Create `tests/test_iceberg_run_rollup.py`:

```python
"""Tests for the Monitor 'Runs' rollup transforms in iceberg_browser."""
from __future__ import annotations

import datetime as dt

import iceberg_browser as ib


def _log_row(run="r1", table="APPT", branch=1, status="SUCCESS", rows=100,
             start="2026-07-02 06:00:00", end="2026-07-02 06:05:00",
             mode="INCREMENTAL", drift=None, err=None, recorded="2026-07-02 06:05:01"):
    def _p(s):
        return dt.datetime.fromisoformat(s) if s else None
    return {
        "pipeline_run_id": run, "table_name": table, "branch_id": branch,
        "load_mode": mode, "row_count": rows, "status": status,
        "start_time": _p(start), "end_time": _p(end),
        "schema_discrepancy": drift, "error_details": err, "recorded_at": _p(recorded),
    }


def test_summarize_groups_by_run():
    rows = [
        _log_row(run="r1", table="APPT", branch=1, rows=100),
        _log_row(run="r1", table="APPT", branch=2, rows=50),
        _log_row(run="r1", table="VISITS", branch=1, rows=25),
        _log_row(run="r2", table="APPT", branch=1, rows=10),
    ]
    out = ib._summarize_runs(rows)
    by_id = {r["run_id"]: r for r in out}
    assert set(by_id) == {"r1", "r2"}
    assert by_id["r1"]["units"] == 3
    assert by_id["r1"]["ok"] == 3
    assert by_id["r1"]["failed"] == 0
    assert by_id["r1"]["rows_total"] == 175
    assert by_id["r1"]["tables"] == 2
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_iceberg_run_rollup.py::test_summarize_groups_by_run -v`
Expected: FAIL with `AttributeError: module 'iceberg_browser' has no attribute '_summarize_runs'`

- [ ] **Step 3: Implement `_summarize_runs`**

Add to `gui/iceberg_browser.py` (after `read_system_table`, before the "Data access" section end):

```python
def _summarize_runs(rows: list[dict], limit_runs: int = 100) -> list[dict]:
    """Group etl_run_log rows by pipeline_run_id into per-run summaries.

    Duration is *wall clock* (max end_time - min start_time), NOT the sum of
    per-unit duration_ms. Returns the newest ``limit_runs`` runs, newest first.
    """
    groups: dict[Any, list[dict]] = {}
    for r in rows:
        groups.setdefault(r.get("pipeline_run_id"), []).append(r)

    ranked: list[tuple[float, dict]] = []
    for run_id, grp in groups.items():
        starts = [r["start_time"] for r in grp if r.get("start_time") is not None]
        ends = [r["end_time"] for r in grp if r.get("end_time") is not None]
        start = min(starts) if starts else None
        end = max(ends) if ends else None
        duration_wall_ms = (
            int((end - start).total_seconds() * 1000) if (start and end) else None
        )
        ok = sum(1 for r in grp if r.get("status") == "SUCCESS")
        recorded = [r["recorded_at"] for r in grp if r.get("recorded_at") is not None]
        sort_dt = start or (max(recorded) if recorded else None)
        sort_ts = sort_dt.timestamp() if sort_dt is not None else float("-inf")
        ranked.append((sort_ts, {
            "run_id": run_id,
            "load_mode": next((r.get("load_mode") for r in grp if r.get("load_mode")), None),
            "start_time": _jsonable(start),
            "end_time": _jsonable(end),
            "duration_wall_ms": duration_wall_ms,
            "rows_total": sum(int(r.get("row_count") or 0) for r in grp),
            "units": len(grp),
            "ok": ok,
            "failed": len(grp) - ok,
            "schema_drift": sum(1 for r in grp if r.get("schema_discrepancy")),
            "errors": sum(1 for r in grp if r.get("error_details")),
            "tables": len({r.get("table_name") for r in grp}),
        }))

    ranked.sort(key=lambda t: t[0], reverse=True)
    return [summary for _, summary in ranked[:limit_runs]]
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_iceberg_run_rollup.py::test_summarize_groups_by_run -v`
Expected: PASS

- [ ] **Step 5: Add the remaining rollup tests**

Append to `tests/test_iceberg_run_rollup.py`:

```python
def test_wall_clock_duration_not_summed():
    # Two 5-minute units that overlap: wall clock is 10 min, summed would be 10 min too,
    # so stagger them. Unit A 06:00-06:05, Unit B 06:03-06:12 -> wall clock = 12 min.
    rows = [
        _log_row(branch=1, start="2026-07-02 06:00:00", end="2026-07-02 06:05:00"),
        _log_row(branch=2, start="2026-07-02 06:03:00", end="2026-07-02 06:12:00"),
    ]
    out = ib._summarize_runs(rows)
    assert out[0]["duration_wall_ms"] == 12 * 60 * 1000  # 06:00 -> 06:12


def test_failed_drift_and_error_counts():
    rows = [
        _log_row(branch=1, status="SUCCESS", drift=None, err=None),
        _log_row(branch=2, status="FAILED", drift=None, err="boom"),
        _log_row(branch=3, status="SUCCESS", drift='{"added": ["x"]}', err=None),
    ]
    out = ib._summarize_runs(rows)
    r = out[0]
    assert r["ok"] == 2
    assert r["failed"] == 1
    assert r["schema_drift"] == 1
    assert r["errors"] == 1


def test_newest_first_and_limit():
    rows = [
        _log_row(run="old", start="2026-07-01 06:00:00", end="2026-07-01 06:05:00"),
        _log_row(run="new", start="2026-07-02 06:00:00", end="2026-07-02 06:05:00"),
        _log_row(run="mid", start="2026-07-01 18:00:00", end="2026-07-01 18:05:00"),
    ]
    out = ib._summarize_runs(rows, limit_runs=2)
    assert [r["run_id"] for r in out] == ["new", "mid"]


def test_null_times_are_tolerated():
    rows = [_log_row(start=None, end=None, recorded=None)]
    out = ib._summarize_runs(rows)
    assert out[0]["duration_wall_ms"] is None
    assert out[0]["start_time"] is None
```

- [ ] **Step 6: Run the full rollup test file**

Run: `python -m pytest tests/test_iceberg_run_rollup.py -v`
Expected: PASS (5 tests)

- [ ] **Step 7: Commit**

```bash
git add gui/iceberg_browser.py tests/test_iceberg_run_rollup.py
git commit -m "feat(gui): add _summarize_runs rollup for etl_run_log"
```

---

## Task 2: Detail + control-join transform (`_run_detail_rows`)

**Files:**
- Modify: `gui/iceberg_browser.py` (add constant + two functions after `_summarize_runs`)
- Test: `tests/test_iceberg_run_rollup.py` (append)

**Interfaces:**
- Consumes: `_jsonable(v)` (existing).
- Produces:
  - `RUN_DETAIL_COLUMNS: list[str]` — the ordered column list for the detail table.
  - `_control_index(control_rows: list[dict]) -> dict[tuple, dict]` — keyed by `(table_name, branch_id)`.
  - `_run_detail_rows(log_rows: list[dict], control_rows: list[dict]) -> list[dict]` — one dict per log row (keys = `RUN_DETAIL_COLUMNS`), control watermark folded in (`control_status, last_cdc_value, last_date_value, control_updated_at`), failed rows first then table/branch.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_iceberg_run_rollup.py`:

```python
def _ctrl_row(table="APPT", branch=1, status="OK", cdc="500", date="2026-07-02",
              updated="2026-07-02 06:05:00"):
    return {
        "table_name": table, "branch_id": branch, "status": status,
        "last_cdc_value": cdc, "last_date_value": date,
        "updated_at": dt.datetime.fromisoformat(updated),
    }


def test_detail_joins_control_watermark():
    logs = [_log_row(table="APPT", branch=1, status="SUCCESS")]
    ctrl = [_ctrl_row(table="APPT", branch=1, status="OK", cdc="500")]
    out = ib._run_detail_rows(logs, ctrl)
    assert len(out) == 1
    row = out[0]
    assert set(ib.RUN_DETAIL_COLUMNS).issubset(row.keys())
    assert row["control_status"] == "OK"
    assert row["last_cdc_value"] == "500"


def test_detail_null_control_when_no_match():
    logs = [_log_row(table="APPT", branch=9, status="SUCCESS")]
    out = ib._run_detail_rows(logs, [])  # no control rows at all
    assert out[0]["control_status"] is None
    assert out[0]["last_cdc_value"] is None
    assert out[0]["control_updated_at"] is None


def test_detail_failed_rows_first():
    logs = [
        _log_row(table="APPT", branch=1, status="SUCCESS"),
        _log_row(table="APPT", branch=2, status="FAILED"),
    ]
    out = ib._run_detail_rows(logs, [])
    assert out[0]["status"] == "FAILED"
    assert out[1]["status"] == "SUCCESS"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_iceberg_run_rollup.py -k detail -v`
Expected: FAIL with `AttributeError: module 'iceberg_browser' has no attribute 'RUN_DETAIL_COLUMNS'` (or `_run_detail_rows`)

- [ ] **Step 3: Implement the constant and functions**

Add to `gui/iceberg_browser.py` after `_summarize_runs`:

```python
RUN_DETAIL_COLUMNS = [
    "table_name", "branch_id", "load_mode", "status", "row_count", "duration_ms",
    "write_disposition", "attempts", "control_status", "last_cdc_value",
    "last_date_value", "control_updated_at", "start_time", "end_time",
    "schema_discrepancy", "error_details",
]


def _control_index(control_rows: list[dict]) -> dict[tuple, dict]:
    """Index the latest etl_control rows by (table_name, branch_id)."""
    return {(r.get("table_name"), r.get("branch_id")): r for r in control_rows}


def _run_detail_rows(log_rows: list[dict], control_rows: list[dict]) -> list[dict]:
    """One row per etl_run_log unit, joined to its current etl_control watermark.

    Failed units sort first, then by table then branch. Control columns are None
    when etl_control has no matching (table_name, branch_id).
    """
    idx = _control_index(control_rows)
    out: list[dict] = []
    for r in log_rows:
        c = idx.get((r.get("table_name"), r.get("branch_id"))) or {}
        out.append({
            "table_name": r.get("table_name"),
            "branch_id": r.get("branch_id"),
            "load_mode": r.get("load_mode"),
            "status": r.get("status"),
            "row_count": r.get("row_count"),
            "duration_ms": r.get("duration_ms"),
            "write_disposition": r.get("write_disposition"),
            "attempts": r.get("attempts"),
            "control_status": c.get("status"),
            "last_cdc_value": c.get("last_cdc_value"),
            "last_date_value": c.get("last_date_value"),
            "control_updated_at": _jsonable(c.get("updated_at")),
            "start_time": _jsonable(r.get("start_time")),
            "end_time": _jsonable(r.get("end_time")),
            "schema_discrepancy": r.get("schema_discrepancy"),
            "error_details": r.get("error_details"),
        })
    out.sort(key=lambda d: (
        d["status"] == "SUCCESS",
        str(d["table_name"] or ""),
        d["branch_id"] is None,
        d["branch_id"] or 0,
    ))
    return out
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_iceberg_run_rollup.py -k detail -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add gui/iceberg_browser.py tests/test_iceberg_run_rollup.py
git commit -m "feat(gui): add _run_detail_rows control-watermark join"
```

---

## Task 3: I/O wrappers (`read_run_summary`, `read_run_detail`)

**Files:**
- Modify: `gui/iceberg_browser.py` (add after the transforms)
- Test: `tests/test_iceberg_run_rollup.py` (append)

**Interfaces:**
- Consumes: `_open_static(table)` (existing, `gui/iceberg_browser.py:200`, raises `FileNotFoundError` when the table has no metadata), `_summarize_runs`, `_run_detail_rows`, `RUN_DETAIL_COLUMNS`.
- Produces:
  - `_scan_pylist(table: str) -> list[dict]` — opens a static Iceberg table and returns `to_arrow().to_pylist()` (timestamps arrive as Python `datetime`).
  - `read_run_summary(limit_runs: int = 100) -> dict` → `{"runs": [...]}`; `{"runs": []}` when `etl_run_log` is missing.
  - `read_run_detail(run_id: str) -> dict` → `{"run_id", "columns": RUN_DETAIL_COLUMNS, "rows": [...]}`; empty `rows` when `etl_run_log` missing; all-null control columns when `etl_control` missing.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_iceberg_run_rollup.py`:

```python
def test_read_run_summary_uses_scan(monkeypatch):
    rows = [_log_row(run="r1", branch=1), _log_row(run="r1", branch=2)]
    monkeypatch.setattr(ib, "_scan_pylist", lambda table: rows)
    out = ib.read_run_summary()
    assert out["runs"][0]["run_id"] == "r1"
    assert out["runs"][0]["units"] == 2


def test_read_run_summary_missing_table(monkeypatch):
    def boom(table):
        raise FileNotFoundError(table)
    monkeypatch.setattr(ib, "_scan_pylist", boom)
    assert ib.read_run_summary() == {"runs": []}


def test_read_run_detail_filters_and_joins(monkeypatch):
    logs = [
        _log_row(run="r1", table="APPT", branch=1),
        _log_row(run="r2", table="APPT", branch=1),  # different run, must be excluded
    ]
    ctrl = [_ctrl_row(table="APPT", branch=1, cdc="777")]

    def fake_scan(table):
        return {"etl_run_log": logs, "etl_control": ctrl}[table]
    monkeypatch.setattr(ib, "_scan_pylist", fake_scan)

    out = ib.read_run_detail("r1")
    assert out["run_id"] == "r1"
    assert out["columns"] == ib.RUN_DETAIL_COLUMNS
    assert len(out["rows"]) == 1
    assert out["rows"][0]["last_cdc_value"] == "777"


def test_read_run_detail_missing_control(monkeypatch):
    logs = [_log_row(run="r1", table="APPT", branch=1)]

    def fake_scan(table):
        if table == "etl_control":
            raise FileNotFoundError(table)
        return logs
    monkeypatch.setattr(ib, "_scan_pylist", fake_scan)

    out = ib.read_run_detail("r1")
    assert len(out["rows"]) == 1
    assert out["rows"][0]["control_status"] is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_iceberg_run_rollup.py -k "read_run" -v`
Expected: FAIL (`AttributeError: ... has no attribute 'read_run_summary'`)

- [ ] **Step 3: Implement the wrappers**

Add to `gui/iceberg_browser.py` after the transforms:

```python
def _scan_pylist(table: str) -> list[dict]:
    """Whole system table as a list of plain dicts (timestamps -> datetime)."""
    tbl = _open_static(table)
    return tbl.scan().to_arrow().to_pylist()


def read_run_summary(limit_runs: int = 100) -> dict[str, Any]:
    """Per-run rollup of etl_run_log, newest first. Empty when the table is absent."""
    try:
        rows = _scan_pylist("etl_run_log")
    except FileNotFoundError:
        return {"runs": []}
    return {"runs": _summarize_runs(rows, limit_runs=limit_runs)}


def read_run_detail(run_id: str) -> dict[str, Any]:
    """One run's units joined to the current etl_control watermark."""
    try:
        log_rows = [
            r for r in _scan_pylist("etl_run_log")
            if r.get("pipeline_run_id") == run_id
        ]
    except FileNotFoundError:
        log_rows = []
    try:
        control_rows = _scan_pylist("etl_control")
    except FileNotFoundError:
        control_rows = []
    return {
        "run_id": run_id,
        "columns": RUN_DETAIL_COLUMNS,
        "rows": _run_detail_rows(log_rows, control_rows),
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_iceberg_run_rollup.py -v`
Expected: PASS (all tests in the file)

- [ ] **Step 5: Commit**

```bash
git add gui/iceberg_browser.py tests/test_iceberg_run_rollup.py
git commit -m "feat(gui): read_run_summary/read_run_detail with missing-table guard"
```

---

## Task 4: Flask routes

**Files:**
- Modify: `gui/app.py` (add two routes next to `api_ib_system`, after line 469)
- Test: `tests/test_app_runs_endpoint.py`

**Interfaces:**
- Consumes: `iceberg_browser.read_run_summary(limit_runs=...)`, `iceberg_browser.read_run_detail(run_id)`, the `@api` decorator, Flask `request`/`jsonify` (all already imported in `gui/app.py`).
- Produces: `GET /api/iceberg/runs?limit=100` → summary JSON; `GET /api/iceberg/runs/<run_id>` → detail JSON.

- [ ] **Step 1: Write the failing test**

Create `tests/test_app_runs_endpoint.py`:

```python
"""Smoke tests for the Runs API routes."""
from __future__ import annotations

import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(
        gui_app.iceberg_browser, "read_run_summary",
        lambda limit_runs=100: {"runs": [{"run_id": "r1", "rows_total": 5}]},
    )
    monkeypatch.setattr(
        gui_app.iceberg_browser, "read_run_detail",
        lambda run_id: {"run_id": run_id, "columns": ["table_name"], "rows": []},
    )
    return gui_app.app.test_client()


def test_runs_summary_route(client):
    resp = client.get("/api/iceberg/runs")
    assert resp.status_code == 200
    assert resp.get_json()["runs"][0]["run_id"] == "r1"


def test_runs_detail_route(client):
    resp = client.get("/api/iceberg/runs/abc123")
    assert resp.status_code == 200
    assert resp.get_json()["run_id"] == "abc123"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_app_runs_endpoint.py -v`
Expected: FAIL — 404 JSON `{"error": "not found: ..."}` because the routes don't exist yet (Flask returns 404 for the unknown path, so `test_runs_summary_route` fails on the JSON assertion / status).

- [ ] **Step 3: Add the routes**

In `gui/app.py`, immediately after the `api_ib_system` function (after line 469):

```python
@app.get("/api/iceberg/runs")
@api
def api_ib_runs():
    limit = request.args.get("limit", 100, type=int)
    return jsonify(iceberg_browser.read_run_summary(limit_runs=min(limit, 500)))


@app.get("/api/iceberg/runs/<run_id>")
@api
def api_ib_run_detail(run_id):
    return jsonify(iceberg_browser.read_run_detail(run_id))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_app_runs_endpoint.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add gui/app.py tests/test_app_runs_endpoint.py
git commit -m "feat(gui): add /api/iceberg/runs summary + detail routes"
```

---

## Task 5: "Runs" tab UI (template + styles)

**Files:**
- Modify: `gui/templates/logs.html` (tab button, panel, `runsView` script, `showTab` wiring)
- Modify: `gui/static/style.css` (append `.run-card` / `.run-badge` rules)

**Interfaces:**
- Consumes: `GET /api/iceberg/runs`, `GET /api/iceberg/runs/<run_id>`; JS helpers `apiGet`, `esc`, `fmtNum`, `fmtDate`, `renderTable`, `el`, `$$` (from `gui/static/app.js`).
- Produces: a new `Runs` tab. No downstream consumers (terminal UI task).

This task has no automated test harness (the repo has no JS test runner). It ends with a **manual verification** step.

- [ ] **Step 1: Add the tab button**

In `gui/templates/logs.html`, in the `<div class="row-flex">` tab row (around line 8-13), add the Runs tab after the DQ results tab:

```html
  <button class="btn tab" data-tab="dq">DQ results</button>
  <button class="btn tab" data-tab="runs">Runs</button>
  <button class="btn tab" data-tab="runlog">ETL run log</button>
```

- [ ] **Step 2: Add the Runs panel**

In `gui/templates/logs.html`, add this `<section>` after the `dq` panel section (after the `</section>` that closes `data-panel="dq"`, before `data-panel="runlog"`):

```html
<section data-panel="runs" hidden>
  <div class="panel">
    <div class="panel-head">
      <h2>Runs <small id="runs-stat" class="muted"></small></h2>
      <button class="btn sm ghost" id="refresh-runs">↻</button>
    </div>
    <div class="filter-bar" id="runs-bar"></div>
    <div id="runs-list"></div>
  </div>
</section>
```

- [ ] **Step 3: Add the `runsView` script**

In `gui/templates/logs.html`, inside the `{% block scripts %}` `<script>`, add before the final wiring (before the `$$(".tab").forEach(...)` line near the bottom):

```javascript
// ---------- Runs rollup (per pipeline_run_id) ----------
function fmtDur(ms) {
  if (ms === null || ms === undefined) return "—";
  let s = Math.round(ms / 1000);
  const h = Math.floor(s / 3600); s -= h * 3600;
  const m = Math.floor(s / 60); s -= m * 60;
  return (h ? `${h}h ` : "") + (h || m ? `${m}m ` : "") + `${s}s`;
}
function runBadge(n, label, cls) {
  return n ? `<span class="run-badge ${cls}">${n} ${label}</span>` : "";
}
const RUN_DETAIL_PILLS = ["status", "control_status"];
const RUN_DETAIL_NUMS  = ["row_count", "duration_ms", "attempts"];

const runsView = (function () {
  let RUNS = [];
  const state = { mode: "", from: "", to: "" };

  function buildBar() {
    const modes = [...new Set(RUNS.map(r => r.load_mode).filter(Boolean))].sort();
    const bar = el("runs-bar");
    bar.innerHTML =
      `<div><label>Mode</label><select data-f="mode"><option value="">All</option>` +
        modes.map(m => `<option value="${esc(m)}">${esc(m)}</option>`).join("") + `</select></div>` +
      `<div><label>From</label><input type="date" data-f="from"></div>` +
      `<div><label>To</label><input type="date" data-f="to"></div>` +
      `<button class="btn ghost sm" data-f="clear" style="align-self:flex-end">Clear</button>` +
      `<span class="spacer"></span><span class="muted" data-f="count"></span>`;
    bar.querySelectorAll("select,input").forEach(n =>
      n.addEventListener("input", () => { state[n.dataset.f] = n.value; render(); }));
    bar.querySelector('[data-f="clear"]').onclick = () => {
      state.mode = state.from = state.to = "";
      bar.querySelectorAll("select,input").forEach(n => n.value = "");
      render();
    };
  }

  function filtered() {
    return RUNS.filter(r => {
      if (state.mode && r.load_mode !== state.mode) return false;
      const d = (r.start_time || "").slice(0, 10);
      if (state.from && d && d < state.from) return false;
      if (state.to && d && d > state.to) return false;
      return true;
    });
  }

  function card(r) {
    const badges =
      `<span class="run-badge ok">${r.ok} ok</span>` +
      runBadge(r.failed, "failed", "failed") +
      runBadge(r.schema_drift, "drift", "warn") +
      runBadge(r.errors, "err", "warn");
    return `<details class="run-card" data-run="${esc(r.run_id)}">
      <summary>
        <span class="run-head">
          <span class="run-rows">${fmtNum(r.rows_total)} rows</span>
          <span class="rd-sep">·</span><span>${fmtDur(r.duration_wall_ms)}</span>
          ${badges}
        </span>
        <span class="run-meta muted">${esc(r.load_mode || "—")} · ${esc(fmtDate(r.start_time))} · ${r.tables} tables</span>
      </summary>
      <div class="run-detail"><div class="muted">Loading…</div></div>
    </details>`;
  }

  async function loadDetail(d) {
    if (d.dataset.loaded) return;
    d.dataset.loaded = "1";
    const box = d.querySelector(".run-detail");
    try {
      const r = await apiGet(`/api/iceberg/runs/${encodeURIComponent(d.dataset.run)}`);
      box.innerHTML = renderTable(r.columns, r.rows, { pillCols: RUN_DETAIL_PILLS, numCols: RUN_DETAIL_NUMS });
    } catch (e) {
      d.dataset.loaded = "";
      box.innerHTML = `<div class="banner warn">${esc(e.message)}</div>`;
    }
  }

  function render() {
    const rows = filtered();
    el("runs-list").innerHTML = rows.map(card).join("") || `<div class="muted">No runs yet.</div>`;
    const cnt = el("runs-bar").querySelector('[data-f="count"]');
    if (cnt) cnt.textContent = `${rows.length} of ${RUNS.length} runs`;
    $$("#runs-list details.run-card").forEach(d =>
      d.addEventListener("toggle", () => { if (d.open) loadDetail(d); }));
  }

  async function load() {
    try {
      const data = await apiGet("/api/iceberg/runs?limit=100");
      RUNS = data.runs || [];
      el("runs-stat").textContent =
        RUNS.length ? `${RUNS.length} runs · last ${fmtDate(RUNS[0].start_time)}` : "";
      buildBar();
      render();
    } catch (e) {
      el("runs-list").innerHTML = `<div class="banner warn">${esc(e.message)}</div>`;
    }
  }
  return { load };
})();
```

- [ ] **Step 4: Wire `runsView` into `showTab` and the refresh button**

In `gui/templates/logs.html`, in `showTab(tab)`, add a line alongside the other loaders:

```javascript
  if (tab === "runs") runsView.load();
  if (tab === "dq") dqView.load();
```

And near the other `el(...).onclick` handlers add:

```javascript
el("refresh-runs").onclick = () => runsView.load();
```

- [ ] **Step 5: Add the CSS**

Append to `gui/static/style.css`:

```css
/* Monitor: per-run rollup cards */
.run-card { background: var(--surface-container); border: 1px solid var(--border-soft); border-radius: var(--radius-md); padding: 8px 12px; margin-bottom: 8px; }
.run-card > summary { cursor: pointer; display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; list-style: none; }
.run-card > summary::-webkit-details-marker { display: none; }
.run-head { display: flex; align-items: center; gap: 8px; font-weight: 600; }
.run-rows { font-weight: 700; }
.run-detail { margin-top: 10px; }
.run-badge { display: inline-block; padding: 2px 8px; border-radius: var(--radius-full); font-size: 11px; font-weight: 700; }
.run-badge.ok { background: var(--success-light); color: #047857; }
.run-badge.failed { background: var(--error-light); color: #b91c1c; }
.run-badge.warn { background: var(--warning-light); color: #b45309; }
```

- [ ] **Step 6: Manual verification**

Confirm the full test suite still passes and the tab works end to end:

```bash
python -m pytest tests/test_iceberg_run_rollup.py tests/test_app_runs_endpoint.py -v
```
Expected: all PASS.

Then launch the GUI and check the tab:
- Run `python gui/app.py` and open `http://127.0.0.1:8765/logs`.
- Click the **Runs** tab. Expected: a `Runs` header stat, a filter bar (Mode / From / To / Clear), and one card per run showing `<rows> rows · <duration>` plus `N ok` (and `failed`/`drift`/`err` badges only when non-zero).
- Expand a run: the detail table lazy-loads with per-`(table, branch)` rows, status pills, and the folded-in `control_status` / `last_cdc_value` / `last_date_value` / `control_updated_at` columns.
- On a fresh lake with no `etl_run_log` yet, the tab shows "No runs yet." (not an error).

- [ ] **Step 7: Commit**

```bash
git add gui/templates/logs.html gui/static/style.css
git commit -m "feat(gui): Runs tab on Monitor page with per-run rollup + drill-down"
```

---

## Self-Review

**Spec coverage:**
- Per-run rollup grouped by `pipeline_run_id` → Task 1. ✓
- Rollup metrics rows+duration, ok/failed, drift/errors → Task 1 (`_summarize_runs`) + Task 5 (card). ✓
- Wall-clock (not summed) duration → Task 1 + `test_wall_clock_duration_not_summed`. ✓
- Drill-down detail with control watermark folded in → Task 2 (`_run_detail_rows`) + Task 5 (lazy `<details>`). ✓
- Server-side aggregation, full history (no 2000-row truncation) → Tasks 1-3 scan the whole table. ✓
- Additive fifth tab, other tabs untouched → Task 5 Steps 1-2. ✓
- Missing-table friendliness → Task 3 guards + `test_read_run_summary_missing_table`, `test_read_run_detail_missing_control` + Task 5 "No runs yet." ✓
- Two routes `/api/iceberg/runs` and `/api/iceberg/runs/<run_id>` → Task 4. ✓
- Read-only, no new deps → no write paths added; reuses pyiceberg + app.js. ✓

**Placeholder scan:** No TBD/TODO/"handle edge cases" — every step has concrete code or exact commands. ✓

**Type consistency:** `_summarize_runs(rows, limit_runs)` → dict keys consumed by the card in Task 5 (`rows_total, duration_wall_ms, ok, failed, schema_drift, errors, tables, load_mode, start_time, run_id`) all match. `RUN_DETAIL_COLUMNS` defined in Task 2, returned by `read_run_detail` in Task 3, and rendered via `r.columns` in Task 5 (no client-side column list to drift). `read_run_summary`/`read_run_detail` signatures match the Task 4 route calls (`limit_runs=`, `run_id`). Control-join keys `(table_name, branch_id)` consistent between `_control_index` and `_run_detail_rows`. ✓
