# Staging Layer Table Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-table and delete-all actions for Iceberg staging-layer tables to the GUI, behind typed confirmations, with watermark cleanup and a live-run guard.

**Architecture:** Deletion logic lives in `gui/iceberg_browser.py` (pure filesystem + control_state.json). `gui/app.py` adds two `DELETE` routes that first consult a new `RunManager.has_live_run()` guard and answer 409 while a pipeline run is alive. The Staging Layer Explorer page (`gui/templates/iceberg.html`) gets a trash action per row and a "Delete all…" button, both opening a shared confirmation modal that requires typing the table name (per table) or `DELETE ALL` (bulk).

**Tech Stack:** Flask, vanilla JS (existing `app.js` helpers: `api`, `apiDel`, `ok`, `err`, `esc`, modal CSS classes `.modal-bg`/`.modal`), pytest.

**Spec:** `docs/superpowers/specs/2026-07-14-staging-table-delete-design.md`

## Global Constraints

- `_dlt*` folders (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`) are NEVER deletable, by any path.
- System tables `etl_control`, `etl_run_log`, `etl_dq_results` (config.SYSTEM_TABLES) ARE deletable: per-table always (with confirmation), in delete-all only when `include_system` is true.
- Deleting a table also pops its key from `control_state.json` (atomic tmp-write + `replace`, `indent=1` to match the ETL's format).
- All mutating routes answer 409 with `{"error": ...}` while any pipeline run is alive (status `running`, or `detached` with a live PID).
- Follow the existing code style: module docstrings, `from __future__ import annotations`, the `@api` wrapper for routes, `esc()` for every user-derived string in HTML.
- The working tree has uncommitted user changes in `gui/iceberg_browser.py`, `gui/templates/iceberg.html`, `gui/static/style.css` — edit on top of them; NEVER revert or stash them. Commit only the files each task touches.

---

### Task 1: Backend deletion functions

**Files:**
- Modify: `gui/iceberg_browser.py` (new section after `read_run_detail`, imports at top)
- Test: `tests/test_iceberg_delete.py` (create)

**Interfaces:**
- Consumes: existing `_load_metadata`, `_current_snapshot`, `config.SYSTEM_TABLES`.
- Produces (used by Task 3's routes):
  - `delete_table(table: str) -> dict` — raises `ValueError` (bad/protected name), `FileNotFoundError` (no such table); returns `{"deleted": [name], "watermarks_cleared": [...], "rows": int, "size_bytes": int, "errors": {}}`
  - `delete_all_tables(include_system: bool = False) -> dict` — returns `{"deleted": [...], "watermarks_cleared": [...], "errors": {name: msg}}`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_iceberg_delete.py`:

```python
"""Staging-layer table deletion: path safety, system-table rules, watermarks."""
from __future__ import annotations

import json

import pytest


def _mk_table(root, name, rows=42, size=1000):
    meta_dir = root / name / "metadata"
    meta_dir.mkdir(parents=True)
    (meta_dir / "00001-a.metadata.json").write_text(json.dumps({
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "summary": {
            "total-records": rows, "total-files-size": size}}],
    }), encoding="utf-8")


@pytest.fixture
def lake(tmp_path, monkeypatch):
    import iceberg_browser as ib

    root = tmp_path / "oasis"
    root.mkdir()
    control = tmp_path / "control_state.json"
    monkeypatch.setattr(ib, "ICEBERG_ROOT", root)
    monkeypatch.setattr(ib, "CONTROL_STATE", control)
    return root, control


def test_delete_table_removes_dir_and_watermark(lake):
    import iceberg_browser as ib

    root, control = lake
    _mk_table(root, "patient_ad")
    control.write_text(json.dumps({"patient_ad": {"x": 1}, "other": {}}), encoding="utf-8")

    out = ib.delete_table("patient_ad")

    assert out["deleted"] == ["patient_ad"]
    assert out["watermarks_cleared"] == ["patient_ad"]
    assert out["rows"] == 42
    assert not (root / "patient_ad").exists()
    assert json.loads(control.read_text(encoding="utf-8")) == {"other": {}}


def test_delete_table_without_control_entry(lake):
    import iceberg_browser as ib

    root, _control = lake
    _mk_table(root, "etl_control")
    out = ib.delete_table("etl_control")  # system tables deletable by name
    assert out["deleted"] == ["etl_control"]
    assert out["watermarks_cleared"] == []


@pytest.mark.parametrize("bad", ["_dlt_loads", "_dlt_version", "..", "a/b", "a\\b", ""])
def test_delete_table_rejects_protected_and_unsafe_names(lake, bad):
    import iceberg_browser as ib

    root, _ = lake
    _mk_table(root, "_dlt_loads")
    with pytest.raises(ValueError):
        ib.delete_table(bad)
    assert (root / "_dlt_loads").exists()


def test_delete_table_unknown_is_not_found(lake):
    import iceberg_browser as ib

    with pytest.raises(FileNotFoundError):
        ib.delete_table("nope")


def test_delete_all_skips_system_unless_included(lake):
    import iceberg_browser as ib

    root, control = lake
    for n in ("patient_ad", "doc", "etl_control", "etl_run_log", "_dlt_loads"):
        _mk_table(root, n)
    control.write_text(json.dumps({"patient_ad": {}, "doc": {}}), encoding="utf-8")

    out = ib.delete_all_tables(include_system=False)
    assert sorted(out["deleted"]) == ["doc", "patient_ad"]
    assert (root / "etl_control").exists()
    assert (root / "_dlt_loads").exists()
    assert sorted(out["watermarks_cleared"]) == ["doc", "patient_ad"]

    out2 = ib.delete_all_tables(include_system=True)
    assert sorted(out2["deleted"]) == ["etl_control", "etl_run_log"]
    assert (root / "_dlt_loads").exists()  # never deletable
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `d:\dlt\.venv\Scripts\python.exe -m pytest tests/test_iceberg_delete.py -v`
Expected: FAIL — `AttributeError: ... has no attribute 'CONTROL_STATE'` / `'delete_table'`

- [ ] **Step 3: Implement**

In `gui/iceberg_browser.py`: extend the config import and add a deletion section at the end of the file.

```python
# top of file, replace the existing config import line:
from config import CONTROL_STATE, ICEBERG_ROOT, SYSTEM_TABLES
```

```python
# --------------------------------------------------------------------------- #
# Deletion (per-table drop + delete-all), spec 2026-07-14-staging-table-delete
# --------------------------------------------------------------------------- #
def _clear_control_state(tables: list[str]) -> list[str]:
    """Pop the tables' watermark entries from control_state.json.

    Atomic tmp-write + replace so a crash can't truncate the store. Returns
    the subset of ``tables`` that actually had an entry. A missing or broken
    control_state.json clears nothing (the ETL will rebuild it).
    """
    if not CONTROL_STATE.exists():
        return []
    try:
        state = json.loads(CONTROL_STATE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    cleared = [t for t in tables if t in state]
    if not cleared:
        return []
    for t in cleared:
        state.pop(t)
    tmp = CONTROL_STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=1), encoding="utf-8")
    tmp.replace(CONTROL_STATE)
    return cleared


def _deletable_dir(table: str) -> Path:
    """Validate a table name for deletion and return its directory.

    Rejects path tricks (separators, ``..``), empty names and the ``_dlt*``
    bookkeeping folders. System tables pass — deleting them explicitly is
    allowed; the pipeline recreates them on the next observability write.
    """
    safe = Path(table).name
    if not safe or safe != table or safe.startswith("_dlt"):
        raise ValueError(f"table not deletable: {table!r}")
    return ICEBERG_ROOT / safe


def delete_table(table: str) -> dict[str, Any]:
    """Drop one staging table: its folder AND its control_state watermarks."""
    tdir = _deletable_dir(table)
    if not (tdir / "metadata").is_dir():
        raise FileNotFoundError(table)
    meta = _load_metadata(table)
    summary = (_current_snapshot(meta) or {}).get("summary", {}) if meta else {}
    shutil.rmtree(tdir)
    return {
        "deleted": [table],
        "watermarks_cleared": _clear_control_state([table]),
        "rows": int(summary.get("total-records", 0) or 0),
        "size_bytes": int(summary.get("total-files-size", 0) or 0),
        "errors": {},
    }


def delete_all_tables(include_system: bool = False) -> dict[str, Any]:
    """Drop every staging table (system tables only when ``include_system``).

    ``_dlt*`` folders are always kept. Per-table failures (e.g. a file locked
    on Windows) are collected in ``errors`` instead of aborting the sweep.
    """
    deleted: list[str] = []
    errors: dict[str, str] = {}
    if ICEBERG_ROOT.is_dir():
        for child in sorted(ICEBERG_ROOT.iterdir()):
            name = child.name
            if name.startswith("_dlt") or not (child / "metadata").is_dir():
                continue
            if name in SYSTEM_TABLES and not include_system:
                continue
            try:
                shutil.rmtree(child)
                deleted.append(name)
            except OSError as exc:
                errors[name] = str(exc)
    return {
        "deleted": deleted,
        "watermarks_cleared": _clear_control_state(deleted),
        "errors": errors,
    }
```

Also add `import shutil` to the stdlib imports at the top.

- [ ] **Step 4: Run tests to verify they pass**

Run: `d:\dlt\.venv\Scripts\python.exe -m pytest tests/test_iceberg_delete.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_iceberg_delete.py gui/iceberg_browser.py
git commit -m "feat(gui): backend deletion for staging-layer tables"
```

---

### Task 2: RunManager.has_live_run()

**Files:**
- Modify: `gui/pipeline_runner.py` (add method to `RunManager`, after `active_count`, ~line 196)
- Test: `tests/test_run_guard.py` (create)

**Interfaces:**
- Consumes: existing `_pid_alive(pid)` module function, `self._runs` registry.
- Produces (used by Task 3): `RunManager.has_live_run(self) -> dict | None` — the first live run dict, else None. `running` counts as live unconditionally (we own the Popen); `detached` counts only if its PID is still alive (a stale detached entry must not block deletes forever).

- [ ] **Step 1: Write the failing test**

Create `tests/test_run_guard.py`:

```python
"""RunManager.has_live_run: running counts; detached only with a live PID."""
from __future__ import annotations


def _mgr_with(monkeypatch, runs, alive_pids=()):
    import pipeline_runner as pr

    mgr = pr.RunManager.__new__(pr.RunManager)  # skip __init__ disk I/O
    import threading

    mgr._lock = threading.RLock()
    mgr._runs = {r["id"]: r for r in runs}
    mgr._procs = {}
    monkeypatch.setattr(pr, "_pid_alive", lambda pid: pid in alive_pids)
    return mgr


def test_running_is_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "running", "pid": 1, "started_at": ""}])
    assert mgr.has_live_run()["id"] == "a"


def test_detached_dead_pid_is_not_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "detached", "pid": 1, "started_at": ""}])
    assert mgr.has_live_run() is None


def test_detached_live_pid_is_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "detached", "pid": 1, "started_at": ""}],
                    alive_pids={1})
    assert mgr.has_live_run()["id"] == "a"


def test_finished_is_not_live(monkeypatch):
    mgr = _mgr_with(monkeypatch, [{"id": "a", "status": "finished", "pid": 1, "started_at": ""}])
    assert mgr.has_live_run() is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `d:\dlt\.venv\Scripts\python.exe -m pytest tests/test_run_guard.py -v`
Expected: FAIL with `AttributeError: 'RunManager' object has no attribute 'has_live_run'`

- [ ] **Step 3: Implement**

Add to `RunManager` in `gui/pipeline_runner.py` (after `active_count`):

```python
    def has_live_run(self) -> dict[str, Any] | None:
        """First run that is actually alive right now, else None.

        ``running`` entries are owned Popens and always count. ``detached``
        entries (adopted from a previous GUI session) count only while their
        PID is alive, so a stale registry entry can't block destructive
        actions forever.
        """
        with self._lock:
            for r in self._runs.values():
                if r["status"] == "running":
                    return r
                if r["status"] == "detached" and _pid_alive(r.get("pid")):
                    return r
        return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `d:\dlt\.venv\Scripts\python.exe -m pytest tests/test_run_guard.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_run_guard.py gui/pipeline_runner.py
git commit -m "feat(gui): RunManager.has_live_run liveness query"
```

---

### Task 3: DELETE routes with run guard

**Files:**
- Modify: `gui/app.py` (after the existing `/api/iceberg/tables/<table>` GET routes, ~line 580)
- Test: `tests/test_iceberg_delete_routes.py` (create)

**Interfaces:**
- Consumes: Task 1's `iceberg_browser.delete_table` / `delete_all_tables`, Task 2's `runner.has_live_run()`, existing `@api` wrapper and `_body()` helper.
- Produces: `DELETE /api/iceberg/tables/<table>` and `DELETE /api/iceberg/tables` (JSON body `{"include_system": bool}`), both → 409 `{"error": ...}` while a run is live; errors map via `@api` (ValueError→400, FileNotFoundError→404).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_iceberg_delete_routes.py`:

```python
"""DELETE /api/iceberg/tables[...] routes: guard, param passing, error mapping."""
from __future__ import annotations

import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run", lambda: None)
    return gui_app.app.test_client()


def test_delete_table_route(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(
        gui_app.iceberg_browser, "delete_table",
        lambda t: {"deleted": [t], "watermarks_cleared": [t], "rows": 1,
                   "size_bytes": 2, "errors": {}},
    )
    resp = client.delete("/api/iceberg/tables/patient_ad", json={})
    assert resp.status_code == 200
    assert resp.get_json()["deleted"] == ["patient_ad"]


def test_delete_table_blocked_while_run_live(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run",
                        lambda: {"id": "r1", "label": "x"})
    resp = client.delete("/api/iceberg/tables/patient_ad", json={})
    assert resp.status_code == 409
    assert "r1" in resp.get_json()["error"]


def test_delete_table_protected_maps_to_400(client, monkeypatch):
    import app as gui_app

    def boom(t):
        raise ValueError("table not deletable")
    monkeypatch.setattr(gui_app.iceberg_browser, "delete_table", boom)
    resp = client.delete("/api/iceberg/tables/_dlt_loads", json={})
    assert resp.status_code == 400


def test_delete_all_passes_include_system(client, monkeypatch):
    import app as gui_app

    seen = {}

    def fake(include_system=False):
        seen["include_system"] = include_system
        return {"deleted": [], "watermarks_cleared": [], "errors": {}}
    monkeypatch.setattr(gui_app.iceberg_browser, "delete_all_tables", fake)

    resp = client.delete("/api/iceberg/tables", json={"include_system": True})
    assert resp.status_code == 200
    assert seen["include_system"] is True

    resp = client.delete("/api/iceberg/tables", json={})
    assert seen["include_system"] is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `d:\dlt\.venv\Scripts\python.exe -m pytest tests/test_iceberg_delete_routes.py -v`
Expected: FAIL — 405 (method not allowed) instead of 200/409/400

- [ ] **Step 3: Implement**

In `gui/app.py`, after `api_iceberg_table` (the GET overview route):

```python
def _run_guard():
    """409 body when a pipeline run is alive, else None (deletes allowed)."""
    live = runner.has_live_run()
    if live:
        return jsonify({"error": (
            f"a pipeline run is active ({live['id']}: {live.get('label') or live.get('command', '')}); "
            "deleting staging tables while a run is loading would corrupt the lake"
        )}), 409
    return None


@app.delete("/api/iceberg/tables/<table>")
@api
def api_iceberg_delete_table(table: str):
    blocked = _run_guard()
    if blocked:
        return blocked
    return jsonify(iceberg_browser.delete_table(table))


@app.delete("/api/iceberg/tables")
@api
def api_iceberg_delete_all():
    blocked = _run_guard()
    if blocked:
        return blocked
    include_system = bool(_body().get("include_system"))
    return jsonify(iceberg_browser.delete_all_tables(include_system=include_system))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `d:\dlt\.venv\Scripts\python.exe -m pytest tests/test_iceberg_delete_routes.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_iceberg_delete_routes.py gui/app.py
git commit -m "feat(gui): DELETE routes for staging tables with live-run guard"
```

---

### Task 4: Explorer UI — buttons + typed-confirmation modal

**Files:**
- Modify: `gui/templates/iceberg.html`

**Interfaces:**
- Consumes: Task 3's routes via `api("DELETE", url, body)`; `app.js` helpers `esc`, `ok`, `err`, `fmtNum`, `fmtBytes`, `el`; CSS classes `.modal-bg`, `.modal`, `.btn.bad`.
- Produces: user-facing delete actions; no code consumes this.

- [ ] **Step 1: Add the Delete all button to the Tables panel head**

In `iceberg.html`, change the panel head (line ~9):

```html
<div class="panel-head"><h2>Tables</h2>
  <span class="spacer"></span>
  <button class="btn sm bad" id="delete-all"><i class="fa-solid fa-trash"></i> Delete all…</button>
  <button class="btn sm ghost" id="refresh">↻</button>
</div>
```

(If `.panel-head` doesn't flex its children, wrap the two buttons in `<div class="row-flex">` instead — match whatever the rendered layout does.)

- [ ] **Step 2: Add a per-row delete action**

In `renderTables()`, add a header cell and a row cell (the row's `onclick` selects the table, so the button must stop propagation):

```js
// thead row gains a trailing <th></th>:
<thead><tr><th>Table</th><th>Type</th><th class="num">Rows</th><th class="num">Size</th><th class="num">Snaps</th><th></th></tr></thead>

// each row gains, before </tr>:
<td><button class="btn sm ghost" title="Delete table"
     onclick="event.stopPropagation();openDeleteModal('${esc(t.table)}')">
     <i class="fa-solid fa-trash"></i></button></td>
```

- [ ] **Step 3: Add the modal markup at the end of the content block (before `{% endblock %}`)**

```html
<div class="modal-bg" id="del-modal">
  <div class="modal" style="max-width:480px">
    <h2 id="del-title"></h2>
    <div id="del-info" style="margin-bottom:12px"></div>
    <label id="del-sys-wrap" hidden style="display:block;margin-bottom:12px">
      <input type="checkbox" id="del-include-sys" checked>
      Include system tables (etl_control, etl_run_log, etl_dq_results)
    </label>
    <div>
      <label id="del-prompt"></label>
      <input id="del-confirm" autocomplete="off" spellcheck="false" style="width:100%">
    </div>
    <div class="row-flex" style="margin-top:16px;justify-content:flex-end;gap:8px">
      <button class="btn ghost" id="del-cancel">Cancel</button>
      <button class="btn bad" id="del-go" disabled>Delete</button>
    </div>
  </div>
</div>
```

- [ ] **Step 4: Add the modal JS to the scripts block**

```js
const SYSTEM_TABLE_NOTE = "System table — the pipeline recreates it on the next run, but its history (runs / watermark mirror) is lost.";
let delTarget = null;   // table name, or null => delete-all mode

function openDeleteModal(table) {
  const t = allTables.find(x => x.table === table);
  delTarget = table;
  el("del-title").textContent = `Delete ${table}?`;
  el("del-info").innerHTML =
    `<div>${fmtNum(t?.rows)} rows · ${fmtBytes(t?.size_bytes)} on disk.</div>` +
    (t?.is_system
      ? `<div class="banner warn" style="margin-top:8px">${esc(SYSTEM_TABLE_NOTE)}</div>`
      : `<div class="muted" style="margin-top:8px">Also clears its extraction watermarks — the next run re-extracts this table from its initial window.</div>`);
  el("del-sys-wrap").hidden = true;
  el("del-prompt").textContent = `Type the table name to confirm:`;
  showDelModal();
}

function openDeleteAllModal() {
  const data = allTables.filter(t => !t.is_system);
  const size = allTables.reduce((s, t) => s + (t.size_bytes || 0), 0);
  delTarget = null;
  el("del-title").textContent = "Delete ALL staging tables?";
  el("del-info").innerHTML =
    `<div>${data.length} data table(s), ${fmtBytes(size)} total. All their extraction watermarks are cleared — the next run re-extracts everything from scratch.</div>`;
  el("del-sys-wrap").hidden = false;
  el("del-prompt").textContent = `Type DELETE ALL to confirm:`;
  showDelModal();
}

function showDelModal() {
  el("del-confirm").value = "";
  el("del-go").disabled = true;
  el("del-modal").classList.add("show");
  el("del-confirm").focus();
}
function hideDelModal() { el("del-modal").classList.remove("show"); }

el("del-confirm").oninput = () => {
  const want = delTarget === null ? "DELETE ALL" : delTarget;
  el("del-go").disabled = el("del-confirm").value !== want;
};

el("del-go").onclick = async () => {
  el("del-go").disabled = true;
  try {
    const res = delTarget === null
      ? await api("DELETE", "/api/iceberg/tables", { include_system: el("del-include-sys").checked })
      : await api("DELETE", `/api/iceberg/tables/${encodeURIComponent(delTarget)}`);
    hideDelModal();
    const nErr = Object.keys(res.errors || {}).length;
    ok(`Deleted ${res.deleted.length} table(s), cleared ${res.watermarks_cleared.length} watermark(s)` +
       (nErr ? ` — ${nErr} failed` : ""));
    if (nErr) err(Object.entries(res.errors).map(([t, m]) => `${t}: ${m}`).join("; "));
    if (delTarget === null || delTarget === curTable) {
      curTable = null;
      el("detail").hidden = true;
      el("detail-head").innerHTML = `<div class="muted">Select a table to inspect.</div>`;
    }
    loadTables();
  } catch (e) { err(e.message); el("del-go").disabled = false; }
};

el("del-cancel").onclick = hideDelModal;
el("delete-all").onclick = openDeleteAllModal;
```

Note: `app.js` already closes any `.modal-bg.show` on Esc/backdrop click — no extra wiring needed.

- [ ] **Step 5: Manual verification (no JS unit tests in this repo)**

Start the GUI (`d:\dlt\.venv\Scripts\python.exe gui/app.py`), open http://127.0.0.1:8765/iceberg and confirm:
- trash icon on each row opens the modal; Delete stays disabled until the exact name is typed
- deleting a table removes it from the list and its `control_state.json` entry
- "Delete all…" requires typing `DELETE ALL`; the system-tables checkbox is visible and checked by default
- starting a pipeline run and trying a delete shows the 409 message

- [ ] **Step 6: Commit**

```bash
git add gui/templates/iceberg.html
git commit -m "feat(gui): delete actions with typed confirmation in staging explorer"
```

---

### Task 5: Full-suite regression + end-to-end verify

- [ ] **Step 1: Run the whole test suite**

Run: `d:\dlt\.venv\Scripts\python.exe -m pytest tests -q`
Expected: everything passes (no regressions from the `config import` change in `iceberg_browser.py`).

- [ ] **Step 2: Exercise the flow end-to-end against a scratch lake**

Point the GUI at a temp copy (or use the real lake and delete a disposable table, e.g. re-runnable `patient_ad`) and verify delete + watermark cleanup + re-run recreates the table. Use the `verify` skill before claiming done.

- [ ] **Step 3: Final commit if anything was adjusted**
