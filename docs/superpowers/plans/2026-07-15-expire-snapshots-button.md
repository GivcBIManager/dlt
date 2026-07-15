# Expire-Snapshots Button Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-table button on the GUI staging explorer that expires all Iceberg snapshots except the latest and deletes the now-orphaned data/manifest files, reclaiming disk space.

**Architecture:** New writable-maintenance module `gui/iceberg_maintenance.py` (kept out of the read-only `iceberg_browser.py`) exposes `expire_snapshots(table)`. It opens the table writable through the ETL pipeline's Iceberg catalog (same path as `apply_snapshot_retention` in `etl/iceberg_load.py:621-658`), expires all unprotected snapshots via pyiceberg's `by_ids(...)`, then deletes any file under the table's `data/`+`metadata/` dirs no longer referenced by the remaining snapshots. A new Flask route wires it to a per-row broom button with a confirm modal in `iceberg.html`.

**Tech Stack:** Flask + Jinja2/vanilla JS (gui/), pyiceberg 0.11.1, dlt (`dlt.common.libs.pyiceberg.get_iceberg_tables`), pytest.

**Spec:** `docs/superpowers/specs/2026-07-15-expire-snapshots-button-design.md`

## Global Constraints

- Test runner: `.venv\Scripts\python.exe -m pytest` from repo root `d:\dlt` (system python has no pytest).
- pyiceberg 0.11.1 has **no `retain_last`** — expire by ids; protected branch/tag heads are auto-skipped by pyiceberg.
- pyiceberg `expire_snapshots` is metadata-only; orphan file deletion is our code and must NEVER delete a referenced file, any `*.metadata.json`, or `version-hint.text`.
- The heavy `dlt`/`etl` imports must stay lazy (inside the function) — `gui/app.py` startup must not import dlt.
- Mutating routes must be called with a JSON body (CSRF gate returns 415 otherwise) — tests use `json={}`.
- `_dlt*` tables are never touched.

---

### Task 1: `gui/iceberg_maintenance.py` — expiry + orphan cleanup logic

**Files:**
- Create: `gui/iceberg_maintenance.py`
- Test: `tests/test_iceberg_expire.py`

**Interfaces:**
- Consumes: `config.ICEBERG_ROOT`, `config.REPO_ROOT` (from `gui/config.py:16,69`); `etl.config.load_settings`, `etl.iceberg_load.build_pipeline`, `dlt.common.libs.pyiceberg.get_iceberg_tables` (lazily).
- Produces: `expire_snapshots(table: str) -> dict` with keys `table` (str), `expired` (int), `remaining` (int), `orphans_deleted` (int), `bytes_freed` (int), `errors` (dict path→msg). Raises `ValueError` for unsafe/`_dlt*` names, `FileNotFoundError` for unknown tables. Also `_writable_table(table)` (monkeypatch seam for tests and for Task 2's route tests).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_iceberg_expire.py`:

```python
"""Snapshot expiry + orphan cleanup for staging Iceberg tables."""
from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog


def _rows(offset: int) -> pa.Table:
    return pa.table({
        "id": pa.array([offset, offset + 1], pa.int64()),
        "name": pa.array([f"a{offset}", f"b{offset}"]),
    })


@pytest.fixture
def lake(tmp_path, monkeypatch):
    import iceberg_maintenance as im

    root = tmp_path / "oasis"
    root.mkdir()
    monkeypatch.setattr(im, "ICEBERG_ROOT", root)
    return root


@pytest.fixture
def table(lake, tmp_path, monkeypatch):
    """Real Iceberg table under the fake lake: 2 appends + 1 overwrite.

    The overwrite makes the appends' parquet files unreferenced once their
    snapshots are expired — that's what proves orphan cleanup frees data files.
    """
    import iceberg_maintenance as im

    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
    )
    catalog.create_namespace("oasis")
    tbl = catalog.create_table(
        "oasis.patient_ad", schema=_rows(0).schema,
        location=(lake / "patient_ad").as_uri(),
    )
    tbl.append(_rows(0))
    tbl.append(_rows(10))
    tbl.overwrite(_rows(20))
    monkeypatch.setattr(im, "_writable_table", lambda name: tbl)
    return tbl


def test_expire_keeps_only_current_snapshot(lake, table):
    import iceberg_maintenance as im

    before = len(table.metadata.snapshots)
    assert before > 1
    current = table.metadata.current_snapshot_id

    out = im.expire_snapshots("patient_ad")

    assert out["table"] == "patient_ad"
    assert out["expired"] == before - 1
    assert out["remaining"] == 1
    assert out["errors"] == {}
    snaps = table.metadata.snapshots
    assert [s.snapshot_id for s in snaps] == [current]


def test_expire_deletes_orphans_keeps_referenced(lake, table):
    import iceberg_maintenance as im

    data_dir = lake / "patient_ad" / "data"
    meta_dir = lake / "patient_ad" / "metadata"
    parquet_before = len(list(data_dir.rglob("*.parquet")))
    metadata_json_before = len(list(meta_dir.glob("*.metadata.json")))
    assert parquet_before == 3  # 2 appends + 1 overwrite

    out = im.expire_snapshots("patient_ad")

    # the appends' parquet files are orphaned by the overwrite + expiry
    assert len(list(data_dir.rglob("*.parquet"))) == 1
    assert out["orphans_deleted"] > 0
    assert out["bytes_freed"] > 0
    # every *.metadata.json survives; the remaining snapshot's files survive
    assert len(list(meta_dir.glob("*.metadata.json"))) >= metadata_json_before
    snap = table.metadata.snapshots[0]
    manifest_list_name = snap.manifest_list.rsplit("/", 1)[-1]
    assert (meta_dir / manifest_list_name).exists()
    # current data still readable and correct
    got = table.scan().to_arrow().sort_by("id")
    assert got.column("id").to_pylist() == [20, 21]


def test_expire_is_idempotent(lake, table):
    import iceberg_maintenance as im

    im.expire_snapshots("patient_ad")
    out = im.expire_snapshots("patient_ad")
    assert out["expired"] == 0
    assert out["orphans_deleted"] == 0
    assert out["remaining"] == 1


@pytest.mark.parametrize("bad", ["_dlt_loads", "..", "a/b", "a\\b", ""])
def test_expire_rejects_protected_and_unsafe_names(lake, bad):
    import iceberg_maintenance as im

    with pytest.raises(ValueError):
        im.expire_snapshots(bad)


def test_expire_unknown_table_is_not_found(lake):
    import iceberg_maintenance as im

    with pytest.raises(FileNotFoundError):
        im.expire_snapshots("nope")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python.exe -m pytest tests/test_iceberg_expire.py -v`
Expected: FAIL/ERROR with `ModuleNotFoundError: No module named 'iceberg_maintenance'`

- [ ] **Step 3: Write the implementation**

Create `gui/iceberg_maintenance.py`:

```python
"""Writable staging-lake maintenance: snapshot expiry + orphan file cleanup.

``iceberg_browser`` stays read-only (StaticTable cannot commit); anything that
rewrites table metadata lives here. Tables are opened writable through the ETL
pipeline's Iceberg catalog — the same commit path the loader's own
``apply_snapshot_retention`` uses — so the new metadata files follow dlt's
naming and are picked up by both the loader and the browser.

pyiceberg's ``expire_snapshots`` only removes snapshot entries from metadata;
the data/manifest files they referenced stay on disk. ``expire_snapshots``
here therefore finishes with an orphan sweep: every file under the table's
``data/`` and ``metadata/`` dirs that no remaining snapshot references is
deleted (``*.metadata.json`` and ``version-hint.text`` are always kept).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from config import ICEBERG_ROOT, REPO_ROOT

# metadata jsons + version hint must survive any sweep: they are the table.
_ALWAYS_KEEP = re.compile(r"(\.metadata\.json|^version-hint\.text)$")


def _validated_dir(table: str) -> Path:
    """Reject path tricks / ``_dlt*`` names; require an existing table dir."""
    safe = Path(table).name
    if not safe or safe != table or safe in (".", "..") or safe.startswith("_dlt"):
        raise ValueError(f"snapshots not expirable for table: {table!r}")
    tdir = ICEBERG_ROOT / safe
    if not (tdir / "metadata").is_dir():
        raise FileNotFoundError(table)
    return tdir


def _writable_table(table: str):
    """Open the staging table writable via the ETL pipeline's catalog."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from dlt.common.libs.pyiceberg import get_iceberg_tables

    from etl.config import load_settings
    from etl.iceberg_load import build_pipeline

    pipeline = build_pipeline(load_settings())
    try:
        return get_iceberg_tables(pipeline, table)[table]
    except ValueError as exc:  # unknown to the pipeline schema
        raise FileNotFoundError(table) from exc


def _local_path(uri: str) -> Path | None:
    """Local Path for a lake URI; None when the lake is remote (s3/az/...)."""
    parsed = urlparse(str(uri))
    if parsed.scheme == "file":
        s = unquote(parsed.path)
    elif not parsed.scheme or len(parsed.scheme) == 1:  # bare path / drive letter
        s = str(uri)
    else:
        return None
    if re.match(r"^/[A-Za-z]:", s):  # file:///D:/... -> D:/...
        s = s[1:]
    return Path(s).resolve()


def _expire_to_latest(tbl) -> int:
    """Expire every unprotected snapshot; returns how many were expired."""
    meta = tbl.metadata
    protected = {ref.snapshot_id for ref in meta.refs.values()}
    if meta.current_snapshot_id is not None:
        protected.add(meta.current_snapshot_id)
    ids = [s.snapshot_id for s in meta.snapshots if s.snapshot_id not in protected]
    if ids:
        tbl.maintenance.expire_snapshots().by_ids(ids).commit()
    return len(ids)


def _referenced_files(tbl) -> set[Path] | None:
    """Every local file the remaining snapshots still reference.

    Returns None when any reference is non-local — then the lake is remote
    and the filesystem sweep must be skipped entirely.
    """
    meta, io = tbl.metadata, tbl.io
    uris: list[str] = []
    for snap in meta.snapshots:
        uris.append(snap.manifest_list)
        for manifest in snap.manifests(io):
            uris.append(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(io, discard_deleted=False):
                uris.append(entry.data_file.file_path)
    for stat in list(meta.statistics or []) + list(meta.partition_statistics or []):
        path = getattr(stat, "statistics_path", None)
        if path:
            uris.append(path)
    keep: set[Path] = set()
    for uri in uris:
        local = _local_path(uri)
        if local is None:
            return None
        keep.add(local)
    return keep


def _delete_orphans(tdir: Path, keep: set[Path]) -> tuple[int, int, dict[str, str]]:
    """Delete unreferenced files under data/ + metadata/; prune empty dirs."""
    deleted = 0
    freed = 0
    errors: dict[str, str] = {}
    for sub in ("data", "metadata"):
        base = tdir / sub
        if not base.is_dir():
            continue
        for f in sorted(base.rglob("*")):
            if not f.is_file() or _ALWAYS_KEEP.search(f.name) or f.resolve() in keep:
                continue
            try:
                size = f.stat().st_size
                f.unlink()
                deleted += 1
                freed += size
            except OSError as exc:
                errors[f.relative_to(tdir).as_posix()] = str(exc)
        # deepest-first so emptied partition dirs collapse upwards
        for d in sorted((p for p in base.rglob("*") if p.is_dir()), reverse=True):
            try:
                d.rmdir()
            except OSError:
                pass  # not empty
    return deleted, freed, errors


def expire_snapshots(table: str) -> dict[str, Any]:
    """Expire all snapshots except the latest and sweep orphaned files."""
    tdir = _validated_dir(table)
    tbl = _writable_table(table)
    expired = _expire_to_latest(tbl)
    result: dict[str, Any] = {
        "table": table,
        "expired": expired,
        "remaining": len(tbl.metadata.snapshots),
        "orphans_deleted": 0,
        "bytes_freed": 0,
        "errors": {},
    }
    keep = _referenced_files(tbl)
    if keep is None:
        result["errors"]["cleanup"] = "lake is not on the local filesystem; orphan sweep skipped"
        return result
    deleted, freed, errors = _delete_orphans(tdir, keep)
    result.update(orphans_deleted=deleted, bytes_freed=freed, errors=errors)
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python.exe -m pytest tests/test_iceberg_expire.py -v`
Expected: 9 passed (`test_expire_keeps_only_current_snapshot`, `test_expire_deletes_orphans_keeps_referenced`, `test_expire_is_idempotent`, 5× `test_expire_rejects_protected_and_unsafe_names`, `test_expire_unknown_table_is_not_found`)

- [ ] **Step 5: Run the full suite to check for regressions**

Run: `.venv\Scripts\python.exe -m pytest -q`
Expected: all pass, no new failures

- [ ] **Step 6: Commit**

```bash
git add gui/iceberg_maintenance.py tests/test_iceberg_expire.py
git commit -m "feat(gui): snapshot expiry + orphan file cleanup for staging tables"
```

---

### Task 2: `POST /api/iceberg/tables/<table>/expire-snapshots` route

**Files:**
- Modify: `gui/app.py` (import block ~line 37-45; `_run_guard` at 556-564; new route after `api_ib_delete_all` at 576-583)
- Test: `tests/test_iceberg_expire_routes.py`

**Interfaces:**
- Consumes: `iceberg_maintenance.expire_snapshots(table) -> dict` (Task 1).
- Produces: `POST /api/iceberg/tables/<table>/expire-snapshots` → 200 JSON result | 409 while a run is live | 404 unknown table | 400 protected name. `_run_guard(action: str = "deleting staging tables")` — existing delete callers unchanged.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_iceberg_expire_routes.py`:

```python
"""POST /api/iceberg/tables/<t>/expire-snapshots: guard + error mapping."""
from __future__ import annotations

import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run", lambda: None)
    return gui_app.app.test_client()


def test_expire_route(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(
        gui_app.iceberg_maintenance, "expire_snapshots",
        lambda t: {"table": t, "expired": 3, "remaining": 1,
                   "orphans_deleted": 5, "bytes_freed": 1024, "errors": {}},
    )
    resp = client.post("/api/iceberg/tables/patient_ad/expire-snapshots", json={})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["expired"] == 3
    assert body["table"] == "patient_ad"


def test_expire_blocked_while_run_live(client, monkeypatch):
    import app as gui_app

    monkeypatch.setattr(gui_app.runner, "has_live_run",
                        lambda: {"id": "r1", "label": "x"})
    resp = client.post("/api/iceberg/tables/patient_ad/expire-snapshots", json={})
    assert resp.status_code == 409
    assert "r1" in resp.get_json()["error"]


def test_expire_unknown_table_maps_404(client, monkeypatch):
    import app as gui_app

    def boom(t):
        raise FileNotFoundError(t)
    monkeypatch.setattr(gui_app.iceberg_maintenance, "expire_snapshots", boom)
    resp = client.post("/api/iceberg/tables/nope/expire-snapshots", json={})
    assert resp.status_code == 404


def test_expire_protected_name_maps_400(client, monkeypatch):
    import app as gui_app

    def boom(t):
        raise ValueError("snapshots not expirable")
    monkeypatch.setattr(gui_app.iceberg_maintenance, "expire_snapshots", boom)
    resp = client.post("/api/iceberg/tables/_dlt_loads/expire-snapshots", json={})
    assert resp.status_code == 400
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python.exe -m pytest tests/test_iceberg_expire_routes.py -v`
Expected: FAIL — `AttributeError: module 'app' has no attribute 'iceberg_maintenance'` (and 404 on the route)

- [ ] **Step 3: Implement the route**

In `gui/app.py`:

3a. Add the import next to the other gui-module imports (the block around lines 37-45, alphabetical — right before/after `import iceberg_browser`):

```python
import iceberg_maintenance  # noqa: E402
```

3b. Generalize `_run_guard` (currently `gui/app.py:556-564`) with an `action` parameter so the message fits both operations; existing delete callers stay `_run_guard()`:

```python
def _run_guard(action: str = "deleting staging tables"):
    """409 body when a pipeline run is alive, else None (mutation allowed)."""
    live = runner.has_live_run()
    if live:
        return jsonify({"error": (
            f"a pipeline run is active ({live['id']}: {live.get('label') or live.get('command', '')}); "
            f"{action} while a run is loading would corrupt the lake"
        )}), 409
    return None
```

3c. Add the route directly after `api_ib_delete_all` (after `gui/app.py:583`):

```python
@app.post("/api/iceberg/tables/<table>/expire-snapshots")
@api
def api_ib_expire_snapshots(table):
    blocked = _run_guard("expiring snapshots")
    if blocked:
        return blocked
    return jsonify(iceberg_maintenance.expire_snapshots(table))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python.exe -m pytest tests/test_iceberg_expire_routes.py tests/test_iceberg_delete_routes.py -v`
Expected: all PASS (delete-route tests prove `_run_guard()` default is unchanged)

- [ ] **Step 5: Run the full suite**

Run: `.venv\Scripts\python.exe -m pytest -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add gui/app.py tests/test_iceberg_expire_routes.py
git commit -m "feat(gui): POST /api/iceberg/tables/<t>/expire-snapshots route"
```

---

### Task 3: Broom button + confirm modal on the staging explorer

**Files:**
- Modify: `gui/templates/iceberg.html` (row template ~line 162-163; modal markup after `#del-modal` at ~line 115; JS after the deletion block at ~line 393)

**Interfaces:**
- Consumes: `POST /api/iceberg/tables/<t>/expire-snapshots` (Task 2); `api/ok/err/esc/fmtNum/fmtBytes` from `gui/static/app.js`; global modal Esc/backdrop close (app.js handles `.modal-bg`).
- Produces: user-facing button; no programmatic interface.

- [ ] **Step 1: Add the per-row button**

In `renderTables()` replace the trailing `<td>` (currently `iceberg.html:162-163`):

```js
        <td>${t.table.startsWith("_dlt") ? "" : `${t.snapshots > 1 ? `<button class="btn sm ghost" title="Expire old snapshots (keep latest)"
             onclick="event.stopPropagation();openExpireModal('${esc(t.table)}')"><i class="fa-solid fa-broom"></i></button>` : ""}<button class="btn sm ghost" title="Delete table"
             onclick="event.stopPropagation();openDeleteModal('${esc(t.table)}')"><i class="fa-solid fa-trash"></i></button>`}</td>
```

(Broom hidden for `_dlt*` tables and tables with ≤ 1 snapshot — nothing to expire.)

- [ ] **Step 2: Add the confirm modal markup**

Directly after the closing `</div>` of `#del-modal` (after `iceberg.html:115`), before `{% endblock %}`:

```html
<div class="modal-bg" id="exp-modal">
  <div class="modal" style="max-width:480px">
    <h2 id="exp-title"></h2>
    <div id="exp-info" style="margin-bottom:12px"></div>
    <div class="row-flex" style="margin-top:16px;justify-content:flex-end;gap:8px">
      <button class="btn ghost" id="exp-cancel">Cancel</button>
      <button class="btn primary" id="exp-go">Expire snapshots</button>
    </div>
  </div>
</div>
```

- [ ] **Step 3: Add the JS**

After the deletion block (after `el("delete-all").onclick = openDeleteAllModal;`, `iceberg.html:393`):

```js
// --- snapshot expiry (keep latest only) ----------------------------------- //
let expTarget = null;

function openExpireModal(table) {
  const t = allTables.find(x => x.table === table);
  expTarget = table;
  el("exp-title").textContent = `Expire snapshots of ${table}?`;
  el("exp-info").innerHTML =
    `<div>${fmtNum(t?.snapshots)} snapshot(s) · ${fmtBytes(t?.size_bytes)} on disk.</div>` +
    `<div class="muted" style="margin-top:8px">Keeps only the latest snapshot and deletes the files older snapshots referenced. Current rows are untouched; time-travel history is lost.</div>`;
  el("exp-go").disabled = false;
  el("exp-modal").classList.add("show");
}
function hideExpireModal() { el("exp-modal").classList.remove("show"); }

el("exp-go").onclick = async () => {
  el("exp-go").disabled = true;
  try {
    const res = await api("POST", `/api/iceberg/tables/${encodeURIComponent(expTarget)}/expire-snapshots`, {});
    hideExpireModal();
    const nErr = Object.keys(res.errors || {}).length;
    ok(`Expired ${res.expired} snapshot(s), removed ${res.orphans_deleted} orphan file(s), freed ${fmtBytes(res.bytes_freed)}` +
       (nErr ? ` — ${nErr} file(s) failed` : ""));
    if (nErr) err(Object.entries(res.errors).map(([f, m]) => `${f}: ${m}`).join("; "));
    if (expTarget === curTable) select(curTable);
    loadTables();
  } catch (e) { err(e.message); el("exp-go").disabled = false; }
};
el("exp-cancel").onclick = hideExpireModal;
```

- [ ] **Step 4: Verify the page still renders**

Run: `.venv\Scripts\python.exe -m pytest tests/test_run_iceberg_pages_render.py -v`
Expected: PASS (template parses; page renders)

- [ ] **Step 5: Run the full suite**

Run: `.venv\Scripts\python.exe -m pytest -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add gui/templates/iceberg.html
git commit -m "feat(gui): expire-snapshots button on staging explorer table rows"
```

---

### Task 4: End-to-end verification against the real GUI

**Files:** none (verification only)

- [ ] **Step 1: Start the GUI** (or use the running instance) and open `http://127.0.0.1:8765/iceberg`.
- [ ] **Step 2:** Pick a small table with several snapshots; note its Snaps count and Size. Click the broom → confirm modal shows counts → Expire. Expect a toast like "Expired N snapshot(s), removed M orphan file(s), freed X".
- [ ] **Step 3:** Re-select the table: Snapshots tab shows exactly 1 (current) snapshot; Sample data still loads.
- [ ] **Step 4:** Start (or simulate) a live run and confirm the button returns the 409 toast.
- [ ] **Step 5:** Confirm the next pipeline run against that table still loads (the loader opens the latest metadata — a smoke `dq_check` or small incremental run suffices).
