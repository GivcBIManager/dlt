# Flow-Run Monitoring (Dagster GraphQL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show scheduled/`run-now` flow runs — list, status, and live per-run logs with the parsed progress dashboard — in the GUI's Monitor page, pulled from the Dagster GraphQL API.

**Architecture:** Two read-only functions in `gui/dagster_client.py` (runs list + cursor-based log tail over GraphQL), two Flask routes in `gui/app.py` that enrich with flow display names and publicise links, and a new "Flow runs" tab in `gui/templates/logs.html` that reuses the existing console + `createLogDash()` viewer. Dagster remains the single source of truth; no orchestrator changes, no new storage.

**Tech Stack:** Python 3.12, Flask, stdlib urllib (no new deps), Dagster GraphQL (`runsOrError`, `logsForRun`), Jinja template + vanilla JS, pytest.

**Spec:** `docs/superpowers/specs/2026-07-15-flow-run-monitoring-design.md`

## Global Constraints

- `dagster_client.py` functions never raise on connection errors: list functions return `[]`, dict functions return `{"error": ...}` shapes.
- Run tests with `.venv\Scripts\python -m pytest` from repo root (plain `python` has no pytest).
- GUI modules are imported top-level (tests do `import dagster_client` with `gui/` on `sys.path` — see `tests/conftest.py`).
- Every commit message ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: `dagster_client.flow_runs()` + `run_log_tail()`

**Files:**
- Modify: `gui/dagster_client.py` (append after `flow_status`)
- Test: `tests/test_dagster_client.py` (append)

**Interfaces:**
- Consumes: existing `_query`, `_first_error`, `run_link`, `flow_naming.flow_id_from_job`.
- Produces:
  - `flow_runs(limit: int = 50) -> list[dict]` — dicts with keys `run_id, job, flow_id, status, start_time, end_time, run_link`.
  - `run_log_tail(run_id: str, cursor: str | None = None) -> dict` — keys `chunk (str), cursor (str|None), has_more (bool), status (str|None), error (str|None)`.
  - `_runs_from_payload(results: list[dict]) -> list[dict]` and `_fmt_event(ev: dict) -> str | None` (pure, unit-tested).

- [ ] **Step 1: Write the failing tests** (append to `tests/test_dagster_client.py`)

```python
def test_runs_from_payload_filters_flow_jobs():
    import dagster_client as dc
    rows = dc._runs_from_payload([
        {"runId": "r1", "jobName": "flow_nightly__a1b2c3d4", "status": "SUCCESS",
         "startTime": 100.0, "endTime": 160.0},
        {"runId": "r2", "jobName": "__ASSET_JOB", "status": "SUCCESS",
         "startTime": 1.0, "endTime": 2.0},
    ])
    assert len(rows) == 1
    assert rows[0]["run_id"] == "r1"
    assert rows[0]["flow_id"] == "a1b2c3d4"
    assert rows[0]["status"] == "SUCCESS"
    assert rows[0]["start_time"] == 100.0
    assert rows[0]["end_time"] == 160.0
    assert rows[0]["run_link"].endswith("/runs/r1")


def test_flow_runs_empty_when_unreachable(monkeypatch):
    import dagster_client as dc
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "59999")
    assert dc.flow_runs() == []


def test_fmt_event_formats_line():
    import dagster_client as dc
    line = dc._fmt_event({"message": "hello", "timestamp": "1752570000000",
                          "level": "INFO"})
    assert line.endswith("| INFO     | hello")
    assert dc._fmt_event({"message": "", "timestamp": "1", "level": "INFO"}) is None


def test_run_log_tail_parses_connection(monkeypatch):
    import dagster_client as dc
    payload = {"data": {
        "logsForRun": {"__typename": "EventConnection",
                       "events": [{"message": "m1", "timestamp": "1752570000000",
                                   "level": "INFO"},
                                  {"message": "m2", "timestamp": "1752570001000",
                                   "level": "ERROR"}],
                       "cursor": "c2", "hasMore": False},
        "runOrError": {"status": "STARTED"},
    }}
    monkeypatch.setattr(dc, "_query", lambda q, v=None: payload)
    r = dc.run_log_tail("r1", cursor="c0")
    assert "m1" in r["chunk"] and "m2" in r["chunk"]
    assert r["cursor"] == "c2"
    assert r["has_more"] is False
    assert r["status"] == "STARTED"
    assert r["error"] is None


def test_run_log_tail_run_not_found(monkeypatch):
    import dagster_client as dc
    payload = {"data": {"logsForRun": {"__typename": "RunNotFoundError",
                                       "message": "no run"},
                        "runOrError": {}}}
    monkeypatch.setattr(dc, "_query", lambda q, v=None: payload)
    r = dc.run_log_tail("nope")
    assert r["chunk"] == "" and r["error"] == "no run"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_dagster_client.py -q`
Expected: 5 FAILED with `AttributeError: ... has no attribute '_runs_from_payload'` (etc.), 3 prior tests pass.

- [ ] **Step 3: Implement** (append to `gui/dagster_client.py`; add `import datetime as dt` to the imports)

```python
def _runs_from_payload(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep flow-job runs and reshape to the GUI's row format."""
    out: list[dict[str, Any]] = []
    for r in results:
        job = r.get("jobName") or ""
        if not job.startswith("flow_"):
            continue
        rid = r.get("runId")
        out.append({
            "run_id": rid,
            "job": job,
            "flow_id": flow_naming.flow_id_from_job(job),
            "status": r.get("status"),
            "start_time": r.get("startTime"),
            "end_time": r.get("endTime"),
            "run_link": run_link(rid) if rid else None,
        })
    return out


def flow_runs(limit: int = 50) -> list[dict[str, Any]]:
    """Recent flow-job runs, newest first. Empty list if Dagster unreachable."""
    q = """
    query FlowRuns($limit: Int!) {
      runsOrError(limit: $limit) {
        ... on Runs { results { runId jobName status startTime endTime } }
      }
    }"""
    res = _query(q, {"limit": limit})
    results = (res.get("data", {}).get("runsOrError", {}) or {}).get("results")
    if not results:
        return []
    return _runs_from_payload(results)


def _fmt_event(ev: dict[str, Any]) -> Optional[str]:
    """One log line per MessageEvent; None for empty/non-message events."""
    msg = ev.get("message")
    if not msg:
        return None
    try:
        hhmmss = dt.datetime.fromtimestamp(
            float(ev.get("timestamp")) / 1000).strftime("%H:%M:%S")
    except (TypeError, ValueError, OSError, OverflowError):
        hhmmss = "--:--:--"
    return f"{hhmmss} | {ev.get('level') or '':<8} | {msg}"


def run_log_tail(run_id: str, cursor: Optional[str] = None) -> dict[str, Any]:
    """New log lines for a run since ``cursor`` -- the GraphQL analogue of the
    byte-offset file tail the Monitor page uses for run_logs files."""
    q = """
    query Tail($runId: ID!, $cursor: String) {
      logsForRun(runId: $runId, afterCursor: $cursor, limit: 2000) {
        __typename
        ... on EventConnection {
          events { __typename ... on MessageEvent { message timestamp level } }
          cursor
          hasMore
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
      runOrError(runId: $runId) { ... on Run { status } }
    }"""
    res = _query(q, {"runId": run_id, "cursor": cursor})
    err_shape = {"chunk": "", "cursor": cursor, "has_more": False, "status": None}
    if "errors" in res:
        return {**err_shape, "error": _first_error(res)}
    data = res.get("data", {})
    node = data.get("logsForRun", {}) or {}
    if node.get("__typename") != "EventConnection":
        return {**err_shape, "error": node.get("message", "log fetch failed")}
    lines = [ln for ev in node.get("events", []) if (ln := _fmt_event(ev))]
    return {
        "chunk": "\n".join(lines) + ("\n" if lines else ""),
        "cursor": node.get("cursor") or cursor,
        "has_more": bool(node.get("hasMore")),
        "status": (data.get("runOrError", {}) or {}).get("status"),
        "error": None,
    }
```

Note `Optional` is already imported (`from typing import Any` — extend to `from typing import Any, Optional` if not present).

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_dagster_client.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add gui/dagster_client.py tests/test_dagster_client.py
git commit -m "feat(gui): Dagster run list + cursor log tail in dagster_client"
```

---

### Task 2: Flask routes `/api/flow-runs` and `/api/flow-runs/<run_id>/log`

**Files:**
- Modify: `gui/app.py` (after `api_dagster_flow_status`, ~line 469)
- Test: `tests/test_flows_api.py` (append; it already has a Flask test-client fixture pattern — follow it)

**Interfaces:**
- Consumes: `dagster_client.flow_runs`, `dagster_client.run_log_tail`, `flows_store.load_flows()`, `_publicise_dagster_links`.
- Produces: `GET /api/flow-runs?limit=` → JSON list of Task-1 rows + `flow_name`; `GET /api/flow-runs/<run_id>/log?cursor=` → Task-1 tail dict.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_flows_api.py`, reusing its client fixture)

```python
def test_api_flow_runs_enriches_flow_name(client, monkeypatch):
    import app as app_mod
    monkeypatch.setattr(app_mod.dagster_client, "flow_runs", lambda limit=50: [
        {"run_id": "r1", "job": "flow_nightly__a1", "flow_id": "a1",
         "status": "SUCCESS", "start_time": 1.0, "end_time": 2.0,
         "run_link": "http://127.0.0.1:3000/runs/r1"}])
    monkeypatch.setattr(app_mod.flows_store, "load_flows",
                        lambda: [{"id": "a1", "name": "Nightly"}])
    rows = client.get("/api/flow-runs").get_json()
    assert rows[0]["flow_name"] == "Nightly"


def test_api_flow_runs_falls_back_to_job_name(client, monkeypatch):
    import app as app_mod
    monkeypatch.setattr(app_mod.dagster_client, "flow_runs", lambda limit=50: [
        {"run_id": "r1", "job": "flow_gone__zz", "flow_id": "zz",
         "status": "FAILURE", "start_time": 1.0, "end_time": None,
         "run_link": None}])
    monkeypatch.setattr(app_mod.flows_store, "load_flows", lambda: [])
    rows = client.get("/api/flow-runs").get_json()
    assert rows[0]["flow_name"] == "flow_gone__zz"


def test_api_flow_run_log_passes_cursor(client, monkeypatch):
    import app as app_mod
    seen = {}
    def fake_tail(run_id, cursor=None):
        seen.update(run_id=run_id, cursor=cursor)
        return {"chunk": "x\n", "cursor": "c1", "has_more": False,
                "status": "SUCCESS", "error": None}
    monkeypatch.setattr(app_mod.dagster_client, "run_log_tail", fake_tail)
    r = client.get("/api/flow-runs/r9/log?cursor=c0").get_json()
    assert seen == {"run_id": "r9", "cursor": "c0"}
    assert r["chunk"] == "x\n"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python -m pytest tests/test_flows_api.py -q`
Expected: new tests FAIL with 404 (route missing); existing tests pass.

- [ ] **Step 3: Implement** (insert in `gui/app.py` directly after `api_dagster_flow_status`)

```python
@app.get("/api/flow-runs")
@api
def api_flow_runs():
    limit = min(request.args.get("limit", 50, type=int), 200)
    runs = dagster_client.flow_runs(limit=limit)
    names = {f["id"]: f["name"] for f in flows_store.load_flows()}
    for r in runs:
        r["flow_name"] = names.get(r["flow_id"]) or r["job"]
    return jsonify(_publicise_dagster_links(runs))


@app.get("/api/flow-runs/<run_id>/log")
@api
def api_flow_run_log(run_id):
    cursor = request.args.get("cursor") or None
    return jsonify(dagster_client.run_log_tail(run_id, cursor))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python -m pytest tests/test_flows_api.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add gui/app.py tests/test_flows_api.py
git commit -m "feat(gui): /api/flow-runs list and per-run log tail routes"
```

---

### Task 3: "Flow runs" tab in the Monitor page

**Files:**
- Modify: `gui/templates/logs.html`
- Test: `tests/test_run_iceberg_pages_render.py` (append a render assertion) — the JS itself is exercised manually (Step 5).

**Interfaces:**
- Consumes: `GET /api/flow-runs`, `GET /api/flow-runs/<run_id>/log?cursor=`; template helpers already in scope: `el`, `$$`, `esc`, `apiGet`, `fmtDate`, `createLogDash`, `mountTable` (see existing script block), `fmtDur` (defined in this file).
- Produces: a `flowruns` tab panel.

- [ ] **Step 1: Add a failing render test** (append to `tests/test_run_iceberg_pages_render.py`, following its existing page-render tests)

```python
def test_logs_page_has_flow_runs_tab(client):
    html = client.get("/logs").get_data(as_text=True)
    assert 'data-tab="flowruns"' in html
    assert 'data-panel="flowruns"' in html
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv\Scripts\python -m pytest tests/test_run_iceberg_pages_render.py -q`
Expected: new test FAILS on the missing `data-tab="flowruns"`.

- [ ] **Step 3: Implement the tab.** Three edits to `gui/templates/logs.html`:

3a. Add the tab button after the `files` button (line 9):

```html
  <button class="btn tab" data-tab="flowruns">Flow runs</button>
```

3b. Add the panel section after the `files` section (after line 48):

```html
<section data-panel="flowruns" hidden>
  <div class="panel" id="fr-log-panel" hidden>
    <div class="panel-head">
      <h2 id="fr-log-title">—</h2>
      <div class="row-flex">
        <a class="btn sm ghost" id="fr-dagster-link" target="_blank" rel="noopener">
          <i class="fa-solid fa-arrow-up-right-from-square"></i> Dagster UI</a>
        <button class="btn sm ghost" id="fr-toggle-raw"><i class="fa-solid fa-terminal"></i> Raw log</button>
      </div>
    </div>
    {% include "_dash.html" %}
    <div id="fr-log" class="console">—</div>
  </div>

  <div class="panel" style="margin-top:18px">
    <div class="panel-head">
      <h2>Scheduled flow runs <small id="fr-stat" class="muted"></small></h2>
      <div class="row-flex">
        <label class="checkbox" style="margin:0"><input type="checkbox" id="fr-auto"><span>auto-refresh</span></label>
        <button class="btn sm ghost" id="fr-refresh">↻</button>
      </div>
    </div>
    <div class="table-wrap" style="max-height:340px">
      <table>
        <thead><tr><th>Flow</th><th>Status</th><th>Started</th><th>Duration</th><th></th></tr></thead>
        <tbody id="fr-list"></tbody>
      </table>
    </div>
  </div>
</section>
```

Note: `_dash.html` renders element ids used by `createLogDash()`. Check whether
its ids are unique per include — if the include hardcodes ids (it is also
included in the `files` panel), reuse the *files* dash only for files and give
the flow-run panel a raw console without the parsed dash, OR parameterize the
include. **Resolution:** open `gui/templates/_dash.html` first; if it uses fixed
ids, skip the include here (drop that line) and only feed the console —
duplicate DOM ids are a bug. Note this in the commit message if dropped.

3c. Add the JS (before the final `loadFiles();` line in the script block):

```javascript
// ---------- Flow runs (Dagster) ----------
const FR_ACTIVE = new Set(["QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING"]);
let frRuns = [], frSel = null, frCursor = null, frStatus = null, frTimer = null, frBusy = false;
const frDash = createLogDash ? null : null;  // parsed dash only if _dash include kept

function frPill(s) {
  const cls = s === "SUCCESS" ? "ok" : (s === "FAILURE" ? "failed" : "warn");
  return `<span class="run-badge ${cls}">${esc(s || "—")}</span>`;
}
async function frLoadRuns() {
  try {
    frRuns = await apiGet("/api/flow-runs?limit=50");
    el("fr-stat").textContent = frRuns.length ? `${frRuns.length} runs` : "";
    el("fr-list").innerHTML = frRuns.map(r => `
      <tr class="clickable ${r.run_id === frSel ? "selected" : ""}" data-r="${esc(r.run_id)}">
        <td>${esc(r.flow_name)}</td>
        <td>${frPill(r.status)}</td>
        <td class="mono">${r.start_time ? fmtDate(new Date(r.start_time * 1000).toISOString()) : "—"}</td>
        <td class="mono">${r.end_time && r.start_time ? fmtDur((r.end_time - r.start_time) * 1000) : "—"}</td>
        <td>${r.run_link ? `<a href="${esc(r.run_link)}" target="_blank" rel="noopener">↗</a>` : ""}</td>
      </tr>`).join("") ||
      `<tr><td colspan="5" class="muted">No flow runs yet (is Dagster running?).</td></tr>`;
    $$("#fr-list tr.clickable").forEach(tr =>
      tr.onclick = () => frOpen(tr.dataset.r));
  } catch (e) { err(e.message); }
}
function frOpen(runId) {
  frSel = runId; frCursor = null; frStatus = null;
  const run = frRuns.find(r => r.run_id === runId);
  el("fr-log-panel").hidden = false;
  el("fr-log-title").textContent = run ? `${run.flow_name} — ${runId.slice(0, 8)}` : runId;
  el("fr-dagster-link").href = run?.run_link || "#";
  el("fr-log").textContent = "";
  $$("#fr-list tr.clickable").forEach(tr => tr.classList.toggle("selected", tr.dataset.r === runId));
  frTail();
}
async function frTail() {
  if (!frSel || frBusy) return;
  frBusy = true;
  try {
    // Drain: keep fetching while the server says there is more.
    for (let i = 0; i < 20; i++) {
      const r = await apiGet(`/api/flow-runs/${encodeURIComponent(frSel)}/log?cursor=${encodeURIComponent(frCursor || "")}`);
      if (r.error) { el("fr-log").textContent += `\n[${r.error}]`; break; }
      const c = el("fr-log");
      const atBottom = c.scrollTop + c.clientHeight >= c.scrollHeight - 30;
      if (r.chunk) c.textContent += r.chunk;
      if (atBottom) c.scrollTop = c.scrollHeight;
      frCursor = r.cursor; frStatus = r.status;
      if (!r.has_more) break;
    }
  } catch (e) { err(e.message); }
  finally { frBusy = false; }
}
el("fr-refresh").onclick = frLoadRuns;
el("fr-toggle-raw").onclick = () => el("fr-log-panel").classList.toggle("raw-hidden");
el("fr-auto").onchange = (e) => {
  clearInterval(frTimer);
  if (e.target.checked)
    frTimer = setInterval(() => {
      frLoadRuns();
      if (frSel && (frStatus === null || FR_ACTIVE.has(frStatus))) frTail();
    }, 3000);
};
```

And extend `showTab` (line 219) with:

```javascript
  if (tab === "flowruns") frLoadRuns();
```

If the `_dash.html` include was kept in 3b (unique ids confirmed), instantiate
`const frDash = createLogDash();` and feed it in `frTail` exactly as
`refreshFile` does (`frDash.feed(r.chunk); frDash.render();` and
`frDash.reset()` inside `frOpen`).

- [ ] **Step 4: Run the render test + full suite**

Run: `.venv\Scripts\python -m pytest tests/test_run_iceberg_pages_render.py -q` then `.venv\Scripts\python -m pytest tests -q`
Expected: all pass.

- [ ] **Step 5: Manual verify (end-to-end).** Start the GUI + Dagster (`start-app.ps1` or existing running instance), trigger a flow via "run now" on the Flows page, open Monitor → Flow runs: the run appears, status pill updates, clicking it streams the log live with auto-refresh on, and the Dagster UI link opens the same run.

- [ ] **Step 6: Commit**

```bash
git add gui/templates/logs.html tests/test_run_iceberg_pages_render.py
git commit -m "feat(gui): Flow runs tab on Monitor page with live Dagster log tail"
```

---

## Self-Review Notes

- Spec coverage: client functions (Task 1), routes + name enrichment + link publicising (Task 2), tab + live tail + dash-or-console (Task 3). Auto-refresh polls list and active-run tail — covers "live progress".
- `_dash.html` id-collision risk is resolved inside Task 3 Step 3b with an explicit decision rule.
- Types consistent: `run_log_tail` dict keys used verbatim in route test and JS (`chunk/cursor/has_more/status/error`).
