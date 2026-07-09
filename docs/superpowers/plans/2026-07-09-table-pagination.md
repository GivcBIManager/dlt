# GUI Data-Table Pagination + 1000-Record Cap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add client-side pagination to every data table in the GUI and guarantee no table loads more than 1000 records into the browser.

**Architecture:** All data tables already render through one shared helper, `renderTable(columns, rows, opts)` in `gui/static/app.js`. We add a sibling helper `mountTable(container, columns, rows, opts)` that keeps page state on the container, renders one page at a time by reusing `renderTable()`, and shows a First/Prev/Next/Last pager only when rows exceed the page size. We switch the 5 data-table call sites to `mountTable`, and lower every data fetch ceiling to 1000 (client requests + Flask route ceilings + a run-detail server cap), with `mountTable`'s cap as the final backstop.

**Tech Stack:** Flask (Python) backend, Jinja2 templates, vanilla JS (no framework, no build step), plain CSS with CSS custom properties. Tests: pytest for Python; the GUI JS has **no** test harness — JS changes are verified by running the app and observing behavior.

## Global Constraints

- Page size: **50** rows/page (`pageSize = 50`).
- Hard cap: **1000** rows (`cap = 1000`) — enforced at fetch and DOM layers.
- Pager controls: **First / Prev / Next / Last** buttons + an **"X–Y of N"** counter. When truncated at the cap the counter reads `X–Y of 1000 (capped)`.
- Pagination is **client-side**: fetch up to 1000 rows once, page locally. No new server pagination endpoints.
- **Reuse** `renderTable()` for per-page markup — do not duplicate its pill/number/escaping logic.
- Scope is **data tables only** — the 5 `renderTable()` call sites. Config/metadata tables (connections, flows, dbt models, Iceberg schema/snapshots/table list, saved pipelines, dashboard control) build their own `<tbody>` inline and MUST NOT be changed.
- Match existing code style: vanilla JS with the `el()`/`esc()`/`$$()` helpers already in `app.js`; CSS via existing `--text-muted` / `--border` vars and `.btn.sm.ghost` button classes.
- No new dependencies, no build tooling.
- Run Python tests with: `python -m pytest <path> -v` from the repo root (`d:\dlt`). `pytest.ini` sets `testpaths = tests` and a repo-local basetemp; `tests/conftest.py` puts `gui/` on `sys.path`, so `import app` and `import iceberg_browser` work.

---

## File Structure

- **Modify** `gui/static/app.js` — add `mountTable()` helper (after `renderTable`).
- **Modify** `gui/static/style.css` — add `.pager` styles.
- **Modify** `gui/templates/iceberg.html` — `loadSample()` + `loadAgg()` call sites; `#limit` input `max`.
- **Modify** `gui/templates/logs.html` — `SysTable.render()`, `renderSummary()`, `loadDetail()` call sites; three endpoint `?limit=2000` → `?limit=1000`.
- **Modify** `gui/app.py` — sample route ceiling `500→1000`; system route ceiling `2000→1000`.
- **Modify** `gui/iceberg_browser.py` — cap `read_run_detail` rows to 1000.
- **Create** `tests/test_table_limits.py` — pytest for the two route ceilings + the run-detail cap.

---

## Task 1: Paginator helper, CSS, and Iceberg sample (flagship slice)

Delivers the reusable `mountTable()` + pager styling, proven end-to-end on the largest data table (Iceberg row sample), plus the server-side sample ceiling raise to 1000 with an automated test.

**Files:**
- Modify: `gui/static/style.css` (append `.pager` block)
- Modify: `gui/static/app.js` (add `mountTable` after `renderTable`, ends line 74)
- Modify: `gui/app.py:435` (sample ceiling)
- Modify: `gui/templates/iceberg.html:62` (`#limit` input) and `gui/templates/iceberg.html:252-261` (`loadSample`)
- Create: `tests/test_table_limits.py` (sample-ceiling test)

**Interfaces:**
- Produces: `mountTable(container, columns, rows, opts = {})` — `container` is a DOM element; `columns` is `string[]`; `rows` is `Array<object>`; `opts` = `{ pillCols?: string[], numCols?: string[], pageSize?: number = 50, cap?: number = 1000 }`. Renders into `container` (replacing its contents) and wires its own pager. No return value.
- Consumes: existing `renderTable(columns, rows, opts)` (unchanged, returns an HTML string).

- [ ] **Step 1: Add `.pager` styles to `gui/static/style.css`**

Append at the end of the file:

```css
/* Pagination bar for data tables (mountTable). */
.pager { display: flex; align-items: center; gap: 8px; margin-top: 10px; flex-wrap: wrap; }
.pager .pager-count { color: var(--text-muted); font-size: .8rem; font-variant-numeric: tabular-nums; }
.pager .btn.sm { padding: .2rem .55rem; line-height: 1; }
```

- [ ] **Step 2: Add `mountTable()` to `gui/static/app.js`**

Insert immediately after the `renderTable` function (after its closing `}` on line 74, before `let _toastTimer;`):

```js
// Mount a paginated table into `container` (a DOM element). Renders one page at
// a time by reusing renderTable(), so pill/number/escaping behavior is identical
// to a plain table. A First/Prev/Next/Last pager appears only when the (capped)
// row count exceeds pageSize, so small tables look exactly as before.
// opts: { pillCols, numCols, pageSize = 50, cap = 1000 }
function mountTable(container, columns, rows, opts = {}) {
  const pageSize = opts.pageSize || 50;
  const cap = opts.cap || 1000;
  const all = rows || [];
  const truncated = all.length > cap;
  const capped = truncated ? all.slice(0, cap) : all;
  const pages = Math.max(1, Math.ceil(capped.length / pageSize));
  let page = 0; // 0-based

  function pagerBar(start, shown) {
    const from = capped.length ? start + 1 : 0;
    const to = start + shown;
    const total = truncated ? `${cap} (capped)` : String(capped.length);
    const dis = (c) => (c ? "disabled" : "");
    return `<div class="pager">
      <button class="btn sm ghost" data-pg="first" ${dis(page === 0)} title="First">⏮</button>
      <button class="btn sm ghost" data-pg="prev" ${dis(page === 0)} title="Previous">◀</button>
      <span class="pager-count">${from}–${to} of ${total}</span>
      <button class="btn sm ghost" data-pg="next" ${dis(page >= pages - 1)} title="Next">▶</button>
      <button class="btn sm ghost" data-pg="last" ${dis(page >= pages - 1)} title="Last">⏭</button>
    </div>`;
  }

  function draw() {
    if (page < 0) page = 0;
    if (page > pages - 1) page = pages - 1;
    const start = page * pageSize;
    const slice = capped.slice(start, start + pageSize);
    let html = renderTable(columns, slice, opts);
    if (capped.length > pageSize) html += pagerBar(start, slice.length);
    container.innerHTML = html;
    container.querySelectorAll("[data-pg]").forEach((b) => {
      b.onclick = () => {
        const to = b.dataset.pg;
        if (to === "first") page = 0;
        else if (to === "prev") page -= 1;
        else if (to === "next") page += 1;
        else if (to === "last") page = pages - 1;
        draw();
      };
    });
  }

  draw();
}
```

- [ ] **Step 3: Raise the sample route ceiling in `gui/app.py`**

At `gui/app.py:435`, change:

```python
        table, limit=min(limit, 500), branch_id=branch_id, snapshot_id=snapshot_id,
```

to:

```python
        table, limit=min(limit, 1000), branch_id=branch_id, snapshot_id=snapshot_id,
```

- [ ] **Step 4: Cap the `#limit` input in `gui/templates/iceberg.html`**

At `gui/templates/iceberg.html:62`, change:

```html
          <div><label>Rows (preview)</label><input id="limit" type="number" value="50" style="width:90px"></div>
```

to:

```html
          <div><label>Rows (preview)</label><input id="limit" type="number" value="50" min="1" max="1000" style="width:90px"></div>
```

- [ ] **Step 5: Write the failing test for the sample ceiling**

Create `tests/test_table_limits.py`:

```python
"""Tests for the GUI's 1000-record load caps (data-table pagination)."""
from __future__ import annotations

import iceberg_browser as ib


def test_sample_endpoint_caps_limit_at_1000(monkeypatch):
    import app as gui_app
    seen = {}

    def fake_sample(table, limit=50, **kw):
        seen["limit"] = limit
        return {"columns": [], "rows": [], "snapshot_id": None}

    monkeypatch.setattr(gui_app.iceberg_browser, "sample_rows", fake_sample)
    resp = gui_app.app.test_client().get("/api/iceberg/tables/foo/sample?limit=5000")
    assert resp.status_code == 200
    assert seen["limit"] == 1000
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `python -m pytest tests/test_table_limits.py::test_sample_endpoint_caps_limit_at_1000 -v`
Expected: PASS (the ceiling change in Step 3 makes the route clamp 5000 → 1000).

- [ ] **Step 7: Switch `loadSample()` to `mountTable`**

At `gui/templates/iceberg.html:252-261`, replace the `loadSample` function body:

```js
async function loadSample() {
  if (!curTable) return;
  el("sample").innerHTML = `<div class="muted">Loading…</div>`;
  try {
    const d = await apiGet(`/api/iceberg/tables/${encodeURIComponent(curTable)}/sample?` + sampleQuery(true));
    el("sample").innerHTML =
      `<div class="muted" style="margin-bottom:8px">${d.rows.length} row(s), ${d.columns.length} columns${sampleSnapshot ? ` · snapshot ${esc(sampleSnapshot)}` : ""}</div>` +
      `<div class="sample-wrap"></div>`;
    mountTable(el("sample").querySelector(".sample-wrap"), d.columns, d.rows, {});
  } catch (e) { el("sample").innerHTML = `<div class="banner warn">${esc(e.message)}</div>`; }
}
```

(Only the last two lines of the `try` change: the table now mounts into a `.sample-wrap` child instead of being string-concatenated, preserving the existing `.sample-wrap .table-wrap` styling.)

- [ ] **Step 8: Verify in the running app (no JS test harness exists)**

Start the app: from `d:\dlt` run `pwsh -File start-app.ps1` (or `python gui/app.py`), open http://127.0.0.1:8765/iceberg.
- Pick a data table with >50 rows, set **Rows (preview)** to `1000`, click **Load**.
- Expected: 50 rows shown, a pager reading `1–50 of 1000` (or `… (capped)` if the table has >1000 rows). **Next**/**Last** advance; **Last** shows the final page; **First**/**Prev** are disabled on page 1; **Next**/**Last** are disabled on the last page.
- Set **Rows (preview)** to `20`, Load → 20 rows, **no pager** (≤ pageSize).
- Confirm no console errors.

- [ ] **Step 9: Commit**

```bash
git add gui/static/style.css gui/static/app.js gui/app.py gui/templates/iceberg.html tests/test_table_limits.py
git commit -m "feat(gui): add mountTable paginator + 1000-cap, wire Iceberg sample"
```

---

## Task 2: Iceberg aggregate / branch-counts table

Switches the second Iceberg data table (`loadAgg`) to the paginator, keeping its existing "No rows" fallback.

**Files:**
- Modify: `gui/templates/iceberg.html:268-284` (`loadAgg`)

**Interfaces:**
- Consumes: `mountTable(container, columns, rows, opts)` from Task 1.

- [ ] **Step 1: Switch the branch-counts render to `mountTable`**

At `gui/templates/iceberg.html:280-282`, replace:

```js
    el("branch-counts").innerHTML = d.rows.length
      ? renderTable(d.columns, d.rows, { numCols: ["branch_id", "rows"] })
      : `<div class="muted">No rows (the table may not have a branch_id column).</div>`;
```

with:

```js
    if (d.rows.length) {
      mountTable(el("branch-counts"), d.columns, d.rows, { numCols: ["branch_id", "rows"] });
    } else {
      el("branch-counts").innerHTML = `<div class="muted">No rows (the table may not have a branch_id column).</div>`;
    }
```

- [ ] **Step 2: Verify in the running app**

Open http://127.0.0.1:8765/iceberg, pick a data table, go to the **Branches** tab, **Compute** with "Aggregate by = Branch".
- Few branches (≤50): table renders, **no pager**.
- Aggregate by **Date** with day granularity on a long-lived table (>50 periods): pager appears and pages correctly.
- A table with no `branch_id` shows the "No rows…" message (unchanged).

- [ ] **Step 3: Commit**

```bash
git add gui/templates/iceberg.html
git commit -m "feat(gui): paginate Iceberg aggregate/branch-counts table"
```

---

## Task 3: Logs system tables + fetch-limit reduction

Switches the three Logs system tables (etl_dq_results, etl_run_log, etl_control), which share `SysTable.render()`, to the paginator and lowers their fetch ceiling to 1000 on both client and server.

**Files:**
- Modify: `gui/templates/logs.html:161-167` (`render`) and `gui/templates/logs.html:206,211,215` (endpoint limits)
- Modify: `gui/app.py:477` (system route ceiling)
- Modify: `tests/test_table_limits.py` (add system-ceiling test)

**Interfaces:**
- Consumes: `mountTable(container, columns, rows, opts)` from Task 1.

- [ ] **Step 1: Lower the system route ceiling in `gui/app.py`**

At `gui/app.py:477`, change:

```python
    return jsonify(iceberg_browser.read_system_table(table, limit=min(limit, 2000)))
```

to:

```python
    return jsonify(iceberg_browser.read_system_table(table, limit=min(limit, 1000)))
```

- [ ] **Step 2: Add the failing test for the system ceiling**

Append to `tests/test_table_limits.py`:

```python
def test_system_endpoint_caps_limit_at_1000(monkeypatch):
    import app as gui_app
    seen = {}

    def fake_sys(table, limit=200):
        seen["limit"] = limit
        return {"table": table, "columns": [], "rows": [], "total": 0}

    monkeypatch.setattr(gui_app.iceberg_browser, "read_system_table", fake_sys)
    resp = gui_app.app.test_client().get("/api/iceberg/system/etl_run_log?limit=5000")
    assert resp.status_code == 200
    assert seen["limit"] == 1000
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `python -m pytest tests/test_table_limits.py::test_system_endpoint_caps_limit_at_1000 -v`
Expected: PASS.

- [ ] **Step 4: Lower the three client fetch limits in `gui/templates/logs.html`**

At `gui/templates/logs.html:206`, `:211`, `:215`, change each `?limit=2000` to `?limit=1000`:

```js
const dqView = SysTable({
  endpoint: "/api/iceberg/system/etl_dq_results?limit=1000", barId: "dq-bar", outId: "dq",
  summaryOutId: "dq-summary", pillCols: ["status"],
  numCols: ["oracle_row_count","iceberg_row_count","row_count_delta","hash_matched","hash_mismatch","hash_total_delta","hash_delta_pct"],
});
const runlogView = SysTable({
  endpoint: "/api/iceberg/system/etl_run_log?limit=1000", barId: "runlog-bar", outId: "runlog",
  pillCols: ["status"], numCols: ["row_count","duration_ms","attempts"],
});
const controlView = SysTable({
  endpoint: "/api/iceberg/system/etl_control?limit=1000", barId: "control-bar", outId: "control",
  pillCols: ["status"], numCols: ["row_count","duration_ms"],
});
```

- [ ] **Step 5: Switch `SysTable.render()` to `mountTable`**

At `gui/templates/logs.html:161-167`, replace:

```js
  function render() {
    const rows = filtered();
    el(cfg.outId).innerHTML = renderTable(DATA.columns, rows, { pillCols: cfg.pillCols, numCols: cfg.numCols });
    const cnt = el(cfg.barId).querySelector('[data-f="count"]');
    if (cnt) cnt.textContent = `${rows.length} of ${DATA.rows.length} rows`;
    if (cfg.summaryOutId) renderSummary(rows);
  }
```

with:

```js
  function render() {
    const rows = filtered();
    mountTable(el(cfg.outId), DATA.columns, rows, { pillCols: cfg.pillCols, numCols: cfg.numCols });
    const cnt = el(cfg.barId).querySelector('[data-f="count"]');
    if (cnt) cnt.textContent = `${rows.length} of ${DATA.rows.length} rows`;
    if (cfg.summaryOutId) renderSummary(rows);
  }
```

(Each filter change calls `render()`, which re-mounts and resets to page 1 — the correct behavior.)

- [ ] **Step 6: Verify in the running app**

Open http://127.0.0.1:8765/logs, open the **DQ results** / **Run log** / **Control** tabs.
- A table with >50 rows shows a pager; **Next**/**Last** page through; counter matches.
- Apply a filter (branch/table/status/date) → the pager **resets to page 1** and pages over the filtered set; the `X of N rows` count in the bar still updates.
- In DevTools Network, confirm the request URL is `...?limit=1000`.
- Confirm no console errors.

- [ ] **Step 7: Commit**

```bash
git add gui/app.py gui/templates/logs.html tests/test_table_limits.py
git commit -m "feat(gui): paginate Logs system tables, cap fetch at 1000"
```

---

## Task 4: Logs DQ summary + run detail, and run-detail server cap

Switches the last two `renderTable()` call sites (the DQ summary rollup and the per-run detail table) to the paginator, and caps `read_run_detail` server-side at 1000 rows with an automated test.

**Files:**
- Modify: `gui/iceberg_browser.py:513-530` (`read_run_detail`)
- Modify: `gui/templates/logs.html:187` (`renderSummary`) and `gui/templates/logs.html:337-348` (`loadDetail`)
- Modify: `tests/test_table_limits.py` (add run-detail cap test)

**Interfaces:**
- Consumes: `mountTable(container, columns, rows, opts)` from Task 1.
- Modifies: `read_run_detail(run_id)` — return value shape unchanged (`{run_id, columns, rows}`); `rows` now capped at 1000.

- [ ] **Step 1: Cap `read_run_detail` rows in `gui/iceberg_browser.py`**

At `gui/iceberg_browser.py:526-530`, replace the return block:

```python
    return {
        "run_id": run_id,
        "columns": RUN_DETAIL_COLUMNS,
        "rows": _run_detail_rows(log_rows, control_rows),
    }
```

with:

```python
    return {
        "run_id": run_id,
        "columns": RUN_DETAIL_COLUMNS,
        "rows": _run_detail_rows(log_rows, control_rows)[:1000],
    }
```

- [ ] **Step 2: Add the failing test for the run-detail cap**

Append to `tests/test_table_limits.py`:

```python
def test_run_detail_caps_rows_at_1000(monkeypatch):
    # 1500 units for one run -> the response is capped to 1000 rows.
    logs = [
        {
            "pipeline_run_id": "r1", "table_name": f"T{i}", "branch_id": i,
            "load_mode": "INCREMENTAL", "row_count": 1, "status": "SUCCESS",
            "start_time": None, "end_time": None,
            "schema_discrepancy": None, "error_details": None, "recorded_at": None,
        }
        for i in range(1500)
    ]

    def fake_scan(table):
        return {"etl_run_log": logs, "etl_control": []}[table]

    monkeypatch.setattr(ib, "_scan_pylist", fake_scan)
    out = ib.read_run_detail("r1")
    assert len(out["rows"]) == 1000
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `python -m pytest tests/test_table_limits.py::test_run_detail_caps_rows_at_1000 -v`
Expected: PASS.

- [ ] **Step 4: Switch `renderSummary()` to `mountTable`**

At `gui/templates/logs.html:187`, replace:

```js
    box.innerHTML = renderTable(["branch", "table_name", "checks", "status_breakdown"], sumRows, { numCols: ["checks"] });
```

with:

```js
    mountTable(box, ["branch", "table_name", "checks", "status_breakdown"], sumRows, { numCols: ["checks"] });
```

(The earlier `box.innerHTML = ...` "No branch/table columns" early return on line 170 stays as-is.)

- [ ] **Step 5: Switch `loadDetail()` to `mountTable`**

At `gui/templates/logs.html:343`, replace:

```js
      box.innerHTML = renderTable(r.columns, r.rows, { pillCols: RUN_DETAIL_PILLS, numCols: RUN_DETAIL_NUMS });
```

with:

```js
      mountTable(box, r.columns, r.rows, { pillCols: RUN_DETAIL_PILLS, numCols: RUN_DETAIL_NUMS });
```

- [ ] **Step 6: Run the whole new test file**

Run: `python -m pytest tests/test_table_limits.py -v`
Expected: all three tests PASS.

- [ ] **Step 7: Verify in the running app**

Open http://127.0.0.1:8765/logs.
- **Monitor / Runs**: expand a run with many units → the run-detail table paginates (or shows no pager if ≤50 units); expand a run with ≤50 units → no pager, unchanged.
- **DQ results** tab: the summary table paginates only if there are >50 branch×table combos; otherwise unchanged.
- Confirm no console errors.

- [ ] **Step 8: Commit**

```bash
git add gui/iceberg_browser.py gui/templates/logs.html tests/test_table_limits.py
git commit -m "feat(gui): paginate Logs summary + run detail, cap run detail at 1000"
```

---

## Final verification

- [ ] Run the full suite: `python -m pytest -q` from `d:\dlt` — expected: no new failures; `tests/test_table_limits.py` passes (3 tests).
- [ ] Grep confirms no stray `renderTable(` remains as a direct `innerHTML` assignment for a data table:
  `git grep -n "innerHTML = renderTable" gui/templates` — expected: **no matches** (all data tables now go through `mountTable`).
- [ ] Manual sweep with the app running: Iceberg sample, Iceberg branch-counts, Logs 3 system tables, Logs DQ summary, Logs run detail all paginate at 50/page with a working First/Prev/Next/Last pager; config/metadata tables (connections, flows, dbt, schema, snapshots) look unchanged.
