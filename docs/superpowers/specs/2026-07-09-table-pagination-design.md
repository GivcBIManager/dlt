# Design: Client-side pagination + 1000-record cap for GUI data tables

Date: 2026-07-09
Status: Approved (design) — pending spec review
Scope: `gui/` Flask control panel

## Goal

Add pagination to every **data table** in the GUI and guarantee no table loads
more than **1000 records** into the browser.

"Data tables" = tables that display query/record output. Small config &
metadata tables (connections, flows, dbt models, Iceberg schema/snapshots/table
list, saved pipelines, dashboard control) are explicitly **out of scope** and
left untouched.

## Context

The GUI is a Flask app: server-rendered Jinja templates + client-side JS that
fetches JSON from `/api/...` and writes tables into the DOM via `innerHTML`.

Every data table renders through one shared helper,
`renderTable(columns, rows, opts)` in `gui/static/app.js`, which returns an HTML
string. Grep confirms `renderTable` has exactly these call sites, all data
tables:

- `gui/templates/iceberg.html:259` — Iceberg row **sample** (primary large table)
- `gui/templates/iceberg.html:281` — Iceberg **aggregate / branch counts**
- `gui/templates/logs.html:163` — shared `render()` for the three **system tables**
  (`etl_dq_results`, `etl_run_log`, `etl_control`)
- `gui/templates/logs.html:187` — DQ **summary** (derived branch×table rollup)
- `gui/templates/logs.html:343` — **run detail** (units for one pipeline run)

No config/metadata table uses `renderTable`; they build their own `<tbody>`
inline. This makes the change surface small and well-bounded.

## Design decisions (confirmed with user)

- **Scope:** data tables only.
- **Page size:** 50 rows/page.
- **Pager controls:** First / Prev / Next / Last + an "X–Y of N" counter.
- **Pagination model:** client-side. Fetch up to 1000 rows once, page locally.
  This matches the app's existing pattern (all rendering is client-side from a
  single JSON payload) and avoids new stateful server endpoints.

## Architecture — one central paginator

Add a sibling helper to `renderTable` in `gui/static/app.js`:

```js
// Mount a paginated table into `container`. Reuses renderTable() per page.
// opts: { pillCols, numCols, pageSize = 50, cap = 1000 }
function mountTable(container, columns, rows, opts = {}) { ... }
```

Behavior:

1. **Cap:** slice `rows` to `opts.cap` (default 1000) as a hard backstop,
   regardless of how many rows arrived. Track whether truncation occurred.
2. **Render page:** render the current page's slice by calling the **existing**
   `renderTable(columns, pageSlice, opts)` — pill/number/formatting behavior is
   unchanged.
3. **Pager bar:** if `cappedRows.length > pageSize`, append a `.pager` bar:
   `⏮  ◀   51–100 of 1000   ▶  ⏭`. If truncated at the cap, the counter reads
   `… of 1000 (capped)`. If `cappedRows.length <= pageSize`, **no pager is
   shown** — small data tables (e.g. branch counts) look identical to today.
4. **State:** store the current page index on the container element
   (e.g. `container._page`). First/Prev/Next/Last handlers clamp the page and
   re-render **in place** (no refetch).
5. **Empty:** empty rows delegate to `renderTable`'s existing "No rows." output.

`renderTable()` itself is unchanged and still used internally per page.

## Call-site changes

Swap each `el(x).innerHTML = renderTable(...)` for
`mountTable(el(x), columns, rows, opts)`:

| Location | Notes |
|---|---|
| `iceberg.html` loadSample (~259) | Keep the "N row(s), M columns" info line above the table; mount the paginator into a child container. |
| `iceberg.html` loadAgg (~281) | Keep the "No rows" fallback for the no-`branch_id` case; otherwise mount. |
| `logs.html` SysTable.render (~163) | Pages over the **filtered** rows. Each `render()` re-mounts, resetting to page 1 — correct behavior on every filter change. The existing "X of N rows" count indicator stays. |
| `logs.html` renderSummary (~187) | Route through `mountTable`; self-hides its pager (usually small). |
| `logs.html` run detail (~343) | Route through `mountTable`. |

Config/metadata tables: **no change.**

## The 1000-record load cap

Enforce ≤1000 at both network and DOM layers:

- **Logs system tables:** client `?limit=2000` → `?limit=1000`
  (`logs.html:206–215`); server ceiling `min(limit, 2000)` → `min(limit, 1000)`
  (`app.py:477`).
- **Iceberg sample:** server ceiling `min(limit, 500)` → `min(limit, 1000)`
  (`app.py:435`). The `#limit` input keeps its default of 50 (a ceiling, not a
  target) and gains `max=1000`.
- **Run detail:** `read_run_detail` (`iceberg_browser.py:513`) currently returns
  all units for a run with no limit; cap its returned rows to 1000 so transfer
  and DOM both stay ≤1000.
- **`mountTable` cap = 1000** is the final backstop regardless of source.

Endpoints that feed non-table UI (e.g. `/api/iceberg/runs` → run *cards*) are
out of scope and unchanged.

## Styling

Add a small `.pager` block to `gui/static/style.css`: a flex row reusing the
existing button styles and muted-text counter. Theme-consistent; no new
dependencies.

## Testing / verification

No JS test framework exists for the GUI. Verify by running the app
(`start-app.ps1`) and exercising:

1. Iceberg sample with `limit=1000` — page through all 20 pages; First/Last jump
   works; counter is correct.
2. A Logs system table — apply a filter and confirm the pager resets to page 1
   and pages over the filtered set; confirm the fetch requests `limit=1000`.
3. A small data table (branch counts with a few branches) — confirm **no** pager
   renders and output is unchanged.
4. Confirm a config table (e.g. connections) is visually unchanged.

## Out of scope / YAGNI

- Server-side / cursor pagination (client-side over a 1000 cap is sufficient here).
- Page-size selector, sortable headers, per-column filtering beyond what exists.
- Pagination for config/metadata tables.
