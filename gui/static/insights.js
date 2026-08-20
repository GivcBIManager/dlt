/* Monitor -> Insights tab: the ETL run-log analytics dashboard.
 *
 * One request (/api/insights/run-log) returns every cut the page draws, so a
 * filter change is a single round trip and a single re-render -- no per-chart
 * fetching and no client-side scan of the raw log. Charts are static SVG built
 * by static/charts.js: after the innerHTML write nothing runs until the next
 * filter change, so an open dashboard costs nothing.
 *
 * The page is organised as a fixed sequence of SECTIONS, each a grid with an
 * explicit column count (see `SECTIONS` below). Cards inside a section are
 * always the same shape and stretch to a common height, so every row lines up
 * on both edges instead of ragging the way an auto-fit grid does when the last
 * row is short. Nothing here is sized in pixels: the SVGs scale through their
 * viewBox, so a two-up section reflows to one-up on a narrow screen and the
 * alignment survives.
 *
 * Layout rules followed here: one filter row above everything it scopes, KPI
 * tiles before charts, a legend on every multi-series chart, and a data-table
 * twin behind each chart's "Data" disclosure (rendered lazily on first open) so
 * no value is reachable only through a tooltip.
 */
const insightsView = (function () {
  let DATA = null;
  let inFlight = false;
  const state = { days: 30, branch: "", table: "", table_type: "", load_mode: "", status: "" };

  const STATUS_COLORS = {
    SUCCESS: VIZ.good, PARTIAL: VIZ.warning, FAILED: VIZ.critical,
    SKIPPED: VIZ.neutral, UNKNOWN: VIZ.neutral,
  };
  const statusColor = (key, i) => STATUS_COLORS[String(key).toUpperCase()] || VIZ.cat[i % VIZ.cat.length];

  const WINDOWS = [[1, "Last 24 hours"], [7, "Last 7 days"], [30, "Last 30 days"],
                   [90, "Last 90 days"], [365, "Last year"], [0, "All history"]];

  /* The three durations the dashboard separates. Each keeps ONE hue everywhere
   * it appears -- card, branch bar, trend line, heat map -- so "the orange one"
   * means the read phase on every chart on the page. */
  const METRICS = [
    { id: "total", label: "Total duration", color: VIZ.cat[0],
      hint: "Oracle read plus Iceberg load, per table×branch." },
    { id: "read", label: "Read duration", color: VIZ.cat[1],
      hint: "Extracting the rows from Oracle and staging them." },
    { id: "load", label: "Load duration", color: VIZ.cat[2],
      hint: "Committing the table to Iceberg (shared by the table's branches)." },
  ];
  const RECORDS_COLOR = VIZ.cat[4];

  const fmtPct = (v) => (v === null || v === undefined ? "—" : `${Number(v).toFixed(1)}%`);
  const bucketWord = () => (DATA && DATA.granularity === "hour" ? "hour" : "day");

  /* ------------------------------------------------------------ filter bar */
  function buildBar() {
    const f = DATA.facets || {};
    const opt = (value, label, current) =>
      `<option value="${esc(value)}"${String(current) === String(value) ? " selected" : ""}>${esc(label)}</option>`;
    const sel = (field, label, options, current) =>
      `<div><label>${esc(label)}</label><select data-i="${field}">` +
      `<option value="">All</option>` +
      options.map(o => opt(o.value, o.label, current)).join("") +
      `</select></div>`;
    const plain = (values) => (values || []).map(v => ({ value: v, label: v }));

    el("ins-bar").innerHTML =
      `<div><label>Window</label><select data-i="days">` +
        WINDOWS.map(([d, lab]) => opt(d, lab, state.days)).join("") +
      `</select></div>` +
      // The run log stores a numeric branch id; the reader picks a NAME. The
      // option value stays the id so the query still matches stored rows.
      sel("branch", "Branch", f.branch || [], state.branch) +
      sel("table_type", "Table type", plain(f.table_type), state.table_type) +
      sel("table", "Table", plain(f.table_name), state.table) +
      sel("load_mode", "Load mode", plain(f.load_mode), state.load_mode) +
      sel("status", "Unit status", plain(f.status), state.status) +
      `<button class="btn ghost sm" data-i="clear" style="align-self:flex-end">Clear</button>` +
      `<span class="spacer"></span><span class="muted" id="ins-window"></span>`;

    el("ins-bar").querySelectorAll("select").forEach(node => node.onchange = () => {
      state[node.dataset.i] = node.dataset.i === "days" ? +node.value : node.value;
      load();
    });
    el("ins-bar").querySelector('[data-i="clear"]').onclick = () => {
      state.branch = state.table = state.table_type = state.load_mode = state.status = "";
      load();
    };
    // Scope counts live here rather than in a tile, so the tile grid stays a
    // clean 4x2 of measurements and this line answers "what am I looking at".
    const w = DATA.window || {}, k = DATA.kpi;
    el("ins-window").textContent =
      `${fmtNum(w.rows_scanned)} log rows scanned` +
      (w.truncated ? ` (capped at ${fmtNum(w.cap)})` : "") +
      ` · ${fmtNum(k.tables)} tables · ${fmtNum(k.branches)} branches · ${fmtNum(k.units)} loads` +
      (k.last_at ? ` · newest ${fmtDate(k.last_at)}` : "");
  }

  /* ---------------------------------------------------------------- tiles */
  function tile(label, value, sub) {
    return `<div class="stat-tile">
      <div class="stat-label">${esc(label)}</div>
      <div class="stat-value">${value}</div>
      <div class="stat-sub">${sub || ""}</div>
    </div>`;
  }

  // Every duration card carries the same four statistics in the same order, so
  // the three of them read as one block rather than three unrelated tiles.
  function durationTile(metric) {
    const k = DATA.kpi;
    const stat = (s) => `<span class="stat-stat"><em>${s}</em>${vizDur(k[`${metric.id}_${s}_ms`])}</span>`;
    return tile(`Avg ${metric.label.toLowerCase()}`, vizDur(k[`${metric.id}_avg_ms`]),
      k[`${metric.id}_n`]
        ? `<span class="stat-stats">${stat("p50")}${stat("p95")}${stat("max")}</span>`
        : `<span class="muted">not recorded</span>`);
  }

  function renderKpi() {
    const k = DATA.kpi;
    const t = DATA.trend || [];
    const badge = (n, label, cls) => (n ? `<span class="run-badge ${cls}">${fmtNum(n)} ${label}</span>` : "");
    el("ins-kpi").innerHTML =
      tile("Pipeline runs", fmtNum(k.runs),
        badge(k.runs_ok, "clean", "ok") + badge(k.runs_partial, "partial", "warn") +
        badge(k.runs_failed, "failed", "failed") ||
        `<span class="muted">no runs in this window</span>`) +
      tile("Records processed", fmtNum(k.rows),
        vizSpark(t.map(d => d.rows), { color: RECORDS_COLOR }) +
        `<span class="muted">${fmtNum(k.rows_per_run)} per run</span>`) +
      tile("Unit success rate", fmtPct(k.success_rate),
        vizMeter(k.success_rate, { label: "unit success rate" }) +
        `<span class="muted">${fmtNum(k.units_ok)} of ${fmtNum(k.units)} loads</span>`) +
      // tile() escapes the label, so it takes the raw character, not an entity.
      tile("Failed loads & runs", `${fmtNum(k.units_failed)} <small>/ ${fmtNum(k.runs_failed + k.runs_partial)} runs</small>`,
        badge(k.retries, "retried", "warn") + badge(k.drift, "schema drift", "warn") ||
        `<span class="muted">no retries or drift</span>`) +
      // Row two is the four durations together: the whole pipeline run first
      // (wall clock, the number anyone asks for), then the per-load breakdown it
      // decomposes into. Scope counts moved to the scanned-window line, which
      // keeps this row at four and the grid exactly 4x2.
      tile("Avg pipeline run", vizDur(k.run_wall_avg_ms),
        k.runs
          ? `<span class="stat-stats">
               <span class="stat-stat"><em>p95</em>${vizDur(k.run_wall_p95_ms)}</span>
               <span class="stat-stat"><em>max</em>${vizDur(k.run_wall_max_ms)}</span>
               <span class="stat-stat"><em>runs</em>${fmtNum(k.runs)}</span>
             </span>`
          : `<span class="muted">no runs in this window</span>`) +
      METRICS.map(durationTile).join("");
  }

  /* --------------------------------------------------------------- charts */
  /* "Not measured" and "measured as zero" are different answers, and a bar
   * chart cannot tell them apart -- an unrecorded phase would draw a row of
   * 0ms bars that reads as "instant". So a card whose metric has no recorded
   * value anywhere in the slice says that instead of drawing anything. */
  const measured = (metric) => !!DATA.kpi[`${metric.id}_n`];
  const unmeasured = (metric) =>
    `<div class="viz-empty">No ${esc(metric.label.toLowerCase())} recorded for this slice.</div>`;

  /* One ranked bar chart of avg <metric>, cut by branch or by table.
   *
   * Which dimension each metric uses is not a style choice. The Iceberg commit
   * is per TABLE -- every branch of a table shares one commit, so its load time
   * is the same number repeated down a branch axis, and total (read + load)
   * inherits most of that flatness. Only the Oracle read is genuinely per
   * branch. So load and total are cut by table, read by branch. */
  const DIMENSIONS = {
    branch: { label: "branch", source: () => DATA.by_branch || [],
              name: (r) => r.label, labelW: 132, idCol: "label" },
    table: { label: "table", source: () => DATA.by_table || [],
             name: (r) => r.key, labelW: 178, idCol: "key" },
  };
  const TOP_BARS = 10;

  function durationCard(metric, dimKey) {
    const dim = DIMENSIONS[dimKey];
    const avg = `${metric.id}_avg_ms`;
    return {
      title: `Avg ${metric.label.toLowerCase()} by ${dim.label}`,
      sub: `${metric.hint} Slowest ${TOP_BARS}; the tick marks p95.`,
      // Ranked by DURATION over the whole list, not by volume: there are ~90
      // tables, and taking the busiest ten first would hide a small table that
      // happens to be the slowest thing on the platform -- the one row this
      // chart exists to surface.
      body: () => (measured(metric)
        ? vizBars(
          [...dim.source()].sort((a, b) => (b[avg] || 0) - (a[avg] || 0)).slice(0, TOP_BARS)
            .map(r => ({
              label: dim.name(r), value: r[avg] || 0, mark: r[`${metric.id}_p95_ms`] || 0,
              note: `${fmtNum(r.units)} loads · ${fmtNum(r.rows)} rows`,
            })),
          { fmt: vizDur, valueLabel: "avg", markLabel: "p95", color: metric.color, labelW: dim.labelW })
        : unmeasured(metric)),
      table: () => renderTable(
        [dim.idCol, "units", "rows", `${metric.id}_avg_ms`, `${metric.id}_p50_ms`,
         `${metric.id}_p95_ms`, `${metric.id}_max_ms`], dim.source(),
        { numCols: ["units", "rows", `${metric.id}_avg_ms`, `${metric.id}_p50_ms`,
                    `${metric.id}_p95_ms`, `${metric.id}_max_ms`] }),
    };
  }

  // Duration against volume on one timeline: the duration on the left axis, the
  // records that produced it on the right. Both dotted -- these are per-bucket
  // aggregates, not a continuously measured signal.
  function trendVsRecordsCard(metric) {
    const t = DATA.trend || [];
    return {
      title: `Avg ${metric.label.toLowerCase()} vs records`,
      sub: `Per ${bucketWord()}. Duration reads the left axis, records the right.`,
      body: () => (measured(metric)
        ? vizLines(t, {
          x: "label", fmt: vizDur, fmtRight: vizCompact,
          series: [
            { key: `${metric.id}_avg_ms`, label: `Avg ${metric.label.toLowerCase()}`, color: metric.color, marker: true },
            { key: "rows", label: "Records", color: RECORDS_COLOR, marker: true, axis: "right" },
          ],
        })
        : unmeasured(metric)),
      table: () => renderTable(["label", `${metric.id}_avg_ms`, `${metric.id}_p95_ms`, "rows", "units"], t,
        { numCols: [`${metric.id}_avg_ms`, `${metric.id}_p95_ms`, "rows", "units"] }),
    };
  }

  function heatCard(metric) {
    const heat = DATA.heatmap || { branches: [], tables: [], cells: [] };
    const idx = new Map((heat.cells || []).map(c => [c.branch + "\t" + c.table, c]));
    const at = (b, t) => {
      const cell = idx.get(b + "\t" + t);
      const v = cell ? cell[`${metric.id}_avg_ms`] : null;
      return v === null || v === undefined ? null : v;
    };
    return {
      title: `Avg ${metric.label.toLowerCase()}: table × branch`,
      sub: `Busiest ${(heat.tables || []).length} tables across every branch; darker means slower.`,
      // A wider canvas than the standard chart box: this is a full-width card
      // with only a handful of branch rows, and the extra width per column is
      // what lets each cell keep its printed value instead of going colour-only.
      body: () => (measured(metric)
        ? vizHeat(heat.branches || [], heat.tables || [], {
          value: at, valueLabel: "avg", fmt: vizDur, width: 1240,
        })
        : unmeasured(metric)),
      table: () => renderTable(
        ["branch", "table", "units", "rows", `${metric.id}_avg_ms`, `${metric.id}_p95_ms`],
        heat.cells || [],
        { numCols: ["units", "rows", `${metric.id}_avg_ms`, `${metric.id}_p95_ms`] }),
    };
  }

  /* ------------------------------------------------- duration tree (by table) */
  /* A three-level table: table type -> table -> branch, each level collapsible.
   *
   * One <table> rather than nested ones, because the whole point is that a
   * branch row's numbers line up under its table's and its type's. Rows are all
   * rendered up front and shown/hidden by class -- at ~90 tables x 8 branches
   * the DOM is small enough that toggling beats rebuilding, and it keeps expand
   * state from being lost on every click. Groups start collapsed: the type
   * summary is the overview, and you open only the branch you are chasing. */
  const TREE_COLS = [
    { key: "units", label: "Loads", num: true, fmt: fmtNum },
    { key: "rows", label: "Records", num: true, fmt: fmtNum },
    { key: "total_avg_ms", label: "Avg total", num: true, fmt: vizDur },
    { key: "read_avg_ms", label: "Avg read", num: true, fmt: vizDur },
    { key: "load_avg_ms", label: "Avg load", num: true, fmt: vizDur },
    { key: "failed", label: "Failed", num: true, fmt: fmtNum },
  ];

  function treeModel() {
    const byTable = [...(DATA.by_table || [])]
      .sort((a, b) => (b.total_avg_ms || 0) - (a.total_avg_ms || 0));
    const branchesOf = new Map();
    (DATA.by_table_branch || []).forEach(leaf => {
      if (!branchesOf.has(leaf.table)) branchesOf.set(leaf.table, []);
      branchesOf.get(leaf.table).push(leaf);
    });
    const types = new Map();
    byTable.forEach(t => {
      const key = t.table_type || "UNKNOWN";
      if (!types.has(key)) types.set(key, []);
      types.get(key).push(t);
    });
    // Type rows come from the server's own table_type rollup, so a type's
    // average is the average over its loads -- not an average of averages.
    const typeStats = new Map((DATA.by_table_type || []).map(e => [e.key, e]));
    return [...types.entries()]
      .map(([key, tables]) => ({ key, stats: typeStats.get(key) || {}, tables, branchesOf }))
      .sort((a, b) => (b.stats.rows || 0) - (a.stats.rows || 0));
  }

  function mountDurationTree(node) {
    const groups = treeModel();
    if (!groups.length) { node.innerHTML = `<div class="muted">No rows.</div>`; return; }

    const cells = (row) => TREE_COLS.map(c =>
      `<td class="num">${row[c.key] === null || row[c.key] === undefined ? "—" : c.fmt(row[c.key])}</td>`).join("");
    const twisty = `<i class="tree-twisty" aria-hidden="true"></i>`;

    let html = `<div class="table-wrap tree-wrap"><table class="tree-table">
      <thead><tr><th>Table type / table / branch</th>` +
      TREE_COLS.map(c => `<th class="num">${esc(c.label)}</th>`).join("") + `</tr></thead><tbody>`;
    groups.forEach((g, gi) => {
      html += `<tr class="tree-row lvl-type" data-toggle="type" data-g="${gi}" tabindex="0" aria-expanded="false">
        <td>${twisty}<span class="tree-label">${esc(g.key)}</span>
        <span class="tree-count">${fmtNum(g.tables.length)} tables</span></td>${cells(g.stats)}</tr>`;
      g.tables.forEach((t, ti) => {
        const leaves = g.branchesOf.get(t.key) || [];
        html += `<tr class="tree-row lvl-table hidden" data-g="${gi}" data-t="${ti}"` +
          (leaves.length ? ` data-toggle="table" tabindex="0" aria-expanded="false"` : "") + `>
          <td>${leaves.length ? twisty : `<i class="tree-twisty leaf"></i>`}<span class="tree-label mono">${esc(t.key)}</span>
          <span class="tree-count">${fmtNum(leaves.length)} branches</span></td>${cells(t)}</tr>`;
        leaves.forEach(leaf => {
          html += `<tr class="tree-row lvl-branch hidden" data-g="${gi}" data-t="${ti}">
            <td><i class="tree-twisty leaf"></i><span class="tree-label">${esc(leaf.branch)}</span></td>${cells(leaf)}</tr>`;
        });
      });
    });
    node.innerHTML = html + `</tbody></table></div>`;

    const rows = Array.from(node.querySelectorAll("tr.tree-row"));
    const toggle = (row) => {
      const open = row.getAttribute("aria-expanded") !== "true";
      row.setAttribute("aria-expanded", String(open));
      const g = row.dataset.g, t = row.dataset.t;
      rows.forEach(other => {
        if (other === row || other.dataset.g !== g) return;
        if (row.dataset.toggle === "type") {
          // Collapsing a type hides its tables AND any branches they had open;
          // re-opening it shows the tables again, collapsed, so the reader never
          // gets a type that expands into a wall of branch rows.
          if (other.classList.contains("lvl-table")) {
            other.classList.toggle("hidden", !open);
            if (!open) other.setAttribute("aria-expanded", "false");
          } else if (other.classList.contains("lvl-branch")) {
            other.classList.add("hidden");
          }
        } else if (other.dataset.t === t && other.classList.contains("lvl-branch")) {
          other.classList.toggle("hidden", !open);
        }
      });
    };
    rows.filter(r => r.dataset.toggle).forEach(row => {
      row.onclick = () => toggle(row);
      row.onkeydown = (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(row); }
      };
    });
  }

  /* The page, in order. Each section names its column count; every card in it
   * is the same shape, which is what keeps the rows flush. */
  function sections() {
    const k = DATA.kpi;
    const t = DATA.trend || [];
    return [{
      title: "Volume and outcome",
      cols: 2,
      cards: [{
        title: "Runs by outcome",
        sub: "A run is clean when every table×branch load in it succeeded.",
        body: () => vizDonut((DATA.runs_by_status || []).map((s, i) => ({
          label: s.key, value: s.n, color: statusColor(s.key, i),
        })), { centerValue: vizCompact(k.runs), centerLabel: "runs" }),
        table: () => renderTable(["key", "n"], DATA.runs_by_status || [], { numCols: ["n"] }),
      }, {
        title: "Records by table type",
        sub: "Where the processed volume sits — masters, transactions or snapshots.",
        body: () => vizDonut((DATA.rows_by_table_type || []).slice(0, 6).map((s, i) => ({
          label: s.key, value: s.n, color: VIZ.cat[i % VIZ.cat.length],
        })), { centerValue: vizCompact(k.rows), centerLabel: "records" }),
        table: () => renderTable(["key", "units", "tables", "rows", "success_rate"],
          DATA.by_table_type || [], { numCols: ["units", "tables", "rows", "success_rate"] }),
      }],
    }, {
      // Each metric is cut by the dimension it actually varies along -- see
      // DIMENSIONS above for why load and total are per table, read per branch.
      // Order is read, load, then total on the right: the two phases first and
      // the sum they add up to last, so the row reads left to right as an
      // equation rather than as three unrelated charts.
      title: "Average durations",
      cols: 3,
      cards: [durationCard(METRICS[1], "branch"),
              durationCard(METRICS[2], "table"),
              durationCard(METRICS[0], "table")],
    }, {
      title: "Trends over time",
      cols: 2,
      cards: [{
        // The headline trend, so it spans the row: this is wall clock for the
        // whole pipeline, not the per-load durations the other cards break out.
        // Runs share the right axis -- a run-duration spike means something very
        // different when the run count moved with it.
        title: "Avg pipeline run duration over time",
        wide: true,
        sub: `Wall clock from a run's first load starting to its last finishing, averaged per ${bucketWord()}.`,
        body: () => vizLines(t, {
          x: "label", fmt: vizDur, fmtRight: vizCompact, height: 250,
          series: [
            { key: "wall_avg_ms", label: "Avg pipeline run", color: VIZ.cat[3], marker: true },
            { key: "runs", label: "Runs", color: VIZ.neutral, marker: true, axis: "right" },
          ],
        }),
        table: () => renderTable(
          ["label", "runs", "runs_ok", "runs_partial", "runs_failed", "wall_avg_ms", "rows"], t,
          { numCols: ["runs", "runs_ok", "runs_partial", "runs_failed", "wall_avg_ms", "rows"] }),
      }, {
        title: "Unit success rate over time",
        sub: `Share of table×branch loads that succeeded, per ${bucketWord()}.`,
        body: () => vizLines(t, {
          x: "label", fmt: fmtPct,
          series: [{ key: "success_rate", label: "Unit success rate", color: VIZ.good, marker: true }],
        }),
        table: () => renderTable(["label", "units", "ok", "failed", "success_rate"], t,
          { numCols: ["units", "ok", "failed", "success_rate"] }),
      }].concat(METRICS.map(trendVsRecordsCard)),
    }, {
      // Read only. A load-time heat map would repeat one value across every
      // branch of a table (one commit covers them all), and a total-time one
      // would mostly repeat it too -- neither grid would carry information.
      title: "Read duration heat map",
      cols: 1,
      cards: [heatCard(METRICS[1])],
    }, {
      title: "By table",
      cols: 1,
      cards: [{
        title: "Duration by table and table type",
        sub: "Table type → table → branch. Every level collapses; each row is the "
           + "average of the rows beneath it.",
        mount: (node) => mountDurationTree(node),
      }],
    }];
  }

  function renderCharts() {
    const built = [];
    const note = loadCoverageNote();
    const html = sections().map(section => {
      const cards = section.cards.map(card => {
        const i = built.push(card) - 1;
        return `<div class="viz-card${card.wide ? " wide" : ""}">
          <div class="viz-head"><h3>${esc(card.title)}</h3>${card.sub ? `<p>${esc(card.sub)}</p>` : ""}</div>
          <div class="viz-body" data-mount="${i}">${card.body ? card.body() : ""}</div>
          ${card.table ? `<details class="viz-twin" data-card="${i}">
            <summary>Data table</summary><div class="viz-twin-body"><div class="muted">Loading…</div></div>
          </details>` : ""}
        </div>`;
      }).join("");
      return `<section class="viz-section">
        <h4 class="viz-section-head">${esc(section.title)}</h4>
        <div class="viz-grid cols-${section.cols}">${cards}</div>
      </section>`;
    }).join("");
    el("ins-charts").innerHTML = note + html;

    // Cards that render a live DOM widget (the paginated table) rather than a
    // static SVG string mount themselves once the markup is in the document.
    $$("#ins-charts [data-mount]").forEach(node => {
      const card = built[+node.dataset.mount];
      if (card && card.mount) card.mount(node);
    });
    // The table twins are the accessible equivalent of each chart, but building
    // them all up front would double the render cost, so they fill in on open.
    $$("#ins-charts details.viz-twin").forEach(node => node.addEventListener("toggle", () => {
      if (!node.open || node.dataset.done) return;
      node.dataset.done = "1";
      node.querySelector(".viz-twin-body").innerHTML = built[+node.dataset.card].table();
    }, { once: false }));
  }

  // The load/total durations only exist for runs recorded by a build that
  // measures the write phase. Saying so beats drawing three empty charts and
  // leaving the reader to guess whether the pipeline is instant or unmeasured.
  function loadCoverageNote() {
    const c = DATA.coverage || {};
    if (!c.units || c.with_load_ms) return "";
    return `<div class="banner warn viz-note" role="status">
      <i class="fa-solid fa-circle-info"></i>
      No load-phase timings in this window — the read duration is all that was recorded for these runs.
      Load and total durations fill in from the next pipeline run onward.
    </div>`;
  }

  function renderTables() {
    el("ins-slow").innerHTML = renderTable(
      ["table", "table_type", "branch", "total_ms", "read_ms", "load_ms", "rows", "status", "when"],
      DATA.slowest || [],
      { pillCols: ["status"], numCols: ["total_ms", "read_ms", "load_ms", "rows"] });
  }

  function render() {
    if (!DATA) return;
    buildBar();
    renderKpi();
    renderCharts();
    renderTables();
  }

  async function load() {
    if (inFlight) return;
    inFlight = true;
    const panel = el("ins-panels");
    panel.classList.add("viz-loading");   // hold the old render, no skeleton flash
    const q = new URLSearchParams({
      days: state.days, branch: state.branch, table: state.table,
      table_type: state.table_type, load_mode: state.load_mode, status: state.status,
    });
    try {
      DATA = await apiGet(`/api/insights/run-log?${q}`);
      el("ins-error").hidden = true;
      render();
    } catch (e) {
      el("ins-error").hidden = false;
      el("ins-error").textContent = e.message;
    } finally {
      inFlight = false;
      panel.classList.remove("viz-loading");
    }
  }

  return { load, render, get data() { return DATA; } };
})();
