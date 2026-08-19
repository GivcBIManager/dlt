/* Monitor -> Insights tab: the ETL run-log analytics dashboard.
 *
 * One request (/api/insights/run-log) returns every cut the page draws, so a
 * filter change is a single round trip and a single re-render -- no per-chart
 * fetching and no client-side scan of the raw log. Charts are static SVG built
 * by static/charts.js: after the innerHTML write nothing runs until the next
 * filter change, so an open dashboard costs nothing.
 *
 * Layout rules followed here: one filter row above everything it scopes, KPI
 * tiles before charts, a legend on every multi-series chart, and a data-table
 * twin behind each chart's "Data" disclosure (rendered lazily on first open) so
 * no value is reachable only through a tooltip.
 */
const insightsView = (function () {
  let DATA = null;
  let inFlight = false;
  const state = { days: 30, branch: "", table: "", load_mode: "", status: "" };

  const STATUS_COLORS = {
    SUCCESS: VIZ.good, PARTIAL: VIZ.warning, FAILED: VIZ.critical,
    SKIPPED: VIZ.neutral, UNKNOWN: VIZ.neutral,
  };
  const statusColor = (key, i) => STATUS_COLORS[String(key).toUpperCase()] || VIZ.cat[i % VIZ.cat.length];

  const DAY_OPTIONS = [[7, "Last 7 days"], [30, "Last 30 days"], [90, "Last 90 days"],
                       [365, "Last year"], [0, "All history"]];

  /* ------------------------------------------------------------ filter bar */
  function buildBar() {
    const f = DATA.facets || {};
    const sel = (field, label, values, current) =>
      `<div><label>${esc(label)}</label><select data-i="${field}">` +
      `<option value="">All</option>` +
      values.map(v => `<option value="${esc(v)}"${String(current) === String(v) ? " selected" : ""}>${esc(v)}</option>`).join("") +
      `</select></div>`;
    el("ins-bar").innerHTML =
      `<div><label>Window</label><select data-i="days">` +
        DAY_OPTIONS.map(([d, lab]) => `<option value="${d}"${state.days === d ? " selected" : ""}>${lab}</option>`).join("") +
      `</select></div>` +
      sel("branch", "Branch", f.branch_id || [], state.branch) +
      sel("table", "Table", f.table_name || [], state.table) +
      sel("load_mode", "Load mode", f.load_mode || [], state.load_mode) +
      sel("status", "Unit status", f.status || [], state.status) +
      `<button class="btn ghost sm" data-i="clear" style="align-self:flex-end">Clear</button>` +
      `<span class="spacer"></span><span class="muted" id="ins-window"></span>`;
    el("ins-bar").querySelectorAll("select").forEach(node => node.onchange = () => {
      state[node.dataset.i] = node.dataset.i === "days" ? +node.value : node.value;
      load();
    });
    el("ins-bar").querySelector('[data-i="clear"]').onclick = () => {
      state.branch = state.table = state.load_mode = state.status = "";
      load();
    };
    const w = DATA.window || {};
    el("ins-window").textContent =
      `${fmtNum(w.rows_scanned)} log rows scanned` + (w.truncated ? ` (capped at ${fmtNum(w.cap)})` : "");
  }

  /* ---------------------------------------------------------------- tiles */
  function tile(label, value, opts = {}) {
    return `<div class="stat-tile${opts.hero ? " hero" : ""}">
      <div class="stat-label">${esc(label)}</div>
      <div class="stat-value">${value}</div>
      ${opts.sub ? `<div class="stat-sub">${opts.sub}</div>` : ""}
    </div>`;
  }
  function renderKpi() {
    const k = DATA.kpi;
    const daily = DATA.daily || [];
    const badge = (n, label, cls) => n ? `<span class="run-badge ${cls}">${fmtNum(n)} ${label}</span>` : "";
    el("ins-kpi").innerHTML =
      tile("Records loaded", fmtNum(k.rows), {
        hero: true,
        sub: vizSpark(daily.map(d => d.rows)) +
             `<span class="muted">${fmtNum(k.rows_per_run)} per run · ${k.throughput_rows_s ? fmtNum(k.throughput_rows_s) + " rows/s" : "—"}</span>`,
      }) +
      tile("Pipeline runs", fmtNum(k.runs), {
        sub: badge(k.runs_ok, "clean", "ok") + badge(k.runs_partial, "partial", "warn") + badge(k.runs_failed, "failed", "failed"),
      }) +
      tile("Unit success rate", `${k.success_rate}%`, {
        sub: vizMeter(k.success_rate, { label: "unit success rate" }) +
             `<span class="muted">${fmtNum(k.units_ok)} of ${fmtNum(k.units)} table×branch loads</span>`,
      }) +
      tile("Scope", `${fmtNum(k.tables)} <small>tables</small>`, {
        sub: `<span class="muted">${fmtNum(k.branches)} branches · ${fmtNum(k.units)} loads</span>`,
      }) +
      tile("Avg run duration", vizDur(k.run_wall_avg_ms), {
        sub: vizSpark(daily.map(d => d.wall_avg_ms), { color: VIZ.cat[1] }) +
             `<span class="muted">p95 ${vizDur(k.run_wall_p95_ms)} · max ${vizDur(k.run_wall_max_ms)}</span>`,
      }) +
      tile("Avg load duration", vizDur(k.unit_avg_ms), {
        sub: `<span class="muted">p50 ${vizDur(k.unit_p50_ms)} · p95 ${vizDur(k.unit_p95_ms)} · max ${vizDur(k.unit_max_ms)}</span>`,
      }) +
      tile("Failed loads", fmtNum(k.units_failed), {
        sub: badge(k.retries, "retried", "warn") + badge(k.drift, "schema drift", "warn") ||
             `<span class="muted">no retries or drift</span>`,
      }) +
      tile("Last activity", esc(fmtDate(k.last_at)), {
        sub: `<span class="muted">since ${esc(fmtDate(k.first_at))}</span>`,
      });
  }

  /* --------------------------------------------------------------- charts */
  // Each card: { title, sub, wide, svg(), table() } -- table() is the twin,
  // built only when the reader opens the disclosure.
  function cards() {
    const d = DATA.daily || [];
    const k = DATA.kpi;
    const heat = DATA.heatmap || { branches: [], tables: [], cells: [] };
    const heatIdx = new Map(heat.cells.map(c => [c.branch + "\t" + c.table, c]));
    const topBranches = (DATA.by_branch || []).slice(0, 10);
    const topTables = (DATA.by_table || []).slice(0, 10);

    return [{
      title: "Runs by day and outcome",
      sub: "A run is clean when every table×branch load in it succeeded.",
      svg: () => vizColumns(d, {
        x: "key", fmt: (v) => fmtNum(Math.round(v)),
        series: [
          { key: "runs_ok", label: "Clean", color: VIZ.good },
          { key: "runs_partial", label: "Partial", color: VIZ.warning },
          { key: "runs_failed", label: "Failed", color: VIZ.critical },
        ],
      }),
      table: () => renderTable(["key", "runs", "runs_ok", "runs_partial", "runs_failed"], d,
        { numCols: ["runs", "runs_ok", "runs_partial", "runs_failed"] }),
    }, {
      title: "Records loaded per day",
      sub: "Rows written across every table and branch.",
      svg: () => vizLines(d, { x: "key", series: [{ key: "rows", label: "Rows", color: VIZ.cat[0] }] }),
      table: () => renderTable(["key", "rows", "units", "runs"], d, { numCols: ["rows", "units", "runs"] }),
    }, {
      title: "Load outcomes",
      sub: "Every table×branch load in the window.",
      svg: () => vizDonut((DATA.unit_status || []).slice(0, 6).map((s, i) => ({
        label: s.key, value: s.n, color: statusColor(s.key, i),
      })), { centerValue: vizCompact(k.units), centerLabel: "loads" }),
      table: () => renderTable(["key", "n"], DATA.unit_status || [], { numCols: ["n"] }),
    }, {
      title: "Write behaviour",
      sub: "How rows reached the lake — load status per unit.",
      svg: () => vizDonut((DATA.load_status || []).slice(0, 6).map((s, i) => ({
        label: s.key, value: s.n, color: statusColor(s.key, i),
      })), { centerValue: vizCompact(k.units), centerLabel: "loads" }),
      table: () => renderTable(["key", "n"], DATA.load_status || [], { numCols: ["n"] }),
    }, {
      title: "Load duration by branch",
      sub: "Average per table×branch load, slowest first; the tick marks p95.",
      svg: () => vizBars([...topBranches].sort((a, b) => (b.avg_ms || 0) - (a.avg_ms || 0)).map(b => ({
        label: b.key, value: b.avg_ms || 0, mark: b.p95_ms || 0,
        note: `${fmtNum(b.units)} loads · ${fmtNum(b.rows)} rows`,
      })), { fmt: vizDur, valueLabel: "avg", markLabel: "p95", color: VIZ.cat[0] }),
      table: () => renderTable(["key", "units", "rows", "avg_ms", "p50_ms", "p95_ms", "max_ms"],
        DATA.by_branch || [], { numCols: ["units", "rows", "avg_ms", "p50_ms", "p95_ms", "max_ms"] }),
    }, {
      title: "Records by branch",
      sub: "Where the volume comes from.",
      svg: () => vizBars(topBranches.map(b => ({
        label: b.key, value: b.rows,
        note: `${b.success_rate}% success · ${fmtNum(b.failed)} failed`,
      })), { fmt: vizCompact, valueLabel: "rows", color: VIZ.cat[2] }),
      table: () => renderTable(["key", "rows", "units", "ok", "failed", "success_rate"],
        DATA.by_branch || [], { numCols: ["rows", "units", "ok", "failed", "success_rate"] }),
    }, {
      title: "Duration trend",
      sub: "Average and p95 of a single table×branch load, per day.",
      svg: () => vizLines(d, {
        x: "key", fmt: vizDur, series: [
          { key: "avg_ms", label: "Average", color: VIZ.cat[0] },
          { key: "p95_ms", label: "p95", color: VIZ.cat[1] },
        ],
      }),
      table: () => renderTable(["key", "avg_ms", "p50_ms", "p95_ms", "max_ms", "total_ms"], d,
        { numCols: ["avg_ms", "p50_ms", "p95_ms", "max_ms", "total_ms"] }),
    }, {
      title: "Slowest tables",
      sub: "Average load duration; the tick marks p95.",
      svg: () => vizBars([...topTables].sort((a, b) => (b.avg_ms || 0) - (a.avg_ms || 0)).map(t => ({
        label: t.key, value: t.avg_ms || 0, mark: t.p95_ms || 0,
        note: `${fmtNum(t.units)} loads · ${fmtNum(t.rows)} rows`,
      })), { fmt: vizDur, valueLabel: "avg", markLabel: "p95", color: VIZ.cat[1], labelW: 190 }),
      table: () => renderTable(["key", "units", "rows", "avg_ms", "p95_ms", "max_ms", "failed"],
        DATA.by_table || [], { numCols: ["units", "rows", "avg_ms", "p95_ms", "max_ms", "failed"] }),
    }, {
      title: "Failures by branch × table",
      wide: true,
      sub: k.units_failed ? "Failed loads per cell — darker means more failures."
                          : "No failed loads in this window.",
      svg: () => k.units_failed
        ? vizHeat(heat.branches, heat.tables, {
            value: (b, t) => (heatIdx.get(b + "\t" + t) || {}).failed ?? null,
            valueLabel: "failures", fmt: (v) => fmtNum(Math.round(v)),
          })
        : `<div class="viz-empty ok"><i class="fa-solid fa-circle-check"></i> Every load in this window succeeded.</div>`,
      table: () => renderTable(["branch", "table", "units", "failed", "rows"], heat.cells,
        { numCols: ["units", "failed", "rows"] }),
    }, {
      title: "Activity by hour of day",
      sub: "When loads actually run (server time).",
      svg: () => vizColumns((DATA.hourly || []).map(h => ({
        key: String(h.hour).padStart(2, "0"), units: h.units,
      })), { x: "key", series: [{ key: "units", label: "Loads", color: VIZ.cat[0] }] }),
      table: () => renderTable(["hour", "units"], DATA.hourly || [], { numCols: ["hour", "units"] }),
    }, {
      title: "Recent runs",
      wide: true,
      sub: "Wall-clock duration of each run, newest last.",
      svg: () => vizColumns([...(DATA.runs || [])].reverse().slice(-40).map(r => ({
        key: (r.start_time || "").slice(5, 16), wall: r.wall_ms || 0, run: r.run_id,
      })), { x: "key", fmt: vizDur, series: [{ key: "wall", label: "Wall clock", color: VIZ.cat[0] }] }),
      table: () => renderTable(
        ["run_id", "status", "start_time", "wall_ms", "units", "ok", "failed", "rows", "tables", "rows_per_s"],
        DATA.runs || [], { pillCols: ["status"], numCols: ["wall_ms", "units", "ok", "failed", "rows", "tables", "rows_per_s"] }),
    }];
  }

  function renderCharts() {
    const built = cards();
    el("ins-charts").innerHTML = built.map((c, i) => `
      <div class="viz-card${c.wide ? " wide" : ""}">
        <div class="viz-head"><h3>${esc(c.title)}</h3>${c.sub ? `<p>${esc(c.sub)}</p>` : ""}</div>
        <div class="viz-body">${c.svg()}</div>
        <details class="viz-twin" data-card="${i}">
          <summary>Data table</summary><div class="viz-twin-body"><div class="muted">Loading…</div></div>
        </details>
      </div>`).join("");
    // The table twins are the accessible equivalent of each chart, but building
    // them all up front would double the render cost, so they fill in on open.
    $$("#ins-charts details.viz-twin").forEach(node => node.addEventListener("toggle", () => {
      if (!node.open || node.dataset.done) return;
      node.dataset.done = "1";
      node.querySelector(".viz-twin-body").innerHTML = built[+node.dataset.card].table();
    }, { once: false }));
  }

  function renderTables() {
    const fails = DATA.failures || [];
    el("ins-fail").innerHTML = fails.length
      ? renderTable(["when", "table", "branch", "status", "error"], fails, { pillCols: ["status"] })
      : `<div class="viz-empty ok"><i class="fa-solid fa-circle-check"></i> No failed loads in this window.</div>`;
    el("ins-slow").innerHTML = renderTable(
      ["table", "branch", "duration_ms", "rows", "status", "when"], DATA.slowest || [],
      { pillCols: ["status"], numCols: ["duration_ms", "rows"] });
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
      load_mode: state.load_mode, status: state.status,
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
