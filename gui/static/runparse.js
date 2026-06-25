/* Shared live-log -> progress dashboard, used by the Run page (live tail) and
 * the Monitor "Log files" tab (whole-file). Parses the pipeline's own log lines
 * into per-table / per-branch progress + an issues feed, and renders into the
 * markup from templates/_dash.html (the rd-* element ids).
 *
 * Recognised lines:
 *   PROGRESS 0:01:23 | load:appointments | tables 2/8 | extract 14/56 1 failed | rows=1,234 | rss=..(peak ..) arrow=..
 *   [branch/table] 1703887 rows (attempt 1)            -> one extract unit done
 *   [table] loaded: disp=merge ok=7 fail=0 rows=4       -> table load result
 *   [table] load failed: <err> / [table] skipped: <why>
 *     master_deliveries  SUCCESS  disp=replace ok=7 fail=0 rows=434154   (final summary)
 * Any WARNING/ERROR line (etl or dlt format) is collected as an issue. Logs that
 * emit none of these keep the dashboard hidden and only the raw text shows.
 *
 * createLogDash(opts) -> { reset(), feed(chunk), flush(), render(), load(text), get dash() }
 *   opts.branchHint : () => number  best guess of total branches (Run page only)
 */
function createLogDash(opts = {}) {
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
    exit: /\[runner\] exited with code (-?\d+)/,
  };

  let dash, lineBuf;

  function fresh() {
    return {
      stage: "", elapsed: "", rows: 0,
      unitsDone: 0, unitsTotal: 0, unitsFailed: 0, tablesDone: 0, tablesTotal: 0,
      rss: "", rssPeak: "", arrow: "", branchesTotal: 0,
      tables: new Map(),   // name -> {ok,fail,branches:Set,load,disp,rows,err,final}
      branches: new Map(),  // key -> {ok,fail}
      issues: [], started: false, exitCode: null, firstTs: null, lastTs: null,
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
    if ((m = RE.exit.exec(line))) { dash.exitCode = +m[1]; return; }
    if (/\|\s*(WARNING|ERROR)\s*\||\|\[(WARNING|ERROR)\]\||UserWarning|Traceback/.test(line)) pushIssue(line, /ERROR|Traceback/.test(line) ? "error" : "warn");
  }

  function feed(chunk) {
    lineBuf += chunk;
    const parts = lineBuf.split("\n");
    lineBuf = parts.pop();
    for (const ln of parts) feedLine(ln);
  }
  function flush() { if (lineBuf) { feedLine(lineBuf); lineBuf = ""; } }

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

  function render() {
    const box = el("run-dash");
    if (!box) return;
    if (!dash || (!dash.started && dash.tables.size === 0 && dash.issues.length === 0)) { box.hidden = true; return; }
    box.hidden = false;

    const bt = branchTotal();
    const done = dash.unitsDone || [...dash.tables.values()].reduce((a, t) => a + t.branches.size, 0);
    const total = dash.unitsTotal || (bt * (dash.tablesTotal || dash.tables.size)) || 0;
    const pct = total ? Math.min(100, Math.round(done / total * 100)) : (dash.exitCode != null ? 100 : 0);
    const failTotal = dash.unitsFailed || [...dash.tables.values()].reduce((a, t) => a + t.fail, 0);

    el("rd-stage").textContent = dash.stage || (dash.exitCode != null ? "done" : "starting");
    el("rd-stage").className = "rd-stage " + stageClass(dash.stage || (dash.exitCode != null ? "done" : ""));
    el("rd-elapsed").textContent = dash.elapsed || elapsedFromTs();
    el("rd-rows").textContent = fmtNum(dash.rows) + " rows";
    el("rd-mem").textContent = dash.rss ? `rss ${dash.rss} (peak ${dash.rssPeak})` : "rss —";
    const fEl = el("rd-fail"); fEl.hidden = !failTotal; fEl.textContent = `${failTotal} failed`;
    el("rd-bar-fill").style.width = pct + "%";
    el("rd-bar-fill").className = "rd-bar-fill" + (dash.exitCode ? " err" : (failTotal ? " has-fail" : ""));
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

  function reset() { dash = fresh(); lineBuf = ""; render(); }
  function load(text) { reset(); feed(text || ""); flush(); render(); }

  reset();
  return { reset, feed, flush, render, load, get dash() { return dash; } };
}
