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
