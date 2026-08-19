/* Monitor -> Flow runs tab: live progress for Dagster-launched flow runs.
 *
 * A flow run is a Dagster job whose steps are the flow's nodes, each running an
 * ETL command as a subprocess and streaming its output into the Dagster event
 * log. That gives three layers of progress, and this tab shows all three:
 *
 *   1. the run     -- status, queue wait, elapsed, steps done / total;
 *   2. the steps   -- per node: pending / running / succeeded / failed, with
 *                     start, duration and retry attempts;
 *   3. the command -- the running node's own log, parsed by the shared
 *                     createLogDash into the same table/branch/rows/RSS progress
 *                     dashboard the Run page shows (prefix "fr-", so it is a
 *                     second independent instance on this page).
 *
 * Cost control, because all of this polls:
 *   - the run list backs off to 20s when nothing is active (4s when something
 *     is), and every timer is chained (never setInterval) so a slow response can
 *     not stack up requests;
 *   - the step detail polls only while the selected run is live, and stops on
 *     its own after the terminal poll;
 *   - the log rides the shared adaptive createTailPoller (fast while bytes flow,
 *     backing off when quiet, paused while the tab is hidden);
 *   - renders are diffed: list rows are rewritten only when their signature
 *     changes, the parsed dashboard is coalesced into one paint per frame, and
 *     the ticking elapsed clocks touch a single text node each.
 */
const flowRuns = (function () {
  const ACTIVE = new Set(["QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING"]);
  const LIST_FAST_MS = 4000, LIST_SLOW_MS = 20000, DETAIL_MS = 2500;

  let runs = [], selected = null, detail = null, cursor = null, status = null;
  let listTimer = null, detailTimer = null, tickTimer = null, logPoller = null;
  let live = false;               // tab visible and wired up
  let listBusy = false, detailBusy = false;
  let rowSigs = new Map(), listIds = "", stepsSig = "";

  const dash = createLogDash({ prefix: "fr-" });
  const renderDash = coalesce(() => dash.render());

  const isActive = (s) => ACTIVE.has(String(s || "").toUpperCase());
  const nowSec = () => Date.now() / 1000;

  function statusPill(s) {
    const key = String(s || "").toUpperCase();
    const cls = key === "SUCCESS" ? "ok" : (key === "FAILURE" ? "failed"
      : isActive(key) ? "running" : "gray");
    return `<span class="run-badge ${cls}">${esc(s || "—")}</span>`;
  }
  function dur(sec) {
    if (sec === null || sec === undefined || !Number.isFinite(sec) || sec < 0) return "—";
    const s = Math.round(sec);
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
    return (h ? `${h}h ` : "") + (h || m ? `${m}m ` : "") + `${s % 60}s`;
  }
  const runElapsed = (r) => r.start_time ? (r.end_time || nowSec()) - r.start_time : null;
  // Dagster timestamps are epoch seconds with a fractional part; seconds is as
  // fine-grained as any of these columns needs to read.
  const stamp = (t) => t ? fmtDate(new Date(t * 1000).toISOString().slice(0, 19)) : "—";

  /* ------------------------------------------------------------- run list */
  function rowHtml(r) {
    const p = r.progress || {};
    const total = p.steps_total || r.steps_planned || 0;
    const done = p.steps_done || 0;
    const pct = total ? Math.round((done / total) * 100) : 0;
    const prog = r.active && total
      ? `<span class="rd-mini"><span class="rd-mini-fill${p.steps_failed ? " err" : ""}" style="width:${pct}%"></span></span>` +
        `<span class="rd-mini-lbl">${done}/${total}</span>`
      : (total ? `<span class="rd-mini-lbl">${total} step${total === 1 ? "" : "s"}</span>` : "—");
    return `<td>${esc(r.flow_name)}</td>` +
      `<td>${statusPill(r.status)}</td>` +
      `<td>${prog}</td>` +
      `<td class="mono">${esc(stamp(r.start_time))}</td>` +
      `<td class="mono fr-dur" data-start="${r.start_time || ""}" data-end="${r.end_time || ""}">${esc(dur(runElapsed(r)))}</td>` +
      `<td>${r.run_link ? `<a href="${esc(r.run_link)}" target="_blank" rel="noopener" title="Open in Dagster">↗</a>` : ""}</td>`;
  }
  function rowSig(r) {
    const p = r.progress || {};
    return [r.status, p.steps_done, p.steps_failed, p.steps_total, r.end_time].join("|");
  }

  function renderList() {
    const body = el("fr-list");
    const ids = runs.map(r => r.run_id).join(",");
    if (ids !== listIds) {          // the set changed -- one full rebuild
      listIds = ids;
      rowSigs = new Map();
      body.innerHTML = runs.map(r =>
        `<tr class="clickable" data-r="${esc(r.run_id)}" tabindex="0" role="button">${rowHtml(r)}</tr>`).join("") ||
        `<tr><td colspan="6" class="muted">No flow runs yet (is Dagster running?).</td></tr>`;
      $$("#fr-list tr.clickable").forEach(tr => tr.onclick = () => open(tr.dataset.r));
    }
    // Same rows: rewrite only the ones whose data actually moved.
    for (const r of runs) {
      const sig = rowSig(r);
      if (rowSigs.get(r.run_id) === sig) continue;
      rowSigs.set(r.run_id, sig);
      const tr = body.querySelector(`tr[data-r="${CSS.escape(r.run_id)}"]`);
      if (tr) tr.innerHTML = rowHtml(r);
    }
    $$("#fr-list tr.clickable").forEach(tr =>
      tr.classList.toggle("selected", tr.dataset.r === selected));
    const running = runs.filter(r => r.active).length;
    el("fr-stat").textContent = runs.length
      ? `${runs.length} runs` + (running ? ` · ${running} running` : "") : "";
  }

  async function loadRuns() {
    if (listBusy) return;
    listBusy = true;
    try {
      runs = await apiGet("/api/flow-runs?limit=50");
      renderList();
      // First visit with something in flight: open it rather than making the
      // reader hunt for the live run.
      if (!selected) {
        const first = runs.find(r => r.active);
        if (first) open(first.run_id);
      }
    } catch (e) {
      el("fr-stat").textContent = e.message;
    } finally {
      listBusy = false;
    }
  }

  function scheduleList() {
    clearTimeout(listTimer);
    if (!live) return;
    const delay = runs.some(r => r.active) ? LIST_FAST_MS : LIST_SLOW_MS;
    listTimer = setTimeout(async () => { await loadRuns(); scheduleList(); }, delay);
  }

  /* -------------------------------------------------------- step timeline */
  function stepRow(s) {
    const started = s.start_time ? stamp(s.start_time) : "—";
    const d = s.duration_s != null ? dur(s.duration_s)
      : (s.start_time && s.status === "IN_PROGRESS" ? dur(nowSec() - s.start_time) : "—");
    const attempts = s.attempts > 1 ? `<span class="run-badge warn">${s.attempts} attempts</span>` : "";
    return `<tr>
      <td class="mono">${esc(s.label || s.node_id)}</td>
      <td><span class="tag">${esc(s.kind || "step")}</span></td>
      <td><span class="pill ${esc(s.pill || "pending")}">${esc(s.status || "PENDING")}</span> ${attempts}</td>
      <td class="mono">${esc(started)}</td>
      <td class="mono fr-sdur" data-start="${s.status === "IN_PROGRESS" ? (s.start_time || "") : ""}">${esc(d)}</td>
    </tr>`;
  }

  function renderDetail() {
    if (!detail) return;
    // Kept in the server's order, which is the flow's dependency order: a step
    // timeline is read top-to-bottom as "what ran, then what runs next".
    const steps = detail.steps || [];
    const total = detail.steps_total || steps.length || 0;
    const done = steps.filter(s => ["SUCCESS", "SKIPPED", "CANCELED"].includes(s.status)).length;
    const failed = steps.filter(s => s.status === "FAILURE").length;
    const pct = total ? Math.round(((done + failed) / total) * 100) : 0;
    const active = isActive(detail.status);

    el("fr-status").innerHTML = statusPill(detail.status);
    el("fr-bar-fill").style.width = pct + "%";
    el("fr-bar-fill").className = "rd-bar-fill" + (failed ? " err" : "");
    el("fr-bar-label").textContent =
      `${done + failed}/${total || "?"} steps · ${pct}%` + (failed ? ` · ${failed} failed` : "");
    const wait = (detail.launch_time && detail.enqueued_time)
      ? dur(detail.launch_time - detail.enqueued_time) : null;
    el("fr-meta").innerHTML =
      `<span>started ${esc(stamp(detail.start_time))}</span>` +
      `<span class="rd-sep">·</span><span class="fr-elapsed" data-start="${active ? (detail.start_time || "") : ""}">${esc(dur(
        detail.start_time ? (detail.end_time || nowSec()) - detail.start_time : null))}</span>` +
      (wait ? `<span class="rd-sep">·</span><span>queued ${esc(wait)}</span>` : "") +
      (steps.some(s => s.status === "IN_PROGRESS")
        ? `<span class="rd-sep">·</span><span>running: ${esc(steps.filter(s => s.status === "IN_PROGRESS").map(s => s.label || s.node_id).join(", "))}</span>`
        : "");
    // Only rewrite the timeline when a step actually moved: the detail poll
    // ticks every 2.5s and the table is otherwise identical each time.
    const sig = steps.map(s => `${s.node_id}:${s.status}:${s.duration_s}`).join("|");
    if (sig !== stepsSig) {
      stepsSig = sig;
      el("fr-steps").innerHTML = steps.map(stepRow).join("") ||
        `<tr><td colspan="5" class="muted">No steps reported yet.</td></tr>`;
    }
  }

  async function loadDetail() {
    if (!selected || detailBusy) return;
    detailBusy = true;
    try {
      detail = await apiGet(`/api/flow-runs/${encodeURIComponent(selected)}/detail`);
      status = detail.status;
      renderDetail();
    } catch (e) {
      el("fr-meta").textContent = e.message;
    } finally {
      detailBusy = false;
    }
  }

  function scheduleDetail() {
    clearTimeout(detailTimer);
    if (!live || !selected) return;
    detailTimer = setTimeout(async () => {
      await loadDetail();
      // One poll past the finish line, then stop: a finished run's steps never
      // change again, so there is nothing left to ask for.
      if (isActive(status)) scheduleDetail();
    }, DETAIL_MS);
  }

  /* --------------------------------------------------------------- the log */
  function setBanner(html) {
    const b = el("fr-banner");
    if (!b || b.innerHTML === html) return;
    b.innerHTML = html;
    b.hidden = !html;
    b.className = "banner " + (/Connection lost/.test(html) ? "error" : "warn");
  }
  function reconnect() {
    if (!logPoller) return;
    setBanner("Reconnecting…");
    logPoller.start();
  }

  function startLog() {
    if (logPoller) logPoller.stop();
    const runId = selected;
    logPoller = createTailPoller({
      fetchChunk: async () => {
        const r = await apiGet(`/api/flow-runs/${encodeURIComponent(runId)}/log?cursor=${encodeURIComponent(cursor || "")}`);
        if (r.error) throw new Error(r.error);
        cursor = r.cursor;
        status = r.status || status;
        return r;
      },
      onChunk: (chunk) => {
        const box = el("fr-log");
        if (box.textContent === "—") box.textContent = "";
        appendConsole(box, chunk);
        dash.feed(chunk);
        renderDash();
      },
      onStatus: (r) => {
        setBanner("");
        // The run's own status rides the log response, so the pill stays fresh
        // between the (slower) detail polls at no extra request cost.
        if (r.status && detail && detail.status !== r.status) {
          detail.status = r.status;
          el("fr-status").innerHTML = statusPill(r.status);
        }
      },
      // Stop only once the run is over AND the server has no more events.
      isTerminal: (r) => !r.has_more && !!r.status && !isActive(r.status),
      onDone: () => { dash.flush(); dash.render(); loadDetail(); loadRuns(); },
      onError: (fails, max) => setBanner(fails >= max
        ? "Connection lost. <a href='#' onclick='flowRuns.reconnect();return false;'>Reconnect</a>"
        : `Reconnecting… (${fails}/${max})`),
    });
    logPoller.start();
  }

  /* ------------------------------------------------------------ selection */
  function open(runId) {
    if (logPoller) logPoller.stop();
    clearTimeout(detailTimer);
    selected = runId;
    cursor = null;
    detail = null;
    const run = runs.find(r => r.run_id === runId);
    status = run ? run.status : null;
    el("fr-run-panel").hidden = false;
    el("fr-title").textContent = run ? run.flow_name : "Flow run";
    el("fr-run-id").textContent = runId.slice(0, 8);
    el("fr-dagster-link").href = (run && run.run_link) || "#";
    el("fr-log").textContent = "";
    stepsSig = "";
    el("fr-steps").innerHTML = `<tr><td colspan="5" class="muted">Loading steps…</td></tr>`;
    setBanner("");
    dash.reset();
    $$("#fr-list tr.clickable").forEach(tr =>
      tr.classList.toggle("selected", tr.dataset.r === runId));
    loadDetail().then(scheduleDetail);
    startLog();
  }

  /* ------------------------------------------------- ticking clocks (1/s) */
  // Elapsed times are the only thing that changes between polls, so they are
  // updated in place: one text write per live clock, no markup rebuilt.
  function tick() {
    if (!live) return;
    const now = nowSec();
    $$("#fr-list .fr-dur").forEach(td => {
      const start = +td.dataset.start;
      if (!start || td.dataset.end) return;
      td.textContent = dur(now - start);
    });
    $$("#fr-panel-body .fr-elapsed, #fr-panel-body .fr-sdur").forEach(node => {
      const start = +node.dataset.start;
      if (start) node.textContent = dur(now - start);
    });
  }

  /* -------------------------------------------------------------- lifecycle */
  function activate() {
    if (live) return;
    live = true;
    loadRuns().then(scheduleList);
    // A finished run's log and steps are already complete -- re-arming their
    // pollers on every tab switch would only re-ask for nothing.
    if (selected && isActive(status)) { loadDetail().then(scheduleDetail); if (logPoller) logPoller.start(); }
    clearInterval(tickTimer);
    tickTimer = setInterval(tick, 1000);
  }
  function deactivate() {
    live = false;
    clearTimeout(listTimer); clearTimeout(detailTimer); clearInterval(tickTimer);
    listTimer = detailTimer = tickTimer = null;
    if (logPoller) logPoller.stop();
  }
  function refresh() { loadRuns(); if (selected) loadDetail(); }

  return { activate, deactivate, refresh, reconnect, open,
           get selected() { return selected; } };
})();
