/* Shared helpers for the OASIS control panel. */

async function api(method, url, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(url, opts);
  let data = null;
  try { data = await res.json(); } catch (e) { /* non-json */ }
  if (!res.ok) {
    const msg = (data && data.error) || `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return data;
}
const apiGet = (u) => api("GET", u);
const apiPost = (u, b) => api("POST", u, b);
const apiPut = (u, b) => api("PUT", u, b);
const apiDel = (u) => api("DELETE", u);

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const el = (id) => document.getElementById(id);

function h(html) { const t = document.createElement("template"); t.innerHTML = html.trim(); return t.content.firstChild; }

function esc(s) {
  if (s === null || s === undefined) return "";
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function fmtNum(n) {
  if (n === null || n === undefined || n === "") return "—";
  const v = Number(n);
  return Number.isFinite(v) ? v.toLocaleString() : esc(n);
}

function fmtBytes(n) {
  if (!n) return "0 B";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0, v = Number(n);
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${u[i]}`;
}

function fmtDate(s) {
  if (!s) return "—";
  return String(s).replace("T", " ").replace("Z", "");
}

function pill(status) {
  const s = String(status || "unknown").toLowerCase();
  return `<span class="pill ${esc(s)}">${esc(status || "—")}</span>`;
}

// Build an HTML table from columns + array-of-objects rows.
// opts.pillCols: column names rendered as status pills.
// opts.numCols: column names right-aligned as numbers.
function renderTable(columns, rows, opts = {}) {
  const pillCols = new Set(opts.pillCols || []);
  const numCols = new Set(opts.numCols || []);
  if (!rows || !rows.length) return `<div class="muted">No rows.</div>`;
  const head = `<thead><tr>${columns.map(c => `<th class="${numCols.has(c) ? "num" : ""}">${esc(c)}</th>`).join("")}</tr></thead>`;
  const body = rows.map(r => `<tr>${columns.map(c => {
    const v = r[c];
    if (pillCols.has(c)) return `<td>${pill(v)}</td>`;
    if (numCols.has(c)) return `<td class="num">${fmtNum(v)}</td>`;
    return `<td class="mono">${esc(v === null || v === undefined ? "—" : v)}</td>`;
  }).join("")}</tr>`).join("");
  return `<div class="table-wrap"><table>${head}<tbody>${body}</tbody></table></div>`;
}

// Mount a paginated table into `container` (a DOM element). Renders one page at
// a time by reusing renderTable(), so pill/number/escaping behavior is identical
// to a plain table. A First/Prev/Next/Last pager appears only when the (capped)
// row count exceeds pageSize, so small tables look exactly as before.
// opts: { pillCols, numCols, pageSize = 50, cap = 1000 }
function mountTable(container, columns, rows, opts = {}) {
  const pageSize = opts.pageSize ?? 50;
  const cap = opts.cap ?? 1000;
  const all = rows || [];
  const truncated = all.length > cap;
  const capped = truncated ? all.slice(0, cap) : all;
  const pages = Math.max(1, Math.ceil(capped.length / pageSize));
  let page = 0; // 0-based

  function pagerBar(start, shown) {
    const from = capped.length ? start + 1 : 0;
    const to = start + shown;
    // `(capped)` marks the mountTable-level 1000-row backstop; upstream fetches
    // already cap at 1000, so this normally only shows String(capped.length).
    const total = truncated ? `${cap} (capped)` : String(capped.length);
    const dis = (c) => (c ? "disabled" : "");
    return `<div class="pager">
      <button class="btn sm ghost" data-pg="first" ${dis(page === 0)} title="First" aria-label="First page">⏮</button>
      <button class="btn sm ghost" data-pg="prev" ${dis(page === 0)} title="Previous" aria-label="Previous page">◀</button>
      <span class="pager-count">${from}–${to} of ${total}</span>
      <button class="btn sm ghost" data-pg="next" ${dis(page >= pages - 1)} title="Next" aria-label="Next page">▶</button>
      <button class="btn sm ghost" data-pg="last" ${dis(page >= pages - 1)} title="Last" aria-label="Last page">⏭</button>
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

function toast(msg, kind = "") {
  const box = el("toasts");
  if (!box) return;
  const t = h(`<div class="toast ${kind}">${esc(msg)}</div>`);
  box.appendChild(t);
  setTimeout(() => t.remove(), 5000);
}
function ok(msg) { toast(msg, "ok"); }
function err(msg) { toast(msg, "err"); }

// Wildcard/substring name filter shared by the Run, Tables and Iceberg pages.
// "*" acts as a glob; otherwise it's a case-insensitive substring match.
function matchFilter(name, q) {
  q = String(q || "").trim().toLowerCase();
  if (!q) return true;
  name = String(name || "").toLowerCase();
  if (q.includes("*")) {
    const re = new RegExp("^" + q.split("*")
      .map(s => s.replace(/[.+?^${}()|[\]\\]/g, "\\$&")).join(".*") + "$");
    return re.test(name);
  }
  return name.includes(q);
}

// Give the bare "↻" icon-only refresh buttons an accessible name.
document.querySelectorAll("button").forEach(b => {
  if (b.textContent.trim() === "↻" && !b.getAttribute("aria-label")) {
    b.setAttribute("aria-label", "Refresh");
    if (!b.title) b.title = "Refresh";
  }
});

// Modal a11y: Esc and backdrop-click close any open ".modal-bg", and Esc closes
// the modal in preference to the sidebar (capture phase runs before the sidebar
// handler below, which then can't fire because propagation is stopped).
(function () {
  document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    const open = document.querySelector(".modal-bg.show");
    if (open) { open.classList.remove("show"); e.stopPropagation(); }
  }, true);
  document.addEventListener("click", (e) => {
    const t = e.target;
    if (t && t.classList && t.classList.contains("modal-bg") && t.classList.contains("show"))
      t.classList.remove("show");
  });
})();

// Sidebar toggle (mobile / narrow viewports)
(function () {
  const toggle = el("sidebar-toggle");
  const scrim = el("scrim");
  const close = () => document.body.classList.remove("nav-open");
  if (toggle) toggle.addEventListener("click", () => document.body.classList.toggle("nav-open"));
  if (scrim) scrim.addEventListener("click", close);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") close(); });
})();
