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

let _toastTimer;
function toast(msg, kind = "") {
  const box = el("toasts");
  if (!box) return;
  const t = h(`<div class="toast ${kind}">${esc(msg)}</div>`);
  box.appendChild(t);
  setTimeout(() => t.remove(), 5000);
}
function ok(msg) { toast(msg, "ok"); }
function err(msg) { toast(msg, "err"); }
