/* Tiny dependency-free SVG chart primitives for the Monitor's Insights tab.
 *
 * Why hand-rolled rather than a charting library: the GUI ships no bundler and
 * loads no CDN JS, and every chart here is static once drawn -- so each renderer
 * builds one SVG string and writes it with a single innerHTML. No canvas, no
 * per-frame work, no resize observers (the SVG scales through its viewBox), and
 * tooltips are native <title> nodes, so hovering costs no JavaScript at all.
 *
 * The palette, mark specs and spacers follow the house data-viz rules:
 *   - categorical hues are assigned in FIXED slot order, never by rank;
 *   - status hues (good/warning/serious/critical) are reserved for status and
 *     always ship with a text label, never colour alone;
 *   - bars are <= 24px thick with a 4px rounded data-end, square at the baseline;
 *   - a 2px surface gap separates touching marks (stack segments, adjacent bars)
 *     and a 2px surface ring rings overlapping dots;
 *   - lines are 2px, area fills are the hue at 10%;
 *   - gridlines are solid hairlines one step off the surface, never dashed;
 *   - text always wears ink tokens, never the series colour.
 * The categorical order below is the validated one (worst adjacent CVD ΔE 9.1,
 * worst normal-vision ΔE 19.6 on a white surface); do not re-order or extend it
 * without re-running the palette validator.
 */
const VIZ = {
  cat: ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"],
  good: "#0ca30c", warning: "#fab219", serious: "#ec835a", critical: "#d03b3b",
  neutral: "#c3c2b7",
  // Single-hue sequential ramp (blue, light -> dark) for magnitude encodings.
  seq: ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"],
  surface: "#ffffff", grid: "#e1e0d9", axis: "#c3c2b7", ink: "#52514e", muted: "#898781",
};

const VIZ_BOX = { w: 720, padL: 58, padR: 14, padT: 14, padB: 30 };
const BAR_MAX = 24;      // mark spec: bars never fill their band
const BAR_RADIUS = 4;    // rounded data-end
const GAP = 2;           // the surface gap between touching marks

/* ---------------------------------------------------------------- helpers */
function vizEsc(s) {
  return String(s === null || s === undefined ? "" : s)
    .replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// Round a maximum up to a "clean" axis top (1 / 2 / 5 x 10^n) so ticks read as
// human numbers rather than 3,847.
function vizNiceMax(max) {
  if (!(max > 0)) return 1;
  const mag = Math.pow(10, Math.floor(Math.log10(max)));
  const norm = max / mag;
  const step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10;
  return step * mag;
}

function vizCompact(n) {
  if (n === null || n === undefined || n === "") return "—";
  const v = Number(n);
  if (!Number.isFinite(v)) return String(n);
  const abs = Math.abs(v);
  if (abs >= 1e9) return (v / 1e9).toFixed(abs >= 1e10 ? 0 : 1) + "B";
  if (abs >= 1e6) return (v / 1e6).toFixed(abs >= 1e7 ? 0 : 1) + "M";
  if (abs >= 1e3) return (v / 1e3).toFixed(abs >= 1e4 ? 0 : 1) + "K";
  return String(Math.round(v * 10) / 10);
}

// h:mm:ss / 1.4s / 320ms -- durations are the dashboard's second currency.
function vizDur(ms) {
  if (ms === null || ms === undefined || ms === "") return "—";
  const v = Number(ms);
  if (!Number.isFinite(v)) return "—";
  if (v < 1000) return Math.round(v) + "ms";
  if (v < 60000) return (v / 1000).toFixed(1) + "s";
  const s = Math.round(v / 1000);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  return (h ? `${h}h ` : "") + (h || m ? `${m}m ` : "") + `${sec}s`;
}

// A bar/column whose data-end is rounded and whose baseline end is square.
// `dir` is the direction the mark grows: "up" (columns) or "right" (bars).
function vizBarPath(x, y, w, h, dir = "up") {
  const r = Math.min(BAR_RADIUS, w / 2, h / 2);
  if (h <= 0 || w <= 0) return "";
  if (dir === "up") {
    return `M${x},${y + h}V${y + r}q0,-${r} ${r},-${r}h${w - 2 * r}q${r},0 ${r},${r}V${y + h}Z`;
  }
  return `M${x},${y}h${w - r}q${r},0 ${r},${r}v${h - 2 * r}q0,${r} -${r},${r}h-${w - r}Z`;
}

function vizSvg(height, body, cls = "", width = VIZ_BOX.w) {
  return `<svg class="viz ${cls}" viewBox="0 0 ${width} ${height}" role="img" ` +
    `preserveAspectRatio="xMidYMid meet">${body}</svg>`;
}

// The STEP is what gets rounded to 1/2/5 x 10^n, not just the axis top: rounding
// only the top leaves ticks like 0 / 2.5 / 5 / 7.5 / 10, which then print as
// "3, 5, 8" once formatted.
// Time is not decimal: a duration axis stepping 10,000,000ms prints
// "11h 6m 40s" ticks. Step it in real time units instead.
const DUR_STEPS = [100, 250, 500, 1e3, 2e3, 5e3, 1e4, 15e3, 3e4, 6e4, 12e4, 3e5,
                   6e5, 9e5, 18e5, 36e5, 72e5, 108e5, 216e5, 432e5, 864e5];

function vizAxisSteps(max, ticks = 4, fmt = vizCompact) {
  const raw = (max || 1) / ticks;
  const step = (fmt === vizDur && DUR_STEPS.find(s => s >= raw)) || vizNiceMax(raw);
  const top = Math.max(step, Math.ceil((max || 1) / step) * step);
  return { step, top, ticks: Math.round(top / step) };
}

// Left padding wide enough for the widest tick label it will actually print --
// a duration axis ("1h 6m 40s") needs far more room than a count one, and a
// fixed padding clips it.
function vizPadL(max, fmt = vizCompact, ticks = 4) {
  const a = vizAxisSteps(max, ticks, fmt);
  let longest = 0;
  for (let i = 0; i <= a.ticks; i++) longest = Math.max(longest, String(fmt(a.step * i)).length);
  return Math.min(120, Math.max(VIZ_BOX.padL, Math.round(longest * 6.1) + 12));
}

// Right-hand gutter for a secondary axis: same sizing rule as vizPadL, plus the
// standard right padding so the last tick label is not flush to the card edge.
function vizPadR(max, fmt = vizCompact, ticks = 4) {
  const a = vizAxisSteps(max, ticks, fmt);
  let longest = 0;
  for (let i = 0; i <= a.ticks; i++) longest = Math.max(longest, String(fmt(a.step * i)).length);
  return Math.min(110, Math.round(longest * 6.1) + 16);
}

// Y gridlines + tick labels. Returns { body, y(v) } so the caller maps values.
// opts.side "right" prints the ticks in the right gutter; opts.grid false skips
// the gridlines, which is what a *secondary* axis wants -- two sets of
// gridlines at different scales cross-hatch the plot and mean nothing.
function vizYAxis(max, plot, fmt = vizCompact, ticks = 4, opts = {}) {
  const right = opts.side === "right";
  const grid = opts.grid !== false && !right;
  const a = vizAxisSteps(max, ticks, fmt);
  const y = (v) => plot.y1 - (a.top ? (v / a.top) * (plot.y1 - plot.y0) : 0);
  let body = "";
  for (let i = 0; i <= a.ticks; i++) {
    const v = a.step * i, yy = y(v);
    if (grid) {
      body += `<line x1="${plot.x0}" y1="${yy}" x2="${plot.x1}" y2="${yy}" stroke="${VIZ.grid}" stroke-width="1"/>`;
    }
    body += right
      ? `<text x="${plot.x1 + 8}" y="${yy + 4}" text-anchor="start" class="viz-tick">${vizEsc(fmt(v))}</text>`
      : `<text x="${plot.x0 - 8}" y="${yy + 4}" text-anchor="end" class="viz-tick">${vizEsc(fmt(v))}</text>`;
  }
  return { body, y, top: a.top };
}

// Show at most one x label per ~64px so ticks never collide. The last label is
// worth forcing (it is the newest point) but only when it has clear air after
// the previous kept one -- otherwise the two overprint each other.
function vizXLabels(labels, plot, band) {
  const slots = Math.max(1, Math.floor((plot.x1 - plot.x0) / 64));
  const every = Math.max(1, Math.ceil(labels.length / slots));
  const last = labels.length - 1;
  const lastKept = Math.floor(last / every) * every;
  const keepLast = last - lastKept >= every * 0.6;
  return labels.map((lab, i) => (i % every === 0 || (keepLast && i === last))
    ? `<text x="${plot.x0 + band * (i + 0.5)}" y="${plot.y1 + 18}" text-anchor="middle" class="viz-tick">${vizEsc(lab)}</text>`
    : "").join("");
}

function vizEmpty(height, msg = "No data in this window") {
  return `<div class="viz-empty" style="min-height:${Math.round(height / 3)}px">${vizEsc(msg)}</div>`;
}

/* A legend is always present for >= 2 series: identity must never be
 * colour-alone. Rendered as HTML (not SVG) so it wraps with the card.
 * A dotted series gets a dotted swatch and a dual-axis series says which axis
 * it reads, so the legend alone is enough to decode the plot. */
function vizLegend(series, opts = {}) {
  if (!series || series.length < 2) return "";
  return `<div class="viz-legend">` + series.map(s => {
    const side = opts.dual ? ` <em class="viz-axis-tag">${s.axis === "right" ? "right" : "left"}</em>` : "";
    const swatch = s.dash
      ? `<i class="dotted" style="--viz-key-color:${s.color}"></i>`
      : `<i style="background:${s.color}"></i>`;
    return `<span class="viz-key">${swatch}${vizEsc(s.label)}${side}</span>`;
  }).join("") + `</div>`;
}

// A dotted stroke reads as "sampled/derived over time". Round caps turn each
// 1-unit dash into a true dot.
const VIZ_DOT_DASH = `stroke-dasharray="1 5" stroke-linecap="round"`;

// Point markers make each plotted bucket findable on a solid line. Past this
// many points they merge into a band and stop being marks at all, so a dense
// series keeps the line and only its endpoint dot.
const VIZ_MARKER_MAX = 60;

/* ------------------------------------------------------- columns (stacked) */
/* opts: { rows, x, series:[{key,label,color}], fmt, height, labelTop } */
function vizColumns(rows, opts = {}) {
  const fmt = opts.fmt || vizCompact;
  const height = opts.height || 230;
  const series = opts.series || [];
  if (!rows.length) return vizEmpty(height);
  const totals = rows.map(r => series.reduce((a, s) => a + (Number(r[s.key]) || 0), 0));
  const peak = Math.max(...totals, 1);
  const plot = { x0: vizPadL(peak, fmt), x1: VIZ_BOX.w - VIZ_BOX.padR, y0: VIZ_BOX.padT, y1: height - VIZ_BOX.padB };
  const axis = vizYAxis(peak, plot, fmt);
  const band = (plot.x1 - plot.x0) / rows.length;
  const bw = Math.min(BAR_MAX, band - 6);

  let marks = "";
  rows.forEach((row, i) => {
    const cx = plot.x0 + band * (i + 0.5) - bw / 2;
    let acc = 0;
    const tip = series.map(s => `${s.label}: ${fmt(Number(row[s.key]) || 0)}`).join("\n");
    // Draw bottom-up; only the topmost segment carries the rounded data-end,
    // interior segments stay square and are separated by the 2px surface gap.
    const drawn = series.map((s, si) => {
      const v = Number(row[s.key]) || 0;
      if (v <= 0) return "";
      const yTop = axis.y(acc + v), yBot = axis.y(acc);
      const hasBelow = acc > 0;
      acc += v;
      const isTop = series.slice(si + 1).every(o => !(Number(row[o.key]) > 0));
      const h = Math.max(1, yBot - yTop - (hasBelow ? GAP : 0));
      return isTop
        ? `<path d="${vizBarPath(cx, yTop, bw, h, "up")}" fill="${s.color}"/>`
        : `<rect x="${cx}" y="${yTop}" width="${bw}" height="${h}" fill="${s.color}"/>`;
    }).join("");
    marks += `<g class="viz-mark"><title>${vizEsc(row[opts.x])}\n${vizEsc(tip)}</title>` +
      `<rect x="${plot.x0 + band * i}" y="${plot.y0}" width="${band}" height="${plot.y1 - plot.y0}" fill="transparent"/>` +
      drawn + `</g>`;
  });

  // Selective direct labels: the tallest column only (never one per point).
  let direct = "";
  if (opts.labelTop !== false) {
    const peak = totals.indexOf(Math.max(...totals));
    if (totals[peak] > 0) {
      direct = `<text x="${plot.x0 + band * (peak + 0.5)}" y="${axis.y(totals[peak]) - 6}" ` +
        `text-anchor="middle" class="viz-label">${vizEsc(fmt(totals[peak]))}</text>`;
    }
  }
  const body = axis.body + marks + direct +
    `<line x1="${plot.x0}" y1="${plot.y1}" x2="${plot.x1}" y2="${plot.y1}" stroke="${VIZ.axis}" stroke-width="1"/>` +
    vizXLabels(rows.map(r => r[opts.x]), plot, band);
  return vizSvg(height, body) + vizLegend(series);
}

/* ------------------------------------------------------------------- lines */
/* opts: { x, series:[{key,label,color,dash,axis}], fmt, fmtRight, height }
 *
 * A series with `axis: "right"` is measured against a second, independently
 * scaled y axis in the right gutter -- that is how "duration vs number of
 * records" plots two incomparable units on one timeline. Only the LEFT axis
 * draws gridlines (see vizYAxis) and the legend tags which side each series
 * reads, so the pairing is never guesswork. `dash: true` draws the series
 * dotted. */
function vizLines(rows, opts = {}) {
  const fmt = opts.fmt || vizCompact;
  const fmtRight = opts.fmtRight || vizCompact;
  const height = opts.height || 230;
  const series = opts.series || [];
  if (!rows.length) return vizEmpty(height);
  // null is a gap in the line, not a zero: a day with no timed load must not
  // draw a dive to the axis. (Number(null) is 0, so test before converting.)
  const val = (row, key) => row[key] == null ? NaN : Number(row[key]);
  const isRight = (s) => s.axis === "right";
  const dual = series.some(isRight);
  const peak = (list) => {
    let max = 0;
    rows.forEach(r => list.forEach(s => {
      const v = val(r, s.key); if (Number.isFinite(v)) max = Math.max(max, v);
    }));
    return max || 1;
  };
  const maxL = peak(series.filter(s => !isRight(s)));
  const maxR = dual ? peak(series.filter(isRight)) : 1;
  const plot = {
    x0: vizPadL(maxL, fmt),
    x1: VIZ_BOX.w - (dual ? vizPadR(maxR, fmtRight) : VIZ_BOX.padR),
    y0: VIZ_BOX.padT, y1: height - VIZ_BOX.padB,
  };
  const axisL = vizYAxis(maxL, plot, fmt);
  const axisR = dual ? vizYAxis(maxR, plot, fmtRight, 4, { side: "right" }) : null;
  const axisFor = (s) => (isRight(s) ? axisR : axisL);
  const fmtFor = (s) => (isRight(s) ? fmtRight : fmt);
  const band = (plot.x1 - plot.x0) / rows.length;
  const px = (i) => plot.x0 + band * (i + 0.5);

  let marks = "";
  series.forEach((s, si) => {
    const axis = axisFor(s), sfmt = fmtFor(s);
    const pts = rows.map((r, i) => ({ i, v: val(r, s.key) })).filter(p => Number.isFinite(p.v));
    if (!pts.length) return;
    const d = pts.map((p, k) => `${k ? "L" : "M"}${px(p.i).toFixed(1)},${axis.y(p.v).toFixed(1)}`).join("");
    // A single solid series gets a 10% wash under the line. A dotted one does
    // not: the fill would read as a solid area and defeat the dotted stroke.
    if (series.length === 1 && !s.dash) {
      marks += `<path d="${d}L${px(pts[pts.length - 1].i).toFixed(1)},${plot.y1}L${px(pts[0].i).toFixed(1)},${plot.y1}Z" fill="${s.color}" opacity=".1"/>`;
    }
    marks += `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round" ` +
      (s.dash ? VIZ_DOT_DASH : `stroke-linecap="round"`) + `/>`;
    // Each point gets a ringed dot; the 2px surface ring is what keeps two
    // series legible where they cross. The endpoint is drawn larger below, so
    // it is left out here rather than drawn over.
    if (s.marker && pts.length <= VIZ_MARKER_MAX) {
      marks += pts.slice(0, -1).map(p =>
        `<circle cx="${px(p.i).toFixed(1)}" cy="${axis.y(p.v).toFixed(1)}" r="3" ` +
        `fill="${s.color}" stroke="${VIZ.surface}" stroke-width="2"/>`).join("");
    }
    const last = pts[pts.length - 1];
    // End marker: >= 8px with a 2px surface ring so crossings stay legible.
    marks += `<circle cx="${px(last.i).toFixed(1)}" cy="${axis.y(last.v).toFixed(1)}" r="4" fill="${s.color}" stroke="${VIZ.surface}" stroke-width="2"/>`;
    marks += `<text x="${(px(last.i) - 8).toFixed(1)}" y="${(axis.y(last.v) - 9 - si * 13).toFixed(1)}" text-anchor="end" class="viz-label">${vizEsc(sfmt(last.v))}</text>`;
  });

  // One wide invisible hit band per x position: the tooltip target is the
  // column, not the 8px dot.
  const hits = rows.map((r, i) => {
    const tip = series.map(s => `${s.label}: ${fmtFor(s)(r[s.key])}`).join("\n");
    return `<g class="viz-mark"><title>${vizEsc(r[opts.x])}\n${vizEsc(tip)}</title>` +
      `<rect x="${plot.x0 + band * i}" y="${plot.y0}" width="${band}" height="${plot.y1 - plot.y0}" fill="transparent"/></g>`;
  }).join("");

  const body = axisL.body + (axisR ? axisR.body : "") + marks + hits +
    `<line x1="${plot.x0}" y1="${plot.y1}" x2="${plot.x1}" y2="${plot.y1}" stroke="${VIZ.axis}" stroke-width="1"/>` +
    vizXLabels(rows.map(r => r[opts.x]), plot, band);
  return vizSvg(height, body) + vizLegend(series, { dual });
}

/* --------------------------------------------------------- horizontal bars */
/* Ranked magnitude: one hue for every bar (never a value-ramp on nominal
 * categories), value labelled at the tip, optional secondary tick (p95).
 * opts: { rows:[{label,value,note,mark}], color, fmt, rowH } */
function vizBars(rows, opts = {}) {
  const fmt = opts.fmt || vizCompact;
  const rowH = opts.rowH || 26;
  const height = Math.max(60, VIZ_BOX.padT + rows.length * rowH + 12);
  if (!rows.length) return vizEmpty(140);
  const labelW = opts.labelW || 150;
  // The right gutter must fit the longest value label ("1h 8m 43s"), or the
  // label rides off the edge of the card.
  const x0 = labelW, x1 = VIZ_BOX.w - 84;
  // Scale to the BARS, not to the secondary marks: one table whose p95 is 200x
  // the average would otherwise flatten every bar on the chart to a sliver.
  // A mark past the axis is pinned to the end and flagged with a chevron; its
  // true value is in the tooltip and the data table.
  const max = vizNiceMax(Math.max(...rows.map(r => r.value || 0), 1));
  const color = opts.color || VIZ.cat[0];
  const bh = Math.min(BAR_MAX, rowH - GAP * 3);

  const body = rows.map((r, i) => {
    const y = VIZ_BOX.padT + i * rowH;
    const w = Math.max(1, Math.min(1, (r.value || 0) / max) * (x1 - x0));
    const beyond = r.mark > max;
    const markX = r.mark ? x0 + Math.min(1, r.mark / max) * (x1 - x0) : null;
    const tip = [r.label, `${opts.valueLabel || "value"}: ${fmt(r.value)}`,
                 r.mark ? `${opts.markLabel || "p95"}: ${fmt(r.mark)}${beyond ? " (beyond the axis)" : ""}` : "",
                 r.note || ""]
      .filter(Boolean).join("\n");
    return `<g class="viz-mark"><title>${vizEsc(tip)}</title>` +
      `<rect x="0" y="${y}" width="${VIZ_BOX.w}" height="${rowH}" fill="transparent"/>` +
      `<text x="${labelW - 10}" y="${y + bh / 2 + 4}" text-anchor="end" class="viz-cat">${vizEsc(r.label)}</text>` +
      `<path d="${vizBarPath(x0, y + (rowH - bh) / 2, w, bh, "right")}" fill="${color}"/>` +
      (markX !== null
        ? `<line x1="${markX}" y1="${y + (rowH - bh) / 2 - 2}" x2="${markX}" y2="${y + (rowH + bh) / 2 + 2}" stroke="${VIZ.ink}" stroke-width="2"/>` +
          (beyond ? `<text x="${markX + 3}" y="${y + bh / 2 + 4}" class="viz-tick">›</text>` : "")
        : "") +
      // Past BOTH the bar end and the p95 tick: a label that overlaps its own
      // secondary mark is unreadable at exactly the rows the reader cares about.
      `<text x="${Math.min(Math.max(x0 + w, markX || 0) + 8, VIZ_BOX.w - 4)}" y="${y + bh / 2 + 4}" class="viz-label">${vizEsc(fmt(r.value))}</text></g>`;
  }).join("");
  const legend = rows.some(r => r.mark) ? `<div class="viz-legend"><span class="viz-key"><i style="background:${color}"></i>${vizEsc(opts.valueLabel || "avg")}</span>` +
    `<span class="viz-key"><i class="tick"></i>${vizEsc(opts.markLabel || "p95")}</span></div>` : "";
  return vizSvg(height, body) + legend;
}

/* ------------------------------------------------------------------- donut */
/* Part-to-whole at a glance only (<= 6 slices, always with a labelled legend).
 * opts: { slices:[{label,value,color}], centerValue, centerLabel, fmt } */
function vizDonut(slices, opts = {}) {
  const fmt = opts.fmt || vizCompact;
  const total = slices.reduce((a, s) => a + (Number(s.value) || 0), 0);
  const size = 200, r = 74, ring = 22, cx = size / 2, cy = size / 2;
  if (!total) return vizEmpty(160);
  let angle = -Math.PI / 2;
  const visible = slices.filter(s => s.value > 0);
  const arcs = visible.map((s) => {
    const frac = s.value / total;
    // A 360° arc starts and ends at the same point, which SVG draws as nothing:
    // a lone slice has to be a stroked circle instead.
    if (frac >= 0.9999) {
      return `<g class="viz-mark"><title>${vizEsc(s.label)}: ${vizEsc(fmt(s.value))} (100%)</title>` +
        `<circle cx="${cx}" cy="${cy}" r="${r - ring / 2}" fill="none" stroke="${s.color}" stroke-width="${ring}"/></g>`;
    }
    // The 2px surface gap becomes a small angular gap between segments.
    const gap = visible.length > 1 ? Math.min(0.06, (Math.PI * 2) * 0.008) : 0;
    const a0 = angle + gap / 2, a1 = angle + frac * Math.PI * 2 - gap / 2;
    angle += frac * Math.PI * 2;
    const large = a1 - a0 > Math.PI ? 1 : 0;
    const pt = (a, rad) => `${(cx + Math.cos(a) * rad).toFixed(2)},${(cy + Math.sin(a) * rad).toFixed(2)}`;
    const d = `M${pt(a0, r)}A${r},${r} 0 ${large} 1 ${pt(a1, r)}` +
      `L${pt(a1, r - ring)}A${r - ring},${r - ring} 0 ${large} 0 ${pt(a0, r - ring)}Z`;
    return `<g class="viz-mark"><title>${vizEsc(s.label)}: ${vizEsc(fmt(s.value))} (${(frac * 100).toFixed(1)}%)</title>` +
      `<path d="${d}" fill="${s.color}"/></g>`;
  }).join("");
  const center = `<text x="${cx}" y="${cy - 2}" text-anchor="middle" class="viz-center">${vizEsc(opts.centerValue ?? fmt(total))}</text>` +
    `<text x="${cx}" y="${cy + 16}" text-anchor="middle" class="viz-tick">${vizEsc(opts.centerLabel || "total")}</text>`;
  const svg = `<svg class="viz viz-donut" viewBox="0 0 ${size} ${size}" role="img" preserveAspectRatio="xMidYMid meet">${arcs}${center}</svg>`;
  const legend = `<div class="viz-legend col">` + visible.map(s =>
    `<span class="viz-key"><i style="background:${s.color}"></i>${vizEsc(s.label)}` +
    `<b>${vizEsc(fmt(s.value))}</b><em>${total ? ((s.value / total) * 100).toFixed(0) : 0}%</em></span>`).join("") + `</div>`;
  return `<div class="viz-donut-wrap">${svg}${legend}</div>`;
}

/* ----------------------------------------------------------------- heatmap */
/* Magnitude on a grid -> one-hue sequential ramp, with a scale legend.
 * opts: { value(row,col) -> number|null, fmt, valueLabel, width }
 *
 * `width` widens the viewBox beyond the standard chart box. A heat map with
 * many columns and few rows is short and wide, so on a full-width card it can
 * afford a bigger canvas -- and the extra column width is what keeps the
 * per-cell value printable instead of colour-only (see the `cw > 34` test). */
function vizHeat(rows, cols, opts = {}) {
  const fmt = opts.fmt || vizCompact;
  if (!rows.length || !cols.length) return vizEmpty(160);
  const labelW = 132, top = 16, cell = 26, gap = GAP;
  const width = opts.width || VIZ_BOX.w;
  const cw = Math.max(14, (width - labelW - 10) / cols.length);
  // Column names are long and the columns are narrow, so they are set at -35°
  // and the bottom band is sized to whatever the longest one actually needs --
  // a fixed band clipped every name past ~13 characters, which is most table
  // names here. sin(35°) ~ 0.57 of a ~5.6px glyph is the vertical run per char.
  const NAME_MAX = 22;
  const longest = Math.min(NAME_MAX, Math.max(...cols.map(c => String(c).length)));
  const bottom = 22 + Math.round(longest * 3.2);
  const height = top + rows.length * cell + bottom;
  let max = 0;
  rows.forEach(r => cols.forEach(c => { const v = opts.value(r, c); if (Number.isFinite(v)) max = Math.max(max, v); }));
  const step = (v) => {
    if (!Number.isFinite(v) || v === null) return null;
    if (v <= 0) return VIZ.surface;
    const i = Math.min(VIZ.seq.length - 1, Math.round((v / (max || 1)) * (VIZ.seq.length - 1)));
    return VIZ.seq[Math.max(1, i)];
  };
  const body = rows.map((r, ri) => {
    const y = top + ri * cell;
    return `<text x="${labelW - 10}" y="${y + cell / 2 + 4}" text-anchor="end" class="viz-cat">${vizEsc(r)}</text>` +
      cols.map((c, ci) => {
        const v = opts.value(r, c);
        const fill = step(v);
        const x = labelW + ci * cw;
        if (fill === null) return "";
        const dark = Number.isFinite(v) && v > 0 && (v / (max || 1)) > 0.6;
        return `<g class="viz-mark"><title>${vizEsc(r)} · ${vizEsc(c)}\n${vizEsc(opts.valueLabel || "value")}: ${vizEsc(fmt(v))}</title>` +
          `<rect x="${x}" y="${y}" width="${cw - gap}" height="${cell - gap}" rx="3" fill="${fill}" ` +
          `stroke="${fill === VIZ.surface ? VIZ.grid : "none"}" stroke-width="1"/>` +
          (v > 0 && cw > 34
            ? `<text x="${x + (cw - gap) / 2}" y="${y + cell / 2 + 3}" text-anchor="middle" class="viz-cell ${dark ? "on-dark" : ""}">${vizEsc(fmt(v))}</text>`
            : "") + `</g>`;
      }).join("");
  }).join("") + cols.map((c, ci) => {
    const x = labelW + ci * cw + (cw - gap) / 2;
    const y = top + rows.length * cell + 14;
    const full = String(c);
    const name = full.length > NAME_MAX ? full.slice(0, NAME_MAX - 1) + "…" : full;
    // The tooltip carries the untruncated name, so a clipped label never hides
    // which table a column is.
    return `<g class="viz-mark"><title>${vizEsc(full)}</title>` +
      `<text x="${x}" y="${y}" text-anchor="end" class="viz-tick" transform="rotate(-35 ${x} ${y})">${vizEsc(name)}</text></g>`;
  }).join("");
  const scale = `<div class="viz-legend"><span class="viz-scale-label">${vizEsc(opts.valueLabel || "value")}</span>` +
    `<span class="viz-scale">${VIZ.seq.map(c => `<i style="background:${c}"></i>`).join("")}</span>` +
    `<span class="viz-scale-label">0 → ${vizEsc(fmt(max))}</span></div>`;
  return vizSvg(height, body, "viz-heat", width) + scale;
}

/* ---------------------------------------------------------------- meter/spark */
/* A single ratio against a limit: fill carries severity, track is a lighter
 * step of the same ramp. */
function vizMeter(pct, opts = {}) {
  const p = Math.max(0, Math.min(100, Number(pct) || 0));
  const color = opts.color || (p >= 99 ? VIZ.good : p >= 90 ? VIZ.warning : VIZ.critical);
  return `<span class="viz-meter" role="img" aria-label="${vizEsc(opts.label || "")} ${p}%">` +
    `<span class="viz-meter-fill" style="width:${p}%;background:${color}"></span></span>`;
}

/* 12-point trend for a stat tile. Values only -- no axis, no labels. */
function vizSpark(values, opts = {}) {
  const vals = (values || []).filter(v => v != null && Number.isFinite(Number(v))).map(Number).slice(-24);
  if (vals.length < 2) return "";
  const w = 120, h = 28, max = Math.max(...vals), min = Math.min(...vals);
  const span = max - min || 1;
  const d = vals.map((v, i) =>
    `${i ? "L" : "M"}${(i / (vals.length - 1) * w).toFixed(1)},${(h - ((v - min) / span) * (h - 4) - 2).toFixed(1)}`).join("");
  const color = opts.color || VIZ.cat[0];
  return `<svg class="viz-spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-hidden="true">` +
    `<path d="${d}" fill="none" stroke="${color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/></svg>`;
}
