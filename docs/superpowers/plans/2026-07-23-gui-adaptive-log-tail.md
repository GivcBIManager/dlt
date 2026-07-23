# Adaptive GUI Log Tailing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the three hand-rolled `setInterval` log-tail loops in the GUI with one shared adaptive poller, so live output arrives ~3x fresher and can no longer be duplicated by overlapping requests.

**Architecture:** A new `gui/static/tail.js` exports `createTailPoller(opts)` — it owns one log's byte offset, poll scheduling and failure state, touches no DOM, and re-arms via chained `setTimeout` only after the previous poll settles. Two small DOM helpers (`appendConsole`, `coalesce`) join the existing shared helpers in `gui/static/app.js`. The Run, Monitor and Models pages then delete their own loops and construct pollers instead. No server-side changes.

**Tech Stack:** Vanilla ES2020 browser JS (no build step, no framework), Jinja2 templates, Flask. Tests are pytest; the JS is exercised headlessly through Playwright + Chromium.

## Global Constraints

- **No server API changes.** `/api/runs/<id>/tail`, `/api/logs/<name>` and `gui/pipeline_runner.py` are untouched by every task in this plan.
- **No build step and no new browser dependencies.** `gui/static/*.js` are plain scripts loaded by `<script src>`; keep them ES2020-compatible and framework-free.
- **Cadence constants:** `fast = 400` ms, `slow = 2000` ms, backoff factor `1.5`, `maxFails = 10`. Define them once in `tail.js` and expose them as overridable options.
- **`isTerminal` must default to `() => false`.** Only `/api/runs/<id>/tail` returns a `status` field; `/api/logs/<name>` returns `{name, offset, chunk, truncated}` with none. A status-based default would read `undefined`, judge every Monitor poll terminal, and stop the loop after one tick.
- **Preserve existing failure UX exactly:** consecutive failures increment a counter and show `Reconnecting… (n/10)`; at 10 the loop stops and shows the `Connection lost / Reconnect` banner; any success resets the counter and clears the banner.
- **Preserve scroll anchoring:** stay pinned to the bottom only when the user is already within 30 px of it, measured *before* the append.
- Spec: `docs/superpowers/specs/2026-07-23-gui-log-tail-performance-design.md`.

---

### Task 1: `createTailPoller` — the shared tailer

**Files:**
- Create: `gui/static/tail.js`
- Modify: `gui/templates/base.html:90-92`
- Modify: `requirements-dev.txt`
- Test: `tests/test_tail_poller.py`

**Interfaces:**
- Consumes: nothing (leaf module).
- Produces: global `createTailPoller(opts) -> {start, stop, pollNow}`.
  - `opts.fetchChunk(offset: number) -> Promise<{offset?: number, chunk?: string, status?: string, returncode?: number}>` — required.
  - `opts.onChunk(chunk: string, res: object) -> void` — called only when `chunk` is a non-empty string.
  - `opts.onStatus(res: object) -> void` — called after every successful poll.
  - `opts.onDone(res: object) -> void` — called once, after the extra final poll, when `isTerminal` is true.
  - `opts.onError(fails: number, max: number, err: Error) -> void` — called on each throw.
  - `opts.isTerminal(res: object) -> boolean` — defaults to `() => false`.
  - `opts.fast`, `opts.slow`, `opts.maxFails` — numbers, defaulting to 400 / 2000 / 10.
  - `start()` begins polling, or resumes from the current offset after a failure — this is the reconnect path, and it must not re-read from 0 or the console would show the log twice. `pollNow()` returns a Promise that resolves once a single immediate poll has been applied. `stop()` cancels any pending timer; a response already in flight still delivers.
  - There is deliberately no `restart()`: every call site that needs a fresh read (`viewRun`, `openFile`, `tailRun`) builds a new poller, which captures the new run/file id in its `fetchChunk` closure anyway.

- [ ] **Step 1: Add Playwright to the dev requirements**

Playwright is already present in `.venv` but undeclared. The tests below skip cleanly when it or its browser is missing, so this does not become a hard requirement for running the suite.

In `requirements-dev.txt`, append:

```
# Headless-browser tests for gui/static/*.js (tests/test_tail_poller.py).
# Needs a one-time browser download:  playwright install chromium
# Tests skip themselves when it or the browser is unavailable.
playwright>=1.40
```

- [ ] **Step 2: Write the failing tests**

Create `tests/test_tail_poller.py`:

```python
"""Headless unit tests for gui/static/tail.js (createTailPoller).

The poller owns byte offset, poll scheduling and failure state and touches no
DOM, so it can be driven directly in a blank page with an injected fetchChunk.
Skipped when playwright or its chromium build is unavailable.
"""
from __future__ import annotations

from pathlib import Path

import pytest

sync_api = pytest.importorskip("playwright.sync_api")

TAIL_JS = Path(__file__).resolve().parents[1] / "gui" / "static" / "tail.js"


@pytest.fixture(scope="module")
def page():
    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as exc:  # noqa: BLE001 - no browser installed
            pytest.skip(f"chromium unavailable: {exc}")
        pg = browser.new_page()
        pg.set_content("<html><body></body></html>")
        pg.add_script_tag(content=TAIL_JS.read_text(encoding="utf-8"))
        yield pg
        browser.close()


def test_slow_polls_never_overlap_or_duplicate(page):
    """A poll slower than the interval must not let a second one start on the
    stale offset -- the bug that duplicated output in the old setInterval loop."""
    res = page.evaluate("""async () => {
      const offsets = [];
      let concurrent = 0, maxConcurrent = 0, n = 0;
      const chunks = [];
      const poller = createTailPoller({
        fast: 10, slow: 20,
        fetchChunk: async (offset) => {
          concurrent++; maxConcurrent = Math.max(maxConcurrent, concurrent);
          offsets.push(offset);
          await new Promise(r => setTimeout(r, 60));   // 6x the fast interval
          concurrent--; n++;
          return { offset: offset + 5, chunk: "abcde" };
        },
        onChunk: (c) => chunks.push(c),
      });
      poller.start();
      await new Promise(r => setTimeout(r, 500));
      poller.stop();
      return { maxConcurrent, offsets, chunkCount: chunks.length };
    }""")
    assert res["maxConcurrent"] == 1
    assert res["offsets"] == sorted(set(res["offsets"]))       # never re-fetched
    assert res["chunkCount"] == len(res["offsets"])            # never re-delivered


def test_cadence_backs_off_when_quiet_and_snaps_back_on_data(page):
    gaps = page.evaluate("""async () => {
      const stamps = [];
      let i = 0;
      const poller = createTailPoller({
        fast: 20, slow: 400,
        fetchChunk: async (offset) => {
          stamps.push(performance.now());
          i++;
          return { offset, chunk: i === 7 ? "data" : "" };   // 7th poll has data
        },
      });
      poller.start();
      await new Promise(r => setTimeout(r, 900));
      poller.stop();
      return stamps.slice(1).map((t, k) => t - stamps[k]);
    }""")
    assert len(gaps) >= 7
    assert gaps[5] > gaps[0] * 2      # ramped up across the quiet polls
    assert gaps[6] < gaps[5] / 2      # snapped back after the data-bearing poll


def test_terminal_status_triggers_one_final_poll(page):
    """The server reads log size before run status, so the last bytes of a run
    can land between those two reads. One extra poll after terminal recovers them."""
    res = page.evaluate("""async () => {
      let n = 0;
      const chunks = [], done = [];
      const poller = createTailPoller({
        fast: 10,
        fetchChunk: async (offset) => {
          n++;
          if (n === 1) return { offset: 5, chunk: "aaaaa", status: "running" };
          if (n === 2) return { offset: 5, chunk: "",      status: "finished" };
          return { offset: 9, chunk: "tail!", status: "finished" };
        },
        onChunk: (c) => chunks.push(c),
        onDone: (r) => done.push(r.status),
        isTerminal: (r) => r.status !== "running" && r.status !== "detached",
      });
      poller.start();
      await new Promise(r => setTimeout(r, 300));
      return { calls: n, chunks, done };
    }""")
    assert res["calls"] == 3                       # not 2, and not still polling
    assert res["chunks"] == ["aaaaa", "tail!"]     # final bytes delivered
    assert res["done"] == ["finished"]             # onDone fired exactly once


def test_default_is_never_terminal(page):
    """Monitor's /api/logs/<name> has no status field; a status-based default
    would see undefined, call it terminal and stop after one poll."""
    res = page.evaluate("""async () => {
      let n = 0, done = 0;
      const poller = createTailPoller({
        fast: 10,
        fetchChunk: async (offset) => { n++; return { offset, chunk: "x" }; },
        onDone: () => { done++; },
      });
      poller.start();
      await new Promise(r => setTimeout(r, 200));
      poller.stop();
      return { calls: n, done };
    }""")
    assert res["calls"] > 3
    assert res["done"] == 0


def test_failures_count_up_stop_at_max_and_reset_on_success(page):
    res = page.evaluate("""async () => {
      let n = 0;
      const seen = [];
      const poller = createTailPoller({
        fast: 10, slow: 10, maxFails: 3,
        fetchChunk: async (offset) => {
          n++;
          if (n === 2) return { offset, chunk: "ok" };   // resets the counter
          throw new Error("boom");
        },
        onError: (fails, max) => seen.push(fails),
      });
      poller.start();
      await new Promise(r => setTimeout(r, 400));
      const callsAtStop = n;
      await new Promise(r => setTimeout(r, 150));
      return { seen, callsAtStop, callsLater: n };
    }""")
    assert res["seen"] == [1, 1, 2, 3]        # reset by the success on poll 2
    assert res["callsLater"] == res["callsAtStop"]   # stopped at maxFails


def test_stop_prevents_further_fetches(page):
    res = page.evaluate("""async () => {
      let n = 0;
      const poller = createTailPoller({
        fast: 10,
        fetchChunk: async (offset) => { n++; return { offset, chunk: "" }; },
      });
      poller.start();
      await new Promise(r => setTimeout(r, 120));
      poller.stop();
      const atStop = n;
      await new Promise(r => setTimeout(r, 200));
      return { atStop, after: n };
    }""")
    assert res["atStop"] >= 2
    assert res["after"] == res["atStop"]


def test_pollnow_resolves_after_applying_one_poll(page):
    """openFile() needs the first read applied before it can lay out the panel."""
    res = page.evaluate("""async () => {
      const chunks = [];
      const poller = createTailPoller({
        fetchChunk: async (offset) => ({ offset: 4, chunk: "abcd" }),
        onChunk: (c) => chunks.push(c),
      });
      await poller.pollNow();
      return { chunks: chunks.slice() };   // already applied when the await returns
    }""")
    assert res["chunks"] == ["abcd"]
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_tail_poller.py -v`

Expected: every test FAILS. Playwright reports `ReferenceError: createTailPoller is not defined`, because `gui/static/tail.js` does not exist yet — `add_script_tag` raises on the missing file first, which is also an acceptable failure at this step.

- [ ] **Step 4: Write `gui/static/tail.js`**

```js
/* Shared adaptive log tailer for the Run, Monitor and Models pages.
 *
 * One instance owns one log's byte offset, poll scheduling and failure state.
 * Scheduling is CHAINED: the next poll is armed only after the previous one
 * settles, so two polls can never race on a stale offset. (The setInterval
 * loops this replaces could, which fetched and appended the same byte range
 * twice.) Cadence adapts -- fast while bytes are flowing, backing off while the
 * log is quiet -- so an active run feels ~3x fresher than the old fixed 1300ms
 * while an idle one costs fewer requests than before.
 *
 * Touches no DOM and reads no globals beyond the injected fetchChunk, so the
 * cadence and guard logic are testable headlessly (tests/test_tail_poller.py).
 */
const TAIL_FAST_MS = 400;    // delay after a poll that returned new bytes
const TAIL_SLOW_MS = 2000;   // ceiling once the log has gone quiet
const TAIL_BACKOFF = 1.5;    // multiplier applied per empty poll
const TAIL_MAX_FAILS = 10;   // consecutive failures before giving up

function createTailPoller(opts = {}) {
  const fetchChunk = opts.fetchChunk;
  const onChunk = opts.onChunk || (() => {});
  const onStatus = opts.onStatus || (() => {});
  const onDone = opts.onDone || (() => {});
  const onError = opts.onError || (() => {});
  // MUST default to never-terminal: /api/logs/<name> returns no status field,
  // so a status check here would see undefined and stop after one poll.
  const isTerminal = opts.isTerminal || (() => false);
  const fast = opts.fast ?? TAIL_FAST_MS;
  const slow = opts.slow ?? TAIL_SLOW_MS;
  const maxFails = opts.maxFails ?? TAIL_MAX_FAILS;

  let offset = 0, delay = fast, fails = 0;
  let timer = null, running = false, inFlight = false, watching = false;

  function disarm() {
    if (timer !== null) { clearTimeout(timer); timer = null; }
  }
  function arm() {
    if (!running || timer !== null || inFlight) return;
    if (typeof document !== "undefined" && document.hidden) return;
    timer = setTimeout(() => { timer = null; poll(); }, delay);
  }
  function halt() { running = false; disarm(); unwatch(); }

  // Pause while the tab is hidden; on return, poll immediately rather than
  // waiting out the current delay (which would show a stale-then-burst jump).
  function onVisibility() {
    if (!running) return;
    if (document.hidden) disarm();
    else { disarm(); delay = fast; poll(); }
  }
  function watch() {
    if (watching || typeof document === "undefined") return;
    document.addEventListener("visibilitychange", onVisibility);
    watching = true;
  }
  function unwatch() {
    if (!watching) return;
    document.removeEventListener("visibilitychange", onVisibility);
    watching = false;
  }

  async function poll() {
    if (inFlight) return;
    inFlight = true;
    try {
      const res = await fetchChunk(offset);
      fails = 0;
      if (typeof res.offset === "number") offset = res.offset;
      if (res.chunk) { delay = fast; onChunk(res.chunk, res); }
      else { delay = Math.min(delay * TAIL_BACKOFF, slow); }
      onStatus(res);
      if (isTerminal(res)) {
        halt();
        // The server reads the log's size BEFORE the run status, so bytes
        // written between those two reads would be lost. Fetch once more.
        try {
          const last = await fetchChunk(offset);
          if (typeof last.offset === "number") offset = last.offset;
          if (last.chunk) onChunk(last.chunk, last);
        } catch (exc) { /* best effort: the run is already over */ }
        onDone(res);
        return;
      }
    } catch (exc) {
      fails++;
      onError(fails, maxFails, exc);
      if (fails >= maxFails) { halt(); return; }
      delay = Math.min(delay * TAIL_BACKOFF, slow);
    } finally {
      inFlight = false;
    }
    arm();
  }

  return {
    /* Begin (or resume after a failure) polling from the current offset. */
    start() {
      if (running) return;
      running = true; fails = 0; delay = fast;
      watch(); disarm(); poll();
    },
    /* Stop polling. An already in-flight response still delivers. */
    stop() { halt(); },
    /* Poll once now; resolves when that poll has been applied. */
    async pollNow() { disarm(); delay = fast; await poll(); },
  };
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_tail_poller.py -v`

Expected: 7 passed. If Chromium is not installed, expected instead is 7 skipped — in that case run `.venv/Scripts/python.exe -m playwright install chromium` and re-run to get real coverage.

- [ ] **Step 6: Load `tail.js` on every page**

In `gui/templates/base.html`, replace lines 90-92:

```html
  <script src="{{ url_for('static', filename='app.js') }}"></script>
  <script src="{{ url_for('static', filename='runparse.js') }}"></script>
  {% block scripts %}{% endblock %}
```

with:

```html
  <script src="{{ url_for('static', filename='app.js') }}"></script>
  <script src="{{ url_for('static', filename='tail.js') }}"></script>
  <script src="{{ url_for('static', filename='runparse.js') }}"></script>
  {% block scripts %}{% endblock %}
```

- [ ] **Step 7: Verify the pages still render with the new script**

Run: `.venv/Scripts/python.exe -m pytest tests/test_run_iceberg_pages_render.py -v`

Expected: 3 passed.

- [ ] **Step 8: Commit**

```bash
git add gui/static/tail.js gui/templates/base.html requirements-dev.txt tests/test_tail_poller.py
git commit -m "feat(gui): shared adaptive log tailer (createTailPoller)"
```

---

### Task 2: `appendConsole` and `coalesce` DOM helpers

**Files:**
- Modify: `gui/static/app.js` (append after `pill()`, around line 59)
- Test: `tests/test_console_helpers.py`

**Interfaces:**
- Consumes: nothing.
- Produces: two globals used by Tasks 3-5.
  - `appendConsole(node: HTMLElement, text: string) -> void` — appends a text node, keeping the view pinned to the bottom only if it already was.
  - `coalesce(fn: () => void) -> () => void` — returns a wrapper that collapses repeated calls into one `requestAnimationFrame` callback.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_console_helpers.py`:

```python
"""Headless tests for the console DOM helpers in gui/static/app.js.

appendConsole replaces `node.textContent += chunk`, which re-serialized the
whole console node on every poll; coalesce collapses a burst of fast polls into
one layout pass.
"""
from __future__ import annotations

from pathlib import Path

import pytest

sync_api = pytest.importorskip("playwright.sync_api")

APP_JS = Path(__file__).resolve().parents[1] / "gui" / "static" / "app.js"


@pytest.fixture(scope="module")
def page():
    with sync_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as exc:  # noqa: BLE001 - no browser installed
            pytest.skip(f"chromium unavailable: {exc}")
        pg = browser.new_page()
        pg.set_content("<html><body></body></html>")
        pg.add_script_tag(content=APP_JS.read_text(encoding="utf-8"))
        yield pg
        browser.close()


def test_append_console_accumulates_text(page):
    text = page.evaluate("""() => {
      const pre = document.createElement("pre");
      document.body.appendChild(pre);
      appendConsole(pre, "one\\n");
      appendConsole(pre, "two\\n");
      const out = pre.textContent;
      pre.remove();
      return out;
    }""")
    assert text == "one\ntwo\n"


def test_append_console_does_not_escape_or_reparse_html(page):
    """Appending a text node must keep log content inert -- a log line
    containing markup stays literal text, never child elements."""
    res = page.evaluate("""() => {
      const pre = document.createElement("pre");
      document.body.appendChild(pre);
      appendConsole(pre, "<b>not markup</b>");
      const out = { text: pre.textContent, children: pre.children.length };
      pre.remove();
      return out;
    }""")
    assert res["text"] == "<b>not markup</b>"
    assert res["children"] == 0


def test_append_console_sticks_to_bottom_when_already_there(page):
    at_bottom = page.evaluate("""() => {
      const pre = document.createElement("pre");
      pre.style.cssText = "height:40px;overflow:auto;margin:0";
      document.body.appendChild(pre);
      for (let i = 0; i < 60; i++) appendConsole(pre, `line ${i}\\n`);
      const stuck = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 30;
      pre.remove();
      return stuck;
    }""")
    assert at_bottom is True


def test_append_console_respects_a_user_who_scrolled_up(page):
    stayed = page.evaluate("""() => {
      const pre = document.createElement("pre");
      pre.style.cssText = "height:40px;overflow:auto;margin:0";
      document.body.appendChild(pre);
      for (let i = 0; i < 60; i++) appendConsole(pre, `line ${i}\\n`);
      pre.scrollTop = 0;                       // user scrolls back to the top
      appendConsole(pre, "new line\\n");
      const top = pre.scrollTop;
      pre.remove();
      return top;
    }""")
    assert stayed == 0


def test_coalesce_collapses_a_burst_into_one_call(page):
    calls = page.evaluate("""async () => {
      let calls = 0;
      const render = coalesce(() => { calls++; });
      render(); render(); render(); render();
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      return calls;
    }""")
    assert calls == 1


def test_coalesce_allows_a_later_call_after_the_frame(page):
    calls = page.evaluate("""async () => {
      let calls = 0;
      const render = coalesce(() => { calls++; });
      render();
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      render();
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      return calls;
    }""")
    assert calls == 2
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_console_helpers.py -v`

Expected: 6 failed with `ReferenceError: appendConsole is not defined` / `coalesce is not defined`.

- [ ] **Step 3: Add the helpers to `gui/static/app.js`**

Insert immediately after the `pill()` function (currently ending at line 59), before the `renderTable` comment block:

```js
// Append log text to a <pre>-style console. Uses a text node rather than
// `node.textContent += text`, which reads back and re-serializes the entire
// console on every append -- cost that grows with the log and showed up as
// increasingly laggy tailing. Stays pinned to the bottom only if the view
// already was, so a user who scrolled up to read something is left alone.
function appendConsole(node, text) {
  const atBottom = node.scrollTop + node.clientHeight >= node.scrollHeight - 30;
  node.append(document.createTextNode(text));
  if (atBottom) node.scrollTop = node.scrollHeight;
}

// Collapse repeated calls into one invocation per animation frame, so a burst
// of fast polls costs a single layout pass instead of one per poll.
function coalesce(fn) {
  let pending = false;
  return function () {
    if (pending) return;
    pending = true;
    requestAnimationFrame(() => { pending = false; fn(); });
  };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_console_helpers.py -v`

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add gui/static/app.js tests/test_console_helpers.py
git commit -m "feat(gui): appendConsole + coalesce console helpers"
```

---

### Task 3: Migrate the Run page

**Files:**
- Modify: `gui/templates/run.html:145-146` (state), `:156-170` (banner/reconnect), `:438-478` (viewRun/pollTail)
- Test: `tests/test_run_page_tailer.py`

**Interfaces:**
- Consumes: `createTailPoller` (Task 1), `appendConsole` + `coalesce` (Task 2).
- Produces: nothing consumed by later tasks. `reconnectTail()` stays a global because the banner's inline `onclick` calls it.

- [ ] **Step 1: Write the failing structural test**

The behaviour lives in a Jinja template, so guard it the way `tests/test_run_iceberg_pages_render.py` already does — assert on the served HTML that the old loop is gone and the new one is wired.

Create `tests/test_run_page_tailer.py`:

```python
"""The Run page must tail through the shared poller, not its own setInterval.

Regression guard for the duplicate-append bug: an async poll driven by
setInterval could start a second request on a stale offset, so the same byte
range was fetched and appended twice.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


@pytest.fixture
def run_html(client):
    return client.get("/run").get_data(as_text=True)


def test_run_page_uses_the_shared_poller(run_html):
    assert "createTailPoller(" in run_html


def test_run_page_has_no_interval_driven_tail_loop(run_html):
    assert "setInterval(pollTail" not in run_html
    assert "tailTimer" not in run_html


def test_run_console_appends_without_reserializing(run_html):
    assert "appendConsole(" in run_html
    assert 'c.textContent += r.chunk' not in run_html


def test_run_dash_render_is_coalesced(run_html):
    assert "coalesce(" in run_html
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_run_page_tailer.py -v`

Expected: 4 failed — `createTailPoller(`, `appendConsole(` and `coalesce(` are absent, and `setInterval(pollTail` / `tailTimer` are still present.

- [ ] **Step 3: Replace the Run page's tail state**

In `gui/templates/run.html`, replace lines 145-146:

```js
let curRun = null, tailOffset = 0, tailTimer = null, tailFails = 0;
const TAIL_MAX_FAILS = 10;
```

with:

```js
let curRun = null, tailPoller = null;
```

- [ ] **Step 4: Make the banner idempotent and simplify reconnect**

Replace lines 156-170 (`setTailBanner` and `reconnectTail`):

```js
function setTailBanner(html) {
  const b = el("tail-banner");
  if (!b) return;
  b.innerHTML = html;
  b.hidden = !html;
  b.className = "banner " + (/Connection lost/.test(html) ? "error" : "warn");
}
function reconnectTail() {
  if (!curRun) return;
  tailFails = 0;
  setTailBanner("Reconnecting…");
  clearInterval(tailTimer);
  pollTail();
  tailTimer = setInterval(pollTail, 1300);
}
```

with:

```js
// A no-op when the text is unchanged: the poller calls this on every tick and
// the polls are now ~3x more frequent, so an unconditional innerHTML write
// would be pure churn.
function setTailBanner(html) {
  const b = el("tail-banner");
  if (!b || b.innerHTML === html) return;
  b.innerHTML = html;
  b.hidden = !html;
  b.className = "banner " + (/Connection lost/.test(html) ? "error" : "warn");
}
// start() resumes from the current offset, so a reconnect picks up exactly
// where the failed poll left off rather than re-downloading the whole log.
function reconnectTail() {
  if (!tailPoller) return;
  setTailBanner("Reconnecting…");
  tailPoller.start();
}
```

- [ ] **Step 5: Replace `viewRun` and delete `pollTail`**

Replace lines 438-478 (`viewRun` through the end of `pollTail`):

```js
function viewRun(id) {
  curRun = id; tailOffset = 0; tailFails = 0;
  setTailBanner("");
  el("cur-run").textContent = id;
  el("console").textContent = "";
  liveDash.reset();
  clearInterval(tailTimer);
  pollTail();
  tailTimer = setInterval(pollTail, 1300);
}

async function pollTail() {
  ...
}
```

with:

```js
// One layout pass per frame no matter how many polls land in it: render()
// rebuilds the branch strip, the per-table tbody and the issues list.
const renderLiveDash = coalesce(() => liveDash.render());

function viewRun(id) {
  curRun = id;
  setTailBanner("");
  el("cur-run").textContent = id;
  el("console").textContent = "";
  liveDash.reset();
  if (tailPoller) tailPoller.stop();
  tailPoller = createTailPoller({
    fetchChunk: (offset) => apiGet(`/api/runs/${id}/tail?offset=${offset}`),
    isTerminal: (r) => r.status !== "running" && r.status !== "detached",
    onChunk: (chunk) => {
      appendConsole(el("console"), chunk);
      liveDash.feed(chunk);
      renderLiveDash();
    },
    onStatus: (r) => {
      const label = pill(r.status) +
        (r.returncode != null ? ` <small>rc=${r.returncode}</small>` : "");
      const box = el("cur-status");
      if (box.innerHTML !== label) box.innerHTML = label;
      el("stop-btn").hidden = !(r.status === "running" || r.status === "detached");
      setTailBanner("");
    },
    onDone: () => { liveDash.flush(); liveDash.render(); loadRuns(); },
    onError: (fails, max) => {
      setTailBanner(fails >= max
        ? "Connection lost. <a href='#' onclick='reconnectTail();return false;'>Reconnect</a>"
        : `Reconnecting… (${fails}/${max})`);
    },
  });
  tailPoller.start();
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_run_page_tailer.py tests/test_run_iceberg_pages_render.py -v`

Expected: 7 passed.

- [ ] **Step 7: Verify against a real run**

Start the GUI (`.venv/Scripts/python.exe gui/app.py`), open <http://127.0.0.1:8765/run>, and launch an `oracle_to_iceberg` run in `INCREMENTAL` mode. Confirm:
- output appears noticeably sooner after each log line than before, without visible bursts;
- no line is ever printed twice in the console;
- scrolling up mid-run keeps your position; scrolling back to the bottom re-pins;
- switching to another browser tab for ~30 s and back shows an immediate catch-up, not a delayed one;
- when the run ends, the status pill flips, the Stop button hides, the run list refreshes, and the final `[runner] exited with code N` line is present.

- [ ] **Step 8: Commit**

```bash
git add gui/templates/run.html tests/test_run_page_tailer.py
git commit -m "perf(gui): tail the Run page through the shared adaptive poller"
```

---

### Task 4: Migrate the Monitor page

**Files:**
- Modify: `gui/templates/logs.html:119` (state), `:269-300` (openFile/refreshFile), `:488-492` (auto-refresh wiring)
- Test: `tests/test_logs_page_tailer.py`

**Interfaces:**
- Consumes: `createTailPoller` (Task 1), `appendConsole` + `coalesce` (Task 2).
- Produces: nothing consumed by later tasks.

Note: `frTail()` / `frLoadRuns()` (the Dagster flow-run tab) are explicitly **out of scope** — they already guard with `frBusy` and poll a cursor-based GraphQL endpoint rather than a byte offset. Leave that timer at 3000 ms.

- [ ] **Step 1: Write the failing structural test**

Create `tests/test_logs_page_tailer.py`:

```python
"""The Monitor page must tail through the shared poller, and must stop
re-fetching the whole log-file list on the tail cadence."""
from __future__ import annotations

import pytest


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


@pytest.fixture
def logs_html(client):
    return client.get("/logs").get_data(as_text=True)


def test_logs_page_uses_the_shared_poller(logs_html):
    assert "createTailPoller(" in logs_html


def test_file_list_refresh_is_off_the_tail_cadence(logs_html):
    assert "setInterval(() => { refreshFile(); loadFiles(); }, 3000)" not in logs_html
    assert "FILES_REFRESH_MS" in logs_html


def test_logs_console_appends_without_reserializing(logs_html):
    assert "appendConsole(" in logs_html
    assert "c.textContent += r.chunk" not in logs_html


def test_flow_run_tailer_is_left_alone(logs_html):
    # frTail already guards with frBusy and is cursor-based, not offset-based.
    assert "frBusy" in logs_html
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_logs_page_tailer.py -v`

Expected: 3 failed (`createTailPoller(`, `FILES_REFRESH_MS`, `appendConsole(` absent), 1 passed (`frBusy` is already there).

- [ ] **Step 3: Replace the Monitor page's tail state**

In `gui/templates/logs.html`, replace line 119:

```js
let curFile = null, autoTimer = null, fileOffset = 0;
```

with:

```js
let curFile = null, filePoller = null, filesTimer = null;
// The file list (name, size, mtime) changes far more slowly than a log's
// contents, so it no longer rides the tail cadence.
const FILES_REFRESH_MS = 15000;
const renderFileDash = coalesce(() => fileDash.render());
```

- [ ] **Step 4: Replace `openFile` and `refreshFile`**

Replace lines 269-300:

```js
async function openFile(name) {
  ...
}
async function refreshFile() {
  ...
}
```

with:

```js
async function openFile(name) {
  curFile = name;
  el("file-title").textContent = name;
  $$("#files-table tr.clickable").forEach(tr => tr.classList.toggle("selected", tr.dataset.f === name));
  if (filePoller) filePoller.stop();
  el("file-content").textContent = "";
  fileDash.reset();
  filePoller = createTailPoller({
    // /api/logs/<name> returns no status field, so isTerminal is left at its
    // never-terminal default -- the auto-refresh checkbox stops this loop.
    fetchChunk: (offset) => apiGet(`/api/logs/${encodeURIComponent(name)}?offset=${offset}`),
    onChunk: (chunk) => {
      const c = el("file-content");
      if (c.textContent === "(empty)") c.textContent = "";
      appendConsole(c, chunk);
      fileDash.feed(chunk);
      renderFileDash();
    },
    onError: (fails, max) => { if (fails === 1) err("Log refresh failed — retrying"); },
  });
  // Await the first read: the panel layout below depends on whether the parsed
  // summary produced anything.
  await filePoller.pollNow();
  fileDash.flush();
  fileDash.render();
  if (!el("file-content").textContent) el("file-content").textContent = "(empty)";
  // Summary-first: when a parsed summary is available, collapse the raw log by
  // default (the "Raw log" button reveals it). Fall back to raw when there's none.
  el("file-panel").classList.toggle("raw-hidden", !el("run-dash").hidden);
  if (el("auto").checked) filePoller.start();
}
```

- [ ] **Step 5: Rewire the auto-refresh checkbox**

Replace lines 488-491:

```js
el("auto").onchange = (e) => {
  clearInterval(autoTimer);
  if (e.target.checked) autoTimer = setInterval(() => { refreshFile(); loadFiles(); }, 3000);
};
```

with:

```js
el("auto").onchange = (e) => {
  clearInterval(filesTimer); filesTimer = null;
  if (e.target.checked) {
    if (filePoller) filePoller.start();
    filesTimer = setInterval(loadFiles, FILES_REFRESH_MS);
  } else if (filePoller) {
    filePoller.stop();
  }
};
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_logs_page_tailer.py -v`

Expected: 4 passed.

- [ ] **Step 7: Verify against a real log**

With the GUI running, open <http://127.0.0.1:8765/logs>, pick a large file from the Log files tab, and confirm:
- it opens with content and the parsed summary, and the raw log is collapsed when a summary exists;
- ticking **Auto** while a run is live streams new lines without duplicates;
- unticking **Auto** stops both the content and file-list refreshes;
- switching to the **Flow runs** tab still tails Dagster runs as before.

- [ ] **Step 8: Commit**

```bash
git add gui/templates/logs.html tests/test_logs_page_tailer.py
git commit -m "perf(gui): tail the Monitor page through the shared adaptive poller"
```

---

### Task 5: Migrate the Models (dbt) page

**Files:**
- Modify: `gui/templates/dbt.html:233` (offset reset), `:239-260` (tailRun), plus the `tailTimer`/`tailOffset` declarations and `reconnectDbtTail`
- Test: `tests/test_dbt_page_tailer.py`

**Interfaces:**
- Consumes: `createTailPoller` (Task 1), `appendConsole` (Task 2).
- Produces: nothing.

- [ ] **Step 1: Write the failing structural test**

Create `tests/test_dbt_page_tailer.py`:

```python
"""The Models page must tail dbt runs through the shared poller."""
from __future__ import annotations

import pytest


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


@pytest.fixture
def dbt_html(client):
    return client.get("/models").get_data(as_text=True)


def test_dbt_page_uses_the_shared_poller(dbt_html):
    assert "createTailPoller(" in dbt_html


def test_dbt_page_has_no_interval_driven_tail_loop(dbt_html):
    assert "setInterval(poll, 1300)" not in dbt_html
    assert "tailTimer" not in dbt_html


def test_dbt_console_appends_without_reserializing(dbt_html):
    assert "appendConsole(" in dbt_html
    assert "c.textContent += r.chunk" not in dbt_html
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dbt_page_tailer.py -v`

Expected: 3 failed.

- [ ] **Step 3: Replace the tail state declaration**

Replace lines 77-78:

```js
let MODELS = [], TESTS = [], curFile = null, curSel = null, newKind = null,
    tailTimer = null, tailOffset = 0, tailFails = 0;
```

with:

```js
let MODELS = [], TESTS = [], curFile = null, curSel = null, newKind = null,
    dbtPoller = null;
```

Then in `runDbt()`, replace line 233:

```js
  tailOffset = 0; tailFails = 0; setDbtBanner("");
```

with:

```js
  setDbtBanner("");
```

The offset and failure counters now belong to the poller, and `tailRun()` builds a fresh one per run.

- [ ] **Step 4: Replace `tailRun` and `reconnectDbtTail`**

Replace lines 239-260 (all of `tailRun`) with:

```js
function tailRun(id) {
  if (dbtPoller) dbtPoller.stop();
  dbtPoller = createTailPoller({
    fetchChunk: (offset) => apiGet(`/api/runs/${id}/tail?offset=${offset}`),
    isTerminal: (r) => r.status !== "running" && r.status !== "detached",
    onChunk: (chunk) => appendConsole(el("dbt-console"), chunk),
    onStatus: (r) => {
      const label = pill(r.status) +
        (r.returncode != null ? ` <small>rc=${r.returncode}</small>` : "");
      const box = el("run-status");
      if (box.innerHTML !== label) box.innerHTML = label;
      setDbtBanner("");
    },
    onDone: () => setDbtBusy(false),
    onError: (fails, max) => {
      if (fails >= max) {
        setDbtBusy(false);
        setDbtBanner(`Connection lost. <a href='#' onclick='reconnectDbtTail("${id}");return false;'>Reconnect</a>`);
      } else {
        setDbtBanner(`Reconnecting… (${fails}/${max})`);
      }
    },
  });
  dbtPoller.start();
}
```

Then replace line 88:

```js
function reconnectDbtTail(id) { tailFails = 0; setDbtBanner("Reconnecting…"); setDbtBusy(true); tailRun(id); }
```

with:

```js
// start() resumes from the current offset. Calling tailRun() here instead would
// build a fresh poller at offset 0 and re-append the whole log to a console
// that already has it.
function reconnectDbtTail(id) {
  if (!dbtPoller) { tailRun(id); return; }
  setDbtBanner("Reconnecting…");
  setDbtBusy(true);
  dbtPoller.start();
}
```

Also make `setDbtBanner` idempotent, matching Task 3. Replace lines 81-87:

```js
function setDbtBanner(html) {
  const b = el("dbt-tail-banner");
  if (!b) return;
  b.innerHTML = html;
  b.hidden = !html;
  b.className = "banner " + (/Connection lost/.test(html) ? "error" : "warn");
}
```

with:

```js
function setDbtBanner(html) {
  const b = el("dbt-tail-banner");
  if (!b || b.innerHTML === html) return;   // no-op: onStatus calls this every poll
  b.innerHTML = html;
  b.hidden = !html;
  b.className = "banner " + (/Connection lost/.test(html) ? "error" : "warn");
}
```

Note: the console now keeps a user's scroll position when they have scrolled up, where the old code forced `c.scrollTop = c.scrollHeight` on every append. That is `appendConsole`'s standard behaviour and matches the Run and Monitor pages.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dbt_page_tailer.py -v`

Expected: 3 passed.

- [ ] **Step 6: Run the whole suite**

Run: `.venv/Scripts/python.exe -m pytest -q`

Expected: no failures, and no test that passed before this branch now fails.

- [ ] **Step 7: Verify against a real dbt run**

With the GUI running, open <http://127.0.0.1:8765/models> and click **Run**. Confirm output streams without duplicates, the status pill flips to `finished`/`failed` at the end, the Run/Test/Compile buttons re-enable, and the reconnect banner behaves if you stop the Flask process mid-run and restart it.

- [ ] **Step 8: Commit**

```bash
git add gui/templates/dbt.html tests/test_dbt_page_tailer.py
git commit -m "perf(gui): tail the Models page through the shared adaptive poller"
```

---

### Task 6: Document the tailer

**Files:**
- Modify: `gui/README.md` (the **Notes** section, currently lines 87-94)

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Add a note to `gui/README.md`**

Append to the **Notes** list:

```markdown
- Live log views (Run, Monitor, Models) share one adaptive tailer,
  `gui/static/tail.js`. It polls fast (~400 ms) while a log is producing output
  and backs off to ~2 s when quiet, pauses while the browser tab is hidden, and
  chains its requests so two polls can never race on a stale byte offset. Tune
  the cadence via the `fast` / `slow` options in `createTailPoller`.
```

- [ ] **Step 2: Commit**

```bash
git add gui/README.md
git commit -m "docs(gui): note the shared adaptive log tailer"
```

---

## Verification

After Task 6, the branch should satisfy:

```bash
.venv/Scripts/python.exe -m pytest -q
```

with no failures, and `grep -rn "setInterval" gui/templates/*.html` should return only the Dagster flow-run timer in `logs.html` (out of scope by design).

## Deferred

Recorded here so they are not silently lost:

- **`progress_interval_s` tuning.** `PROGRESS` heartbeats are emitted every 5.0 s (`etl/config.py:318`), which is a hard floor on how often the dashboard's stage / elapsed / rows / rss fields can change, regardless of poll rate. Dropping it to ~2 s is a one-line config change that would make those numbers move more often, at the cost of more log volume.
- **SSE or server-side long-polling.** Both would beat this design on latency *and* request count, but each holds a Flask thread per viewer and needs reconnect/teardown handling plus a polling fallback. Revisit only if the adaptive poller still feels stale on a real run.
- **Console buffer capping.** `appendConsole` removes the per-tick quadratic cost, but a multi-hour run still retains unbounded scrollback in the tab. That is a memory concern, separate from this latency work.
