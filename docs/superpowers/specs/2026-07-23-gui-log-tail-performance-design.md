# Adaptive log tailing in the GUI (Run / Monitor / Models)

**Date:** 2026-07-23
**Status:** approved

## Problem

Live log output in the GUI arrives late and in bursts rather than smoothly.
Three pages tail a run's log, and each hand-rolls the same polling loop with its
own offset, failure counter, banner and `setInterval`:

- `gui/templates/run.html` — `pollTail()`, 1300 ms
- `gui/templates/logs.html` — `refreshFile()`, 3000 ms (shared with `loadFiles()`)
- `gui/templates/dbt.html` — `tailRun()`'s inner `poll()`, 1300 ms

Four defects compound into the perceived lag:

1. **No in-flight guard.** The loops are `async` functions driven by
   `setInterval`. When a poll exceeds its interval, the next one starts while
   `tailOffset` is still stale, so both requests read the same byte range and
   both append it. Output is duplicated *and* delivered in doubled bursts.
   `frTail()` in `logs.html` is the only tailer that guards (`frBusy`).
2. **Unconditional re-render.** `run.html` calls `liveDash.render()` on every
   tick, outside the `if (r.chunk)` branch. `render()` rebuilds the branch
   strip, the whole per-table `<tbody>` and the issues list through
   `innerHTML =` — a full teardown every 1.3 s even when zero bytes arrived.
3. **Quadratic appends.** `c.textContent += r.chunk` re-serializes the entire
   console node on every append, so per-tick cost grows with the log.
4. **Fixed cadence.** A 1300 ms interval puts a hard 0–1300 ms staleness floor
   under every line, whether or not the log is actively streaming.

A fifth issue is server-side: `RunManager.tail()` reads the file size and *then*
the run status, so bytes written between those two reads are lost when the run
ends on that same tick — the last chunk of a finished run can be silently
dropped.

**Not a UI problem:** `PROGRESS` heartbeats are emitted every
`progress_interval_s` (default `5.0`, `etl/config.py:318`). The dashboard's
stage / elapsed / rows / rss fields can never be fresher than that regardless of
poll rate. The per-unit `[branch/table] N rows` lines are event-driven and *can*
be made near-instant.

## Decision

Extract one shared, adaptive tailer into `gui/static/tail.js` and migrate all
three call sites to it. Client-side only; no server API changes.

Rejected alternatives:

- **SSE push** (`/api/runs/<id>/stream` + `EventSource`) — lowest latency, but
  holds a Flask thread per viewer and needs reconnect, teardown and
  proxy-buffering handling *plus* a polling fallback. Too much surface for a
  1–2 user admin panel until polling is proven insufficient.
- **Server-side long-poll** (hold `/tail` open ~1 s waiting for new bytes) —
  would beat this design on both latency and request count, but it is a server
  change that holds threads, i.e. most of SSE's cost. Kept in reserve.
- **Lowering `progress_interval_s` to ~2 s** — a one-line ETL config change that
  makes the dashboard's numbers move more often. Orthogonal to this work and
  increases log volume; deferred as an optional follow-up tuning knob.

## Components

### 1. `gui/static/tail.js` — `createTailPoller(opts)`

A single unit owning offset, scheduling and failure state. It depends only on
`apiGet` and its injected callbacks — no DOM access — so the cadence and guard
logic are testable against a fake fetcher.

**Options:**

| Option | Meaning |
| --- | --- |
| `fetchChunk(offset)` | async, returns `{offset, chunk, status?, returncode?}` |
| `onChunk(chunk, res)` | called only when `chunk` is non-empty |
| `onStatus(res)` | called every successful poll |
| `onDone(res)` | terminal status reached, after the final poll |
| `onError(fails, max)` | a poll threw; `fails` is the consecutive count |
| `isTerminal(res)` | defaults to `() => false` — see below |
| `fast`, `slow`, `maxFails` | cadence + retry knobs (defaults below) |

**API:** `start()` (begin, or resume from the current offset after a failure —
this is the reconnect path, and it must *not* re-read from 0 or the console
would show the log twice), `stop()` (make the poller inert: cancel any pending
timer *and* discard a response already in flight), and `pollNow()` (poll once
immediately, resolving when that poll has been applied — `openFile()` needs the
first read applied before it can decide how to lay out the panel; if a poll is
already in flight it joins that one rather than no-opping).

`stop()` must discard the in-flight response, not deliver it. An earlier draft
of this design had it deliver — on the reasoning that those bytes were asked
for — which is wrong here because the callbacks write into DOM that a log
switch has already reassigned. Concretely: `openFile("b.log")` stops the poller
and clears `#file-content`, then `a.log`'s outstanding response lands and
appends `a.log`'s bytes into the console now showing `b.log`, and feeds those
lines into the shared `fileDash` parser. `viewRun()` has the same shape when
switching runs. Implemented with a generation counter bumped on stop and
re-checked after every `await` that can span one.

There is deliberately no `restart()`. Every call site that needs a fresh read
(`viewRun`, `openFile`, `tailRun`) builds a new poller, which has to capture the
new run/file id in its `fetchChunk` closure anyway.

`isTerminal` **must default to never-terminal**, not to a status check. Only
`/api/runs/<id>/tail` returns a `status` field; `/api/logs/<name>` returns
`{name, offset, chunk, truncated}` with no status at all. A status-based default
would see `undefined`, judge every Monitor poll terminal and stop the loop after
one tick. `run.html` and `dbt.html` pass
`res => res.status !== "running" && res.status !== "detached"` explicitly;
`logs.html` passes nothing and is stopped by its auto-refresh checkbox instead.

The poller never touches the DOM or the offset of another poller; each call site
owns its own instance.

### 2. Scheduling — chained, adaptive, guarded

`setTimeout` is re-armed **only after the previous poll settles**, which
structurally eliminates defect 1 — there can never be two in-flight polls
sharing a stale offset.

Cadence:

- `FAST = 400` ms — next delay after any poll that returned a non-empty chunk.
- `SLOW = 2000` ms — ceiling.
- Quiet backoff: `delay = min(delay * 1.5, SLOW)` after each empty chunk.
- Any non-empty chunk snaps the delay back to `FAST`.
- `start()`, `pollNow()` and refocus poll immediately at `FAST`.

`fast` and `slow` are named constants exposed as options so they can be retuned
without editing the loop.

**Accepted trade-off:** an actively streaming run rises from ~46 req/min to
~150. Each request is a `stat()` plus a `seek`/`read` of only the new bytes,
with no database access, so the per-request cost is small — but it is roughly
3x more requests during the phase when the machine is busiest with the pipeline
itself. An idle or quiet run falls to ~30 req/min, below today's constant rate.

### 3. Terminal-status handling

On observing a terminal status the poller performs **one final `fetchChunk`**
before invoking `onDone` and stopping. This closes the server-side race in
`RunManager.tail()` (file size read before run status) without changing the
endpoint.

### 4. Rendering — only on change, coalesced

Two changes at the call sites, not in the poller:

- `liveDash.render()` / `fileDash.render()` move inside the "chunk arrived"
  path, so a quiet tick costs nothing.
- Renders are deferred through `requestAnimationFrame`, with any pending frame
  reused, so a burst of fast polls produces one layout pass rather than three.
- Console appends become `c.append(document.createTextNode(chunk))` instead of
  `c.textContent += chunk`, so a tick no longer re-serializes the whole node.

The existing "stick to bottom unless the user scrolled up" behaviour
(`c.scrollTop + c.clientHeight >= c.scrollHeight - 30`) is preserved, measured
before the append as it is today.

### 5. Visibility handling

While `document.hidden` is true the poller stops re-arming; on
`visibilitychange` back to visible it polls immediately rather than waiting out
the current delay. This removes background-tab chatter and the stale-then-burst
effect on refocus.

### 6. Call-site migration

- **`run.html`** — keeps `liveDash`, `setTailBanner()` and the stop-button
  wiring; `viewRun()` builds a poller, `reconnectTail()` calls `start()` to
  resume from where the failed poll left off. `onDone` flushes the dash and
  refreshes the run list, as today.
- **`logs.html`** — `refreshFile()` becomes the poller's `onChunk`; the
  `fileOffset === 0` "fresh file" branch becomes a new poller plus an awaited
  `pollNow()` in `openFile()`. `loadFiles()` moves **off** the tail cadence onto
  its own 15 s timer, since the file list (name, size, mtime) does not need
  re-fetching every 3 s.
- **`dbt.html`** — drops its duplicate loop; `tailRun()` builds a poller and
  keeps `setDbtBusy()` / `setDbtBanner()` in the callbacks.

`base.html` loads `tail.js` alongside `app.js` and `runparse.js`.

## Error handling

Failure semantics are preserved exactly as they are today, just centralised:
a poll that throws increments a consecutive-failure counter and fires
`onError(fails, max)`; the loop keeps retrying on the backoff schedule until
`maxFails` (10), at which point it stops and the page shows its existing
"Connection lost / Reconnect" banner. Any successful poll resets the counter to
zero and clears the banner. A `404` from a purged or unknown log is a normal
throw and follows the same path — no special case.

## Testing

`createTailPoller` takes an injected `fetchChunk` and touches no DOM, so it is
unit-testable directly:

- a slow poll never overlaps the next — with a fetcher that resolves after
  2x the interval, assert one in-flight request at a time and that no byte
  range is delivered to `onChunk` twice;
- the cadence ramps `FAST → SLOW` across consecutive empty chunks and snaps
  back to `FAST` on the first non-empty one;
- a terminal status triggers exactly one extra `fetchChunk` before `onDone`,
  and a chunk returned by that final poll is still delivered;
- consecutive throws increment `onError` and stop at `maxFails`; a success in
  between resets the counter;
- `stop()` cancels a pending timer, no further `fetchChunk` occurs, and a
  response already in flight is discarded rather than delivered — no `onChunk`,
  `onStatus`, `onDone` or `onError` fires for it.
- `stop()` followed immediately by `start()` on the same instance, with a fetch
  still outstanding, resumes cleanly. The stale in-flight promise must not be
  handed to the restarted poller, or it resolves under the old generation, the
  re-arm is skipped, and the poller goes permanently dead. This is reachable
  from the Monitor page's auto-refresh checkbox.

DOM wiring (append path, render coalescing, scroll anchoring, visibility pause)
is verified by tailing a real `oracle_to_iceberg` run from the Run page and a
large existing file from Monitor.

## Out of scope

- SSE or server-side long-polling (see rejected alternatives).
- Any change to `/api/runs/<id>/tail`, `/api/logs/<name>` or
  `RunManager.tail()`.
- Console buffer capping / scrollback trimming for very long runs. The
  `createTextNode` change removes the per-tick quadratic cost; unbounded
  retained scrollback is a separate memory concern, not this latency fix.
- `progress_interval_s` tuning in `etl/config.py`.
- The Dagster flow-run tailer (`frTail()` in `logs.html`), which already guards
  with `frBusy` and polls a cursor-based GraphQL endpoint rather than a byte
  offset.
