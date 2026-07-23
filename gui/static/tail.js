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
  let currentPoll = null;

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

  // Callers must never see a promise that resolves without a poll having
  // been applied -- if one is already running, join it instead of no-oping.
  function poll() {
    if (inFlight) return currentPoll;
    currentPoll = runPoll();
    return currentPoll;
  }

  async function runPoll() {
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
    /* Poll once now; resolves when that poll has been applied. If a poll is
     * already in flight, joins it instead of no-oping -- the caller still
     * gets a promise that only resolves once a poll has actually landed. */
    async pollNow() { disarm(); delay = fast; await poll(); },
  };
}
