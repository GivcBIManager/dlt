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
 * stop() makes the instance fully inert: it cancels the pending timer AND
 * marks any fetch already in flight as stale, so that response's onChunk /
 * onStatus / onDone / onError never fire once it lands.
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
  let currentPoll = null, currentPollGen = -1;
  // Bumped by stop()/halt() so a fetchChunk already in flight at that moment
  // can tell, once it resolves, that its result is stale and must be dropped.
  let gen = 0;

  function disarm() {
    if (timer !== null) { clearTimeout(timer); timer = null; }
  }
  function arm() {
    if (!running || timer !== null || inFlight) return;
    if (typeof document !== "undefined" && document.hidden) return;
    timer = setTimeout(() => { timer = null; poll(); }, delay);
  }
  function halt() { running = false; gen++; disarm(); unwatch(); }

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
    if (inFlight) {
      if (currentPollGen === gen) return currentPoll;
      // The outstanding fetch belongs to a generation stop() already
      // invalidated -- its result will be dropped when it lands (below).
      // Preserve single-flight (no second concurrent fetchChunk) by waiting
      // for it to settle, then run a genuine poll under the current
      // generation instead of handing back a promise that would resolve
      // without ever applying a live result.
      currentPollGen = gen;
      currentPoll = currentPoll.then(poll);
      return currentPoll;
    }
    currentPollGen = gen;
    currentPoll = runPoll();
    return currentPoll;
  }

  async function runPoll() {
    const myGen = gen;
    inFlight = true;
    try {
      const res = await fetchChunk(offset);
      if (myGen !== gen) return;   // stopped while this fetch was in flight
      fails = 0;
      if (typeof res.offset === "number") offset = res.offset;
      if (res.chunk) { delay = fast; onChunk(res.chunk, res); }
      else { delay = Math.min(delay * TAIL_BACKOFF, slow); }
      onStatus(res);
      if (isTerminal(res)) {
        halt();
        // The server reads the log's size BEFORE the run status, so bytes
        // written between those two reads would be lost. Fetch once more.
        // halt() just bumped gen, but the check above already passed for
        // this poll -- that bump must not suppress this poll's own final
        // delivery, so nothing below re-checks gen.
        try {
          const last = await fetchChunk(offset);
          if (typeof last.offset === "number") offset = last.offset;
          if (last.chunk) onChunk(last.chunk, last);
        } catch (exc) { /* best effort: the run is already over */ }
        onDone(res);
        return;
      }
    } catch (exc) {
      if (myGen !== gen) return;   // stopped while this fetch was in flight
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
    /* Stop polling. Fully inert: a fetch already in flight is discarded when
     * it lands -- none of onChunk/onStatus/onDone/onError fire for it. */
    stop() { halt(); },
    /* Poll once now; resolves when that poll has been applied. If a poll is
     * already in flight, joins it instead of no-oping -- the caller still
     * gets a promise that only resolves once a poll has actually landed. */
    async pollNow() { disarm(); delay = fast; await poll(); },
  };
}
