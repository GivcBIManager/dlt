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
      // stop() cancels the pending timer AND marks the fetch already in
      // flight (up to 60ms long here) as stale, so its chunk is dropped when
      // it lands rather than delivered. Give it room to land (and confirm it
      // does NOT arrive) before reading the arrays below.
      await new Promise(r => setTimeout(r, 100));
      return { maxConcurrent, offsets, chunkCount: chunks.length };
    }""")
    assert res["maxConcurrent"] == 1
    assert res["offsets"] == sorted(set(res["offsets"]))       # never re-fetched
    # Every completed-before-stop fetch delivered exactly once; the one still
    # in flight at stop() time is discarded, not re-delivered -- so exactly
    # one fewer chunk than fetches started, never more, never a duplicate.
    assert res["chunkCount"] == len(res["offsets"]) - 1


def test_stop_discards_a_response_already_in_flight(page):
    """stop() must make the poller fully inert: a fetch already in flight when
    stop() is called must not fire onChunk/onStatus/onDone/onError once it
    lands, even though the fetch itself still runs to completion."""
    res = page.evaluate("""async () => {
      let calls = 0;
      const events = [];
      const poller = createTailPoller({
        fast: 10, slow: 20,
        fetchChunk: async (offset) => {
          calls++;
          await new Promise(r => setTimeout(r, 150));   // well past the poll interval
          return { offset: offset + 5, chunk: "abcde", status: "running" };
        },
        onChunk: () => events.push("chunk"),
        onStatus: () => events.push("status"),
        onDone: () => events.push("done"),
        onError: () => events.push("error"),
      });
      poller.start();
      await new Promise(r => setTimeout(r, 30));   // the first fetch is now in flight
      poller.stop();
      await new Promise(r => setTimeout(r, 200));  // let the in-flight fetch resolve
      return { calls, events };
    }""")
    assert res["calls"] == 1          # the in-flight fetch still ran to completion
    assert res["events"] == []        # but nothing it produced was ever delivered


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


def test_pollnow_joins_an_in_flight_poll_instead_of_no_oping(page):
    """pollNow() called mid-fetch must not resolve until that poll actually
    lands -- a naive `if (inFlight) return;` guard in poll() would let it
    resolve immediately with nothing delivered, breaking Monitor's openFile()
    which lays out the panel right after `await filePoller.pollNow()`."""
    res = page.evaluate("""async () => {
      const chunks = [];
      let calls = 0;
      const poller = createTailPoller({
        fast: 10, slow: 20,
        fetchChunk: async (offset) => {
          calls++;
          await new Promise(r => setTimeout(r, 150));   // well over the fast interval
          return { offset: offset + 5, chunk: "abcde" };
        },
        onChunk: (c) => chunks.push(c),
      });
      poller.start();
      await new Promise(r => setTimeout(r, 30));   // start()'s first poll is mid-flight
      const before = performance.now();
      await poller.pollNow();
      const elapsed = performance.now() - before;
      poller.stop();
      return { calls, chunksAtResolve: chunks.length, elapsed };
    }""")
    assert res["calls"] == 1                    # joined, not a second concurrent fetch
    assert res["chunksAtResolve"] == 1           # resolved only after the chunk landed
    assert res["elapsed"] > 80                   # didn't resolve early on the in-flight poll
