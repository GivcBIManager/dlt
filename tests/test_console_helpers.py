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


def test_set_status_pill_renders_pill_and_returncode(page):
    html = page.evaluate("""() => {
      const box = document.createElement("div");
      setStatusPill(box, "failed", 2);
      return box.innerHTML;
    }""")
    assert 'class="pill failed"' in html
    assert "rc=2" in html


def test_set_status_pill_omits_returncode_while_running(page):
    html = page.evaluate("""() => {
      const box = document.createElement("div");
      setStatusPill(box, "running", null);
      return box.innerHTML;
    }""")
    assert 'class="pill running"' in html
    assert "rc=" not in html


def test_set_status_pill_skips_redundant_writes(page):
    """onStatus fires on every poll, so rewriting identical innerHTML would
    discard and rebuild the pill node ~150x/min during an active run. Checked by
    node identity: tagging the node instead would itself change box.innerHTML
    and defeat the very guard under test."""
    kept = page.evaluate("""() => {
      const box = document.createElement("div");
      setStatusPill(box, "running", null);
      const firstNode = box.firstElementChild;
      setStatusPill(box, "running", null);   // identical -> must not rebuild
      return box.firstElementChild === firstNode;
    }""")
    assert kept is True


def test_set_status_pill_rewrites_when_status_changes(page):
    html = page.evaluate("""() => {
      const box = document.createElement("div");
      setStatusPill(box, "running", null);
      setStatusPill(box, "finished", 0);
      return box.innerHTML;
    }""")
    assert 'class="pill finished"' in html
    assert "rc=0" in html
