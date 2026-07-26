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
