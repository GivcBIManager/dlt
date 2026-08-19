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


def test_flow_run_tab_moved_to_its_own_module(client, logs_html):
    """The flow-run tab is no longer inline JS: it owns pollers, a step
    timeline and a parsed dashboard, which belong in a static module."""
    assert "flowruns.js" in logs_html
    assert "frTail" not in logs_html
    assert client.get("/static/flowruns.js").status_code == 200


@pytest.fixture
def flowruns_js(client):
    return client.get("/static/flowruns.js").get_data(as_text=True)


def test_flow_run_tail_uses_the_shared_poller(flowruns_js):
    """The old hand-rolled drain loop is replaced by the adaptive poller, so
    the flow-run log backs off when quiet and pauses on a hidden tab."""
    assert "createTailPoller(" in flowruns_js
    assert "appendConsole(" in flowruns_js


def test_flow_run_tail_stops_on_a_finished_run(flowruns_js):
    # isTerminal must require BOTH a terminal status and a drained cursor,
    # otherwise the last lines of a finishing run are never fetched.
    assert "isTerminal: (r) => !r.has_more && !!r.status && !isActive(r.status)" in flowruns_js


def test_flow_run_polls_stop_when_the_tab_is_left(logs_html, flowruns_js):
    assert "flowRuns.deactivate()" in logs_html
    assert "function deactivate()" in flowruns_js


def test_flow_run_list_rows_are_diffed(flowruns_js):
    """A 4s list refresh must not rebuild every row -- only the ones whose
    status/progress actually moved."""
    assert "rowSig(" in flowruns_js
    assert "rowSigs.get(r.run_id) === sig" in flowruns_js


def test_reconnect_file_tail_is_defined(logs_html):
    """Monitor must match Run/Models: a dead poller needs a reconnect path,
    not silent freezing behind a still-ticked auto-refresh checkbox."""
    assert "function reconnectFileTail(" in logs_html


def test_file_tail_failure_shows_a_reconnect_link(logs_html):
    # Scoped to the file tailer's own onError -- must wire the same
    # onclick='reconnectFileTail();return false;' pattern run.html uses for
    # #tail-banner, not just define the function unused.
    assert "onclick='reconnectFileTail();return false;'" in logs_html


def test_file_tail_uses_a_banner_helper(logs_html):
    assert "setFileBanner(" in logs_html
