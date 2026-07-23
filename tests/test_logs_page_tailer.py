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
    # Scoped to the file-content tailer (openFile/etc, which precedes frTail in
    # the template). The out-of-scope frTail() legitimately still reserializes
    # its own #fr-log console the old way -- it is a separate, cursor-based
    # tailer this task must not touch.
    file_tailer_js = logs_html.split("async function frTail")[0]
    assert "c.textContent += r.chunk" not in file_tailer_js


def test_flow_run_tailer_is_left_alone(logs_html):
    # frTail already guards with frBusy and is cursor-based, not offset-based.
    assert "frBusy" in logs_html
