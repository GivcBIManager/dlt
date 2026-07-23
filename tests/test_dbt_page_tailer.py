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
