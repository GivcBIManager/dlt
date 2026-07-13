"""The free-form ``custom`` run script is gated off by default (RCE surface)."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_ALLOW_CUSTOM_CMD", raising=False)


def test_custom_rejected_by_default():
    import commands
    with pytest.raises(ValueError, match="custom"):
        commands.build_argv({"script": "custom", "custom": "echo hi"})


def test_custom_allowed_when_opted_in(monkeypatch):
    monkeypatch.setenv("OASIS_ALLOW_CUSTOM_CMD", "1")
    import commands
    argv, label = commands.build_argv({"script": "custom", "custom": "echo hi"})
    assert argv == ["echo", "hi"]
    assert label.startswith("custom")


def test_custom_empty_still_rejected_when_opted_in(monkeypatch):
    monkeypatch.setenv("OASIS_ALLOW_CUSTOM_CMD", "1")
    import commands
    with pytest.raises(ValueError, match="empty"):
        commands.build_argv({"script": "custom", "custom": ""})
