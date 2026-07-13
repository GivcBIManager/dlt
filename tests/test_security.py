"""Tests for the GUI security gate (bind safety, auth, command lockdown)."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_GUI_TOKEN", raising=False)
    monkeypatch.delenv("OASIS_ALLOW_CUSTOM_CMD", raising=False)


# --- is_loopback ----------------------------------------------------------- #
@pytest.mark.parametrize("addr", ["127.0.0.1", "127.5.6.7", "::1", "localhost"])
def test_is_loopback_true(addr):
    import security
    assert security.is_loopback(addr) is True


@pytest.mark.parametrize("addr", ["0.0.0.0", "10.0.0.4", "192.168.1.5", "8.8.8.8", None, ""])
def test_is_loopback_false(addr):
    import security
    assert security.is_loopback(addr) is False


# --- check_bind (fail-closed on public bind without a token) --------------- #
def test_check_bind_allows_loopback_without_token():
    import security
    security.check_bind("127.0.0.1", None)  # must not raise


def test_check_bind_rejects_public_without_token():
    import security
    with pytest.raises(RuntimeError):
        security.check_bind("0.0.0.0", None)


def test_check_bind_allows_public_with_token():
    import security
    security.check_bind("0.0.0.0", "s3cret")  # must not raise


# --- debugger_allowed ------------------------------------------------------ #
def test_debugger_allowed_only_on_loopback():
    import security
    assert security.debugger_allowed("127.0.0.1") is True
    assert security.debugger_allowed("0.0.0.0") is False


# --- request_authorized ---------------------------------------------------- #
def test_request_authorized_loopback_without_token():
    import security
    assert security.request_authorized("127.0.0.1", None, None) is True


def test_request_authorized_public_requires_matching_token():
    import security
    assert security.request_authorized("10.0.0.9", None, "abc") is False
    assert security.request_authorized("10.0.0.9", "wrong", "abc") is False
    assert security.request_authorized("10.0.0.9", "abc", "abc") is True


def test_request_authorized_public_allowed_when_no_token_configured():
    # check_bind guarantees a token exists for public binds, but the gate itself
    # should not lock out a loopback-only deployment that set no token.
    import security
    assert security.request_authorized("127.0.0.1", None, None) is True


# --- custom command lockdown ---------------------------------------------- #
def test_custom_commands_disabled_by_default():
    import security
    assert security.custom_commands_allowed() is False


def test_custom_commands_enabled_by_env(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_ALLOW_CUSTOM_CMD", "1")
    assert security.custom_commands_allowed() is True
