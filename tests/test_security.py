"""Tests for the GUI security gate (bind safety, auth, command lockdown)."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_GUI_USER", raising=False)
    monkeypatch.delenv("OASIS_GUI_PASSWORD", raising=False)
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


# --- gui_credentials ------------------------------------------------------- #
def test_gui_credentials_unset_is_none():
    import security
    assert security.gui_credentials() is None


def test_gui_credentials_requires_both(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_GUI_USER", "admin")
    assert security.gui_credentials() is None
    monkeypatch.delenv("OASIS_GUI_USER")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "pw")
    assert security.gui_credentials() is None


def test_gui_credentials_set(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_GUI_USER", "  admin  ")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "s3cret")
    assert security.gui_credentials() == ("admin", "s3cret")


def test_gui_credentials_blank_password_is_none(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_GUI_USER", "admin")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "   ")
    assert security.gui_credentials() is None


# --- credentials_match ----------------------------------------------------- #
def test_credentials_match_ok():
    import security
    assert security.credentials_match("admin", "pw", ("admin", "pw")) is True


@pytest.mark.parametrize("user,password", [
    ("admin", "wrong"), ("wrong", "pw"), ("", "pw"), ("admin", ""), (None, None),
])
def test_credentials_match_rejects(user, password):
    import security
    assert security.credentials_match(user, password, ("admin", "pw")) is False


def test_credentials_match_no_expected():
    import security
    assert security.credentials_match("admin", "pw", None) is False


# --- load_or_create_secret_key --------------------------------------------- #
def test_secret_key_created_and_persisted(tmp_path):
    import security
    path = tmp_path / "secret_key"
    key = security.load_or_create_secret_key(path)
    assert len(key) == 32
    assert path.read_bytes() == key
    # second call returns the same key
    assert security.load_or_create_secret_key(path) == key


def test_secret_key_regenerated_when_corrupt(tmp_path):
    import security
    path = tmp_path / "secret_key"
    path.write_bytes(b"short")  # < 16 bytes: treated as corrupt
    key = security.load_or_create_secret_key(path)
    assert len(key) == 32
    assert path.read_bytes() == key


# --- check_bind (fail-closed on public bind without credentials) ----------- #
def test_check_bind_allows_loopback_without_credentials():
    import security
    security.check_bind("127.0.0.1", None)  # must not raise


def test_check_bind_rejects_public_without_credentials():
    import security
    with pytest.raises(RuntimeError):
        security.check_bind("0.0.0.0", None)


def test_check_bind_allows_public_with_credentials():
    import security
    security.check_bind("0.0.0.0", ("admin", "pw"))  # must not raise


# --- debugger_allowed ------------------------------------------------------ #
def test_debugger_allowed_only_on_loopback():
    import security
    assert security.debugger_allowed("127.0.0.1") is True
    assert security.debugger_allowed("0.0.0.0") is False


# --- request_authorized ---------------------------------------------------- #
def test_request_authorized_loopback_without_login():
    import security
    assert security.request_authorized("127.0.0.1", False) is True


def test_request_authorized_public_requires_login():
    import security
    assert security.request_authorized("10.0.0.9", False) is False
    assert security.request_authorized("10.0.0.9", True) is True


# --- custom command lockdown ----------------------------------------------- #
def test_custom_commands_disabled_by_default():
    import security
    assert security.custom_commands_allowed() is False


def test_custom_commands_enabled_by_env(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_ALLOW_CUSTOM_CMD", "1")
    assert security.custom_commands_allowed() is True
