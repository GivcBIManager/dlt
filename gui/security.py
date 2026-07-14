"""GUI security gate: bind safety, request auth, and command lockdown.

The control panel can launch processes and edit pipeline config, so exposing it
on a non-loopback interface without authentication is a remote-code-execution
risk. This module centralises the guard rails:

* ``check_bind`` fails closed when bound to a public interface with no
  credentials configured.
* ``request_authorized`` allows local (loopback) use with no login but requires
  a signed-in session for any non-loopback client.
* ``custom_commands_allowed`` keeps the free-form ``custom`` run script off by
  default (it runs arbitrary argv).

Configuration (all optional):
* ``OASIS_GUI_USER`` / ``OASIS_GUI_PASSWORD``  login credentials required for
  non-loopback clients (both must be set).
* ``OASIS_ALLOW_CUSTOM_CMD`` set to ``1`` to permit the ``custom`` run script.
"""
from __future__ import annotations

import hmac
import ipaddress
import os
import secrets
from pathlib import Path

_LOOPBACK_NAMES = {"localhost", "localhost.localdomain"}


def is_loopback(addr: str | None) -> bool:
    """True for loopback hosts (127.0.0.0/8, ::1, ``localhost``)."""
    if not addr:
        return False
    if addr in _LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(addr).is_loopback
    except ValueError:
        return False


def gui_credentials() -> tuple[str, str] | None:
    """The configured (user, password) pair, or ``None`` unless both are set."""
    user = (os.environ.get("OASIS_GUI_USER") or "").strip()
    password = os.environ.get("OASIS_GUI_PASSWORD") or ""
    if not user or not password.strip():
        return None
    return user, password


def credentials_match(user: str | None, password: str | None,
                      expected: tuple[str, str] | None) -> bool:
    """Constant-time credential comparison; False if anything is missing."""
    if not user or not password or not expected:
        return False
    ok_user = hmac.compare_digest(str(user), expected[0])
    ok_password = hmac.compare_digest(str(password), expected[1])
    return ok_user and ok_password


def load_or_create_secret_key(path: Path | str) -> bytes:
    """Return the persisted session-signing key, creating it on first use.

    A short or unreadable file is treated as corrupt and regenerated (existing
    sessions are invalidated, users just sign in again) — never crash over it.
    """
    path = Path(path)
    try:
        data = path.read_bytes()
        if len(data) >= 16:
            return data
    except OSError:
        pass
    key = secrets.token_bytes(32)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(key)
        harden_file(path)
    except OSError:
        pass  # ephemeral key: sessions won't survive a restart, but the app runs
    return key


def custom_commands_allowed() -> bool:
    """Whether the free-form ``custom`` run script is permitted."""
    return (os.environ.get("OASIS_ALLOW_CUSTOM_CMD") or "").strip() == "1"


def check_bind(host: str, credentials: tuple[str, str] | None) -> None:
    """Fail closed: refuse a non-loopback bind unless credentials are configured."""
    if not is_loopback(host) and not credentials:
        raise RuntimeError(
            f"Refusing to bind the GUI to {host!r} without authentication. "
            "Set OASIS_GUI_USER and OASIS_GUI_PASSWORD, or bind to 127.0.0.1."
        )


def debugger_allowed(host: str) -> bool:
    """The Werkzeug debugger is an RCE vector; only allow it on a loopback bind."""
    return is_loopback(host)


SECRET_BACKUPS_KEEP = 10


def harden_file(path: Path | str) -> None:
    """Restrict a secrets file to owner read/write (0600) where supported.

    A no-op on filesystems that don't honour POSIX bits (e.g. Windows), where
    ``chmod`` silently does nothing useful; failures are swallowed so a write is
    never lost over a permission tweak.
    """
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass


def prune_backups(directory: Path | str, pattern: str, keep: int = SECRET_BACKUPS_KEEP) -> None:
    """Delete all but the newest ``keep`` files matching ``pattern`` in ``directory``.

    Timestamped backup names (``*.YYYYMMDD-HHMMSS.bak``) sort chronologically by
    name, so the tail of the sorted list is the newest ``keep``.
    """
    directory = Path(directory)
    files = sorted(directory.glob(pattern), key=lambda p: p.name)
    stale = files if keep <= 0 else files[:-keep]
    for p in stale:
        try:
            p.unlink()
        except OSError:
            pass


def request_authorized(remote_addr: str | None, logged_in: bool) -> bool:
    """Authorize a request.

    Loopback clients are trusted (single-user local tool). Any other client
    must have signed in through the login page (session flag). When no
    credentials are configured the deployment is loopback-only (guaranteed by
    ``check_bind``), so only local clients reach here.
    """
    if is_loopback(remote_addr):
        return True
    return logged_in
