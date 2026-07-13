"""GUI security gate: bind safety, request auth, and command lockdown.

The control panel can launch processes and edit pipeline config, so exposing it
on a non-loopback interface without authentication is a remote-code-execution
risk. This module centralises the guard rails:

* ``check_bind`` fails closed when bound to a public interface with no token.
* ``request_authorized`` allows local (loopback) use with no token but requires
  a shared token for any non-loopback client.
* ``custom_commands_allowed`` keeps the free-form ``custom`` run script off by
  default (it runs arbitrary argv).

Configuration (all optional):
* ``OASIS_GUI_TOKEN``        shared token required for non-loopback clients.
* ``OASIS_ALLOW_CUSTOM_CMD`` set to ``1`` to permit the ``custom`` run script.
"""
from __future__ import annotations

import hmac
import ipaddress
import os
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


def gui_token() -> str | None:
    """The configured shared token, or ``None`` if unset/blank."""
    tok = (os.environ.get("OASIS_GUI_TOKEN") or "").strip()
    return tok or None


def custom_commands_allowed() -> bool:
    """Whether the free-form ``custom`` run script is permitted."""
    return (os.environ.get("OASIS_ALLOW_CUSTOM_CMD") or "").strip() == "1"


def check_bind(host: str, token: str | None) -> None:
    """Fail closed: refuse a non-loopback bind unless a token is configured."""
    if not is_loopback(host) and not token:
        raise RuntimeError(
            f"Refusing to bind the GUI to {host!r} without authentication. "
            "Set OASIS_GUI_TOKEN to a shared secret, or bind to 127.0.0.1."
        )


def debugger_allowed(host: str) -> bool:
    """The Werkzeug debugger is an RCE vector; only allow it on a loopback bind."""
    return is_loopback(host)


def token_matches(provided: str | None, token: str | None) -> bool:
    """Constant-time token comparison; False if either side is missing."""
    if not provided or not token:
        return False
    return hmac.compare_digest(str(provided), str(token))


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


def request_authorized(remote_addr: str | None, provided_token: str | None,
                       token: str | None) -> bool:
    """Authorize a request.

    Loopback clients are trusted (single-user local tool). Any other client must
    present a token matching the configured one. When no token is configured the
    deployment is loopback-only (guaranteed by ``check_bind``), so only local
    clients reach here.
    """
    if is_loopback(remote_addr):
        return True
    return token_matches(provided_token, token)
