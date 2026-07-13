"""Read/write the global ``[smtp]`` block in ``.dlt/secrets.toml``.

Same surgical, comment-preserving strategy as connections.py: splice only the
``[smtp]`` section, back up + re-parse-validate before committing. The password
is only overwritten when a new one is supplied.
"""
from __future__ import annotations

import re
import shutil
import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Any

import config
import security

try:
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore

_SECTION_RE = re.compile(r"^\s*\[([^\]]+)\]\s*$")
_FIELDS = ["host", "port", "username", "password", "from", "use_tls"]
_NUM = {"port"}
_BOOL = {"use_tls"}


def _read() -> dict[str, Any]:
    if not config.SECRETS_TOML.exists():
        return {}
    with config.SECRETS_TOML.open("rb") as fh:
        return dict(_toml.load(fh).get("smtp", {}))


def get_smtp() -> dict[str, Any]:
    s = _read()
    return {
        "host": s.get("host", ""), "port": s.get("port", 587),
        "username": s.get("username", ""), "from": s.get("from", ""),
        "use_tls": bool(s.get("use_tls", True)),
        "has_password": bool(s.get("password")),
    }


def _fmt(field: str, val: Any) -> str:
    if field in _NUM:
        return str(int(val))
    if field in _BOOL:
        return "true" if val else "false"
    s = str(val).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _emit(data: dict[str, Any]) -> list[str]:
    lines = ["[smtp]"]
    for f in _FIELDS:
        if data.get(f) not in (None, ""):
            lines.append(f"{f} = {_fmt(f, data[f])}")
    return lines


def _read_lines() -> list[str]:
    if not config.SECRETS_TOML.exists():
        return []
    return config.SECRETS_TOML.read_text(encoding="utf-8").splitlines()


def _section_span(lines: list[str], name: str) -> tuple[int, int] | None:
    headers = [(i, m.group(1).strip())
               for i, line in enumerate(lines) if (m := _SECTION_RE.match(line))]
    for idx, (i, nm) in enumerate(headers):
        if nm == name:
            end = headers[idx + 1][0] if idx + 1 < len(headers) else len(lines)
            while end - 1 > i and lines[end - 1].strip() == "":
                end -= 1
            return i, end
    return None


def _write(lines: list[str]) -> None:
    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"
    if config.SECRETS_TOML.exists():
        config.STATE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = config.STATE_DIR / f"secrets.toml.{stamp}.bak"
        shutil.copy2(config.SECRETS_TOML, backup)
        security.harden_file(backup)
    tmp = config.SECRETS_TOML.with_suffix(".toml.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        with tmp.open("rb") as fh:
            _toml.load(fh)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ValueError(f"refused to write corrupt secrets.toml: {exc}") from exc
    tmp.replace(config.SECRETS_TOML)
    security.harden_file(config.SECRETS_TOML)
    security.prune_backups(config.STATE_DIR, "secrets.toml.*.bak")


def save_smtp(payload: dict[str, Any]) -> dict[str, Any]:
    if not str(payload.get("host") or "").strip():
        raise ValueError("'host' is required")
    if not str(payload.get("from") or "").strip():
        raise ValueError("'from' address is required")
    existing = _read()
    data = {
        "host": payload.get("host"), "port": payload.get("port") or 587,
        "username": payload.get("username"), "from": payload.get("from"),
        "use_tls": bool(payload.get("use_tls", True)),
    }
    pw = str(payload.get("password") or "").strip()
    data["password"] = pw if pw else existing.get("password")

    lines = _read_lines()
    span = _section_span(lines, "smtp")
    block = _emit(data)
    if span is None:
        new = (lines + ["", *block]) if lines else block
    else:
        i, end = span
        new = lines[:i] + block + lines[end:]
    _write(new)
    return get_smtp()


def send_test(to: str) -> dict[str, Any]:
    s = _read()
    if not all(s.get(k) for k in ("host", "port", "from")):
        return {"ok": False, "error": "SMTP not fully configured (host, port, from)"}
    to = (to or s.get("from")).strip()
    try:
        msg = EmailMessage()
        msg["From"] = s["from"]
        msg["To"] = to
        msg["Subject"] = "[OASIS] SMTP test"
        msg.set_content("This is a test email from the HNH ETLPipeline Manager.")
        with smtplib.SMTP(s["host"], int(s["port"]), timeout=20) as srv:
            if s.get("use_tls", True):
                srv.starttls()
            if s.get("username"):
                srv.login(s["username"], s.get("password") or "")
            srv.send_message(msg)
        return {"ok": True, "message": f"Test email sent to {to}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
