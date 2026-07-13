"""Create / edit the single ``[clickhouse]`` section in ``.dlt/secrets.toml``.

Mirrors ``connections.py``: surgical block-level text editing so every other
section, comment and blank line stays byte-for-byte intact. Reads parse with
tomllib; writes are validated by re-parsing and a timestamped backup is kept.
The password never leaves the server (``get_clickhouse`` redacts it).
"""
from __future__ import annotations

import re
import subprocess
from typing import Any

import toml_edit
from config import SECRETS_TOML, STATE_DIR

try:  # tomllib stdlib on 3.11+, tomli backport on 3.10
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore

_SECTION_RE = re.compile(r"^\s*\[([^\]]+)\]\s*$")

DEFAULTS: dict[str, Any] = {
    "host": "", "port": 8123, "user": "default", "database": "default",
    "secure": False, "connect_timeout": 10,
}
FIELD_ORDER = ["host", "port", "user", "password", "database", "secure", "connect_timeout"]
_NUM_FIELDS = {"port", "connect_timeout"}
_BOOL_FIELDS = {"secure"}


def _raw() -> dict[str, Any]:
    if not SECRETS_TOML.exists():
        return {}
    with SECRETS_TOML.open("rb") as fh:
        return dict(_toml.load(fh).get("clickhouse", {}))


def get_clickhouse() -> dict[str, Any]:
    sec = _raw()
    out = {k: sec.get(k, DEFAULTS[k]) for k in DEFAULTS}
    out["has_password"] = bool(sec.get("password"))
    return out


def _fmt(field: str, val: Any) -> str | None:
    if val is None or val == "":
        return None
    if field in _NUM_FIELDS:
        return str(int(val))
    if field in _BOOL_FIELDS:
        return "true" if (val is True or str(val).strip().lower() == "true") else "false"
    s = str(val).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _emit_block(data: dict[str, Any]) -> list[str]:
    lines = ["[clickhouse]"]
    for field in FIELD_ORDER:
        if field in data:
            v = _fmt(field, data[field])
            if v is not None:
                lines.append(f"{field} = {v}")
    return lines


def _find_block(lines: list[str]) -> tuple[int, int] | None:
    """(header_index, end_index) of the ``[clickhouse]`` section, or None."""
    headers = [(i, m.group(1).strip())
               for i, line in enumerate(lines) if (m := _SECTION_RE.match(line))]
    for idx, (i, name) in enumerate(headers):
        if name == "clickhouse":
            end = headers[idx + 1][0] if idx + 1 < len(headers) else len(lines)
            return i, end
    return None


def _read_lines() -> list[str]:
    return toml_edit.read_lines(SECRETS_TOML)


def _write(lines: list[str]) -> None:
    toml_edit.write_lines(SECRETS_TOML, lines, backup_dir=STATE_DIR,
                          backup_prefix="secrets.toml", harden=True)


def save_clickhouse(payload: dict[str, Any]) -> dict[str, Any]:
    if not str(payload.get("host") or "").strip():
        raise ValueError("'host' is required")
    for f in _NUM_FIELDS:
        if payload.get(f) not in (None, ""):
            try:
                int(payload[f])
            except (TypeError, ValueError):
                raise ValueError(f"'{f}' must be a whole number") from None

    merged = dict(_raw())
    for f in ("host", "port", "user", "database", "secure", "connect_timeout"):
        if f in payload and payload[f] not in (None, ""):
            merged[f] = payload[f]
    # Password only overwritten when a fresh non-blank one is supplied.
    if str(payload.get("password") or "").strip():
        merged["password"] = payload["password"]

    lines = _read_lines()
    block = _find_block(lines)
    new_block = _emit_block(merged)
    if block is None:
        new = (lines + ["", *new_block]) if lines else new_block
    else:
        h, end = block
        # trim trailing blanks inside the old block
        while end - 1 > h and lines[end - 1].strip() == "":
            end -= 1
        new = lines[:h] + new_block + lines[end:]
    _write(new)
    return get_clickhouse()


def test_connection() -> dict[str, Any]:
    """Run ``dbt debug`` against the generated profile. Never raises."""
    import dbt_config
    try:
        dbt_config.write_profiles()
    except ValueError as exc:
        return {"ok": False, "output": str(exc)}
    try:
        proc = subprocess.run(
            [dbt_config.dbt_executable(), "debug",
             "--project-dir", str(dbt_config.dbt_dir()),
             "--profiles-dir", str(dbt_config.dbt_dir())],
            capture_output=True, text=True, timeout=60,
        )
        return {"ok": proc.returncode == 0, "output": (proc.stdout or "") + (proc.stderr or "")}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "output": f"{type(exc).__name__}: {exc}"}
