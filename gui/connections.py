"""Create / edit / delete Oracle branch connections in ``.dlt/secrets.toml``.

The pipeline reads ``[oracle_branches.<key>]`` sections from ``secrets.toml``.
This module edits *only* those sections, surgically: it splices the affected
branch block in place and leaves every other section (Spark, web app,
ClickHouse, …), comment and blank line byte-for-byte untouched. There is no
stdlib TOML *writer*, and rewriting the whole file would destroy the hand-kept
comments, so block-level text editing is the safe path.

Reads use ``tomllib`` (reliable parse); writes are validated by re-parsing the
result and a timestamped backup is kept (and restored on corruption).
"""

from __future__ import annotations

import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from config import SECRETS_TOML, STATE_DIR

try:  # tomllib is stdlib on 3.11+, tomli backport on 3.10
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore

_SECTION_RE = re.compile(r"^\s*\[([^\]]+)\]\s*$")
_KEY_RE = re.compile(r"^[A-Za-z0-9_]+$")

# Canonical field order when (re)emitting a branch block.
FIELD_ORDER = ["name", "host", "port", "username", "password", "database",
               "fetch_batch_size", "id"]
_NUM_FIELDS = {"port", "id", "fetch_batch_size"}
_REQUIRED = ["host", "port", "username", "database"]


# --------------------------------------------------------------------------- #
# Read
# --------------------------------------------------------------------------- #
def _all_branches() -> dict[str, dict[str, Any]]:
    if not SECRETS_TOML.exists():
        return {}
    with SECRETS_TOML.open("rb") as fh:
        return dict(_toml.load(fh).get("oracle_branches", {}))


def _safe(key: str, sec: dict[str, Any]) -> dict[str, Any]:
    """A branch dict with the password redacted (never leaves the server)."""
    return {
        "key": key,
        "name": sec.get("name", key),
        "id": sec.get("id"),
        "host": sec.get("host"),
        "port": sec.get("port"),
        "database": sec.get("database"),
        "username": sec.get("username"),
        "fetch_batch_size": sec.get("fetch_batch_size"),
        "has_password": bool(sec.get("password")),
    }


def list_connections() -> list[dict[str, Any]]:
    out = [_safe(k, v) for k, v in _all_branches().items()]
    out.sort(key=lambda b: (b["id"] is None, b["id"]))
    return out


def get_connection(key: str) -> dict[str, Any]:
    branches = _all_branches()
    if key not in branches:
        raise KeyError(key)
    return _safe(key, branches[key])


# --------------------------------------------------------------------------- #
# Block-level text editing
# --------------------------------------------------------------------------- #
def _fmt_value(field: str, val: Any) -> str | None:
    if val is None or val == "":
        return None
    if field in _NUM_FIELDS:
        return str(int(val))
    s = str(val).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _emit_block(key: str, data: dict[str, Any]) -> list[str]:
    lines = [f"[oracle_branches.{key}]"]
    done: set[str] = set()
    for field in FIELD_ORDER:
        if field in data:
            v = _fmt_value(field, data[field])
            if v is not None:
                lines.append(f"{field} = {v}")
                done.add(field)
    # Preserve any non-standard keys a branch may carry.
    for field, val in data.items():
        if field in done or field in FIELD_ORDER:
            continue
        v = _fmt_value(field, val)
        if v is not None:
            lines.append(f"{field} = {v}")
    return lines


def _branch_blocks(lines: list[str]) -> list[dict[str, Any]]:
    """Locate every ``[oracle_branches.<key>]`` block.

    Returns dicts with ``key``, ``header`` (header line index) and ``end``
    (index of the next section header, or EOF).
    """
    headers: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        m = _SECTION_RE.match(line)
        if m:
            headers.append((i, m.group(1).strip()))
    blocks: list[dict[str, Any]] = []
    for idx, (i, name) in enumerate(headers):
        if not name.startswith("oracle_branches."):
            continue
        end = headers[idx + 1][0] if idx + 1 < len(headers) else len(lines)
        blocks.append({"key": name.split(".", 1)[1], "header": i, "end": end})
    return blocks


def _content_end(lines: list[str], block: dict[str, Any]) -> int:
    """Block end with trailing blank lines trimmed off."""
    end = block["end"]
    while end - 1 > block["header"] and lines[end - 1].strip() == "":
        end -= 1
    return end


def _write(lines: list[str]) -> None:
    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"
    backup: Path | None = None
    if SECRETS_TOML.exists():
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = STATE_DIR / f"secrets.toml.{stamp}.bak"
        shutil.copy2(SECRETS_TOML, backup)
    tmp = SECRETS_TOML.with_suffix(".toml.tmp")
    tmp.write_text(text, encoding="utf-8")
    # Validate before committing; restore the backup if we produced bad TOML.
    try:
        with tmp.open("rb") as fh:
            _toml.load(fh)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ValueError(f"refused to write corrupt secrets.toml: {exc}") from exc
    tmp.replace(SECRETS_TOML)


def _read_lines() -> list[str]:
    if not SECRETS_TOML.exists():
        return []
    return SECRETS_TOML.read_text(encoding="utf-8").splitlines()


def _validate_payload(data: dict[str, Any], *, require: bool) -> None:
    for f in _REQUIRED:
        if require and not str(data.get(f) or "").strip():
            raise ValueError(f"'{f}' is required")
    for f in _NUM_FIELDS:
        if data.get(f) not in (None, ""):
            try:
                int(data[f])
            except (TypeError, ValueError):
                raise ValueError(f"'{f}' must be a whole number") from None


# --------------------------------------------------------------------------- #
# Mutations
# --------------------------------------------------------------------------- #
def add_connection(payload: dict[str, Any]) -> dict[str, Any]:
    key = str(payload.get("key") or "").strip()
    if not _KEY_RE.match(key):
        raise ValueError("Branch key must be letters, digits or underscore")
    if key in _all_branches():
        raise ValueError(f"Branch '{key}' already exists")
    _validate_payload(payload, require=True)
    if not str(payload.get("password") or "").strip():
        raise ValueError("'password' is required for a new branch")

    data = {f: payload.get(f) for f in FIELD_ORDER if payload.get(f) not in (None, "")}
    lines = _read_lines()
    block_lines = _emit_block(key, data)
    branches = _branch_blocks(lines)
    if branches:
        at = _content_end(lines, branches[-1])
        new = lines[:at] + ["", *block_lines] + lines[at:]
    elif lines:
        new = lines + ["", *block_lines]
    else:
        new = block_lines
    _write(new)
    return get_connection(key)


def update_connection(key: str, payload: dict[str, Any]) -> dict[str, Any]:
    branches = _all_branches()
    if key not in branches:
        raise KeyError(key)
    new_key = str(payload.get("key") or key).strip()
    if new_key != key:
        if not _KEY_RE.match(new_key):
            raise ValueError("Branch key must be letters, digits or underscore")
        if new_key in branches:
            raise ValueError(f"Branch '{new_key}' already exists")
    _validate_payload(payload, require=False)

    merged = dict(branches[key])
    for f in ("name", "host", "port", "username", "database", "fetch_batch_size", "id"):
        if f in payload and payload[f] not in (None, ""):
            merged[f] = payload[f]
    # Password is only overwritten when a fresh one is supplied (the UI never
    # round-trips the existing secret).
    if str(payload.get("password") or "").strip():
        merged["password"] = payload["password"]

    lines = _read_lines()
    block = next((b for b in _branch_blocks(lines) if b["key"] == key), None)
    if block is None:  # present in parse but not locatable as a block -> bail
        raise KeyError(key)
    end = _content_end(lines, block)
    new = lines[: block["header"]] + _emit_block(new_key, merged) + lines[end:]
    _write(new)
    return get_connection(new_key)


def delete_connection(key: str) -> bool:
    lines = _read_lines()
    block = next((b for b in _branch_blocks(lines) if b["key"] == key), None)
    if block is None:
        return False
    end = _content_end(lines, block)
    # Also swallow one trailing blank separator to avoid piling up blank lines.
    if end < len(lines) and lines[end].strip() == "":
        end += 1
    new = lines[: block["header"]] + lines[end:]
    _write(new)
    return True


# --------------------------------------------------------------------------- #
# Connectivity test (best effort)
# --------------------------------------------------------------------------- #
def test_connection(key: str) -> dict[str, Any]:
    """Try a quick login to the branch. Best-effort and never raises."""
    branches = _all_branches()
    if key not in branches:
        raise KeyError(key)
    sec = branches[key]
    try:
        import oracledb  # type: ignore
    except ImportError:
        return {"ok": False, "error": "python-oracledb is not installed on this host"}

    # Mirror the pipeline's thick-mode / dsn_mode settings from config.toml.
    dsn_mode, lib_dir, thick = _oracle_runtime_opts()
    if thick:
        try:
            oracledb.init_oracle_client(lib_dir=lib_dir or None)
        except Exception:  # noqa: BLE001 - already initialised / lib missing
            pass
    try:
        dsn = (oracledb.makedsn(sec["host"], int(sec["port"]), sid=sec["database"])
               if dsn_mode == "sid"
               else oracledb.makedsn(sec["host"], int(sec["port"]), service_name=sec["database"]))
        conn = oracledb.connect(user=sec.get("username"), password=sec.get("password"),
                                dsn=dsn, tcp_connect_timeout=6)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM DUAL")
            cur.fetchone()
        finally:
            conn.close()
        return {"ok": True, "message": f"Connected to {sec['host']}:{sec['port']} ({sec['database']})"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _oracle_runtime_opts() -> tuple[str, str | None, bool]:
    from config import CONFIG_TOML

    try:
        with CONFIG_TOML.open("rb") as fh:
            etl = _toml.load(fh).get("etl", {})
    except (OSError, Exception):  # noqa: BLE001
        etl = {}
    lib = etl.get("oracle_client_lib_dir")
    if lib and not Path(lib).is_dir():
        lib = None
    return str(etl.get("dsn_mode", "service_name")), lib, bool(etl.get("thick_mode", False))
