"""Read-only views over the pipeline workspace.

Branch list (from ``.dlt/secrets.toml`` -- *without* passwords), the CDC
watermark store (``control_state.json``), the ``[etl]`` tuning block, and the
log files produced by launched runs. Nothing here mutates project state.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from config import (
    CONFIG_TOML,
    CONTROL_STATE,
    LOG_DIR,
    SECRETS_TOML,
)

# tomllib is stdlib on 3.11+; fall back to the tomli backport on 3.10.
try:  # pragma: no cover - import shim
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore


def _read_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as fh:
        return _toml.load(fh)


# --------------------------------------------------------------------------- #
# Branches
# --------------------------------------------------------------------------- #
def list_branches() -> list[dict[str, Any]]:
    """Every ``[oracle_branches.*]`` section as a safe dict (password redacted)."""
    raw = _read_toml(SECRETS_TOML).get("oracle_branches", {})
    branches = []
    for key, sec in raw.items():
        branches.append(
            {
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
        )
    branches.sort(key=lambda b: (b["id"] is None, b["id"]))
    return branches


def branch_keys() -> list[str]:
    return [b["key"] for b in list_branches()]


def etl_settings() -> dict[str, Any]:
    """The ``[etl]`` block plus the destination bucket, for the dashboard."""
    cfg = _read_toml(CONFIG_TOML)
    etl = dict(cfg.get("etl", {}))
    bucket = (
        cfg.get("destination", {})
        .get("filesystem", {})
        .get("bucket_url")
    )
    etl["_bucket_url"] = bucket
    return etl


# --------------------------------------------------------------------------- #
# Control state (CDC watermarks)
# --------------------------------------------------------------------------- #
def load_control_state() -> dict[str, Any]:
    if not CONTROL_STATE.exists():
        return {}
    try:
        return json.loads(CONTROL_STATE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def control_rows() -> list[dict[str, Any]]:
    """Flatten control_state.json into one row per (table, branch)."""
    state = load_control_state()
    rows: list[dict[str, Any]] = []
    for table, branches in state.items():
        for branch, info in branches.items():
            cdc = (info or {}).get("last_cdc") or {}
            date = (info or {}).get("last_date") or {}
            rows.append(
                {
                    "table": table,
                    "branch": branch,
                    "status": info.get("status"),
                    "row_count": info.get("row_count"),
                    "duration_ms": info.get("duration_ms"),
                    "last_run_at": info.get("last_run_at"),
                    "last_cdc": cdc.get("value"),
                    "last_date": date.get("value"),
                }
            )
    rows.sort(key=lambda r: (r["table"], str(r["branch"])))
    return rows


def control_summary() -> dict[str, Any]:
    """Aggregate health numbers for the dashboard cards."""
    rows = control_rows()
    statuses: dict[str, int] = {}
    total_rows = 0
    last_run: str | None = None
    for r in rows:
        statuses[r["status"] or "UNKNOWN"] = statuses.get(r["status"] or "UNKNOWN", 0) + 1
        total_rows += int(r["row_count"] or 0)
        if r["last_run_at"] and (last_run is None or r["last_run_at"] > last_run):
            last_run = r["last_run_at"]
    return {
        "units": len(rows),
        "tables": len({r["table"] for r in rows}),
        "branches": len({r["branch"] for r in rows}),
        "total_rows": total_rows,
        "statuses": statuses,
        "last_run_at": last_run,
    }


# --------------------------------------------------------------------------- #
# Log files
# --------------------------------------------------------------------------- #
def list_log_files() -> list[dict[str, Any]]:
    """Run/cron log files under run_logs/, newest first."""
    if not LOG_DIR.exists():
        return []
    out = []
    for p in LOG_DIR.glob("*.log"):
        try:
            st = p.stat()
        except OSError:
            continue
        out.append(
            {
                "name": p.name,
                "size": st.st_size,
                "modified": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
                "mtime": st.st_mtime,
            }
        )
    out.sort(key=lambda f: f["mtime"], reverse=True)
    return out


def read_log_file(name: str, max_bytes: int = 400_000) -> str:
    """Return the tail of a log file (path-traversal safe)."""
    safe = Path(name).name
    p = LOG_DIR / safe
    if not p.exists() or p.parent.resolve() != LOG_DIR.resolve():
        raise FileNotFoundError(name)
    data = p.read_bytes()
    if len(data) > max_bytes:
        data = b"...[truncated]...\n" + data[-max_bytes:]
    return data.decode("utf-8", errors="replace")
