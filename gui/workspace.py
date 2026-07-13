"""Read-only views over the pipeline workspace.

Branch list (from ``.dlt/secrets.toml`` -- *without* passwords), the CDC
watermark store (``control_state.json``), the ``[etl]`` tuning block, and the
log files produced by launched runs. Nothing here mutates project state.
"""

from __future__ import annotations

import json
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

import security
from config import (
    CONFIG_TOML,
    CONTROL_STATE,
    LOG_DIR,
    SECRETS_TOML,
    STATE_DIR,
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
    """Every ``[oracle_branches.*]`` section as a safe dict (password redacted).

    Delegates to ``connections`` (the owner of branch config) so the redacted
    shape and ordering stay defined in one place.
    """
    import connections
    return connections.list_connections()


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


# Keys the dashboard lets you edit (a write allowlist; ``_bucket_url`` and any
# other derived/destination keys are intentionally excluded).
EDITABLE_ETL_KEYS = {
    "dataset_name", "pipeline_name", "max_branch_workers", "max_table_workers",
    "pool_min", "pool_max", "pool_increment", "pool_acquire_timeout_s",
    "pool_acquire_attempts", "max_retries", "retry_interval_s",
    "snapshot_expire_days", "snapshot_min_to_keep", "dsn_mode",
    "dq_hash_delta_tolerance_pct",
}
_ETL_KV_RE = re.compile(r"^(\s*)([A-Za-z0-9_]+)(\s*=\s*)(.*?)(\s*(?:#.*)?)$")


def _fmt_toml_scalar(existing_raw: str, value: Any) -> str:
    """Render ``value`` matching the quoting/type of the existing raw value."""
    existing_raw = existing_raw.strip()
    quoted = len(existing_raw) >= 2 and existing_raw[0] in "\"'"
    if quoted or isinstance(value, str) and not _looks_numeric(str(value)):
        s = str(value).replace("\\", "\\\\").replace('"', '\\"')
        return f'"{s}"'
    sv = str(value).strip().lower()
    if sv in ("true", "false"):
        return sv
    return str(value).strip()


def _looks_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def _update_toml_block(section: str, allowlist: set[str], updates: dict[str, Any]) -> dict[str, Any]:
    """Edit scalar keys inside ``[section]`` of config.toml in place.

    Only keys already present in the block and on ``allowlist`` are touched;
    every other line is preserved verbatim. Keeps a timestamped backup and
    validates by re-parsing.
    """
    bad = [k for k in updates if k not in allowlist]
    if bad:
        raise ValueError(f"Not editable: {', '.join(sorted(bad))}")
    if not CONFIG_TOML.exists():
        raise FileNotFoundError("config.toml")

    lines = CONFIG_TOML.read_text(encoding="utf-8").splitlines()
    in_block = False
    applied: dict[str, Any] = {}
    for i, line in enumerate(lines):
        header = re.match(r"^\s*\[([^\]]+)\]\s*$", line)
        if header:
            in_block = header.group(1).strip() == section
            continue
        if not in_block:
            continue
        m = _ETL_KV_RE.match(line)
        if not m:
            continue
        key = m.group(2)
        if key in updates:
            new_raw = _fmt_toml_scalar(m.group(4), updates[key])
            lines[i] = f"{m.group(1)}{key}{m.group(3)}{new_raw}{m.group(5)}"
            applied[key] = updates[key]

    missing = [k for k in updates if k not in applied]
    if missing:
        raise ValueError(f"Key(s) not found in [{section}]: {', '.join(missing)}")

    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = STATE_DIR / f"config.toml.{stamp}.bak"
    shutil.copy2(CONFIG_TOML, backup)
    tmp = CONFIG_TOML.with_suffix(".toml.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        with tmp.open("rb") as fh:
            _toml.load(fh)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ValueError(f"refused to write corrupt config.toml: {exc}") from exc
    tmp.replace(CONFIG_TOML)
    security.prune_backups(STATE_DIR, "config.toml.*.bak")
    return {"applied": applied, "backup": str(backup)}


def update_etl_settings(updates: dict[str, Any]) -> dict[str, Any]:
    """Edit scalar keys inside the ``[etl]`` block of ``config.toml`` in place.

    Only keys already present in the block and on the allowlist are touched;
    every other line (comments, other sections) is preserved verbatim. Keeps a
    timestamped backup and validates the result by re-parsing.
    """
    return _update_toml_block("etl", EDITABLE_ETL_KEYS, updates)


# --------------------------------------------------------------------------- #
# dbt-core materialization settings
# --------------------------------------------------------------------------- #
EDITABLE_DBT_KEYS = {
    "project_dir", "target", "threads", "default_materialization", "dbt_executable",
}


def dbt_settings() -> dict[str, Any]:
    """The ``[dbt]`` block of config.toml (defaults applied by callers)."""
    return dict(_read_toml(CONFIG_TOML).get("dbt", {}))


def update_dbt_settings(updates: dict[str, Any]) -> dict[str, Any]:
    return _update_toml_block("dbt", EDITABLE_DBT_KEYS, updates)


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


def read_log_file(name: str, max_bytes: int = 5_242_880) -> str:
    """Return the tail of a log file (path-traversal safe)."""
    safe = Path(name).name
    p = LOG_DIR / safe
    if not p.exists() or p.parent.resolve() != LOG_DIR.resolve():
        raise FileNotFoundError(name)
    data = p.read_bytes()
    if len(data) > max_bytes:
        data = b"...[truncated]...\n" + data[-max_bytes:]
    return data.decode("utf-8", errors="replace")


def tail_log_file(name: str, offset: int = 0, max_bytes: int = 5_242_880) -> dict[str, Any]:
    """Return log bytes after ``offset`` plus the new end offset (path-safe).

    Mirrors ``RunManager.tail`` for arbitrary run_logs files so the Monitor page
    can poll incrementally instead of re-downloading the whole file each tick.
    From ``offset <= 0`` on a file larger than ``max_bytes``, only the trailing
    ``max_bytes`` are returned (prefixed with a truncation marker). An ``offset``
    past the current size means the file was rotated/replaced, so it restarts.
    """
    safe = Path(name).name
    p = LOG_DIR / safe
    if not p.exists() or p.parent.resolve() != LOG_DIR.resolve():
        raise FileNotFoundError(name)
    size = p.stat().st_size
    if offset > size:  # rotated/replaced under us -> re-read from the tail
        offset = 0
    truncated = False
    start = offset
    if offset <= 0 and size > max_bytes:
        start = size - max_bytes
        truncated = True
    chunk = ""
    if start < size:
        with p.open("rb") as fh:
            fh.seek(max(start, 0))
            chunk = fh.read().decode("utf-8", errors="replace")
        if truncated:
            chunk = "...[truncated]...\n" + chunk
    return {"name": safe, "offset": size, "chunk": chunk, "truncated": truncated}


def purge_logs(before: str | None = None, days: int | None = None) -> dict[str, Any]:
    """Delete ``run_logs/*.log`` files last modified before a cutoff.

    Pass either ``before`` (an ISO ``YYYY-MM-DD`` date — files modified strictly
    before midnight of that day are removed) or ``days`` (older than N days).
    Returns the deleted file names and reclaimed byte total.
    """
    if before:
        try:
            cutoff = datetime.combine(date.fromisoformat(before[:10]), datetime.min.time()).timestamp()
        except ValueError:
            raise ValueError(f"Invalid date: {before!r} (expected YYYY-MM-DD)") from None
    elif days is not None:
        cutoff = datetime.now().timestamp() - int(days) * 86400
    else:
        raise ValueError("Provide 'before' (YYYY-MM-DD) or 'days'")

    deleted: list[str] = []
    freed = 0
    if LOG_DIR.exists():
        for p in LOG_DIR.glob("*.log"):
            try:
                st = p.stat()
                if st.st_mtime < cutoff:
                    freed += st.st_size
                    p.unlink()
                    deleted.append(p.name)
            except OSError:
                continue
    return {"deleted": deleted, "count": len(deleted), "freed_bytes": freed}
