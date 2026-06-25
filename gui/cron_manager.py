"""Manage scheduled pipeline runs as Ubuntu cron jobs.

Job definitions live in ``gui/state/schedules.json`` (the source of truth, so the
UI works on any OS). On a host with ``crontab`` available, the enabled jobs are
rendered into a delimited *managed block* and installed into the user's crontab,
leaving any hand-written cron lines outside the block untouched.

Each cron line ``cd <repo> && <venv-python> <script> ... >> <log> 2>&1`` runs the
same command the *Run* page would, with output appended to ``run_logs/``.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import uuid
from datetime import datetime
from typing import Any

import commands
from config import LOG_DIR, REPO_ROOT, SCHEDULES_JSON, STATE_DIR

BEGIN = "# >>> OASIS-GUI managed block (do not edit by hand) >>>"
END = "# <<< OASIS-GUI managed block <<<"
_TAG_RE = re.compile(r"#\s*OASIS-GUI\s+id=(\S+)")

PRESETS = [
    {"label": "Every 15 minutes", "expr": "*/15 * * * *"},
    {"label": "Hourly (top of hour)", "expr": "0 * * * *"},
    {"label": "Every 6 hours", "expr": "0 */6 * * *"},
    {"label": "Daily 02:00", "expr": "0 2 * * *"},
    {"label": "Weekdays 07:00", "expr": "0 7 * * 1-5"},
    {"label": "Weekly Sun 03:00", "expr": "0 3 * * 0"},
]


def _slug(text: str) -> str:
    s = re.sub(r"[^0-9A-Za-z._-]+", "_", text or "").strip("_")
    return s or "job"


def crontab_available() -> bool:
    return os.name != "nt" and shutil.which("crontab") is not None


# --------------------------------------------------------------------------- #
# Job store (JSON, source of truth)
# --------------------------------------------------------------------------- #
def load_jobs() -> list[dict[str, Any]]:
    if not SCHEDULES_JSON.exists():
        return []
    try:
        return json.loads(SCHEDULES_JSON.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save_jobs(jobs: list[dict[str, Any]]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = SCHEDULES_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(jobs, indent=2), encoding="utf-8")
    tmp.replace(SCHEDULES_JSON)


_CRON_FIELD = re.compile(r"^[\d*/,\-]+$")


def validate_expr(expr: str) -> None:
    """Lightweight 5-field cron expression check (also accepts @-shortcuts)."""
    expr = (expr or "").strip()
    if expr.startswith("@"):
        if expr not in ("@reboot", "@hourly", "@daily", "@weekly", "@monthly", "@yearly", "@annually"):
            raise ValueError(f"Unknown cron shortcut: {expr}")
        return
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError("Cron expression must have 5 fields (min hour dom mon dow)")
    for p in parts:
        if not _CRON_FIELD.match(p):
            raise ValueError(f"Invalid cron field: {p!r}")


def _render_job(job: dict[str, Any]) -> dict[str, Any]:
    """Attach the derived argv / command line / cron line to a job dict."""
    argv, label = commands.build_argv(job.get("spec", {}))
    log_path = LOG_DIR / f"cron-{_slug(job.get('name', job['id']))}.log"
    inner = (
        f"cd {shlex.quote(str(REPO_ROOT))} && "
        + " ".join(shlex.quote(a) for a in argv)
        + f" >> {shlex.quote(str(log_path))} 2>&1"
    )
    # cron treats % specially in the command -- escape any literal %.
    inner = inner.replace("%", r"\%")
    cron_line = f"{job['expr']} {inner}  # OASIS-GUI id={job['id']} name={_slug(job.get('name', ''))}"
    out = dict(job)
    out["label"] = label
    out["command"] = " ".join(shlex.quote(a) for a in argv)
    out["log"] = log_path.name
    out["cron_line"] = cron_line
    return out


def list_jobs() -> list[dict[str, Any]]:
    return [_render_job(j) for j in load_jobs()]


def add_job(name: str, expr: str, spec: dict[str, Any]) -> dict[str, Any]:
    validate_expr(expr)
    commands.build_argv(spec)  # validate the spec builds
    jobs = load_jobs()
    job = {
        "id": uuid.uuid4().hex[:8],
        "name": name.strip() or "job",
        "expr": expr.strip(),
        "spec": spec,
        "enabled": True,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    jobs.append(job)
    _save_jobs(jobs)
    return _render_job(job)


def update_job(job_id: str, **fields: Any) -> dict[str, Any]:
    jobs = load_jobs()
    for job in jobs:
        if job["id"] == job_id:
            if "expr" in fields and fields["expr"]:
                validate_expr(fields["expr"])
            if "spec" in fields and fields["spec"] is not None:
                commands.build_argv(fields["spec"])
            for k in ("name", "expr", "spec", "enabled"):
                if k in fields and fields[k] is not None:
                    job[k] = fields[k]
            _save_jobs(jobs)
            return _render_job(job)
    raise KeyError(job_id)


def delete_job(job_id: str) -> bool:
    jobs = load_jobs()
    new = [j for j in jobs if j["id"] != job_id]
    if len(new) == len(jobs):
        return False
    _save_jobs(new)
    return True


# --------------------------------------------------------------------------- #
# Crontab rendering / installation
# --------------------------------------------------------------------------- #
def render_block(jobs: list[dict[str, Any]] | None = None) -> str:
    jobs = jobs if jobs is not None else list_jobs()
    lines = [BEGIN, f"# generated {datetime.now().isoformat(timespec='seconds')} -- edit jobs in the OASIS panel"]
    for job in jobs:
        if not job.get("enabled", True):
            lines.append(f"# (disabled) {job.get('name')}: {job['cron_line']}")
            continue
        lines.append(job["cron_line"])
    lines.append(END)
    return "\n".join(lines) + "\n"


def _current_crontab() -> str:
    try:
        res = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return ""
    # No crontab yet -> non-zero exit, empty content.
    return res.stdout if res.returncode == 0 else ""


def _strip_block(text: str) -> str:
    out, skip = [], False
    for line in text.splitlines():
        if line.strip() == BEGIN:
            skip = True
            continue
        if line.strip() == END:
            skip = False
            continue
        if not skip:
            out.append(line)
    return "\n".join(out).strip("\n")


def install() -> dict[str, Any]:
    """Write the managed block into the real crontab (Linux/macOS only)."""
    if not crontab_available():
        return {
            "installed": False,
            "reason": "crontab not available on this host (cron is Ubuntu/Linux only)",
            "block": render_block(),
        }
    existing = _strip_block(_current_crontab())
    payload = (existing + "\n\n" if existing else "") + render_block()
    try:
        proc = subprocess.run(["crontab", "-"], input=payload, text=True,
                              capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as exc:
        return {"installed": False, "reason": str(exc), "block": render_block()}
    if proc.returncode != 0:
        return {"installed": False, "reason": proc.stderr.strip() or "crontab failed",
                "block": render_block()}
    return {"installed": True, "count": sum(1 for j in list_jobs() if j.get("enabled", True))}


def uninstall() -> dict[str, Any]:
    """Remove only the managed block from the crontab."""
    if not crontab_available():
        return {"installed": False, "reason": "crontab not available on this host"}
    stripped = _strip_block(_current_crontab())
    try:
        subprocess.run(["crontab", "-"], input=stripped + "\n", text=True,
                      capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as exc:
        return {"installed": False, "reason": str(exc)}
    return {"installed": True, "removed": True}


def status() -> dict[str, Any]:
    """Whether cron is usable here and which managed lines are live."""
    available = crontab_available()
    installed_ids: list[str] = []
    if available:
        for line in _current_crontab().splitlines():
            m = _TAG_RE.search(line)
            if m and not line.strip().startswith("#"):
                installed_ids.append(m.group(1))
    return {
        "available": available,
        "platform": os.name,
        "installed_ids": installed_ids,
        "block": render_block(),
    }
