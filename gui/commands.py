"""Translate a UI command spec into an argv list.

Shared by the *Run* page (execute now) and the *Schedule* page (render the cron
command), so a scheduled job runs exactly what the run form would.
"""

from __future__ import annotations

import os
import shlex
from typing import Any

from config import REPO_ROOT, SCRIPTS, python_executable

SCRIPT_CHOICES = ["oracle_to_iceberg", "dq_check", "snapshot_diff", "fresh_run", "custom"]


def _split(value: str | None) -> list[str]:
    if not value:
        return []
    # POSIX tokenizing on every OS: it strips quotes correctly so a quoted
    # ``python -c "..."`` survives intact. On Windows, single-quote any literal
    # backslash path (e.g. 'C:\dir\x') to keep the backslashes.
    return shlex.split(value, posix=True)


def _csv(value: Any) -> str:
    """Join a list or pass through a comma/space string."""
    if isinstance(value, (list, tuple)):
        return ",".join(str(v).strip() for v in value if str(v).strip())
    return str(value or "").strip()


def build_argv(spec: dict[str, Any]) -> tuple[list[str], str]:
    """Return ``(argv, label)`` for a command spec.

    ``spec['script']`` selects the entry point; the remaining keys are the flags
    for that script. A ``custom`` script runs ``spec['custom']`` verbatim.
    """
    script = (spec.get("script") or "oracle_to_iceberg").strip()
    py = python_executable()

    if script == "custom":
        argv = _split(spec.get("custom"))
        if not argv:
            raise ValueError("Custom command is empty")
        return argv, "custom: " + " ".join(argv[:4])

    if script == "fresh_run":
        if os.name == "nt":
            return ["cmd", "/c", str(REPO_ROOT / "fresh_run.cmd")], "fresh_run"
        return ["bash", str(REPO_ROOT / "fresh_run.sh")], "fresh_run"

    if script not in SCRIPTS:
        raise ValueError(f"Unknown script: {script}")

    argv = [py, str(SCRIPTS[script])]
    label_bits = [script]

    if script == "oracle_to_iceberg":
        mode = (spec.get("mode") or "INCREMENTAL").upper()
        argv += ["--mode", mode]
        label_bits.append(mode)
        category = spec.get("category")
        if category and category != "both":
            argv += ["--category", category]
            label_bits.append(category)
        if spec.get("self_test"):
            argv.append("--self-test")
        if spec.get("no_progress"):
            argv.append("--no-progress")

    elif script == "dq_check":
        if spec.get("since"):
            argv += ["--since", str(spec["since"]).strip()]
        if spec.get("until"):
            argv += ["--until", str(spec["until"]).strip()]
        if spec.get("no_hash"):
            argv.append("--no-hash")
        if spec.get("no_write"):
            argv.append("--no-write")
        if spec.get("no_progress"):
            argv.append("--no-progress")
        if spec.get("csv"):
            argv += ["--csv", str(spec["csv"]).strip()]
        if spec.get("self_test"):
            argv.append("--self-test")

    elif script == "snapshot_diff":
        if spec.get("table"):
            argv += ["--table", str(spec["table"]).strip()]
            label_bits.append(str(spec["table"]).strip())
        if spec.get("unique_key"):
            argv += ["--unique-key", str(spec["unique_key"]).strip()]
        if spec.get("as_of"):
            argv += ["--as-of", str(spec["as_of"]).strip()]

    # Shared filters (oracle_to_iceberg + dq_check accept --branch / --tables).
    if script in ("oracle_to_iceberg", "dq_check"):
        branch = _csv(spec.get("branches") or spec.get("branch"))
        if branch:
            argv += ["--branch", branch]
            label_bits.append(branch)
        tables = _csv(spec.get("tables"))
        if tables:
            argv += ["--tables", tables]
            label_bits.append(tables)
        if spec.get("log_level"):
            argv += ["--log-level", str(spec["log_level"]).strip()]

    argv += _split(spec.get("extra"))
    return argv, " ".join(label_bits)


def preview(spec: dict[str, Any]) -> str:
    """A copy-pasteable command line for display."""
    argv, _ = build_argv(spec)
    return " ".join(shlex.quote(a) for a in argv)
