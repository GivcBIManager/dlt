"""Shared paths and constants for the OASIS control-panel GUI.

Everything in the GUI resolves locations relative to the *repo root* (the
directory that holds ``oracle_to_iceberg.py`` / ``tables.json`` / ``.dlt``), so
the panel works unchanged whether it is launched from Windows or Ubuntu and from
any current working directory.
"""

from __future__ import annotations

import os
from pathlib import Path

# gui/config.py -> gui/ -> <repo root>
REPO_ROOT = Path(__file__).resolve().parent.parent

# --- project artefacts the GUI reads/writes ------------------------------- #
TABLES_JSON = REPO_ROOT / "tables.json"
CONTROL_STATE = REPO_ROOT / "control_state.json"
SECRETS_TOML = REPO_ROOT / ".dlt" / "secrets.toml"
CONFIG_TOML = REPO_ROOT / ".dlt" / "config.toml"

# Iceberg lake: <bucket_url>/<dataset>. Default bucket is ``iceberg_output`` and
# the dataset is ``oasis`` ([etl].dataset_name in config.toml).
ICEBERG_BUCKET = REPO_ROOT / "iceberg_output"
ICEBERG_DATASET = "oasis"
ICEBERG_ROOT = ICEBERG_BUCKET / ICEBERG_DATASET

# Entry-point scripts the panel can launch.
SCRIPTS = {
    "oracle_to_iceberg": REPO_ROOT / "oracle_to_iceberg.py",
    "dq_check": REPO_ROOT / "dq_check.py",
    "snapshot_diff": REPO_ROOT / "snapshot_diff.py",
}

# --- GUI's own state (logs, run registry, schedules) ---------------------- #
GUI_DIR = REPO_ROOT / "gui"
STATE_DIR = GUI_DIR / "state"
LOG_DIR = REPO_ROOT / "run_logs"          # one file per launched run
RUNS_REGISTRY = STATE_DIR / "runs.json"
SCHEDULES_JSON = STATE_DIR / "schedules.json"

# Iceberg system tables (rendered specially in the monitor).
ETL_CONTROL_TABLE = "etl_control"
ETL_RUN_LOG_TABLE = "etl_run_log"
ETL_DQ_TABLE = "etl_dq_results"
SYSTEM_TABLES = {ETL_CONTROL_TABLE, ETL_RUN_LOG_TABLE, ETL_DQ_TABLE}


def ensure_dirs() -> None:
    """Create the GUI's writable directories (idempotent)."""
    for d in (STATE_DIR, LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


def python_executable() -> str:
    """The interpreter used to launch pipeline runs.

    Prefers the venv this GUI runs under (``sys.executable``); honours an
    explicit ``OASIS_PYTHON`` override so a scheduler can pin a specific
    interpreter.
    """
    import sys

    return os.environ.get("OASIS_PYTHON") or sys.executable
