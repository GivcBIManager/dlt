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
PIPELINES_JSON = STATE_DIR / "pipelines.json"
FLOWS_JSON = STATE_DIR / "flows.json"

# --- Dagster orchestration -------------------------------------------------- #
ORCHESTRATOR_DIR = REPO_ROOT / "orchestrator"
DAGSTER_HOME = REPO_ROOT / ".dagster_home"

# --- dbt materialization layer --------------------------------------------- #
# The dbt project lives at <repo root>/dbt. profiles.yml is GENERATED from app
# config ([clickhouse] in secrets.toml + [dbt] in config.toml); never hand-edited.
DBT_DIR = REPO_ROOT / "dbt"
DBT_PROFILES = DBT_DIR / "profiles.yml"


def dbt_executable() -> str:
    """Absolute path to the ``dbt`` entry point (or a bare name for PATH lookup).

    Resolution order: an explicit ``OASIS_DBT`` override; then the ``dbt``
    launcher installed next to the running interpreter (the venv's Scripts/bin
    dir, where ``pip install dbt-clickhouse`` puts it); else the bare name
    ``dbt`` to resolve on PATH. Resolving next to ``sys.executable`` means the
    GUI finds dbt even when launched via the venv python *without* the venv
    being activated (so its Scripts dir is not on PATH) -- mirroring
    ``python_executable()``.
    """
    import sys

    override = os.environ.get("OASIS_DBT")
    if override:
        return override
    launcher = "dbt.exe" if os.name == "nt" else "dbt"
    candidate = Path(sys.executable).parent / launcher
    return str(candidate) if candidate.is_file() else "dbt"


def dagster_host() -> str:
    return os.environ.get("OASIS_DAGSTER_HOST", "127.0.0.1")


def dagster_port() -> int:
    return int(os.environ.get("OASIS_DAGSTER_PORT", "3000"))


def dagster_base_url() -> str:
    return f"http://{dagster_host()}:{dagster_port()}"


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
