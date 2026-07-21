"""Regression: the GUI must import metastore_read with only gui/ on sys.path.

`python gui/app.py` puts ONLY the gui/ directory on sys.path (gui/app.py:23),
not the repo root. metastore_read imports the ``etl`` package (repo root), so it
must add the repo root to sys.path itself (mirroring gui/iceberg_maintenance.py)
or the running GUI 500s with ``ModuleNotFoundError: No module named 'etl'`` on
the Postgres-backed /api/iceberg/system/* endpoints.

The rest of the test suite can't catch this because conftest.py puts the repo
root on sys.path. This test isolates the GUI's real import environment via a
subprocess whose cwd is gui/ and whose env has no repo-root PYTHONPATH.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GUI_DIR = REPO_ROOT / "gui"


def _run_in_gui(code: str) -> subprocess.CompletedProcess:
    # Simulate `python gui/app.py`: cwd = gui/ (so gui/ is on sys.path via ''),
    # and strip PYTHONPATH so the repo root is NOT injected from the environment.
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(GUI_DIR), env=env, capture_output=True, text=True)


def test_metastore_read_imports_with_only_gui_on_path():
    proc = _run_in_gui("import metastore_read")
    assert proc.returncode == 0, (
        "metastore_read must be importable with only gui/ on sys.path "
        f"(as the running GUI has it):\n{proc.stderr}")


def test_iceberg_browser_reaches_metastore_read_import():
    # iceberg_browser lazily imports metastore_read inside its system-table
    # readers; importing it and touching that import path must not blow up on etl.
    proc = _run_in_gui(
        "import iceberg_browser; from metastore_read import read_table_rows")
    assert proc.returncode == 0, (
        f"iceberg_browser -> metastore_read import chain broken:\n{proc.stderr}")
