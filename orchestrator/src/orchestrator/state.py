"""Bridge between the Dagster code location and the GUI's command/config layer.

Resolves the repo root robustly, puts ``gui/`` on ``sys.path``, and re-exports
``build_argv`` plus JSON readers so assets run *exactly* the argv the Run page
would. This is the single import seam; assets/email/build import only from here.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def _find_repo_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "oracle_to_iceberg.py").exists():
            return p
    raise RuntimeError("orchestrator: could not locate repo root (oracle_to_iceberg.py)")


REPO_ROOT = _find_repo_root(Path(__file__).resolve())

_gui_dir = REPO_ROOT / "gui"
if str(_gui_dir) not in sys.path:
    sys.path.insert(0, str(_gui_dir))

import commands as _commands  # noqa: E402  (after sys.path insert)
import config as _gui_config  # noqa: E402
import flow_naming  # noqa: E402  (re-exported so orchestrator modules share GUI naming)

build_argv = _commands.build_argv


def read_pipelines() -> dict[str, dict[str, Any]]:
    p = _gui_config.PIPELINES_JSON
    if not p.exists():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    return {x["id"]: x for x in data}


def read_flows() -> list[dict[str, Any]]:
    p = _gui_config.FLOWS_JSON
    if not p.exists():
        return []
    return json.loads(p.read_text(encoding="utf-8"))


def secrets_path() -> Path:
    return _gui_config.SECRETS_TOML


def ensure_dbt_profiles(spec: dict[str, Any] | None) -> None:
    """Generate dbt/profiles.yml before a dbt asset runs (no-op otherwise)."""
    if (spec or {}).get("script") == "dbt":
        import dbt_config  # gui/ is already on sys.path (see above)
        dbt_config.write_profiles()
