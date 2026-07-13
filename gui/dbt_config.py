"""Resolve dbt settings and GENERATE dbt/profiles.yml from app config.

App config is the single source of truth: ClickHouse creds live in
``secrets.toml [clickhouse]`` and dbt knobs in ``config.toml [dbt]``. This module
renders both into a profiles.yml the dbt CLI consumes. profiles.yml is generated
(git-ignored) and never hand-edited.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

import clickhouse_config
import config
from workspace import dbt_settings

DBT_DIR = config.DBT_DIR

_DEFAULTS = {"target": "dev", "threads": 4, "project_dir": "dbt",
             "default_materialization": "table", "dbt_executable": "dbt"}


def _settings() -> dict[str, Any]:
    s = dict(_DEFAULTS)
    s.update({k: v for k, v in dbt_settings().items() if v not in (None, "")})
    return s


def dbt_dir() -> Path:
    pd = str(_settings().get("project_dir") or "dbt")
    p = Path(pd)
    if p.is_absolute():
        return p
    # Relative project_dir resolves against the repo root; use DBT_DIR's
    # parent as the root so a monkeypatched DBT_DIR (tests) and the default
    # both resolve correctly.
    return DBT_DIR.parent / p


def dbt_target() -> str:
    return str(_settings().get("target") or "dev")


def dbt_threads() -> int:
    try:
        return int(_settings().get("threads") or 4)
    except (TypeError, ValueError):
        return 4


def dbt_executable() -> str:
    # An explicit, non-default `[dbt].dbt_executable` path wins; otherwise defer
    # to config.dbt_executable(), which resolves the launcher next to the running
    # interpreter (the venv) so the GUI finds dbt without the venv being on PATH.
    configured = str(_settings().get("dbt_executable") or "").strip()
    if configured and configured != "dbt":
        return configured
    return config.dbt_executable()


def render_profiles() -> dict[str, Any]:
    ch = clickhouse_config._raw()
    host = str(ch.get("host") or "").strip()
    if not host:
        raise ValueError("configure ClickHouse first (secrets.toml [clickhouse].host)")
    target = dbt_target()
    output = {
        "type": "clickhouse",
        "driver": "http",
        "host": host,
        "port": int(ch.get("port", 8123)),
        "user": str(ch.get("user", "default")),
        "password": str(ch.get("password", "")),
        "schema": str(ch.get("database", "default")),
        "secure": bool(ch.get("secure", False)),
        "connect_timeout": int(ch.get("connect_timeout", 10)),
        "threads": dbt_threads(),
    }
    return {"oasis": {"target": target, "outputs": {target: output}}}


def write_profiles() -> Path:
    profiles = render_profiles()
    d = dbt_dir()
    d.mkdir(parents=True, exist_ok=True)
    path = d / "profiles.yml"
    tmp = path.with_suffix(".yml.tmp")
    tmp.write_text(yaml.safe_dump(profiles, sort_keys=False), encoding="utf-8")
    tmp.replace(path)
    return path
