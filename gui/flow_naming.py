"""Deterministic, readable Dagster names derived from a flow.

Single source of truth shared by the GUI (``gui/``) and the Dagster code
location (``orchestrator/``). The orchestrator imports this module through the
``sys.path`` seam that ``orchestrator/state.py`` sets up, so the job / schedule
/ asset / group names Dagster builds always match the names the GUI uses to
launch and toggle them.

All outputs are constrained to ``[A-Za-z0-9_]`` (valid Dagster identifiers). The
``__<flow id>`` suffix guarantees uniqueness (two distinct flow names can
slugify to the same slug) and lets :func:`flow_id_from_job` recover the id --
``slugify`` never emits ``__``, so the delimiter is unambiguous.
"""
from __future__ import annotations

import re
from typing import Any

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    slug = _NON_ALNUM.sub("_", (name or "").strip().lower()).strip("_")
    return slug or "flow"


def base_name(flow: dict[str, Any]) -> str:
    return f"{slugify(flow['name'])}__{flow['id']}"


def job_name(flow: dict[str, Any]) -> str:
    return f"flow_{base_name(flow)}"


def schedule_name(flow: dict[str, Any]) -> str:
    return f"{job_name(flow)}_schedule"


def group_name(flow: dict[str, Any]) -> str:
    return slugify(flow["name"])


def asset_key_prefix(flow: dict[str, Any]) -> str:
    return base_name(flow)


def flow_id_from_job(name: str) -> str:
    """Recover the flow id from a name built by :func:`job_name`."""
    return name.rsplit("__", 1)[-1]
