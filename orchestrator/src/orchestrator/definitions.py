"""Dagster code location entry point (launch target for ``dagster dev -m``)."""
from __future__ import annotations

from orchestrator.build import build_all_defs

defs = build_all_defs()
