"""Dagster code location entry point. ``defs`` is the launch target.

Launched by gui/dagster_service.py via ``dagster dev -m orchestrator.definitions``.
"""
from __future__ import annotations

import dagster as dg

defs = dg.Definitions()
