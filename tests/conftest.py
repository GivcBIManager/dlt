"""Pytest shared fixtures: import paths + isolated state dir."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "gui"))
sys.path.insert(0, str(REPO_ROOT / "orchestrator" / "src"))
sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    """Point config's JSON state at a temp dir so stores never touch real state."""
    import config

    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    monkeypatch.setattr(config, "PIPELINES_JSON", tmp_path / "pipelines.json")
    monkeypatch.setattr(config, "FLOWS_JSON", tmp_path / "flows.json")
    return tmp_path
