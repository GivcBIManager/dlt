"""CRUD for the saved Pipeline library (``gui/state/pipelines.json``).

A pipeline is a named, validated command spec (the same spec the Run page
builds). Each becomes one Dagster asset. Stored as the source of truth so the
orchestrator can read it without importing Flask.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

import commands
import config


def _load() -> list[dict[str, Any]]:
    p = config.PIPELINES_JSON
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save(items: list[dict[str, Any]]) -> None:
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.PIPELINES_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(items, indent=2), encoding="utf-8")
    tmp.replace(config.PIPELINES_JSON)


def _render(p: dict[str, Any]) -> dict[str, Any]:
    _, label = commands.build_argv(p["spec"])
    out = dict(p)
    out["command"] = commands.preview(p["spec"])
    out["label"] = label
    return out


def load_pipelines() -> list[dict[str, Any]]:
    return [_render(p) for p in _load()]


def get_pipeline(pid: str) -> dict[str, Any] | None:
    return next((_render(p) for p in _load() if p["id"] == pid), None)


def add_pipeline(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    commands.build_argv(spec)  # validates; raises ValueError on bad spec
    name = (name or "").strip()
    if not name:
        raise ValueError("Pipeline name is required")
    items = _load()
    if any(p["name"] == name for p in items):
        raise ValueError(f"Pipeline '{name}' already exists")
    p = {"id": uuid.uuid4().hex[:8], "name": name, "spec": spec,
         "created_at": datetime.now().isoformat(timespec="seconds")}
    items.append(p)
    _save(items)
    return _render(p)


def update_pipeline(pid: str, **fields: Any) -> dict[str, Any]:
    items = _load()
    for p in items:
        if p["id"] == pid:
            if fields.get("spec") is not None:
                commands.build_argv(fields["spec"])
            for k in ("name", "spec"):
                if fields.get(k) is not None:
                    p[k] = fields[k]
            _save(items)
            return _render(p)
    raise KeyError(pid)


def delete_pipeline(pid: str) -> bool:
    items = _load()
    new = [p for p in items if p["id"] != pid]
    if len(new) == len(items):
        return False
    _save(new)
    return True
