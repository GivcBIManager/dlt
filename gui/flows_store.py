"""CRUD + validation for Flows (DAGs) in ``gui/state/flows.json``.

A flow is a set of nodes (each referencing a saved pipeline) plus dependency
edges, a cron schedule + timezone, email recipients, and an enabled flag. The
orchestrator turns each flow into Dagster assets + a job + a schedule + email
sensors.
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any

import config
import pipelines_store

_CRON_FIELD = re.compile(r"^[\d*/,\-]+$")


def validate_cron(expr: str) -> None:
    expr = (expr or "").strip()
    parts = expr.split()
    if len(parts) != 5 or not all(_CRON_FIELD.match(p) for p in parts):
        raise ValueError("Cron expression must have 5 valid fields (min hour dom mon dow)")


def validate_flow(nodes: list[dict[str, Any]], *, known_pipeline_ids: set[str]) -> None:
    if not nodes:
        raise ValueError("A flow needs at least one node")
    ids = [n["node_id"] for n in nodes]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate node_id in flow")
    idset = set(ids)
    for n in nodes:
        if n["pipeline_id"] not in known_pipeline_ids:
            raise ValueError(f"Node {n['node_id']} references unknown pipeline {n['pipeline_id']}")
        for d in n.get("deps", []):
            if d not in idset:
                raise ValueError(f"Node {n['node_id']} depends on unknown node {d}")
            if d == n["node_id"]:
                raise ValueError(f"Node {n['node_id']} depends on itself")
    _assert_acyclic(nodes)


def _assert_acyclic(nodes: list[dict[str, Any]]) -> None:
    deps = {n["node_id"]: list(n.get("deps", [])) for n in nodes}
    WHITE, GREY, BLACK = 0, 1, 2
    color = {k: WHITE for k in deps}

    def visit(u: str) -> None:
        color[u] = GREY
        for v in deps[u]:
            if color[v] == GREY:
                raise ValueError(f"Dependency cycle detected at node {v}")
            if color[v] == WHITE:
                visit(v)
        color[u] = BLACK

    for k in deps:
        if color[k] == WHITE:
            visit(k)


def _load() -> list[dict[str, Any]]:
    p = config.FLOWS_JSON
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save(items: list[dict[str, Any]]) -> None:
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.FLOWS_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(items, indent=2), encoding="utf-8")
    tmp.replace(config.FLOWS_JSON)


def _known_pipeline_ids() -> set[str]:
    return {p["id"] for p in pipelines_store.load_pipelines()}


def load_flows() -> list[dict[str, Any]]:
    return _load()


def get_flow(fid: str) -> dict[str, Any] | None:
    return next((f for f in _load() if f["id"] == fid), None)


def _normalize_email(email: dict[str, Any] | None) -> dict[str, list[str]]:
    email = email or {}
    def clean(v: Any) -> list[str]:
        if isinstance(v, str):
            v = re.split(r"[,\s]+", v)
        return [s.strip() for s in (v or []) if s and s.strip()]
    return {"on_success": clean(email.get("on_success")),
            "on_failure": clean(email.get("on_failure"))}


def add_flow(name: str, nodes: list[dict[str, Any]], cron: str, timezone: str,
             email: dict[str, Any] | None, enabled: bool = True) -> dict[str, Any]:
    name = (name or "").strip()
    if not name:
        raise ValueError("Flow name is required")
    validate_cron(cron)
    validate_flow(nodes, known_pipeline_ids=_known_pipeline_ids())
    items = _load()
    if any(f["name"] == name for f in items):
        raise ValueError(f"Flow '{name}' already exists")
    f = {"id": uuid.uuid4().hex[:8], "name": name, "nodes": nodes,
         "cron": cron.strip(), "timezone": (timezone or "UTC").strip(),
         "email": _normalize_email(email), "enabled": bool(enabled),
         "created_at": datetime.now().isoformat(timespec="seconds")}
    items.append(f)
    _save(items)
    return f


def update_flow(fid: str, **fields: Any) -> dict[str, Any]:
    items = _load()
    for f in items:
        if f["id"] == fid:
            if fields.get("cron") is not None:
                validate_cron(fields["cron"])
            if fields.get("nodes") is not None:
                validate_flow(fields["nodes"], known_pipeline_ids=_known_pipeline_ids())
            for k in ("name", "nodes", "cron", "timezone", "enabled"):
                if fields.get(k) is not None:
                    f[k] = fields[k]
            if fields.get("email") is not None:
                f["email"] = _normalize_email(fields["email"])
            _save(items)
            return f
    raise KeyError(fid)


def delete_flow(fid: str) -> bool:
    items = _load()
    new = [f for f in items if f["id"] != fid]
    if len(new) == len(items):
        return False
    _save(new)
    return True


def referencing_flows(pipeline_id: str) -> list[dict[str, Any]]:
    return [f for f in _load()
            if any(n["pipeline_id"] == pipeline_id for n in f["nodes"])]
