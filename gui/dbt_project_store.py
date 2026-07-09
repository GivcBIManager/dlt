"""Browse and edit files in the dbt project, safely.

Lists models & tests (filesystem scan, enriched best-effort by ``dbt ls``),
reads/writes/creates ``.sql``/``.yml`` files, and refuses any path that escapes
the dbt project dir or uses a disallowed extension.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import config
import dbt_config

_ALLOWED_SUFFIX = {".sql", ".yml", ".yaml"}

MODEL_TEMPLATE = """\
-- {name}: materialize a local Iceberg table into a native ClickHouse table.
--
-- WARNING: the icebergLocal(...) path is read by the CLICKHOUSE SERVER from its
-- own filesystem, not this host. Use a path valid on the ClickHouse host.
{{{{ config(materialized='{materialization}') }}}}

select *
from icebergLocal('/absolute/path/on/clickhouse/iceberg_output/oasis/CHANGE_ME')
"""

TEST_TEMPLATE = """\
-- {name}: a singular data test. It must return zero rows to pass.
select *
from {{{{ ref('CHANGE_ME') }}}}
where 1 = 0
"""


def _root() -> Path:
    return dbt_config.dbt_dir().resolve()


def _resolve(rel: str) -> Path:
    """Resolve ``rel`` under the dbt dir; raise on escape / bad extension."""
    rel = str(rel or "").strip()
    if not rel:
        raise ValueError("empty path")
    p = (_root() / rel).resolve()
    root = _root()
    if root not in p.parents and p != root:
        raise ValueError(f"path escapes the dbt project: {rel!r}")
    if p.suffix.lower() not in _ALLOWED_SUFFIX:
        raise ValueError(f"only {sorted(_ALLOWED_SUFFIX)} files are allowed")
    return p


def _rel(p: Path) -> str:
    return p.relative_to(_root()).as_posix()


def _scan(subdir: str) -> list[dict[str, Any]]:
    base = _root() / subdir
    if not base.exists():
        return []
    return [{"name": p.stem, "path": _rel(p), "resource_type": subdir.rstrip("s")}
            for p in sorted(base.rglob("*.sql"))]


def _dbt_ls(resource_type: str) -> list[dict[str, Any]]:
    """Best-effort ``dbt ls`` enrichment; returns [] on any failure."""
    d = str(dbt_config.dbt_dir())
    try:
        proc = subprocess.run(
            [config.dbt_executable(), "ls", "--resource-type", resource_type,
             "--output", "json", "--project-dir", d, "--profiles-dir", d],
            capture_output=True, text=True, timeout=60,
        )
        if proc.returncode != 0:
            return []
        out = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = obj.get("name")
            if name:
                out.append({"name": name, "path": obj.get("original_file_path", ""),
                            "resource_type": resource_type})
        return out
    except Exception:  # noqa: BLE001
        return []


def _merge(fs: list[dict[str, Any]], ls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name = {x["name"]: x for x in fs}
    for x in ls:
        by_name.setdefault(x["name"], x)
    return sorted(by_name.values(), key=lambda x: x["name"])


def list_models() -> list[dict[str, Any]]:
    return _merge(_scan("models"), _dbt_ls("model"))


def list_tests() -> list[dict[str, Any]]:
    return _merge(_scan("tests"), _dbt_ls("test"))


def read_file(rel: str) -> str:
    p = _resolve(rel)
    if not p.exists():
        raise FileNotFoundError(rel)
    return p.read_text(encoding="utf-8")


def write_file(rel: str, content: str) -> dict[str, Any]:
    p = _resolve(rel)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(content if content is not None else "", encoding="utf-8")
    tmp.replace(p)
    return {"path": _rel(p)}


def delete_file(rel: str) -> bool:
    p = _resolve(rel)
    if not p.exists():
        return False
    p.unlink()
    return True


def create_from_template(name: str, kind: str, materialization: str = "table") -> dict[str, Any]:
    stem = "".join(c for c in str(name or "").strip() if c.isalnum() or c in ("_", "-"))
    if not stem:
        raise ValueError("name must be alphanumeric / underscore")
    if kind == "model":
        rel = f"models/{stem}.sql"
        body = MODEL_TEMPLATE.format(name=stem, materialization=materialization or "table")
    elif kind == "test":
        rel = f"tests/{stem}.sql"
        body = TEST_TEMPLATE.format(name=stem)
    else:
        raise ValueError("kind must be 'model' or 'test'")
    if _resolve(rel).exists():
        raise ValueError(f"{rel} already exists")
    return write_file(rel, body)
