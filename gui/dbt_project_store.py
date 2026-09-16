"""Browse and edit files in the dbt project, safely.

Lists models & tests (filesystem scan, enriched best-effort by ``dbt ls``),
reads/writes/creates ``.sql``/``.yml`` files, and refuses any path that escapes
the dbt project dir or uses a disallowed extension.
"""
from __future__ import annotations

import json
import re
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import dbt_config

_ALLOWED_SUFFIX = {".sql", ".yml", ".yaml"}
_ALLOWED_SUBDIRS = {"models", "tests", "macros"}
# Layer folders under models/. A model must live in one of them: the folder is
# what classifies it, and dbt_project.yml configures tags per layer.
_LAYERS = ("staging", "intermediate", "marts")
_DEFAULT_LAYER = "staging"

# dbt's global default when a model declares no config(materialized=...).
_DEFAULT_MATERIALIZATION = "view"
_MAT_RE = re.compile(r"materialized\s*=\s*['\"](\w+)['\"]")

MODEL_TEMPLATE = """\
-- {name}: materialize a local Iceberg table into a native ClickHouse table.
--
-- Read the lake through iceberg_source('<table>') rather than calling
-- icebergLocal() directly: the macro emits the same table function AND
-- registers the table as a dbt source, which is what puts this model on the
-- lineage graph. The table must be declared in
-- models/staging/_oasis_lake__sources.yml first.
--
-- Document this model (description, tags, column tests) in
-- models/{layer}/_{layer}__models.yml, or via the Metadata panel on this page.
{{{{ config(materialized='{materialization}') }}}}

select *
from {{{{ iceberg_source('CHANGE_ME') }}}}
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
    parts = p.relative_to(root).parts
    if not parts or parts[0] not in _ALLOWED_SUBDIRS:
        raise ValueError(
            f"dbt files must live under {sorted(_ALLOWED_SUBDIRS)} (got {rel!r})")
    return p


def _rel(p: Path) -> str:
    return p.relative_to(_root()).as_posix()


def _meta(p: Path) -> dict[str, Any]:
    """Filesystem metadata (size + timestamps) for a project file."""
    st = p.stat()
    return {
        "size": st.st_size,
        "modified": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
        "created": datetime.fromtimestamp(st.st_ctime).isoformat(timespec="seconds"),
    }


def _materialization(p: Path) -> str:
    """Parse ``config(materialized='...')``; fall back to dbt's default."""
    try:
        m = _MAT_RE.search(p.read_text(encoding="utf-8"))
    except OSError:
        return _DEFAULT_MATERIALIZATION
    return m.group(1) if m else _DEFAULT_MATERIALIZATION


def _scan(subdir: str) -> list[dict[str, Any]]:
    base = _root() / subdir
    if not base.exists():
        return []
    rtype = subdir.rstrip("s")
    out = []
    for p in sorted(base.rglob("*.sql")):
        entry = {"name": p.stem, "path": _rel(p), "resource_type": rtype,
                 "type": "test" if subdir == "tests" else _materialization(p)}
        entry.update(_meta(p))
        out.append(entry)
    return out


def _dbt_ls(resource_type: str) -> list[dict[str, Any]]:
    """Best-effort ``dbt ls`` enrichment; returns [] on any failure."""
    d = str(dbt_config.dbt_dir())
    try:
        proc = subprocess.run(
            [dbt_config.dbt_executable(), "ls", "--resource-type", resource_type,
             "--output", "json", "--project-dir", d, "--profiles-dir", d],
            capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace",
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


# ``dbt ls`` shells out and parses the whole project (~seconds); cache its output
# keyed on the newest models/tests file mtime so navigating the Models page does
# not spawn a subprocess per request. The filesystem ``_scan`` stays uncached, so
# a newly added file still shows up immediately even on a cache hit.
_LS_CACHE: dict[tuple[str, str], tuple[float, list[dict[str, Any]]]] = {}
_LS_LOCK = threading.Lock()


def _project_sig() -> float:
    """Newest mtime across the project's model/test source files (0 if none)."""
    root = _root()
    latest = 0.0
    for sub in ("models", "tests"):
        base = root / sub
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if p.suffix.lower() in _ALLOWED_SUFFIX:
                try:
                    latest = max(latest, p.stat().st_mtime)
                except OSError:
                    pass
    return latest


def _sync_lake_sources() -> None:
    """Declare newly loaded lake tables before dbt parses the project.

    Imported lazily so this module stays usable without ruamel.yaml.
    """
    try:
        import dbt_sources
    except ImportError:
        return
    dbt_sources.sync_safe()


def _dbt_ls_cached(resource_type: str) -> list[dict[str, Any]]:
    key = (str(_root()), resource_type)
    sig = _project_sig()
    with _LS_LOCK:
        hit = _LS_CACHE.get(key)
        if hit is not None and hit[0] == sig:
            return hit[1]
    # Cache miss, so `dbt ls` is about to parse the project, and one model that
    # reads an undeclared lake table makes it fail outright. Declare new tables
    # first, then re-take the signature in case that rewrote the sources file.
    _sync_lake_sources()
    sig = _project_sig()
    result = _dbt_ls(resource_type)
    with _LS_LOCK:
        _LS_CACHE[key] = (sig, result)
    return result


def list_models() -> list[dict[str, Any]]:
    return _merge(_scan("models"), _dbt_ls_cached("model"))


def list_tests() -> list[dict[str, Any]]:
    return _merge(_scan("tests"), _dbt_ls_cached("test"))


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


def _sanitize(name: str) -> str:
    return "".join(c for c in str(name or "").strip() if c.isalnum() or c in ("_", "-"))


def template_for(kind: str, name: str = "", materialization: str = "table",
                 layer: str = _DEFAULT_LAYER) -> str:
    """Render the starter template for a new model/test (frontend preview)."""
    if kind == "model":
        stem = _sanitize(name) or "new_model"
        return MODEL_TEMPLATE.format(name=stem, materialization=materialization or "table",
                                     layer=layer if layer in _LAYERS else _DEFAULT_LAYER)
    if kind == "test":
        stem = _sanitize(name) or "new_test"
        return TEST_TEMPLATE.format(name=stem)
    raise ValueError("kind must be 'model' or 'test'")


def create_from_template(name: str, kind: str, materialization: str = "table",
                         content: str | None = None,
                         layer: str = _DEFAULT_LAYER) -> dict[str, Any]:
    stem = _sanitize(name)
    if not stem:
        raise ValueError("name must be alphanumeric / underscore")
    if kind == "model":
        # New models go into a layer folder, never models/ root: a model outside
        # a layer picks up no layer tag and cannot be documented by the metadata
        # editor, which addresses models by the layer folder they live in.
        if layer not in _LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {list(_LAYERS)}")
        rel = f"models/{layer}/{stem}.sql"
    elif kind == "test":
        rel = f"tests/{stem}.sql"
    else:
        raise ValueError("kind must be 'model' or 'test'")
    if _resolve(rel).exists():
        raise ValueError(f"{rel} already exists")
    # Honor caller-supplied editor content; else fall back to the template.
    body = content if (content and content.strip()) else template_for(
        kind, stem, materialization, layer)
    return write_file(rel, body)
