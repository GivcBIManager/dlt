"""Read and edit dbt model metadata -- layer, tags, description, column tests.

The dbt project classifies models into three layers by FOLDER:

    models/staging/       reads exactly one Iceberg lake table, 1:1
    models/intermediate/  joins 2+ lake tables into one business entity
    models/marts/         built on other dbt models via ref()

Each layer owns one schema file, ``models/<layer>/_<layer>__models.yml``, which
carries the description, tags and data tests for every model in that layer.
This module is the read/write layer the Models page drives.

Two things it is careful about:

* **Comments survive.** The schema files are heavily commented and those
  comments are the documentation. Edits go through ruamel.yaml in round-trip
  mode, which preserves comments, key order and block style. PyYAML would
  silently drop all of it.

* **Moving a layer moves both halves.** A model's layer is its folder, so
  changing it renames the ``.sql`` file *and* migrates its YAML block to the
  new layer's schema file. Doing only one leaves the project inconsistent.

Changing a layer does NOT change the model's relation name in ClickHouse: no
``+schema:`` is configured per layer in dbt_project.yml, precisely so that
reclassifying a model cannot break BI reports already pointed at it.
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

import dbt_config

LAYERS = ("staging", "intermediate", "marts")

# Column-level generic tests the editor offers. Values are what gets written
# into the YAML `data_tests:` list for a column.
COLUMN_TESTS = ("not_null", "unique")


def _flow(items: list[str]) -> Any:
    """A YAML list rendered inline as ["a", "b"] rather than one item per line.

    Short lists (tags, a column's data tests) are far easier to scan inline, and
    it is how the hand-written schema files are formatted -- so a save from the
    GUI leaves them looking the same as before rather than exploding every
    two-element list into a block.
    """
    from ruamel.yaml.comments import CommentedSeq

    seq = CommentedSeq(items)
    seq.fa.set_flow_style()
    return seq


def _yaml() -> YAML:
    y = YAML()  # round-trip mode: preserves comments, key order, block style
    y.preserve_quotes = True
    y.width = 100
    # Match the hand-written schema files: list items indent 4 from their key
    # and the dash sits at offset 2, i.e. "models:\n  - name: ...".
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def _root() -> Path:
    return dbt_config.dbt_dir().resolve()


def schema_path(layer: str) -> Path:
    if layer not in LAYERS:
        raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
    return _root() / "models" / layer / f"_{layer}__models.yml"


def model_path(layer: str, name: str) -> Path:
    return _root() / "models" / layer / f"{name}.sql"


def _load(path: Path) -> Any:
    if not path.exists():
        return {"version": 2, "models": []}
    with path.open(encoding="utf-8") as fh:
        return _yaml().load(fh) or {"version": 2, "models": []}


def _dump(path: Path, doc: Any) -> None:
    """Write ``doc`` to ``path`` atomically, so a crash cannot truncate it."""
    buf = io.StringIO()
    _yaml().dump(doc, buf)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(buf.getvalue(), encoding="utf-8")
    tmp.replace(path)


def _layer_of(name: str) -> str | None:
    """Which layer folder holds ``name``.sql, or None if the model has no file."""
    for layer in LAYERS:
        if model_path(layer, name).exists():
            return layer
    return None


def _find_block(doc: Any, name: str) -> Any | None:
    for entry in doc.get("models") or []:
        if entry.get("name") == name:
            return entry
    return None


def _tags_of(block: Any) -> list[str]:
    """Model-level tags from `config: tags:`, ignoring the layer tag.

    The layer tag (staging / intermediate / marts) is applied project-wide by
    dbt_project.yml, not stored per model, so it must not be echoed back into
    the editor as if it were an editable tag.
    """
    cfg = block.get("config") or {}
    return [t for t in (cfg.get("tags") or []) if t not in LAYERS]


def describe(name: str) -> dict[str, Any]:
    """Current metadata for one model, for the editor form."""
    layer = _layer_of(name)
    if layer is None:
        raise FileNotFoundError(f"no .sql file for model {name!r} in any layer folder")

    block = _find_block(_load(schema_path(layer)), name)
    if block is None:
        return {"name": name, "layer": layer, "documented": False,
                "description": "", "tags": [], "columns": [], "grain": []}

    columns = []
    for col in block.get("columns") or []:
        columns.append({
            "name": col.get("name", ""),
            "description": str(col.get("description") or "").strip(),
            "tests": [t for t in (col.get("data_tests") or []) if isinstance(t, str)],
        })

    grain: list[str] = []
    for test in block.get("data_tests") or []:
        if isinstance(test, dict) and "unique_combination_final" in test:
            args = test["unique_combination_final"].get("arguments") or {}
            grain = list(args.get("combination_of_columns") or [])

    return {
        "name": name,
        "layer": layer,
        "documented": True,
        "description": str(block.get("description") or "").strip(),
        "tags": _tags_of(block),
        "columns": columns,
        "grain": grain,
    }


def _apply(block: Any, meta: dict[str, Any]) -> None:
    """Write description / tags / column tests from ``meta`` into ``block``."""
    from ruamel.yaml.scalarstring import LiteralScalarString

    desc = str(meta.get("description") or "").strip()
    if desc:
        # Literal block scalar keeps multi-line prose readable in the file
        # instead of collapsing it into one very long quoted line.
        block["description"] = LiteralScalarString(desc + "\n") if "\n" in desc else desc
    else:
        block.pop("description", None)

    tags = [str(t).strip() for t in (meta.get("tags") or []) if str(t).strip()]
    tags = [t for t in tags if t not in LAYERS]  # layer tag comes from dbt_project.yml
    cfg = block.get("config")
    if tags:
        if cfg is None:
            block["config"] = {"tags": _flow(tags)}
        else:
            cfg["tags"] = _flow(tags)
    elif cfg is not None:
        cfg.pop("tags", None)
        if not cfg:
            block.pop("config", None)

    incoming = meta.get("columns")
    if incoming is None:
        return

    # The incoming list REPLACES the stored one -- clearing a column's
    # description and tests in the editor is how you remove it from the docs.
    # Matching on name first preserves any other keys already on that column
    # (quoting, meta, a test config) instead of rebuilding the node from scratch.
    existing = {c.get("name"): c for c in (block.get("columns") or [])}
    out = []
    for col in incoming:
        cname = str(col.get("name") or "").strip()
        if not cname:
            continue
        node = existing.get(cname)
        if node is None:
            node = {"name": cname}
        cdesc = str(col.get("description") or "").strip()
        if cdesc:
            node["description"] = cdesc
        else:
            node.pop("description", None)
        tests = [t for t in (col.get("tests") or []) if t in COLUMN_TESTS]
        if tests:
            node["data_tests"] = _flow(tests)
        else:
            node.pop("data_tests", None)
        out.append(node)
    block["columns"] = out


def save(name: str, meta: dict[str, Any]) -> dict[str, Any]:
    """Persist metadata for ``name``, moving layers if ``meta['layer']`` differs.

    Returns the metadata as re-read from disk, so the caller sees exactly what
    was written rather than what it asked for.
    """
    current = _layer_of(name)
    if current is None:
        raise FileNotFoundError(f"no .sql file for model {name!r} in any layer folder")

    target = str(meta.get("layer") or current)
    if target not in LAYERS:
        raise ValueError(f"unknown layer {target!r}; expected one of {LAYERS}")

    src_doc = _load(schema_path(current))
    block = _find_block(src_doc, name)

    if target != current:
        # Move the SQL file first: the layer IS the folder, and if this fails
        # we must not have already rewritten the schema files.
        dst_sql = model_path(target, name)
        dst_sql.parent.mkdir(parents=True, exist_ok=True)
        if dst_sql.exists():
            raise ValueError(f"{dst_sql.relative_to(_root())} already exists")
        model_path(current, name).rename(dst_sql)

        # Detach the block from the old layer's schema file.
        if block is not None:
            src_doc["models"] = [m for m in src_doc["models"] if m.get("name") != name]
            _dump(schema_path(current), src_doc)

        dst_doc = _load(schema_path(target))
        if block is None:
            block = {"name": name}
        dst_doc.setdefault("models", []).append(block)
        _apply(block, meta)
        _dump(schema_path(target), dst_doc)
    else:
        if block is None:
            block = {"name": name}
            src_doc.setdefault("models", []).append(block)
        _apply(block, meta)
        _dump(schema_path(current), src_doc)

    return describe(name)


def forget(name: str) -> bool:
    """Drop ``name``'s block from whichever layer schema file holds it.

    Called when a model's .sql is deleted. Without it dbt keeps parsing the
    orphaned block and warns on every invocation ("Did not find matching node
    for patch with name ..."), and any column tests it declared become tests
    against a node that no longer exists.
    """
    removed = False
    for layer in LAYERS:
        path = schema_path(layer)
        if not path.exists():
            continue
        doc = _load(path)
        models = doc.get("models") or []
        keep = [m for m in models if m.get("name") != name]
        if len(keep) != len(models):
            doc["models"] = keep
            _dump(path, doc)
            removed = True
    return removed


def catalog_columns(name: str) -> list[dict[str, str]]:
    """Real column names/types for ``name`` from the dbt catalog, if generated.

    Sourced from ``target/catalog.json`` (written by ``dbt docs generate``), so
    the editor offers the columns the table actually has rather than making the
    user type them. Returns [] when docs have never been generated.
    """
    import json

    cat = _root() / "target" / "catalog.json"
    if not cat.exists():
        return []
    try:
        data = json.loads(cat.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    for node in (data.get("nodes") or {}).values():
        if (node.get("metadata") or {}).get("name") == name:
            cols = sorted((node.get("columns") or {}).values(),
                          key=lambda c: c.get("index", 0))
            return [{"name": c.get("name", ""), "type": c.get("type", "")} for c in cols]
    return []


def overview() -> list[dict[str, Any]]:
    """Every model with its layer, tag and documentation status, for the list."""
    out = []
    for layer in LAYERS:
        doc = _load(schema_path(layer))
        blocks = {m.get("name"): m for m in (doc.get("models") or [])}
        folder = _root() / "models" / layer
        if not folder.exists():
            continue
        for sql in sorted(folder.glob("*.sql")):
            block = blocks.get(sql.stem)
            described = bool(block and str(block.get("description") or "").strip())
            cols = (block.get("columns") if block else None) or []
            tested = sum(1 for c in cols if c.get("data_tests"))
            out.append({
                "name": sql.stem,
                "layer": layer,
                "path": sql.relative_to(_root()).as_posix(),
                "tags": _tags_of(block) if block else [],
                "described": described,
                "columns_documented": len(cols),
                "columns_tested": tested,
            })
    return out
