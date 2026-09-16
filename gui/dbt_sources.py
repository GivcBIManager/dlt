"""Keep dbt's ``oasis_lake`` source in step with the Iceberg lake.

Every table the dlt pipeline lands under the lake root (``config.ICEBERG_ROOT``)
is declared as a table of the ``oasis_lake`` dbt source in
``models/staging/_oasis_lake__sources.yml``. Models read the lake through the
``iceberg_source()`` macro, which calls ``source()``, and dbt refuses to parse a
project in which any model names an undeclared table -- so a new lake table has
to be declared before a model can use it, or every dbt command breaks.

``sync()`` runs before each dbt command the GUI or a Dagster flow launches (and
before the Models page's ``dbt ls``), so new tables are declared automatically.
It only ever ADDS tables and refreshes the facts it derives itself:

* a new table gets a generated "Not yet documented." description, a load-type
  tag, and its Oracle origin, unique key and CDC column from tables.json;
* an existing table keeps everything a person wrote -- description, extra tags,
  columns, tests -- and only has those derived facts refreshed (a description
  that is still the generated one is regenerated too);
* a declared table that has left the lake is kept and reported as stale, never
  deleted: dropping it would break any model still reading it with a far less
  obvious error, and would throw its documentation away.

The file is rewritten only when its content changes, so an unchanged lake
leaves its mtime alone (the Models page caches ``dbt ls`` on that signature).

Run it by hand with ``python gui/dbt_sources.py`` (``--check`` to report only).
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import re
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

import config
import dbt_config
import tables_store

log = logging.getLogger(__name__)

SOURCE_NAME = "oasis_lake"
REL_PATH = Path("models") / "staging" / "_oasis_lake__sources.yml"

# tables.json section -> load type recorded on the source table.
_LOAD_TYPES = {"masters": "master", "transactions": "transaction", "snapshots": "snapshot"}
_DERIVED_META = ("oracle_table", "load_type", "unique_key", "cdc_column")

# Descriptions this module writes. Anything else is a person's and is kept.
_GENERATED_RE = re.compile(
    r"^(Oracle \S+, loaded by the dlt pipeline as a \w+ table"
    r"|Iceberg table with no tables\.json entry)\. Not yet documented\.$")

_SOURCE_DESCRIPTION = (
    "Apache Iceberg tables extracted from the Oasis HIS Oracle database (schemas "
    "OASIS / DEVDBA / APP_ADMIN) by the dlt pipelines, landed on the ClickHouse "
    "host and read via icebergLocal().")

HEADER = """\
# Oasis Iceberg lake -- the landing zone written by the dlt extract jobs.
#
# MAINTAINED BY gui/dbt_sources.py: every table under the lake root is declared
# here automatically, before each dbt command run from the GUI or a Dagster flow
# (or on demand: `python gui/dbt_sources.py`). Edit freely -- descriptions,
# extra tags, columns and tests you add are kept. Only the `meta` facts and the
# load-type tag are refreshed from tables.json, and a description is only
# rewritten while it still reads "Not yet documented.".
#
# These are NOT relations ClickHouse can read by name. Every model reads them
# through the icebergLocal() table function, emitted by the `iceberg_source`
# macro in macros/iceberg_source.sql. Declaring them here is what lets dbt parse
# those models and gives the lineage graph its upstream edges.
#
# The lake root is the `iceberg_root` var in dbt_project.yml and is a path on
# the CLICKHOUSE HOST's filesystem.
"""


def _yaml() -> YAML:
    y = YAML()  # round-trip: keeps quoting and folded descriptions as written
    y.preserve_quotes = True
    y.width = 4096  # never re-wrap a person's prose
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def _flow(items: list[str]) -> CommentedSeq:
    seq = CommentedSeq(items)
    seq.fa.set_flow_style()
    return seq


def sources_path() -> Path:
    return dbt_config.dbt_dir() / REL_PATH


def lake_tables() -> list[str]:
    """Iceberg tables under the lake root, named as the pipeline names them.

    Same test as the Staging Layer Explorer: a directory with a ``metadata/``
    folder. That leaves out dlt's ``_dlt_*`` state folders (no Iceberg metadata)
    and stray files; the pipeline's own ``etl_*`` control tables are left out by
    name, since no model should build on them.
    """
    root = config.ICEBERG_ROOT
    if not root.is_dir():
        raise FileNotFoundError(f"Iceberg lake root not found: {root}")
    return sorted(
        p.name for p in root.iterdir()
        if (p / "metadata").is_dir()
        and p.name not in config.SYSTEM_TABLES
        and not p.name.startswith("_"))


def _origins() -> dict[str, dict[str, str]]:
    """``{lake table: derived facts}`` from tables.json."""
    doc = tables_store.load_raw()
    out: dict[str, dict[str, str]] = {}
    for section, load_type in _LOAD_TYPES.items():
        for entry in doc.get(section) or []:
            if not isinstance(entry, dict):
                continue
            name = tables_store._dataset_name(entry)
            if not name:
                continue
            table = str(entry.get("table") or "").strip()
            out[name] = {
                # A subquery source has no single Oracle table to point at.
                "oracle_table": "(subquery)" if table.startswith("(") else table,
                "load_type": load_type,
                "unique_key": str(entry.get("unique_key") or ""),
                "cdc_column": str(entry.get("cdc_column") or ""),
            }
    return out


def _generated_description(origin: dict[str, str] | None) -> str:
    if not origin:
        return "Iceberg table with no tables.json entry. Not yet documented."
    return (f"Oracle {origin['oracle_table']}, loaded by the dlt pipeline as a "
            f"{origin['load_type']} table. Not yet documented.")


def _refresh(entry: CommentedMap, origin: dict[str, str] | None) -> None:
    """Bring the derived parts of one table entry up to date, in place.

    Values are only assigned when they actually differ: reassigning an equal
    value would drop the quoting the file was written with and churn the diff.
    """
    desc = str(entry.get("description") or "").strip()
    if not desc or _GENERATED_RE.match(desc):
        new = _generated_description(origin)
        if desc != new:
            entry["description"] = new

    old_tags = [str(t) for t in (entry.get("tags") or [])]
    tags = [t for t in old_tags if t not in _LOAD_TYPES.values()]
    if origin:
        tags.insert(0, origin["load_type"])
    if tags != old_tags:
        if tags:
            entry["tags"] = _flow(tags)
        else:
            entry.pop("tags", None)

    meta = entry.get("meta")
    if origin:
        if meta is None:
            meta = entry["meta"] = CommentedMap()
        for key in _DERIVED_META:
            if key not in meta or str(meta[key]) != origin[key]:
                meta[key] = origin[key]
    elif meta is not None:
        for key in _DERIVED_META:
            meta.pop(key, None)
        if not meta:
            entry.pop("meta", None)


def _render(doc: Any, lake: list[str],
            origins: dict[str, dict[str, str]]) -> tuple[str, list[str], list[str]]:
    """Return ``(file text, added tables, stale tables)``."""
    if not isinstance(doc, dict):
        doc = CommentedMap()
    sources = doc.get("sources")
    if not isinstance(sources, list):
        sources = CommentedSeq()
    src = next((s for s in sources if isinstance(s, dict) and s.get("name") == SOURCE_NAME), None)
    if src is None:
        src = CommentedMap()
        src["name"] = SOURCE_NAME
        src["description"] = _SOURCE_DESCRIPTION
        src["tags"] = _flow(["source", "iceberg", "oasis"])
        sources.append(src)

    by_name: dict[str, CommentedMap] = {}
    for t in src.get("tables") or []:
        if isinstance(t, dict) and t.get("name"):
            by_name[str(t["name"])] = t
    in_lake = set(lake)
    added = [n for n in lake if n not in by_name]
    stale = sorted(n for n in by_name if n not in in_lake)
    for name in added:
        entry = CommentedMap()
        entry["name"] = name
        entry["description"] = ""
        by_name[name] = entry
    for name, entry in by_name.items():
        _refresh(entry, origins.get(name))
    src["tables"] = CommentedSeq(by_name[n] for n in sorted(by_name))

    # The header is owned here, so any top-level comment the file carried is
    # dropped and the header re-emitted; everything below `sources:` round-trips.
    body = CommentedMap()
    body["sources"] = sources
    for key, value in doc.items():
        if key not in ("version", "sources"):
            body[key] = value
    buf = io.StringIO()
    _yaml().dump(body, buf)
    text = buf.getvalue()
    # Exactly one blank line between table entries, however the round-trip
    # scattered the blank lines it carried.
    text = re.sub(r"\n+(?=      - name: )", "\n\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).rstrip() + "\n"
    version = doc.get("version", 2)
    return f"version: {version}\n\n{HEADER}\n{text}", added, stale


def sync(write: bool = True) -> dict[str, Any]:
    """Declare every lake table in the sources file (see the module docstring).

    ``write=False`` reports what would change without touching the file.
    """
    path = sources_path()
    lake = lake_tables()
    old = path.read_text(encoding="utf-8") if path.exists() else ""
    doc = _yaml().load(old) if old.strip() else None
    new, added, stale = _render(doc, lake, _origins())
    changed = new != old
    if changed and write:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Per-process temp name: the GUI and a Dagster step can sync at once.
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        tmp.write_text(new, encoding="utf-8")
        tmp.replace(path)
    return {"path": str(path), "tables": len(lake), "added": added, "stale": stale,
            "changed": changed, "written": changed and write}


def sync_safe() -> dict[str, Any] | None:
    """``sync()`` for the pre-dbt hooks: never lets it block the dbt command.

    Does nothing for a directory that is not a dbt project. Any failure (lake
    unreadable, malformed YAML) is logged and swallowed -- dbt then runs
    against the file as it stands, which is what happened before this existed.
    """
    try:
        if not (dbt_config.dbt_dir() / "dbt_project.yml").exists():
            return None
        result = sync()
    except Exception:  # noqa: BLE001 - a pre-dbt convenience must not fail the run
        log.warning("could not sync dbt lake sources", exc_info=True)
        return None
    if result["added"]:
        log.info("declared %d new lake table(s) as dbt sources: %s",
                 len(result["added"]), ", ".join(result["added"]))
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Declare every Iceberg lake table as a table of the oasis_lake dbt source.")
    ap.add_argument("--check", action="store_true",
                    help="report only; exit 1 if the sources file is out of date")
    args = ap.parse_args(argv)
    r = sync(write=not args.check)
    print(f"{r['tables']} lake tables; {len(r['added'])} new: {', '.join(r['added']) or '-'}")
    if r["stale"]:
        print(f"declared but no longer in the lake (kept): {', '.join(r['stale'])}")
    if args.check:
        print(("OUT OF DATE: " if r["changed"] else "up to date: ") + r["path"])
        return 1 if r["changed"] else 0
    print(("updated: " if r["written"] else "already up to date: ") + r["path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
