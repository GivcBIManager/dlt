"""Keep dbt's ``ofusion_*`` sources in step with the Oracle Fusion Iceberg warehouse.

This is the second lake's answer to ``dbt_sources.py``, and it has one extra job
that the oasis lake does not need: resolving each table's PHYSICAL PATH.

The oasis lake puts a table at ``<root>/<table>``, so the ``iceberg_source``
macro builds the path from the table name alone. The Fusion warehouse instead
lays tables out as::

    <root>/<domain>/<table>/<run stamp>/{data,metadata}

where the run stamp (``20260921T070725Z-6d27cc``) is minted by the pipeline that
writes the warehouse -- a pipeline that does NOT live in this repo. The stamp
differs per table, a table that was reloaded from scratch has several (older
ones left behind as empty shells), and ClickHouse's ``icebergLocal()`` takes no
glob, so nothing in dbt can derive the path. This module resolves it by looking:
for every table, the newest stamp directory that actually holds Iceberg
metadata wins, and the answer is written into a generated macro that
``ofusion_source()`` reads.

So ``sync()`` writes TWO generated files:

* ``models/fusion/staging/_ofusion__sources.yml`` -- one dbt source per domain
  (``ofusion_finance``, ``ofusion_hcm``, ...), declaring every table so models
  can name them and lineage has upstream edges;
* ``macros/_ofusion_paths.sql`` -- the ``ofusion_paths()`` lookup table of
  ``<domain>.<table> -> absolute path``, consumed by the ``ofusion_source``
  macro. Generated whole; never hand-edit it.

Like ``dbt_sources.sync()`` it only ever ADDS tables and refreshes the facts it
derives itself. Everything a person writes in the sources file -- descriptions,
extra tags, columns, tests -- is kept, and a table that has left the warehouse
is kept and reported as stale rather than deleted.

It runs before each dbt command the GUI or a Dagster flow launches, next to
``dbt_sources.sync_safe()``. That is not a convenience here: because the stamps
are minted outside this repo, a path resolved yesterday can be stale today, and
a stale path fails the model at query time with ClickHouse's unhelpful "metadata
file ... doesn't exist".

Run it by hand with ``python gui/ofusion_sources.py`` (``--check`` to report
only).
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import re
from pathlib import Path
from typing import Any

from ruamel.yaml.comments import CommentedMap, CommentedSeq

import config
import dbt_config
# Shared on purpose: both generated sources files must round-trip YAML with the
# same settings, or a person's quoting and folded prose would churn depending on
# which module rewrote the file last.
from dbt_sources import _flow, _yaml

log = logging.getLogger(__name__)

SOURCE_PREFIX = "ofusion_"
REL_SOURCES = Path("models") / "fusion" / "staging" / "_ofusion__sources.yml"
REL_PATHS_MACRO = Path("macros") / "_ofusion_paths.sql"

# Facts this module derives and therefore owns in each table entry's `meta`.
_DERIVED_META = ("domain", "run_stamp", "iceberg_path")

# Descriptions this module writes. Anything else is a person's and is kept.
_GENERATED_RE = re.compile(
    r"^Oracle Fusion \w+ table, landed as Iceberg by the ofusion pipeline\. "
    r"Not yet documented\.$")

SOURCES_HEADER = """\
# Oracle Fusion Iceberg warehouse -- the SECOND lake, one dbt source per domain.
#
# MAINTAINED BY gui/ofusion_sources.py: every table under the warehouse root is
# declared here automatically, before each dbt command run from the GUI or a
# Dagster flow (or on demand: `python gui/ofusion_sources.py`). Edit freely --
# descriptions, extra tags, columns and tests you add are kept. Only the `meta`
# facts are refreshed from the warehouse, and a description is only rewritten
# while it still reads "Not yet documented.".
#
# These are NOT relations ClickHouse can read by name. Every model reads them
# through the icebergLocal() table function, emitted by the `ofusion_source`
# macro in macros/ofusion_source.sql:
#
#     from {{ ofusion_source('fact_ap_payment', 'finance') }}
#
# `meta.iceberg_path` is the resolved path for reference only -- the macro reads
# the generated macros/_ofusion_paths.sql, not this file. Both are rewritten
# together, because the run stamp in the path is minted per load by a pipeline
# outside this repo and changes when a table is rebuilt.
#
# The warehouse root is config.OFUSION_ROOT ($OFUSION_ROOT) and is a path on the
# CLICKHOUSE HOST's filesystem.
"""

MACRO_HEADER = """\
{#
    ofusion_paths() -- <domain>.<table> -> the table's absolute Iceberg path.

    GENERATED WHOLE by gui/ofusion_sources.py. Do not edit: your changes are
    overwritten before the next dbt command runs.

    Each path ends in the run stamp directory the Fusion pipeline wrote the
    table under. The stamp is not derivable from the table name, which is why
    this lookup exists at all; see macros/ofusion_source.sql for how it is used
    and gui/ofusion_sources.py for how each entry is resolved.
#}
"""


def _dbt_dir() -> Path:
    return dbt_config.dbt_dir()


def sources_path() -> Path:
    return _dbt_dir() / REL_SOURCES


def paths_macro_path() -> Path:
    return _dbt_dir() / REL_PATHS_MACRO


def _resolve_table(table_dir: Path) -> tuple[Path, str] | None:
    """``(iceberg path, run stamp)`` for one table directory, or None.

    A directory counts as an Iceberg table only once it holds at least one
    ``metadata/*.metadata.json``. The Fusion pipeline creates the stamp
    directory before it writes anything and leaves the empty shell behind when a
    load fails, so "has a metadata/ folder" -- the test the oasis lake uses --
    would happily resolve to a stamp with no table in it.

    Of the stamps that do hold metadata the newest wins, by name: the stamp
    leads with a UTC timestamp, so lexical order is chronological, and unlike
    mtime it does not flap when an old snapshot's files are touched.

    A table written directly at ``<table>/metadata`` (no stamp) is accepted too,
    so a change of layout at the source degrades to the oasis lake's shape
    instead of silently dropping every table.
    """
    if any(table_dir.glob("metadata/*.metadata.json")):
        return table_dir, ""
    stamps = sorted(
        (p for p in table_dir.iterdir()
         if p.is_dir() and any(p.glob("metadata/*.metadata.json"))),
        key=lambda p: p.name)
    if not stamps:
        return None
    return stamps[-1], stamps[-1].name


def warehouse_tables() -> dict[str, dict[str, str]]:
    """``{"<domain>.<table>": {domain, table, path, run_stamp}}``, sorted by key.

    Domains are the top-level directories of the warehouse root (``finance``,
    ``hcm``, ...); dot-prefixed directories are skipped so a pipeline's own
    scratch or state folder never becomes a dbt source.
    """
    root = config.OFUSION_ROOT
    if not root.is_dir():
        raise FileNotFoundError(f"Fusion warehouse root not found: {root}")
    out: dict[str, dict[str, str]] = {}
    for domain_dir in sorted(root.iterdir(), key=lambda p: p.name):
        if not domain_dir.is_dir() or domain_dir.name.startswith((".", "_")):
            continue
        for table_dir in sorted(domain_dir.iterdir(), key=lambda p: p.name):
            if not table_dir.is_dir() or table_dir.name.startswith((".", "_")):
                continue
            resolved = _resolve_table(table_dir)
            if resolved is None:
                continue
            path, stamp = resolved
            out[f"{domain_dir.name}.{table_dir.name}"] = {
                "domain": domain_dir.name,
                "table": table_dir.name,
                "path": str(path),
                "run_stamp": stamp,
            }
    return out


def _generated_description(domain: str) -> str:
    return (f"Oracle Fusion {domain} table, landed as Iceberg by the ofusion "
            f"pipeline. Not yet documented.")


def _refresh(entry: CommentedMap, facts: dict[str, str] | None) -> None:
    """Bring the derived parts of one table entry up to date, in place.

    Values are only assigned when they actually differ: reassigning an equal
    value would drop the quoting the file was written with and churn the diff.
    """
    desc = str(entry.get("description") or "").strip()
    if facts and (not desc or _GENERATED_RE.match(desc)):
        new = _generated_description(facts["domain"])
        if desc != new:
            entry["description"] = new

    meta = entry.get("meta")
    if facts:
        if meta is None:
            meta = entry["meta"] = CommentedMap()
        derived = {"domain": facts["domain"], "run_stamp": facts["run_stamp"],
                   "iceberg_path": facts["path"]}
        for key in _DERIVED_META:
            if key not in meta or str(meta[key]) != derived[key]:
                meta[key] = derived[key]
    elif meta is not None:
        # Left the warehouse: drop the facts that are no longer true, keep the
        # entry and anything a person put in meta alongside them.
        for key in _DERIVED_META:
            meta.pop(key, None)
        if not meta:
            entry.pop("meta", None)


def _source_description(domain: str) -> str:
    return (f"Oracle Fusion {domain} subject area, landed as Apache Iceberg on "
            f"the ClickHouse host by the ofusion pipeline and read via "
            f"icebergLocal().")


def _render_sources(doc: Any, tables: dict[str, dict[str, str]]
                    ) -> tuple[str, list[str], list[str]]:
    """Return ``(file text, added keys, stale keys)``."""
    if not isinstance(doc, dict):
        doc = CommentedMap()
    sources = doc.get("sources")
    if not isinstance(sources, list):
        sources = CommentedSeq()

    by_source: dict[str, CommentedMap] = {
        str(s["name"]): s for s in sources
        if isinstance(s, dict) and str(s.get("name", "")).startswith(SOURCE_PREFIX)}
    domains = sorted({f["domain"] for f in tables.values()})
    for domain in domains:
        name = SOURCE_PREFIX + domain
        if name not in by_source:
            src = CommentedMap()
            src["name"] = name
            src["description"] = _source_description(domain)
            src["tags"] = _flow(["source", "iceberg", "ofusion", domain])
            by_source[name] = src
            sources.append(src)

    added: list[str] = []
    stale: list[str] = []
    for name, src in by_source.items():
        domain = name[len(SOURCE_PREFIX):]
        entries: dict[str, CommentedMap] = {}
        for t in src.get("tables") or []:
            if isinstance(t, dict) and t.get("name"):
                entries[str(t["name"])] = t
        want = {f["table"]: f for f in tables.values() if f["domain"] == domain}
        for table in sorted(want):
            if table not in entries:
                entry = CommentedMap()
                entry["name"] = table
                entry["description"] = ""
                entries[table] = entry
                added.append(f"{domain}.{table}")
        for table, entry in entries.items():
            facts = want.get(table)
            if facts is None:
                stale.append(f"{domain}.{table}")
            _refresh(entry, facts)
        src["tables"] = CommentedSeq(entries[t] for t in sorted(entries))

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
    # Exactly one blank line between entries, however the round-trip scattered
    # the blank lines it carried.
    text = re.sub(r"\n+(?=      - name: )", "\n\n", text)
    text = re.sub(r"\n+(?=  - name: )", "\n\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).rstrip() + "\n"
    version = doc.get("version", 2)
    return f"version: {version}\n\n{SOURCES_HEADER}\n{text}", sorted(added), sorted(stale)


def _render_paths_macro(tables: dict[str, dict[str, str]]) -> str:
    """The ``ofusion_paths()`` macro: a Jinja dict literal, one line per table."""
    lines = [f"        {key!r}: {facts['path']!r}," for key, facts in tables.items()]
    body = "\n".join(lines)
    return (f"{MACRO_HEADER}"
            f"{{% macro ofusion_paths() %}}\n"
            f"    {{{{ return({{\n{body}\n    }}) }}}}\n"
            f"{{% endmacro %}}\n")


def _write_if_changed(path: Path, text: str, write: bool) -> bool:
    """Write ``text`` to ``path`` only when it differs. Returns "was changed".

    An unchanged file keeps its mtime: the Models page caches ``dbt ls`` on a
    signature built from these files' mtimes.
    """
    old = path.read_text(encoding="utf-8") if path.exists() else ""
    if old == text:
        return False
    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Per-process temp name: the GUI and a Dagster step can sync at once.
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    return True


def sync(write: bool = True) -> dict[str, Any]:
    """Declare every warehouse table and resolve its path (see module docstring).

    ``write=False`` reports what would change without touching either file.
    """
    tables = warehouse_tables()
    src_path, macro_path = sources_path(), paths_macro_path()

    old = src_path.read_text(encoding="utf-8") if src_path.exists() else ""
    doc = _yaml().load(old) if old.strip() else None
    new_sources, added, stale = _render_sources(doc, tables)

    sources_changed = _write_if_changed(src_path, new_sources, write)
    paths_changed = _write_if_changed(macro_path, _render_paths_macro(tables), write)
    changed = sources_changed or paths_changed
    return {"path": str(src_path), "paths_macro": str(macro_path),
            "tables": len(tables), "domains": sorted({f["domain"] for f in tables.values()}),
            "added": added, "stale": stale,
            "changed": changed, "written": changed and write}


def sync_safe() -> dict[str, Any] | None:
    """``sync()`` for the pre-dbt hooks: never lets it block the dbt command.

    Does nothing for a directory that is not a dbt project, or when the Fusion
    warehouse is not mounted on this machine -- dbt then runs against the files
    as they stand, which is right for anyone working only on the oasis lake.
    """
    try:
        if not (_dbt_dir() / "dbt_project.yml").exists():
            return None
        result = sync()
    except FileNotFoundError:
        log.debug("Fusion warehouse not present at %s; leaving its dbt sources alone",
                  config.OFUSION_ROOT)
        return None
    except Exception:  # noqa: BLE001 - a pre-dbt convenience must not fail the run
        log.warning("could not sync dbt Fusion sources", exc_info=True)
        return None
    if result["added"]:
        log.info("declared %d new Fusion table(s) as dbt sources: %s",
                 len(result["added"]), ", ".join(result["added"]))
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Declare every Oracle Fusion warehouse table as a dbt source "
                    "and resolve its Iceberg path.")
    ap.add_argument("--check", action="store_true",
                    help="report only; exit 1 if either generated file is out of date")
    args = ap.parse_args(argv)
    r = sync(write=not args.check)
    print(f"{r['tables']} tables across {len(r['domains'])} domain(s) "
          f"({', '.join(r['domains']) or '-'}); "
          f"{len(r['added'])} new: {', '.join(r['added']) or '-'}")
    if r["stale"]:
        print(f"declared but no longer in the warehouse (kept): {', '.join(r['stale'])}")
    if args.check:
        print(("OUT OF DATE: " if r["changed"] else "up to date: ")
              + f"{r['path']}, {r['paths_macro']}")
        return 1 if r["changed"] else 0
    print(("updated: " if r["written"] else "already up to date: ")
          + f"{r['path']}, {r['paths_macro']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
