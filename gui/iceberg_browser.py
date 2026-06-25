"""Explore the Iceberg lake under ``iceberg_output/oasis``.

Table listing, schema, partition spec and snapshot history are read straight
from the table's ``*.metadata.json`` (no engine needed -- fast and dependency
light). Sample rows and per-branch counts use ``pyiceberg`` lazily, so the rest
of the panel keeps working even if pyiceberg is unavailable.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

from config import ICEBERG_ROOT, SYSTEM_TABLES

_VER_RE = re.compile(r"(\d+)-")


# --------------------------------------------------------------------------- #
# Metadata (pure JSON, no pyiceberg)
# --------------------------------------------------------------------------- #
def _table_dir(table: str) -> Path:
    safe = Path(table).name
    return ICEBERG_ROOT / safe


def _latest_metadata(table: str) -> Path | None:
    meta_dir = _table_dir(table) / "metadata"
    if not meta_dir.is_dir():
        return None
    metas = list(meta_dir.glob("*.metadata.json"))
    if not metas:
        return None

    def ver(p: Path) -> int:
        m = _VER_RE.match(p.name)
        return int(m.group(1)) if m else -1

    return max(metas, key=lambda p: (ver(p), p.stat().st_mtime))


def _load_metadata(table: str) -> dict[str, Any] | None:
    path = _latest_metadata(table)
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _current_snapshot(meta: dict[str, Any]) -> dict[str, Any] | None:
    cur = meta.get("current-snapshot-id")
    for s in meta.get("snapshots", []):
        if s.get("snapshot-id") == cur:
            return s
    snaps = meta.get("snapshots") or []
    return snaps[-1] if snaps else None


def _ms_to_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    return dt.datetime.fromtimestamp(ms / 1000).isoformat(timespec="seconds")


def list_tables() -> list[dict[str, Any]]:
    """All Iceberg datasets with headline numbers, data tables first."""
    if not ICEBERG_ROOT.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for child in sorted(ICEBERG_ROOT.iterdir()):
        if not (child / "metadata").is_dir():
            continue
        table = child.name
        meta = _load_metadata(table)
        rows = files = size = snaps = 0
        updated = None
        if meta:
            snaps = len(meta.get("snapshots") or [])
            updated = _ms_to_iso(meta.get("last-updated-ms"))
            cur = _current_snapshot(meta)
            if cur:
                summary = cur.get("summary", {})
                rows = int(summary.get("total-records", 0) or 0)
                files = int(summary.get("total-data-files", 0) or 0)
                size = int(summary.get("total-files-size", 0) or 0)
        out.append(
            {
                "table": table,
                "is_system": table in SYSTEM_TABLES,
                "rows": rows,
                "files": files,
                "size_bytes": size,
                "snapshots": snaps,
                "updated": updated,
            }
        )
    out.sort(key=lambda t: (t["is_system"], t["table"]))
    return out


def table_overview(table: str) -> dict[str, Any]:
    """Schema, partition spec, properties and snapshot history for one table."""
    meta = _load_metadata(table)
    if meta is None:
        raise FileNotFoundError(table)

    # Current schema fields.
    cur_schema_id = meta.get("current-schema-id", 0)
    schema = next(
        (s for s in meta.get("schemas", []) if s.get("schema-id") == cur_schema_id),
        (meta.get("schemas") or [{}])[0],
    )
    fields = [
        {
            "id": f.get("id"),
            "name": f.get("name"),
            "type": _fmt_type(f.get("type")),
            "required": f.get("required", False),
        }
        for f in schema.get("fields", [])
    ]

    # Partition spec (resolve source field names).
    field_by_id = {f.get("id"): f.get("name") for f in schema.get("fields", [])}
    spec = next(
        (s for s in meta.get("partition-specs", []) if s.get("spec-id") == meta.get("default-spec-id")),
        (meta.get("partition-specs") or [{}])[0],
    )
    partitions = [
        {
            "name": pf.get("name"),
            "transform": pf.get("transform"),
            "source": field_by_id.get(pf.get("source-id"), pf.get("source-id")),
        }
        for pf in spec.get("fields", [])
    ]

    # Snapshot history, newest first.
    snaps = []
    for s in sorted(meta.get("snapshots", []), key=lambda x: x.get("timestamp-ms", 0), reverse=True):
        summary = s.get("summary", {})
        snaps.append(
            {
                "snapshot_id": s.get("snapshot-id"),
                "committed": _ms_to_iso(s.get("timestamp-ms")),
                "operation": summary.get("operation"),
                "added_records": int(summary.get("added-records", 0) or 0),
                "total_records": int(summary.get("total-records", 0) or 0),
                "total_files": int(summary.get("total-data-files", 0) or 0),
                "is_current": s.get("snapshot-id") == meta.get("current-snapshot-id"),
            }
        )

    cur = _current_snapshot(meta) or {}
    cur_summary = cur.get("summary", {})
    return {
        "table": table,
        "is_system": table in SYSTEM_TABLES,
        "location": meta.get("location"),
        "format_version": meta.get("format-version"),
        "updated": _ms_to_iso(meta.get("last-updated-ms")),
        "current_snapshot_id": meta.get("current-snapshot-id"),
        "rows": int(cur_summary.get("total-records", 0) or 0),
        "files": int(cur_summary.get("total-data-files", 0) or 0),
        "size_bytes": int(cur_summary.get("total-files-size", 0) or 0),
        "fields": fields,
        "partitions": partitions,
        "properties": meta.get("properties", {}),
        "snapshots": snaps,
    }


def _fmt_type(t: Any) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, dict):
        kind = t.get("type")
        if kind == "list":
            return f"list<{_fmt_type(t.get('element'))}>"
        if kind == "map":
            return f"map<{_fmt_type(t.get('key'))},{_fmt_type(t.get('value'))}>"
        if kind == "struct":
            return "struct"
        return str(kind)
    return str(t)


# --------------------------------------------------------------------------- #
# Data access (lazy pyiceberg)
# --------------------------------------------------------------------------- #
def _open_static(table: str):
    from pyiceberg.table import StaticTable

    meta = _latest_metadata(table)
    if meta is None:
        raise FileNotFoundError(table)
    uri = "file://" + str(meta.resolve()).replace("\\", "/")
    return StaticTable.from_metadata(uri)


def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat(sep=" ")
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return str(v)


def sample_rows(table: str, limit: int = 50, branch_id: int | None = None) -> dict[str, Any]:
    """First ``limit`` rows (optionally one branch partition) as dicts."""
    tbl = _open_static(table)
    kwargs: dict[str, Any] = {}
    if branch_id is not None:
        kwargs["row_filter"] = f"branch_id = {int(branch_id)}"
    scan = tbl.scan(**kwargs)

    columns = [f.name for f in tbl.schema().fields]
    rows: list[dict[str, Any]] = []
    for batch in scan.to_arrow_batch_reader():
        if not batch.num_rows:
            continue
        take = min(batch.num_rows, limit - len(rows))
        sub = batch.slice(0, take).to_pylist()
        rows.extend({k: _jsonable(v) for k, v in r.items()} for r in sub)
        if len(rows) >= limit:
            break
    return {"columns": columns, "rows": rows}


def branch_counts(table: str) -> list[dict[str, Any]]:
    """Row count per ``branch_id`` for a data table."""
    import pyarrow.compute as pc

    tbl = _open_static(table)
    names = [f.name for f in tbl.schema().fields]
    if "branch_id" not in names:
        return []
    arrow = tbl.scan(selected_fields=("branch_id",)).to_arrow()
    if arrow.num_rows == 0:
        return []
    counts = pc.value_counts(arrow.column("branch_id"))
    out = []
    for entry in counts.to_pylist():
        out.append({"branch_id": _jsonable(entry["values"]), "rows": entry["counts"]})
    out.sort(key=lambda r: (r["branch_id"] is None, r["branch_id"]))
    return out


def read_system_table(table: str, limit: int = 200) -> dict[str, Any]:
    """Latest ``limit`` rows of a system table (etl_run_log / etl_dq_results / ...).

    Reads the whole (modest) table, sorts by the best available timestamp column
    descending, and returns the most recent rows.
    """
    tbl = _open_static(table)
    arrow = tbl.scan().to_arrow()
    columns = [f.name for f in arrow.schema]

    # Choose a sort column: prefer an explicit time/start column.
    sort_col = next(
        (c for c in ("started_at", "start_time", "run_started_at", "window_until",
                     "last_run_at", "recorded_updated_at", "insert_at")
         if c in columns),
        None,
    )
    if sort_col is not None:
        import pyarrow.compute as pc

        order = pc.sort_indices(arrow, sort_keys=[(sort_col, "descending")])
        arrow = arrow.take(order)

    rows = [
        {k: _jsonable(v) for k, v in r.items()}
        for r in arrow.slice(0, limit).to_pylist()
    ]
    return {"table": table, "columns": columns, "rows": rows, "total": arrow.num_rows}
