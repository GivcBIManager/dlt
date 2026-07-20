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
import shutil
from pathlib import Path
from typing import Any

import tables_store
from config import ICEBERG_ROOT, SYSTEM_TABLES

_VER_RE = re.compile(r"(\d+)-")

# tables.json section -> singular label shown/filtered in the explorer.
_CATEGORY_LABELS = {"masters": "master", "transactions": "transaction", "snapshots": "snapshot"}


def _category_index() -> dict[str, str]:
    """Map Iceberg table name -> tables.json category ('master' / ...).

    The Iceberg name is derived the same way the pipeline derives it
    (etl.config.TableDef.dataset_table_name): drop the owner prefix, then
    lower-snake the object name. A missing or unreadable tables.json just
    yields an empty index -- tables then show as uncategorized data.
    """
    try:
        doc = tables_store.load_raw()
    except (OSError, ValueError):
        return {}
    idx: dict[str, str] = {}
    for section, label in _CATEGORY_LABELS.items():
        items = doc.get(section) or []
        if not isinstance(items, list):
            continue
        for entry in items:
            if not isinstance(entry, dict):
                continue
            raw = str(entry.get("name") or "").strip()
            if not raw:
                ref = str(entry.get("table") or "")
                raw = ref.split(".", 1)[1] if "." in ref else ref
            name = re.sub(r"[^0-9a-zA-Z]+", "_", raw).strip("_").lower()
            if name:
                idx[name] = label
    return idx


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


def _id_str(snapshot_id: Any) -> str | None:
    # Snapshot ids are 64-bit ints that exceed JS Number.MAX_SAFE_INTEGER, so
    # they must cross the JSON boundary as strings or the browser rounds them
    # (e.g. ...950847 -> ...950000) and the round-trip snapshot lookup fails.
    return None if snapshot_id is None else str(snapshot_id)


def list_tables() -> list[dict[str, Any]]:
    """All Iceberg datasets with headline numbers, data tables first."""
    if not ICEBERG_ROOT.is_dir():
        return []
    categories = _category_index()
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
                "category": categories.get(table),
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
                "snapshot_id": _id_str(s.get("snapshot-id")),
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
        "current_snapshot_id": _id_str(meta.get("current-snapshot-id")),
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


def _scan_kwargs(branch_id: int | None, snapshot_id: int | None) -> dict[str, Any]:
    # branch_id is a partition value -> push it down as a row filter (prunes
    # files). Date filtering is applied in Python below (robust across the date /
    # timestamp / numeric-Julian columns this lake mixes).
    kwargs: dict[str, Any] = {}
    if branch_id is not None:
        kwargs["row_filter"] = f"branch_id = {int(branch_id)}"
    if snapshot_id is not None:
        kwargs["snapshot_id"] = int(snapshot_id)
    return kwargs


def _date_ok(value: Any, date_from: str | None, date_to: str | None) -> bool:
    d = "" if value is None else str(value)[:10]
    if date_from and d < date_from:
        return False
    if date_to and d > date_to:
        return False
    return True


def sample_rows(table: str, limit: int = 50, branch_id: int | None = None,
                snapshot_id: int | None = None, date_col: str | None = None,
                date_from: str | None = None, date_to: str | None = None) -> dict[str, Any]:
    """First ``limit`` rows after branch / snapshot / date-range filtering."""
    tbl = _open_static(table)
    scan = tbl.scan(**_scan_kwargs(branch_id, snapshot_id))
    columns = [f.name for f in tbl.schema().fields]
    by_date = bool(date_col and (date_from or date_to))

    rows: list[dict[str, Any]] = []
    for batch in scan.to_arrow_batch_reader():
        if not batch.num_rows:
            continue
        for r in batch.to_pylist():
            jr = {k: _jsonable(v) for k, v in r.items()}
            if by_date and not _date_ok(jr.get(date_col), date_from, date_to):
                continue
            rows.append(jr)
            if len(rows) >= limit:
                break
        if len(rows) >= limit:
            break
    return {"columns": columns, "rows": rows, "snapshot_id": snapshot_id}


def iter_csv(table: str, branch_id: int | None = None, snapshot_id: int | None = None,
             date_col: str | None = None, date_from: str | None = None,
             date_to: str | None = None):
    """Stream the *entire* filtered table as CSV rows — ignores any row limit."""
    import csv
    import io

    tbl = _open_static(table)
    columns = [f.name for f in tbl.schema().fields]
    scan = tbl.scan(**_scan_kwargs(branch_id, snapshot_id))
    by_date = bool(date_col and (date_from or date_to))

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")

    def flush() -> str:
        out = buf.getvalue()
        buf.seek(0)
        buf.truncate(0)
        return out

    writer.writerow(columns)
    yield flush()
    for batch in scan.to_arrow_batch_reader():
        if not batch.num_rows:
            continue
        for r in batch.to_pylist():
            jr = {c: _jsonable(r.get(c)) for c in columns}
            if by_date and not _date_ok(jr.get(date_col), date_from, date_to):
                continue
            writer.writerow([jr[c] for c in columns])
        yield flush()


def aggregate(table: str, by: str = "branch", date_col: str | None = None,
              gran: str = "day") -> dict[str, Any]:
    """Row counts grouped by ``branch_id``, a truncated date column, or both."""
    import pyarrow as pa
    import pyarrow.compute as pc

    tbl = _open_static(table)
    names = [f.name for f in tbl.schema().fields]
    want_branch = by in ("branch", "both") and "branch_id" in names
    want_date = by in ("date", "both")
    if want_date and (not date_col or date_col not in names):
        raise ValueError("A valid date column is required to aggregate by date")

    select = [c for c in (("branch_id" if want_branch else None), (date_col if want_date else None)) if c]
    if not select:
        return {"columns": [], "rows": []}

    arrow = tbl.scan(selected_fields=tuple(select)).to_arrow()
    cols: dict[str, Any] = {}
    group_keys: list[str] = []
    out_cols: list[str] = []
    if want_branch:
        cols["branch_id"] = arrow.column("branch_id")
        group_keys.append("branch_id")
        out_cols.append("branch_id")
    if want_date:
        cols["period"] = _date_bucket(arrow.column(date_col), gran, pa, pc)
        group_keys.append("period")
        out_cols.append("period")
    out_cols.append("rows")

    if arrow.num_rows == 0:
        return {"columns": out_cols, "rows": []}

    gt = pa.table(cols)
    grouped = gt.group_by(group_keys).aggregate([(group_keys[0], "count")])
    count_name = f"{group_keys[0]}_count"
    rows = []
    for rec in grouped.to_pylist():
        row = {k: _jsonable(rec.get(k)) for k in group_keys}
        row["rows"] = rec.get(count_name)
        rows.append(row)
    rows.sort(key=lambda r: tuple((r[k] is None, r[k]) for k in group_keys))
    return {"columns": out_cols, "rows": rows}


def _date_bucket(arr, gran: str, pa, pc):
    fmt = {"year": "%Y", "month": "%Y-%m", "day": "%Y-%m-%d"}.get(gran, "%Y-%m-%d")
    if pa.types.is_temporal(arr.type):
        return pc.strftime(arr, format=fmt)
    n = {"year": 4, "month": 7, "day": 10}.get(gran, 10)
    return pc.utf8_slice_codeunits(pc.cast(arr, pa.string()), 0, n)


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

    The three observability tables now live in Postgres (etl_meta.*); the sort,
    limit and total count are all pushed into SQL so this never materializes the
    whole (unbounded) table just to keep ``limit`` rows of it. Everything else is
    read from Iceberg.
    """
    if table in SYSTEM_TABLES:
        from metastore_read import count_table_rows, read_table_rows, table_columns

        schema_columns = table_columns(table)
        sort_col = next((c for c in ("check_time", "start_time", "recorded_at",
                                     "updated_at", "last_run_at") if c in schema_columns), None)
        rows = read_table_rows(table, order_by=sort_col, descending=True, limit=limit)
        columns = list(rows[0].keys()) if rows else schema_columns
        total = count_table_rows(table)
        rows = [{k: _jsonable(v) for k, v in r.items()} for r in rows]
        return {"table": table, "columns": columns, "rows": rows, "total": total}

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


def _summarize_runs(rows: list[dict], limit_runs: int = 100) -> list[dict]:
    """Group etl_run_log rows by pipeline_run_id into per-run summaries.

    Duration is *wall clock* (max end_time - min start_time), NOT the sum of
    per-unit duration_ms. Returns the newest ``limit_runs`` runs, newest first.
    """
    groups: dict[Any, list[dict]] = {}
    for r in rows:
        groups.setdefault(r.get("pipeline_run_id"), []).append(r)

    ranked: list[tuple[float, dict]] = []
    for run_id, grp in groups.items():
        starts = [r["start_time"] for r in grp if r.get("start_time") is not None]
        ends = [r["end_time"] for r in grp if r.get("end_time") is not None]
        start = min(starts) if starts else None
        end = max(ends) if ends else None
        duration_wall_ms = (
            int((end - start).total_seconds() * 1000) if (start and end) else None
        )
        ok = sum(1 for r in grp if r.get("status") == "SUCCESS")
        recorded = [r["recorded_at"] for r in grp if r.get("recorded_at") is not None]
        sort_dt = start or (max(recorded) if recorded else None)
        sort_ts = sort_dt.timestamp() if sort_dt is not None else float("-inf")
        ranked.append((sort_ts, {
            "run_id": run_id,
            "load_mode": next((r.get("load_mode") for r in grp if r.get("load_mode")), None),
            "start_time": _jsonable(start),
            "end_time": _jsonable(end),
            "duration_wall_ms": duration_wall_ms,
            "rows_total": sum(int(r.get("row_count") or 0) for r in grp),
            "units": len(grp),
            "ok": ok,
            "failed": len(grp) - ok,
            "schema_drift": sum(1 for r in grp if r.get("schema_discrepancy")),
            "errors": sum(1 for r in grp if r.get("error_details")),
            "tables": len({r.get("table_name") for r in grp}),
        }))

    ranked.sort(key=lambda t: t[0], reverse=True)
    return [summary for _, summary in ranked[:limit_runs]]


RUN_DETAIL_COLUMNS = [
    "table_name", "branch_id", "load_mode", "status", "row_count", "duration_ms",
    "write_disposition", "attempts", "control_status", "last_cdc_value",
    "last_date_value", "control_updated_at", "start_time", "end_time",
    "schema_discrepancy", "error_details",
]


def _control_index(control_rows: list[dict]) -> dict[tuple, dict]:
    """Index the latest etl_control rows by (table_name, branch_id)."""
    return {(r.get("table_name"), r.get("branch_id")): r for r in control_rows}


def _run_detail_rows(log_rows: list[dict], control_rows: list[dict]) -> list[dict]:
    """One row per etl_run_log unit, joined to its current etl_control watermark.

    Failed units sort first, then by table then branch. Control columns are None
    when etl_control has no matching (table_name, branch_id).
    """
    idx = _control_index(control_rows)
    out: list[dict] = []
    for r in log_rows:
        c = idx.get((r.get("table_name"), r.get("branch_id"))) or {}
        out.append({
            "table_name": r.get("table_name"),
            "branch_id": r.get("branch_id"),
            "load_mode": r.get("load_mode"),
            "status": r.get("status"),
            "row_count": r.get("row_count"),
            "duration_ms": r.get("duration_ms"),
            "write_disposition": r.get("write_disposition"),
            "attempts": r.get("attempts"),
            "control_status": c.get("status"),
            "last_cdc_value": c.get("last_cdc_value"),
            "last_date_value": c.get("last_date_value"),
            "control_updated_at": _jsonable(c.get("updated_at")),
            "start_time": _jsonable(r.get("start_time")),
            "end_time": _jsonable(r.get("end_time")),
            "schema_discrepancy": r.get("schema_discrepancy"),
            "error_details": r.get("error_details"),
        })
    out.sort(key=lambda d: (
        d["status"] == "SUCCESS",
        str(d["table_name"] or ""),
        d["branch_id"] is None,
        d["branch_id"] or 0,
    ))
    return out


# --------------------------------------------------------------------------- #
# I/O wrappers (system table reads)
# --------------------------------------------------------------------------- #
def _scan_pylist(table: str, row_filter=None) -> list[dict]:
    """System table as a list of plain dicts (timestamps -> datetime).

    The three observability tables (etl_control / etl_run_log / etl_dq_results)
    now live in Postgres, so an ``EqualTo(field, value)`` predicate (the only
    kind ever passed here -- the run-id pushdown) is pushed into the SQL WHERE
    clause via ``metastore_read.read_table_rows``; nothing is filtered in Python.
    Other tables still read from Iceberg with the predicate pushed into the scan
    so non-matching partitions/files are pruned.
    """
    if table in SYSTEM_TABLES:
        from metastore_read import read_table_rows
        if row_filter is not None:  # only EqualTo(pipeline_run_id, X) is used
            field, value = row_filter.term.name, row_filter.literal.value
            return read_table_rows(table, equals=(field, value))
        return read_table_rows(table)
    tbl = _open_static(table)
    scan = tbl.scan(row_filter=row_filter) if row_filter is not None else tbl.scan()
    return scan.to_arrow().to_pylist()


def read_run_summary(limit_runs: int = 100) -> dict[str, Any]:
    """Per-run rollup of the newest ``limit_runs`` runs, newest first.

    Only those runs' rows are ever pulled from Postgres (see
    ``metastore_read.read_recent_run_log``), so this stays cheap no matter how
    large ``etl_run_log`` has grown. Empty when the table is absent.
    """
    from metastore_read import read_recent_run_log
    try:
        rows = read_recent_run_log(limit_runs)
    except FileNotFoundError:
        return {"runs": []}
    return {"runs": _summarize_runs(rows, limit_runs=limit_runs)}


def read_run_detail(run_id: str) -> dict[str, Any]:
    """One run's units joined to the current etl_control watermark."""
    from pyiceberg.expressions import EqualTo

    try:
        # The run-id predicate is pushed all the way into SQL (see _scan_pylist /
        # metastore_read.read_table_rows), so this never materializes more than
        # one run's worth of etl_run_log rows.
        log_rows = _scan_pylist("etl_run_log", EqualTo("pipeline_run_id", run_id))
    except FileNotFoundError:
        log_rows = []
    try:
        control_rows = _scan_pylist("etl_control")
    except FileNotFoundError:
        control_rows = []
    return {
        "run_id": run_id,
        "columns": RUN_DETAIL_COLUMNS,
        "rows": _run_detail_rows(log_rows, control_rows)[:1000],
    }


# --------------------------------------------------------------------------- #
# Deletion (per-table drop + delete-all), spec 2026-07-14-staging-table-delete
# --------------------------------------------------------------------------- #
def _clear_control_state(tables: list[str]) -> list[str]:
    """Delete the tables' watermark rows from Postgres ``etl_meta.control_state``.

    Returns the subset of ``tables`` that actually had a row (so the next ETL run
    rebuilds them from scratch). A missing ``[postgres]`` config or an
    unreachable database clears nothing rather than raising.
    """
    try:
        from metastore_read import open_metastore
        store = open_metastore()
    except Exception:  # noqa: BLE001 - deletion must not hard-fail on metastore issues
        return []
    from sqlalchemy import delete
    cleared: list[str] = []
    with store.engine.begin() as conn:
        for t in tables:
            res = conn.execute(delete(store.control_state).where(
                store.control_state.c.table_name == t))
            if res.rowcount:
                cleared.append(t)
    return cleared


def _deletable_dir(table: str) -> Path:
    """Validate a table name for deletion and return its directory.

    Rejects path tricks (separators, ``..``), empty names and the ``_dlt*``
    bookkeeping folders. System tables pass — deleting them explicitly is
    allowed; the pipeline recreates them on the next observability write.
    """
    safe = Path(table).name
    if not safe or safe != table or safe in (".", "..") or safe.startswith("_dlt"):
        raise ValueError(f"table not deletable: {table!r}")
    return ICEBERG_ROOT / safe


def delete_table(table: str) -> dict[str, Any]:
    """Drop one staging table: its folder AND its control_state watermarks."""
    tdir = _deletable_dir(table)
    if not (tdir / "metadata").is_dir():
        raise FileNotFoundError(table)
    meta = _load_metadata(table)
    summary = (_current_snapshot(meta) or {}).get("summary", {}) if meta else {}
    shutil.rmtree(tdir)
    return {
        "deleted": [table],
        "watermarks_cleared": _clear_control_state([table]),
        "rows": int(summary.get("total-records", 0) or 0),
        "size_bytes": int(summary.get("total-files-size", 0) or 0),
        "errors": {},
    }


def delete_all_tables(include_system: bool = False) -> dict[str, Any]:
    """Drop every staging table (system tables only when ``include_system``).

    ``_dlt*`` folders are always kept. Per-table failures (e.g. a file locked
    on Windows) are collected in ``errors`` instead of aborting the sweep.
    """
    deleted: list[str] = []
    errors: dict[str, str] = {}
    if ICEBERG_ROOT.is_dir():
        for child in sorted(ICEBERG_ROOT.iterdir()):
            name = child.name
            if name.startswith("_dlt") or not (child / "metadata").is_dir():
                continue
            if name in SYSTEM_TABLES and not include_system:
                continue
            try:
                shutil.rmtree(child)
                deleted.append(name)
            except OSError as exc:
                errors[name] = str(exc)
    return {
        "deleted": deleted,
        "watermarks_cleared": _clear_control_state(deleted),
        "errors": errors,
    }
