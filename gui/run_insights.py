"""Aggregated ETL run-log analytics for the Monitor page's Insights tab.

``etl_run_log`` holds one row per (table, branch) load attempt per pipeline run.
The GUI's Insights tab needs a dozen different cuts of that (per time bucket, per
branch, per table, per table type, per (table, branch)), so this module reads ONE
bounded window of rows and derives every cut from it in a single pass, rather
than issuing one GROUP BY round trip per chart.

The payload carries exactly the cuts the tab draws and nothing else -- a rollup
whose chart is removed is removed here too. It is one response per filter change,
so a spare cut is bytes on the wire for every interaction, not a free option.

Three durations are charted, and they are different things:

* **read**  -- this unit's Oracle extract + stage (``read_duration_ms``, and
  ``duration_ms`` for rows written before that column existed).
* **load**  -- the Iceberg commit of the table this unit belongs to. One commit
  covers every branch of a table, so the pipeline stamps the same elapsed time
  on each of that table's units (``load_duration_ms``).
* **total** -- read + load, i.e. the unit's whole trip from Oracle to the lake.

``total`` is deliberately left *unknown* rather than falling back to ``read``
when the load phase was never recorded: pretending a read-only number is a total
would quietly understate every duration chart on the page. Rows older than the
build that added the columns therefore report ``None`` for load/total, and
:func:`summarize` reports how many rows carry them (``coverage``) so the GUI can
say so out loud instead of drawing a misleadingly empty chart.

Two labels the run log does not itself store are resolved here: ``branch_id`` ->
branch *name* (from the ``[oracle_branches.*]`` config) and ``table_name`` ->
table *type* (masters / transactions / snapshots, from tables.json). Both are
resolved for the whole window before filtering, so they are available to every
cut and to the filter facets.

The read is bounded two ways: a time window pushed into SQL (``WHERE
coalesce(start_time, recorded_at) >= cutoff``) and a hard row cap, so the payload
cost is predictable no matter how large the log has grown. :func:`summarize` is a
pure function over already-fetched rows, so the whole shaping layer is testable
without a database (see ``tests/test_run_insights.py``).
"""
from __future__ import annotations

import datetime as dt
import math
from collections import defaultdict
from typing import Any, Iterable, Optional

# Columns the dashboard actually uses -- projected in SQL so the wire never
# carries error_details-sized text for rows nothing will render.
COLUMNS = (
    "pipeline_run_id", "table_name", "branch_id", "load_mode", "row_count",
    "start_time", "end_time", "duration_ms", "read_duration_ms",
    "load_duration_ms", "total_duration_ms", "status", "attempts",
    "write_disposition", "load_status", "error_details", "schema_discrepancy",
    "recorded_at",
)

DEFAULT_DAYS = 30
ROW_CAP = 200_000
TOP_N = 10          # ranked bar charts
# The heat map is a full-width card with only a handful of branch rows, so it
# can carry far more table columns than a bar chart can carry bars. Capped at
# what stays legible once the columns are drawn, not at TOP_N.
HEAT_N = 30
SUCCESS = "SUCCESS"
UNKNOWN = "UNKNOWN"

# A window this short is charted per hour rather than per day -- a "last 24
# hours" view bucketed by day is one or two columns wide and says nothing.
HOURLY_MAX_DAYS = 2

# The three duration families, in the order the dashboard presents them.
METRICS = ("total", "read", "load")


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat(sep=" ", timespec="seconds") if isinstance(value, dt.datetime) \
            else value.isoformat()
    return str(value)


def _when(row: dict) -> Optional[dt.datetime]:
    """The row's timeline anchor: its start, else when it was recorded."""
    for key in ("start_time", "recorded_at", "end_time"):
        val = row.get(key)
        if isinstance(val, dt.datetime):
            return val
    return None


def _num(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _ms(value: Any) -> Optional[int]:
    """A duration column as an int, keeping "not recorded" distinct from zero."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 1) if whole else 0.0


def percentile(values: list[int], p: float) -> Optional[int]:
    """Nearest-rank percentile of an unsorted list (None when empty)."""
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil(p * len(ordered)) - 1))
    return ordered[idx]


def _stats(durations: list[int]) -> dict[str, Optional[int]]:
    if not durations:
        return {"avg_ms": None, "p50_ms": None, "p95_ms": None,
                "max_ms": None, "min_ms": None, "total_ms": 0, "n": 0}
    return {
        "avg_ms": int(sum(durations) / len(durations)),
        "p50_ms": percentile(durations, 0.50),
        "p95_ms": percentile(durations, 0.95),
        "max_ms": max(durations),
        "min_ms": min(durations),
        "total_ms": sum(durations),
        "n": len(durations),
    }


# --------------------------------------------------------------------------- #
# The three durations
# --------------------------------------------------------------------------- #
def read_ms(row: dict) -> Optional[int]:
    """Oracle extract + stage. ``duration_ms`` is the pre-split name for it."""
    value = _ms(row.get("read_duration_ms"))
    return value if value is not None else _ms(row.get("duration_ms"))


def load_ms(row: dict) -> Optional[int]:
    """Iceberg commit of this unit's table (None before the column existed)."""
    return _ms(row.get("load_duration_ms"))


def total_ms(row: dict) -> Optional[int]:
    """read + load -- None, never a silent fallback to read alone."""
    stored = _ms(row.get("total_duration_ms"))
    if stored is not None:
        return stored
    read, load = read_ms(row), load_ms(row)
    return None if load is None else (read or 0) + load


_DURATION = {"total": total_ms, "read": read_ms, "load": load_ms}


# --------------------------------------------------------------------------- #
# Labels the run log does not store: branch name, table type
# --------------------------------------------------------------------------- #
def branch_labels() -> dict[str, str]:
    """``{branch_id: branch name}`` from the ``[oracle_branches.*]`` config.

    Best-effort: an unreadable/absent secrets.toml just means the dashboard
    falls back to showing the raw id, which is what it did before.
    """
    try:
        import connections
        return {str(b["id"]): str(b.get("name") or b.get("key") or b["id"])
                for b in connections.list_connections() if b.get("id") is not None}
    except Exception:  # noqa: BLE001 - a label lookup must never fail the page
        return {}


def table_types() -> dict[str, str]:
    """``{table_name: masters|transactions|snapshots}`` from tables.json."""
    try:
        import tables_store
        return tables_store.dataset_categories()
    except Exception:  # noqa: BLE001 - see branch_labels
        return {}


def label_rows(rows: list[dict], *, branches: Optional[dict] = None,
               types: Optional[dict] = None) -> list[dict]:
    """Stamp ``branch`` (display name) and ``table_type`` onto every row.

    Done once over the whole window, before filtering, so the facet lists and
    every cut share one resolution pass. Rows are copied rather than mutated so
    the caller's fetched rows stay exactly what the database returned.
    """
    branches = branch_labels() if branches is None else branches
    types = table_types() if types is None else types
    out = []
    for row in rows:
        bid = "" if row.get("branch_id") is None else str(row["branch_id"])
        table = "" if row.get("table_name") is None else str(row["table_name"])
        out.append({**row,
                    "branch_id": bid,
                    "branch": branches.get(bid) or bid or UNKNOWN,
                    "table_type": types.get(table, UNKNOWN)})
    return out


# --------------------------------------------------------------------------- #
# Buckets
# --------------------------------------------------------------------------- #
class _Bucket:
    """Running tallies for one grouping key (a time bucket, branch, table...)."""

    __slots__ = ("units", "ok", "failed", "rows", "runs", "tables", "branches",
                 "retries", "drift", "last_at", "dur", "label")

    def __init__(self) -> None:
        self.units = self.ok = self.failed = self.rows = 0
        self.retries = self.drift = 0
        self.dur: dict[str, list[int]] = {m: [] for m in METRICS}
        self.runs: set = set()
        self.tables: set = set()
        self.branches: set = set()
        self.last_at: Optional[dt.datetime] = None
        self.label: Optional[str] = None

    def add(self, row: dict, when: Optional[dt.datetime]) -> None:
        self.units += 1
        ok = row.get("status") == SUCCESS
        self.ok += ok
        self.failed += not ok
        self.rows += _num(row.get("row_count"))
        for metric in METRICS:
            value = _DURATION[metric](row)
            if value is not None:
                self.dur[metric].append(value)
        if _num(row.get("attempts")) > 1:
            self.retries += 1
        if row.get("schema_discrepancy"):
            self.drift += 1
        self.runs.add(row.get("pipeline_run_id"))
        self.tables.add(row.get("table_name"))
        self.branches.add(row.get("branch_id"))
        if when and (self.last_at is None or when > self.last_at):
            self.last_at = when

    def out(self, key: str) -> dict:
        entry = {
            "key": key, "label": self.label or key,
            "units": self.units, "ok": self.ok, "failed": self.failed,
            "rows": self.rows, "runs": len(self.runs), "tables": len(self.tables),
            "branches": len(self.branches), "retries": self.retries, "drift": self.drift,
            "success_rate": _pct(self.ok, self.units), "last_at": _iso(self.last_at),
        }
        # Flat, prefixed keys (total_avg_ms, read_p95_ms, ...) so a chart can name
        # its series with one string and a data table can list them as columns.
        for metric in METRICS:
            for stat, value in _stats(self.dur[metric]).items():
                entry[f"{metric}_{stat}"] = value
        return entry


def _rollup(rows: Iterable[dict], key_field: str,
            label_field: Optional[str] = None) -> list[dict]:
    """One ``_Bucket`` per distinct value of ``key_field``."""
    buckets: dict[str, _Bucket] = defaultdict(_Bucket)
    for row in rows:
        key = str(row.get(key_field) or UNKNOWN)
        bucket = buckets[key]
        if label_field and bucket.label is None:
            bucket.label = str(row.get(label_field) or key)
        bucket.add(row, _when(row))
    return [b.out(k) for k, b in buckets.items()]


def _counts(rows: Iterable[dict], field: str) -> list[dict]:
    """Value -> count for one column, biggest first (None folded to 'UNKNOWN')."""
    tally: dict[str, int] = defaultdict(int)
    for row in rows:
        tally[str(row.get(field) or UNKNOWN)] += 1
    return [{"key": k, "n": n} for k, n in
            sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))]


def _sums(rows: Iterable[dict], field: str, value_field: str) -> list[dict]:
    """Value -> summed ``value_field``, biggest first (for the records donuts)."""
    tally: dict[str, int] = defaultdict(int)
    for row in rows:
        tally[str(row.get(field) or UNKNOWN)] += _num(row.get(value_field))
    return [{"key": k, "n": n} for k, n in
            sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))]


def _facets(rows: Iterable[dict]) -> dict[str, list]:
    """Distinct filter values, computed over the whole window (not the slice)."""
    plain = ("table_name", "table_type", "load_mode", "status", "load_status")
    seen: dict[str, set] = {f: set() for f in plain}
    branches: dict[str, str] = {}
    for row in rows:
        for field, bag in seen.items():
            val = row.get(field)
            if val not in (None, ""):
                bag.add(str(val))
        bid = row.get("branch_id")
        if bid not in (None, ""):
            branches[str(bid)] = str(row.get("branch") or bid)
    out: dict[str, list] = {field: sorted(bag) for field, bag in seen.items()}
    # Branches carry a label as well as a value: the filter shows the name, the
    # query still travels as the id the run log actually stores.
    out["branch"] = [{"value": bid, "label": name}
                     for bid, name in sorted(branches.items(), key=lambda kv: kv[1].lower())]
    return out


# --------------------------------------------------------------------------- #
# Time bucketing
# --------------------------------------------------------------------------- #
def bucket_key(when: Optional[dt.datetime], granularity: str) -> tuple[str, str]:
    """``(sort key, short display label)`` for one row's time bucket."""
    if when is None:
        return UNKNOWN, UNKNOWN
    if granularity == "hour":
        return when.strftime("%Y-%m-%d %H"), when.strftime("%H:00")
    return when.date().isoformat(), when.strftime("%m-%d")


def granularity_for(days: int) -> str:
    return "hour" if 0 < days <= HOURLY_MAX_DAYS else "day"


# --------------------------------------------------------------------------- #
# Run-level rollup
# --------------------------------------------------------------------------- #
def _run_rollup(rows: list[dict]) -> list[dict]:
    """One entry per pipeline_run_id, newest first.

    A run's status is derived from its units: all-ok is SUCCESS, none-ok is
    FAILED, anything between is PARTIAL -- the distinction the "runs by status"
    chart exists to show (a run that loaded 40 of 42 tables is not a failure and
    is not a clean success either).
    """
    groups: dict[Any, list[dict]] = defaultdict(list)
    for row in rows:
        groups[row.get("pipeline_run_id")].append(row)

    out = []
    for run_id, units in groups.items():
        starts = [r["start_time"] for r in units if isinstance(r.get("start_time"), dt.datetime)]
        ends = [r["end_time"] for r in units if isinstance(r.get("end_time"), dt.datetime)]
        whens = [w for w in (_when(r) for r in units) if w]
        ok = sum(1 for r in units if r.get("status") == SUCCESS)
        failed = len(units) - ok
        start = min(starts) if starts else (min(whens) if whens else None)
        end = max(ends) if ends else None
        wall_ms = int((end - start).total_seconds() * 1000) if (start and end and end >= start) else None
        rows_total = sum(_num(r.get("row_count")) for r in units)
        # Read time is per unit and adds up. Load time is per *table* -- the same
        # commit stamped on each of that table's branches -- so summing it unit by
        # unit would multiply it by the branch count. Count each table once.
        read_total = sum(v for v in (read_ms(r) for r in units) if v is not None)
        per_table: dict[Any, int] = {}
        for r in units:
            value = load_ms(r)
            if value is not None:
                per_table[r.get("table_name")] = max(per_table.get(r.get("table_name"), 0), value)
        load_total = sum(per_table.values())
        out.append({
            "run_id": run_id,
            "status": SUCCESS if failed == 0 else ("FAILED" if ok == 0 else "PARTIAL"),
            "load_mode": next((r.get("load_mode") for r in units if r.get("load_mode")), None),
            "start_time": _iso(start), "end_time": _iso(end),
            "wall_ms": wall_ms,
            "read_ms": read_total, "load_ms": load_total,
            "busy_ms": read_total + load_total,
            "units": len(units), "ok": ok, "failed": failed,
            "rows": rows_total,
            "tables": len({r.get("table_name") for r in units}),
            "branches": len({r.get("branch_id") for r in units}),
            "retries": sum(1 for r in units if _num(r.get("attempts")) > 1),
            "drift": sum(1 for r in units if r.get("schema_discrepancy")),
            "rows_per_s": round(rows_total / (wall_ms / 1000), 1) if wall_ms else None,
            "_sort": start or dt.datetime.min,
        })
    out.sort(key=lambda r: r["_sort"], reverse=True)
    for entry in out:
        entry.pop("_sort")
    return out


# --------------------------------------------------------------------------- #
# The payload
# --------------------------------------------------------------------------- #
def _matches(row: dict, filters: dict[str, str]) -> bool:
    for field, wanted in filters.items():
        if wanted and str(row.get(field) or "") != wanted:
            return False
    return True


def summarize(rows: list[dict], *, branch: str = "", table: str = "",
              table_type: str = "", load_mode: str = "", status: str = "",
              top_n: int = TOP_N, heat_n: int = HEAT_N, granularity: str = "day",
              labelled: bool = False) -> dict:
    """Every cut the Insights tab draws, from one pass over ``rows``.

    ``rows`` is the whole time window; the filter arguments select the slice all
    the charts are drawn against. Facet lists come from the window (so a filter
    never removes its own option), everything else from the slice. Unless
    ``labelled`` says the caller already ran :func:`label_rows`, branch names and
    table types are resolved here.
    """
    if not labelled:
        rows = label_rows(rows)
    facets = _facets(rows)
    slice_ = [r for r in rows if _matches(r, {
        "branch_id": branch, "table_name": table, "table_type": table_type,
        "load_mode": load_mode, "status": status})]

    by_bucket: dict[str, _Bucket] = defaultdict(_Bucket)
    # Every heat cell carries all three durations, so one pass feeds three maps.
    heat: dict[tuple, _Bucket] = defaultdict(_Bucket)
    total_rows = retries = drift = ok_units = 0
    have_load = 0
    first_at = last_at = None

    for row in slice_:
        when = _when(row)
        key, label = bucket_key(when, granularity)
        bucket = by_bucket[key]
        bucket.label = label
        bucket.add(row, when)
        if when:
            first_at = when if first_at is None or when < first_at else first_at
            last_at = when if last_at is None or when > last_at else last_at
        heat[(str(row.get("branch") or UNKNOWN), str(row.get("table_name") or UNKNOWN))].add(row, when)
        total_rows += _num(row.get("row_count"))
        retries += _num(row.get("attempts")) > 1
        drift += bool(row.get("schema_discrepancy"))
        ok_units += row.get("status") == SUCCESS
        have_load += load_ms(row) is not None

    runs = _run_rollup(slice_)
    run_walls = [r["wall_ms"] for r in runs if r["wall_ms"] is not None]
    # Runs are graphed per bucket beside their units, so the time buckets carry a
    # per-status run tally too (a run belongs to the bucket it started in).
    runs_per_bucket: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"runs_ok": 0, "runs_partial": 0, "runs_failed": 0, "wall_ms": []})
    for run in runs:
        stamp = run["start_time"]
        when = dt.datetime.fromisoformat(stamp) if stamp else None
        key, _label = bucket_key(when, granularity)
        tally = runs_per_bucket[key]
        tally[{"SUCCESS": "runs_ok", "PARTIAL": "runs_partial"}.get(run["status"], "runs_failed")] += 1
        if run["wall_ms"] is not None:
            tally["wall_ms"].append(run["wall_ms"])

    trend = []
    for key in sorted(by_bucket):
        entry = by_bucket[key].out(key)
        tally = dict(runs_per_bucket.get(key) or {})
        walls = tally.pop("wall_ms", [])
        entry.update(tally or {"runs_ok": 0, "runs_partial": 0, "runs_failed": 0})
        entry["wall_avg_ms"] = int(sum(walls) / len(walls)) if walls else None
        trend.append(entry)

    by_branch = sorted(_rollup(slice_, "branch_id", "branch"), key=lambda e: -e["rows"])
    by_table = sorted(_rollup(slice_, "table_name"), key=lambda e: -e["rows"])
    by_table_type = sorted(_rollup(slice_, "table_type"), key=lambda e: -e["rows"])
    # The per-table rollup is what the "duration by table" data table renders, so
    # it has to carry the type each table was grouped under.
    types = {str(r.get("table_name") or UNKNOWN): r.get("table_type") for r in slice_}
    for entry in by_table:
        entry["table_type"] = types.get(entry["key"], UNKNOWN)

    # The tree the "duration by table" card renders: table type -> table ->
    # branch. The first two levels come from the rollups above; this is the leaf
    # level. Deliberately a narrow projection rather than a full _Bucket.out() --
    # it is the one rollup with (tables x branches) cardinality, so it is the one
    # that would dominate the payload if it carried every statistic.
    leaves: dict[tuple, _Bucket] = defaultdict(_Bucket)
    for row in slice_:
        key = (str(row.get("table_name") or UNKNOWN), str(row.get("branch_id") or UNKNOWN))
        bucket = leaves[key]
        bucket.label = str(row.get("branch") or key[1])
        bucket.add(row, _when(row))
    by_table_branch = []
    for (table_name, branch_id), bucket in leaves.items():
        entry = bucket.out(branch_id)
        by_table_branch.append({
            "table": table_name, "branch_id": branch_id, "branch": entry["label"],
            "units": entry["units"], "failed": entry["failed"], "rows": entry["rows"],
            **{f"{m}_avg_ms": entry[f"{m}_avg_ms"] for m in METRICS},
        })
    by_table_branch.sort(key=lambda e: (e["table"], -e["rows"]))

    # Heatmap axes are capped so the grid stays readable (and small on the wire);
    # the ranked tables above stay the complete list. Branches are few, so they
    # take the whole axis; tables get their own, larger cap (see HEAT_N).
    #
    # Table columns are ordered by TOTAL READ TIME, not by load count: the grid
    # is read against the question "where is the read phase actually spending
    # its time", and a table loaded often but quickly should not outrank one
    # loaded rarely that burns an hour every time. That also means the cap keeps
    # the heaviest tables rather than merely the busiest ones.
    heat_branches = [e["label"] for e in sorted(by_branch, key=lambda e: -e["units"])[:top_n]]
    heat_tables = [e["key"] for e in
                   sorted(by_table, key=lambda e: -(e["read_total_ms"] or 0))[:heat_n]]
    # Narrow projection, like the tree leaves: the grid is (branches x tables)
    # cells and a full statistics bucket per cell made the heat map the single
    # largest thing on the wire by a wide margin. A cell needs the value it
    # paints, the p95 its data table shows, and enough context to be a tooltip.
    heat_cells = []
    for b in heat_branches:
        for t in heat_tables:
            cell = heat.get((b, t))
            if cell is None:
                continue
            entry = cell.out(t)
            heat_cells.append({
                "branch": b, "table": t,
                "units": entry["units"], "failed": entry["failed"], "rows": entry["rows"],
                **{f"{m}_{s}": entry[f"{m}_{s}"] for m in METRICS for s in ("avg_ms", "p95_ms")},
            })

    slowest = [{
        "table": r.get("table_name"), "table_type": r.get("table_type"),
        "branch": r.get("branch"), "rows": _num(r.get("row_count")),
        "total_ms": total_ms(r), "read_ms": read_ms(r), "load_ms": load_ms(r),
        "status": r.get("status"), "when": _iso(_when(r)),
    } for r in sorted((r for r in slice_ if total_ms(r) is not None or read_ms(r) is not None),
                      key=lambda r: total_ms(r) if total_ms(r) is not None else (read_ms(r) or 0),
                      reverse=True)[:10]]

    units = len(slice_)
    whole = _Bucket()
    for row in slice_:
        whole.add(row, _when(row))
    kpi = {
        "runs": len(runs),
        "runs_ok": sum(1 for r in runs if r["status"] == SUCCESS),
        "runs_partial": sum(1 for r in runs if r["status"] == "PARTIAL"),
        "runs_failed": sum(1 for r in runs if r["status"] == "FAILED"),
        "units": units, "units_ok": ok_units, "units_failed": units - ok_units,
        "success_rate": _pct(ok_units, units),
        "run_success_rate": _pct(sum(1 for r in runs if r["status"] == SUCCESS), len(runs)),
        "rows": total_rows,
        "rows_per_run": int(total_rows / len(runs)) if runs else 0,
        "tables": len({r.get("table_name") for r in slice_}),
        "table_types": len({r.get("table_type") for r in slice_}),
        "branches": len({r.get("branch_id") for r in slice_}),
        "retries": retries, "drift": drift,
        "run_wall_avg_ms": int(sum(run_walls) / len(run_walls)) if run_walls else None,
        "run_wall_p95_ms": percentile(run_walls, 0.95),
        "run_wall_max_ms": max(run_walls) if run_walls else None,
        "first_at": _iso(first_at), "last_at": _iso(last_at),
    }
    for metric in METRICS:
        for stat, value in _stats(whole.dur[metric]).items():
            kpi[f"{metric}_{stat}"] = value
    throughput_base = kpi["total_total_ms"] or kpi["read_total_ms"]
    kpi["throughput_rows_s"] = (round(total_rows / (throughput_base / 1000), 1)
                                if throughput_base else None)

    return {
        "facets": facets,
        "filters": {"branch": branch, "table": table, "table_type": table_type,
                    "load_mode": load_mode, "status": status},
        "granularity": granularity,
        # How much of the slice can answer the load/total duration questions at
        # all. The GUI says so plainly rather than drawing an empty chart.
        "coverage": {"units": units, "with_load_ms": have_load},
        "kpi": kpi,
        "trend": trend,
        "runs_by_status": _counts([{"status": r["status"]} for r in runs], "status"),
        "rows_by_table_type": _sums(slice_, "table_type", "row_count"),
        "by_branch": by_branch,
        "by_table": by_table,
        "by_table_branch": by_table_branch,
        "by_table_type": by_table_type,
        "heatmap": {"branches": heat_branches, "tables": heat_tables, "cells": heat_cells},
        "slowest": slowest,
    }


# --------------------------------------------------------------------------- #
# I/O
# --------------------------------------------------------------------------- #
def read_window(days: int = DEFAULT_DAYS, *, cap: int = ROW_CAP,
                now: Optional[dt.datetime] = None) -> tuple[list[dict], bool]:
    """The newest ``cap`` run-log rows inside a ``days``-wide window.

    ``days <= 0`` means "all history" (still capped). Returns
    ``(rows, truncated)``; ``truncated`` is True when the cap clipped the window,
    so the GUI can say the numbers cover only part of it.
    """
    from sqlalchemy import func, select

    from metastore_read import open_metastore

    store = open_metastore()
    table = store.etl_run_log
    anchor = func.coalesce(table.c.start_time, table.c.recorded_at)
    # Safe to name read/load/total_duration_ms even against a database created
    # before they existed: open_metastore() runs ensure_schema(), whose additive
    # ALTER TABLE ... ADD COLUMN IF NOT EXISTS pass has already put them there.
    # Rows written before the migration simply read NULL for them, which is
    # exactly what `coverage` in the payload exists to report.
    stmt = select(*[table.c[name] for name in COLUMNS])
    if days and days > 0:
        cutoff = (now or dt.datetime.now()) - dt.timedelta(days=days)
        stmt = stmt.where(anchor >= cutoff)
    stmt = stmt.order_by(anchor.desc().nulls_last()).limit(cap + 1)
    with store.engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(stmt)]
    truncated = len(rows) > cap
    return rows[:cap], truncated


def insights(days: int = DEFAULT_DAYS, *, branch: str = "", table: str = "",
             table_type: str = "", load_mode: str = "", status: str = "",
             cap: int = ROW_CAP) -> dict:
    """Full Insights payload for one window + filter slice."""
    rows, truncated = read_window(days, cap=cap)
    payload = summarize(label_rows(rows), branch=branch, table=table,
                        table_type=table_type, load_mode=load_mode, status=status,
                        granularity=granularity_for(days), labelled=True)
    payload["window"] = {"days": days, "rows_scanned": len(rows),
                         "truncated": truncated, "cap": cap}
    return payload
