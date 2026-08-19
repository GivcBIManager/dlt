"""Aggregated ETL run-log analytics for the Monitor page's Insights tab.

``etl_run_log`` holds one row per (table, branch) load attempt per pipeline run.
The GUI's Insights tab needs a dozen different cuts of that (per day, per branch,
per table, per status, per run), so this module reads ONE bounded window of rows
and derives every cut from it in a single pass, rather than issuing one GROUP BY
round trip per chart.

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
    "start_time", "end_time", "duration_ms", "status", "attempts",
    "write_disposition", "load_status", "error_details", "schema_discrepancy",
    "recorded_at",
)

DEFAULT_DAYS = 30
ROW_CAP = 200_000
TOP_N = 12          # ranked bar charts / heatmap axes
SUCCESS = "SUCCESS"


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
        return {"avg_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None, "total_ms": 0}
    return {
        "avg_ms": int(sum(durations) / len(durations)),
        "p50_ms": percentile(durations, 0.50),
        "p95_ms": percentile(durations, 0.95),
        "max_ms": max(durations),
        "total_ms": sum(durations),
    }


class _Bucket:
    """Running tallies for one grouping key (a day, a branch, a table...)."""

    __slots__ = ("units", "ok", "failed", "rows", "durations", "runs", "tables",
                 "branches", "retries", "drift", "last_at")

    def __init__(self) -> None:
        self.units = self.ok = self.failed = self.rows = 0
        self.retries = self.drift = 0
        self.durations: list[int] = []
        self.runs: set = set()
        self.tables: set = set()
        self.branches: set = set()
        self.last_at: Optional[dt.datetime] = None

    def add(self, row: dict, when: Optional[dt.datetime]) -> None:
        self.units += 1
        ok = row.get("status") == SUCCESS
        self.ok += ok
        self.failed += not ok
        self.rows += _num(row.get("row_count"))
        if row.get("duration_ms") is not None:
            self.durations.append(_num(row.get("duration_ms")))
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
        return {
            "key": key, "units": self.units, "ok": self.ok, "failed": self.failed,
            "rows": self.rows, "runs": len(self.runs), "tables": len(self.tables),
            "branches": len(self.branches), "retries": self.retries, "drift": self.drift,
            "success_rate": _pct(self.ok, self.units), "last_at": _iso(self.last_at),
            **_stats(self.durations),
        }


def _counts(rows: Iterable[dict], field: str) -> list[dict]:
    """Value -> count for one column, biggest first (None folded to 'UNKNOWN')."""
    tally: dict[str, int] = defaultdict(int)
    for row in rows:
        tally[str(row.get(field) or "UNKNOWN")] += 1
    return [{"key": k, "n": n} for k, n in
            sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))]


def _facets(rows: Iterable[dict]) -> dict[str, list[str]]:
    """Distinct filter values, computed over the whole window (not the slice)."""
    seen: dict[str, set] = {f: set() for f in
                            ("branch_id", "table_name", "load_mode", "status", "load_status")}
    for row in rows:
        for field, bag in seen.items():
            val = row.get(field)
            if val not in (None, ""):
                bag.add(str(val))
    return {field: sorted(bag) for field, bag in seen.items()}


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
        out.append({
            "run_id": run_id,
            "status": SUCCESS if failed == 0 else ("FAILED" if ok == 0 else "PARTIAL"),
            "load_mode": next((r.get("load_mode") for r in units if r.get("load_mode")), None),
            "start_time": _iso(start), "end_time": _iso(end),
            "wall_ms": wall_ms,
            "busy_ms": sum(_num(r.get("duration_ms")) for r in units),
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
              load_mode: str = "", status: str = "", top_n: int = TOP_N) -> dict:
    """Every cut the Insights tab draws, from one pass over ``rows``.

    ``rows`` is the whole time window; the filter arguments select the slice all
    the charts are drawn against. Facet lists come from the window (so a filter
    never removes its own option), everything else from the slice.
    """
    facets = _facets(rows)
    slice_ = [r for r in rows if _matches(r, {
        "branch_id": branch, "table_name": table,
        "load_mode": load_mode, "status": status})]

    by_day: dict[str, _Bucket] = defaultdict(_Bucket)
    by_branch: dict[str, _Bucket] = defaultdict(_Bucket)
    by_table: dict[str, _Bucket] = defaultdict(_Bucket)
    by_hour = [0] * 24
    heat: dict[tuple, dict] = defaultdict(lambda: {"units": 0, "failed": 0, "rows": 0})
    durations: list[int] = []
    total_rows = retries = drift = ok_units = 0
    first_at = last_at = None

    for row in slice_:
        when = _when(row)
        day = when.date().isoformat() if when else "unknown"
        by_day[day].add(row, when)
        by_branch[str(row.get("branch_id") or "—")].add(row, when)
        by_table[str(row.get("table_name") or "—")].add(row, when)
        if when:
            by_hour[when.hour] += 1
            first_at = when if first_at is None or when < first_at else first_at
            last_at = when if last_at is None or when > last_at else last_at
        cell = heat[(str(row.get("branch_id") or "—"), str(row.get("table_name") or "—"))]
        cell["units"] += 1
        cell["rows"] += _num(row.get("row_count"))
        cell["failed"] += row.get("status") != SUCCESS
        if row.get("duration_ms") is not None:
            durations.append(_num(row.get("duration_ms")))
        total_rows += _num(row.get("row_count"))
        retries += _num(row.get("attempts")) > 1
        drift += bool(row.get("schema_discrepancy"))
        ok_units += row.get("status") == SUCCESS

    runs = _run_rollup(slice_)
    run_walls = [r["wall_ms"] for r in runs if r["wall_ms"] is not None]
    # Runs are graphed per day beside their units, so the daily buckets carry a
    # per-status run tally too (a run belongs to the day it started).
    runs_per_day: dict[str, dict[str, int]] = defaultdict(
        lambda: {"runs_ok": 0, "runs_partial": 0, "runs_failed": 0, "wall_ms": []})
    for run in runs:
        day = (run["start_time"] or "unknown")[:10] or "unknown"
        bucket = runs_per_day[day]
        bucket[{"SUCCESS": "runs_ok", "PARTIAL": "runs_partial"}.get(run["status"], "runs_failed")] += 1
        if run["wall_ms"] is not None:
            bucket["wall_ms"].append(run["wall_ms"])

    runs_by_status = _counts([{"status": r["status"]} for r in runs], "status")
    units = len(slice_)

    daily = []
    for day in sorted(by_day):
        entry = by_day[day].out(day)
        tally = runs_per_day.get(day)
        walls = tally.pop("wall_ms") if tally else []
        entry.update(tally or {"runs_ok": 0, "runs_partial": 0, "runs_failed": 0})
        entry["wall_avg_ms"] = int(sum(walls) / len(walls)) if walls else None
        daily.append(entry)
    ranked_branches = sorted((b.out(k) for k, b in by_branch.items()),
                             key=lambda e: -e["rows"])
    ranked_tables = sorted((t.out(k) for k, t in by_table.items()),
                           key=lambda e: -e["rows"])

    # Heatmap axes are capped so the grid stays readable (and small on the wire);
    # the ranked tables above stay the complete list.
    heat_branches = [e["key"] for e in sorted(ranked_branches, key=lambda e: -e["units"])[:top_n]]
    heat_tables = [e["key"] for e in sorted(ranked_tables, key=lambda e: -e["units"])[:top_n]]
    heat_cells = [
        {"branch": b, "table": t, **heat[(b, t)]}
        for b in heat_branches for t in heat_tables if (b, t) in heat
    ]

    failures = [{
        "table": r.get("table_name"), "branch": r.get("branch_id"),
        "status": r.get("status"), "when": _iso(_when(r)),
        "run_id": r.get("pipeline_run_id"),
        "error": (str(r.get("error_details"))[:300] if r.get("error_details") else None),
    } for r in sorted((r for r in slice_ if r.get("status") != SUCCESS),
                      key=lambda r: _when(r) or dt.datetime.min, reverse=True)[:25]]

    slowest = [{
        "table": r.get("table_name"), "branch": r.get("branch_id"),
        "duration_ms": _num(r.get("duration_ms")), "rows": _num(r.get("row_count")),
        "status": r.get("status"), "when": _iso(_when(r)),
    } for r in sorted((r for r in slice_ if r.get("duration_ms") is not None),
                      key=lambda r: _num(r.get("duration_ms")), reverse=True)[:10]]

    busy_ms = sum(durations)
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
        "branches": len({r.get("branch_id") for r in slice_}),
        "retries": retries, "drift": drift,
        "busy_ms": busy_ms,
        "throughput_rows_s": round(total_rows / (busy_ms / 1000), 1) if busy_ms else None,
        "run_wall_avg_ms": int(sum(run_walls) / len(run_walls)) if run_walls else None,
        "run_wall_p95_ms": percentile(run_walls, 0.95),
        "run_wall_max_ms": max(run_walls) if run_walls else None,
        "first_at": _iso(first_at), "last_at": _iso(last_at),
        **{f"unit_{k}": v for k, v in _stats(durations).items()},
    }

    return {
        "facets": facets,
        "filters": {"branch": branch, "table": table,
                    "load_mode": load_mode, "status": status},
        "kpi": kpi,
        "daily": daily,
        "hourly": [{"hour": h, "units": n} for h, n in enumerate(by_hour)],
        "runs_by_status": runs_by_status,
        "unit_status": _counts(slice_, "status"),
        "load_status": _counts(slice_, "load_status"),
        "load_mode": _counts(slice_, "load_mode"),
        "write_disposition": _counts(slice_, "write_disposition"),
        "by_branch": ranked_branches,
        "by_table": ranked_tables,
        "heatmap": {"branches": heat_branches, "tables": heat_tables, "cells": heat_cells},
        "runs": runs[:50],
        "failures": failures,
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
             load_mode: str = "", status: str = "", cap: int = ROW_CAP) -> dict:
    """Full Insights payload for one window + filter slice."""
    rows, truncated = read_window(days, cap=cap)
    payload = summarize(rows, branch=branch, table=table,
                        load_mode=load_mode, status=status)
    payload["window"] = {"days": days, "rows_scanned": len(rows),
                         "truncated": truncated, "cap": cap}
    return payload
