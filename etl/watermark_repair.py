"""Repair and seed control_state watermarks (backs maintenance/repair_watermarks.py).

A datetime watermark stored in the future is always a bad source row that
predates ``oracle_extract._clamp_future_watermark``: the next incremental query
(``cdc > mark``) matches nothing, so the branch is frozen. The correct resume
point is the newest *real* value of the same column already in the lake --
everything up to it was loaded before the mark was poisoned -- so resetting
to it re-reads exactly the rows the freeze skipped. "Real" is judged per row
against its own load time (``Recorded_updated_at``), not against now: a source
may hold many bad rows on assorted future dates (item_barcode/jazan), some of
which have since passed, but no genuine change is stamped after it was loaded.

Helper-driven tables are reported but not repaired: their watermark column is
the helper's (``ETL_HELPER_CDC``/``_DATE``), which is stripped before the write,
so the lake cannot supply a resume point.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Callable, Optional

import pyarrow as pa
import pyarrow.compute as pc

from .config import TableDef

WM_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
_FIELDS = ("last_cdc", "last_date")


@dataclass(frozen=True)
class FutureMark:
    table: str              # dataset (Iceberg) table name
    branch: str             # branch key, e.g. "unaizah"
    field: str              # "last_cdc" | "last_date"
    value: str              # the stored (future) value
    column: Optional[str]   # source column the mark tracks; None = helper-driven/unknown


def _parse(value) -> Optional[dt.datetime]:
    for fmt in (WM_FORMAT, "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(str(value), fmt)
        except (TypeError, ValueError):
            continue
    return None


def _tracked_column(tdef: Optional[TableDef], field: str) -> Optional[str]:
    if tdef is None or tdef.is_helper_driven:
        return None
    return tdef.cdc_column if field == "last_cdc" else tdef.where_date_column


def find_future_marks(control: dict, tdefs: dict[str, TableDef],
                      now: dt.datetime) -> list[FutureMark]:
    """Every stored datetime watermark later than ``now``, sorted by table/branch."""
    out: list[FutureMark] = []
    for table in sorted(control):
        for branch in sorted(control[table]):
            entry = control[table][branch] or {}
            for field in _FIELDS:
                wm = entry.get(field) or {}
                if wm.get("kind", "datetime") != "datetime":
                    continue
                stamp = _parse(wm.get("value"))
                if stamp is None or stamp <= now:
                    continue
                out.append(FutureMark(table, branch, field, wm["value"],
                                      _tracked_column(tdefs.get(table), field)))
    return out


def _as_naive(arr):
    """Lake timestamps are UTC-tagged source wall clock: drop the tag, don't shift."""
    if pa.types.is_date(arr.type):
        return arr.cast(pa.timestamp("us"))
    if pa.types.is_timestamp(arr.type) and arr.type.tz is not None:
        return arr.cast(pa.timestamp("us"))
    return arr


def _max_loaded_before(values, loaded_at) -> Optional[dt.datetime]:
    """Max of ``values`` over rows whose value is no later than that row's load time.

    ``loaded_at`` is the row's ``Recorded_updated_at`` (naive local, the same
    wall clock the source values carry). Nulls on either side drop the row.
    """
    values = _as_naive(values)
    if len(values) == 0:
        return None
    real = pc.fill_null(pc.less_equal(values, _as_naive(loaded_at)), False)
    kept = pc.filter(values, real)
    return pc.max(kept).as_py() if len(kept) else None


def repair_value(mark: FutureMark,
                 lake_max: Callable[[str, str, str], Optional[dt.datetime]]) -> Optional[str]:
    """The reset value for ``mark`` (a watermark string), or None if it can't be derived."""
    if mark.column is None:
        return None
    got = lake_max(mark.table, mark.branch, mark.column)
    return got.strftime(WM_FORMAT) if got is not None else None


def lake_column(settings, branches: dict, table: str, branch_key: str,
                columns: list[str]) -> Optional[pa.Table]:
    """``columns`` (source names) of one branch partition of a lake table, or None."""
    from pyiceberg.expressions import EqualTo

    from .dq_check import _norm, dataset_root, open_lake_table

    st = open_lake_table(dataset_root(settings), table)
    if st is None:
        return None
    wanted = tuple(_norm(c) for c in columns)
    scan = st.scan(row_filter=EqualTo("branch_id", branches[branch_key].id),
                   selected_fields=wanted)
    return scan.to_arrow()


def lake_max_real(settings, branches: dict) -> Callable[[str, str, str], Optional[dt.datetime]]:
    """A ``lake_max`` callable for ``repair_value`` backed by the real lake."""
    from .dq_check import _norm

    loaded_col = settings.recorded_ts_column

    def _lake_max(table: str, branch_key: str, column: str) -> Optional[dt.datetime]:
        tbl = lake_column(settings, branches, table, branch_key, [column, loaded_col])
        if tbl is None or tbl.num_rows == 0:
            return None
        return _max_loaded_before(tbl.column(_norm(column)), tbl.column(_norm(loaded_col)))

    return _lake_max


def apply_marks(store, updates: list[tuple[str, str, str, dict]]) -> None:
    """Write ``(table, branch, field, watermark_dict)`` updates and save once.

    ``store`` is a loaded ``ControlStore``; its ``save()`` upserts every row, so
    this must not run while a pipeline run is in flight (its own end-of-run
    save would write the old values back).
    """
    for table, branch, field, wm in updates:
        store.data.setdefault(table, {}).setdefault(branch, {})[field] = wm
    store.save()


def min_key_since(keys, dates, since: dt.datetime) -> Optional[str]:
    """Smallest insert key whose row date is on/after ``since`` (as a mark string)."""
    dates = _as_naive(dates)
    if len(keys) == 0:
        return None
    mask = pc.fill_null(pc.greater_equal(dates, pa.scalar(since, pa.timestamp("us"))), False)
    kept = pc.drop_null(pc.filter(keys, mask))
    if len(kept) == 0:
        return None
    return str(pc.min(kept).as_py())


def seed_key_mark(value: str) -> dict:
    """``last_key`` entry for a seeded insert-key watermark."""
    return {"value": value, "kind": "number"}
