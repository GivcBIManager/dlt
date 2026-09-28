# Watermark Repair + delivery_charge Insert Key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unfreeze branches whose CDC watermark was poisoned by a future-dated source row, and stop `delivery_charge` from silently missing charges inserted under already-loaded delivery lines.

**Architecture:** Two independently shippable parts that share `etl_meta.control_state`.
- **Part A (Tasks 1–3):** `_wm_advance` stops letting a stored future datetime win. A new `etl/watermark_repair.py` module plus the `maintenance/repair_watermarks.py` CLI reset the stored future marks to the newest *non-future* value already in the lake, which is the true resume point.
- **Part B (Tasks 4–8):** tables gain an optional monotonic `insert_key_column`, tracked as a third watermark (`last_key`). It drives a third, disjoint `UNION ALL` branch in the incremental query: `t.<key> > last_key - lookback AND LNNVL(<cdc> > wm) [AND LNNVL(<date> >= wm)]`. That branch catches rows that neither the CDC branch nor the date branch selects.

**Tech Stack:** Python 3.12, python-oracledb (Oracle 11g, thick mode), pyarrow, pyiceberg StaticTable, SQLAlchemy/Postgres metastore, pytest. Run everything from `/home/bi/workspace/dlt` with `.venv/bin/python`.

**Spec:** the 2026-09-28 DQ mismatch analysis, summarized under Background below and in memory `dq-mismatch-root-causes`. There is no separate spec doc.

## Background (what the analysis proved)

- `control_state` holds `last_cdc = 2029-03-06 17:06:13` for `staff_contracts/unaizah` and `2029-12-27 11:39:43` for `item_barcode/jazan`.
  - Both tables are plain masters, so their incremental query is only `WHERE t.AMEND_LAST_DATE > <last_cdc>`. That matches nothing, so both branches are frozen.
  - `_clamp_future_watermark` (commit `453384f`) prevents *new* future captures. But `_wm_advance` (`etl/iceberg_load.py:92`) keeps whichever value is larger, so these stored values never go away.
  - Several future `last_date` values also exist (orders_master, doc, docl, delivery_lines, master_deliveries, account_transactions, ar_stat_of_invoices/jazan). They only disable the "new by date" branch. Task 1 lets them heal on the next run.
- `delivery_charge` has no CDC column of its own. It is loaded through `OASIS.DELIVERY_LINES.AMEND_LAST_DATE` (helper join).
  - All 2,137 charges missing from the lake on Muhayil sit on lines the lake already has. Sibling charges on those lines are loaded, and the line's `AMEND_LAST_DATE` never moved.
  - The missing charges have *higher* `DELIVERY_CHARGE_ID`s than older charges, so the ID behaves like an insert sequence.
  - About 246k rows are missing across the 8 branches month-to-date.

## Global Constraints

- The source is Oracle 11g. SQL must be 11g-valid. `LNNVL` is available (10g+). No `FETCH FIRST` and no identity columns.
- Datetime watermarks are strings in `"%Y-%m-%d %H:%M:%S.%f"`, kind `"datetime"`. Numeric watermarks are kind `"number"`, with values like `"1505160.0"` (Oracle `NUMBER` arrives as Arrow `double`).
- Lake timestamps are `timestamp[us, tz=UTC]` holding the **source wall clock**. Drop the tz tag without shifting (`cast(pa.timestamp("us"))`) before comparing with naive local times. The same rule is used in `dq_check._canon_array`.
- `now_local()` (`etl/config.py:59`) is naive local wall-clock time. Use it for every "is this in the future" comparison.
- `MetaStore._upsert` sets any column missing from a row to NULL. `ControlStore.save()` must therefore write *every* control_state column, including the new `last_key_*` ones.
- `ControlStore.save()` upserts the entire in-memory dict. Repair and seed tools must only `--apply` while **no pipeline run is in flight**. Otherwise the run's end-of-run save puts the old values back.
- `tables.json` is edited both by hand and from the GUI Tables page (`gui/tables_store.py` + `gui/templates/tables.html`). Every new key must be accepted by the GUI validator and preserved by the GUI edit form.
- Work on a feature branch (the repo is on `main`). End every commit message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A NULL helper CDC value** (`h.AMEND_LAST_DATE IS NULL`) on a newly inserted charge should still be picked up by the insert-key branch, and never duplicated. Task 5 pins the exact `LNNVL(...)` predicate form, since `NOT (x > y)` would drop NULL rows.
2. **A float-string key watermark** (`"1505160.0"`) minus the lookback should render as a plain decimal literal (`1504160.0`), not `1.50416E+6` and not a Python float artefact. Pinned in Task 5.
3. **Editing the delivery_charge row on the GUI Tables page** should keep `insert_key_column` and `insert_key_lookback`. The form has no inputs for them. Task 4 carries them over, with a manual check step (the repo has no JS test harness).
4. **A pipeline run in flight while `--apply` runs** would silently undo the repair. The run's `advance()` keeps the future mark, because it captured nothing, and then its `save()` overwrites. Task 3 and Task 8 each end with a post-run re-read of `control_state` that fails loudly if the value reverted.
5. **Lake timestamps carry a UTC tag over wall-clock values.** The repair value must equal the stored wall clock, not be shifted by +3h, and future lake rows (the poisoned row itself) must be excluded. Pinned in Task 2 (`_max_not_after` with a tz-tagged array).

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `etl/iceberg_load.py` | Modify | `_is_future_mark`, `_wm_advance` guard (Task 1); `ControlStore` load/advance/save of `last_key` (Task 6) |
| `etl/watermark_repair.py` | Create | Pure logic: find future marks, compute lake-derived reset values, seed insert-key marks, apply to a `ControlStore` (Tasks 2, 7) |
| `maintenance/repair_watermarks.py` | Create | Thin CLI over `etl.watermark_repair` (`future` and `seed-key` subcommands), dry-run by default (Tasks 2, 7) |
| `etl/config.py` | Modify | `TableDef.insert_key_column` / `insert_key_lookback`, parse and validate in `load_table_defs` (Task 4) |
| `gui/tables_store.py` | Modify | Accept and validate the two new keys (Task 4) |
| `gui/templates/tables.html` | Modify | Preserve the two keys when a row is edited (Task 4) |
| `etl/oracle_extract.py` | Modify | `_insert_key_branch`, `build_query(key_wm=...)`, `extract_table` passes and captures `last_key`, `_key_watermark_from_parquet`, `ExtractResult.new_key` (Tasks 5, 6) |
| `etl/metastore.py` | Modify | `control_state.last_key_value` / `last_key_kind` columns (Task 6) |
| `tables.json` | Modify | Enable the insert key on `OASIS.DELIVERY_CHARGE` (Task 8) |
| `tests/test_wm_advance.py` | Create | Task 1 |
| `tests/test_watermark_repair.py` | Create | Tasks 2, 7 |
| `tests/test_insert_key.py` | Create | Tasks 4, 5 |
| `tests/test_insert_key_capture.py` | Create | Task 6 |
| `tests/test_tables_store_validation.py` | Modify | Task 4 GUI validator cases |

---

## Part A — Poisoned watermarks

### Task 1: `_wm_advance` never lets a stored future datetime win

**Files:**
- Modify: `etl/iceberg_load.py:89-105`
- Test: `tests/test_wm_advance.py`

**Interfaces:**
- Produces: `_is_future_mark(wm: Optional[dict]) -> bool` in `etl.iceberg_load`. Task 2 does **not** import it: it has its own parser, because it also accepts values without a fraction.

- [ ] **Step 1: Write the failing test**

Create `tests/test_wm_advance.py`:

```python
"""A stored future-dated watermark must not outrank a fresh, real capture.

Captures are clamped to now (``_clamp_future_watermark``), but marks stored
before that clamp existed stay in control_state, and ``_wm_advance`` used to
keep the greater value -- so a 2029 mark could never be replaced. Any stored
datetime mark later than now is a bad source row by construction.
"""
from __future__ import annotations

import datetime as dt

from etl.iceberg_load import _is_future_mark, _wm_advance
from etl.oracle_extract import Watermark

POISON = {"value": "2029-03-06 17:06:13.000000", "kind": "datetime"}


def _ago(days: float) -> str:
    return (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S.%f")


def test_future_stored_mark_yields_to_a_fresh_capture():
    new = Watermark(value=_ago(1), kind="datetime")
    assert _wm_advance(dict(POISON), new) == new.to_dict()


def test_future_stored_mark_is_kept_when_the_run_captured_nothing():
    # Nothing to replace it with: the repair tool (Task 2) owns this case.
    assert _wm_advance(dict(POISON), Watermark(value=None)) == POISON


def test_past_stored_mark_still_only_moves_forward():
    old = {"value": _ago(1), "kind": "datetime"}
    assert _wm_advance(old, Watermark(value=_ago(5), kind="datetime")) == old


def test_number_marks_are_never_treated_as_future():
    # APPOINTMENTS.JULIAN_DATE-style marks are numbers, not datetimes.
    old = {"value": "2461252.0", "kind": "number"}
    assert not _is_future_mark(old)
    assert _wm_advance(old, Watermark(value="2461200.0", kind="number")) == old


def test_unparseable_or_absent_marks_are_not_future():
    assert not _is_future_mark(None)
    assert not _is_future_mark({"value": None, "kind": "datetime"})
    assert not _is_future_mark({"value": "garbage", "kind": "datetime"})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_wm_advance.py -v`
Expected: collection error, `ImportError: cannot import name '_is_future_mark'`.

- [ ] **Step 3: Implement**

In `etl/iceberg_load.py`, replace the `_wm_advance` function (lines 92-105) with:

```python
def _is_future_mark(wm: Optional[dict]) -> bool:
    """True for a stored datetime watermark later than now.

    Captures are clamped to now (``oracle_extract._clamp_future_watermark``), so
    a stored future mark can only be a bad source row that predates the clamp.
    """
    if not wm or wm.get("value") is None or wm.get("kind", "datetime") != "datetime":
        return False
    try:
        stored = dt.datetime.strptime(str(wm["value"]), "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return False
    return stored > now_local()


def _wm_advance(old: Optional[dict], new: Watermark) -> Optional[dict]:
    """Return the greater of an existing stored watermark and a fresh one.

    A stored mark in the future never wins: it would otherwise pin the branch
    past every real row forever (the fresh capture is already clamped to now).
    """
    if new.value is None:
        return old
    if old is None:
        return new.to_dict()
    if _is_future_mark(old):
        log.warning("stored watermark %s is in the future; replacing it with %s",
                    old["value"], new.value)
        return new.to_dict()
    try:
        if new.kind == "number":
            greater = float(new.value) > float(old["value"])
        else:  # datetime/string compare lexically (fixed format)
            greater = str(new.value) > str(old["value"])
    except (ValueError, KeyError, TypeError):
        greater = True
    return new.to_dict() if greater else old
```

(`dt`, `log`, `Optional` and `now_local` are already imported at the top of the module.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_wm_advance.py tests/test_watermark_clamp.py tests/test_control_store_threading.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_wm_advance.py
git commit -m "fix(etl): a stored future watermark no longer outranks a real capture

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Watermark repair module + `future` CLI

**Files:**
- Create: `etl/watermark_repair.py`
- Create: `maintenance/repair_watermarks.py`
- Test: `tests/test_watermark_repair.py`

**Interfaces:**
- Consumes: `ControlStore` (`etl.iceberg_load`), which exposes `.data` (nested dict) and `.save()`. Also `TableDef` (`etl.config`), `dq_check.dataset_root`, `dq_check.open_lake_table`, `dq_check._norm`.
- Produces (Task 7 extends this module and relies on these exact names):
  - `WM_FORMAT = "%Y-%m-%d %H:%M:%S.%f"`
  - `@dataclass(frozen=True) class FutureMark: table: str; branch: str; field: str; value: str; column: Optional[str]`
  - `find_future_marks(control: dict, tdefs: dict[str, TableDef], now: dt.datetime) -> list[FutureMark]`
  - `_as_naive(arr: pa.ChunkedArray | pa.Array) -> pa.ChunkedArray | pa.Array`: drops the tz tag, and turns date columns into `timestamp[us]`.
  - `_max_not_after(arr, now: dt.datetime) -> Optional[dt.datetime]`
  - `repair_value(mark: FutureMark, lake_max: Callable[[str, str, str], Optional[dt.datetime]]) -> Optional[str]`
  - `lake_column(settings, branches: dict, table: str, branch_key: str, columns: list[str]) -> Optional[pa.Table]`
  - `lake_max_not_after(settings, branches: dict, now: dt.datetime) -> Callable[[str, str, str], Optional[dt.datetime]]`
  - `apply_marks(store, updates: list[tuple[str, str, str, dict]]) -> None`: each tuple is `(table, branch, field, watermark_dict)`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_watermark_repair.py`:

```python
"""Repair of future-dated control_state watermarks (maintenance/repair_watermarks.py)."""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from etl.config import CATEGORY_MASTER, CATEGORY_TRANSACTION, HelperJoin, TableDef
from etl.watermark_repair import (
    FutureMark,
    _max_not_after,
    apply_marks,
    find_future_marks,
    repair_value,
)

NOW = dt.datetime(2026, 9, 28, 12, 0, 0)


def _master(name: str) -> TableDef:
    return TableDef(table=f"OASIS.{name.upper()}", unique_key="ID", cdc_column="AMEND_LAST_DATE",
                    where_date_column=None, where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _helper_txn(name: str) -> TableDef:
    return TableDef(table=f"OASIS.{name.upper()}", unique_key="ID", cdc_column=None,
                    where_date_column="DELIVERY_DATE", where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_TRANSACTION,
                    helper=HelperJoin(table="OASIS.PARENT", join_keys=(("P", "P"),),
                                      cdc_column="AMEND_LAST_DATE",
                                      where_date_column="AMEND_LAST_DATE"))


def _mark(value: str, kind: str = "datetime") -> dict:
    return {"value": value, "kind": kind}


CONTROL = {
    "staff_contracts": {
        "unaizah": {"last_cdc": _mark("2029-03-06 17:06:13.000000"), "last_date": None},
        "jazan": {"last_cdc": _mark("2026-09-27 15:47:54.000000"), "last_date": None},
    },
    "appointments": {"abha": {"last_cdc": None, "last_date": _mark("2461252.0", "number")}},
    "delivery_lines": {"khamis": {"last_cdc": None, "last_date": _mark("2027-01-01 00:21:05")}},
}
TDEFS = {"staff_contracts": _master("staff_contracts"),
         "delivery_lines": _helper_txn("delivery_lines")}


def test_only_future_datetime_marks_are_reported():
    marks = find_future_marks(CONTROL, TDEFS, NOW)
    assert [(m.table, m.branch, m.field) for m in marks] == [
        ("delivery_lines", "khamis", "last_date"),
        ("staff_contracts", "unaizah", "last_cdc"),
    ]


def test_a_mark_without_fractional_seconds_is_still_parsed():
    (dl,) = [m for m in find_future_marks(CONTROL, TDEFS, NOW) if m.table == "delivery_lines"]
    assert dl.value == "2027-01-01 00:21:05"


def test_plain_table_mark_tracks_its_own_column_helper_table_has_none():
    marks = {m.table: m for m in find_future_marks(CONTROL, TDEFS, NOW)}
    assert marks["staff_contracts"].column == "AMEND_LAST_DATE"
    # The helper's watermark alias is stripped before the Iceberg write, so the
    # lake cannot tell us the resume point: manual.
    assert marks["delivery_lines"].column is None


def test_repair_value_formats_the_lake_max_as_a_watermark():
    mark = FutureMark("staff_contracts", "unaizah", "last_cdc",
                      "2029-03-06 17:06:13.000000", "AMEND_LAST_DATE")
    got = repair_value(mark, lambda t, b, c: dt.datetime(2026, 8, 23, 7, 48, 46))
    assert got == "2026-08-23 07:48:46.000000"


def test_repair_value_is_none_when_it_cannot_be_derived():
    helper = FutureMark("delivery_lines", "khamis", "last_date", "2027-01-01 00:21:05", None)
    assert repair_value(helper, lambda t, b, c: dt.datetime(2026, 1, 1)) is None
    plain = FutureMark("staff_contracts", "unaizah", "last_cdc", "2029-...", "AMEND_LAST_DATE")
    assert repair_value(plain, lambda t, b, c: None) is None


def test_max_not_after_keeps_wall_clock_and_skips_the_poisoned_row():
    # Lake stores the source wall clock tagged UTC; the poisoned 2029 row is in there too.
    arr = pa.chunked_array([pa.array(
        [dt.datetime(2026, 8, 23, 7, 48, 46), dt.datetime(2029, 3, 6, 17, 6, 13), None],
        pa.timestamp("us", tz="UTC"))])
    assert _max_not_after(arr, NOW) == dt.datetime(2026, 8, 23, 7, 48, 46)


def test_max_not_after_handles_date_columns_and_empty_input():
    dates = pa.array([dt.date(2026, 9, 1), dt.date(2030, 1, 1)], pa.date32())
    assert _max_not_after(dates, NOW) == dt.datetime(2026, 9, 1)
    assert _max_not_after(pa.array([], pa.timestamp("us")), NOW) is None


class _FakeStore:
    def __init__(self, data):
        self.data = data
        self.saved = 0

    def save(self):
        self.saved += 1


def test_apply_marks_rewrites_only_the_named_field_and_saves_once():
    store = _FakeStore({"staff_contracts": {"unaizah": {
        "last_cdc": _mark("2029-03-06 17:06:13.000000"), "status": "SUCCESS"}}})
    apply_marks(store, [("staff_contracts", "unaizah", "last_cdc",
                         _mark("2026-08-23 07:48:46.000000"))])
    entry = store.data["staff_contracts"]["unaizah"]
    assert entry["last_cdc"] == _mark("2026-08-23 07:48:46.000000")
    assert entry["status"] == "SUCCESS"
    assert store.saved == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_watermark_repair.py -v`
Expected: collection error, `ModuleNotFoundError: No module named 'etl.watermark_repair'`.

- [ ] **Step 3: Implement the module**

Create `etl/watermark_repair.py`:

```python
"""Repair and seed control_state watermarks (backs maintenance/repair_watermarks.py).

A datetime watermark stored in the future is always a bad source row that
predates ``oracle_extract._clamp_future_watermark``: the next incremental query
(``cdc > mark``) matches nothing, so the branch is frozen. The correct resume
point is the newest *non-future* value of the same column already in the lake
-- everything up to it was loaded before the mark was poisoned -- so resetting
to it re-reads exactly the rows the freeze skipped.

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


def _max_not_after(arr, now: dt.datetime) -> Optional[dt.datetime]:
    """Max of a timestamp/date array ignoring values after ``now`` (and nulls)."""
    arr = _as_naive(arr)
    if len(arr) == 0:
        return None
    kept = pc.filter(arr, pc.less_equal(arr, pa.scalar(now, pa.timestamp("us"))))
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


def lake_max_not_after(settings, branches: dict,
                       now: dt.datetime) -> Callable[[str, str, str], Optional[dt.datetime]]:
    """A ``lake_max`` callable for ``repair_value`` backed by the real lake."""
    from .dq_check import _norm

    def _lake_max(table: str, branch_key: str, column: str) -> Optional[dt.datetime]:
        tbl = lake_column(settings, branches, table, branch_key, [column])
        if tbl is None or tbl.num_rows == 0:
            return None
        return _max_not_after(tbl.column(_norm(column)), now)

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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_watermark_repair.py -v`
Expected: all 9 PASS.

- [ ] **Step 5: Write the CLI**

Create `maintenance/repair_watermarks.py`:

```python
#!/usr/bin/env python
"""Repair future-dated control_state watermarks (and, later, seed insert keys).

Thin CLI over ``etl.watermark_repair``. Dry run by default; ``--apply`` writes.
Never ``--apply`` while a pipeline run is in flight: the run's end-of-run save
would write the old values back.

    python maintenance/repair_watermarks.py future                     # report all
    python maintenance/repair_watermarks.py future --field last_cdc \\
        --only staff_contracts/unaizah --only item_barcode/jazan --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# maintenance/ is a subdirectory of the repo root, where the etl package lives.
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from etl import config
from etl import watermark_repair as wr
from etl.iceberg_load import ControlStore
from etl.metastore import MetaStore


def _context():
    settings = config.load_settings({})
    if settings.postgres is None:
        raise SystemExit("no [postgres] config in .dlt/secrets.toml")
    branches = config.load_branches()
    tdefs = {t.dataset_table_name: t
             for t in config.load_table_defs(REPO / "tables.json")}
    store = ControlStore(MetaStore(settings.postgres)).load()
    return settings, branches, tdefs, store


def cmd_future(args) -> int:
    settings, branches, tdefs, store = _context()
    now = config.now_local()
    marks = wr.find_future_marks(store.as_dict(), tdefs, now)
    if args.field:
        marks = [m for m in marks if m.field == args.field]
    if args.only:
        wanted = set(args.only)
        marks = [m for m in marks if f"{m.table}/{m.branch}" in wanted]
    if not marks:
        print("no future-dated watermarks match")
        return 0

    lake_max = wr.lake_max_not_after(settings, branches, now)
    updates = []
    for m in marks:
        new = wr.repair_value(m, lake_max)
        verdict = new or ("manual: helper-driven" if m.column is None else "manual: no lake rows")
        print(f"{m.table:30s} {m.branch:9s} {m.field:9s} {m.value:28s} -> {verdict}")
        if new:
            updates.append((m.table, m.branch, m.field, {"value": new, "kind": "datetime"}))

    if not args.apply:
        print(f"\ndry run: {len(updates)} repairable mark(s); re-run with --apply to write")
        return 0
    wr.apply_marks(store, updates)
    print(f"\nwrote {len(updates)} mark(s) to control_state")
    return 0


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("future", help="reset future-dated watermarks to the lake's max")
    f.add_argument("--only", action="append", metavar="TABLE/BRANCH",
                   help="restrict to these units (repeatable)")
    f.add_argument("--field", choices=["last_cdc", "last_date"])
    f.add_argument("--apply", action="store_true", help="write (default: dry run)")
    f.set_defaults(func=cmd_future)
    return p.parse_args(argv)


def main(argv) -> int:
    logging.basicConfig(level=logging.WARNING)
    args = parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
```

- [ ] **Step 6: Smoke-test the dry run against the real metastore and lake (read-only)**

Run: `.venv/bin/python maintenance/repair_watermarks.py future --field last_cdc`
Expected: two lines, and no writes:
- `staff_contracts  unaizah  last_cdc  2029-03-06 17:06:13.000000 -> 2026-0…` (a date in 2026 at or before today)
- `item_barcode  jazan  last_cdc  2029-12-27 11:39:43.000000 -> 2026-0…`

The output ends with `dry run: 2 repairable mark(s)`. If either shows `manual: no lake rows`, stop and investigate before continuing.

- [ ] **Step 7: Commit**

```bash
git add etl/watermark_repair.py maintenance/repair_watermarks.py tests/test_watermark_repair.py
git commit -m "feat(maintenance): repair future-dated watermarks from the lake's max

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Apply the repair to the two frozen units (operational)

No code. This task changes production `control_state`. Do it with the user, and get their go-ahead on the dry-run output from Task 2 Step 6 first.

- [ ] **Step 1: Confirm no pipeline run is in flight**

Check the Dagster UI (or `tail -c 200000 run_logs/dagster.log | grep -a 'STEP_START\|RUN_SUCCESS\|RUN_FAILURE' | tail -3`). The latest run must be finished. Do not continue while a run is active.

- [ ] **Step 2: Apply**

Run: `.venv/bin/python maintenance/repair_watermarks.py future --field last_cdc --only staff_contracts/unaizah --only item_barcode/jazan --apply`
Expected: the same two lines as the dry run, then `wrote 2 mark(s) to control_state`.

- [ ] **Step 3: Verify the stored values**

Run:
```bash
.venv/bin/python -c "
import sys; sys.path.insert(0,'.')
from etl import config; from etl.metastore import MetaStore; from etl.iceberg_load import ControlStore
c=ControlStore(MetaStore(config.load_settings({}).postgres)).load()
print(c.entry('staff_contracts','unaizah')['last_cdc'], c.entry('item_barcode','jazan')['last_cdc'])"
```
Expected: both values are the 2026 dates printed in Step 2.

- [ ] **Step 4: After the next scheduled pipeline run, re-verify and DQ-check**

Re-run the Step 3 command.
- Expected: both `last_cdc` values are at or after the Step 2 values, and ≤ now.
- If either is back at `2029-…`, a run overlapped the apply (Review Focus 4). Repeat Step 1 and Step 2.

Then run: `.venv/bin/python dq_check.py --tables STAFF_CONTRACTS,ITEM_BARCODE --branch unaizah,jazan --no-write`

Expected: `staff_contracts/unaizah` `CNT_DELTA` goes from about `+25` to about `0`. Some hash `MISMATCH` rows will **remain**. Those are updates that don't bump `AMEND_LAST_DATE` (cause #1 of the analysis), which this plan does not fix. Don't treat them as a failure of this task.

---

## Part B — delivery_charge insert key

### Task 4: `insert_key_column` / `insert_key_lookback` table config (ETL + GUI)

**Files:**
- Modify: `etl/config.py`. Add the `TableDef` fields after `incremental_cdc_only` (~line 136). Parse and validate in `load_table_defs` (~lines 504-535).
- Modify: `gui/tables_store.py`. Update `KNOWN_KEYS` (~line 94) and `_validate_entry` (~line 138).
- Modify: `gui/templates/tables.html`. Update the save handler, just before `const newCat = editing.category;` (~line 291).
- Test: `tests/test_insert_key.py` (create), `tests/test_tables_store_validation.py` (append)

**Interfaces:**
- Produces: `TableDef.insert_key_column: Optional[str] = None` and `TableDef.insert_key_lookback: int = 0`. Tasks 5, 6, 7 and 8 read these.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_insert_key.py`:

```python
"""Insert-key watermark: a monotonic child key that catches rows inserted under
a parent whose CDC column never moves (DELIVERY_CHARGE under DELIVERY_LINES).
Covers config parse/validation (Task 4) and the query branch (Task 5)."""
from __future__ import annotations

import json

import pytest

from etl.config import load_table_defs


def _dc_entry(**over) -> dict:
    entry = {
        "table": "OASIS.DELIVERY_CHARGE", "unique_key": "DELIVERY_CHARGE_ID",
        "cdc_column": None, "where_date_column": "DELIVERY_DATE",
        "helper": {"table": "OASIS.DELIVERY_LINES",
                   "join": [["DELIVERY_LINE", "DELIVERY_LINE"]],
                   "cdc_column": "AMEND_LAST_DATE", "where_date_column": "AMEND_LAST_DATE"},
        "insert_key_column": "DELIVERY_CHARGE_ID", "insert_key_lookback": 1000,
    }
    entry.update(over)
    return {k: v for k, v in entry.items() if v is not ...}


def _load(tmp_path, entry, category="transactions"):
    p = tmp_path / "tables.json"
    p.write_text(json.dumps({category: [entry]}), encoding="utf-8")
    return load_table_defs(p)


# --- config ------------------------------------------------------------------ #
def test_insert_key_is_parsed(tmp_path):
    (tdef,) = _load(tmp_path, _dc_entry())
    assert tdef.insert_key_column == "DELIVERY_CHARGE_ID"
    assert tdef.insert_key_lookback == 1000


def test_insert_key_defaults_off(tmp_path):
    (tdef,) = _load(tmp_path, _dc_entry(insert_key_column=..., insert_key_lookback=...))
    assert tdef.insert_key_column is None
    assert tdef.insert_key_lookback == 0


@pytest.mark.parametrize("over, category, match", [
    (dict(incremental_cdc_only=True), "transactions", "incremental_cdc_only"),
    (dict(insert_key_column=...), "transactions", "insert_key_lookback"),
    (dict(insert_key_lookback=-1), "transactions", "non-negative"),
    (dict(insert_key_column="ID; DROP TABLE X"), "transactions", "identifier"),
    (dict(helper=...), "transactions", "CDC source"),
    (dict(), "snapshots", "snapshot"),
])
def test_insert_key_rejects_bad_config(tmp_path, over, category, match):
    with pytest.raises(ValueError, match=match):
        _load(tmp_path, _dc_entry(**over), category)
```

Append to `tests/test_tables_store_validation.py`:

```python
# --- insert key (etl insert_key_column / insert_key_lookback) --------------- #
def test_insert_key_ok():
    assert _errs(_doc(insert_key_column="ID", insert_key_lookback=1000)) == []


def test_insert_key_column_must_be_an_identifier():
    assert any("insert_key_column" in e
               for e in _errs(_doc(insert_key_column="ID; DROP TABLE X")))


def test_insert_key_lookback_must_be_non_negative_int_with_a_column():
    assert any("insert_key_lookback" in e for e in _errs(_doc(insert_key_lookback=5)))
    assert any("insert_key_lookback" in e
               for e in _errs(_doc(insert_key_column="ID", insert_key_lookback=-1)))
    assert any("insert_key_lookback" in e
               for e in _errs(_doc(insert_key_column="ID", insert_key_lookback="10")))


def test_insert_key_not_with_cdc_only():
    assert any("insert_key_column" in e for e in
               _errs(_doc(insert_key_column="ID", incremental_cdc_only=True)))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_insert_key.py tests/test_tables_store_validation.py -v`
Expected: the new tests FAIL. For example, `test_insert_key_is_parsed` fails with `AttributeError: 'TableDef' object has no attribute 'insert_key_column'`, and the GUI tests fail with `unknown key 'insert_key_column'` in the error list or an unexpected empty list. The existing tests still PASS.

- [ ] **Step 3: Implement the `TableDef` fields**

In `etl/config.py`, add directly after the `incremental_cdc_only: bool = False` field of `TableDef`:

```python
    # Monotonic child key (e.g. DELIVERY_CHARGE_ID) tracked as a third
    # watermark (``last_key``). It adds a disjoint UNION ALL branch that
    # selects rows whose key passed the mark but that neither the CDC nor the
    # date branch picked -- rows inserted under a parent (helper) row whose CDC
    # column never moved. ``insert_key_lookback`` re-reads that many keys below
    # the mark each run, to absorb sequence values committed out of order.
    insert_key_column: Optional[str] = None
    insert_key_lookback: int = 0
```

- [ ] **Step 4: Parse and validate in `load_table_defs`**

In the `TableDef(...)` constructor call inside `load_table_defs`, add after `incremental_cdc_only=...`:

```python
                insert_key_column=entry.get("insert_key_column") or None,
                insert_key_lookback=int(entry.get("insert_key_lookback") or 0),
```

Then add after the existing `incremental_cdc_only` guard (the `if tdef.incremental_cdc_only and not tdef.cdc_capture_column:` block) and before `defs.append(tdef)`:

```python
            if tdef.insert_key_lookback < 0:
                raise ValueError(
                    f"{tdef.table}: 'insert_key_lookback' must be non-negative")
            if tdef.insert_key_lookback and not tdef.insert_key_column:
                raise ValueError(
                    f"{tdef.table}: 'insert_key_lookback' requires 'insert_key_column'")
            if tdef.insert_key_column:
                if not _IDENT_RE.match(tdef.insert_key_column):
                    raise ValueError(
                        f"{tdef.table}: 'insert_key_column' must be a plain identifier")
                if tdef.is_snapshot:
                    raise ValueError(
                        f"{tdef.table}: 'insert_key_column' does not apply to a snapshot table")
                if tdef.incremental_cdc_only:
                    raise ValueError(
                        f"{tdef.table}: 'insert_key_column' is not supported with "
                        f"'incremental_cdc_only'")
                # The key branch rides the incremental path, which needs CDC.
                if not tdef.cdc_capture_column:
                    raise ValueError(
                        f"{tdef.table}: 'insert_key_column' requires a CDC source "
                        f"('cdc_column' or a helper)")
```

- [ ] **Step 5: GUI validator**

In `gui/tables_store.py`, add `"insert_key_column",` and `"insert_key_lookback",` to `KNOWN_KEYS`. In `_validate_entry`, add directly before the `for k in entry:` unknown-key loop:

```python
    # Mirrors the insert-key checks in etl/config.load_table_defs.
    ik = str(entry.get("insert_key_column") or "").strip()
    lookback = entry.get("insert_key_lookback")
    if ik and not _is_sql_identifier(ik):
        errs.append(f"{name}: 'insert_key_column' must be a valid column identifier")
    if lookback not in (None, ""):
        if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 0:
            errs.append(f"{name}: 'insert_key_lookback' must be a non-negative integer")
        elif lookback and not ik:
            errs.append(f"{name}: 'insert_key_lookback' requires 'insert_key_column'")
    if ik and entry.get("incremental_cdc_only") is True:
        errs.append(f"{name}: 'insert_key_column' is not supported with 'incremental_cdc_only'")
    if ik and category == "snapshots":
        errs.append(f"{name}: 'insert_key_column' does not apply to snapshots")
```

- [ ] **Step 6: GUI form preserves the keys on edit**

In `gui/templates/tables.html`, directly before `const newCat = editing.category;`, add:

```javascript
  // Keys the form has no inputs for: carry them over so an edit never drops them.
  const orig = editing.index >= 0 ? ((DOC[editing.category] || [])[editing.index] || {}) : {};
  for (const k of ["insert_key_column", "insert_key_lookback"]) {
    if (orig[k] !== undefined && orig[k] !== null) e[k] = orig[k];
  }
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_insert_key.py tests/test_tables_store_validation.py tests/test_incremental_cdc_only.py -v`
Expected: all PASS.

- [ ] **Step 8: Manual GUI check (Review Focus 3)**

Temporarily add `"insert_key_column": "DELIVERY_CHARGE_ID", "insert_key_lookback": 1000` to the `OASIS.DELIVERY_CHARGE` entry in a **copy** of `tables.json`, pointed to by the GUI config, or directly on a dev GUI instance.
1. Open the Tables page, edit the delivery_charge row, change nothing, then save.
2. Confirm that both keys are still in the saved JSON.
3. Revert the temporary edit. Task 8 makes the real one.

- [ ] **Step 9: Commit**

```bash
git add etl/config.py gui/tables_store.py gui/templates/tables.html tests/test_insert_key.py tests/test_tables_store_validation.py
git commit -m "feat(config): insert_key_column/insert_key_lookback table settings

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The insert-key query branch

**Files:**
- Modify: `etl/oracle_extract.py`. Add `_insert_key_branch` after `_build_incremental_query` (~line 355). Change `build_query` (~line 221). Pass `key_wm` in `extract_table` (~line 960).
- Test: `tests/test_insert_key.py` (append)

**Interfaces:**
- Consumes: `TableDef.insert_key_column` and `TableDef.insert_key_lookback` (Task 4).
- Produces:
  - `build_query(tdef, settings, cdc_wm, date_wm, key_wm: Optional[Watermark] = None) -> str`
  - `_insert_key_branch(tdef, shape, base, cdc_wm, date_wm, key_wm) -> Optional[str]`
  - `extract_table` reads `watermarks.get("last_key")`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_insert_key.py`:

```python
# --- query branch (Task 5) --------------------------------------------------- #
from etl.config import (  # noqa: E402
    CATEGORY_TRANSACTION, MODE_INCREMENTAL, MODE_INITIAL, HelperJoin, Settings, TableDef,
)
from etl.oracle_extract import Watermark, build_query  # noqa: E402

WM = "2026-09-28 06:05:39.000000"
LIT = "TO_DATE('2026-09-28 06:05:39', 'YYYY-MM-DD HH24:MI:SS')"
HELPER = HelperJoin(table="OASIS.DELIVERY_LINES", join_keys=(("DELIVERY_LINE", "DELIVERY_LINE"),),
                    cdc_column="AMEND_LAST_DATE", where_date_column="AMEND_LAST_DATE")


def _dc(**over) -> TableDef:
    kw = dict(table="OASIS.DELIVERY_CHARGE", unique_key="DELIVERY_CHARGE_ID", cdc_column=None,
              where_date_column="DELIVERY_DATE", where_operator=None,
              where_value_of_initial_run=None, category=CATEGORY_TRANSACTION, helper=HELPER,
              insert_key_column="DELIVERY_CHARGE_ID", insert_key_lookback=1000)
    kw.update(over)
    return TableDef(**kw)


def _q(tdef, key="1505160.0", date=WM, mode=MODE_INCREMENTAL):
    return build_query(
        tdef, Settings(mode=mode),
        Watermark(value=WM, kind="datetime"),
        Watermark(value=date, kind="datetime") if date else Watermark(value=None),
        Watermark(value=key, kind="number") if key else None,
    )


def test_key_branch_is_a_third_disjoint_union_branch():
    parts = _q(_dc()).split("\nUNION ALL\n")
    assert len(parts) == 3
    base = parts[0].split(" WHERE ")[0]
    # LNNVL, not NOT(...): a NULL helper CDC must still qualify (Review Focus 1);
    # the lookback renders as a plain decimal (Review Focus 2).
    assert parts[2] == (
        f"{base} WHERE t.DELIVERY_CHARGE_ID > 1504160.0"
        f" AND LNNVL(h.AMEND_LAST_DATE > {LIT})"
        f" AND LNNVL(h.AMEND_LAST_DATE >= {LIT})"
    )


def test_first_two_branches_are_unchanged_by_the_key():
    with_key = _q(_dc()).split("\nUNION ALL\n")[:2]
    without = _q(_dc(insert_key_column=None, insert_key_lookback=0)).split("\nUNION ALL\n")
    assert with_key == without


def test_no_key_watermark_yet_means_no_key_branch():
    # First run after enabling: the run captures last_key from its own rows.
    assert _q(_dc(), key=None) == _q(_dc(insert_key_column=None, insert_key_lookback=0))


def test_without_a_date_watermark_the_key_branch_excludes_only_cdc_rows():
    parts = _q(_dc(), date=None).split("\nUNION ALL\n")
    assert len(parts) == 2
    assert parts[1].endswith(
        f"WHERE t.DELIVERY_CHARGE_ID > 1504160.0 AND LNNVL(h.AMEND_LAST_DATE > {LIT})")


def test_zero_lookback_uses_the_mark_itself():
    assert "t.DELIVERY_CHARGE_ID > 1505160.0 AND" in _q(_dc(insert_key_lookback=0))


def test_initial_mode_ignores_the_key():
    assert "DELIVERY_CHARGE_ID >" not in _q(_dc(), mode=MODE_INITIAL)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_insert_key.py -v -k "branch or key_watermark or lookback or initial"`
Expected: FAIL with `TypeError: build_query() takes 4 positional arguments but 5 were given`.

- [ ] **Step 3: Implement**

In `etl/oracle_extract.py`, change the `build_query` signature and its incremental block:

```python
def build_query(
    tdef: TableDef,
    settings: Settings,
    cdc_wm: Watermark,
    date_wm: Watermark,
    key_wm: Optional[Watermark] = None,
) -> str:
```

Replace:

```python
    if incremental_ready:
        if tdef.incremental_cdc_only:
            return _build_cdc_only_query(shape, base, cdc_wm)
        return _build_incremental_query(shape, base, cdc_wm, date_wm, ceiling_pred)
```

with:

```python
    if incremental_ready:
        if tdef.incremental_cdc_only:
            return _build_cdc_only_query(shape, base, cdc_wm)
        query = _build_incremental_query(shape, base, cdc_wm, date_wm, ceiling_pred)
        key_branch = _insert_key_branch(
            tdef, shape, base, cdc_wm, date_wm, key_wm or Watermark(value=None))
        return f"{query}\nUNION ALL\n{key_branch}" if key_branch else query
```

Add after `_build_incremental_query`:

```python
def _insert_key_branch(
    tdef: TableDef,
    shape: _QueryShape,
    base: str,
    cdc_wm: Watermark,
    date_wm: Watermark,
    key_wm: Watermark,
) -> Optional[str]:
    """The disjoint third branch for ``TableDef.insert_key_column``.

    Selects rows whose monotonic key passed the ``last_key`` mark (less the
    configured lookback) but that neither other branch selected: rows inserted
    under a parent whose CDC column never moved (a DELIVERY_CHARGE added to an
    already-loaded DELIVERY_LINE). ``LNNVL(p)`` is TRUE when ``p`` is FALSE *or
    UNKNOWN*, so it is the exact complement of each branch's predicate: a row
    with a NULL helper CDC -- selected by neither branch -- still qualifies,
    which ``NOT (p)`` would drop. Keeps UNION ALL duplicate-free, like the
    other two branches.
    """
    if not tdef.insert_key_column or key_wm.value is None or shape.cdc_ref is None:
        return None
    floor = Decimal(str(key_wm.value)) - tdef.insert_key_lookback
    preds = [
        f"t.{tdef.insert_key_column} > {format(floor, 'f')}",
        f"LNNVL({shape.cdc_ref} > {format_watermark(cdc_wm)})",
    ]
    if shape.date_ref is not None and date_wm.value is not None:
        preds.append(f"LNNVL({shape.date_ref} >= {format_watermark(date_wm)})")
    return f"{base} WHERE " + " AND ".join(preds)
```

(`Decimal` is already imported. `_column_max_watermark` uses it.)

In `extract_table`, change:

```python
    query = build_query(tdef, settings, cdc_wm, date_wm)
```

to:

```python
    key_wm = Watermark.from_dict(watermarks.get("last_key"))
    query = build_query(tdef, settings, cdc_wm, date_wm, key_wm)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_insert_key.py tests/test_incremental_cdc_only.py tests/test_query_sources.py tests/test_plan_no_cdc_fallback.py -v`
Expected: all PASS.

- [ ] **Step 5: Validate the SQL on Oracle (read-only, one branch)**

Run the generated query as a `COUNT(*)` against Muhayil, to prove it parses on 11g and that the branches are disjoint:

```bash
.venv/bin/python - <<'EOF'
import sys; sys.path.insert(0, '.')
from pathlib import Path
from etl import config
from etl.oracle_extract import Watermark, build_query, ensure_oracle_client
from etl.iceberg_load import ControlStore
from etl.metastore import MetaStore
import dataclasses, oracledb
s = config.load_settings({})
ensure_oracle_client(s)
br = config.load_branches()["Muhayil"]
t = next(t for t in config.load_table_defs(Path("tables.json")) if t.dataset_table_name == "delivery_charge")
t = dataclasses.replace(t, insert_key_column="DELIVERY_CHARGE_ID", insert_key_lookback=1000)
e = ControlStore(MetaStore(s.postgres)).load().entry("delivery_charge", "Muhayil")
key = Watermark(value="1490000", kind="number")
q = build_query(t, s, Watermark.from_dict(e["last_cdc"]), Watermark.from_dict(e["last_date"]), key)
conn = oracledb.connect(user=br.username, password=br.password, dsn=br.dsn(s.dsn_mode))
cur = conn.cursor()
cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT DELIVERY_CHARGE_ID) FROM ({q})")
print(cur.fetchone())
EOF
```

Expected: one row `(n, n)` with both numbers **equal**, which proves no duplicates across branches. `n` should be in the low thousands (the charges that DQ found missing, plus the lookback). An `ORA-` error means the SQL must be fixed before continuing.

- [ ] **Step 6: Commit**

```bash
git add etl/oracle_extract.py tests/test_insert_key.py
git commit -m "feat(etl): insert-key UNION ALL branch for rows no CDC branch sees

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Capture and persist the `last_key` watermark

**Files:**
- Modify: `etl/metastore.py:24-34` (`_control_state_table`)
- Modify: `etl/oracle_extract.py`. Update `ExtractResult` (~line 85). Add `_key_watermark_from_parquet` after `_watermarks_from_parquet` (~line 946). Update `extract_table` (~line 973).
- Modify: `etl/iceberg_load.py`. Update `ControlStore.load` / `advance` / `save` (~lines 118-170).
- Test: `tests/test_insert_key_capture.py`

**Interfaces:**
- Consumes: `build_query(..., key_wm)` and `extract_table` reading `watermarks["last_key"]` (Task 5).
- Produces:
  - `ExtractResult.new_key: Watermark`
  - `_key_watermark_from_parquet(path: Path, tdef: TableDef) -> Watermark`
  - Control entries carry `"last_key": {"value", "kind"} | None`.
  - `control_state` gains the `last_key_value` and `last_key_kind` columns.

- [ ] **Step 1: Write the failing test**

Create `tests/test_insert_key_capture.py`:

```python
"""last_key: captured from the staged parquet, advanced, and persisted."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from etl import oracle_extract
from etl.config import CATEGORY_TRANSACTION, MODE_INCREMENTAL, BranchConfig, HelperJoin, Settings, TableDef
from etl.iceberg_load import ControlStore
from etl.oracle_extract import ExtractResult, Watermark, _key_watermark_from_parquet, extract_table

HELPER = HelperJoin(table="OASIS.DELIVERY_LINES", join_keys=(("DELIVERY_LINE", "DELIVERY_LINE"),),
                    cdc_column="AMEND_LAST_DATE", where_date_column="AMEND_LAST_DATE")


def _dc(key="DELIVERY_CHARGE_ID") -> TableDef:
    return TableDef(table="OASIS.DELIVERY_CHARGE", unique_key="DELIVERY_CHARGE_ID",
                    cdc_column=None, where_date_column="DELIVERY_DATE", where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_TRANSACTION,
                    helper=HELPER, insert_key_column=key, insert_key_lookback=1000 if key else 0)


def _staged(tmp_path, ids):
    path = tmp_path / "Muhayil.parquet"
    # Oracle NUMBER arrives as Arrow double.
    pq.write_table(pa.table({"DELIVERY_CHARGE_ID": pa.array(ids, pa.float64())}), path)
    return path


def test_key_mark_is_the_max_of_the_staged_key(tmp_path):
    wm = _key_watermark_from_parquet(_staged(tmp_path, [1492547.0, 1505160.0]), _dc())
    assert (wm.value, wm.kind) == ("1505160.0", "number")


def test_no_insert_key_or_missing_file_gives_no_mark(tmp_path):
    assert _key_watermark_from_parquet(_staged(tmp_path, [1.0]), _dc(key=None)).value is None
    assert _key_watermark_from_parquet(tmp_path / "absent.parquet", _dc()).value is None


class _Conn:
    def close(self):
        pass


class _Pool:
    def acquire(self):
        return _Conn()


def test_extract_table_uses_and_captures_last_key(tmp_path, monkeypatch):
    staged = _staged(tmp_path, [1505160.0])
    seen = {}

    def fake_fetch(conn, query, *a, **k):
        seen["query"] = query
        return 1, None, staged

    monkeypatch.setattr(oracle_extract, "fetch_and_stage", fake_fetch)
    branch = BranchConfig(key="Muhayil", name="m", id=9, host="h", port=1,
                          username="u", password="p", database="d")
    wms = {"last_cdc": {"value": "2026-09-28 05:27:09.000000", "kind": "datetime"},
           "last_date": {"value": "2026-09-28 05:27:09.000000", "kind": "datetime"},
           "last_key": {"value": "1500000.0", "kind": "number"}}
    res = extract_table(_Pool(), branch, _dc(), Settings(mode=MODE_INCREMENTAL), wms)
    assert "t.DELIVERY_CHARGE_ID > 1499000.0" in seen["query"]
    assert res.new_key.value == "1505160.0"


class _FakeMeta:
    def __init__(self, rows=None):
        self.rows = rows or []

    def ensure_schema(self):
        pass

    def read_control_state(self):
        return self.rows

    def upsert_control_state(self, rows):
        self.rows = rows


def test_control_store_round_trips_last_key():
    meta = _FakeMeta()
    cs = ControlStore(meta).load()
    cs.advance(ExtractResult(table_def=_dc(), branch="Muhayil", branch_id=9, status="SUCCESS",
                             new_key=Watermark(value="1505160.0", kind="number")))
    cs.save()
    (row,) = meta.rows
    assert (row["last_key_value"], row["last_key_kind"]) == ("1505160.0", "number")
    again = ControlStore(meta).load()
    assert again.entry("delivery_charge", "Muhayil")["last_key"] == {
        "value": "1505160.0", "kind": "number"}


def test_control_store_loads_rows_written_before_the_column_existed():
    # Older deployments' rows have NULL last_key_* -> no key mark.
    meta = _FakeMeta([{"table_name": "doc", "branch_id": "abha", "last_cdc_value": None,
                       "last_cdc_kind": None, "last_date_value": None, "last_date_kind": None,
                       "last_key_value": None, "last_key_kind": None, "status": "SUCCESS",
                       "row_count": 1, "duration_ms": 1, "last_run_at": None}])
    assert ControlStore(meta).load().entry("doc", "abha")["last_key"] is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_insert_key_capture.py -v`
Expected: collection error, `ImportError: cannot import name '_key_watermark_from_parquet'`.

- [ ] **Step 3: Implement the metastore columns**

In `etl/metastore.py` `_control_state_table`, add after the `last_date_value` / `last_date_kind` line:

```python
        Column("last_key_value", String), Column("last_key_kind", String),
```

(`MetaStore.ensure_schema` → `_add_missing_columns` adds these to the existing production table as NULL columns. No manual DDL is needed.)

- [ ] **Step 4: Implement the capture in `oracle_extract.py`**

In `ExtractResult`, add after `new_date`:

```python
    new_key: Watermark = field(default_factory=Watermark)
```

Add after `_watermarks_from_parquet`:

```python
def _key_watermark_from_parquet(path: Path, tdef: TableDef) -> Watermark:
    """Max of ``tdef.insert_key_column`` in the staged parquet (the ``last_key`` mark)."""
    col = tdef.insert_key_column
    if not col:
        return Watermark(value=None)
    try:
        if col not in pq.read_schema(path).names:
            return Watermark(value=None)
        return _column_max_watermark(pq.read_table(path, columns=[col]), col)
    except Exception:  # noqa: BLE001 - watermark read is best-effort
        return Watermark(value=None)
```

In `extract_table`, directly after `result.new_cdc, result.new_date = _watermarks_from_parquet(staged_path, tdef)`, add:

```python
            result.new_key = _key_watermark_from_parquet(staged_path, tdef)
```

- [ ] **Step 5: Implement persistence in `ControlStore`**

In `ControlStore.load`, add to the per-branch dict, after the `"last_date": ...` entry:

```python
                "last_key": ({"value": r["last_key_value"], "kind": r["last_key_kind"]}
                             if r.get("last_key_value") is not None else None),
```

In `ControlStore.advance`, after the `cur["last_date"] = ...` line:

```python
            cur["last_key"] = _wm_advance(cur.get("last_key"), result.new_key)
```

In `ControlStore.save`, next to the `date = info.get("last_date") or {}` line, add `key = info.get("last_key") or {}`. Add these to the row dict after the `last_date_*` pair:

```python
                        "last_key_value": key.get("value"), "last_key_kind": key.get("kind"),
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_insert_key_capture.py tests/test_insert_key.py tests/test_control_store_threading.py tests/test_control_store_pg.py tests/test_metastore.py tests/test_wm_advance.py -v`
Expected: all PASS. The Postgres tests skip unless `OASIS_TEST_PG_DSN` is set. If a throwaway test Postgres is available, set it and confirm `test_control_store_pg.py` and `test_metastore.py` pass with the new columns.

- [ ] **Step 7: Run the full suite**

Run: `.venv/bin/python -m pytest -q`
Expected: no new failures compared with a run of `git stash; .venv/bin/python -m pytest -q; git stash pop` on the pre-plan tree.

- [ ] **Step 8: Commit**

```bash
git add etl/metastore.py etl/oracle_extract.py etl/iceberg_load.py tests/test_insert_key_capture.py
git commit -m "feat(etl): capture and persist the last_key insert-key watermark

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: `seed-key` CLI subcommand (backfill starting point)

When a table first gets an insert key, the first run has no `last_key`, so it captures one from its own rows. From then on, new late-added charges are caught, but the ones **already** missing are not. Seeding `last_key` to the lake's smallest key on or after a chosen date makes the next run re-read every charge inserted since that date that the lake lacks.

**Files:**
- Modify: `etl/watermark_repair.py` (append)
- Modify: `maintenance/repair_watermarks.py` (add the subcommand)
- Test: `tests/test_watermark_repair.py` (append)

**Interfaces:**
- Consumes: `TableDef.insert_key_column` (Task 4), the `last_key` entry key (Task 6), and `_as_naive`, `lake_column`, `apply_marks` (Task 2).
- Produces:
  - `min_key_since(keys: pa.ChunkedArray | pa.Array, dates: pa.ChunkedArray | pa.Array, since: dt.datetime) -> Optional[str]`
  - `seed_key_mark(value: str) -> dict`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_watermark_repair.py`:

```python
# --- insert-key seeding (Task 7) --------------------------------------------- #
from etl.watermark_repair import min_key_since, seed_key_mark  # noqa: E402


def test_min_key_since_uses_wall_clock_dates_and_ignores_nulls():
    keys = pa.array([1490400.0, 1492547.0, 1505159.0, None], pa.float64())
    dates = pa.array([dt.datetime(2026, 8, 31, 23, 0), dt.datetime(2026, 9, 2, 21, 30),
                      dt.datetime(2026, 9, 1, 19, 48), dt.datetime(2026, 9, 5)],
                     pa.timestamp("us", tz="UTC"))
    assert min_key_since(keys, dates, dt.datetime(2026, 9, 1)) == "1492547.0"


def test_min_key_since_none_when_nothing_qualifies():
    keys = pa.array([1.0], pa.float64())
    dates = pa.array([dt.datetime(2026, 1, 1)], pa.timestamp("us"))
    assert min_key_since(keys, dates, dt.datetime(2026, 9, 1)) is None


def test_seed_key_mark_is_a_number_watermark():
    assert seed_key_mark("1492547.0") == {"value": "1492547.0", "kind": "number"}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_watermark_repair.py -v`
Expected: collection error, `ImportError: cannot import name 'min_key_since'`.

- [ ] **Step 3: Implement**

Append to `etl/watermark_repair.py`:

```python
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
```

In `maintenance/repair_watermarks.py`, add the command function after `cmd_future`:

```python
def cmd_seed_key(args) -> int:
    import datetime as dt

    settings, branches, tdefs, store = _context()
    tdef = tdefs.get(args.table)
    if tdef is None or not tdef.insert_key_column:
        raise SystemExit(f"{args.table}: no 'insert_key_column' in tables.json")
    if not tdef.where_date_column:
        raise SystemExit(f"{args.table}: needs a 'where_date_column' to pick the floor by date")
    since = dt.datetime.strptime(args.since, "%Y-%m-%d")
    wanted = args.branch or sorted(branches)
    from etl.dq_check import _norm

    updates = []
    for b in wanted:
        tbl = wr.lake_column(settings, branches, args.table, b,
                             [tdef.insert_key_column, tdef.where_date_column])
        floor = (wr.min_key_since(tbl.column(_norm(tdef.insert_key_column)),
                                  tbl.column(_norm(tdef.where_date_column)), since)
                 if tbl is not None and tbl.num_rows else None)
        current = (store.entry(args.table, b).get("last_key") or {}).get("value")
        print(f"{args.table:20s} {b:9s} last_key {str(current):14s} -> {floor or 'skip: no lake rows'}")
        if floor:
            updates.append((args.table, b, "last_key", wr.seed_key_mark(floor)))

    if not args.apply:
        print(f"\ndry run: {len(updates)} branch(es) would be seeded; re-run with --apply")
        return 0
    wr.apply_marks(store, updates)
    print(f"\nseeded {len(updates)} branch(es)")
    return 0
```

In `parse_args`, add after the `future` subparser:

```python
    s = sub.add_parser("seed-key", help="seed last_key to the lake's first key since a date")
    s.add_argument("-t", "--table", required=True, help="dataset table name, e.g. delivery_charge")
    s.add_argument("--since", required=True, metavar="YYYY-MM-DD",
                   help="re-read every row inserted since this date that the lake lacks")
    s.add_argument("--branch", action="append", help="restrict to these branches (repeatable)")
    s.add_argument("--apply", action="store_true", help="write (default: dry run)")
    s.set_defaults(func=cmd_seed_key)
```

Update the module docstring's usage block with:

```
    python maintenance/repair_watermarks.py seed-key -t delivery_charge --since 2026-09-01
    python maintenance/repair_watermarks.py seed-key -t delivery_charge --since 2026-09-01 \\
        --branch Muhayil --apply
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_watermark_repair.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add etl/watermark_repair.py maintenance/repair_watermarks.py tests/test_watermark_repair.py
git commit -m "feat(maintenance): seed-key subcommand to backfill insert-key tables

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Enable on delivery_charge and backfill (operational)

**Files:**
- Modify: `tables.json`, the `OASIS.DELIVERY_CHARGE` entry in `transactions`.

Merge cost is the risk here. Past profiling found that large merges time out on pyiceberg's high-cardinality `In` predicate (memory `iceberg-merge-bottleneck`). Jazan alone is missing about 68k charges month-to-date, so backfill **one or two branches per run**, smallest first.

- [ ] **Step 1: Enable the key**

In `tables.json`, add these two keys to the `OASIS.DELIVERY_CHARGE` entry:

```json
      "insert_key_column": "DELIVERY_CHARGE_ID",
      "insert_key_lookback": 1000,
```

Run: `.venv/bin/python -c "import sys; sys.path.insert(0,'.'); from pathlib import Path; from etl.config import load_table_defs; t=[d for d in load_table_defs(Path('tables.json')) if d.dataset_table_name=='delivery_charge'][0]; print(t.insert_key_column, t.insert_key_lookback)"`
Expected: `DELIVERY_CHARGE_ID 1000`

Commit:

```bash
git add tables.json
git commit -m "chore(tables): track DELIVERY_CHARGE_ID as delivery_charge's insert key

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 2: Dry-run the seed for every branch**

Run: `.venv/bin/python maintenance/repair_watermarks.py seed-key -t delivery_charge --since 2026-09-01`
Expected: 8 lines, `delivery_charge <branch> last_key None -> <id>.0`, then `dry run: 8 branch(es) would be seeded`. Show this output to the user before applying.

- [ ] **Step 3: Seed the smallest branch first**

Confirm no pipeline run is in flight (as in Task 3 Step 1), then run:
`.venv/bin/python maintenance/repair_watermarks.py seed-key -t delivery_charge --since 2026-09-01 --branch Muhayil --apply`
Expected: `seeded 1 branch(es)`.

The other branches get no seed. Their first run captures `last_key` from their own rows, which catches future late charges but no backfill. They are seeded in Step 5.

- [ ] **Step 4: After the next run, verify Muhayil**

1. Re-read `control_state` for `delivery_charge/Muhayil`: `last_key` should now be ≥ the seeded value (the max key of that run). If it is `None` or the seeded value exactly, the run overlapped the apply or didn't pick up the change (Review Focus 4). Investigate before going further.
2. Check `run_logs/` for the `delivery_charge` load of that run. It must be `SUCCESS` with no merge timeout.
3. Run: `.venv/bin/python dq_check.py --tables DELIVERY_CHARGE --branch Muhayil --no-write`

Expected: `ONLY_ORA` drops from about 2,137 to about 0. `MISMATCH` rows (the `crd_*`, `cancel_flag` and `refund_type` updates on existing charges) **remain**, because charge updates don't move the line's CDC. That is cause #1, outside this plan.

- [ ] **Step 5: Seed the remaining branches in batches**

Seed in this order, one pipeline run apart, repeating the Step 4 checks after each:
1. `ghirnata` + `abha`
2. `unaizah` + `madinah`
3. `khamis`
4. `jazan`
5. `alrabwah`

Each batch uses `--branch <a> --branch <b> --apply`. If any run's delivery_charge merge times out, stop. Report it to the user rather than seeding more branches.

---

## Self-review notes

- **Spec coverage.**
  - Fix 1, part one ("make `_wm_advance` refuse stored future values"): Task 1.
  - Fix 1, part two ("reset staff_contracts/unaizah and item_barcode/jazan"): Tasks 2 and 3.
  - Fix 3 ("insert watermark on DELIVERY_CHARGE_ID in addition to the helper join"): Tasks 4–6 cover config, query and persistence. Task 7 plus Task 8 cover backfilling the charges already missing.
  - Future `last_date` marks are only reported by the dry run. Task 1 heals them on their next run. That is intentional, because the analysis rated them low impact.
- **Not in scope:** causes #1 (updates without a CDC bump) and #2 (hard deletes). The Task 3 and Task 8 verification steps say explicitly that their `MISMATCH` rows remain.
