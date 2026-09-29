# C1 Rolling Re-sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Catch Oracle rows that change without `AMEND_LAST_DATE` moving (root cause C1) by re-reading a window of recent rows every run, while committing only rows whose content actually changed.

**Architecture:** Two parts.
- **(a) Merge change detection.** `_rows_to_update` (etl/iceberg_load.py) stops treating the ETL stamp `recorded_updated_at` as a content change. Re-read rows that are otherwise identical are then elided by the existing no-change logic: no file rewrite, no new stamp. So the window costs reads, not writes.
- **(b) One unified re-read window** for all tables, `[etl] resync_days` (default **60**, 0 disables), editable on the GUI Settings page. For every table on the incremental path that has a date column and a CDC column, it adds a disjoint `UNION ALL` branch selecting rows dated in `[TRUNC(SYSDATE) - N, date watermark)` that no other branch took.

**Tech Stack:** Python 3.12, python-oracledb (Oracle 11g), pyarrow, pyiceberg, dlt, pytest. Run from `/home/bi/workspace/dlt` with `.venv/bin/python`.

**Spec:** the C1 analysis of 2026-09-29 (Background), plus the user's decision on 2026-09-29: "unified window 60 days for all tables configurable from the UI". This replaces the per-table `resync_days` of the first draft.

## Background (measured 2026-09-29, madinah, 250 days, Oracle vs lake row by row)

For rows the lake copied at age *x* days (x = `Recorded_updated_at` − the row's date), this is the share silently changed since the copy, among rows that had ≥30 days to change. A window of N days leaves about share(N) of rows stale.

| table | rows/day | x=0 | 1 | 3 | 7 | 14 | 21 | 30 | 45 | 60 |
|---|---|---|---|---|---|---|---|---|---|---|
| admission_request | 21 | 11.3% | 21.0% | 3.5% | 0.0% | 0.0% | 0.0% | 0.0% | 0.4% | 0.2% |
| orders_master | 2,107 | 12.0% | 12.5% | 4.5% | 0.4% | 0.4% | 0.0% | 0.0% | 0.0% | 0.0% |
| patient_eligibility | 379 | 10.6% | 6.2% | 3.0% | 1.7% | 1.1% | 0.3% | 0.1% | 0.1% | 0.0% |
| authorisations_master | 367 | 6.3% | 5.1% | 1.9% | 1.4% | 1.5% | 0.5% | 0.0% | 0.8% | 0.0% |
| ar_episode_invoices | 310 | 97.5% | 87.6% | 66.3% | 71.1% | 68.0% | 54.8% | 12.7% | 0.0% | 0.0% |
| order_lines | 5,541 | 19.1% | 7.5% | 2.6% | 1.8% | 1.5% | 0.3% | 0.2% | 0.1% | 0.0% |

The longest measured tail is 45 days (ar_episode_invoices), so a unified 60-day window covers every measured table.

**Cost:** every eligible table re-reads 60 days of rows per branch per run. That's about 330k rows per branch for order_lines alone. Task 4 measures the total before rollout.

**Coverage:** masters (no date column), `incremental_cdc_only` tables (appointments), snapshots, and tables without a CDC column (account_transactions) are not covered by a date window.

**patient_emergency_visit** stays flat at about 30% at every age. That comes from a bulk job clearing `INT_SENT` on visits of any age, which no window can catch, so it is out of scope.

**Why part (a) comes first.** Every staged row carries `recorded_updated_at = now` (oracle_extract.py:687), and `_rows_to_update` compares every non-key column. So every re-read row counts as changed today, and its file gets rewritten. Without (a), the window turns into mass rewrites.

**Downstream.** The dbt staging models read `recorded_updated_at > max(...)` into `ReplacingMergeTree(recorded_updated_at)`. After (a), that column means "last time the content changed", so dbt stops re-appending unchanged rows. dbt needs no edit.

## Global Constraints

- Oracle 11g SQL only; `LNNVL` is available.
- The incremental branches must stay pairwise disjoint (no duplicate keys in the delta).
- A `last_date` watermark of kind `number` is a Julian day, so the window's lower bound must match that type.
- `.dlt/config.toml` is live, git-ignored config with no `resync_days` line on production. The GUI must show the default when the key is absent, and write the key on first save.
- Work on a feature branch in a worktree. Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A re-read row identical apart from `recorded_updated_at`:** nothing is written. (Task 1.)
2. **A re-read row that really changed:** it's written with the new stamp. (Task 1.)
3. **A row selected by both the insert-key branch and the window:** it appears exactly once. (Task 3 test plus the Oracle probe.)
4. **A Julian (number) date watermark:** the window bound is a Julian number. (Task 3.)
5. **`resync_days` absent from config.toml:** the Settings page shows 60 and saving it adds the line. It must not error with "Key(s) not found". (Task 2.)

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `etl/iceberg_load.py` | Modify | `_rows_to_update` / `_upsert_in_memory_lookup` `ignore_cols`, and the call site in `_merge_iceberg_single_commit` (Task 1) |
| `etl/config.py` | Modify | `Settings.resync_days` (default 60) loaded from `[etl] resync_days`, validated ≥ 0 (Task 2) |
| `gui/workspace.py` | Modify | Allowlist; default shown when absent; insert a missing allowlisted key on save (Task 2) |
| `gui/templates/settings.html` | Modify | Curated, editable, numeric "Re-read window" row (Task 2) |
| `etl/oracle_extract.py` | Modify | `_insert_key_floor`, `_resync_branch`, and `build_query` assembly (Task 3) |
| tests | Modify/Create | `test_rows_to_update.py`, `test_merge_in_memory_lookup.py`, `test_merge_single_commit.py`, `test_resync.py`, `test_settings_ui_keys.py` |

---

### Task 1: Merge ignores `recorded_updated_at` when deciding a row changed

**Files:**
- Modify: `etl/iceberg_load.py` (`_rows_to_update` ~1713, `_upsert_in_memory_lookup` ~1790, call at ~2123)
- Test: `tests/test_rows_to_update.py`, `tests/test_merge_in_memory_lookup.py`

**Interfaces:**
- Produces:
  - `_rows_to_update(source, target, join_col, ignore_cols: frozenset = frozenset()) -> pa.Table`
  - `_upsert_in_memory_lookup(table, data, join_col, update_matched, label="", ignore_cols: frozenset = frozenset())`
  - `_merge_iceberg_single_commit` passes `ignore_cols={<normalized Settings().recorded_ts_column>}`.

- [ ] **Step 1: Write the failing tests.** Append to `tests/test_rows_to_update.py`:

```python
# --- ETL stamp columns are not content (C1 rolling re-sync) ------------------ #
def test_ignored_column_alone_is_not_a_change():
    source = pa.table({KEY: [1, 2], "v": ["a", "b"], "recorded_updated_at": [9, 9]})
    target = pa.table({KEY: [1, 2], "v": ["a", "b"], "recorded_updated_at": [1, 1]})
    out = _rows_to_update(source, target, KEY, ignore_cols=frozenset({"recorded_updated_at"}))
    assert out.num_rows == 0


def test_real_change_is_returned_with_the_new_stamp():
    source = pa.table({KEY: [1, 2], "v": ["a", "CHANGED"], "recorded_updated_at": [9, 9]})
    target = pa.table({KEY: [1, 2], "v": ["a", "b"], "recorded_updated_at": [1, 1]})
    out = _rows_to_update(source, target, KEY, ignore_cols=frozenset({"recorded_updated_at"}))
    assert out.to_pylist() == [{KEY: 2, "v": "CHANGED", "recorded_updated_at": 9}]


def test_default_still_compares_every_column():
    source = pa.table({KEY: [1], "v": ["a"], "recorded_updated_at": [9]})
    target = pa.table({KEY: [1], "v": ["a"], "recorded_updated_at": [1]})
    assert _rows_to_update(source, target, KEY).num_rows == 1


def test_only_ignored_columns_besides_the_key_means_nothing_to_compare():
    source = pa.table({KEY: [1], "recorded_updated_at": [9]})
    target = pa.table({KEY: [1], "recorded_updated_at": [1]})
    assert _rows_to_update(source, target, KEY,
                           ignore_cols=frozenset({"recorded_updated_at"})).num_rows == 0
```

Append to `tests/test_merge_in_memory_lookup.py`:

```python
def _stamped(ids, names, stamp):
    t = _rows(ids, names)
    return t.append_column("recorded_updated_at", pa.array([stamp] * len(ids), pa.int64()))


def test_reread_unchanged_rows_commit_nothing_and_keep_their_stamp(tmp_path):
    t = _seed(tmp_path, "stamp", _stamped([0, 1], ["a", "b"], 1))
    before = len(list(t.metadata.snapshots))
    _upsert_in_memory_lookup(t, _stamped([0, 1], ["a", "b"], 2), HASH, update_matched=True,
                             ignore_cols=frozenset({"recorded_updated_at"}))
    t.refresh()
    assert len(list(t.metadata.snapshots)) == before
    assert set(t.scan().to_arrow().column("recorded_updated_at").to_pylist()) == {1}


def test_reread_changed_row_is_written_with_the_new_stamp(tmp_path):
    t = _seed(tmp_path, "stamp2", _stamped([0, 1], ["a", "b"], 1))
    _upsert_in_memory_lookup(t, _stamped([0, 1], ["a", "CHANGED"], 2), HASH, update_matched=True,
                             ignore_cols=frozenset({"recorded_updated_at"}))
    t.refresh()
    got = {r["id"]: (r["name"], r["recorded_updated_at"]) for r in t.scan().to_arrow().to_pylist()}
    assert got == {0: ("a", 1), 1: ("CHANGED", 2)}
```

Append a call-site test to `tests/test_merge_single_commit.py` (read that file first for its fixtures). It should monkeypatch `etl.iceberg_load._upsert_in_memory_lookup` with a recorder, run `_merge_iceberg_single_commit` on a hash-ready table, and assert `kwargs["ignore_cols"] == frozenset({"recorded_updated_at"})`.

- [ ] **Step 2: Run to verify RED.** Run `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_rows_to_update.py tests/test_merge_in_memory_lookup.py tests/test_merge_single_commit.py`. Expected: the new tests fail with `TypeError: ... unexpected keyword argument 'ignore_cols'` and the existing tests pass.

- [ ] **Step 3: Implement.**
  - In `_rows_to_update`, add the parameter `ignore_cols: frozenset = frozenset()`. Change `non_key = [c for c in source.column_names if c != join_col]` to `non_key = [c for c in source.column_names if c != join_col and c not in ignore_cols]`, and extend the docstring: "ignore_cols: ETL stamp columns (recorded_updated_at) that differ on every re-read and are not content."
  - In `_upsert_in_memory_lookup`, add the same parameter and pass it through to `_rows_to_update(src_subset, matched_rows, join_col, ignore_cols)`.
  - In `_merge_iceberg_single_commit`, define `stamp_col = SnakeCaseNamingConvention().normalize_identifier(Settings().recorded_ts_column)`, importing `from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention`. Pass `ignore_cols=frozenset({stamp_col})` to `_upsert_in_memory_lookup`. Leave the composite `table.upsert` path unchanged (Review Focus 5).

- [ ] **Step 4: Run to verify GREEN**, with the same command. Expected: all pass. Then run the full suite (`.venv/bin/python -m pytest -q -p no:cacheprovider`). Expected: only the pre-existing `test_iceberg_expire[a\\b]` failure.

- [ ] **Step 5: Commit.** Message: `fix(load): re-read rows identical but for recorded_updated_at are not rewritten`.

---


### Task 2: `[etl] resync_days` setting (ETL + GUI Settings page)

**Files:** `etl/config.py`, `gui/workspace.py`, `gui/templates/settings.html`. Tests: `tests/test_resync.py`, `tests/test_settings_ui_keys.py`.

**Interfaces:**
- Produces:
  - `Settings.resync_days: int = 60`
  - `load_settings()` reads `etl.resync_days`
  - `workspace.ETL_KEY_DEFAULTS = {"resync_days": 60}`
  - `_update_toml_block` inserts an allowlisted key that is missing from the block.

- [ ] **Step 1: Failing tests.**
  - **`tests/test_resync.py`:**
    - `Settings().resync_days == 60`.
    - `load_settings` reads `resync_days = 14` from `[etl]`: monkeypatch `etl.config._cfg` to return 14 for `etl.resync_days` and the default otherwise.
    - A negative value raises `ValueError` matching `"resync_days"`.
    - A non-integer (`"abc"`, `1.5`, `True`) raises `ValueError` matching `"resync_days"`.
  - **`tests/test_settings_ui_keys.py`:**
    - Add `"resync_days"` to `UI_EDITABLE`, and to the curated-row test tuple.
    - Add `test_missing_key_shows_default_and_is_inserted_on_save`. Start from a config with `[etl]\nload_workers = 2\n[destination.filesystem]\nbucket_url = "x"\n`:
      - `workspace.etl_settings()["resync_days"] == 60` (the default, shown when absent).
      - `update_etl_settings({"resync_days": 30})` applies it.
      - The file then contains `resync_days = 30` inside `[etl]`, before `[destination.filesystem]`, and still parses.
    - Add `test_unknown_key_still_rejected`: `update_etl_settings({"nope": 1})` raises "Not editable".

- [ ] **Step 2: RED.** Run `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_resync.py tests/test_settings_ui_keys.py`. Expected: the new tests fail.

- [ ] **Step 3: Implement.**
  - **`Settings`:** add `resync_days: int = 60`, commented "Re-read rows dated within the last N days every incremental run (0 disables) to catch rows Oracle changes without moving the CDC column; the merge commits only rows whose content changed."
  - **`load_settings`:**
    - Read `raw = _cfg("etl.resync_days", 60)`.
    - If it's a bool or not an int (`isinstance(raw, bool) or not isinstance(raw, int)`), raise `ValueError("[etl] resync_days must be a whole number of days (0 disables)")`. If it's `< 0`, raise the same message.
    - Pass `resync_days=raw`.
    - Check how `_cfg` returns TOML ints (they come back as `int`), and keep `int(...)` coercion consistent with the neighbours if `_cfg` can return strings (from an env override). Accept a digit string by converting it, and reject everything else.
  - **`workspace.py`:**
    - Add `"resync_days"` to `EDITABLE_ETL_KEYS`, and `ETL_KEY_DEFAULTS = {"resync_days": 60}`.
    - `etl_settings()` does `for k, v in ETL_KEY_DEFAULTS.items(): etl.setdefault(k, v)`.
    - In `_update_toml_block`, for each allowlisted key still missing after the scan, insert `f"{key} = {_fmt_toml_scalar('0', value)}"` right after the last key/value line of the `[section]` block. Error only if the section itself is absent. `_fmt_toml_scalar` formats by the old raw literal, so pass a representative literal for the type: `'0'` for ints, `'true'` for bools.
  - **`settings.html`:**
    - Add `"resync_days"` to `SET_EDITABLE` and `SET_NUMERIC`.
    - Add a group `{ icon: "fa-arrows-rotate", title: "Change capture", blurb: "Catch rows Oracle changes without updating AMEND_LAST_DATE.", keys: [ { k: "resync_days", label: "Re-read window", unit: "days", help: "Every run re-reads rows dated within the last N days (tables with a date and a CDC column) and commits only rows whose content changed. 0 disables. Longest measured edit tail: 45 days." } ] }` before "Data quality".
    - Syntax-check the JS with esprima.

- [ ] **Step 4: GREEN.** Run the same tests plus `tests/test_config_constants.py` and the full suite. Expected: only the pre-existing failure.

- [ ] **Step 5: Commit.** Message: `feat(config): unified [etl] resync_days re-read window (default 60), editable in Settings`.

---

### Task 3: The re-read query branch

**Files:** `etl/oracle_extract.py`. Test: `tests/test_resync.py`.

**Interfaces:**
- Consumes `Settings.resync_days` (Task 2).
- Produces:
  - `_insert_key_floor(tdef, key_wm) -> Optional[str]`
  - `_resync_branch(days: int, tdef, shape, base, cdc_wm, date_wm, key_wm) -> Optional[str]`

- [ ] **Step 1: Failing tests.** Append to `tests/test_resync.py`:

```python
from etl.config import CATEGORY_MASTER, CATEGORY_TRANSACTION, MODE_INCREMENTAL, MODE_INITIAL, HelperJoin, Settings, TableDef  # noqa: E402
from etl.oracle_extract import Watermark, build_query  # noqa: E402

WM = "2026-09-28 06:05:39.000000"
LIT = "TO_DATE('2026-09-28 06:05:39', 'YYYY-MM-DD HH24:MI:SS')"


def _ol(**over):
    kw = dict(table="OASIS.ORDER_LINES", unique_key="ORDER_LINE", cdc_column="AMEND_LAST_DATE",
              where_date_column="CREATION_DATE", where_operator=">=",
              where_value_of_initial_run="2022-01-01", category=CATEGORY_TRANSACTION)
    kw.update(over)
    return TableDef(**kw)


def _q(tdef, days=60, date=WM, key=None, mode=MODE_INCREMENTAL, date_kind="datetime"):
    return build_query(tdef, Settings(mode=mode, resync_days=days), Watermark(value=WM, kind="datetime"),
                       Watermark(value=date, kind=date_kind) if date else Watermark(value=None),
                       Watermark(value=key, kind="number") if key else None)


def test_resync_branch_is_appended_and_disjoint():
    parts = _q(_ol()).split("\nUNION ALL\n")
    assert len(parts) == 3
    base = parts[0].split(" WHERE ")[0]
    assert parts[2] == (f"{base} WHERE t.CREATION_DATE >= TRUNC(SYSDATE) - 60"
                        f" AND t.CREATION_DATE < {LIT}"
                        f" AND LNNVL(t.AMEND_LAST_DATE > {LIT})")


def test_zero_days_disables_and_leaves_the_query_unchanged():
    assert _q(_ol(), days=0) == _q(_ol(), days=0).split("\nUNION ALL\nSELECT")[0] or True
    assert "TRUNC(SYSDATE)" not in _q(_ol(), days=0)
    assert _q(_ol()).split("\nUNION ALL\n")[:2] == _q(_ol(), days=0).split("\nUNION ALL\n")


def test_resync_excludes_rows_the_insert_key_branch_takes():
    helper = HelperJoin(table="OASIS.DELIVERY_LINES", join_keys=(("DELIVERY_LINE", "DELIVERY_LINE"),),
                        cdc_column="AMEND_LAST_DATE", where_date_column="AMEND_LAST_DATE")
    t = _ol(table="OASIS.DELIVERY_CHARGE", unique_key="DELIVERY_CHARGE_ID", cdc_column=None,
            where_date_column="DELIVERY_DATE", helper=helper,
            insert_key_column="DELIVERY_CHARGE_ID", insert_key_lookback=1000)
    parts = _q(t, key="1505160.0").split("\nUNION ALL\n")
    assert len(parts) == 4
    assert "h.AMEND_LAST_DATE >= TRUNC(SYSDATE) - 60" in parts[3]
    assert parts[3].endswith("AND LNNVL(t.DELIVERY_CHARGE_ID > 1504160.0)")


def test_julian_date_watermark_gets_a_julian_bound():
    q = _q(_ol(where_date_column="JULIAN_DATE"), date="2461252.0", date_kind="number")
    assert "t.JULIAN_DATE >= TO_NUMBER(TO_CHAR(TRUNC(SYSDATE) - 60, 'J'))" in q


def test_not_applied_without_date_column_watermark_cdc_only_or_initial():
    assert "TRUNC(SYSDATE)" not in _q(_ol(), date=None)
    assert "TRUNC(SYSDATE)" not in _q(_ol(), mode=MODE_INITIAL)
    assert "TRUNC(SYSDATE)" not in _q(_ol(incremental_cdc_only=True))
    master = _ol(table="OASIS.ROOM_MASTER", unique_key="ROOM_NO", where_date_column=None,
                 where_operator=None, where_value_of_initial_run=None, category=CATEGORY_MASTER)
    assert "TRUNC(SYSDATE)" not in _q(master)
```

(While implementing, drop the tautological first assert in `test_zero_days_...`; the other two carry the check.)

- [ ] **Step 2: RED.** Run `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_resync.py`. Expected: the query tests fail.

- [ ] **Step 3: Implement** in `etl/oracle_extract.py`:

```python
def _insert_key_floor(tdef: TableDef, key_wm: Watermark) -> Optional[str]:
    """``last_key - lookback`` as a plain decimal literal, or None when inactive."""
    if not tdef.insert_key_column or key_wm.value is None:
        return None
    return format(Decimal(str(key_wm.value)) - tdef.insert_key_lookback, "f")


def _resync_branch(days, tdef, shape, base, cdc_wm, date_wm, key_wm) -> Optional[str]:
    """Re-read rows dated within the last ``days`` that no other branch took.

    Catches rows Oracle changes without moving the CDC column (C1). Disjoint from
    every other branch: ``date < date_wm`` excludes the new-rows branch,
    ``LNNVL(cdc > cdc_wm)`` the updated-rows branch, ``LNNVL(key > floor)`` the
    insert-key branch -- LNNVL keeps NULLs in, like the insert-key branch.
    """
    if not days or shape.date_ref is None or date_wm.value is None or shape.cdc_ref is None:
        return None
    lower = (f"TO_NUMBER(TO_CHAR(TRUNC(SYSDATE) - {int(days)}, 'J'))"
             if date_wm.kind == "number" else f"TRUNC(SYSDATE) - {int(days)}")
    preds = [f"{shape.date_ref} >= {lower}",
             f"{shape.date_ref} < {format_watermark(date_wm)}",
             f"LNNVL({shape.cdc_ref} > {format_watermark(cdc_wm)})"]
    floor = _insert_key_floor(tdef, key_wm)
    if floor is not None:
        preds.append(f"LNNVL(t.{tdef.insert_key_column} > {floor})")
    return f"{base} WHERE " + " AND ".join(preds)
```

`_insert_key_branch` uses `_insert_key_floor`, and its output is unchanged. In `build_query`'s incremental, non-cdc-only branch:

```python
key_wm = key_wm or Watermark(value=None)
parts = [_build_incremental_query(shape, base, cdc_wm, date_wm, ceiling_pred)]
parts += [b for b in (
    _insert_key_branch(tdef, shape, base, cdc_wm, date_wm, key_wm),
    _resync_branch(settings.resync_days, tdef, shape, base, cdc_wm, date_wm, key_wm),
) if b]
return "\nUNION ALL\n".join(parts)
```

- [ ] **Step 4: GREEN.** Run the resync, insert_key, incremental_cdc_only, query_sources, and plan_no_cdc_fallback tests plus the full suite. Existing query tests construct `Settings(mode=...)`, whose default is now 60. Any test that pins the exact incremental SQL for a table with a date column must now expect the extra branch. Update those expectations (recording each one as a ruling), or pin `resync_days=0` where the test is about something else.

- [ ] **Step 5: Oracle probe (read-only).** On madinah, for order_lines and delivery_charge with the live watermarks and `resync_days=60`, run `SELECT COUNT(*), COUNT(DISTINCT <key>) FROM (<q>)`. Expected: the two counts are equal.

- [ ] **Step 6: Commit.** Message: `feat(etl): unified re-read window branch for rows changed without a CDC bump`.

---

### Task 4: Re-read volume at 60 days, measured before rollout (read-only)

- [ ] For each eligible table on madinah (a large branch), count Oracle rows in the resync branch only: rows dated in `[TRUNC(SYSDATE)-60, date watermark)` and not already selected. Report per table, the total, and a per-run estimate across the 8 branches (scaled by each table's branch row share from `control_state` row counts). Compare against today's typical per-run rows.
- [ ] Present the numbers to the user before the merge. Rollout (merge to `main`, then the next run) happens only on their go-ahead. `resync_days` can be lowered or set to 0 on the Settings page with no code change.
