# Parallel Per-Table Load Pipelines Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load N tables into Iceberg concurrently — one dlt pipeline per table, a configurable worker pool (`etl.load_workers`) — with everything still landing in the same bucket/dataset, and `load_workers = 1` reproducing today's serial behavior exactly.

**Architecture:** Replace the single shared dlt pipeline + 1-worker load executor with per-table pipelines (name `<pipeline_name>__<table>`, built on the worker thread that loads the table) drained by an N-worker pool. Prerequisite: every destination *read* helper switches from the pipeline's local schema (`get_iceberg_tables`) to the persistent Postgres Iceberg catalog (`get_catalog().load_table`), because a fresh per-table pipeline's schema is empty on its first run and schema-based reads would silently degrade (lost `insert_at` carry-forward, a squash that expires prior history). `_PipelineHolder` and the startup package sweep disappear — failure isolation becomes per-table by construction.

**Tech Stack:** Python 3.13, dlt 1.28.1 (filesystem destination, Iceberg format, Postgres SqlCatalog via `[iceberg_catalog]`), pyiceberg 0.11.1, SQLAlchemy 2.0, pytest.

**Spec:** [docs/superpowers/specs/2026-07-27-parallel-per-table-load-pipelines-design.md](../specs/2026-07-27-parallel-per-table-load-pipelines-design.md)

## Global Constraints

- **The merge-hash bytes are FROZEN.** Nothing in this plan touches hashing; do not modify `_serialize_keys`, `_merge_hash_array`, or anything they call.
- **No new third-party dependencies.** The venv has NO numpy.
- **Best-effort rule:** destination reads must never fail a load — log a warning and degrade to current behavior.
- **Data placement is invariant:** same bucket, same dataset, same Iceberg table names/locations. Pipeline names only change dlt's LOCAL bookkeeping and `_dlt_*` bookkeeping file names in the dataset.
- **`load_workers = 1` must reproduce today's strictly-serial load behavior.**
- **Do not modify anything under `.venv/`.**
- Windows dev host. Run tests with `.venv/Scripts/python.exe -m pytest`.
- Postgres-gated tests use the `pg_meta` fixture and skip when `OASIS_TEST_PG_DSN` is unset — "passed or skipped" is the expected outcome for those.
- Work on branch `perf/parallel-table-loads` off `main`.
- `tables.json` is gitignored and untouched by this plan.
- **Catalog-access hazard in tests:** after Task 2, any test that drives `_load_one_table` without stubbing reaches `_open_dest_table`, which resolves the real `[iceberg_catalog]` config. Every such test must either `monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)` or pass `Settings(snapshot_maintenance=False)` *and* stub the merge-path helpers, so the suite never opens a live catalog.

## File Structure

- `etl/iceberg_load.py` — `_open_dest_table` (new), converted read helpers, catalog-based retention, `ControlStore` lock, `_table_pipeline_name` (new), `build_pipeline` name override, parallel `load_and_record`, `_PipelineHolder` deleted.
- `etl/progress.py` — `PipelineMonitor` active-load set (`begin_load`/`end_load`).
- `etl/config.py` — `Settings.load_workers` + clamp + loader line.
- `etl/metastore.py` — engine `connect_timeout`.
- `oracle_to_iceberg.py` — `--load-workers` flag.
- `gui/workspace.py` — `load_workers` in `EDITABLE_ETL_KEYS`.
- `gui/iceberg_maintenance.py` — `_writable_table` via catalog.
- `README.md` — parallel-load tuning runbook.
- Tests: new `tests/test_open_dest_table.py`, `tests/test_control_store_threading.py`, `tests/test_monitor_active_loads.py`, `tests/test_table_pipeline_name.py`, `tests/test_parallel_load_dispatch.py`, `tests/test_load_workers_setting.py`; updates to `tests/test_coerce_nulls_isolation.py`, `tests/test_merge_schema_destination_widen.py`, `tests/test_merge_hash_merge.py`, `tests/test_cleanup_staged.py`, `tests/test_snapshot_retention_guards.py`, `tests/test_load_timeout.py`, `tests/test_pending_packages.py`.

---

### Task 0: Branch

- [ ] **Step 1: Create the working branch**

```bash
git checkout main && git pull && git checkout -b perf/parallel-table-loads
```

---

### Task 1: `_open_dest_table` — catalog-backed destination reads

The persistent Postgres `SqlCatalog` (configured under `[iceberg_catalog]` in `.dlt/config.toml` + secrets) knows every table ever loaded, independent of any dlt pipeline's local schema. This helper is the single door for all destination *reads*; the dlt *write* path is untouched. It mirrors `gui/iceberg_browser.py`, which already calls `get_catalog()` and uses `f"{dataset}.{table}"` identifiers.

**Files:**
- Modify: `etl/iceberg_load.py` (insert new function directly above `_coerce_unified_nulls`, ~line 277)
- Test: `tests/test_open_dest_table.py` (new)

**Interfaces:**
- Consumes: `dlt.common.libs.pyiceberg.get_catalog()` (resolves `[iceberg_catalog]` config; returns a pyiceberg Catalog), `pyiceberg.exceptions.NoSuchTableError`, `Settings.dataset_name`.
- Produces: `_open_dest_table(settings: Settings, table_name: str) -> Optional[IcebergTable]` — pyiceberg `Table` when `<dataset>.<table>` is registered, `None` for a missing table (silent) or any failure (warning). **Never raises.** Tasks 2–3 call this.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_open_dest_table.py`:

```python
"""_open_dest_table: catalog-backed destination reads, independent of any dlt
pipeline's local schema. Best-effort: absent table or broken catalog -> None."""
from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from etl import iceberg_load
from etl.config import Settings


def _cat(tmp_path):
    cat = SqlCatalog(
        "t", uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
        warehouse=(tmp_path / "wh").as_uri(),
        # pyarrow's io chokes on file:///D:/ URIs on Windows; fsspec handles them
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    cat.create_namespace("oasis")
    return cat


def test_open_dest_table_loads_registered_table(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    cat.create_table("oasis.foo", schema=rows.schema).append(rows)
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    tbl = iceberg_load._open_dest_table(Settings(), "foo")

    assert tbl is not None
    assert {f.name for f in tbl.schema().fields} == {"id"}


def test_open_dest_table_none_when_absent(tmp_path, monkeypatch):
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog",
                        lambda: _cat(tmp_path))
    assert iceberg_load._open_dest_table(Settings(), "nope") is None


def test_open_dest_table_none_on_catalog_failure(monkeypatch):
    def boom():
        raise RuntimeError("catalog down")

    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", boom)
    assert iceberg_load._open_dest_table(Settings(), "foo") is None


def test_open_dest_table_asks_for_dataset_qualified_identifier(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    cat.create_table("oasis.bar", schema=rows.schema)
    asked = []
    real = cat.load_table

    def spy(ident):
        asked.append(ident)
        return real(ident)

    monkeypatch.setattr(cat, "load_table", spy)
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    assert iceberg_load._open_dest_table(Settings(), "bar") is not None
    assert asked == ["oasis.bar"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_open_dest_table.py -v`
Expected: FAIL — `AttributeError: module 'etl.iceberg_load' has no attribute '_open_dest_table'`.

- [ ] **Step 3: Implement**

In `etl/iceberg_load.py`, directly above `_coerce_unified_nulls` (~line 277):

```python
def _open_dest_table(settings: Settings, table_name: str):
    """Open ``<dataset>.<table>`` from the configured Iceberg catalog, or None.

    Destination READS go through the persistent catalog (``[iceberg_catalog]``
    config), NOT a dlt pipeline's local schema: a per-table pipeline's schema
    is empty until its first successful run, so a schema-based read
    (``get_iceberg_tables``) would silently degrade every destination-dependent
    helper on that table's first run under a fresh pipeline -- lost insert_at
    carry-forward, a squash that treats prior history as this run's snapshots.
    The catalog knows every table ever loaded regardless of local state. The
    dlt WRITE path is untouched.

    Best-effort by contract and NEVER raises: a missing table (normal first
    load) returns None silently; any other failure logs a warning and returns
    None so the caller degrades exactly as before.
    """
    try:
        from dlt.common.libs.pyiceberg import get_catalog
        from pyiceberg.exceptions import NoSuchTableError
    except ImportError as exc:
        log.warning("pyiceberg catalog access unavailable: %s", exc)
        return None
    try:
        return get_catalog().load_table(f"{settings.dataset_name}.{table_name}")
    except NoSuchTableError:
        return None  # first load: the table does not exist yet
    except Exception as exc:  # noqa: BLE001 - best effort by contract
        log.warning("[%s] could not open destination table via catalog: %s",
                    table_name, exc)
        return None
```

- [ ] **Step 4: Run the new tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_open_dest_table.py -v`
Expected: all 4 PASS.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass (function is not yet called anywhere).

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_open_dest_table.py
git commit -m "feat(load): catalog-backed destination-table reader" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Convert destination-read helpers to `_open_dest_table`

Seven read helpers switch from `get_iceberg_tables(pipeline, ...)` to `_open_dest_table(settings, ...)`; their `pipeline` parameter becomes `settings`. `_load_one_table` keeps its `pipeline` parameter (the dlt write path needs it) and passes `settings` to the helpers instead.

**Files:**
- Modify: `etl/iceberg_load.py` — `_coerce_unified_nulls` (~277), `_read_destination_arrow_types` (~327), `_widen_schema_to_destination` (~349), `_existing_insert_at` (~427), `_table_is_hash_ready` (~559), `_table_snapshot_ids` (~903), `_squash_table_run_snapshots` (~934), call sites inside `_load_one_table` (~1385–1458)
- Test (update): `tests/test_coerce_nulls_isolation.py`, `tests/test_merge_schema_destination_widen.py`, `tests/test_merge_hash_merge.py`, `tests/test_cleanup_staged.py`, `tests/test_load_timeout.py`, `tests/test_pending_packages.py`

**Interfaces:**
- Consumes: `_open_dest_table(settings, table_name)` from Task 1.
- Produces (new signatures; Tasks 3 and 8 rely on them):
  - `_coerce_unified_nulls(settings: Settings, tdef: TableDef, schema: pa.Schema) -> pa.Schema`
  - `_read_destination_arrow_types(settings: Settings, tdef: TableDef) -> dict`
  - `_widen_schema_to_destination(settings: Settings, tdef: TableDef, schema: pa.Schema) -> pa.Schema`
  - `_existing_insert_at(settings: Settings, tdef: TableDef, branches: list[int], unified_schema: pa.Schema, hash_ready: bool = False) -> Optional[pa.Table]`
  - `_table_is_hash_ready(settings: Settings, tdef: TableDef, hash_col: str) -> bool`
  - `_table_snapshot_ids(settings: Settings, table_name: str) -> set[int]`
  - `_squash_table_run_snapshots(settings: Settings, table_name: str, before_ids: set[int]) -> None`

- [ ] **Step 1: Update the tests to the new seam (failing first)**

**`tests/test_coerce_nulls_isolation.py`** — replace the whole file body below the imports-and-fixtures (keep `_tdef` and `_StoredTable` as they are; add `Settings` to the config import; module docstring: replace the last sentence with "Reads now go through the persistent catalog, which opens exactly one identifier — the isolation is structural."):

```python
from etl.config import CATEGORY_MASTER, Settings, TableDef


def test_coerce_nulls_uses_target_type_via_catalog(monkeypatch):
    requested = []

    def fake_open(settings, name):
        requested.append(name)
        return _StoredTable()

    monkeypatch.setattr(iceberg_load, "_open_dest_table", fake_open)

    # DEFAULT_DUAL_CODE is all-null this run but stored as double.
    schema = pa.schema([("ios", pa.int64()), ("DEFAULT_DUAL_CODE", pa.null())])
    out = iceberg_load._coerce_unified_nulls(Settings(), _tdef(), schema)

    # Coerced to the stored double, NOT the unsafe string fallback.
    assert out.field("DEFAULT_DUAL_CODE").type == pa.float64()
    # Exactly the target table was requested.
    assert requested == ["lab_ios"]


def test_coerce_nulls_falls_back_to_string_when_table_absent(monkeypatch):
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: None)
    schema = pa.schema([("ios", pa.int64()), ("X", pa.null())])
    out = iceberg_load._coerce_unified_nulls(Settings(), _tdef(), schema)
    assert out.field("X").type == pa.string()
```

**`tests/test_merge_schema_destination_widen.py`** — add `Settings` to the config import; rewrite the four tests (keep `_tdef`, `_StoredTable`, `_run_schema`):

```python
def test_merge_widens_key_to_destination_string(monkeypatch):
    requested = []

    def fake_open(settings, name):
        requested.append(name)
        return _StoredTable()

    monkeypatch.setattr(iceberg_load, "_open_dest_table", fake_open)

    out = iceberg_load._widen_schema_to_destination(Settings(), _tdef(), _run_schema())

    # The drifting key adopts the on-disk string; other columns are untouched.
    assert out.field("rule_ios").type == pa.string()
    assert out.field("contract_no").type == pa.int64()
    assert out.field("branch_id").type == pa.int64()
    assert requested == ["contract_rules"]


def test_merge_widen_noop_when_types_already_match(monkeypatch):
    class _MatchingStored:
        def schema(self) -> Schema:
            return Schema(
                NestedField(1, "contract_no", LongType(), required=False),
                NestedField(2, "rule_ios", StringType(), required=False),
                NestedField(3, "branch_id", LongType(), required=False),
            )

    monkeypatch.setattr(iceberg_load, "_open_dest_table",
                        lambda s, n: _MatchingStored())
    run = pa.schema([
        ("contract_no", pa.int64()),
        ("rule_ios", pa.string()),
        ("branch_id", pa.int64()),
    ])
    out = iceberg_load._widen_schema_to_destination(Settings(), _tdef(), run)
    assert out.equals(run)


def test_merge_widen_returns_schema_when_table_absent(monkeypatch):
    # First incremental of a table that does not exist yet: nothing to widen
    # against -> return the run schema unchanged (best effort).
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: None)
    run = _run_schema()
    out = iceberg_load._widen_schema_to_destination(Settings(), _tdef(), run)
    assert out.equals(run)


def test_merge_widen_survives_read_error(monkeypatch):
    class _Boom:
        def schema(self):
            raise RuntimeError("schema unreadable")

    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: _Boom())
    run = _run_schema()
    out = iceberg_load._widen_schema_to_destination(Settings(), _tdef(), run)
    # Best effort: a dest read failure must never fail the load.
    assert out.equals(run)
```

**`tests/test_merge_hash_merge.py`** — add `from etl import iceberg_load` next to the existing top imports. Replace `test_hash_ready_true_only_when_column_present` (lines 19–36) with:

```python
def test_hash_ready_true_only_when_column_present(tmp_path, monkeypatch):
    cat = _cat(tmp_path, "r")
    with_hash = pa.table({"id": pa.array([1], pa.int64()),
                          "merge_hash": pa.array([b"x" * 16], pa.binary())})
    without = pa.table({"id": pa.array([1], pa.int64())})
    t_ready = cat.create_table("oasis.ready", schema=with_hash.schema)
    t_ready.append(with_hash)
    t_plain = cat.create_table("oasis.plain", schema=without.schema)
    t_plain.append(without)
    from etl.config import Settings

    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: t_ready)
    assert iceberg_load._table_is_hash_ready(Settings(), _Tdef, "merge_hash") is True
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: t_plain)
    assert iceberg_load._table_is_hash_ready(Settings(), _Tdef, "merge_hash") is False
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: None)
    assert iceberg_load._table_is_hash_ready(Settings(), _Tdef, "merge_hash") is False
```

And the three `_existing_insert_at` tests (lines 163–203): in each, replace the `monkeypatch.setattr("dlt.common.libs.pyiceberg.get_iceberg_tables", ...)` line with `monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: t)` and the call with the new argument order — for example the first test becomes:

```python
def test_existing_insert_at_hash_ready_projects_and_renames(tmp_path, monkeypatch):
    # Directly exercises _existing_insert_at's hash_ready branch: scan -> select
    # (merge_hash, insert_at) -> rename insert -> return.
    t = _eia_stored_table(_cat(tmp_path, "eia_ok"), "eia_hash", with_hash=True)
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda s, n: t)
    settings = Settings()
    unified = pa.schema([(settings.inserted_ts_column, pa.string())])
    out = _existing_insert_at(settings, _TdefKeyed, [1], unified, hash_ready=True)
    assert out is not None
    assert set(out.column_names) == {"merge_hash", settings.inserted_ts_column}
    assert out.column(settings.inserted_ts_column).to_pylist() == ["2020-01-01"]
```

Apply the same two-line change (monkeypatch target + call signature `_existing_insert_at(settings, _TdefKeyed, [1], unified, hash_ready=True)`) to `test_existing_insert_at_hash_ready_none_when_stored_lacks_hash` and `test_existing_insert_at_hash_ready_normalizes_hash_col_case`.

**`tests/test_open_dest_table.py`** — append the headline-hazard regression test (prior snapshot history must be visible with NO pipeline involved, so the post-load squash can never mistake it for this run's snapshots):

```python
def test_snapshot_ids_come_from_catalog_not_pipeline(tmp_path, monkeypatch):
    cat = _cat(tmp_path)
    rows = pa.table({"id": pa.array([1], pa.int64())})
    tbl = cat.create_table("oasis.hist", schema=rows.schema)
    tbl.append(rows)
    tbl.append(rows)          # 2 snapshots of prior-run history
    monkeypatch.setattr("dlt.common.libs.pyiceberg.get_catalog", lambda: cat)

    ids = iceberg_load._table_snapshot_ids(Settings(), "hist")

    assert len(ids) == 2      # a fresh per-table pipeline still sees history
```

**`tests/test_cleanup_staged.py`** — in `test_load_one_table_merge_deletes_staged` add one line to the monkeypatch block (the squash after a successful merge must not open the real catalog):

```python
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)
```

**`tests/test_load_timeout.py`** — in `test_timed_out_commit_marks_plan_poisoned_and_skips_cleanup` add the same line next to the existing monkeypatches:

```python
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)
```

**`tests/test_pending_packages.py`** — in `test_failed_table_load_drops_pending_packages` add the same line next to the `boom` monkeypatch:

```python
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)
```

- [ ] **Step 2: Run the updated tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_coerce_nulls_isolation.py tests/test_merge_schema_destination_widen.py tests/test_merge_hash_merge.py -v`
Expected: FAIL — the helpers still take `pipeline` first and never call `_open_dest_table` (signature `TypeError`s / assertion failures on `requested`).

- [ ] **Step 3: Convert the helpers**

In `etl/iceberg_load.py`:

**`_coerce_unified_nulls`** — new signature `def _coerce_unified_nulls(settings: Settings, tdef: TableDef, schema: pa.Schema) -> pa.Schema:`; replace the try-block body:

```python
    overrides: dict[str, pa.DataType] = {}
    try:
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        tbl = _open_dest_table(settings, tdef.dataset_table_name)
        if tbl is not None:
            # dlt normalizes identifiers to lower snake; for these clean
            # UPPER_SNAKE / already-lower names that is just lower-casing.
            dest = {f.name: f.type for f in schema_to_pyarrow(tbl.schema())}
            for name in null_names:
                t = dest.get(name.lower())
                if t is not None and not pa.types.is_null(t):
                    overrides[name] = t
    except Exception as exc:  # noqa: BLE001 - best effort; string fallback is safe
        log.warning("[%s] could not read destination types for null-column "
                    "coercion: %s", tdef.dataset_table_name, exc)
```

In its docstring, replace the "(read best-effort from the Iceberg table)" parenthetical with "(read best-effort through the persistent catalog — see ``_open_dest_table``)".

**`_read_destination_arrow_types`** — new signature `def _read_destination_arrow_types(settings: Settings, tdef: TableDef) -> dict:`; body:

```python
    try:
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        tbl = _open_dest_table(settings, tdef.dataset_table_name)
        if tbl is None:
            return {}
        return {f.name.lower(): f.type for f in schema_to_pyarrow(tbl.schema())}
    except Exception as exc:  # noqa: BLE001 - best effort; no widening is safe
        log.warning("[%s] could not read destination types for merge schema "
                    "widening: %s", tdef.dataset_table_name, exc)
        return {}
```

Docstring: drop the "Opens ONLY the target table…" paragraph (single-identifier reads are structural now) and note "Reads through the persistent catalog (``_open_dest_table``)."

**`_widen_schema_to_destination`** — new signature `def _widen_schema_to_destination(settings: Settings, tdef: TableDef, schema: pa.Schema) -> pa.Schema:`; the only body change is `dest = _read_destination_arrow_types(settings, tdef)`.

**`_existing_insert_at`** — new signature:

```python
def _existing_insert_at(
    settings: Settings, tdef: TableDef, branches: list[int],
    unified_schema: pa.Schema, hash_ready: bool = False,
) -> Optional[pa.Table]:
```

Replace the first try-block (the `get_iceberg_tables` import + call, through `if tbl is None: return None`) with:

```python
    try:
        from pyiceberg.expressions import In
    except ImportError as exc:
        log.warning("[%s] insert_at carry-forward unavailable: %s",
                    tdef.dataset_table_name, exc)
        return None

    tbl = _open_dest_table(settings, tdef.dataset_table_name)
    if tbl is None:
        return None  # first load of this table: nothing to preserve
```

Everything from `insert_norm = insert_col.lower()` on is unchanged.

**`_table_is_hash_ready`** — new signature `def _table_is_hash_ready(settings: Settings, tdef: TableDef, hash_col: str) -> bool:`; body:

```python
    tbl = _open_dest_table(settings, tdef.dataset_table_name)
    if tbl is None:
        return False
    try:
        return hash_col.lower() in {f.name for f in tbl.schema().fields}
    except Exception:  # noqa: BLE001 - best effort
        return False
```

**`_table_snapshot_ids`** — new signature `def _table_snapshot_ids(settings: Settings, table_name: str) -> set[int]:`; body:

```python
    try:
        tbl = _open_dest_table(settings, table_name)
        if tbl is None:
            return set()
        return {s.snapshot_id for s in tbl.metadata.snapshots}
    except Exception:  # noqa: BLE001 - first run: table not created yet
        return set()
```

**`_squash_table_run_snapshots`** — new signature `def _squash_table_run_snapshots(settings: Settings, table_name: str, before_ids: set[int]) -> None:`; body:

```python
    try:
        tbl = _open_dest_table(settings, table_name)
        if tbl is None:
            return
        expired = _squash_run_snapshots(tbl, before_ids)
        if expired:
            log.info("[%s] squashed %d intra-run snapshot(s); 1 kept for this run",
                     table_name, expired)
    except Exception as exc:  # noqa: BLE001 - maintenance never fails the load
        log.warning("[%s] snapshot squash failed: %s", table_name, exc)
```

**Call sites in `_load_one_table`** (the `pipeline` parameter stays — the write path uses it):

```python
    plan.unified_schema = _coerce_unified_nulls(settings, tdef, plan.unified_schema)
```

```python
    if plan.disposition == "merge":
        plan.unified_schema = _widen_schema_to_destination(
            settings, tdef, plan.unified_schema)
```

```python
    before_ids = (_table_snapshot_ids(settings, tdef.dataset_table_name)
                  if settings.snapshot_maintenance else set())
```

```python
            hash_ready = _table_is_hash_ready(settings, tdef, settings.merge_hash_column)
            existing = _existing_insert_at(
                settings, tdef,
                [r.branch_id for r in plan.success], plan.unified_schema,
                hash_ready=hash_ready)
```

```python
        if settings.snapshot_maintenance:
            _squash_table_run_snapshots(settings, tdef.dataset_table_name, before_ids)
```

- [ ] **Step 4: Run the affected files, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_coerce_nulls_isolation.py tests/test_merge_schema_destination_widen.py tests/test_merge_hash_merge.py tests/test_cleanup_staged.py tests/test_carry_forward_prefilter.py tests/test_load_timeout.py tests/test_pending_packages.py tests/test_open_dest_table.py -v`
Expected: all PASS (or SKIP where `pg_meta` gates). `test_carry_forward_prefilter` passes unmodified: its `lambda p_, t, s: s` stubs are arity-compatible with the new `(settings, tdef, schema)` calls and it already sets `snapshot_maintenance=False`.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_coerce_nulls_isolation.py tests/test_merge_schema_destination_widen.py tests/test_merge_hash_merge.py tests/test_cleanup_staged.py tests/test_load_timeout.py tests/test_pending_packages.py tests/test_open_dest_table.py
git commit -m "refactor(load): destination reads go through the persistent catalog" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Snapshot retention enumerates the catalog

`apply_snapshot_retention` loses its `pipeline` parameter and enumerates `get_catalog().list_tables(dataset)` instead of the shared pipeline's local schema. `_dlt*` names are skipped (preserves `include_dlt_tables=False` semantics). Bonus: the catalog remembers tables a local-schema rebuild forgot.

**Files:**
- Modify: `etl/iceberg_load.py` — `apply_snapshot_retention` (~948), its call in `load_and_record`'s finalize block (~1586)
- Test (update): `tests/test_snapshot_retention_guards.py`

**Interfaces:**
- Consumes: `get_catalog()` (call-time import), existing guarded property/expiry logic, `Settings.dataset_name / snapshot_maintenance / snapshot_expire_days / snapshot_min_to_keep`.
- Produces: `apply_snapshot_retention(settings: Settings) -> None` (Task 8's `load_and_record` keeps calling it this way).

- [ ] **Step 1: Update the tests (failing first)**

In `tests/test_snapshot_retention_guards.py`, replace `_retention` and add one test at the end of the file:

```python
class _FakeCatalog:
    def __init__(self, tables):
        self._tables = tables            # {(namespace, name): tbl}

    def list_tables(self, namespace):
        assert namespace == "oasis"      # settings.dataset_name
        return list(self._tables)

    def load_table(self, ident):
        return self._tables[ident]


def _retention(monkeypatch, tbl, **settings_kw):
    import dlt.common.libs.pyiceberg as ice

    monkeypatch.setattr(ice, "get_catalog",
                        lambda: _FakeCatalog({("oasis", "foo"): tbl}))
    iceberg_load.apply_snapshot_retention(Settings(**settings_kw))
    tbl.refresh()
```

```python
def test_retention_skips_dlt_system_tables(tmp_path, table, monkeypatch):
    import dlt.common.libs.pyiceberg as ice

    loaded = []

    class _Cat:
        def list_tables(self, ns):
            return [("oasis", "_dlt_loads"), ("oasis", "foo")]

        def load_table(self, ident):
            loaded.append(ident)
            return table

    monkeypatch.setattr(ice, "get_catalog", lambda: _Cat())
    iceberg_load.apply_snapshot_retention(Settings())
    assert loaded == [("oasis", "foo")]
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/test_snapshot_retention_guards.py -v`
Expected: FAIL — `apply_snapshot_retention` still takes `(pipeline, settings)` and calls `get_iceberg_tables`.

- [ ] **Step 3: Implement**

Replace `apply_snapshot_retention` — signature `def apply_snapshot_retention(settings: Settings) -> None:`; keep the `snapshot_maintenance` early-return, `max_age_ms`/`cutoff` computation, and `props` dict exactly as they are; replace the import guard and enumeration:

```python
    if not settings.snapshot_maintenance:
        return
    try:
        from dlt.common.libs.pyiceberg import get_catalog
    except ImportError:
        log.warning("pyiceberg table access unavailable; skipping snapshot retention")
        return
```

(then the unchanged `max_age_ms` / `cutoff` / `props` block, then:)

```python
    try:
        catalog = get_catalog()
        idents = catalog.list_tables(settings.dataset_name)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not list Iceberg tables for retention: %s", exc)
        return

    for ident in idents:
        name = ident[-1] if isinstance(ident, tuple) else str(ident).rsplit(".", 1)[-1]
        if name.startswith("_dlt"):
            continue  # dlt bookkeeping tables are not retention targets
        try:
            tbl = catalog.load_table(ident)
```

…and the rest of the per-table body (the `current = tbl.properties` guard through the `except`/`log.warning`) unchanged, with the loop's log lines using `name`. Update the docstring: "Enumerates tables from the persistent Iceberg catalog (every table ever loaded, regardless of any pipeline's local schema); ``_dlt*`` names are skipped."

In `load_and_record`'s finalize block, change the call and its comment:

```python
        # Observability + retention reflect everything that completed this run.
        # Observability writes go straight to Postgres via control.store (the
        # same MetaStore ControlStore already holds -- no second engine/pool).
        # Retention enumerates tables from the persistent Iceberg catalog.
        monitor.set_activity("finalize")
        _write_observability(control.store, plans, settings, run_id)
        apply_snapshot_retention(settings)
```

- [ ] **Step 4: Run the file, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_snapshot_retention_guards.py -v`
Expected: all 5 PASS.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass — `test_load_timeout.py` / `test_pending_packages.py` stub `apply_snapshot_retention` with `lambda *a, **k: None`, which absorbs the arity change.

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_snapshot_retention_guards.py
git commit -m "refactor(load): snapshot retention enumerates the persistent catalog" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: GUI maintenance opens tables via the catalog

`gui/iceberg_maintenance.py::_writable_table` reads through the shared pipeline's local schema, which goes stale once loads stop using that pipeline (a table first loaded after cutover would 404 in the GUI). Convert it to the catalog — the same pattern `gui/iceberg_browser.py` already uses.

**Files:**
- Modify: `gui/iceberg_maintenance.py:40-53` (`_writable_table`), module docstring lines 3–7
- Test: existing `tests/test_iceberg_expire.py`, `tests/test_iceberg_expire_routes.py`

**Interfaces:**
- Consumes: `get_catalog()`, `load_settings().dataset_name`, `pyiceberg.exceptions.NoSuchTableError`.
- Produces: `_writable_table(table: str)` — same contract (pyiceberg `Table`, or `FileNotFoundError` for an unknown table).

- [ ] **Step 1: Replace `_writable_table`**

```python
def _writable_table(table: str):
    """Open the staging table writable via the persistent Iceberg catalog."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from dlt.common.libs.pyiceberg import get_catalog
    from pyiceberg.exceptions import NoSuchTableError

    from etl.config import load_settings

    try:
        return get_catalog().load_table(f"{load_settings().dataset_name}.{table}")
    except NoSuchTableError as exc:  # unknown to the catalog
        raise FileNotFoundError(table) from exc
```

In the module docstring, replace "Tables are opened writable through the ETL pipeline's Iceberg catalog" with "Tables are opened writable through the persistent Iceberg catalog (same commit path the loader uses)".

- [ ] **Step 2: Run the GUI maintenance tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_iceberg_expire.py tests/test_iceberg_expire_routes.py -v`
Expected: PASS with **no test changes** — `test_iceberg_expire.py` stubs `im._writable_table` directly (`monkeypatch.setattr(im, "_writable_table", lambda name: tbl)`) and the route tests stub `expire_snapshots` itself, so neither touches the function's internals.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add gui/iceberg_maintenance.py
git commit -m "refactor(gui): maintenance opens tables via the persistent catalog" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: ControlStore is safe under concurrent load workers

`advance()` mutates the shared nested dict and `save()` iterates it — today safe only because loads are serial. One `RLock` wraps both.

**Files:**
- Modify: `etl/iceberg_load.py` — `ControlStore.__init__` (~104), `advance` (~131), `save` (~142)
- Test: `tests/test_control_store_threading.py` (new)

**Interfaces:**
- Consumes: `threading.RLock` (module already imports `threading`).
- Produces: unchanged public surface (`load/entry/advance/as_dict/save`); `advance`/`save` are now thread-safe. Task 8 relies on this.

- [ ] **Step 1: Write the failing stress test**

Create `tests/test_control_store_threading.py`:

```python
"""ControlStore.advance/save must be safe under concurrent load workers."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from etl.iceberg_load import ControlStore


class _FakeStore:
    def __init__(self):
        self.saved_rows = []

    def upsert_control_state(self, rows):
        self.saved_rows.append(rows)


class _Result:
    """Duck-typed ExtractResult: only what advance() touches."""

    def __init__(self, table, branch):
        self.table, self.branch = table, branch
        self.new_cdc = None
        self.new_date = None
        self.status = "SUCCESS"
        self.row_count = 1
        self.duration_ms = 1


def test_concurrent_advance_and_save_lose_nothing():
    control = ControlStore(_FakeStore())

    def work(i):
        for j in range(50):
            control.advance(_Result(f"t{i}_{j}", f"b{j % 3}"))
            control.save()     # iterates the dict others are mutating

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(work, range(8)))   # re-raises any worker exception

    assert len(control.data) == 8 * 50
    final_tables = {r["table_name"] for r in control.store.saved_rows[-1]}
    assert len(final_tables) == 8 * 50
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/test_control_store_threading.py -v`
Expected: FAIL — almost always `RuntimeError: dictionary changed size during iteration` from `save()`. This is a concurrency test: if it passes spuriously, run it 2–3 times to observe the failure before implementing.

- [ ] **Step 3: Implement the lock**

In `ControlStore.__init__`:

```python
    def __init__(self, store: "MetaStore"):
        self.store = store
        self.data: dict = {}
        # advance()/save() run on concurrent load workers; the lock keeps dict
        # mutation and save's iteration consistent. load()/as_dict()/entry()
        # are only called when no loads are in flight (run start / between
        # phases), so they stay lock-free.
        self._lock = threading.RLock()
```

Wrap the whole body of `advance` in `with self._lock:` and the whole body of `save` (row-building **and** the upsert; upserts are ms-scale, contention is negligible) in `with self._lock:`.

- [ ] **Step 4: Run the test, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_control_store_threading.py -v`
Expected: PASS (run twice to be sure).

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_control_store_threading.py
git commit -m "fix(load): lock ControlStore for concurrent load workers" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: PipelineMonitor tracks an active-load set

Concurrent workers calling `set_activity("load:X")` would clobber each other and corrupt peak-memory attribution. The monitor gains `begin_load`/`end_load` over an insertion-ordered set; the label reads `load[2]:appointments,claims` while loads are in flight, else the coarse phase label (`extract`, `draining-loads`, `finalize`).

**Files:**
- Modify: `etl/progress.py` — `PipelineMonitor.__init__` (~200), `set_activity` (~246), `_refresh_peaks` (~263), `_heartbeat` (~283); class docstring
- Modify: `etl/iceberg_load.py` — `_load_one_table`'s two `monitor.set_activity(...)` call sites (~1407, ~1487)
- Test: `tests/test_monitor_active_loads.py` (new)

**Interfaces:**
- Consumes: existing `self._lock`.
- Produces: `begin_load(table: str) -> None`, `end_load(table: str) -> None`, `_activity_label() -> str`. `set_activity` keeps its name/signature for phase labels. Task 8's tests rely on `begin_load`/`end_load` existing.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_monitor_active_loads.py`:

```python
"""Peak/heartbeat labels track the SET of in-flight table loads."""
from __future__ import annotations

from etl.progress import PipelineMonitor


def _mon():
    return PipelineMonitor(total_units=1, total_tables=1, enabled=False)


def test_label_is_phase_when_no_loads_active():
    m = _mon()
    m.set_activity("extract")
    assert m._activity_label() == "extract"


def test_label_lists_active_loads_in_start_order():
    m = _mon()
    m.set_activity("extract")
    m.begin_load("appointments")
    m.begin_load("claims")
    assert m._activity_label() == "load[2]:appointments,claims"
    m.end_load("appointments")
    assert m._activity_label() == "load[1]:claims"
    m.end_load("claims")
    assert m._activity_label() == "extract"


def test_end_load_of_unknown_table_is_noop():
    m = _mon()
    m.end_load("never-started")
    assert m._activity_label() == "starting"


def test_peak_attribution_names_the_inflight_load():
    m = _mon()
    m.begin_load("big_table")
    m._refresh_peaks()
    report = m.stop()
    assert "big_table" in (report.rss_peak_activity or "")
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/test_monitor_active_loads.py -v`
Expected: FAIL — `AttributeError: 'PipelineMonitor' object has no attribute 'begin_load'`.

- [ ] **Step 3: Implement**

In `PipelineMonitor.__init__`, replace `self._activity = "starting"            # plain str: assignment is atomic` with:

```python
        self._phase = "starting"               # coarse label when no loads run
        self._active_loads: dict[str, None] = {}   # insertion-ordered set
```

Replace `set_activity` and add the new methods:

```python
    def set_activity(self, label: str) -> None:
        """Coarse phase label (extract/draining-loads/finalize); shown whenever
        no table load is in flight. Plain str: assignment is atomic."""
        self._phase = label

    def begin_load(self, table: str) -> None:
        """Mark a table load in flight (called from a load worker thread)."""
        with self._lock:
            self._active_loads[table] = None

    def end_load(self, table: str) -> None:
        with self._lock:
            self._active_loads.pop(table, None)

    def _activity_label(self) -> str:
        """The in-flight loads if any (``load[2]:a,b``), else the phase label."""
        with self._lock:
            active = list(self._active_loads)
        if active:
            return f"load[{len(active)}]:" + ",".join(active)
        return self._phase
```

In `_refresh_peaks`, change `act = self._activity` to `act = self._activity_label()`.

In `_heartbeat`, compute the label BEFORE taking the lock (it takes the lock itself) and use it:

```python
    def _heartbeat(self, elapsed: float) -> str:
        activity = self._activity_label()
        with self._lock:
            ud, uf, rows = self._units_done, self._units_failed, self._rows
            tl = self._tables_loaded
        failed = f" {uf} failed" if uf else ""
        return (f"PROGRESS {_elapsed(elapsed)} | {activity} | "
                f"tables {tl}/{self.total_tables} | "
                f"extract {ud}/{self.total_units}{failed} | "
                f"rows={rows:,} | rss={_mb(self._rss_cur())}"
                f"(peak {_mb(self._rss_peak)}) arrow={_mb(_arrow_current())}")
```

In the class docstring, change "``set_activity`` to label what is currently running" to "``set_activity`` for phase labels and ``begin_load``/``end_load`` from load workers".

In `etl/iceberg_load.py::_load_one_table`, replace:

```python
    monitor.set_activity(f"load:{tdef.dataset_table_name}")
```
with
```python
    monitor.begin_load(tdef.dataset_table_name)
```

and in its `finally` block replace:

```python
        # Hand the label back to extraction (which may still be running).
        monitor.set_activity("extract")
```
with
```python
        monitor.end_load(tdef.dataset_table_name)
```

- [ ] **Step 4: Run the tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_monitor_active_loads.py -v`
Expected: all 4 PASS.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add etl/progress.py etl/iceberg_load.py tests/test_monitor_active_loads.py
git commit -m "feat(progress): monitor tracks the set of in-flight table loads" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Per-table pipeline naming

**Files:**
- Modify: `etl/iceberg_load.py` — `build_pipeline` (~1008); add `_table_pipeline_name` directly above it
- Test: `tests/test_table_pipeline_name.py` (new)

**Interfaces:**
- Consumes: `Settings.pipeline_name`, `TableDef.dataset_table_name` (already normalized lowercase).
- Produces: `_table_pipeline_name(settings: Settings, tdef: TableDef) -> str` returning `f"{settings.pipeline_name}__{tdef.dataset_table_name}"`; `build_pipeline(settings, pipelines_dir=None, pipeline_name=None)` where `pipeline_name=None` keeps `settings.pipeline_name` (existing callers unchanged). Task 8 uses both.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_table_pipeline_name.py`:

```python
"""Per-table pipeline names: stable, deterministic, derived from settings."""
from __future__ import annotations

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, Settings, TableDef


def _tdef():
    return TableDef(
        table="OASIS.APPOINTMENTS", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def test_table_pipeline_name_is_stable_and_derived():
    assert (iceberg_load._table_pipeline_name(Settings(), _tdef())
            == "oracle_to_iceberg__appointments")


def test_build_pipeline_honors_name_override(tmp_path):
    p = iceberg_load.build_pipeline(
        Settings(destination_bucket_url=str(tmp_path / "bucket")),
        pipelines_dir=str(tmp_path / "pipes"),
        pipeline_name="oracle_to_iceberg__appointments")
    assert p.pipeline_name == "oracle_to_iceberg__appointments"


def test_build_pipeline_defaults_to_settings_name(tmp_path):
    p = iceberg_load.build_pipeline(
        Settings(destination_bucket_url=str(tmp_path / "bucket")),
        pipelines_dir=str(tmp_path / "pipes"))
    assert p.pipeline_name == "oracle_to_iceberg"
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/test_table_pipeline_name.py -v`
Expected: FAIL — `_table_pipeline_name` missing; `build_pipeline` rejects the `pipeline_name` kwarg.

- [ ] **Step 3: Implement**

Above `build_pipeline`:

```python
def _table_pipeline_name(settings: Settings, tdef: TableDef) -> str:
    """Stable per-table dlt pipeline name: ``<pipeline_name>__<table>``.

    Deterministic (no assignment state) and stable across runs, so a table's
    local schema/state/pending packages persist in its own working dir under
    dlt's pipelines_dir. ``dataset_table_name`` is already normalized
    lowercase, so the result is a valid pipeline/directory name.
    """
    return f"{settings.pipeline_name}__{tdef.dataset_table_name}"
```

`build_pipeline` gains the override:

```python
def build_pipeline(settings: Settings, pipelines_dir: Optional[str] = None,
                   pipeline_name: Optional[str] = None):
    # pipelines_dir is dlt's LOCAL bookkeeping (schema/state/load packages), not
    # the destination. None keeps dlt's default (~/.dlt/pipelines).
    # pipeline_name overrides the settings-level name for per-table load
    # pipelines (see _table_pipeline_name); None keeps the shared name for
    # GUI/maintenance callers. Neither affects where data lands.
    return dlt.pipeline(
        pipeline_name=pipeline_name or settings.pipeline_name,
        destination=dlt.destinations.filesystem(
            bucket_url=settings.destination_bucket_url
        ),
        dataset_name=settings.dataset_name,
        pipelines_dir=pipelines_dir,
    )
```

- [ ] **Step 4: Run the tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_table_pipeline_name.py -v`
Expected: all 3 PASS.

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add etl/iceberg_load.py tests/test_table_pipeline_name.py
git commit -m "feat(load): stable per-table pipeline naming" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Parallel load executor, per-table pipelines, `_PipelineHolder` deleted

The core change. `Settings.load_workers` (default 2, clamped ≥ 1) sizes the load pool; `_load_task` builds the table's own pipeline on the worker thread and sweeps its pending packages; a commit timeout abandons only that table's pipeline (no rebuild machinery); the startup sweep and `_PipelineHolder` disappear.

**Files:**
- Modify: `etl/config.py` — `Settings` (below `load_commit_timeout_s`, ~line 339), `__post_init__` (~356), `load_settings()` (~568)
- Modify: `etl/iceberg_load.py` — delete `_PipelineHolder` (~1023–1053) and the `tempfile` import (line 37, now unused); rework `load_and_record` (~1492); module docstring lines 14–23
- Test: `tests/test_load_workers_setting.py` (new), `tests/test_parallel_load_dispatch.py` (new); update `tests/test_load_timeout.py`, `tests/test_pending_packages.py`

**Interfaces:**
- Consumes: `_table_pipeline_name` + `build_pipeline(…, pipeline_name=…)` (Task 7), `monitor.begin_load/end_load` (Task 6), locked `ControlStore` (Task 5), `apply_snapshot_retention(settings)` (Task 3), converted helpers (Task 2).
- Produces: `Settings.load_workers: int = 2` (clamped in `__post_init__`); `load_and_record` with the same signature but an N-worker pool; no `_PipelineHolder` symbol.

- [ ] **Step 1: Write the failing settings tests**

Create `tests/test_load_workers_setting.py`:

```python
"""load_workers: parallel load slots, default 2, never below 1."""
from __future__ import annotations

from etl.config import Settings


def test_default_load_workers_is_two():
    assert Settings().load_workers == 2


def test_load_workers_clamped_to_at_least_one():
    assert Settings(load_workers=0).load_workers == 1
    assert Settings(load_workers=-3).load_workers == 1


def test_load_workers_accepts_higher_values():
    assert Settings(load_workers=4).load_workers == 4
```

- [ ] **Step 2: Write the failing dispatch tests**

Create `tests/test_parallel_load_dispatch.py`:

```python
"""Load dispatch: N workers overlap distinct tables, each through its own
per-table pipeline; load_workers=1 keeps loads strictly serial."""
from __future__ import annotations

import threading
import time

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef
from etl.iceberg_load import ControlStore, TableLoadPlan
from etl.oracle_extract import ExtractResult


class _FakeStore:
    def upsert_control_state(self, rows):
        pass


def _tdef(name):
    return TableDef(
        table=f"OASIS.{name}", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _drive(monkeypatch, tables, load_workers, body):
    """Run load_and_record with _load_one_table stubbed to `body(tdef)`."""
    built = []

    def fake_build(settings, pipelines_dir=None, pipeline_name=None):
        built.append(pipeline_name)
        return object()

    def fake_load(pipeline, tdef, batch, settings, control, total_branches,
                  branches_in_run, monitor):
        body(tdef)
        plan = TableLoadPlan(tdef=tdef, success=[], failed=[])
        plan.load_status = "SUCCESS"
        return plan

    monkeypatch.setattr(iceberg_load, "build_pipeline", fake_build)
    monkeypatch.setattr(iceberg_load, "clear_pending_packages", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_load_one_table", fake_load)
    monkeypatch.setattr(iceberg_load, "_write_observability", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "apply_snapshot_retention", lambda *a, **k: None)

    def run_extraction(on_table_done):
        for t in tables:
            on_table_done(ExtractResult(table_def=t, branch="b1", branch_id=1,
                                        status="SUCCESS", row_count=1,
                                        staged_path=None))

    summary = iceberg_load.load_and_record(
        run_extraction_fn=run_extraction, tables=tables,
        settings=Settings(mode=MODE_INCREMENTAL, progress_enabled=False,
                          load_workers=load_workers),
        control=ControlStore(_FakeStore()), run_id="r",
        total_branches=1, branches_in_run=1)
    return summary, built


def test_two_workers_overlap_two_tables(monkeypatch):
    barrier = threading.Barrier(2, timeout=10)

    summary, built = _drive(
        monkeypatch, [_tdef("A"), _tdef("B")], load_workers=2,
        body=lambda tdef: barrier.wait())   # both must be in flight at once

    assert sorted(built) == ["oracle_to_iceberg__a", "oracle_to_iceberg__b"]
    assert [p.load_status for p in summary.plans] == ["SUCCESS", "SUCCESS"]
    assert summary.extraction_error is None


def test_single_worker_is_strictly_serial(monkeypatch):
    state = {"active": 0, "peak": 0}
    lock = threading.Lock()

    def body(tdef):
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        time.sleep(0.05)
        with lock:
            state["active"] -= 1

    summary, _ = _drive(monkeypatch, [_tdef("A"), _tdef("B"), _tdef("C")],
                        load_workers=1, body=body)

    assert state["peak"] == 1
    assert len(summary.plans) == 3


def test_zero_workers_clamps_to_serial(monkeypatch):
    state = {"active": 0, "peak": 0}
    lock = threading.Lock()

    def body(tdef):
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        time.sleep(0.02)
        with lock:
            state["active"] -= 1

    summary, _ = _drive(monkeypatch, [_tdef("A"), _tdef("B")],
                        load_workers=0, body=body)

    assert state["peak"] == 1
    assert len(summary.plans) == 2
```

- [ ] **Step 3: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/test_load_workers_setting.py tests/test_parallel_load_dispatch.py -v`
Expected: FAIL — `Settings.__init__() got an unexpected keyword argument 'load_workers'`.

- [ ] **Step 4: Add the setting**

In `etl/config.py`, directly below the `load_commit_timeout_s` field:

```python
    # Parallel load slots: how many tables may load into Iceberg concurrently,
    # each through its own per-table dlt pipeline (same bucket + dataset; see
    # iceberg_load._table_pipeline_name). Peak load RSS scales roughly
    # linearly -- every in-flight table can materialize up to
    # load_group_max_bytes of staged parquet (x3-6 once decoded to Arrow) --
    # so when raising this, lower load_group_max_bytes to compensate.
    # 1 = the pre-parallel strictly-serial behavior.
    load_workers: int = 2
```

In `__post_init__`, after the merge_hash line:

```python
        # A worker pool needs at least one worker; clamp here so every entry
        # point (config file, CLI, direct construction) gets the same floor.
        self.load_workers = max(1, int(self.load_workers))
```

In `load_settings()`, next to the `load_commit_timeout_s` line:

```python
        load_workers=int(_cfg("etl.load_workers", 2)),
```

Run: `.venv/Scripts/python.exe -m pytest tests/test_load_workers_setting.py -v`
Expected: all 3 PASS. (The dispatch tests still fail — the executor is next.)

- [ ] **Step 5: Rework `load_and_record`; delete `_PipelineHolder`**

In `etl/iceberg_load.py`:

1. Delete the `_PipelineHolder` class (~1023–1053) and remove `import tempfile` (line 37 — it has no other users; verify with a search before deleting).

2. In `load_and_record`, replace the holder + startup sweep + pool creation + `_load_task`:

```python
    # Make every Iceberg merge commit a single snapshot instead of one per
    # 1,000 rows (see _install_single_commit_merge). Must run before any load.
    _install_single_commit_merge()
    table_defs = {t.dataset_table_name: t for t in tables}
    order = {t.dataset_table_name: i for i, t in enumerate(tables)}

    remaining = {name: branches_in_run for name in table_defs}
    collected: dict[str, list[ExtractResult]] = {name: [] for name in table_defs}
    all_results: list[ExtractResult] = []
    lock = threading.Lock()
    load_pool = ThreadPoolExecutor(max_workers=settings.load_workers,
                                   thread_name_prefix="load")
    load_futures = []
```

(the `monitor = PipelineMonitor(...)` block is unchanged)

```python
    def _load_task(tdef: TableDef, batch: list[ExtractResult]) -> TableLoadPlan:
        # Each table loads through its OWN dlt pipeline, built on the worker
        # thread that runs it (dlt contexts are thread-affine, so concurrent
        # pipelines must live on separate threads). The name is stable across
        # runs, so pending debris from a crashed/failed earlier run can only
        # ever belong to this table -- swept here instead of once at startup.
        # A commit timeout poisons only this pipeline; nothing else touches it
        # this run, so it is simply abandoned (the old shared-pipeline
        # _PipelineHolder rebuild is gone). The zombie's leftover package is
        # dropped by this same sweep on the table's next run.
        pipeline = build_pipeline(
            settings, pipeline_name=_table_pipeline_name(settings, tdef))
        clear_pending_packages(pipeline, f"{tdef.dataset_table_name}:pre-load")
        plan = _load_one_table(pipeline, tdef, batch, settings, control,
                               total_branches, branches_in_run, monitor)
        if plan.load_timed_out:
            log.warning("[%s] abandoning this table's pipeline after commit "
                        "timeout", tdef.dataset_table_name)
        return plan
```

`on_table_done`, the extraction try/finally, `plans = [f.result() ...]`, and the sort are unchanged. The finalize block is already holder-free after Task 3.

3. Update `load_and_record`'s docstring paragraph ("Loads are serialized because a dlt pipeline is not safe to run concurrently with itself, but each table still lands as soon as it is ready rather than waiting for the rest.") to:

```
    ``on_table_done`` is called for every (branch, table) result; when a table
    has results from all ``branches_in_run`` branches it is handed to a
    ``settings.load_workers``-wide pool and written immediately -- concurrently
    with extraction and with other tables' loads. A dlt pipeline is not safe to
    run concurrently with ITSELF, so every table loads through its own
    per-table pipeline (see _table_pipeline_name); same-table exclusivity is
    structural (a table becomes ready exactly once per run).
```

4. Update the module docstring: replace the paragraph at lines 20–23 ("Loads are serialized (a dlt pipeline is not safe to run concurrently with itself) but start eagerly, …") with:

```
Loads run on a small worker pool (``Settings.load_workers``); each table loads
through its own per-table dlt pipeline (a dlt pipeline is not safe to run
concurrently with itself, but distinct pipelines on distinct threads are
fine), starting eagerly the moment the table is ready. Once everything
finishes, observability rows are written to Postgres and snapshot retention
is applied via the persistent Iceberg catalog.
```

- [ ] **Step 6: Update the timeout/pending tests**

**`tests/test_load_timeout.py`:**

1. Delete `test_rebuild_uses_a_fresh_pipelines_dir_so_it_cannot_readopt_the_zombie` (the holder is gone; abandonment is now structural).
2. Replace `test_load_and_record_rebuilds_pipeline_after_commit_timeout` with:

```python
def test_load_and_record_continues_past_a_timed_out_table(tmp_path, monkeypatch, pg_meta):
    """Table A's hung commit must not stop table B: each table has its own
    pipeline, so A is FAILED+timed-out and simply abandoned, B succeeds."""
    built = []

    def fake_build(settings, pipelines_dir=None, pipeline_name=None):
        built.append(pipeline_name)
        return _pipeline(tmp_path / (pipeline_name or "p"))

    monkeypatch.setattr(iceberg_load, "build_pipeline", fake_build)
    monkeypatch.setattr(iceberg_load, "_write_observability", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "apply_snapshot_retention", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_coerce_unified_nulls", lambda s, t, sc: sc)
    monkeypatch.setattr(iceberg_load, "_widen_schema_to_destination",
                        lambda s, t, sc: sc)
    monkeypatch.setattr(iceberg_load, "_table_is_hash_ready", lambda *a, **k: False)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)

    t_hang = _merge_tdef()                                   # -> "foo"
    t_ok = TableDef(table="OASIS.BAR", unique_key="ID",
                    cdc_column="AMEND_LAST_DATE", where_date_column=None,
                    where_operator=None, where_value_of_initial_run=None,
                    category=CATEGORY_MASTER)

    def fake_eia(settings, tdef, *a, **k):
        if tdef.dataset_table_name == "foo":
            raise TimeoutError("hung 900s")
        return None

    monkeypatch.setattr(iceberg_load, "_existing_insert_at", fake_eia)

    def run_extraction(on_table_done):
        for tdef in (t_hang, t_ok):
            on_table_done(ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                                        status="SUCCESS", row_count=2,
                                        staged_path=_staged(tmp_path)))

    summary = iceberg_load.load_and_record(
        run_extraction_fn=run_extraction, tables=[t_hang, t_ok],
        settings=Settings(mode=MODE_INCREMENTAL, progress_enabled=False,
                          snapshot_maintenance=False, load_workers=1),
        control=iceberg_load.ControlStore(pg_meta),
        run_id="r", total_branches=2, branches_in_run=1)

    by_name = {p.tdef.dataset_table_name: p for p in summary.plans}
    assert by_name["foo"].load_timed_out is True
    assert by_name["foo"].load_status == "FAILED"
    assert by_name["bar"].load_status == "SUCCESS"
    assert built == ["oracle_to_iceberg__foo", "oracle_to_iceberg__bar"]
```

(`CATEGORY_MASTER` is already imported at line 61.)

3. In the comment banner above these tests (lines 52–54: "the orchestrator must abandon (rebuild) it rather than clear/reuse it"), change "abandon (rebuild) it" to "abandon it (per-table pipelines: nothing rebuilds, nothing else uses it)".

**`tests/test_pending_packages.py`:**

1. Update the module docstring: replace "Because all tables share one pipeline, a single poisoned package … would otherwise be retried and fail again on every later table's run" with "Each table now has its own pipeline, so debris can only ever block that table; it is swept just before the table loads."
2. Replace `test_load_and_record_starts_with_clean_pipeline` with:

```python
def test_each_table_sweeps_its_own_pending_debris_before_loading(tmp_path, monkeypatch, pg_meta):
    """Crash debris in a table's own pipeline is swept just before that table
    loads (the per-table replacement for the old startup sweep)."""
    pipeline = _pipeline(tmp_path)
    pipeline.extract([{"id": 1}], table_name="leftover")
    assert pipeline.has_pending_data

    built = []

    def fake_build(settings, pipelines_dir=None, pipeline_name=None):
        built.append(pipeline_name)
        return pipeline

    monkeypatch.setattr(iceberg_load, "build_pipeline", fake_build)
    monkeypatch.setattr(iceberg_load, "_write_observability", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "apply_snapshot_retention", lambda *a, **k: None)

    tdef = TableDef(table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
                    where_date_column=None, where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_MASTER)
    staged = tmp_path / "staged.parquet"
    pq.write_table(pa.table({"ID": pa.array([], pa.int64())}), staged)

    def run_extraction(on_table_done):
        on_table_done(ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                                    status="SUCCESS", row_count=0,
                                    staged_path=staged))

    summary = iceberg_load.load_and_record(
        run_extraction_fn=run_extraction, tables=[tdef],
        settings=Settings(mode=MODE_INCREMENTAL, progress_enabled=False,
                          snapshot_maintenance=False),
        control=iceberg_load.ControlStore(pg_meta), run_id="test-run",
        total_branches=1, branches_in_run=1)

    assert built == ["oracle_to_iceberg__foo"]
    assert not pipeline.has_pending_data          # swept before the load
    assert summary.plans[0].load_status == "SUCCESS"   # 0-row skip path
```

- [ ] **Step 7: Run everything**

Run: `.venv/Scripts/python.exe -m pytest tests/test_parallel_load_dispatch.py tests/test_load_workers_setting.py tests/test_load_timeout.py tests/test_pending_packages.py -v`
Expected: PASS (pg_meta tests skip without `OASIS_TEST_PG_DSN`).

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass. `grep -n "_PipelineHolder\|tempfile" etl/iceberg_load.py` must return nothing.

- [ ] **Step 8: Commit**

```bash
git add etl/config.py etl/iceberg_load.py tests/test_load_workers_setting.py tests/test_parallel_load_dispatch.py tests/test_load_timeout.py tests/test_pending_packages.py
git commit -m "feat(load): parallel per-table load pipelines (load_workers)" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Surface — CLI flag, GUI key, metastore timeout, runbook

**Files:**
- Modify: `oracle_to_iceberg.py` — `parse_args` (~line 56, next to the other worker flags), `build_overrides` (~line 87)
- Modify: `gui/workspace.py:78` — `EDITABLE_ETL_KEYS`
- Modify: `etl/metastore.py:97` — engine `connect_args`
- Modify: `README.md` — tuning runbook subsection
- Test: existing `tests/test_settings_ui_keys.py`, `tests/test_metastore.py`

**Interfaces:**
- Consumes: `Settings.load_workers` (Task 8; `build_overrides` may pass `None` — `load_settings` keeps the config default then).
- Produces: `--load-workers` CLI flag; `"load_workers"` editable from the GUI Settings page; MetaStore connections fail fast.

- [ ] **Step 1: CLI flag**

In `parse_args`, after the `--max-table-workers` line:

```python
    p.add_argument("--load-workers", type=int,
                   help="parallel table loads (per-table pipelines; default 2, "
                        "1 = serial)")
```

In `build_overrides`'s dict, after `"max_table_workers"`:

```python
        "load_workers": args.load_workers,
```

- [ ] **Step 2: GUI editable key**

In `gui/workspace.py`, line 78, extend the load-tuning row:

```python
    "load_batch_rows", "load_group_max_bytes", "load_commit_timeout_s",
    "load_workers",
```

- [ ] **Step 3: Metastore connect timeout**

In `etl/metastore.py::MetaStore.__init__`:

```python
        # pool_pre_ping revalidates pooled connections; connect_timeout keeps a
        # wedged Postgres (proxy accepts, server never answers) from hanging
        # the run -- with parallel load workers every worker would block on it.
        self.engine: Engine = create_engine(
            cfg.sqlalchemy_url(), pool_pre_ping=True,
            connect_args={"connect_timeout": 10})
```

- [ ] **Step 4: README runbook**

In `README.md`, add this subsection alongside the existing `[etl]` tuning documentation (search for `load_group_max_bytes`; if it is not documented there yet, add the subsection near the configuration docs):

```markdown
### Parallel table loads

`etl.load_workers` (default 2) sets how many tables may load into Iceberg
concurrently, each through its own per-table dlt pipeline
(`oracle_to_iceberg__<table>`), all landing in the same bucket/dataset.

- Peak load RSS scales roughly linearly with workers: each in-flight table
  can materialize up to `load_group_max_bytes` of staged parquet (x3-6 once
  decoded to Arrow). When raising `load_workers`, lower
  `load_group_max_bytes` to compensate.
- `load_workers = 1` restores the previous strictly-serial load behavior.
- Each concurrent load also runs pyiceberg's internal write pool; on small
  servers cap it with the `PYICEBERG_MAX_WORKERS` environment variable.
- Concurrent loads open more Postgres connections (Iceberg catalog + app
  metastore). Add `connect_timeout` to the catalog URI in `.dlt/secrets.toml`
  (e.g. `...?connect_timeout=10`) so a wedged Postgres fails fast; the app
  metastore already sets its own 10s timeout.
```

- [ ] **Step 5: Run the guard tests, then the full suite**

Run: `.venv/Scripts/python.exe -m pytest tests/test_settings_ui_keys.py tests/test_metastore.py -v`
Expected: PASS (or SKIP for Postgres-gated metastore tests).

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass.

Run: `.venv/Scripts/python.exe oracle_to_iceberg.py --help`
Expected: `--load-workers` appears in the help text.

- [ ] **Step 6: Commit**

```bash
git add oracle_to_iceberg.py gui/workspace.py etl/metastore.py README.md
git commit -m "feat(etl): expose load_workers (CLI/GUI); metastore connect timeout" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Final verification

- [ ] Full suite: `.venv/Scripts/python.exe -m pytest -q` — all pass/skip, zero failures.
- [ ] Offline smoke: `.venv/Scripts/python.exe oracle_to_iceberg.py --mode INITIAL --self-test --load-workers 2` — completes; log shows per-table pipeline names and `load[N]:` heartbeat labels; re-run with `--load-workers 1` — completes identically (serial).
- [ ] `git log --oneline main..` shows one commit per task.

## Validation after merge (manual, on the server)

Not part of the automated tasks — record results in the run log / memory:

1. Deploy the branch to `/home/bi/workspace/dlt` (no new dependencies). Add `?connect_timeout=10` to the `[iceberg_catalog]` URI in `.dlt/secrets.toml`.
2. Run one INITIAL with `load_workers = 2` and progress on. Compare wall-clock and peak RSS against the 2026-07-20 baseline (serial load thread 92–98% busy). Watch the `load[2]:` heartbeat and the two-peak RSS profile; if RSS is too high, lower `etl.load_group_max_bytes` — no code change needed.
3. Confirm first-run correctness of the catalog-read path on existing tables: an INCREMENTAL merge after cutover must carry forward `insert_at` (spot-check a merged row) and must NOT expire prior snapshot history (snapshot count per table only squashes this run's intermediates).
4. If load overlap is poor (workers idle), check whether commits serialize on Postgres; only then consider raising `load_workers` to 3 with a proportionally smaller group budget.
5. The old shared working dir `~/.dlt/pipelines/oracle_to_iceberg` is inert after cutover and may be deleted.
