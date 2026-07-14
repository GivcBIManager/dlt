# Query-Based Sources in tables.json — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow a tables.json entry to use an Oracle inline-view subquery as its source, with a required `name` key that becomes the Iceberg table name.

**Architecture:** The extraction SQL already renders `FROM {table} t`, and Oracle accepts `FROM (SELECT ...) t` as an inline view — so the SQL builders need no structural change. The work is: (1) `TableDef` learns `name` / `is_query` and derives its identifiers from `name` for query entries, (2) the loader requires `name` on query entries, (3) the GUI validator and edit form learn the `name` key, (4) docs.

**Tech Stack:** Python 3 dataclasses, pytest, Flask/Jinja GUI (vanilla JS), Oracle SQL.

**Spec:** `docs/superpowers/specs/2026-07-14-query-based-sources-design.md`

## Global Constraints

- `name` is **required** when `table` starts with `(` (a subquery), **rejected** on plain-table entries.
- `name` is normalized with the existing `etl.config._normalize_name` (lower-snake) to form `dataset_table_name`.
- Plain-table entries must behave byte-for-byte as today (backward compatible).
- Run tests with the repo venv from `d:\dlt`: `python -m pytest <file> -v` (use `.venv\Scripts\python` if `python` is not the venv).
- Commit after every green task; commit messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: TableDef `name` / `is_query` + loader validation

**Files:**
- Modify: `etl/config.py` (TableDef ~lines 105-199, `load_table_defs` ~lines 363-385)
- Test: `tests/test_query_sources.py` (create)

**Interfaces:**
- Consumes: existing `TableDef`, `load_table_defs`, `_normalize_name` in `etl/config.py`.
- Produces: `TableDef.name: Optional[str] = None` (new last field), `TableDef.is_query -> bool` (property), `TableDef.object_name` returns `name` for query entries, `TableDef.owner` returns `""` for query entries, `load_table_defs` raises `ValueError` for a query entry without `name`. Tasks 2-3 rely on exactly these names.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_query_sources.py`:

```python
"""Query-based sources: an inline-view subquery in ``table`` plus an explicit
``name`` that becomes the Iceberg table / staging / control-state name."""
from __future__ import annotations

import json

import pytest

from etl.config import (
    CATEGORY_TRANSACTION,
    TableDef,
    load_table_defs,
)

QUERY = (
    "(SELECT v.VISIT_ID, v.AMEND_LAST_DATE, v.VISIT_DATE, m.STATUS "
    "FROM OASIS.VISITS v JOIN OASIS.VISIT_MASTER m ON m.VISIT_ID = v.VISIT_ID)"
)


def _tdef(**over) -> TableDef:
    kw = dict(
        table=QUERY,
        unique_key="VISIT_ID",
        cdc_column="AMEND_LAST_DATE",
        where_date_column="VISIT_DATE",
        where_operator=None,
        where_value_of_initial_run=None,
        category=CATEGORY_TRANSACTION,
        name="visits_enriched",
    )
    kw.update(over)
    return TableDef(**kw)


# --- TableDef identifiers --------------------------------------------------- #
def test_is_query_true_for_subquery():
    assert _tdef().is_query is True


def test_is_query_false_for_plain_table():
    assert _tdef(table="OASIS.VISITS").is_query is False


def test_dataset_table_name_comes_from_name():
    assert _tdef().dataset_table_name == "visits_enriched"


def test_dataset_table_name_is_normalized():
    assert _tdef(name="Visits-Enriched").dataset_table_name == "visits_enriched"


def test_object_name_is_name_for_query_entry():
    # --tables CLI filter matches on object_name, so query entries match by name
    assert _tdef().object_name == "visits_enriched"


def test_owner_empty_for_query_entry():
    assert _tdef().owner == ""


def test_plain_table_derives_names_from_identifier():
    t = _tdef(table="OASIS.VISITS", name=None)
    assert t.owner == "OASIS"
    assert t.object_name == "VISITS"
    assert t.dataset_table_name == "visits"


# --- loader ------------------------------------------------------------------ #
def _write_tables_json(tmp_path, entry):
    p = tmp_path / "tables.json"
    p.write_text(json.dumps({"transactions": [entry]}), encoding="utf-8")
    return p


def test_load_table_defs_query_entry(tmp_path):
    p = _write_tables_json(tmp_path, {
        "table": QUERY,
        "name": "visits_enriched",
        "unique_key": "VISIT_ID",
        "cdc_column": "AMEND_LAST_DATE",
        "where_date_column": "VISIT_DATE",
    })
    (tdef,) = load_table_defs(p)
    assert tdef.is_query
    assert tdef.dataset_table_name == "visits_enriched"


def test_load_table_defs_query_entry_without_name_raises(tmp_path):
    p = _write_tables_json(tmp_path, {"table": QUERY, "unique_key": "VISIT_ID"})
    with pytest.raises(ValueError, match="name"):
        load_table_defs(p)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_query_sources.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'name'` (or similar) on most tests.

- [ ] **Step 3: Implement in `etl/config.py`**

3a. Add the field at the end of `TableDef` (after `where_operator_max: Optional[str] = None`):

```python
    # Explicit Iceberg table name. Only meaningful (and required) when ``table``
    # is an inline-view subquery source -- plain tables keep deriving their name
    # from the OWNER.TABLE identifier.
    name: Optional[str] = None
```

3b. Replace the `owner` / `object_name` properties (currently lines 128-134) with:

```python
    @property
    def is_query(self) -> bool:
        """True when ``table`` is an inline-view subquery, not an identifier."""
        return self.table.lstrip().startswith("(")

    @property
    def owner(self) -> str:
        if self.is_query:
            return ""
        return self.table.split(".", 1)[0] if "." in self.table else ""

    @property
    def object_name(self) -> str:
        if self.is_query:
            return self.name or ""
        return self.table.split(".", 1)[1] if "." in self.table else self.table
```

`dataset_table_name` stays unchanged — it normalizes `object_name`, which now yields `name` for query entries.

3c. In `load_table_defs`, build the def into a local, validate, then append (replace the current `defs.append(TableDef(...))` block):

```python
        for entry in data.get(category, []):
            tdef = TableDef(
                table=entry["table"],
                unique_key=entry.get("unique_key"),
                cdc_column=entry.get("cdc_column"),
                where_date_column=entry.get("where_date_column"),
                where_operator=entry.get("where_operator"),
                where_value_of_initial_run=entry.get("where_value_of_initial_run"),
                category=category,
                helper=_parse_helper(entry),
                where_value_max=entry.get("where_value_max"),
                where_operator_max=entry.get("where_operator_max"),
                name=entry.get("name"),
            )
            if tdef.is_query and not (tdef.name or "").strip():
                raise ValueError(
                    f"{category} entry uses a subquery source and requires a "
                    f"'name' (the Iceberg table name): {entry['table'][:80]}"
                )
            defs.append(tdef)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_query_sources.py -v`
Expected: all PASS.

Then run the full suite to catch regressions (the `owner`/`object_name` rewrite must not change plain-table behavior):

Run: `python -m pytest tests/ -q`
Expected: all PASS (same pass count as before this task, plus the new tests).

- [ ] **Step 5: Commit**

```bash
git add etl/config.py tests/test_query_sources.py
git commit -m "feat(etl): query-based sources with explicit Iceberg name in TableDef"
```

---

### Task 2: SQL-rendering tests for query entries (test-only)

The SQL builders need no code change — this task pins that claim with tests so a future refactor can't silently break inline-view sources.

**Files:**
- Test: `tests/test_query_sources.py` (extend)

**Interfaces:**
- Consumes: `TableDef` from Task 1; `etl.oracle_extract.build_query(tdef, settings, cdc_wm, date_wm) -> str`; `etl.oracle_extract.Watermark(value=..., kind=...)`; `etl.config.Settings(mode=...)`, `MODE_INITIAL`, `MODE_INCREMENTAL`, `CATEGORY_SNAPSHOT`.
- Produces: nothing new — regression coverage only.

- [ ] **Step 1: Add the tests**

Append to `tests/test_query_sources.py` (and extend the existing import from `etl.config` with `CATEGORY_SNAPSHOT, MODE_INCREMENTAL, MODE_INITIAL, Settings`):

```python
from etl.oracle_extract import Watermark, build_query


# --- SQL rendering: the subquery is a valid Oracle inline view --------------- #
def test_build_query_initial_wraps_inline_view():
    tdef = _tdef(where_operator=">=", where_value_of_initial_run="2026-06-01")
    settings = Settings(mode=MODE_INITIAL)
    q = build_query(tdef, settings, Watermark(value=None), Watermark(value=None))
    assert q.startswith(f"SELECT t.* FROM {QUERY} t")
    assert "t.VISIT_DATE >= TO_DATE('2026-06-01', 'YYYY-MM-DD')" in q


def test_build_query_incremental_union_uses_inline_view():
    tdef = _tdef()
    settings = Settings(mode=MODE_INCREMENTAL)
    cdc_wm = Watermark(value="2026-07-01 00:00:00.000000", kind="datetime")
    date_wm = Watermark(value="2026-07-01 00:00:00.000000", kind="datetime")
    q = build_query(tdef, settings, cdc_wm, date_wm)
    # both UNION ALL branches select from the inline view
    assert q.count(f"FROM {QUERY} t") == 2
    assert "UNION ALL" in q
    assert "t.AMEND_LAST_DATE >" in q
    assert "t.VISIT_DATE >=" in q


def test_build_query_snapshot_full_copy_of_query():
    tdef = _tdef(category=CATEGORY_SNAPSHOT, unique_key="",
                 cdc_column=None, where_date_column=None)
    settings = Settings(mode=MODE_INCREMENTAL)
    q = build_query(tdef, settings, Watermark(value=None), Watermark(value=None))
    assert q == f"SELECT * FROM {QUERY}"
```

- [ ] **Step 2: Run the tests**

Run: `python -m pytest tests/test_query_sources.py -v`
Expected: all PASS on the first run (the builders already handle inline views). If any fail, the builder has a real gap — fix it in `etl/oracle_extract.py`, do not weaken the test.

- [ ] **Step 3: Commit**

```bash
git add tests/test_query_sources.py
git commit -m "test(etl): pin inline-view SQL rendering for query-based sources"
```

---

### Task 3: GUI validator (`tables_store.py`) learns `name`

**Files:**
- Modify: `gui/tables_store.py` (KNOWN_KEYS ~line 54, `_validate_entry` ~lines 75-114, `validate` duplicate check ~line 191)
- Test: `tests/test_tables_store_validation.py` (extend + update one existing test)

**Interfaces:**
- Consumes: existing `tables_store.validate(doc) -> list[str]`, `_IDENT_RE`.
- Produces: `validate` accepts a `name` key; requires it on subquery entries; rejects it on plain-table entries; rejects non-identifier names; duplicate detection keys on `name or table`. Task 4's form relies on these exact rules.

- [ ] **Step 1: Update the existing test + add failing tests**

In `tests/test_tables_store_validation.py`, the existing subquery test currently passes with no `name` — update it (it must now carry a name):

```python
def test_inline_view_subquery_table_ok():
    # a source table may be an inline-view subquery (raw-SQL escape hatch);
    # it must carry an explicit Iceberg 'name'
    doc = _doc(table="(SELECT ID, AMT FROM DEVDBA.GL_INTERFACE WHERE AMT > 0)",
               name="gl_interface_positive")
    assert _errs(doc) == []
```

Append a new section:

```python
# --- query-based sources: 'name' key ---------------------------------------- #
def test_subquery_without_name_rejected():
    errs = _errs(_doc(table="(SELECT ID, AMT FROM DEVDBA.GL_INTERFACE)"))
    assert errs and any("'name'" in e for e in errs)


def test_name_on_plain_table_rejected():
    errs = _errs(_doc(name="renamed"))
    assert errs and any("'name'" in e for e in errs)


def test_invalid_name_rejected():
    errs = _errs(_doc(table="(SELECT 1 AS ID FROM DUAL)", name="bad name;"))
    assert errs


def test_duplicate_by_name_rejected():
    e1 = {"table": "(SELECT 1 AS ID FROM DUAL)", "name": "same",
          "unique_key": "ID"}
    e2 = {"table": "(SELECT 2 AS ID FROM DUAL)", "name": "SAME",
          "unique_key": "ID"}
    doc = {"masters": [e1, e2], "transactions": [], "snapshots": []}
    assert any("Duplicate" in e for e in _errs(doc))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_tables_store_validation.py -v`
Expected: the four new tests FAIL (`name` is an unknown key today, and no name-requirement exists); `test_inline_view_subquery_table_ok` also FAILS (unknown key 'name').

- [ ] **Step 3: Implement in `gui/tables_store.py`**

3a. Add `"name"` to `KNOWN_KEYS`:

```python
KNOWN_KEYS = {
    "table",
    "name",
    "unique_key",
    "cdc_column",
    "where_date_column",
    "where_operator",
    "where_value_of_initial_run",
    "where_value_max",
    "where_operator_max",
    "helper",
}
```

3b. In `_validate_entry`, replace the opening block (from `table = str(...)` through the `_is_valid_table_ref` check) with:

```python
    table = str(entry.get("table") or "").strip()
    if not table:
        errs.append(f"{where}: 'table' is required (e.g. OASIS.MY_TABLE)")
    is_query = table.startswith("(")
    iceberg_name = str(entry.get("name") or "").strip()
    name = iceberg_name or table or where
    if table and not _is_valid_table_ref(table):
        errs.append(f"{name}: 'table' must be a valid identifier (e.g. OASIS.MY_TABLE)")

    # 'name' is the Iceberg table name for subquery sources. Plain tables derive
    # their name from the identifier, so a stray 'name' there is ambiguous.
    if is_query and not iceberg_name:
        errs.append(f"{name}: subquery sources require 'name' (the Iceberg table name)")
    if iceberg_name and not is_query:
        errs.append(f"{name}: 'name' is only allowed when 'table' is a subquery")
    if iceberg_name and not _IDENT_RE.match(iceberg_name):
        errs.append(f"{name}: 'name' must be a plain identifier (e.g. visits_enriched)")
```

3c. In `validate`, key duplicate detection on the effective name (replace the `t = str((entry or {}).get("table") ...)` line):

```python
            t = str((entry or {}).get("name")
                    or (entry or {}).get("table") or "").strip().upper()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_tables_store_validation.py -v`
Expected: all PASS.

Run: `python -m pytest tests/ -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add gui/tables_store.py tests/test_tables_store_validation.py
git commit -m "feat(gui): validate 'name' key for query-based sources in tables.json"
```

---

### Task 4: GUI edit form + README docs

No JS test infra exists in this repo — verification is the pytest suite (unchanged) plus a form-logic walkthrough in the browser if the GUI is running. Keep the JS mirror of the validator rules exact.

**Files:**
- Modify: `gui/templates/tables.html` (modal ~line 57, `summarize` ~line 134, `renderList` ~lines 152-156, `openEdit` ~line 209, `applyEdit` ~lines 226-232)
- Modify: `README.md` (section "1. Configuration", ~lines 93-131)

**Interfaces:**
- Consumes: validator rules from Task 3 (`name` required iff subquery, plain identifier).
- Produces: user-facing form support; no code interfaces.

- [ ] **Step 1: Add the form field**

In the modal, directly under the Table input (line 57), add:

```html
    <div><label>Table (OWNER.NAME or subquery) *</label><input id="f-table" title="e.g. OASIS.MY_TABLE or (SELECT ...) inline-view subquery"></div>
    <div><label>Iceberg name (subquery sources only)</label><input id="f-name" title="e.g. visits_enriched — names the Iceberg table when Table is a (SELECT ...) subquery"></div>
```

(The first line replaces the existing `f-table` row — only its label/title text changes.)

- [ ] **Step 2: Wire load/save + list rendering**

In `openEdit`, after `el("f-table").value = e.table || "";` add:

```js
  el("f-name").value = e.name || "";
```

In `applyEdit`, after the `if (!key && ...)` guard and before `const e = { table };`, add the mirror of the validator rules:

```js
  const nm = el("f-name").value.trim();
  const isQuery = table.startsWith("(");
  if (isQuery && !nm) return err("Iceberg name is required for subquery sources");
  if (!isQuery && nm) return err("Iceberg name is only used when Table is a subquery");
```

and after `const e = { table };` add:

```js
  if (nm) e.name = nm;
```

In `renderList`, show and filter by the effective name — replace the `.filter(...)` line and the card title line:

```js
  const shown = items.map((e, i) => ({ e, i }))
    .filter(({ e }) => matchFilter(e.name || e.table, q));
```

```js
        <b class="mono">${esc(e.name || e.table || "(unnamed)")}</b>
```

In `summarize`, flag query entries — add as the first line of the function body:

```js
  if ((e.table || "").trim().startsWith("(")) bits.push("source=subquery");
```

(note: `bits` must already be declared; keep `const bits = [];` first, then this line.)

- [ ] **Step 3: Document in README.md**

In "### 1. Configuration", after the helper-driven CDC bullet (ends ~line 131), add:

```markdown
- **Query-based sources.** An entry's `table` may be an Oracle inline-view
  subquery instead of a physical table. Such an entry must also set `name`,
  which becomes the Iceberg table name (lower-snake normalized). All other
  options (`unique_key`, `cdc_column`, `where_date_column`, INITIAL range,
  category) work as usual, with one constraint: those columns must be
  **projected by the subquery**, because filters are rendered as `t.<col>`
  against the inline view. Do not project a column named `BRANCH_ID` (it
  collides with the injected branch column — alias it inside the subquery).
  `name` is only allowed on subquery entries; plain tables keep deriving
  their name from `OWNER.TABLE`.

  ```json
  {
    "table": "(SELECT v.VISIT_ID, v.AMEND_LAST_DATE, v.VISIT_DATE, m.STATUS FROM OASIS.VISITS v JOIN OASIS.VISIT_MASTER m ON m.VISIT_ID = v.VISIT_ID)",
    "name": "visits_enriched",
    "unique_key": "VISIT_ID",
    "cdc_column": "AMEND_LAST_DATE",
    "where_date_column": "VISIT_DATE"
  }
  ```
```

- [ ] **Step 4: Verify**

Run: `python -m pytest tests/ -q`
Expected: all PASS (no Python behavior changed in this task).

If the GUI is running, open the Tables page and check: adding an entry with `table = (SELECT 1 AS ID FROM DUAL)` and no Iceberg name is blocked with "Iceberg name is required for subquery sources"; with a name it saves and the card shows the name with a `source=subquery` tag; a plain table with a name is blocked.

- [ ] **Step 5: Commit**

```bash
git add gui/templates/tables.html README.md
git commit -m "feat(gui): Iceberg-name form field for query-based sources + docs"
```
