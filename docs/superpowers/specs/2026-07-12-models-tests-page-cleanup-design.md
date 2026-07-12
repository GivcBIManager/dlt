# Models & Tests page cleanup — design note

**Date:** 2026-07-12
**Branch:** feat/flow-builder
**Scope:** `gui/templates/dbt.html`, `gui/dbt_project_store.py`, `gui/app.py`

A focused refinement of the **Models & Tests** page (`/models`). Five user asks, all
on this one page. Config editing moves out of the UI; the file lists gain metadata;
new-file creation moves inline into the editor with a materialization dropdown.

## Requirements

1. **Config is backend-only.** Remove the "ClickHouse & dbt settings" form from the
   UI. Creds stay in `.dlt/secrets.toml [clickhouse]` and dbt knobs in the workspace
   settings, edited via config files. `/api/dbt/config` GET/PUT stay server-side
   (unused by this page, still available); nothing removed server-side.
2. **Health check status only.** Replace the settings panel with a slim status strip:
   a `Connection health` label + a status pill. Auto-runs the existing
   `POST /api/dbt/test-connection` (`dbt debug`) on page load — pill shows
   `checking…` → `connected` / `failed`. On **failure only**, a collapsible
   `<details>` shows the dbt output. A `Recheck` button re-runs it.
3. **Metadata lists.** Models & Tests render as tables with columns
   **Name · Type · Modified · Created · Size** (via the shared `renderTable`/`fmtBytes`/
   `fmtDate` helpers). Name stays a click-to-open control.
4. **New file named in the editor, no popup.** Clicking **New model/New test** enters
   "new-file mode" in the editor panel: inline name input (focused), the editor
   preloads the rendered starter template, and **Save file** creates it. No
   `prompt()`.
5. **Materialization dropdown when creating a model.** In new-model mode, a dropdown
   (`table` · `view` · `incremental` · `ephemeral`, default `table`) sits by the name
   input. Changing it rewrites the `config(materialized='…')` line in the editor in
   place (works after edits too). Tests get no dropdown. The chosen value is what the
   list's `Type` column shows.

## Backend changes (`dbt_project_store.py`)

- `_scan(subdir)` enriches each entry with `size` (bytes), `modified` / `created`
  (ISO-8601 local, from `os.stat` mtime / ctime), and a `type`:
  - models: parsed `config(materialized='…')`, fallback `view` (dbt's global default);
  - tests: `"test"`.
  - `dbt ls`-only entries (no file on disk) keep null metadata → rendered as `—`.
- `template_for(kind, name, materialization)` returns the rendered template text
  (single source for the frontend preview). Backs a new route.
- `create_from_template(name, kind, materialization="table", content=None)`: when
  `content` is non-blank, write it verbatim; else render the template. Name
  sanitizing / extension / exists checks unchanged.

## Backend changes (`app.py`)

- `GET /api/dbt/template?kind=&materialization=&name=` → `{content}`.
- `POST /api/dbt/file` passes an optional `content` through to
  `create_from_template`.

## Frontend (`dbt.html`)

- Remove the settings panel + `loadConfig`/`saveConfig` + their bindings.
- Add the health strip (auto-check on load, `Recheck`, failure `<details>`).
- Render Models/Tests via `renderTable` with the metadata columns.
- New-file mode: name input + (models) materialization `<select>`, template preload
  from `/api/dbt/template`, dropdown rewrites the `materialized='…'` token in place,
  Save posts `{name, kind, materialization, content}`.

## Testing

- Extend `tests/test_dbt_project_store.py`: metadata fields present and typed;
  materialization parsed from `config()`; `template_for` renders the chosen
  materialization; `create_from_template` honors `content` and falls back to template.
- Extend `tests/test_dbt_api.py`: `GET /api/dbt/template` returns content; `POST`
  with `content` writes verbatim.
- Frontend is exercised via the running app (verify skill).

## Non-goals

- No change to how `dbt run`/`test`/`compile`/`debug` are executed or tailed.
- No new config surface; removing the form does not remove the config endpoints.
