# Codebase Fix Plan — 2026-07-13

> **Status:** Phase 1, Phase 2, and Phase 0 are implemented and tested
> (commit `9f8dd0e` for Phase 1+2). Phase 0 adds `gui/security.py` (bind/auth/
> command-lockdown/backup helpers), a `before_request` auth gate + CSRF
> content-type check, fail-closed bind + debugger gate in `main()`, secret-backup
> hardening (0600 + prune) across the config writers, and SQL-identifier
> validation in `tables_store`. Deferred: the `/api/dagster/{status,stop}` routes
> and the empty `orchestrator/.../defs/` dg-scaffold (kept intentionally).
>
> **Phase 3 (complete except 3.2):** done — 3.1 (drop redundant DQ COUNT scan),
> 3.3 (vectorized decimal canonicalizer), 3.4 (stream cursor-fallback to parquet,
> also fixing the cursor leak), 3.5 (push run-id filter into the run-detail scan;
> cache `dbt ls` on mtime; offset-tail the logs page), 3.6 (`pa.repeat` for
> constant columns). **Skipped 3.2** (drop per-row hashing): no dependency-free
> vectorized crypto hash is available and dropping the digest would raise
> join-table memory, the binding constraint here. Note: 3.4's streaming was
> verified with a fake cursor (batching, empty-schema, injected columns); it can't
> be exercised end-to-end without a live Oracle result that trips the fallback.
>
> **Phase 4 (complete):** 4.1 shared `gui/toml_edit.py` helper (connections /
> smtp / clickhouse delegate their read + backup + validate + atomic-replace +
> prune to it; workspace's config.toml writer left as-is by design), 4.2
> `workspace.list_branches` delegates to `connections.list_connections` (one
> canonical redacted shape), 4.3 `matchFilter` moved into `app.js` and removed
> from the three templates.
>
> **Phase 5 (mostly complete):** 5.1 resilient live-tail (run + dbt: retry +
> reconnect banner), 5.2 editor dirty-state guards (dbt + tables, beforeunload),
> 5.3 initial-load error handling (dbt lists, flows), 5.4 flow-node validation +
> ✕ delete button, 5.6 consistency wins (Stop confirm/‘Stopping…’, inline SMTP
> recipient, row-Test feedback, fixed dead ‘No snapshots’ branch, out-of-band CSV
> export, raw-JSON-toggle icon), plus removed two dead topbar controls. 5.7 did
> the high-value a11y subset: modal `role="dialog"`/Esc/backdrop (Esc now beats the
> sidebar) and accessible names on the ↻ buttons.
>
> **Follow-up pass (previously-skipped items):** 3.2 revisited — the DQ payload
> digest is now a compact 16-byte binary instead of a 32-char hex string (halves
> the hash column in the accumulated key/hash tables and the join; memory is the
> binding constraint), applied in both `dq_check` and `snapshot_diff` and verified
> through `_compare`. The remaining a11y is done: a load-time pass in `app.js`
> derives `<label for=>` from the app's `label`→control sibling pairs, clickable
> rows are keyboard-activatable (`tabindex`/`role` + Enter/Space), and decorative
> Material Symbols icons are `aria-hidden`. The stale `test_dbt_scaffold` was fixed
> (scan any model for the icebergLocal/ClickHouse pattern instead of a renamed
> filename) — the suite is now fully green (212 passed). **Still deferred:** 5.5
> vendor fonts/icons — needs the WOFF2 binaries fetched + committed, not feasible
> in this environment; Inter/JetBrains already fall back to system fonts.

Derived from the full-codebase review (security/bad-practice, performance, UI/UX,
redundancy/dead-code, Windows↔Linux portability). Ordered into phases by
risk-to-ship and blast radius. Each item lists the file(s), the concrete change,
and how to verify.

Guiding principle: **Phase 0 first** (security — the app is currently an
unauthenticated RCE surface). Phases 1–2 are low-risk, high-value and can land in
any order. Phases 3–5 are larger and should be TDD'd.

---

## Phase 0 — Security (do first, blocks safe deployment)

### 0.1 Authenticate the GUI + lock down arbitrary command execution
- **Files:** `gui/app.py` (all routes), `gui/commands.py:64-68` (`build_argv` `custom` path), `gui/build.py:36-41`.
- **Change:**
  - Add a shared-token auth gate (e.g. `OASIS_GUI_TOKEN`) enforced by a Flask
    `before_request` hook on all `/api/*` and mutating routes; reject when unset
    unless bound to loopback.
  - Bind to `127.0.0.1` by default; only honor `OASIS_GUI_HOST=0.0.0.0` when a
    token is configured (fail closed with a clear log message otherwise).
  - Replace the free-form `custom` script with an allowlist of known scripts, OR
    require an explicit `OASIS_ALLOW_CUSTOM_CMD=1` opt-in that is refused unless
    authenticated + loopback.
- **Verify:** unauth `POST /api/run` returns 401; `POST /api/run` with
  `script=custom` refused unless explicitly enabled; existing tests still pass.
- **Neutralizes:** RCE, the CSRF angle (0.3), and the SQL-injection-via-config
  angle (0.4) in one move.

### 0.2 Never expose the Werkzeug debugger
- **File:** `gui/app.py:601,609`.
- **Change:** refuse `OASIS_GUI_DEBUG=1` when host is non-loopback; document it as
  local-only.
- **Verify:** starting with debug + non-loopback host logs a refusal and disables
  the debugger.

### 0.3 CSRF protection on state-changing endpoints
- **File:** `gui/app.py` (`_body()` and all POST/PUT/DELETE handlers).
- **Change:** require `Content-Type: application/json` and a non-simple header
  (e.g. `X-Requested-With`) so cross-site simple-form POSTs are rejected; front-end
  fetches already send JSON so the client change is minimal. (Auth token in 0.1
  also covers this; do both for defense in depth.)
- **Verify:** a form-encoded cross-origin POST is rejected; the app's own fetches
  still work.

### 0.4 Validate config-driven SQL identifiers
- **Files:** `etl/oracle_extract.py:118-317` (`format_initial_value`, query
  builders), `etl/dq_check.py:556-568`, `gui/tables_store.py` (`validate`).
- **Change:** validate table/column/`unique_key` fields against an identifier
  regex; treat `where_value_*`/expression fields as privileged (only settable by an
  authenticated user). Reject on save in `tables_store.validate`, not just at run
  time.
- **Verify:** a `tables.json` entry with `1=1; drop` in a key field is rejected on
  PUT.

### 0.5 Protect and prune plaintext secret backups
- **Files:** `gui/connections.py:140`, `gui/smtp_config.py:85`,
  `gui/clickhouse_config.py:90` (and the shared helper from 4.1 once it exists).
- **Change:** `chmod 0600` on backup files (POSIX), cap to N most-recent backups,
  prune older ones. Consider moving backups outside the web-served tree.
- **Verify:** after several writes only N backups remain; POSIX mode is 0600.

---

## Phase 1 — Portability blockers (trivial, unblock Linux)

### 1.1 Executable bit on shell scripts
- **Files:** `setup.sh`, `start-app.sh`, `stop-app.sh` (currently `100644`).
- **Change:** `git update-index --chmod=+x setup.sh start-app.sh stop-app.sh`.
- **Verify:** `git ls-files -s *.sh` shows `100755`.

### 1.2 Add `.gitattributes` to pin line endings
- **File:** new `.gitattributes`.
- **Change:**
  ```
  *.sh  text eol=lf
  *.ps1 text eol=crlf
  *.cmd text eol=crlf
  ```
  then `git add --renormalize .`.
- **Verify:** `.sh` files check out with LF; non-git tarball deploy no longer ships
  `bash\r`.

### 1.3 `encoding="utf-8", errors="replace"` on subprocess text pipes
- **Files:** `orchestrator/src/orchestrator/assets.py:33`,
  `gui/dbt_project_store.py:107`.
- **Change:** add `encoding="utf-8", errors="replace"` to the `Popen`/`run` calls.
- **Verify:** non-ASCII child output no longer raises `UnicodeDecodeError` under a
  C/POSIX locale.

### 1.4 Detached-run stop should signal the process group on POSIX
- **File:** `gui/pipeline_runner.py:149-151`.
- **Change:** on POSIX use `os.killpg(pid, SIGTERM)` (child is a session leader) with
  `os.kill` fallback.
- **Verify:** stopping a GUI-restart-surviving run terminates its children too.

### 1.5 Fix `fresh_run.cmd` corrupted first line
- **File:** `fresh_run.cmd:1` — `re@echo off` → `@echo off`.
- **Verify:** running it no longer prints `'re@echo' is not recognized`.

---

## Phase 2 — Quick cleanup (low-risk deletions & config fixes)

### 2.1 Delete duplicate / broken / dead files
- `gui/Logo English.svg` (byte-identical to `gui/static/logo.svg`, referenced
  nowhere).
- `dbt/tests/product_cout_test.sql` (typo name, now broken vs rewritten model);
  commit the new `dbt/tests/products_count_test.sql` in its place.
- `orchestrator/src/orchestrator/defs/` (empty package, referenced nowhere).
- **Verify:** grep confirms zero references; app + tests still pass.

### 2.2 Remove dead code
- `gui/config.py:41` `SCHEDULES_JSON` + stale `gui/state/schedules.json`.
- `gui/app.py:348,360` unused `/api/dagster/status` and `/api/dagster/stop` routes
  (confirm no external monitor depends on them before removing).
- `gui/static/app.js:129` `_toastTimer`.
- `orchestrator/pyproject.toml:28` `registry_modules` pointing at a nonexistent
  module.
- **Verify:** grep shows no callers; tests pass.

### 2.3 Config default mismatches
- `etl/config.py:282` `progress_interval_s=10.0` vs `:487` `load_settings` passes
  `5.0` — pick one.
- `etl/config.py:244-245` `pool_backoff_base_s`/`pool_backoff_cap_s` not wired into
  the `[etl]` mapping in `load_settings` — add them.
- **Verify:** a `[etl] progress_interval_s` override is reflected in `Settings`.

### 2.4 Decide `tables.json` tracking
- `tables.json` is runtime-mutated but tracked (app dirties the repo);
  `tables_template.json` is referenced nowhere.
- **Change:** either wire `tables_template.json` into setup as the seed and untrack
  `tables.json` (`git rm --cached`, add to `.gitignore`), or delete the template.
  Recommend: untrack `tables.json`, seed from template on first run.
- **Verify:** using the app no longer shows `tables.json` as modified in git status.

---

## Phase 3 — Performance (TDD each; measure before/after)

### 3.1 DQ: eliminate the redundant `COUNT(*)` scan  ← highest leverage
- **File:** `etl/dq_check.py:707,721-725`.
- **Change:** when `do_hash` is true, set `oracle_row_count = src_rows` (already
  equal by construction) and skip `_oracle_count`; only issue `COUNT(*)` in the
  `--no-hash` path.
- **Verify:** DQ result row counts unchanged on a known table; one fewer Oracle
  scan per unit (log/trace). Roughly halves DQ Oracle time.

### 3.2 DQ: drop per-row Python hashing
- **Files:** `etl/dq_check.py:191-207`, mirror in `snapshot_diff.py:126-147`.
- **Change:** the digest only shrinks the join key — join on the fingerprint string
  directly, or vectorize (xxhash over Arrow buffers / polars `hash_rows`). Store
  digest as `binary(16)` not hex text if kept.
- **Verify:** identical mismatch detection on a seeded dataset; large CPU/memory
  drop on a wide window.

### 3.3 DQ: vectorize the decimal canonicalizer
- **File:** `etl/dq_check.py:173` (`_canon_array`/`_canon_decimal`).
- **Change:** `pc.cast(col, pa.string())` + regex strip trailing zeros/dot instead
  of per-value `Decimal`.
- **Verify:** canonicalized values byte-identical to the current path on a decimal
  column fixture.

### 3.4 Oracle cursor-fallback: stream to parquet
- **File:** `etl/oracle_extract.py:414-439`, `_stage_via_cursor:572-576`.
- **Change:** restructure around `pq.ParquetWriter`, converting each `fetchmany`
  chunk to an Arrow batch — mirror `_stage_via_arrow`. Also wrap the cursor in
  `try/finally` (fixes the leak from 0.x too).
- **Verify:** a table forced down the fallback path loads without materializing the
  whole result set (memory profile flat).

### 3.5 GUI: cache/curb per-request full scans
- **Files:** `gui/dbt_project_store.py:103-144` (cache `dbt ls` on `models/` mtime,
  or drop enrichment), `gui/iceberg_browser.py:373-530` (push `run_id` into
  `row_filter`, project needed columns, cache rollup on metadata path),
  `gui/templates/logs.html:384` (offset-based tailing like run.html).
- **Verify:** opening Models page no longer spawns two dbt processes; run-detail
  view issues a filtered scan; logs page fetches deltas not the whole file.

### 3.6 Extraction hot-path allocations
- **File:** `etl/oracle_extract.py:463-484` (`inject_columns`).
- **Change:** `pa.repeat(pa.scalar(...), n)` instead of `pa.array([x]*n)`.
- **Verify:** identical output columns; fewer transient Python objects.

---

## Phase 4 — Redundancy consolidation (refactor; keep behavior identical)

### 4.1 Extract a shared `toml_edit` helper
- **Files:** collapse the duplicated backup→validate→atomic-replace machinery from
  `gui/connections.py`, `gui/clickhouse_config.py`, `gui/smtp_config.py`,
  `gui/workspace.py` into one module.
- **Change:** single `read_lines` / `format_value` / `emit_block` / `write` with the
  backup-pruning + 0600 mode from 0.5 baked in.
- **Verify:** existing config tests pass unchanged; each editor delegates to the
  helper.

### 4.2 De-duplicate branch listing
- **Files:** `gui/workspace.py:42-61`, `gui/connections.py:42-67` — one canonical
  `list_branches` returning the redacted shape both callers need.
- **Verify:** `/api/overview`, `/api/branches`, `/api/connections` return identical
  payloads.

### 4.3 Move JS `matchFilter` into `app.js`
- **Files:** remove verbatim copies from `run.html:240`, `tables.html:138`,
  `iceberg.html:105`; export from `gui/static/app.js`.
- **Verify:** wildcard filtering still works on all three pages.

---

## Phase 5 — UI/UX (prioritized by user pain)

### 5.1 Resilient live-tail (top pain)
- **Files:** `gui/templates/run.html:368`, `gui/templates/dbt.html:207`.
- **Change:** retry with backoff (2–3 attempts) before stopping; on give-up show a
  "connection lost — reconnect" banner in the console area instead of freezing.

### 5.2 Dirty-state guards on editors
- **Files:** `gui/templates/dbt.html` (SQL editor), `gui/templates/tables.html`.
- **Change:** track a dirty flag vs loaded content; `confirm` before switching
  files / leaving; `beforeunload` guard; visible unsaved indicator. Fix
  `removeEntry` wording (it's a staged local change, not a server delete).

### 5.3 Error handling on initial loads
- **Files:** `gui/templates/flows.html:289`, `gui/templates/dbt.html:110-116`.
- **Change:** try/catch → in-container error banner with Retry (copy the
  `logs.html` `SysTable` pattern, which is the best-executed data view).

### 5.4 Flows: reject invalid/default-filled nodes on save
- **File:** `gui/templates/flows.html:261-263`.
- **Change:** on save, reject nodes with empty command / unresolved selects with a
  pointed error; add a ✕ delete button to `.dfnode-title` (currently keyboard-only).

### 5.5 Vendor fonts & icons (offline resilience)
- **File:** `gui/templates/base.html:8-12`.
- **Change:** vendor Inter / JetBrains Mono / Material Symbols / Font Awesome
  subsets into `gui/static/` (drawflow is already vendored — same pattern).

### 5.6 Smaller consistency wins
- Confirm + "stopping…" state on Stop (`run.html`).
- Replace the SMTP-test `prompt()` with an inline input (`connections.html:232`).
- Fix the dead "No snapshots" branch (`iceberg.html:187-199`).
- CSV export: disable button + toast, avoid navigating to a raw JSON error page
  (`iceberg.html:264-267`).
- In-flight feedback on row-level connection Test (`connections.html:163-170`).

### 5.7 Accessibility pass (mechanical)
- Add `for=`/`id` to ~40 form fields; `role="dialog"` + focus trap + Esc/backdrop
  dismissal on modals; `aria-label` on icon-only buttons; make clickable rows
  keyboard-reachable; fix the `esc()`-apostrophe onclick escaping by moving to
  delegated `data-` listeners (pattern already in `dbt.html:223-227`).

---

## Suggested execution order
1. **Phase 0** (security) — one PR, highest priority.
2. **Phase 1 + Phase 2** (portability + cleanup) — one low-risk PR, mostly mechanical.
3. **Phase 3.1** alone (DQ double-scan) — big win, tiny change.
4. **Phase 5.1–5.4** (UI pain) — one PR.
5. **Phase 3.2–3.6, Phase 4, Phase 5.5–5.7** — as capacity allows, each TDD'd.

Each phase is independently shippable; nothing in a later phase depends on an
earlier one except 4.1 reusing the 0.5 backup-pruning logic.
