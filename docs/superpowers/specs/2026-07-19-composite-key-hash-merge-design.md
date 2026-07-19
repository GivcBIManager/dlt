# Hash-Keyed Composite Merge — Design Spec

**Goal:** Replace the per-row `Or(AND EqualTo…)` composite match filter that dominates
incremental `merge` cost (90–96% of wall-clock on every profiled table) with a
single-column `In(merge_hash, …)` filter, by deriving one stored hash column from the
`(PK…, BRANCH_ID)` key. Merge semantics, column naming, and typing are unchanged — this
is a performance change plus one additive column.

## Why this works

pyiceberg's `create_match_filter` (`upsert_util.py`) takes the fast `In(col, values)`
path **only when `len(join_cols) == 1`**. The loader always appends `BRANCH_ID` to the
merge key (`etl/iceberg_load.py:421`), so every non-snapshot merge is composite (≥2
columns) and builds `Or(AND EqualTo…)` — one clause per delta row, cost super-linear in
delta size. Folding the composite key into a single `merge_hash` column collapses the
filter to a single `In`. The read-only profiler already benchmarks this exact swap
(`diagnostics/merge_profile.py --hash-key`, `benchmark_hash`).

## Locked decisions (from brainstorming Q&A)

1. **Hash is the sole join key.** The merge joins on `merge_hash` alone (single-column
   `In`), not on the composite key. No in-memory exact-key backstop.
2. **128-bit, collision-proof.** Stored as one opaque `fixed[16]` column. Collision
   probability ≈ 1e-23 at 100M rows — no realistic chance of a silent wrong-row
   overwrite, ever. This also gives equal-hash ⟺ equal-composite-key, so upsert
   match/insert and duplicate-key detection behave identically to the composite merge.
   (A 64-bit `long` was rejected: sole-key + collision = silent corruption, ~1-in-37k
   per full merge on a 22M-row table — unacceptable for healthcare/finance data.)
3. **Reload-gated rollout.** No backfill tool. A table gets `merge_hash` only when it is
   next fully re-loaded (`replace`). Existing already-loaded tables keep today's
   composite merge until reloaded; new tables get the hash on their first load.

## Architecture

The write and merge paths share one chokepoint: `_iceberg_resource._finish`
(`etl/iceberg_load.py:429`), which every yielded batch flows through for **both**
`replace`/`append` (initial load) and `merge` (incremental). The hash is computed and the
batch sorted there. The merge's join-column choice lives in `_merge_iceberg_single_commit`
(`etl/iceberg_load.py:758`). The readiness gate piggybacks on `_existing_insert_at`
(`etl/iceberg_load.py:320`), which already opens the stored table and reads its schema.

```text
initial load  ─┐
                ├─▶ _iceberg_resource._finish ─▶ [cast → +merge_hash → sort_by(merge_hash)] ─▶ Iceberg
incremental  ──┘                                        (only when hash-ready)

incremental merge ─▶ _merge_iceberg_single_commit ─▶ join_cols = ["merge_hash"]  (hash-ready)
                                                    └▶ join_cols = composite       (not ready → today)
```

## Global constraints

- pyiceberg pinned at **0.11.1**; `In` fast path requires `len(join_cols) == 1`.
- Tables are partitioned by `BRANCH_ID` (identity transform, `etl/iceberg_load.py:440`).
  A sole-hash merge filter carries **no** `branch_id` predicate, so it loses partition
  pruning; sort-by-hash writes recover part of it via per-file `merge_hash` min/max.
- Merge semantics, existing column naming, and typing must be **unchanged**. Preserve the
  single-commit-per-merge property (`tests/test_merge_single_commit.py`).
- dlt normalizes identifiers to lower snake; `merge_hash` is already lower.
- No new third-party dependency (default: stdlib `blake2b`).
- Tests use the `SqlCatalog` + sqlite + `FsspecFileIO` pattern from
  `tests/test_merge_single_commit.py`.

---

## Component 1: the `merge_hash` column

- **Column:** `merge_hash`, Iceberg `binary` (Arrow `pa.binary()`, every value exactly 16
  bytes), configured via `settings.merge_hash_column` (mirrors `branch_id_column`,
  `inserted_ts_column`). Written **only when the table is/becomes hash-ready**; not declared
  a dlt `primary_key` (the composite key stays dlt's key — the merge overrides `join_cols`
  locally instead), so its non-null-ness is enforced in practice (always computed) rather
  than via a hint. (Variable `binary` over `fixed[16]` avoids dlt Arrow→Iceberg
  fixed-type mapping friction; `In`, sort, and min/max stats behave identically.)
- **Hash function:** stdlib `hashlib.blake2b(digest_size=16)` — 128-bit, deterministic
  across processes and library versions, no new dependency. (Python's built-in `hash()` is
  per-process salted → unusable; this is the correctness crux.)
- **Canonical serialization:** a **typed, length-prefixed** byte encoding of each key
  column value, in a fixed key-column order, so the same logical key always produces
  identical bytes and distinct keys produce distinct pre-hash bytes
  (`("a","bc") ≠ ("ab","c")`, null distinguishable from empty string). Encoded per row from
  the columns **after** `cast_table_to_schema`, so initial-load and incremental-delta hash
  identical canonical types.
- **Helper:** `_merge_hash_array(table: pa.Table, key_cols: list[str]) -> pa.Array`
  returning a `binary(16)` array aligned to `table`'s rows.

## Component 2: write path (initial load + every write)

- The caller passes an explicit **`write_hash: bool`** into `_iceberg_resource`. `_finish`
  writes `merge_hash` (append via `_merge_hash_array`, then
  `tbl.sort_by([("merge_hash", "ascending")])`) **iff `write_hash` is true**; otherwise it
  behaves exactly as today. Clustering lets per-file `merge_hash` min/max prune the
  `In(merge_hash)` scan.
- **The exact `write_hash` rule** (computed by each caller, not inferred from disposition
  alone):
  - snapshot/append tables (`tdef.is_snapshot`) → **False** (never hashed);
  - full rebuild (`_run_per_branch_rebuild`, i.e. `disposition == "replace"`) → **True for
    every branch of the rebuild**, including the branches it writes with `append` after the
    first branch's `replace` — so the whole table ends up hashed;
  - incremental merge (`disposition == "merge"`) → **True iff the stored table is already
    hash-ready** (Component 4), else False.
- Because a full rebuild sets `write_hash=True` for all its branches, a `replace` is the
  sole path that flips a not-ready table to ready (= the reload gate).
- Sort/append are correctness-neutral: row order does not affect a merge or an append.

## Component 3: merge path

- `_merge_iceberg_single_commit` computes today's composite `join_cols`, then **overrides**
  to `["merge_hash"]` iff both the stored `table.schema()` **and** the delta `data` carry
  `merge_hash`. Otherwise composite (unchanged behavior).
- The `table.upsert(join_cols=…)` call is otherwise untouched: `when_matched_update_all`,
  `when_not_matched_insert_all`, `case_sensitive=True`, one commit.
- Duplicate-key detection is preserved: a duplicate `merge_hash` in a delta ⟺ a duplicate
  composite key (collision-proof), i.e. the same abort behavior as today, not a new one.
- **Carry-forward join** ("any operation involving the composite key"): when hash-ready,
  `_existing_insert_at` selects the stored `merge_hash` (+ `insert_at`) and
  `_carry_forward_insert_at` joins the batch to existing rows on `merge_hash` — a cheaper
  single-column Arrow join, same collision-proof guarantee. Not ready → composite join
  (unchanged).

## Component 4: reload-gated readiness (safety-critical)

- **Readiness signal:** the stored Iceberg table already has a `merge_hash` column,
  detected in `_existing_insert_at` (already reads `tbl.schema().fields`). This readiness
  bool becomes the merge path's `write_hash` value (Component 2) threaded to
  `_iceberg_resource` alongside `existing_insert_at`.
- **Discipline that makes the signal trustworthy:** an *incremental* merge writes
  `merge_hash` **only if the table is already ready**. On a not-ready table it emits **no**
  hash, so `union_by_name` (`etl/iceberg_load.py:785`) never adds a half-populated
  `merge_hash` — a table with some null and some hashed rows would treat an existing row as
  new and insert a duplicate. Only a full `replace` populates the column for all rows.
- **Invariant:** a table is hash-ready ⟺ it was fully reloaded after this change. Presence
  of the column implies every row has a non-null hash.

## Scope / non-goals

- **Snapshot/append tables:** untouched (never merged, no key).
- **No backfill tool:** existing tables keep the composite merge until reloaded.
- **No-CDC `replace`-only tables:** get the column + sort but never run the hash merge
  (harmless; leaves them ready if a branch-subset merge ever occurs).
- **No change** to existing column names, types, or merge semantics — additive column +
  faster join only.

## Validation & tests (profile-first, TDD)

1. **Profile first.** Run `diagnostics/merge_profile.py <table> --hash-key` (and
   `--hash-sorted`) on the real problem tables (`patient_master_data`,
   `patient_episodes`, `external_accounts_data`, `purchaser_ios_prices`); record the
   composite-`Or` vs hash-`In` per-stage speedup and matched-file counts. This is the
   acceptance target the implementation must reproduce on a hash-ready table.
2. **Unit — hashing:**
   - stability **across a fresh interpreter process** (subprocess) — same key → same 16
     bytes (guards against a salted/nondeterministic hash regression);
   - serialization injectivity: `("a","bc") ≠ ("ab","c")`, null vs empty string, key-column
     order fixed;
   - `_merge_hash_array` returns `binary(16)`, NOT NULL, row-aligned.
3. **Merge (SqlCatalog + sqlite):**
   - hash-ready table: update-existing + insert-new correct via the `["merge_hash"]` path;
   - not-ready table: falls back to composite **and stays not-ready** (no `merge_hash`
     column added by the incremental merge);
   - `tests/test_merge_single_commit.py` stays green (single commit per merge preserved);
   - sort-by-hash write is correctness-neutral (row content/count unchanged).
4. **Carry-forward:** `insert_at` preserved for updated rows on a hash-ready table via the
   `merge_hash` join.

## Defaults chosen (open to change at review)

- `hashlib.blake2b(digest_size=16)` (stdlib, no dep) over `xxhash` (faster C, new dep).
- Variable-length `binary` (Arrow `pa.binary()`, 16-byte values) over `fixed[16]` — avoids
  dlt Arrow→Iceberg fixed-type mapping friction; functionally identical for `In`/sort/stats.

## Related

- `docs/superpowers/plans/2026-07-19-composite-key-merge-optimization.md` — the
  profile-first plan that **descoped** the composite-`Or` fix. This spec is that descoped
  structural lever, done via a stored single-column hash rather than a per-branch `In`
  driver.
- Memory: `oasis-composite-merge-cost`, `oasis-iceberg-hang-fix-branch`.
