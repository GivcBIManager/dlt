# Flow Builder Enhancement — Design

**Date:** 2026-07-12
**Status:** Approved (design), pending implementation plan
**Area:** `gui/` Flows page + `orchestrator/` Dagster code location

## Goal

Enhance the Flows page (`gui/templates/flows.html`) and its backing code so that:

1. The schedule timezone always defaults to the **server's** timezone (auto-detected, editable).
2. The DAG is built with a **drag-and-drop canvas**: drag component types onto a canvas, connect them to declare dependencies, and configure each component as a **pipeline**, **dbt run**, **dbt test**, **dbt build**, or a **custom command**.
3. The scheduler uses **friendly presets** (hourly / daily / weekly / monthly / every-N) with time + day pickers that generate the cron, plus an **Advanced** raw-cron escape hatch.
4. Dagster assets/jobs/schedules use a **readable naming style** so it is obvious in the Dagster UI which flow an asset belongs to.

## Confirmed decisions

- **Canvas tech:** vendor [Drawflow](https://github.com/jerosoler/Drawflow) (MIT) as a single JS/CSS file in `gui/static/`. No CDN, no build step. Matches the codebase's vanilla-JS, no-bundler approach.
- **Timezone:** auto-detect the server IANA zone (via `tzlocal`), prefill it as the default for new flows, keep the field editable. Existing flows keep their saved zone.
- **Scheduler:** presets + an Advanced raw-cron toggle. Saved value remains a cron string (no backend schema change).
- **Dagster naming:** derive names from the flow name (slug) plus the short flow id for uniqueness. Renaming a flow regenerates names on reload; old run/asset history is left under the old name.

## Non-goals

- No change to how a node's command actually executes (`commands.build_argv` is reused as-is; `custom` is already supported).
- No change to the cron *storage* format or the 5-field validation.
- No migration of existing Dagster run/asset history to new names.
- No JS unit-test harness is introduced; client-side cron↔preset logic is verified by running the app.

---

## Architecture overview

The GUI (`gui/`) and the Dagster code location (`orchestrator/`) already share code through a single seam: `orchestrator/state.py` inserts `gui/` onto `sys.path` and re-exports `commands.build_argv` and the JSON readers. We extend that seam with **one new GUI module** used by both sides so Dagster names can never drift between the two.

### Files touched

| File | Change |
|---|---|
| `gui/flow_naming.py` | **NEW** — single source of truth for slug + job/schedule/group names + asset key prefix. |
| `gui/flows_store.py` | Add `command` node kind validation; add timezone validation; pass through optional `graph` field. |
| `gui/app.py` | `/api/flows` returns `server_timezone`; `run`/`toggle` resolve job/schedule names via `flow_naming`. |
| `gui/dagster_client.py` | `flow_status()` returns a `flow_id` field (parsed from the job name). |
| `gui/templates/flows.html` | Drawflow canvas + palette + per-node config + preset scheduler + timezone dropdown; export/import translation. |
| `gui/static/drawflow.min.js`, `gui/static/drawflow.min.css` | **NEW** — vendored library. |
| `orchestrator/src/orchestrator/build.py` | Use `flow_naming` for group/job/schedule/sensor names; handle `command` node kind. |
| `orchestrator/src/orchestrator/assets.py` | Use `flow_naming.asset_key` + `group_name`. |
| `orchestrator/src/orchestrator/email.py` | Sensor names derived from `flow_naming` base name. |
| `requirements-gui.txt` | Add `tzlocal`. |

---

## Component 1 — `gui/flow_naming.py` (Dagster naming)

Single source of truth, all outputs constrained to `[A-Za-z0-9_]` (valid Dagster identifiers).

```python
slugify("Nightly Masters → DQ")   # -> "nightly_masters_dq"   (non-alnum runs collapse to a single "_"; never "__")
base_name(flow)                    # -> f"{slug}__{flow['id']}"          e.g. "nightly_masters_dq__a1b2c3d4"
job_name(flow)                     # -> f"flow_{base_name(flow)}"        e.g. "flow_nightly_masters_dq__a1b2c3d4"
schedule_name(flow)                # -> f"{job_name(flow)}_schedule"
group_name(flow)                   # -> slugify(flow['name'])            e.g. "nightly_masters_dq"  (clean graph label)
asset_key_prefix(flow)             # -> base_name(flow)
asset_key(flow, node_id)           # -> AssetKey([base_name(flow), node_id])  (built in orchestrator from the string prefix)
flow_id_from_job(job_name)         # -> job_name.rsplit("__", 1)[-1]    (slug never contains "__", so this is reliable)
```

- `slugify`: `re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")`, falling back to `"flow"` when empty.
- The `__<id>` suffix guarantees global uniqueness of job/schedule/asset-prefix names even if two distinct flow names slugify to the same slug.
- `group_name` is the clean slug (readable label in the asset graph). Two flows with the same slug would *visually merge* in one group box, but their asset keys remain distinct (they carry the id). This is an accepted, rare cosmetic edge case.
- `flow_naming` lives in `gui/` and is imported by the orchestrator through the existing `sys.path` seam, so both sides always agree.

### Example (Dagster UI)

```
Group:     nightly_masters_dq
Assets:    nightly_masters_dq__a1b2c3d4/extract
           nightly_masters_dq__a1b2c3d4/dq_check
Job:       flow_nightly_masters_dq__a1b2c3d4
Schedule:  flow_nightly_masters_dq__a1b2c3d4_schedule
Sensors:   flow_nightly_masters_dq__a1b2c3d4_success_email
           flow_nightly_masters_dq__a1b2c3d4_failure_email
```

### Backend wiring for names

- `app.py` `run`/`toggle` endpoints load the flow (`flows_store.get_flow(fid)`), then compute `flow_naming.job_name(flow)` / `schedule_name(flow)` instead of assuming `flow_<fid>` / `flow_<fid>_schedule`. If the flow is missing, return a 404-style error.
- `dagster_client.flow_status()` adds `"flow_id": flow_naming.flow_id_from_job(job["name"])` to each entry. The frontend maps status by `flow_id` instead of by the literal `flow_<id>` job string.

---

## Component 2 — Timezone

- `requirements-gui.txt` gains `tzlocal`.
- New helper (in `gui/config.py` or a small `gui/tz.py`): `server_timezone()` returns `tzlocal.get_localzone_name()`, falling back to `"UTC"` on any error.
- `/api/flows` response gains `"server_timezone": server_timezone()`.
- Form: the free-text timezone `<input>` becomes a `<select>` populated from `zoneinfo.available_timezones()` (sorted). Default selection = `server_timezone` for a new flow; for an existing flow, its saved zone (added to the option list if not already present).
- **New validation** in `flows_store` (`add_flow` + `update_flow`): reject a timezone string that isn't a valid `zoneinfo.ZoneInfo(tz)`. The hardcoded `value="Asia/Riyadh"` in the HTML is removed.

---

## Component 3 — Node kinds (add `command`)

Palette exposes component types; the on-disk node model gains one kind.

| Palette item | Stored node | `build_argv` spec |
|---|---|---|
| Pipeline | `{node_id, kind:"pipeline", pipeline_id, deps}` | pipeline's saved `spec` (unchanged) |
| dbt run / dbt test / dbt build | `{node_id, kind:"dbt", dbt:{dbt_command, select}, deps}` | unchanged; `run`/`test`/`build` presets the command |
| Custom command | `{node_id, kind:"command", command:"…", deps}` **(NEW)** | `{script:"custom", custom:"…"}` |

- `commands.build_argv` already handles `script:"custom"` → no change at the command layer.
- `flows_store.validate_flow`: add a `command` branch requiring a non-empty `command` string; keep the existing `pipeline` and `dbt` branches and the acyclic/dep checks.
- `build.py._build_flow`: add `kind == "command"` → `spec = {"script": "custom", "custom": node["command"]}`, label `f"custom: {command[:40]}"`.
- **Security note:** custom commands execute arbitrary shell on the server. This capability already exists on the Run page (`script:"custom"`), so exposing it in flows is consistent and not new attack surface.

---

## Component 4 — Drag-and-drop canvas (Drawflow)

- Vendor `drawflow.min.js` + `drawflow.min.css` into `gui/static/` (fetched and committed; MIT, ~40KB).
- **Build tab layout:** a left **palette** of draggable chips (Pipeline, dbt run, dbt test, dbt build, Custom command) beside a right **Drawflow canvas** (pan/zoom). Each node has one input port and one output port.
- **Interaction:** drag a chip onto the canvas → creates a node. Drag an output port to an input port → creates a dependency edge. **Direction convention: `A.output → B.input` means "B depends on A"** (A runs first).
- **Node config** is rendered inside each node card (compact selects/inputs), reusing the pipeline list and dbt models/tests already fetched by the page:
  - pipeline → `<select>` of pipelines.
  - dbt → command `<select>` (run/test/build) + a model/test `<select>`.
  - command → a text input for the command line.
- **Save (export → normalize):** `editor.export()` yields nodes + connections. Translate to:
  - `nodes[]`: `node_id = "n" + drawflowNodeId`; `kind` + config read from the node's fields; `deps` = the source node_ids of incoming connections.
  - `graph`: the raw Drawflow export JSON, stored verbatim in a **new optional `graph` field** on the flow record for faithful reload (includes positions).
- **Edit (import):**
  - If the flow has a stored `graph`, `editor.import(graph)` restores the exact canvas.
  - Otherwise (older flows), **auto-layout**: assign nodes to columns by topological depth, place them, and draw edges from each node's `deps`. Every existing flow therefore opens correctly.
- The old textual "DAG preview" box is replaced by the live canvas plus a small validity line (root/cycle hints). The server still enforces acyclicity on save (unchanged `_assert_acyclic`).

### Flow record schema (additive, backward compatible)

```jsonc
{
  "id": "a1b2c3d4",
  "name": "nightly masters → dq",
  "nodes": [
    {"node_id": "n1", "kind": "pipeline", "pipeline_id": "p1", "deps": []},
    {"node_id": "n2", "kind": "dbt", "dbt": {"dbt_command": "test", "select": "stg_products"}, "deps": ["n1"]},
    {"node_id": "n3", "kind": "command", "command": "python tools/notify.py", "deps": ["n2"]}
  ],
  "cron": "0 2 * * *",
  "timezone": "Asia/Riyadh",
  "email": {"on_success": [], "on_failure": []},
  "enabled": true,
  "graph": { /* opaque Drawflow export; optional; stored verbatim */ },
  "created_at": "2026-07-12T02:00:00"
}
```

`flows_store` persists `graph` when present and returns it untouched; it is not otherwise validated. `add_flow` gains an optional `graph=None` parameter (populated from `b.get("graph")` in `app.py`); `update_flow` already accepts arbitrary `**fields`, so it persists `graph` when passed in the body.

---

## Component 5 — Scheduler (presets + advanced)

Client-side cron builder. Pure functions `cronFromPreset(preset)` and `presetFromCron(cron)` keep the logic isolated and testable-by-inspection.

| Preset | Inputs | Cron |
|---|---|---|
| Every N minutes | N | `*/N * * * *` |
| Hourly | minute M | `M * * * *` |
| Daily | HH:MM | `M H * * *` |
| Weekly | day-of-week D, HH:MM | `M H * * D` |
| Monthly | day-of-month D, HH:MM | `M H D * *` |
| Advanced | raw 5 fields | as typed (today's UI) |

- A live human-readable summary (e.g. "Weekly on Monday at 02:00") and the generated cron string are shown read-only next to the controls.
- **On edit:** `presetFromCron(savedCron)` selects the matching preset and fills its inputs; if the cron matches no known pattern (hand-written), the form opens in **Advanced** with the raw cron intact.
- Backend: no change — still stores and validates a 5-field cron string.

---

## Error handling

- Invalid timezone → `400` with a clear message (new validation).
- Empty custom command / missing dbt select / bad dbt command → existing `ValueError` messages surfaced by the `@api` decorator.
- Dagster unreachable → existing best-effort behavior (banner + empty status), unchanged.
- Unparseable saved cron on edit → Advanced mode; the form never crashes.
- A single malformed flow still can't break the whole code location (`build_all_defs` already skips-and-logs per flow).

---

## Testing

pytest, using the existing `state_dir` fixture and conventions.

- **`tests/test_flow_naming.py` (new):** `slugify` edge cases (spaces, arrows/unicode, leading/trailing punctuation, empty → `"flow"`, no `"__"` ever produced); `base_name`/`job_name`/`schedule_name` shape; `flow_id_from_job(job_name(flow)) == flow["id"]` round-trip.
- **`tests/test_flows_store.py` (extend):** accept a `command` node; reject a `command` node with an empty command; reject an invalid timezone; accept a valid timezone; `graph` field round-trips through `add_flow`/`get_flow`.
- **`tests/test_orchestrator_build.py` (extend):** asset keys/group/job/schedule use the readable `flow_naming` names for a sample flow; a `command` node builds an asset whose spec is the custom command.
- **`tests/test_dagster_client.py` (extend):** `flow_status()` entries include the correct `flow_id` parsed from a readable job name.
- **Client-side** cron↔preset and Drawflow export/import translation: no JS harness in this repo; verified by running the app (create a flow of each node kind, each preset, save, reload/edit, confirm round-trip and readable names in the Dagster UI).

---

## Rollout / compatibility

- Existing `flows.json` records load unchanged (no `graph`, kinds `pipeline`/`dbt`): they auto-layout in the new canvas and keep their saved timezone.
- On the next Dagster reload, existing flows' assets/jobs/schedules **change names** to the readable scheme (one-time history reset under the old names — accepted).
- `tzlocal` is the only new runtime dependency; `drawflow` is a committed static asset (no package).
