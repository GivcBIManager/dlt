# Flow Builder Enhancement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the Flows page as a drag-and-drop DAG canvas with a server-timezone default, a friendly cron scheduler, a custom-command node kind, and readable Dagster asset/job/schedule names.

**Architecture:** A new `gui/flow_naming.py` is the single source of truth for Dagster names, imported by both the GUI and the orchestrator through the existing `orchestrator/state.py` `sys.path` seam. Backend changes (naming, validation, timezone) land first with pytest coverage; the front-end (`flows.html`) is rewritten last around the vendored Drawflow library and verified by running the app.

**Tech Stack:** Python 3.13, Flask, Dagster 1.13.11, pytest; vanilla JS + [Drawflow](https://github.com/jerosoler/Drawflow) (vendored, MIT); `tzlocal` for server-timezone detection; `zoneinfo` (stdlib).

## Global Constraints

- Python: 3.13 (repo venv). Use modern type syntax; `from __future__ import annotations` matches existing modules.
- No new runtime deps except `tzlocal` (pure-python). Drawflow is a committed static asset, not a package.
- Vanilla JS only in templates — no bundler, no framework. Load libs via `url_for('static', ...)`, never a CDN for app logic.
- All Dagster identifiers must match `[A-Za-z0-9_]`.
- Flow record changes are additive/backward compatible: existing `flows.json` must still load.
- Tests use the existing `state_dir` fixture (`tests/conftest.py`) which points `config.*_JSON` at a tmp dir.
- Run tests with the repo venv: `.venv/Scripts/python.exe -m pytest`.
- Commit after each task (frequent commits). End commit messages with the `Co-Authored-By` trailer.

---

### Task 1: `flow_naming.py` — readable Dagster names

**Files:**
- Create: `gui/flow_naming.py`
- Test: `tests/test_flow_naming.py`

**Interfaces:**
- Produces: `slugify(name: str) -> str`, `base_name(flow: dict) -> str`, `job_name(flow: dict) -> str`, `schedule_name(flow: dict) -> str`, `group_name(flow: dict) -> str`, `asset_key_prefix(flow: dict) -> str`, `flow_id_from_job(name: str) -> str`. `flow` is a dict with at least `id` and `name`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_flow_naming.py`:

```python
import pytest


def test_slugify_basic():
    from flow_naming import slugify
    assert slugify("Nightly Masters") == "nightly_masters"


def test_slugify_collapses_and_strips_punctuation():
    from flow_naming import slugify
    assert slugify("  Nightly  Masters -> DQ!!  ") == "nightly_masters_dq"


def test_slugify_never_produces_double_underscore():
    from flow_naming import slugify
    assert "__" not in slugify("a --- b @@@ c ___ d")


def test_slugify_empty_falls_back_to_flow():
    from flow_naming import slugify
    assert slugify("") == "flow"
    assert slugify("→→→") == "flow"


def test_name_builders():
    import flow_naming as fn
    flow = {"id": "a1b2c3d4", "name": "Nightly Masters"}
    assert fn.base_name(flow) == "nightly_masters__a1b2c3d4"
    assert fn.job_name(flow) == "flow_nightly_masters__a1b2c3d4"
    assert fn.schedule_name(flow) == "flow_nightly_masters__a1b2c3d4_schedule"
    assert fn.group_name(flow) == "nightly_masters"
    assert fn.asset_key_prefix(flow) == "nightly_masters__a1b2c3d4"


def test_flow_id_round_trips_through_job_name():
    import flow_naming as fn
    flow = {"id": "deadbeef", "name": "Weird -- Name __ x"}
    assert fn.flow_id_from_job(fn.job_name(flow)) == "deadbeef"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_flow_naming.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'flow_naming'`).

- [ ] **Step 3: Write minimal implementation**

Create `gui/flow_naming.py`:

```python
"""Deterministic, readable Dagster names derived from a flow.

Single source of truth shared by the GUI (``gui/``) and the Dagster code
location (``orchestrator/``). The orchestrator imports this module through the
``sys.path`` seam that ``orchestrator/state.py`` sets up, so the job / schedule
/ asset / group names Dagster builds always match the names the GUI uses to
launch and toggle them.

All outputs are constrained to ``[A-Za-z0-9_]`` (valid Dagster identifiers). The
``__<flow id>`` suffix guarantees uniqueness (two distinct flow names can
slugify to the same slug) and lets :func:`flow_id_from_job` recover the id --
``slugify`` never emits ``__``, so the delimiter is unambiguous.
"""
from __future__ import annotations

import re
from typing import Any

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    slug = _NON_ALNUM.sub("_", (name or "").strip().lower()).strip("_")
    return slug or "flow"


def base_name(flow: dict[str, Any]) -> str:
    return f"{slugify(flow['name'])}__{flow['id']}"


def job_name(flow: dict[str, Any]) -> str:
    return f"flow_{base_name(flow)}"


def schedule_name(flow: dict[str, Any]) -> str:
    return f"{job_name(flow)}_schedule"


def group_name(flow: dict[str, Any]) -> str:
    return slugify(flow["name"])


def asset_key_prefix(flow: dict[str, Any]) -> str:
    return base_name(flow)


def flow_id_from_job(name: str) -> str:
    """Recover the flow id from a name built by :func:`job_name`."""
    return name.rsplit("__", 1)[-1]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_flow_naming.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add gui/flow_naming.py tests/test_flow_naming.py
git commit -m "feat(flows): shared readable Dagster naming (flow_naming)"
```

---

### Task 2: `flows_store` — command node kind, timezone validation, graph passthrough

**Files:**
- Modify: `gui/flows_store.py`
- Test: `tests/test_flows_store.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `validate_timezone(tz: str) -> None` (raises `ValueError`); `validate_flow` now accepts `kind == "command"` nodes (`{node_id, kind:"command", command:str, deps}`); `add_flow(..., graph: dict | None = None)` persists `graph`; `update_flow` persists a `graph` field when passed.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_flows_store.py`:

```python
def test_validate_accepts_command_node():
    import flows_store as fs
    node = {"node_id": "c1", "kind": "command", "command": "python tools/x.py", "deps": []}
    fs.validate_flow([node], known_pipeline_ids=set())  # must not raise


def test_validate_rejects_command_without_command():
    import flows_store as fs
    bad = {"node_id": "c1", "kind": "command", "command": "   ", "deps": []}
    with pytest.raises(ValueError, match="command"):
        fs.validate_flow([bad], known_pipeline_ids=set())


def test_add_flow_rejects_bad_timezone(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    with pytest.raises(ValueError, match="[Tt]imezone"):
        fs.add_flow("tzbad", nodes, "0 2 * * *", "Not/AZone", {})


def test_add_flow_accepts_valid_tz_and_stores_graph(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    graph = {"drawflow": {"Home": {"data": {"1": {"id": 1, "name": "pipeline"}}}}}
    f = fs.add_flow("tzok", nodes, "0 2 * * *", "Asia/Riyadh", {}, graph=graph)
    got = fs.get_flow(f["id"])
    assert got["timezone"] == "Asia/Riyadh"
    assert got["graph"] == graph


def test_update_flow_persists_graph(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    f = fs.add_flow("upd", nodes, "0 2 * * *", "UTC", {})
    g = {"drawflow": {"Home": {"data": {}}}}
    fs.update_flow(f["id"], graph=g)
    assert fs.get_flow(f["id"])["graph"] == g
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_flows_store.py -v -k "command or timezone or graph"`
Expected: FAIL (command node rejected as unknown kind / no `validate_timezone` / `graph` not stored).

- [ ] **Step 3: Implement the changes**

In `gui/flows_store.py`, add the command kind to the sets near the top:

```python
_DBT_NODE_COMMANDS = {"run", "test", "build"}
```

(unchanged — keep it). Add a timezone validator after `validate_cron`:

```python
def validate_timezone(tz: str) -> None:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        ZoneInfo((tz or "").strip())
    except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
        raise ValueError(f"Unknown timezone: {tz!r}") from exc
```

In `validate_flow`, add a `command` branch. Replace the existing kind dispatch block:

```python
        kind = n.get("kind", "pipeline")
        if kind == "pipeline":
            if n.get("pipeline_id") not in known_pipeline_ids:
                raise ValueError(f"Node {n['node_id']} references unknown pipeline {n.get('pipeline_id')}")
        elif kind == "dbt":
            dbt = n.get("dbt") or {}
            if (dbt.get("dbt_command") or "").strip() not in _DBT_NODE_COMMANDS:
                raise ValueError(
                    f"Node {n['node_id']}: dbt command must be one of {sorted(_DBT_NODE_COMMANDS)}")
            if not str(dbt.get("select") or "").strip():
                raise ValueError(f"Node {n['node_id']}: dbt node needs a non-empty 'select'")
        elif kind == "command":
            if not str(n.get("command") or "").strip():
                raise ValueError(f"Node {n['node_id']}: command node needs a non-empty 'command'")
        else:
            raise ValueError(f"Node {n['node_id']}: unknown kind {kind!r}")
```

In `add_flow`, add the `graph` parameter and timezone validation. Replace the signature and body head:

```python
def add_flow(name: str, nodes: list[dict[str, Any]], cron: str, timezone: str,
             email: dict[str, Any] | None, enabled: bool = True,
             graph: dict[str, Any] | None = None) -> dict[str, Any]:
    name = (name or "").strip()
    if not name:
        raise ValueError("Flow name is required")
    validate_cron(cron)
    tz = (timezone or "UTC").strip()
    validate_timezone(tz)
    validate_flow(nodes, known_pipeline_ids=_known_pipeline_ids())
    items = _load()
    if any(f["name"] == name for f in items):
        raise ValueError(f"Flow '{name}' already exists")
    f = {"id": uuid.uuid4().hex[:8], "name": name, "nodes": nodes,
         "cron": cron.strip(), "timezone": tz,
         "email": _normalize_email(email), "enabled": bool(enabled),
         "created_at": datetime.now().isoformat(timespec="seconds")}
    if graph is not None:
        f["graph"] = graph
    items.append(f)
    _save(items)
    return f
```

In `update_flow`, validate a supplied timezone and persist `graph`. Replace the inner update block:

```python
            if fields.get("cron") is not None:
                validate_cron(fields["cron"])
            if fields.get("timezone") is not None:
                validate_timezone(fields["timezone"])
            if fields.get("nodes") is not None:
                validate_flow(fields["nodes"], known_pipeline_ids=_known_pipeline_ids())
            for k in ("name", "nodes", "cron", "timezone", "enabled", "graph"):
                if fields.get(k) is not None:
                    f[k] = fields[k]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_flows_store.py -v`
Expected: PASS (all, including the pre-existing tests).

- [ ] **Step 5: Commit**

```bash
git add gui/flows_store.py tests/test_flows_store.py
git commit -m "feat(flows): command node kind, timezone validation, graph passthrough"
```

---

### Task 3: Orchestrator — readable names, command kind, per-flow job selection

**Files:**
- Modify: `orchestrator/src/orchestrator/state.py` (re-export `flow_naming`)
- Modify: `orchestrator/src/orchestrator/assets.py` (name via prefix/group args)
- Modify: `orchestrator/src/orchestrator/build.py` (readable names + command kind + explicit-key selection)
- Modify: `orchestrator/src/orchestrator/email.py` (sensor names from base)
- Test: `tests/test_orchestrator_build.py`, `tests/test_orchestrator_assets.py` (both use the OLD `asset_key`/`build_asset` signatures and MUST be updated)

**Interfaces:**
- Consumes: `flow_naming.*` from Task 1 (via `state.flow_naming`).
- Produces: `assets.asset_key(prefix: str, node_id: str) -> AssetKey`; `assets.build_asset(prefix: str, group: str, node_id: str, name: str, spec: dict, dep_keys: list[AssetKey]) -> AssetsDefinition`; `email.build_email_sensors(base: str, flow_name: str, job, success_to, failure_to) -> list`.

- [ ] **Step 1: Update the existing build tests to the new names + add command test**

Replace the body of `tests/test_orchestrator_build.py` with:

```python
import json

import dagster as dg


def _seed(state_dir):
    (state_dir / "pipelines.json").write_text(json.dumps([
        {"id": "pa", "name": "a", "spec": {"script": "dq_check"}},
        {"id": "pb", "name": "b", "spec": {"script": "dq_check"}},
    ]))
    (state_dir / "flows.json").write_text(json.dumps([{
        "id": "f1", "name": "nightly",
        "nodes": [
            {"node_id": "n1", "pipeline_id": "pa", "deps": []},
            {"node_id": "n2", "pipeline_id": "pb", "deps": ["n1"]},
        ],
        "cron": "0 2 * * *", "timezone": "UTC",
        "email": {"on_success": ["x@y"], "on_failure": ["x@y"]},
        "enabled": True,
    }]))


def _wire(monkeypatch):
    import config
    from orchestrator import state
    monkeypatch.setattr(state._gui_config, "PIPELINES_JSON", config.PIPELINES_JSON)
    monkeypatch.setattr(state._gui_config, "FLOWS_JSON", config.FLOWS_JSON)


def test_build_all_defs_uses_readable_names(state_dir, monkeypatch):
    from orchestrator import build
    _wire(monkeypatch)
    _seed(state_dir)

    defs = build.build_all_defs()
    keys = {a.key for a in defs.resolve_all_asset_specs()}
    assert dg.AssetKey(["nightly__f1", "n1"]) in keys
    assert dg.AssetKey(["nightly__f1", "n2"]) in keys
    spec_n2 = next(a for a in defs.resolve_all_asset_specs()
                   if a.key == dg.AssetKey(["nightly__f1", "n2"]))
    assert dg.AssetKey(["nightly__f1", "n1"]) in {d.asset_key for d in spec_n2.deps}
    assert spec_n2.group_name == "nightly"
    assert defs.get_schedule_def("flow_nightly__f1_schedule").cron_schedule == "0 2 * * *"
    assert defs.get_job_def("flow_nightly__f1") is not None


def test_build_all_defs_handles_dbt_node(state_dir, monkeypatch):
    from orchestrator import build
    _wire(monkeypatch)
    (state_dir / "pipelines.json").write_text("[]")
    (state_dir / "flows.json").write_text(json.dumps([{
        "id": "f9", "name": "materialize",
        "nodes": [{"node_id": "m1", "kind": "dbt",
                   "dbt": {"dbt_command": "run", "select": "stg_products"}, "deps": []}],
        "cron": "0 3 * * *", "timezone": "UTC",
        "email": {"on_success": [], "on_failure": []}, "enabled": True,
    }]))
    defs = build.build_all_defs()
    keys = {a.key for a in defs.resolve_all_asset_specs()}
    assert dg.AssetKey(["materialize__f9", "m1"]) in keys


def test_build_all_defs_handles_command_node(state_dir, monkeypatch):
    from orchestrator import build
    _wire(monkeypatch)
    (state_dir / "pipelines.json").write_text("[]")
    (state_dir / "flows.json").write_text(json.dumps([{
        "id": "f7", "name": "notify",
        "nodes": [{"node_id": "c1", "kind": "command",
                   "command": "python tools/notify.py", "deps": []}],
        "cron": "0 4 * * *", "timezone": "UTC",
        "email": {"on_success": [], "on_failure": []}, "enabled": True,
    }]))
    defs = build.build_all_defs()
    keys = {a.key for a in defs.resolve_all_asset_specs()}
    assert dg.AssetKey(["notify__f7", "c1"]) in keys
    assert defs.get_job_def("flow_notify__f7") is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_build.py -v`
Expected: FAIL (asset keys still `flow_f1/...`; command node raises "unknown kind").

- [ ] **Step 3: Re-export `flow_naming` through the state seam**

In `orchestrator/src/orchestrator/state.py`, after the existing `import config as _gui_config` line, add:

```python
import flow_naming  # noqa: E402  (re-exported so orchestrator modules share GUI naming)
```

(Leave `build_argv = _commands.build_argv` as is. `flow_naming` is now reachable as `state.flow_naming`.)

- [ ] **Step 4: Update `assets.py` to name via arguments**

Replace `orchestrator/src/orchestrator/assets.py`'s `asset_key` and `build_asset` with:

```python
def asset_key(prefix: str, node_id: str) -> AssetKey:
    return AssetKey([prefix, node_id])


def build_asset(prefix: str, group: str, node_id: str, name: str,
                spec: dict[str, Any], dep_keys: list[AssetKey]) -> dg.AssetsDefinition:
    key = asset_key(prefix, node_id)

    @dg.asset(key=key, deps=dep_keys, group_name=group,
              description=name, compute_kind="subprocess")
    def _asset(context: dg.AssetExecutionContext) -> MaterializeResult:
        state.ensure_dbt_profiles(spec)
        argv, label = state.build_argv(spec)
        context.log.info("Running: %s", " ".join(argv))
        start = time.time()
        proc = subprocess.Popen(
            argv, cwd=str(state.REPO_ROOT),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            context.log.info(line.rstrip())
        rc = proc.wait()
        duration = round(time.time() - start, 1)
        if rc != 0:
            raise Failure(description=f"{label}: command exited with code {rc}")
        return MaterializeResult(metadata={
            "exit_code": rc,
            "duration_s": duration,
            "command": MetadataValue.text(" ".join(argv)),
        })

    return _asset
```

- [ ] **Step 5: Update `build.py`**

Replace `_build_flow` in `orchestrator/src/orchestrator/build.py` with:

```python
def _build_flow(flow: dict, pipelines: dict[str, dict]):
    prefix = state.flow_naming.asset_key_prefix(flow)
    group = state.flow_naming.group_name(flow)
    node_ids = {n["node_id"] for n in flow["nodes"]}
    flow_assets = []
    for node in flow["nodes"]:
        dep_keys = [asset_mod.asset_key(prefix, d) for d in node.get("deps", [])]
        for d in node.get("deps", []):
            if d not in node_ids:
                raise ValueError(f"flow {flow['id']}: unknown dep {d}")
        kind = node.get("kind", "pipeline")
        if kind == "dbt":
            dbt = node.get("dbt") or {}
            spec = {"script": "dbt", "dbt_command": dbt.get("dbt_command", "run"),
                    "select": dbt.get("select", "")}
            name = f"dbt {spec['dbt_command']} {spec['select']}".strip()
        elif kind == "command":
            cmd = (node.get("command") or "").strip()
            if not cmd:
                raise ValueError(f"flow {flow['id']}: command node {node['node_id']} is empty")
            spec = {"script": "custom", "custom": cmd}
            name = f"custom: {cmd[:40]}"
        else:
            pid = node["pipeline_id"]
            if pid not in pipelines:
                raise ValueError(f"flow {flow['id']}: unknown pipeline {pid}")
            spec = pipelines[pid]["spec"]
            name = pipelines[pid].get("name", node["node_id"])
        flow_assets.append(asset_mod.build_asset(
            prefix, group, node["node_id"], name, spec, dep_keys))

    node_keys = [asset_mod.asset_key(prefix, n["node_id"]) for n in flow["nodes"]]
    job = dg.define_asset_job(
        state.flow_naming.job_name(flow),
        selection=dg.AssetSelection.assets(*node_keys))

    enabled = flow.get("enabled", True)
    schedule = dg.ScheduleDefinition(
        name=state.flow_naming.schedule_name(flow),
        job=job,
        cron_schedule=flow["cron"],
        execution_timezone=flow.get("timezone", "UTC"),
        default_status=(dg.DefaultScheduleStatus.RUNNING if enabled
                        else dg.DefaultScheduleStatus.STOPPED),
    )

    email = flow.get("email", {})
    sensors = email_mod.build_email_sensors(
        state.flow_naming.base_name(flow), flow["name"], job,
        email.get("on_success", []), email.get("on_failure", []))

    return flow_assets, job, schedule, sensors
```

> Note: the job now selects its flow's assets **by explicit asset key** (`AssetSelection.assets`), not by group. Because `group_name` is the clean slug and two flows *could* share a slug, selecting by group would cross-select; selecting by the id-carrying asset keys keeps each job scoped to exactly one flow.

- [ ] **Step 6: Update `email.py` sensor names**

In `orchestrator/src/orchestrator/email.py`, change `build_email_sensors`'s first parameter from `flow_id` to `base` and update the two sensor `name=` lines:

```python
def build_email_sensors(base: str, flow_name: str, job: Any,
                        success_to: list[str], failure_to: list[str]) -> list:
    sensors: list[Any] = []

    if success_to:
        @dg.run_status_sensor(
            name=f"flow_{base}_success_email",
            run_status=dg.DagsterRunStatus.SUCCESS,
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _success(context: dg.RunStatusSensorContext) -> None:
            _send_for_run(context, "SUCCEEDED", success_to, flow_name)

        sensors.append(_success)

    if failure_to:
        @dg.run_failure_sensor(
            name=f"flow_{base}_failure_email",
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _failure(context: dg.RunFailureSensorContext) -> None:
            _send_for_run(context, "FAILED", failure_to, flow_name,
                          error=context.failure_event.message)

        sensors.append(_failure)

    return sensors
```

- [ ] **Step 7: Update `test_orchestrator_assets.py` for the new signatures**

The existing file asserts `asset_key("f1","n1") == AssetKey(["flow_f1","n1"])` and calls `build_asset("f1","n1","noop",spec,[])` (old 5-arg form). Replace the whole file with:

```python
import sys

import dagster as dg


def test_asset_key_shape():
    from orchestrator import assets
    assert assets.asset_key("nightly__f1", "n1") == dg.AssetKey(["nightly__f1", "n1"])


def test_asset_runs_command_and_succeeds(monkeypatch):
    from orchestrator import assets, state
    # A trivial spec whose build_argv yields a fast, zero-exit command.
    monkeypatch.setattr(state, "build_argv",
                        lambda spec: ([sys.executable, "-c", "print('hi')"], "noop"))
    a = assets.build_asset("nightly__f1", "nightly", "n1", "noop", {"script": "x"}, [])
    result = dg.materialize([a])
    assert result.success


def test_asset_raises_on_nonzero(monkeypatch):
    from orchestrator import assets, state
    monkeypatch.setattr(state, "build_argv",
                        lambda spec: ([sys.executable, "-c", "import sys; sys.exit(3)"], "boom"))
    a = assets.build_asset("nightly__f1", "nightly", "n2", "boom", {"script": "x"}, [])
    result = dg.materialize([a], raise_on_error=False)
    assert not result.success
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_build.py tests/test_orchestrator_assets.py tests/test_orchestrator_email.py -v`
Expected: PASS (all).

- [ ] **Step 9: Commit**

```bash
git add orchestrator/src/orchestrator/state.py orchestrator/src/orchestrator/assets.py orchestrator/src/orchestrator/build.py orchestrator/src/orchestrator/email.py tests/test_orchestrator_build.py tests/test_orchestrator_assets.py
git commit -m "feat(orchestrator): readable flow names, command nodes, per-flow job selection"
```

---

### Task 4: `dagster_client.flow_status` returns `flow_id` + name-aware run/toggle

**Files:**
- Modify: `gui/dagster_client.py` (extract pure `_rows_from_repos`, add `flow_id`)
- Modify: `gui/app.py` (`run`/`toggle` resolve names via `flow_naming`)
- Test: `tests/test_dagster_client.py`

**Interfaces:**
- Consumes: `flow_naming.flow_id_from_job`, `flow_naming.job_name`, `flow_naming.schedule_name`.
- Produces: `dagster_client._rows_from_repos(nodes: list[dict]) -> list[dict]` (each row gains `"flow_id"`).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dagster_client.py`:

```python
def test_rows_from_repos_parses_flow_id():
    import dagster_client as dc
    nodes = [{
        "jobs": [{"name": "flow_nightly__a1b2c3d4",
                  "runs": [{"runId": "r1", "status": "SUCCESS", "startTime": 1.0}]}],
        "schedules": [{"name": "flow_nightly__a1b2c3d4_schedule",
                       "scheduleState": {"status": "RUNNING"}}],
    }]
    rows = dc._rows_from_repos(nodes)
    assert len(rows) == 1
    assert rows[0]["flow_id"] == "a1b2c3d4"
    assert rows[0]["job"] == "flow_nightly__a1b2c3d4"
    assert rows[0]["schedule_state"] == "RUNNING"
    assert rows[0]["last_run_status"] == "SUCCESS"
    assert rows[0]["run_link"].endswith("/runs/r1")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dagster_client.py::test_rows_from_repos_parses_flow_id -v`
Expected: FAIL (`_rows_from_repos` does not exist).

- [ ] **Step 3: Refactor `flow_status` to use a pure helper**

In `gui/dagster_client.py`, add `import flow_naming` near the top imports (after `import config`), then replace the `flow_status` function with:

```python
def _rows_from_repos(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for repo in nodes:
        sched_by_job = {s["name"]: s for s in repo.get("schedules", [])}
        for job in repo.get("jobs", []):
            if not job["name"].startswith("flow_"):
                continue
            runs = job.get("runs") or []
            last = runs[0] if runs else {}
            sched = sched_by_job.get(f"{job['name']}_schedule", {})
            out.append({
                "job": job["name"],
                "flow_id": flow_naming.flow_id_from_job(job["name"]),
                "schedule_state": (sched.get("scheduleState") or {}).get("status"),
                "last_run_status": last.get("status"),
                "last_run_id": last.get("runId"),
                "last_run_at": last.get("startTime"),
                "link": job_link(job["name"]),
                "run_link": run_link(last["runId"]) if last.get("runId") else None,
            })
    return out


def flow_status() -> list[dict[str, Any]]:
    """Per-job latest-run + schedule state. Empty list if Dagster unreachable."""
    q = """
    query Status {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            jobs { name runs(limit: 1) { runId status startTime } }
            schedules { name scheduleState { status } }
          }
        }
      }
    }"""
    res = _query(q)
    nodes = (res.get("data", {}).get("repositoriesOrError", {}) or {}).get("nodes")
    if not nodes:
        return []
    return _rows_from_repos(nodes)
```

- [ ] **Step 4: Make `run`/`toggle` resolve names from the flow**

In `gui/app.py`, add `import flow_naming  # noqa: E402` with the other flat imports (e.g. after `import flows_store`). Replace `api_flows_run` and `api_flows_toggle`:

```python
@app.post("/api/flows/<fid>/run")
@api
def api_flows_run(fid):
    flow = flows_store.get_flow(fid)
    if flow is None:
        raise KeyError(fid)
    return jsonify(dagster_client.launch_job(flow_naming.job_name(flow)))


@app.post("/api/flows/<fid>/toggle")
@api
def api_flows_toggle(fid):
    enabled = bool(_body().get("enabled", True))
    flow = flows_store.update_flow(fid, enabled=enabled)
    dagster_client.reload_location()
    sched = flow_naming.schedule_name(flow)
    res = dagster_client.start_schedule(sched) if enabled else dagster_client.stop_schedule(sched)
    return jsonify({"enabled": enabled, **res})
```

Also update `api_flows_add` to pass `graph` (from Task 2). Replace it:

```python
@app.post("/api/flows")
@api
def api_flows_add():
    b = _body()
    f = flows_store.add_flow(b.get("name", ""), b.get("nodes", []),
                             b.get("cron", ""), b.get("timezone", "UTC"),
                             b.get("email", {}), b.get("enabled", True),
                             b.get("graph"))
    dagster_client.reload_location()
    return jsonify(f)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python.exe -m pytest tests/test_dagster_client.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add gui/dagster_client.py gui/app.py tests/test_dagster_client.py
git commit -m "feat(flows): flow_status exposes flow_id; run/toggle resolve readable names"
```

---

### Task 5: Server-timezone detection endpoint

**Files:**
- Modify: `requirements-gui.txt` (add `tzlocal`)
- Modify: `gui/config.py` (`server_timezone()`)
- Modify: `gui/app.py` (`/api/flows` returns `server_timezone`)
- Test: `tests/test_app_runs_endpoint.py` (or a new `tests/test_flows_api.py`)

**Interfaces:**
- Produces: `config.server_timezone() -> str` (IANA zone name; `"UTC"` on failure). `/api/flows` JSON gains `"server_timezone"`.

- [ ] **Step 1: Ensure `tzlocal` is installed**

Run: `.venv/Scripts/python.exe -c "import tzlocal; print(tzlocal.get_localzone_name())"`
Expected: prints a zone (e.g. `Asia/Riyadh`). If it errors, run `.venv/Scripts/python.exe -m pip install tzlocal` first.

- [ ] **Step 2: Write the failing test**

Create `tests/test_flows_api.py`:

```python
def test_flows_list_includes_valid_server_timezone(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(gui_app.flows_store, "load_flows", lambda: [])
    monkeypatch.setattr(gui_app.pipelines_store, "load_pipelines", lambda: [])
    client = gui_app.app.test_client()
    resp = client.get("/api/flows")
    assert resp.status_code == 200
    tz = resp.get_json()["server_timezone"]
    from zoneinfo import ZoneInfo
    ZoneInfo(tz)  # must be a real zone (raises otherwise)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_flows_api.py -v`
Expected: FAIL (`KeyError: 'server_timezone'`).

- [ ] **Step 4: Implement**

In `requirements-gui.txt`, add under the Flask line:

```
# Server timezone detection (IANA zone name) for the Flows scheduler default.
tzlocal>=5.0
```

In `gui/config.py`, add:

```python
def server_timezone() -> str:
    """The server's IANA timezone name (e.g. 'Asia/Riyadh'); 'UTC' on failure."""
    try:
        import tzlocal
        return tzlocal.get_localzone_name() or "UTC"
    except Exception:  # noqa: BLE001 - never let tz detection break the API
        return "UTC"
```

In `gui/app.py`, update `api_flows_list`:

```python
@app.get("/api/flows")
@api
def api_flows_list():
    return jsonify({
        "flows": flows_store.load_flows(),
        "pipelines": pipelines_store.load_pipelines(),
        "dagster": dagster_service.status(),
        "server_timezone": config.server_timezone(),
    })
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_flows_api.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add requirements-gui.txt gui/config.py gui/app.py tests/test_flows_api.py
git commit -m "feat(flows): expose server_timezone from /api/flows"
```

---

### Task 6: Vendor Drawflow static assets

**Files:**
- Create: `gui/static/drawflow.min.js`
- Create: `gui/static/drawflow.min.css`

**Interfaces:** none (static assets consumed by Task 7).

- [ ] **Step 1: Download the pinned library files**

Run (PowerShell):

```powershell
Invoke-WebRequest -Uri "https://cdn.jsdelivr.net/npm/drawflow@0.0.59/dist/drawflow.min.js"  -OutFile "gui/static/drawflow.min.js"
Invoke-WebRequest -Uri "https://cdn.jsdelivr.net/npm/drawflow@0.0.59/dist/drawflow.min.css" -OutFile "gui/static/drawflow.min.css"
```

- [ ] **Step 2: Verify the files are real (non-empty, expected content)**

Run: `.venv/Scripts/python.exe -c "import pathlib; js=pathlib.Path('gui/static/drawflow.min.js').read_text(encoding='utf-8'); css=pathlib.Path('gui/static/drawflow.min.css').read_text(encoding='utf-8'); print('js', len(js)); print('css', len(css)); assert 'Drawflow' in js and len(js) > 20000 and len(css) > 500"`
Expected: prints sizes; assertion passes (no output error).

> If the network is unavailable, obtain `drawflow@0.0.59` `dist/drawflow.min.js` and `dist/drawflow.min.css` by another means and place them at those paths — the version pin matters because Task 7's drop-position math relies on Drawflow's `precanvas`/`zoom` API.

- [ ] **Step 3: Commit**

```bash
git add gui/static/drawflow.min.js gui/static/drawflow.min.css
git commit -m "chore(flows): vendor Drawflow 0.0.59 (MIT)"
```

---

### Task 7: Rewrite `flows.html` — canvas, palette, node config, scheduler, timezone

**Files:**
- Replace: `gui/templates/flows.html`

**Interfaces:**
- Consumes: `/api/flows` (`flows`, `pipelines`, `dagster`, `server_timezone`), `/api/dbt/models`, `/api/dbt/tests`, `/api/dagster/flow-status` (rows with `flow_id`), `/api/flows` POST/PUT (body includes `nodes`, `cron`, `timezone`, `graph`, `email`), `/api/flows/<id>/run|toggle|` DELETE. Uses shared helpers from `gui/static/app.js` (`apiGet/apiPost/apiPut/apiDel`, `el`, `$`, `$$`, `esc`, `pill`, `ok`, `err`). Drawflow global from Task 6.

This task has no automated test harness; it is verified by running the app (Step 3). Write the whole file, then verify.

- [ ] **Step 1: Replace `gui/templates/flows.html` with the full new page**

```html
{% extends "base.html" %}
{% block content %}
<link rel="stylesheet" href="{{ url_for('static', filename='drawflow.min.css') }}">
<style>
  .flow-build { display:grid; grid-template-columns: 190px 1fr; gap:12px; }
  .palette { display:flex; flex-direction:column; gap:8px; }
  .palette .chip { display:flex; align-items:center; gap:8px; padding:8px 10px;
    border:1px solid var(--border,#2a3346); border-radius:8px; background:var(--panel-2,#1b1f2a);
    cursor:grab; user-select:none; font-size:.85rem; }
  .palette .chip:hover { border-color: var(--primary,#5b8cff); }
  #dfcanvas { position:relative; height:470px; border:1px solid var(--border,#2a3346);
    border-radius:10px; background:var(--panel,#12151d); overflow:hidden; }
  .drawflow .drawflow-node { background:#1b2130; border:1px solid #2a3346; border-radius:10px;
    color:#e6e9ef; min-width:190px; padding:0; box-shadow:0 2px 10px rgba(0,0,0,.25); }
  .drawflow .drawflow-node.selected { border-color: var(--primary,#5b8cff); }
  .dfnode-title { font-weight:600; padding:8px 10px; border-bottom:1px solid #2a3346;
    display:flex; gap:8px; align-items:center; }
  .dfnode-body { padding:8px 10px; display:flex; flex-direction:column; gap:6px; }
  .dfnode-field { width:100%; }
  .drawflow .drawflow-node .input, .drawflow .drawflow-node .output { background:var(--primary,#5b8cff); }
  .sched-grid { display:flex; gap:10px; align-items:flex-end; flex-wrap:wrap; }
  .sched-hint { margin-top:6px; }
</style>

<div class="page-head">
  <h1><i class="fa-solid fa-diagram-project"></i> Flows</h1>
  <p>Drag components onto the canvas, connect them to set run order, then schedule. Downstream steps run only after upstream steps succeed.</p>
</div>
<div id="dagster-banner"></div>

<div class="row-flex" style="margin-bottom:16px">
  <button class="btn tab primary" data-tab="build">Build</button>
  <button class="btn tab" data-tab="list">All flows <span id="flow-count" class="tag"></span></button>
</div>

<section data-panel="build">
  <div class="panel">
    <div class="panel-head"><h2 id="f-title">New flow</h2>
      <button class="btn ghost sm" id="f-reset" hidden>Cancel edit</button></div>
    <div class="form-row">
      <div><label>Name</label><input id="f-name" placeholder="nightly-masters-then-dq"></div>
      <div><label>Timezone</label><select id="f-tz"></select></div>
    </div>

    <label style="margin-top:12px">DAG</label>
    <div class="flow-build">
      <div class="palette" id="palette">
        <div class="chip" draggable="true" data-kind="pipeline"><i class="fa-solid fa-database"></i> Pipeline</div>
        <div class="chip" draggable="true" data-kind="dbt" data-cmd="run"><i class="fa-solid fa-cube"></i> dbt run</div>
        <div class="chip" draggable="true" data-kind="dbt" data-cmd="test"><i class="fa-solid fa-vial"></i> dbt test</div>
        <div class="chip" draggable="true" data-kind="dbt" data-cmd="build"><i class="fa-solid fa-hammer"></i> dbt build</div>
        <div class="chip" draggable="true" data-kind="command"><i class="fa-solid fa-terminal"></i> Custom command</div>
        <p class="muted" style="font-size:.75rem;margin-top:8px">Drag onto canvas. Drag a node's right port to another node's left port to link (source runs first). Click a node and press Delete to remove.</p>
      </div>
      <div id="dfcanvas"></div>
    </div>

    <label style="margin-top:16px">Schedule</label>
    <div class="sched-grid">
      <div><label>Frequency</label>
        <select id="sched-freq">
          <option value="minutes">Every N minutes</option>
          <option value="hourly">Hourly</option>
          <option value="daily" selected>Daily</option>
          <option value="weekly">Weekly</option>
          <option value="monthly">Monthly</option>
          <option value="advanced">Advanced (cron)</option>
        </select>
      </div>
      <div id="sched-inputs" class="row-flex" style="gap:10px;align-items:flex-end"></div>
    </div>
    <div class="cron-grid" id="sched-adv" hidden style="margin-top:10px">
      <div><label>Min</label><input id="cf-min" class="mono" value="0"></div>
      <div><label>Hour</label><input id="cf-hour" class="mono" value="2"></div>
      <div><label>DoM</label><input id="cf-dom" class="mono" value="*"></div>
      <div><label>Mon</label><input id="cf-mon" class="mono" value="*"></div>
      <div><label>DoW</label><input id="cf-dow" class="mono" value="*"></div>
    </div>
    <div class="muted sched-hint" id="sched-summary">—</div>

    <div class="form-row" style="margin-top:12px">
      <div><label>Email on success</label><input id="f-succ" placeholder="ops@x.com, lead@x.com"></div>
      <div><label>Email on failure</label><input id="f-fail" placeholder="ops@x.com"></div>
    </div>

    <div class="btn-row"><button class="btn primary" id="f-save"><i class="fa-solid fa-floppy-disk"></i> Save flow</button></div>
  </div>
</section>

<section data-panel="list" hidden>
  <div class="panel">
    <div class="panel-head"><h2>All flows</h2><button class="btn sm ghost" id="f-refresh">↻</button></div>
    <div class="table-wrap"><table id="flows-table">
      <thead><tr><th>Name</th><th>Schedule</th><th>State</th><th>Last run</th><th>Nodes</th><th></th></tr></thead>
      <tbody></tbody>
    </table></div>
  </div>
</section>
{% endblock %}

{% block scripts %}
<script src="{{ url_for('static', filename='drawflow.min.js') }}"></script>
<script>
let PIPELINES = [], FLOWS = [], editingId = null, nodeSeq = 0,
    DBT_MODELS = [], DBT_TESTS = [], SERVER_TZ = "UTC", editor = null;

/* ---------- timezone ---------- */
function tzList() {
  try { return Intl.supportedValuesOf("timeZone"); }
  catch (e) { return ["UTC","Asia/Riyadh","Europe/London","America/New_York","Asia/Dubai"]; }
}
function fillTz(selected) {
  const zones = tzList().slice();
  const sel = selected || SERVER_TZ;
  if (sel && !zones.includes(sel)) zones.unshift(sel);
  el("f-tz").innerHTML = zones.map(z => `<option ${z===sel?"selected":""}>${esc(z)}</option>`).join("");
}

/* ---------- scheduler ---------- */
function pad(n){ return String(n).padStart(2,"0"); }
function timeVals(){ const [H,M]=(el("sf-time")?.value||"02:00").split(":").map(Number); return {H:H||0,M:M||0}; }
function presetFromCron(cron) {
  const p = (cron||"").trim().split(/\s+/);
  if (p.length !== 5) return {freq:"advanced"};
  const [mi,ho,dom,mon,dow] = p, num = s => /^\d+$/.test(s);
  let m;
  if ((m = mi.match(/^\*\/(\d+)$/)) && ho==="*"&&dom==="*"&&mon==="*"&&dow==="*") return {freq:"minutes", n:+m[1]};
  if (num(mi)&&ho==="*"&&dom==="*"&&mon==="*"&&dow==="*") return {freq:"hourly", min:+mi};
  if (num(mi)&&num(ho)&&dom==="*"&&mon==="*"&&dow==="*") return {freq:"daily", H:+ho, M:+mi};
  if (num(mi)&&num(ho)&&dom==="*"&&mon==="*"&&num(dow)) return {freq:"weekly", H:+ho, M:+mi, dow:+dow};
  if (num(mi)&&num(ho)&&num(dom)&&mon==="*"&&dow==="*") return {freq:"monthly", H:+ho, M:+mi, dom:+dom};
  return {freq:"advanced"};
}
function schedInputs(preset) {
  const f = el("sched-freq").value, p = preset || {};
  let html = "";
  if (f === "minutes") html = `<div><label>Every N min</label><input id="sf-n" class="mono" style="width:90px" value="${p.n||15}"></div>`;
  else if (f === "hourly") html = `<div><label>At minute</label><input id="sf-min" class="mono" style="width:90px" value="${p.min??0}"></div>`;
  else if (f === "daily") html = `<div><label>At</label><input id="sf-time" type="time" value="${pad(p.H??2)}:${pad(p.M??0)}"></div>`;
  else if (f === "weekly") html =
    `<div><label>Day</label><select id="sf-dow">${["Sun","Mon","Tue","Wed","Thu","Fri","Sat"].map((d,i)=>`<option value="${i}" ${(p.dow??1)===i?"selected":""}>${d}</option>`).join("")}</select></div>
     <div><label>At</label><input id="sf-time" type="time" value="${pad(p.H??2)}:${pad(p.M??0)}"></div>`;
  else if (f === "monthly") html =
    `<div><label>Day of month</label><input id="sf-dom" class="mono" style="width:90px" value="${p.dom??1}"></div>
     <div><label>At</label><input id="sf-time" type="time" value="${pad(p.H??2)}:${pad(p.M??0)}"></div>`;
  el("sched-inputs").innerHTML = html;
  el("sched-adv").hidden = f !== "advanced";
  el("sched-inputs").querySelectorAll("input,select").forEach(x => x.addEventListener("input", updateSched));
  updateSched();
}
function cronFromForm() {
  const f = el("sched-freq").value;
  if (f === "advanced")
    return [el("cf-min"),el("cf-hour"),el("cf-dom"),el("cf-mon"),el("cf-dow")].map(e=>e.value.trim()||"*").join(" ");
  if (f === "minutes") { const n = Math.max(1, +el("sf-n").value||15); return `*/${n} * * * *`; }
  if (f === "hourly") { const m = Math.min(59, Math.max(0, +el("sf-min").value||0)); return `${m} * * * *`; }
  const {H,M} = timeVals();
  if (f === "daily") return `${M} ${H} * * *`;
  if (f === "weekly") return `${M} ${H} * * ${el("sf-dow").value}`;
  if (f === "monthly") { const dom = Math.max(1, +el("sf-dom").value||1); return `${M} ${H} ${dom} * *`; }
  return "0 2 * * *";
}
function cronText(cron) {
  const p = presetFromCron(cron);
  const days=["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];
  if (p.freq==="minutes") return `Every ${p.n} minutes`;
  if (p.freq==="hourly") return `Hourly at :${pad(p.min)}`;
  if (p.freq==="daily") return `Daily at ${pad(p.H)}:${pad(p.M)}`;
  if (p.freq==="weekly") return `Weekly on ${days[p.dow]} at ${pad(p.H)}:${pad(p.M)}`;
  if (p.freq==="monthly") return `Monthly on day ${p.dom} at ${pad(p.H)}:${pad(p.M)}`;
  return `cron: ${cron}`;
}
function updateSched() {
  const cron = cronFromForm();
  el("sched-summary").textContent = `${cronText(cron)}  ·  ${cron}`;
}
function applyCron(cron) {
  const p = presetFromCron(cron);
  el("sched-freq").value = p.freq;
  if (p.freq === "advanced") {
    const parts = (cron||"0 2 * * *").split(/\s+/);
    ["cf-min","cf-hour","cf-dom","cf-mon","cf-dow"].forEach((cid,i)=>el(cid).value=parts[i]||"*");
  }
  schedInputs(p);
}

/* ---------- drawflow node building ---------- */
function pipeOptions(sel){ return PIPELINES.map(p=>`<option value="${p.id}" ${sel===p.id?"selected":""}>${esc(p.name)}</option>`).join(""); }
function selOptions(sel){
  return [...DBT_MODELS.map(m=>`<option value="${esc(m.name)}" ${sel===m.name?"selected":""}>model: ${esc(m.name)}</option>`),
          ...DBT_TESTS.map(t=>`<option value="${esc(t.name)}" ${sel===t.name?"selected":""}>test: ${esc(t.name)}</option>`)].join("");
}
function cmdOptions(sel){ return ["run","test","build"].map(c=>`<option ${sel===c?"selected":""}>${c}</option>`).join(""); }
function nodeHtml(kind, d) {
  d = d || {};
  if (kind === "pipeline")
    return `<div><div class="dfnode-title"><i class="fa-solid fa-database"></i> Pipeline</div>
      <div class="dfnode-body"><select df-pipeline_id class="dfnode-field">${pipeOptions(d.pipeline_id)}</select></div></div>`;
  if (kind === "dbt")
    return `<div><div class="dfnode-title"><i class="fa-solid fa-cube"></i> dbt</div>
      <div class="dfnode-body">
        <select df-dbt_command class="dfnode-field">${cmdOptions(d.dbt_command||"run")}</select>
        <select df-select class="dfnode-field">${selOptions(d.select)}</select></div></div>`;
  return `<div><div class="dfnode-title"><i class="fa-solid fa-terminal"></i> Command</div>
    <div class="dfnode-body"><input df-command class="dfnode-field" placeholder="python tools/x.py" value="${esc(d.command||"")}"></div></div>`;
}
function addFlowNode(kind, data, x, y) {
  const nid = (data && data.node_id) || ("n" + (++nodeSeq));
  const m = /^n(\d+)$/.exec(nid); if (m) nodeSeq = Math.max(nodeSeq, +m[1]);
  const nodeData = Object.assign({node_id: nid}, data || {});
  if (kind === "dbt" && !nodeData.dbt_command) nodeData.dbt_command = "run";
  return editor.addNode(kind, 1, 1, x, y, kind, nodeData, nodeHtml(kind, nodeData), false);
}

/* ---------- editor load / collect ---------- */
function autoLayoutPositions(nodes) {
  const byId = Object.fromEntries(nodes.map(n=>[n.node_id,n])), depth = {};
  function d(id, seen){ if (depth[id]!=null) return depth[id]; if (seen.has(id)) return 0; seen.add(id);
    const deps=(byId[id].deps||[]); depth[id]=deps.length?1+Math.max(...deps.map(x=>d(x,seen))):0; return depth[id]; }
  nodes.forEach(n=>d(n.node_id,new Set()));
  const col = {}, pos = {};
  nodes.forEach(n=>{ const c=depth[n.node_id]; col[c]=(col[c]||0); pos[n.node_id]={x:40+c*240, y:40+col[c]*150}; col[c]++; });
  return pos;
}
function loadEditor(f) {
  editor.clear(); nodeSeq = 0;
  const nodes = (f && f.nodes) || [];
  if (!nodes.length) return;
  let positions = null;
  const g = f && f.graph && f.graph.drawflow && f.graph.drawflow.Home && f.graph.drawflow.Home.data;
  if (g) { positions = {}; for (const id in g) { const nd=g[id].data||{}; positions[nd.node_id||("n"+id)]={x:g[id].pos_x,y:g[id].pos_y}; } }
  if (!positions) positions = autoLayoutPositions(nodes);
  const idMap = {};
  nodes.forEach(n => {
    const p = positions[n.node_id] || {x:40,y:40};
    const data = n.kind==="dbt" ? {node_id:n.node_id, dbt_command:(n.dbt||{}).dbt_command, select:(n.dbt||{}).select}
               : n.kind==="command" ? {node_id:n.node_id, command:n.command}
               : {node_id:n.node_id, pipeline_id:n.pipeline_id};
    idMap[n.node_id] = addFlowNode(n.kind, data, p.x, p.y);
  });
  nodes.forEach(n => (n.deps||[]).forEach(dep => {
    if (idMap[dep] != null && idMap[n.node_id] != null)
      editor.addConnection(idMap[dep], idMap[n.node_id], "output_1", "input_1");
  }));
}
function collectNodes() {
  const home = editor.export().drawflow.Home.data;
  const idToNode = {};
  for (const id in home) idToNode[id] = (home[id].data && home[id].data.node_id) || ("n"+id);
  const out = [];
  for (const id in home) {
    const n = home[id], kind = n.name, data = n.data||{}, deps = [];
    for (const inName in (n.inputs||{})) (n.inputs[inName].connections||[]).forEach(c => deps.push(idToNode[c.node]));
    const base = { node_id: idToNode[id], kind, deps };
    if (kind === "pipeline") base.pipeline_id = data.pipeline_id || (PIPELINES[0]||{}).id;
    else if (kind === "dbt") base.dbt = { dbt_command: data.dbt_command||"run", select: data.select||"" };
    else if (kind === "command") base.command = data.command||"";
    out.push(base);
  }
  return out;
}

/* ---------- save / crud ---------- */
async function save() {
  const nodes = collectNodes();
  if (!nodes.length) { err("Add at least one node"); return; }
  const body = { name: el("f-name").value.trim(), nodes, cron: cronFromForm(),
    timezone: el("f-tz").value, graph: editor.export(),
    email: { on_success: el("f-succ").value, on_failure: el("f-fail").value } };
  try {
    if (editingId) await apiPut(`/api/flows/${editingId}`, body);
    else await apiPost("/api/flows", body);
    ok("Saved"); resetForm(); await load(); showTab("list");
  } catch (e) { err(e.message); }
}
function resetForm() {
  editingId=null; nodeSeq=0; el("f-name").value=""; if (editor) editor.clear();
  el("f-reset").hidden=true; el("f-title").textContent="New flow";
  el("f-succ").value=""; el("f-fail").value=""; fillTz(SERVER_TZ);
  el("sched-freq").value="daily"; applyCron("0 2 * * *");
}

async function load() {
  const d = await apiGet("/api/flows");
  PIPELINES = d.pipelines; FLOWS = d.flows; SERVER_TZ = d.server_timezone || "UTC";
  try { const [m,t]=[await apiGet("/api/dbt/models"), await apiGet("/api/dbt/tests")];
        DBT_MODELS=m.models||[]; DBT_TESTS=t.tests||[]; }
  catch (e) { DBT_MODELS=[]; DBT_TESTS=[]; }
  if (!editingId) fillTz(SERVER_TZ);
  el("flow-count").textContent = FLOWS.length;
  el("dagster-banner").innerHTML = d.dagster.running
    ? `<div class="banner info">Dagster running — <a href="${d.dagster.url}" target="_blank">open UI</a></div>`
    : `<div class="banner warn">Dagster is not running. <button class="btn sm" onclick="startDagster()">Start Dagster</button></div>`;
  renderFlows();
}
async function renderFlows() {
  let status=[]; try { status=await apiGet("/api/dagster/flow-status"); } catch(e){}
  const byFlow = Object.fromEntries(status.map(s=>[s.flow_id, s]));
  $("#flows-table tbody").innerHTML = FLOWS.map(f => {
    const st = byFlow[f.id] || {};
    const link = st.run_link ? `<a href="${st.run_link}" target="_blank">${esc(st.last_run_status||"—")}</a>` : (st.last_run_status||"—");
    return `<tr>
      <td><b>${esc(f.name)}</b></td>
      <td>${esc(cronText(f.cron))} <span class="muted mono">${esc(f.timezone)}</span></td>
      <td>${pill(f.enabled ? "ok" : "stopped")}</td>
      <td>${link}</td>
      <td>${f.nodes.length}</td>
      <td class="row-flex">
        <button class="btn sm" onclick="runFlow('${f.id}')">Run now</button>
        <button class="btn sm" onclick="toggleFlow('${f.id}', ${f.enabled?"false":"true"})">${f.enabled?"Disable":"Enable"}</button>
        <button class="btn sm ghost" onclick="editFlow('${f.id}')">Edit</button>
        <button class="btn sm bad" onclick="delFlow('${f.id}')">Delete</button>
      </td></tr>`;
  }).join("") || `<tr><td colspan="6" class="muted">No flows yet.</td></tr>`;
}
function editFlow(id) {
  const f = FLOWS.find(x=>x.id===id); if (!f) return;
  editingId=id; el("f-title").textContent="Edit: "+f.name; el("f-reset").hidden=false;
  el("f-name").value=f.name; fillTz(f.timezone);
  el("f-succ").value=(f.email.on_success||[]).join(", "); el("f-fail").value=(f.email.on_failure||[]).join(", ");
  applyCron(f.cron);
  loadEditor(f);
  showTab("build"); window.scrollTo({top:0,behavior:"smooth"});
}
async function runFlow(id){ try{ const r=await apiPost(`/api/flows/${id}/run`,{}); r.ok?ok("Launched run "+r.run_id.slice(0,8)):err(r.error); setTimeout(load,1500);}catch(e){err(e.message);} }
async function toggleFlow(id,en){ try{ await apiPost(`/api/flows/${id}/toggle`,{enabled:en==="true"||en===true}); load(); }catch(e){err(e.message);} }
async function delFlow(id){ if(!confirm("Delete this flow?"))return; try{ await apiDel(`/api/flows/${id}`); ok("Deleted"); load(); }catch(e){err(e.message);} }
async function startDagster(){ try{ await apiPost("/api/dagster/start",{}); ok("Starting Dagster…"); setTimeout(load,4000);}catch(e){err(e.message);} }

function showTab(t){ $$(".tab").forEach(b=>b.classList.toggle("primary",b.dataset.tab===t)); $$("[data-panel]").forEach(p=>p.hidden=p.dataset.panel!==t); if(t==="list") renderFlows(); }

/* ---------- init ---------- */
function initEditor() {
  editor = new Drawflow(el("dfcanvas"));
  editor.reroute = true;
  editor.start();
  const canvas = el("dfcanvas");
  canvas.addEventListener("dragover", e => e.preventDefault());
  canvas.addEventListener("drop", e => {
    e.preventDefault();
    const kind = e.dataTransfer.getData("kind"); if (!kind) return;
    const cmd = e.dataTransfer.getData("cmd");
    const pc = editor.precanvas, z = editor.zoom;
    const x = e.clientX * (pc.clientWidth / (pc.clientWidth * z)) - (pc.getBoundingClientRect().x * (pc.clientWidth / (pc.clientWidth * z)));
    const y = e.clientY * (pc.clientHeight / (pc.clientHeight * z)) - (pc.getBoundingClientRect().y * (pc.clientHeight / (pc.clientHeight * z)));
    addFlowNode(kind, kind==="dbt" ? {dbt_command: cmd||"run"} : {}, x, y);
  });
  $$("#palette .chip").forEach(chip => chip.addEventListener("dragstart", e => {
    e.dataTransfer.setData("kind", chip.dataset.kind);
    e.dataTransfer.setData("cmd", chip.dataset.cmd || "");
  }));
}

$$(".tab").forEach(b=>b.onclick=()=>showTab(b.dataset.tab));
el("sched-freq").addEventListener("change", () => schedInputs());
el("f-save").onclick=save; el("f-reset").onclick=resetForm; el("f-refresh").onclick=load;
initEditor();
fillTz(SERVER_TZ);
applyCron("0 2 * * *");
load();
</script>
{% endblock %}
```

- [ ] **Step 2: Start the app**

Run (background): `.venv/Scripts/python.exe gui/app.py`
Then open `http://127.0.0.1:8765/flows`.

- [ ] **Step 3: Manual verification (exercise every feature)**

Confirm each, fixing any Drawflow quirk before moving on:

1. **Timezone** — the Timezone dropdown defaults to the server zone (matches `.venv/Scripts/python.exe -c "import tzlocal;print(tzlocal.get_localzone_name())"`).
2. **Palette drag-drop** — drag each chip (Pipeline, dbt run, dbt test, dbt build, Custom command) onto the canvas; a configured node appears where dropped.
3. **Connect** — drag one node's right port to another's left port; an edge is drawn.
4. **Configure** — set a pipeline, a dbt select, and a command string on the respective nodes.
5. **Scheduler** — switch Frequency across every option; the summary line and cron update; "Advanced" shows the 5 cron boxes.
6. **Save** — save the flow; it appears under "All flows" with a human-readable schedule + timezone.
7. **Edit round-trip** — click Edit; the canvas restores nodes at their saved positions with the right config, edges, schedule preset, timezone. Save again; no duplication/loss.
8. **Backward compat** — if a pre-existing flow exists in `gui/state/flows.json` without `graph`, Edit auto-lays it out correctly.
9. **Dagster names** — with Dagster running, open its UI: the asset group is the flow slug (e.g. `nightly_masters`), assets are `nightly_masters__<id>/<node>`, and the job/schedule are `flow_nightly_masters__<id>[_schedule]`.
10. **Run now / Enable-Disable / Delete** from the list still work (Run launches; the row's Last-run/State reflect it after refresh).

Stop the app when done (Ctrl-C / kill the background process).

- [ ] **Step 4: Commit**

```bash
git add gui/templates/flows.html
git commit -m "feat(flows): drag-and-drop canvas, preset scheduler, timezone dropdown"
```

---

### Task 8: Full test-suite + docs pass

**Files:**
- Modify: `docs/superpowers/specs/2026-07-12-flow-builder-enhancement-design.md` (mark implemented) — optional
- Verify only otherwise.

- [ ] **Step 1: Run the whole suite**

Run: `.venv/Scripts/python.exe -m pytest -q`
Expected: all tests pass. Investigate and fix any regression before proceeding.

- [ ] **Step 2: Commit any fixups**

```bash
git add -A
git commit -m "test(flows): fix up suite for readable-naming changes"
```

---

## Self-Review

**1. Spec coverage**

- Timezone always matches server (auto-detect, prefill, editable) → Task 5 (`server_timezone`), Task 7 (dropdown default). ✓
- Drag-and-drop DAG with configurable pipeline / dbt run / dbt test / custom command → Task 7 (Drawflow palette + node config), Task 2/Task 3 (`command` kind). ✓ (dbt build included as a bonus chip.)
- Manual connections declaring dependencies → Task 7 (`collectNodes` maps connections → `deps`). ✓
- Scheduler dropdown (daily/monthly/weekly/hourly/etc.) + time, plus advanced → Task 7 (`schedInputs`/`cronFromForm`/`presetFromCron`). ✓
- Readable Dagster asset/job/schedule naming → Task 1 (`flow_naming`), Task 3 (build/assets/email), Task 4 (run/toggle + `flow_status.flow_id`). ✓
- Backward compatibility of existing flows → Task 7 Step 3.8 (auto-layout), Task 2 (`graph` optional). ✓

**2. Placeholder scan** — no TBD/TODO; every code step contains full code. ✓

**3. Type/name consistency**
- `flow_naming` functions used identically across Tasks 3/4. ✓
- `asset_key(prefix, node_id)` new signature used in both `assets.py` and `build.py` (Task 3). ✓
- `build_email_sensors(base, ...)` — call site in `build.py` passes `state.flow_naming.base_name(flow)`. ✓
- Front-end `presetFromCron`/`cronFromForm`/`cronText`/`applyCron`/`schedInputs` names all defined and cross-referenced in Task 7. ✓
- `collectNodes` emits `{node_id, kind, deps, pipeline_id|dbt|command}` exactly matching `flows_store.validate_flow` branches (Task 2). ✓
- `/api/flows` body `graph` is produced by `save()` (Task 7) and consumed by `add_flow`/`update_flow` (Task 2) + `api_flows_add` (Task 4). ✓
