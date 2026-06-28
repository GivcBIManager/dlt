# Dagster Scheduling Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the cron-based scheduler with a Dagster orchestration layer where users compose DAGs ("Flows") from a saved Pipeline library, wire dependency-on-success, schedule them, and get success/failure emails — all GUI-supervised, in the existing `.venv`, on Windows and Linux.

**Architecture:** A new `orchestrator/` Dagster code location (scaffolded with `create-dagster`) builds all Dagster objects dynamically from `gui/state/pipelines.json` + `gui/state/flows.json`: one `@asset` per flow node (subprocessing the existing entry-point scripts via the GUI's `commands.build_argv`), `deps=` edges giving topological run-on-success, one asset job + schedule per flow, and success/failure run-status sensors emailing over global SMTP. The Flask GUI gains a Pipeline library page, a Flow builder/list page, an SMTP settings form, and a supervisor (`gui/dagster_service.py`) + GraphQL client (`gui/dagster_client.py`) that launch `dagster dev`, reload the code location on change, and surface live status + deep links.

**Tech Stack:** Python 3.13, Flask (existing GUI), Dagster (`dagster`, `dagster-webserver`, `dagster-graphql`), vanilla JS/Jinja templates, `pytest` (new, dev-only), stdlib `smtplib`, `subprocess`.

## Global Constraints

- **Cross-platform:** every code path must work on Windows and Linux. Use the existing process-group kill pattern from `gui/pipeline_runner.py` (`CREATE_NEW_PROCESS_GROUP`/`CTRL_BREAK_EVENT` on Windows, `start_new_session`/`killpg` on POSIX). No `crontab`, no shell=True.
- **Single venv:** Dagster installs into the existing `.venv` via `requirements-gui.txt`. Do **not** use `create-dagster --uv-sync` (it makes a separate venv).
- **Asset execution contract:** an asset runs *exactly* the argv the Run page would — always go through `commands.build_argv(spec)`. Never re-implement command construction.
- **Reuse, don't duplicate:** the orchestrator imports `commands` and `config` from `gui/` (verified import-clean without Flask). SMTP editing reuses the surgical block-edit + backup + re-parse-validate pattern from `gui/connections.py`.
- **State files are local/gitignored** (`gui/state/` already in `.gitignore`): `pipelines.json`, `flows.json` follow the `schedules.json` convention.
- **Dagster version:** pin a single 3.13-compatible release across all requirements (chosen in Task 1; same version everywhere). All GraphQL queries target that version.
- **Ports/host:** Dagster defaults to `127.0.0.1:3000`, overridable via `OASIS_DAGSTER_HOST` / `OASIS_DAGSTER_PORT`.
- **Standards:** follow `dignified-python` (modern type syntax, pathlib, explicit checks). Match the surrounding file style.

## File Structure

**New files**
- `orchestrator/` — scaffolded Dagster project (Task 2). Package dir `orchestrator/src/orchestrator/`:
  - `state.py` — repo-root resolution + JSON readers + bridge to `gui/commands` & `gui/config`.
  - `assets.py` — asset factory (one subprocess-running asset per flow node).
  - `email.py` — SMTP send, body render, success/failure sensor factories.
  - `build.py` — `build_all_defs()` → `dg.Definitions`.
  - `definitions.py` — `defs = build_all_defs()` (the launch target).
- `gui/pipelines_store.py` — CRUD for `pipelines.json`.
- `gui/flows_store.py` — CRUD + cycle/ref validation for `flows.json`.
- `gui/smtp_config.py` — read/write `[smtp]` in `secrets.toml`; send test email.
- `gui/dagster_service.py` — supervise `dagster dev` (start/stop/status, `dagster.yaml` gen).
- `gui/dagster_client.py` — GraphQL: status, reload, start/stop schedule, launch run, deep links.
- `gui/templates/pipelines.html`, `gui/templates/flows.html`.
- `requirements-dev.txt` — `pytest`.
- `tests/conftest.py`, `tests/test_pipelines_store.py`, `tests/test_flows_store.py`, `tests/test_orchestrator_state.py`, `tests/test_orchestrator_assets.py`, `tests/test_orchestrator_email.py`, `tests/test_orchestrator_build.py`, `tests/test_smtp_config.py`, `tests/test_dagster_service.py`, `tests/test_dagster_client.py`.

**Modified files**
- `requirements-gui.txt` — add Dagster deps.
- `gui/config.py` — new path/port constants.
- `gui/app.py` — new routes + API; remove `/schedule`; mount Dagster supervisor.
- `gui/templates/base.html` — nav: add Pipelines + Flows, remove Schedule.
- `.gitignore` — `.dagster_home/`.
- `setup.ps1` / `setup.sh` — scaffold/editable-install note for `orchestrator`.

**Retired (removed from active path)**
- `gui/cron_manager.py`, `gui/templates/schedule.html` (deleted in Task 15).

---

## Phase 0 — Foundations

### Task 1: Dependencies, config constants, gitignore, test harness

**Files:**
- Modify: `requirements-gui.txt`
- Create: `requirements-dev.txt`
- Modify: `gui/config.py`
- Modify: `.gitignore`
- Create: `tests/conftest.py`
- Test: `tests/test_config_constants.py`

**Interfaces:**
- Produces: `config.PIPELINES_JSON`, `config.FLOWS_JSON`, `config.ORCHESTRATOR_DIR`, `config.DAGSTER_HOME: Path`; `config.dagster_host() -> str`, `config.dagster_port() -> int`, `config.dagster_base_url() -> str`.

- [ ] **Step 1: Pick and pin the Dagster version.** Run `.venv/Scripts/python.exe -m pip index versions dagster` (or check pypi) and choose the latest release that supports Python 3.13. Use that exact version (referred to below as `X.Y.Z`) in every requirement.

- [ ] **Step 2: Add Dagster deps to `requirements-gui.txt`.** Append:

```
# Dagster orchestration (scheduling module). Pin one version across the stack.
dagster==X.Y.Z
dagster-webserver==X.Y.Z
dagster-graphql==X.Y.Z
```

- [ ] **Step 3: Create `requirements-dev.txt`:**

```
# Dev/test-only dependencies.
-r requirements-gui.txt
pytest>=8.0
```

- [ ] **Step 4: Install dev deps.** Run: `.venv/Scripts/python.exe -m pip install -r requirements-dev.txt`
Expected: dagster, dagster-webserver, dagster-graphql, pytest install without error.

- [ ] **Step 5: Add constants to `gui/config.py`.** After the `SCHEDULES_JSON` line add:

```python
PIPELINES_JSON = STATE_DIR / "pipelines.json"
FLOWS_JSON = STATE_DIR / "flows.json"

# --- Dagster orchestration -------------------------------------------------- #
ORCHESTRATOR_DIR = REPO_ROOT / "orchestrator"
DAGSTER_HOME = REPO_ROOT / ".dagster_home"


def dagster_host() -> str:
    return os.environ.get("OASIS_DAGSTER_HOST", "127.0.0.1")


def dagster_port() -> int:
    return int(os.environ.get("OASIS_DAGSTER_PORT", "3000"))


def dagster_base_url() -> str:
    return f"http://{dagster_host()}:{dagster_port()}"
```

- [ ] **Step 6: Add `.dagster_home/` to `.gitignore`.** Under the `# GUI runtime artifacts` block add a line:

```
.dagster_home/
```

- [ ] **Step 7: Create `tests/conftest.py`:**

```python
"""Pytest shared fixtures: import paths + isolated state dir."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "gui"))
sys.path.insert(0, str(REPO_ROOT / "orchestrator" / "src"))


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    """Point config's JSON state at a temp dir so stores never touch real state."""
    import config

    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    monkeypatch.setattr(config, "PIPELINES_JSON", tmp_path / "pipelines.json")
    monkeypatch.setattr(config, "FLOWS_JSON", tmp_path / "flows.json")
    return tmp_path
```

- [ ] **Step 8: Write `tests/test_config_constants.py`:**

```python
def test_dagster_defaults(monkeypatch):
    import config
    monkeypatch.delenv("OASIS_DAGSTER_PORT", raising=False)
    monkeypatch.delenv("OASIS_DAGSTER_HOST", raising=False)
    assert config.dagster_port() == 3000
    assert config.dagster_base_url() == "http://127.0.0.1:3000"
    assert config.PIPELINES_JSON.name == "pipelines.json"
    assert config.FLOWS_JSON.name == "flows.json"
```

- [ ] **Step 9: Run the test.** Run: `.venv/Scripts/python.exe -m pytest tests/test_config_constants.py -v`
Expected: PASS.

- [ ] **Step 10: Commit.**

```bash
git add requirements-gui.txt requirements-dev.txt gui/config.py .gitignore tests/conftest.py tests/test_config_constants.py
git commit -m "feat(config): Dagster deps, config constants, test harness"
```

---

### Task 2: Scaffold the `orchestrator` Dagster code location

**Files:**
- Create: `orchestrator/` (scaffold) + editable install
- Modify: `setup.ps1`, `setup.sh`

**Interfaces:**
- Produces: an importable `orchestrator.definitions` module exposing a `defs` symbol that `dagster dev -m orchestrator.definitions` can load.

- [ ] **Step 1: Scaffold the project (no separate venv).** From the repo root run:
`uvx create-dagster@X.Y.Z project orchestrator`
(Answer "no"/skip if it offers to create a venv; we install into `.venv`.) This creates `orchestrator/` with `pyproject.toml` and `orchestrator/src/orchestrator/` containing the package.

- [ ] **Step 2: Confirm the package path.** Run: `ls orchestrator/src/orchestrator`
Expected: a package dir with `definitions.py` (and possibly a `defs/` folder). Note the actual path; the modules in later tasks go in this package dir. If the layout differs, adjust later task paths to match.

- [ ] **Step 3: Editable-install the package into `.venv`.** Run: `.venv/Scripts/python.exe -m pip install -e orchestrator`
Expected: installs `orchestrator` so `import orchestrator` works in the venv.

- [ ] **Step 4: Replace generated `definitions.py`** at `orchestrator/src/orchestrator/definitions.py` with a placeholder that loads cleanly (real builder lands in Task 8):

```python
"""Dagster code location entry point. ``defs`` is the launch target.

Launched by gui/dagster_service.py via ``dagster dev -m orchestrator.definitions``.
"""
from __future__ import annotations

import dagster as dg

defs = dg.Definitions()
```

If the scaffold created a `defs/` autoload folder, leave it empty/unused — `-m orchestrator.definitions` loads the `defs` object directly and does not depend on it.

- [ ] **Step 5: Smoke-test the code location loads.** Run:
`.venv/Scripts/python.exe -c "import orchestrator.definitions as d; print(type(d.defs))"`
Expected: `<class 'dagster._core.definitions.definitions_class.Definitions'>` (or similar).

- [ ] **Step 6: Add a setup note.** In `setup.ps1` after the `pip install -r requirements-gui.txt` line add:

```powershell
Write-Host "==> Installing the orchestrator code location (editable)"
& $vpy -m pip install -e orchestrator
```

In `setup.sh` after its `pip install -r requirements-gui.txt` line add:

```sh
echo "==> Installing the orchestrator code location (editable)"
"$VPY" -m pip install -e orchestrator
```

(Use the script's existing venv-python variable name; check the file.)

- [ ] **Step 7: Commit.**

```bash
git add orchestrator setup.ps1 setup.sh
git commit -m "feat(orchestrator): scaffold Dagster code location"
```

---

## Phase 1 — Data stores

### Task 3: Pipeline library store

**Files:**
- Create: `gui/pipelines_store.py`
- Test: `tests/test_pipelines_store.py`

**Interfaces:**
- Consumes: `commands.build_argv`, `commands.preview`, `config.PIPELINES_JSON`, `config.STATE_DIR`.
- Produces:
  - `load_pipelines() -> list[dict]`
  - `get_pipeline(pid: str) -> dict | None`
  - `add_pipeline(name: str, spec: dict) -> dict`
  - `update_pipeline(pid: str, **fields) -> dict`  (fields: `name`, `spec`)
  - `delete_pipeline(pid: str) -> bool`
  - Each pipeline dict: `{id, name, spec, command, label, created_at}`.

- [ ] **Step 1: Write `tests/test_pipelines_store.py`:**

```python
import pytest


def test_add_and_get_pipeline(state_dir):
    import pipelines_store as ps
    spec = {"script": "oracle_to_iceberg", "mode": "INCREMENTAL",
            "category": "masters", "tables": "PATIENT_MASTER_DATA"}
    p = ps.add_pipeline("masters-incr", spec)
    assert p["id"] and p["name"] == "masters-incr"
    assert "oracle_to_iceberg.py" in p["command"]
    assert ps.get_pipeline(p["id"])["name"] == "masters-incr"
    assert len(ps.load_pipelines()) == 1


def test_add_rejects_bad_spec(state_dir):
    import pipelines_store as ps
    with pytest.raises(ValueError):
        ps.add_pipeline("bad", {"script": "custom", "custom": ""})


def test_update_and_delete(state_dir):
    import pipelines_store as ps
    p = ps.add_pipeline("a", {"script": "dq_check"})
    ps.update_pipeline(p["id"], name="renamed")
    assert ps.get_pipeline(p["id"])["name"] == "renamed"
    assert ps.delete_pipeline(p["id"]) is True
    assert ps.load_pipelines() == []
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_pipelines_store.py -v`
Expected: FAIL (`ModuleNotFoundError: pipelines_store`).

- [ ] **Step 3: Write `gui/pipelines_store.py`:**

```python
"""CRUD for the saved Pipeline library (``gui/state/pipelines.json``).

A pipeline is a named, validated command spec (the same spec the Run page
builds). Each becomes one Dagster asset. Stored as the source of truth so the
orchestrator can read it without importing Flask.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

import commands
import config


def _load() -> list[dict[str, Any]]:
    p = config.PIPELINES_JSON
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save(items: list[dict[str, Any]]) -> None:
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.PIPELINES_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(items, indent=2), encoding="utf-8")
    tmp.replace(config.PIPELINES_JSON)


def _render(p: dict[str, Any]) -> dict[str, Any]:
    _, label = commands.build_argv(p["spec"])
    out = dict(p)
    out["command"] = commands.preview(p["spec"])
    out["label"] = label
    return out


def load_pipelines() -> list[dict[str, Any]]:
    return [_render(p) for p in _load()]


def get_pipeline(pid: str) -> dict[str, Any] | None:
    return next((_render(p) for p in _load() if p["id"] == pid), None)


def add_pipeline(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    commands.build_argv(spec)  # validates; raises ValueError on bad spec
    name = (name or "").strip()
    if not name:
        raise ValueError("Pipeline name is required")
    items = _load()
    if any(p["name"] == name for p in items):
        raise ValueError(f"Pipeline '{name}' already exists")
    p = {"id": uuid.uuid4().hex[:8], "name": name, "spec": spec,
         "created_at": datetime.now().isoformat(timespec="seconds")}
    items.append(p)
    _save(items)
    return _render(p)


def update_pipeline(pid: str, **fields: Any) -> dict[str, Any]:
    items = _load()
    for p in items:
        if p["id"] == pid:
            if fields.get("spec") is not None:
                commands.build_argv(fields["spec"])
            for k in ("name", "spec"):
                if fields.get(k) is not None:
                    p[k] = fields[k]
            _save(items)
            return _render(p)
    raise KeyError(pid)


def delete_pipeline(pid: str) -> bool:
    items = _load()
    new = [p for p in items if p["id"] != pid]
    if len(new) == len(items):
        return False
    _save(new)
    return True
```

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_pipelines_store.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit.**

```bash
git add gui/pipelines_store.py tests/test_pipelines_store.py
git commit -m "feat(gui): pipeline library store"
```

---

### Task 4: Flow store with cycle/reference validation

**Files:**
- Create: `gui/flows_store.py`
- Test: `tests/test_flows_store.py`

**Interfaces:**
- Consumes: `config.FLOWS_JSON`, `config.STATE_DIR`, `pipelines_store.get_pipeline`.
- Produces:
  - `validate_flow(nodes: list[dict], *, known_pipeline_ids: set[str]) -> None`  (raises `ValueError` on cycle, dup `node_id`, unknown dep, unknown `pipeline_id`)
  - `load_flows() -> list[dict]`
  - `get_flow(fid: str) -> dict | None`
  - `add_flow(name, nodes, cron, timezone, email, enabled=True) -> dict`
  - `update_flow(fid, **fields) -> dict`
  - `delete_flow(fid) -> bool`
  - `referencing_flows(pipeline_id: str) -> list[dict]`
  - Node shape: `{"node_id": str, "pipeline_id": str, "deps": list[str]}`.
  - Flow shape: `{id, name, nodes, cron, timezone, email:{on_success:[],on_failure:[]}, enabled, created_at}`.

- [ ] **Step 1: Write `tests/test_flows_store.py`:**

```python
import pytest


def _seed_pipelines(state_dir):
    import pipelines_store as ps
    a = ps.add_pipeline("a", {"script": "dq_check"})
    b = ps.add_pipeline("b", {"script": "dq_check"})
    return a["id"], b["id"]


def test_validate_rejects_cycle():
    import flows_store as fs
    nodes = [
        {"node_id": "n1", "pipeline_id": "p", "deps": ["n2"]},
        {"node_id": "n2", "pipeline_id": "p", "deps": ["n1"]},
    ]
    with pytest.raises(ValueError, match="cycle"):
        fs.validate_flow(nodes, known_pipeline_ids={"p"})


def test_validate_rejects_unknown_dep():
    import flows_store as fs
    nodes = [{"node_id": "n1", "pipeline_id": "p", "deps": ["ghost"]}]
    with pytest.raises(ValueError):
        fs.validate_flow(nodes, known_pipeline_ids={"p"})


def test_add_flow_and_reference(state_dir):
    import flows_store as fs
    pa, pb = _seed_pipelines(state_dir)
    nodes = [
        {"node_id": "n1", "pipeline_id": pa, "deps": []},
        {"node_id": "n2", "pipeline_id": pb, "deps": ["n1"]},
    ]
    f = fs.add_flow("nightly", nodes, "0 2 * * *", "Asia/Riyadh",
                    {"on_success": ["x@y.com"], "on_failure": ["x@y.com"]})
    assert f["id"] and f["enabled"] is True
    assert [r["id"] for r in fs.referencing_flows(pa)] == [f["id"]]


def test_add_flow_rejects_bad_cron(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    with pytest.raises(ValueError):
        fs.add_flow("bad", nodes, "not a cron", "UTC", {})
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_flows_store.py -v`
Expected: FAIL (`ModuleNotFoundError: flows_store`).

- [ ] **Step 3: Write `gui/flows_store.py`:**

```python
"""CRUD + validation for Flows (DAGs) in ``gui/state/flows.json``.

A flow is a set of nodes (each referencing a saved pipeline) plus dependency
edges, a cron schedule + timezone, email recipients, and an enabled flag. The
orchestrator turns each flow into Dagster assets + a job + a schedule + email
sensors.
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any

import config
import pipelines_store

_CRON_FIELD = re.compile(r"^[\d*/,\-]+$")


def validate_cron(expr: str) -> None:
    expr = (expr or "").strip()
    parts = expr.split()
    if len(parts) != 5 or not all(_CRON_FIELD.match(p) for p in parts):
        raise ValueError("Cron expression must have 5 valid fields (min hour dom mon dow)")


def validate_flow(nodes: list[dict[str, Any]], *, known_pipeline_ids: set[str]) -> None:
    if not nodes:
        raise ValueError("A flow needs at least one node")
    ids = [n["node_id"] for n in nodes]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate node_id in flow")
    idset = set(ids)
    for n in nodes:
        if n["pipeline_id"] not in known_pipeline_ids:
            raise ValueError(f"Node {n['node_id']} references unknown pipeline {n['pipeline_id']}")
        for d in n.get("deps", []):
            if d not in idset:
                raise ValueError(f"Node {n['node_id']} depends on unknown node {d}")
            if d == n["node_id"]:
                raise ValueError(f"Node {n['node_id']} depends on itself")
    _assert_acyclic(nodes)


def _assert_acyclic(nodes: list[dict[str, Any]]) -> None:
    deps = {n["node_id"]: list(n.get("deps", [])) for n in nodes}
    WHITE, GREY, BLACK = 0, 1, 2
    color = {k: WHITE for k in deps}

    def visit(u: str) -> None:
        color[u] = GREY
        for v in deps[u]:
            if color[v] == GREY:
                raise ValueError(f"Dependency cycle detected at node {v}")
            if color[v] == WHITE:
                visit(v)
        color[u] = BLACK

    for k in deps:
        if color[k] == WHITE:
            visit(k)


def _load() -> list[dict[str, Any]]:
    p = config.FLOWS_JSON
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save(items: list[dict[str, Any]]) -> None:
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.FLOWS_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(items, indent=2), encoding="utf-8")
    tmp.replace(config.FLOWS_JSON)


def _known_pipeline_ids() -> set[str]:
    return {p["id"] for p in pipelines_store.load_pipelines()}


def load_flows() -> list[dict[str, Any]]:
    return _load()


def get_flow(fid: str) -> dict[str, Any] | None:
    return next((f for f in _load() if f["id"] == fid), None)


def _normalize_email(email: dict[str, Any] | None) -> dict[str, list[str]]:
    email = email or {}
    def clean(v: Any) -> list[str]:
        if isinstance(v, str):
            v = re.split(r"[,\s]+", v)
        return [s.strip() for s in (v or []) if s and s.strip()]
    return {"on_success": clean(email.get("on_success")),
            "on_failure": clean(email.get("on_failure"))}


def add_flow(name: str, nodes: list[dict[str, Any]], cron: str, timezone: str,
             email: dict[str, Any] | None, enabled: bool = True) -> dict[str, Any]:
    name = (name or "").strip()
    if not name:
        raise ValueError("Flow name is required")
    validate_cron(cron)
    validate_flow(nodes, known_pipeline_ids=_known_pipeline_ids())
    items = _load()
    if any(f["name"] == name for f in items):
        raise ValueError(f"Flow '{name}' already exists")
    f = {"id": uuid.uuid4().hex[:8], "name": name, "nodes": nodes,
         "cron": cron.strip(), "timezone": (timezone or "UTC").strip(),
         "email": _normalize_email(email), "enabled": bool(enabled),
         "created_at": datetime.now().isoformat(timespec="seconds")}
    items.append(f)
    _save(items)
    return f


def update_flow(fid: str, **fields: Any) -> dict[str, Any]:
    items = _load()
    for f in items:
        if f["id"] == fid:
            if fields.get("cron") is not None:
                validate_cron(fields["cron"])
            if fields.get("nodes") is not None:
                validate_flow(fields["nodes"], known_pipeline_ids=_known_pipeline_ids())
            for k in ("name", "nodes", "cron", "timezone", "enabled"):
                if fields.get(k) is not None:
                    f[k] = fields[k]
            if fields.get("email") is not None:
                f["email"] = _normalize_email(fields["email"])
            _save(items)
            return f
    raise KeyError(fid)


def delete_flow(fid: str) -> bool:
    items = _load()
    new = [f for f in items if f["id"] != fid]
    if len(new) == len(items):
        return False
    _save(new)
    return True


def referencing_flows(pipeline_id: str) -> list[dict[str, Any]]:
    return [f for f in _load()
            if any(n["pipeline_id"] == pipeline_id for n in f["nodes"])]
```

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_flows_store.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit.**

```bash
git add gui/flows_store.py tests/test_flows_store.py
git commit -m "feat(gui): flow store with cycle and reference validation"
```

---

## Phase 2 — Orchestrator core

### Task 5: Orchestrator state bridge

**Files:**
- Create: `orchestrator/src/orchestrator/state.py`
- Test: `tests/test_orchestrator_state.py`

**Interfaces:**
- Produces (module `orchestrator.state`):
  - `REPO_ROOT: Path`
  - `build_argv(spec: dict) -> tuple[list[str], str]`  (re-exported from gui `commands`)
  - `read_pipelines() -> dict[str, dict]`  (keyed by pipeline id)
  - `read_flows() -> list[dict]`
  - `secrets_path() -> Path`

- [ ] **Step 1: Write `tests/test_orchestrator_state.py`:**

```python
import json


def test_state_reads_json_and_bridges_build_argv(state_dir, monkeypatch):
    import config
    # state.py reads via the gui config module attributes
    (state_dir / "pipelines.json").write_text(json.dumps(
        [{"id": "p1", "name": "x",
          "spec": {"script": "oracle_to_iceberg", "mode": "INCREMENTAL"}}]))
    (state_dir / "flows.json").write_text(json.dumps([{"id": "f1", "nodes": []}]))

    from orchestrator import state
    monkeypatch.setattr(state, "_gui_config", config)  # use the patched paths

    pipes = state.read_pipelines()
    assert pipes["p1"]["name"] == "x"
    assert state.read_flows()[0]["id"] == "f1"
    argv, _ = state.build_argv({"script": "oracle_to_iceberg", "mode": "INCREMENTAL"})
    assert "oracle_to_iceberg.py" in " ".join(argv)
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_state.py -v`
Expected: FAIL (`ModuleNotFoundError: orchestrator.state`).

- [ ] **Step 3: Write `orchestrator/src/orchestrator/state.py`:**

```python
"""Bridge between the Dagster code location and the GUI's command/config layer.

Resolves the repo root robustly, puts ``gui/`` on ``sys.path``, and re-exports
``build_argv`` plus JSON readers so assets run *exactly* the argv the Run page
would. This is the single import seam; assets/email/build import only from here.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def _find_repo_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "oracle_to_iceberg.py").exists():
            return p
    raise RuntimeError("orchestrator: could not locate repo root (oracle_to_iceberg.py)")


REPO_ROOT = _find_repo_root(Path(__file__).resolve())

_gui_dir = REPO_ROOT / "gui"
if str(_gui_dir) not in sys.path:
    sys.path.insert(0, str(_gui_dir))

import commands as _commands  # noqa: E402  (after sys.path insert)
import config as _gui_config  # noqa: E402

build_argv = _commands.build_argv


def read_pipelines() -> dict[str, dict[str, Any]]:
    p = _gui_config.PIPELINES_JSON
    if not p.exists():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    return {x["id"]: x for x in data}


def read_flows() -> list[dict[str, Any]]:
    p = _gui_config.FLOWS_JSON
    if not p.exists():
        return []
    return json.loads(p.read_text(encoding="utf-8"))


def secrets_path() -> Path:
    return _gui_config.SECRETS_TOML
```

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_state.py -v`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add orchestrator/src/orchestrator/state.py tests/test_orchestrator_state.py
git commit -m "feat(orchestrator): state bridge to gui commands/config"
```

---

### Task 6: Asset factory

**Files:**
- Create: `orchestrator/src/orchestrator/assets.py`
- Test: `tests/test_orchestrator_assets.py`

**Interfaces:**
- Consumes: `orchestrator.state.build_argv`, `orchestrator.state.REPO_ROOT`.
- Produces:
  - `asset_key(flow_id: str, node_id: str) -> dg.AssetKey`  → `AssetKey(["flow_<flow_id>", node_id])`
  - `build_asset(flow_id: str, node_id: str, name: str, spec: dict, dep_keys: list[dg.AssetKey]) -> dg.AssetsDefinition`
  - The asset subprocesses `build_argv(spec)` from `REPO_ROOT`, streams output to `context.log`, raises `dg.Failure` on non-zero exit, returns `dg.MaterializeResult` with `exit_code`, `duration_s`, `command` metadata.

- [ ] **Step 1: Write `tests/test_orchestrator_assets.py`:**

```python
import sys

import dagster as dg


def test_asset_key_shape():
    from orchestrator import assets
    assert assets.asset_key("f1", "n1") == dg.AssetKey(["flow_f1", "n1"])


def test_asset_runs_command_and_succeeds(monkeypatch):
    from orchestrator import assets, state
    # A trivial spec whose build_argv yields a fast, zero-exit command.
    monkeypatch.setattr(state, "build_argv",
                        lambda spec: ([sys.executable, "-c", "print('hi')"], "noop"))
    a = assets.build_asset("f1", "n1", "noop", {"script": "x"}, [])
    result = dg.materialize([a])
    assert result.success


def test_asset_raises_on_nonzero(monkeypatch):
    from orchestrator import assets, state
    monkeypatch.setattr(state, "build_argv",
                        lambda spec: ([sys.executable, "-c", "import sys; sys.exit(3)"], "boom"))
    a = assets.build_asset("f1", "n2", "boom", {"script": "x"}, [])
    result = dg.materialize([a], raise_on_error=False)
    assert not result.success
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_assets.py -v`
Expected: FAIL (`ModuleNotFoundError: orchestrator.assets`).

- [ ] **Step 3: Write `orchestrator/src/orchestrator/assets.py`:**

```python
"""Asset factory: one subprocess-running asset per flow node.

The asset runs exactly the argv the Run page would (via state.build_argv),
streams child output into the Dagster run log, and fails the asset on a
non-zero exit. Dependencies are declared with ``deps=`` so Dagster runs nodes
in topological order and only starts a downstream node after upstreams succeed.
"""
from __future__ import annotations

import subprocess
import time
from typing import Any

import dagster as dg

from orchestrator import state


def asset_key(flow_id: str, node_id: str) -> dg.AssetKey:
    return dg.AssetKey([f"flow_{flow_id}", node_id])


def build_asset(flow_id: str, node_id: str, name: str, spec: dict[str, Any],
                dep_keys: list[dg.AssetKey]) -> dg.AssetsDefinition:
    key = asset_key(flow_id, node_id)

    @dg.asset(key=key, deps=dep_keys, group_name=f"flow_{flow_id}",
              description=name, compute_kind="subprocess")
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
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
            raise dg.Failure(description=f"{label}: command exited with code {rc}")
        return dg.MaterializeResult(metadata={
            "exit_code": rc,
            "duration_s": duration,
            "command": dg.MetadataValue.text(" ".join(argv)),
        })

    return _asset
```

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_assets.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit.**

```bash
git add orchestrator/src/orchestrator/assets.py tests/test_orchestrator_assets.py
git commit -m "feat(orchestrator): subprocess asset factory"
```

---

### Task 7: Email module (render + send + sensor factories)

**Files:**
- Create: `orchestrator/src/orchestrator/email.py`
- Test: `tests/test_orchestrator_email.py`

**Interfaces:**
- Consumes: `orchestrator.state.secrets_path`.
- Produces:
  - `load_smtp() -> dict | None`  (reads `[smtp]` from secrets.toml; `None` if absent/incomplete)
  - `run_url(run_id: str) -> str`
  - `render_body(flow_name, status, run_id, started, ended, url, error=None) -> str`
  - `send_email(smtp: dict, recipients: list[str], subject: str, body: str) -> None`
  - `build_email_sensors(flow_id, flow_name, job, success_to, failure_to) -> list`

- [ ] **Step 1: Write `tests/test_orchestrator_email.py`:**

```python
import smtpd  # noqa: F401  (ensure stdlib present on this Python)


def test_load_smtp_reads_section(tmp_path, monkeypatch):
    from orchestrator import email, state
    secrets = tmp_path / "secrets.toml"
    secrets.write_text(
        '[smtp]\nhost="mail.x"\nport=587\nusername="u"\npassword="p"\n'
        'from="oasis@x"\nuse_tls=true\n')
    monkeypatch.setattr(state, "secrets_path", lambda: secrets)
    smtp = email.load_smtp()
    assert smtp["host"] == "mail.x" and smtp["port"] == 587 and smtp["use_tls"] is True


def test_load_smtp_none_when_incomplete(tmp_path, monkeypatch):
    from orchestrator import email, state
    secrets = tmp_path / "secrets.toml"
    secrets.write_text('[smtp]\nhost="mail.x"\n')  # missing from/port
    monkeypatch.setattr(state, "secrets_path", lambda: secrets)
    assert email.load_smtp() is None


def test_render_body_contains_link_and_status():
    from orchestrator import email
    body = email.render_body("nightly", "SUCCEEDED", "abc123", "t0", "t1",
                             "http://h:3000/runs/abc123")
    assert "nightly" in body and "SUCCEEDED" in body and "runs/abc123" in body


def test_send_email_uses_smtp(monkeypatch):
    from orchestrator import email
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout=0): sent["addr"] = (host, port)
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def starttls(self): sent["tls"] = True
        def login(self, u, p): sent["login"] = (u, p)
        def send_message(self, msg): sent["to"] = msg["To"]

    monkeypatch.setattr(email.smtplib, "SMTP", FakeSMTP)
    smtp = {"host": "h", "port": 587, "username": "u", "password": "p",
            "from": "f@x", "use_tls": True}
    email.send_email(smtp, ["a@x", "b@x"], "subj", "body")
    assert sent["addr"] == ("h", 587) and sent["tls"] is True
    assert sent["login"] == ("u", "p") and "a@x" in sent["to"]
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_email.py -v`
Expected: FAIL (`ModuleNotFoundError: orchestrator.email`).

- [ ] **Step 3: Write `orchestrator/src/orchestrator/email.py`:**

```python
"""SMTP notification: load config, render bodies, send, and build the
success/failure run-status sensors that fire one email per flow run.
"""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Any

import dagster as dg

from orchestrator import state

try:
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore

_REQUIRED = ("host", "port", "from")


def load_smtp() -> dict[str, Any] | None:
    p = state.secrets_path()
    if not p.exists():
        return None
    with p.open("rb") as fh:
        smtp = _toml.load(fh).get("smtp", {})
    if not all(smtp.get(k) for k in _REQUIRED):
        return None
    return {
        "host": smtp["host"], "port": int(smtp["port"]),
        "username": smtp.get("username"), "password": smtp.get("password"),
        "from": smtp["from"], "use_tls": bool(smtp.get("use_tls", True)),
    }


def run_url(run_id: str) -> str:
    host = os.environ.get("OASIS_DAGSTER_HOST", "127.0.0.1")
    port = os.environ.get("OASIS_DAGSTER_PORT", "3000")
    return f"http://{host}:{port}/runs/{run_id}"


def render_body(flow_name: str, status: str, run_id: str, started: str,
                ended: str, url: str, error: str | None = None) -> str:
    lines = [
        f"Flow:    {flow_name}",
        f"Status:  {status}",
        f"Run id:  {run_id}",
        f"Started: {started}",
        f"Ended:   {ended}",
        f"Dagster: {url}",
    ]
    if error:
        lines += ["", "Error:", error]
    return "\n".join(lines)


def send_email(smtp: dict[str, Any], recipients: list[str], subject: str,
               body: str) -> None:
    if not recipients:
        return
    msg = EmailMessage()
    msg["From"] = smtp["from"]
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)
    with smtplib.SMTP(smtp["host"], smtp["port"], timeout=30) as server:
        if smtp["use_tls"]:
            server.starttls()
        if smtp.get("username"):
            server.login(smtp["username"], smtp.get("password") or "")
        server.send_message(msg)


def _send_for_run(context, status: str, recipients: list[str], flow_name: str,
                  error: str | None = None) -> None:
    smtp = load_smtp()
    if not smtp or not recipients:
        if not smtp:
            context.log.warning("SMTP not configured; skipping %s email", status)
        return
    run = context.dagster_run
    url = run_url(run.run_id)
    subject = f"[OASIS] Flow {flow_name} {status} — run {run.run_id[:8]}"
    body = render_body(flow_name, status, run.run_id,
                       str(getattr(run, "start_time", "")),
                       str(getattr(run, "end_time", "")), url, error)
    send_email(smtp, recipients, subject, body)
    context.log.info("Sent %s email to %s", status, recipients)


def build_email_sensors(flow_id: str, flow_name: str, job: Any,
                        success_to: list[str], failure_to: list[str]) -> list:
    sensors: list[Any] = []

    if success_to:
        @dg.run_status_sensor(
            name=f"flow_{flow_id}_success_email",
            run_status=dg.DagsterRunStatus.SUCCESS,
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _success(context: dg.RunStatusSensorContext) -> None:
            _send_for_run(context, "SUCCEEDED", success_to, flow_name)

        sensors.append(_success)

    if failure_to:
        @dg.run_failure_sensor(
            name=f"flow_{flow_id}_failure_email",
            monitored_jobs=[job],
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _failure(context: dg.RunFailureSensorContext) -> None:
            _send_for_run(context, "FAILED", failure_to, flow_name,
                          error=context.failure_event.message)

        sensors.append(_failure)

    return sensors
```

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_email.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit.**

```bash
git add orchestrator/src/orchestrator/email.py tests/test_orchestrator_email.py
git commit -m "feat(orchestrator): SMTP email + run-status sensors"
```

---

### Task 8: `build_all_defs` + definitions entry point

**Files:**
- Create: `orchestrator/src/orchestrator/build.py`
- Modify: `orchestrator/src/orchestrator/definitions.py`
- Test: `tests/test_orchestrator_build.py`

**Interfaces:**
- Consumes: `state.read_pipelines`, `state.read_flows`, `assets.build_asset`, `assets.asset_key`, `email.build_email_sensors`.
- Produces: `build_all_defs() -> dg.Definitions`. Per valid flow: assets (one per node, group `flow_<id>`), one job `flow_<id>`, one `ScheduleDefinition` `flow_<id>_schedule` (cron+tz, RUNNING iff enabled), success/failure email sensors. Invalid flows are skipped (logged), never crash the location.

- [ ] **Step 1: Write `tests/test_orchestrator_build.py`:**

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


def test_build_all_defs_creates_assets_job_schedule(state_dir, monkeypatch):
    import config
    from orchestrator import build, state
    monkeypatch.setattr(state._gui_config, "PIPELINES_JSON", config.PIPELINES_JSON)
    monkeypatch.setattr(state._gui_config, "FLOWS_JSON", config.FLOWS_JSON)
    _seed(state_dir)

    defs = build.build_all_defs()
    keys = {a.key for a in defs.get_all_asset_specs()}
    assert dg.AssetKey(["flow_f1", "n1"]) in keys
    assert dg.AssetKey(["flow_f1", "n2"]) in keys
    # n2 depends on n1
    spec_n2 = next(a for a in defs.get_all_asset_specs()
                   if a.key == dg.AssetKey(["flow_f1", "n2"]))
    assert dg.AssetKey(["flow_f1", "n1"]) in {d.asset_key for d in spec_n2.deps}
    assert defs.get_schedule_def("flow_f1_schedule").cron_schedule == "0 2 * * *"
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_build.py -v`
Expected: FAIL (`ModuleNotFoundError: orchestrator.build`).

- [ ] **Step 3: Write `orchestrator/src/orchestrator/build.py`:**

```python
"""Turn pipelines.json + flows.json into a single dg.Definitions.

Each flow → assets (one per node) + an asset job + a schedule + email sensors.
A flow that fails validation is skipped (logged) so one bad flow never breaks
the whole code location.
"""
from __future__ import annotations

import logging

import dagster as dg

from orchestrator import assets as asset_mod
from orchestrator import email as email_mod
from orchestrator import state

_log = logging.getLogger("orchestrator.build")


def _build_flow(flow: dict, pipelines: dict[str, dict]):
    flow_id = flow["id"]
    node_ids = {n["node_id"] for n in flow["nodes"]}
    flow_assets = []
    for node in flow["nodes"]:
        pid = node["pipeline_id"]
        if pid not in pipelines:
            raise ValueError(f"flow {flow_id}: unknown pipeline {pid}")
        for d in node.get("deps", []):
            if d not in node_ids:
                raise ValueError(f"flow {flow_id}: unknown dep {d}")
        dep_keys = [asset_mod.asset_key(flow_id, d) for d in node.get("deps", [])]
        flow_assets.append(asset_mod.build_asset(
            flow_id, node["node_id"], pipelines[pid].get("name", node["node_id"]),
            pipelines[pid]["spec"], dep_keys))

    job = dg.define_asset_job(
        f"flow_{flow_id}", selection=dg.AssetSelection.groups(f"flow_{flow_id}"))

    enabled = flow.get("enabled", True)
    schedule = dg.ScheduleDefinition(
        name=f"flow_{flow_id}_schedule",
        job=job,
        cron_schedule=flow["cron"],
        execution_timezone=flow.get("timezone", "UTC"),
        default_status=(dg.DefaultScheduleStatus.RUNNING if enabled
                        else dg.DefaultScheduleStatus.STOPPED),
    )

    email = flow.get("email", {})
    sensors = email_mod.build_email_sensors(
        flow_id, flow["name"], job,
        email.get("on_success", []), email.get("on_failure", []))

    return flow_assets, job, schedule, sensors


def build_all_defs() -> dg.Definitions:
    pipelines = state.read_pipelines()
    flows = state.read_flows()
    all_assets, all_jobs, all_schedules, all_sensors = [], [], [], []
    for flow in flows:
        try:
            a, j, s, sens = _build_flow(flow, pipelines)
        except Exception as exc:  # noqa: BLE001 - skip a bad flow, keep the rest
            _log.warning("Skipping flow %s: %s", flow.get("id"), exc)
            continue
        all_assets += a
        all_jobs.append(j)
        all_schedules.append(s)
        all_sensors += sens
    return dg.Definitions(
        assets=all_assets, jobs=all_jobs,
        schedules=all_schedules, sensors=all_sensors)
```

- [ ] **Step 4: Point `definitions.py` at the builder.** Replace `orchestrator/src/orchestrator/definitions.py` contents with:

```python
"""Dagster code location entry point (launch target for ``dagster dev -m``)."""
from __future__ import annotations

from orchestrator.build import build_all_defs

defs = build_all_defs()
```

- [ ] **Step 5: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_orchestrator_build.py -v`
Expected: PASS.

- [ ] **Step 6: Verify the location loads under Dagster.** Run:
`.venv/Scripts/python.exe -c "import orchestrator.definitions as d; print(len(list(d.defs.get_all_asset_specs())), 'assets')"`
Expected: prints an asset count (0 if no flows saved yet — that's fine).

- [ ] **Step 7: Commit.**

```bash
git add orchestrator/src/orchestrator/build.py orchestrator/src/orchestrator/definitions.py tests/test_orchestrator_build.py
git commit -m "feat(orchestrator): build_all_defs from pipelines + flows JSON"
```

---

## Phase 3 — GUI bridge (supervisor + client + SMTP)

### Task 9: SMTP config store + test email

**Files:**
- Create: `gui/smtp_config.py`
- Test: `tests/test_smtp_config.py`

**Interfaces:**
- Consumes: `config.SECRETS_TOML`, `config.STATE_DIR`.
- Produces:
  - `get_smtp() -> dict`  (password redacted: `{host, port, username, from, use_tls, has_password}`)
  - `save_smtp(payload: dict) -> dict`  (surgical `[smtp]` block edit, backup + re-parse validate; keeps existing password if blank)
  - `send_test(to: str) -> dict`  (`{ok, message|error}`)

- [ ] **Step 1: Write `tests/test_smtp_config.py`:**

```python
def test_save_and_get_smtp(tmp_path, monkeypatch):
    import config
    import smtp_config as sc
    secrets = tmp_path / "secrets.toml"
    secrets.write_text('[oracle_branches.jazan]\nhost="db"\nport=1521\n'
                       'username="u"\ndatabase="X"\npassword="p"\n')
    monkeypatch.setattr(config, "SECRETS_TOML", secrets)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)

    sc.save_smtp({"host": "mail.x", "port": 587, "username": "user",
                  "from": "oasis@x", "use_tls": True, "password": "secret"})
    got = sc.get_smtp()
    assert got["host"] == "mail.x" and got["has_password"] is True
    assert "password" not in got
    # the oracle branch must be byte-preserved
    assert "[oracle_branches.jazan]" in secrets.read_text()


def test_save_smtp_keeps_password_when_blank(tmp_path, monkeypatch):
    import config
    import smtp_config as sc
    secrets = tmp_path / "secrets.toml"
    secrets.write_text('[smtp]\nhost="h"\nport=25\nfrom="f@x"\npassword="keep"\n')
    monkeypatch.setattr(config, "SECRETS_TOML", secrets)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    sc.save_smtp({"host": "h2", "port": 26, "from": "f@x", "use_tls": False})
    assert 'password = "keep"' in secrets.read_text()
    assert 'host = "h2"' in secrets.read_text()
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_smtp_config.py -v`
Expected: FAIL (`ModuleNotFoundError: smtp_config`).

- [ ] **Step 3: Write `gui/smtp_config.py`** (mirrors the block-edit/backup/validate pattern from `connections.py`, for the single `[smtp]` section):

```python
"""Read/write the global ``[smtp]`` block in ``.dlt/secrets.toml``.

Same surgical, comment-preserving strategy as connections.py: splice only the
``[smtp]`` section, back up + re-parse-validate before committing. The password
is only overwritten when a new one is supplied.
"""
from __future__ import annotations

import re
import shutil
import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Any

import config

try:
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore

_SECTION_RE = re.compile(r"^\s*\[([^\]]+)\]\s*$")
_FIELDS = ["host", "port", "username", "password", "from", "use_tls"]
_NUM = {"port"}
_BOOL = {"use_tls"}


def _read() -> dict[str, Any]:
    if not config.SECRETS_TOML.exists():
        return {}
    with config.SECRETS_TOML.open("rb") as fh:
        return dict(_toml.load(fh).get("smtp", {}))


def get_smtp() -> dict[str, Any]:
    s = _read()
    return {
        "host": s.get("host", ""), "port": s.get("port", 587),
        "username": s.get("username", ""), "from": s.get("from", ""),
        "use_tls": bool(s.get("use_tls", True)),
        "has_password": bool(s.get("password")),
    }


def _fmt(field: str, val: Any) -> str:
    if field in _NUM:
        return str(int(val))
    if field in _BOOL:
        return "true" if val else "false"
    s = str(val).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _emit(data: dict[str, Any]) -> list[str]:
    lines = ["[smtp]"]
    for f in _FIELDS:
        if data.get(f) not in (None, ""):
            lines.append(f"{f} = {_fmt(f, data[f])}")
    return lines


def _read_lines() -> list[str]:
    if not config.SECRETS_TOML.exists():
        return []
    return config.SECRETS_TOML.read_text(encoding="utf-8").splitlines()


def _section_span(lines: list[str], name: str) -> tuple[int, int] | None:
    headers = [(i, m.group(1).strip())
               for i, line in enumerate(lines) if (m := _SECTION_RE.match(line))]
    for idx, (i, nm) in enumerate(headers):
        if nm == name:
            end = headers[idx + 1][0] if idx + 1 < len(headers) else len(lines)
            while end - 1 > i and lines[end - 1].strip() == "":
                end -= 1
            return i, end
    return None


def _write(lines: list[str]) -> None:
    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"
    if config.SECRETS_TOML.exists():
        config.STATE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(config.SECRETS_TOML, config.STATE_DIR / f"secrets.toml.{stamp}.bak")
    tmp = config.SECRETS_TOML.with_suffix(".toml.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        with tmp.open("rb") as fh:
            _toml.load(fh)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ValueError(f"refused to write corrupt secrets.toml: {exc}") from exc
    tmp.replace(config.SECRETS_TOML)


def save_smtp(payload: dict[str, Any]) -> dict[str, Any]:
    if not str(payload.get("host") or "").strip():
        raise ValueError("'host' is required")
    if not str(payload.get("from") or "").strip():
        raise ValueError("'from' address is required")
    existing = _read()
    data = {
        "host": payload.get("host"), "port": payload.get("port") or 587,
        "username": payload.get("username"), "from": payload.get("from"),
        "use_tls": bool(payload.get("use_tls", True)),
    }
    pw = str(payload.get("password") or "").strip()
    data["password"] = pw if pw else existing.get("password")

    lines = _read_lines()
    span = _section_span(lines, "smtp")
    block = _emit(data)
    if span is None:
        new = (lines + ["", *block]) if lines else block
    else:
        i, end = span
        new = lines[:i] + block + lines[end:]
    _write(new)
    return get_smtp()


def send_test(to: str) -> dict[str, Any]:
    s = _read()
    if not all(s.get(k) for k in ("host", "port", "from")):
        return {"ok": False, "error": "SMTP not fully configured (host, port, from)"}
    to = (to or s.get("from")).strip()
    try:
        msg = EmailMessage()
        msg["From"] = s["from"]
        msg["To"] = to
        msg["Subject"] = "[OASIS] SMTP test"
        msg.set_content("This is a test email from the HNH ETLPipeline Manager.")
        with smtplib.SMTP(s["host"], int(s["port"]), timeout=20) as srv:
            if s.get("use_tls", True):
                srv.starttls()
            if s.get("username"):
                srv.login(s["username"], s.get("password") or "")
            srv.send_message(msg)
        return {"ok": True, "message": f"Test email sent to {to}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
```

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_smtp_config.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit.**

```bash
git add gui/smtp_config.py tests/test_smtp_config.py
git commit -m "feat(gui): SMTP config store + test email"
```

---

### Task 10: Dagster supervisor service

**Files:**
- Create: `gui/dagster_service.py`
- Test: `tests/test_dagster_service.py`

**Interfaces:**
- Consumes: `config.DAGSTER_HOME`, `config.ORCHESTRATOR_DIR`, `config.dagster_host/port`, `config.python_executable`, `config.LOG_DIR`.
- Produces (class `DagsterService` + module singleton `service`):
  - `ensure_home() -> Path`  (creates `DAGSTER_HOME` + writes `dagster.yaml` with run-concurrency limit if missing)
  - `launch_argv() -> list[str]`  (`[python, -m, dagster, dev, -m, orchestrator.definitions, -h, host, -p, port]`)
  - `start() -> dict`  / `stop() -> dict` / `status() -> dict`  (`{running, pid, url}`)
  - `is_running() -> bool`

- [ ] **Step 1: Write `tests/test_dagster_service.py`:**

```python
def test_launch_argv_and_yaml(tmp_path, monkeypatch):
    import config
    import dagster_service as dsv
    monkeypatch.setattr(config, "DAGSTER_HOME", tmp_path / ".dagster_home")
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3001")

    svc = dsv.DagsterService()
    argv = svc.launch_argv()
    assert "-m" in argv and "dagster" in argv and "dev" in argv
    assert "orchestrator.definitions" in argv
    assert "3001" in argv

    home = svc.ensure_home()
    assert (home / "dagster.yaml").exists()
    assert "run_queue" in (home / "dagster.yaml").read_text()


def test_status_when_not_started(monkeypatch, tmp_path):
    import config
    import dagster_service as dsv
    monkeypatch.setattr(config, "DAGSTER_HOME", tmp_path / ".dagster_home")
    svc = dsv.DagsterService()
    st = svc.status()
    assert st["running"] is False and st["url"].startswith("http://")
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_dagster_service.py -v`
Expected: FAIL (`ModuleNotFoundError: dagster_service`).

- [ ] **Step 3: Write `gui/dagster_service.py`:**

```python
"""Supervise a local Dagster instance (webserver + daemon) for the GUI.

Launches one combined process — ``python -m dagster dev -m orchestrator.definitions``
— sharing an absolute DAGSTER_HOME with a generated dagster.yaml (run-queue
concurrency limit). Cross-platform process-group handling mirrors
pipeline_runner.py so the whole tree can be killed on Windows and POSIX.
"""
from __future__ import annotations

import os
import signal
import subprocess
import threading
from pathlib import Path
from typing import Any

import config

_DAGSTER_YAML = """\
run_queue:
  max_concurrent_runs: 1
telemetry:
  enabled: false
"""


class DagsterService:
    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._lock = threading.RLock()
        self._log_path = config.LOG_DIR / "dagster.log"

    # --- setup ------------------------------------------------------------ #
    def ensure_home(self) -> Path:
        home = config.DAGSTER_HOME
        home.mkdir(parents=True, exist_ok=True)
        yaml = home / "dagster.yaml"
        if not yaml.exists():
            yaml.write_text(_DAGSTER_YAML, encoding="utf-8")
        return home

    def launch_argv(self) -> list[str]:
        return [
            config.python_executable(), "-m", "dagster", "dev",
            "-m", "orchestrator.definitions",
            "-h", config.dagster_host(), "-p", str(config.dagster_port()),
        ]

    # --- lifecycle -------------------------------------------------------- #
    def is_running(self) -> bool:
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    def start(self) -> dict[str, Any]:
        with self._lock:
            if self.is_running():
                return self.status()
            self.ensure_home()
            config.LOG_DIR.mkdir(parents=True, exist_ok=True)
            env = dict(os.environ)
            env["DAGSTER_HOME"] = str(config.DAGSTER_HOME)
            env.setdefault("OASIS_DAGSTER_HOST", config.dagster_host())
            env.setdefault("OASIS_DAGSTER_PORT", str(config.dagster_port()))
            kwargs: dict[str, Any] = {}
            if os.name == "nt":
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True
            log_fh = open(self._log_path, "a", encoding="utf-8", buffering=1)
            self._proc = subprocess.Popen(
                self.launch_argv(), cwd=str(config.ORCHESTRATOR_DIR),
                stdout=log_fh, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, env=env, **kwargs,
            )
        return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                self._proc = None
                return {"running": False}
            try:
                if os.name == "nt":
                    proc.send_signal(signal.CTRL_BREAK_EVENT)
                    proc.terminate()
                else:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError):
                try:
                    proc.terminate()
                except OSError:
                    pass
            self._proc = None
        return {"running": False}

    def status(self) -> dict[str, Any]:
        running = self.is_running()
        return {
            "running": running,
            "pid": self._proc.pid if running and self._proc else None,
            "url": config.dagster_base_url(),
        }


service = DagsterService()
```

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_dagster_service.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Manual launch check.** Run: `.venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'gui'); import dagster_service as d; print(d.service.start()); import time; time.sleep(20); print(d.service.status())"`
Then open `http://127.0.0.1:3000` in a browser — the Dagster UI should load. Stop with the same module's `service.stop()` (or Ctrl+C / kill the process). Check `run_logs/dagster.log` for startup lines.
Expected: UI reachable; status shows `running: True`.

- [ ] **Step 6: Commit.**

```bash
git add gui/dagster_service.py tests/test_dagster_service.py
git commit -m "feat(gui): Dagster supervisor service"
```

---

### Task 11: Dagster GraphQL client

**Files:**
- Create: `gui/dagster_client.py`
- Test: `tests/test_dagster_client.py`

**Interfaces:**
- Consumes: `config.dagster_base_url`.
- Produces:
  - `graphql_url() -> str`  → `<base>/graphql`
  - `run_link(run_id) -> str`, `job_link(job_name) -> str`
  - `reload_location() -> dict`
  - `start_schedule(name) -> dict`, `stop_schedule(name) -> dict`
  - `launch_job(job_name) -> dict`
  - `flow_status() -> list[dict]`  (per job: `{job, schedule_state, last_run_status, last_run_id, last_run_at}`) — best-effort, returns `[]` if Dagster unreachable.
  - All network calls use stdlib `urllib.request` with a short timeout and never raise on connection error (return `{"ok": False, "error": ...}`).

- [ ] **Step 1: Write `tests/test_dagster_client.py`:**

```python
def test_link_builders(monkeypatch):
    import config
    import dagster_client as dc
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3000")
    monkeypatch.setenv("OASIS_DAGSTER_HOST", "127.0.0.1")
    assert dc.graphql_url() == "http://127.0.0.1:3000/graphql"
    assert dc.run_link("abc") == "http://127.0.0.1:3000/runs/abc"
    assert "flow_f1" in dc.job_link("flow_f1")


def test_flow_status_returns_empty_when_unreachable(monkeypatch):
    import dagster_client as dc
    # Point at a closed port so the request fails fast.
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "59999")
    assert dc.flow_status() == []
```

- [ ] **Step 2: Run to verify it fails.** Run: `.venv/Scripts/python.exe -m pytest tests/test_dagster_client.py -v`
Expected: FAIL (`ModuleNotFoundError: dagster_client`).

- [ ] **Step 3: Write `gui/dagster_client.py`:**

```python
"""Thin Dagster GraphQL client for status, reload, and schedule control.

Uses only stdlib urllib so it adds no dependency. Every call is best-effort:
on a connection error it returns an error dict (or [] for status) rather than
raising, so the GUI stays responsive when Dagster is down.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

import config

_TIMEOUT = 6


def graphql_url() -> str:
    return f"{config.dagster_base_url()}/graphql"


def run_link(run_id: str) -> str:
    return f"{config.dagster_base_url()}/runs/{run_id}"


def job_link(job_name: str) -> str:
    return f"{config.dagster_base_url()}/jobs/{job_name}"


def _query(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        graphql_url(), data=payload,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        return {"errors": [{"message": str(exc)}]}


def reload_location() -> dict[str, Any]:
    # Reload all repository locations (single workspace here).
    q = """
    mutation Reload {
      reloadWorkspace {
        __typename
        ... on WorkspaceLocationStatusEntries { entries { name } }
      }
    }"""
    res = _query(q)
    ok = "errors" not in res
    return {"ok": ok, "error": None if ok else res["errors"][0]["message"]}


def _set_schedule(mutation: str, schedule_name: str) -> dict[str, Any]:
    q = f"""
    mutation Toggle($name: String!) {{
      {mutation}(scheduleSelector: {{
        repositoryLocationName: "orchestrator",
        repositoryName: "__repository__",
        scheduleName: $name
      }}) {{ __typename }}
    }}"""
    res = _query(q, {"name": schedule_name})
    ok = "errors" not in res
    return {"ok": ok, "error": None if ok else res["errors"][0]["message"]}


def start_schedule(name: str) -> dict[str, Any]:
    return _set_schedule("startSchedule", name)


def stop_schedule(name: str) -> dict[str, Any]:
    return _set_schedule("stopSchedule", name)


def launch_job(job_name: str) -> dict[str, Any]:
    q = """
    mutation Launch($job: String!) {
      launchRun(executionParams: {
        selector: {
          repositoryLocationName: "orchestrator",
          repositoryName: "__repository__",
          jobName: $job
        }, mode: "default"
      }) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
      }
    }"""
    res = _query(q, {"job": job_name})
    if "errors" in res:
        return {"ok": False, "error": res["errors"][0]["message"]}
    node = res.get("data", {}).get("launchRun", {})
    if node.get("__typename") == "LaunchRunSuccess":
        return {"ok": True, "run_id": node["run"]["runId"]}
    return {"ok": False, "error": node.get("message", "launch failed")}


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
    out: list[dict[str, Any]] = []
    for repo in nodes:
        sched_by_job = {s["name"]: s for s in repo.get("schedules", [])}
        for job in repo.get("jobs", []):
            runs = job.get("runs") or []
            last = runs[0] if runs else {}
            sched = sched_by_job.get(f"{job['name']}_schedule", {})
            out.append({
                "job": job["name"],
                "schedule_state": (sched.get("scheduleState") or {}).get("status"),
                "last_run_status": last.get("status"),
                "last_run_id": last.get("runId"),
                "last_run_at": last.get("startTime"),
                "link": job_link(job["name"]),
                "run_link": run_link(last["runId"]) if last.get("runId") else None,
            })
    return out
```

> **Note for the implementer:** the exact `repositoryName` (`__repository__`) and a couple of mutation field names can vary slightly by Dagster version. After Task 10's manual launch, open `http://127.0.0.1:3000/graphql` (GraphiQL) and confirm `reloadWorkspace`, `startSchedule`, `stopSchedule`, `launchRun`, and the repository/schedule selectors against the running schema. Adjust the query strings here to match; keep the function signatures unchanged.

- [ ] **Step 4: Run to verify it passes.** Run: `.venv/Scripts/python.exe -m pytest tests/test_dagster_client.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Live integration check (Dagster running from Task 10).** With the service started, run:
`.venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'gui'); import dagster_client as dc; print(dc.reload_location()); print(dc.flow_status())"`
Expected: `reload_location` returns `{'ok': True, ...}`; `flow_status` returns a list (possibly empty). Fix selector/field names per the GraphiQL schema if errors appear.

- [ ] **Step 6: Commit.**

```bash
git add gui/dagster_client.py tests/test_dagster_client.py
git commit -m "feat(gui): Dagster GraphQL client (status, reload, control)"
```

---

## Phase 4 — GUI surface (routes + pages + nav)

### Task 12: Flask routes & API

**Files:**
- Modify: `gui/app.py`

**Interfaces:**
- Consumes: `pipelines_store`, `flows_store`, `smtp_config`, `dagster_service.service`, `dagster_client`.
- Produces routes:
  - Pages: `GET /pipelines`, `GET /flows`
  - Pipelines API: `GET/POST /api/pipelines`, `PUT/DELETE /api/pipelines/<pid>` (DELETE blocks if referenced)
  - Flows API: `GET/POST /api/flows`, `PUT/DELETE /api/flows/<fid>`, `POST /api/flows/<fid>/run`, `POST /api/flows/<fid>/toggle`
  - SMTP API: `GET/PUT /api/smtp`, `POST /api/smtp/test`
  - Dagster API: `GET /api/dagster/status`, `POST /api/dagster/start`, `POST /api/dagster/stop`, `GET /api/dagster/flow-status`
  - On any flow/pipeline mutation: call `dagster_client.reload_location()` (best-effort).

- [ ] **Step 1: Add imports** in `gui/app.py` (with the other flat imports near the top):

```python
import dagster_client  # noqa: E402
import flows_store  # noqa: E402
import pipelines_store  # noqa: E402
import smtp_config  # noqa: E402
from dagster_service import service as dagster_service  # noqa: E402
```

- [ ] **Step 2: Add the two pages** after the existing `page_*` routes:

```python
@app.route("/pipelines")
def page_pipelines():
    return render_template("pipelines.html", active="pipelines")


@app.route("/flows")
def page_flows():
    return render_template("flows.html", active="flows")
```

- [ ] **Step 3: Add the Pipelines API** (after the Run API block):

```python
@app.get("/api/pipelines")
@api
def api_pipelines_list():
    return jsonify(pipelines_store.load_pipelines())


@app.post("/api/pipelines")
@api
def api_pipelines_add():
    b = _body()
    p = pipelines_store.add_pipeline(b.get("name", ""), b.get("spec", {}))
    dagster_client.reload_location()
    return jsonify(p)


@app.put("/api/pipelines/<pid>")
@api
def api_pipelines_update(pid):
    p = pipelines_store.update_pipeline(pid, **_body())
    dagster_client.reload_location()
    return jsonify(p)


@app.delete("/api/pipelines/<pid>")
@api
def api_pipelines_delete(pid):
    refs = flows_store.referencing_flows(pid)
    if refs:
        names = ", ".join(f["name"] for f in refs)
        raise ValueError(f"Pipeline is used by flow(s): {names}")
    deleted = pipelines_store.delete_pipeline(pid)
    dagster_client.reload_location()
    return jsonify({"deleted": deleted})
```

- [ ] **Step 4: Add the Flows API:**

```python
@app.get("/api/flows")
@api
def api_flows_list():
    return jsonify({
        "flows": flows_store.load_flows(),
        "pipelines": pipelines_store.load_pipelines(),
        "dagster": dagster_service.status(),
    })


@app.post("/api/flows")
@api
def api_flows_add():
    b = _body()
    f = flows_store.add_flow(b.get("name", ""), b.get("nodes", []),
                             b.get("cron", ""), b.get("timezone", "UTC"),
                             b.get("email", {}), b.get("enabled", True))
    dagster_client.reload_location()
    return jsonify(f)


@app.put("/api/flows/<fid>")
@api
def api_flows_update(fid):
    f = flows_store.update_flow(fid, **_body())
    dagster_client.reload_location()
    return jsonify(f)


@app.delete("/api/flows/<fid>")
@api
def api_flows_delete(fid):
    deleted = flows_store.delete_flow(fid)
    dagster_client.reload_location()
    return jsonify({"deleted": deleted})


@app.post("/api/flows/<fid>/run")
@api
def api_flows_run(fid):
    return jsonify(dagster_client.launch_job(f"flow_{fid}"))


@app.post("/api/flows/<fid>/toggle")
@api
def api_flows_toggle(fid):
    enabled = bool(_body().get("enabled", True))
    flows_store.update_flow(fid, enabled=enabled)
    dagster_client.reload_location()
    fn = f"flow_{fid}_schedule"
    res = dagster_client.start_schedule(fn) if enabled else dagster_client.stop_schedule(fn)
    return jsonify({"enabled": enabled, **res})
```

- [ ] **Step 5: Add the SMTP + Dagster API:**

```python
@app.get("/api/smtp")
@api
def api_smtp_get():
    return jsonify(smtp_config.get_smtp())


@app.put("/api/smtp")
@api
def api_smtp_put():
    return jsonify(smtp_config.save_smtp(_body()))


@app.post("/api/smtp/test")
@api
def api_smtp_test():
    return jsonify(smtp_config.send_test(_body().get("to", "")))


@app.get("/api/dagster/status")
@api
def api_dagster_status():
    return jsonify(dagster_service.status())


@app.post("/api/dagster/start")
@api
def api_dagster_start():
    return jsonify(dagster_service.start())


@app.post("/api/dagster/stop")
@api
def api_dagster_stop():
    return jsonify(dagster_service.stop())


@app.get("/api/dagster/flow-status")
@api
def api_dagster_flow_status():
    return jsonify(dagster_client.flow_status())
```

- [ ] **Step 6: Auto-start Dagster on GUI boot (opt-out via env).** In `main()`, before `app.run(...)`, add:

```python
    if os.environ.get("OASIS_DAGSTER_AUTOSTART", "1") == "1":
        try:
            dagster_service.start()
            print(f"Dagster UI -> {config.dagster_base_url()}")
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] could not start Dagster: {exc}")
```

Add `import config` to the top imports if not already present (it imports `ensure_dirs` from config; change to `import config` and use `config.ensure_dirs()` or add a separate `import config`).

- [ ] **Step 7: Smoke-test the app imports and routes register.** Run:
`.venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'gui'); import app; print(sorted(r.rule for r in app.app.url_map.iter_rules() if 'api' in r.rule))"`
Expected: lists the new `/api/pipelines`, `/api/flows`, `/api/smtp`, `/api/dagster/*` routes; no import error.

- [ ] **Step 8: Commit.**

```bash
git add gui/app.py
git commit -m "feat(gui): routes & API for pipelines, flows, SMTP, Dagster"
```

---

### Task 13: Pipeline library page

**Files:**
- Create: `gui/templates/pipelines.html`

**Interfaces:**
- Consumes: `/api/pipelines`, `/api/command/preview`. Reuses `base.html` block structure and the existing `apiGet/apiPost/apiPut/apiDel`, `el/$/$$`, `ok/err`, `esc` helpers (see `schedule.html`/`static/app.js` for the available globals).

- [ ] **Step 1: Inspect available JS helpers.** Read `gui/static/app.js` and `gui/templates/run.html` to confirm the helper names (`apiGet`, `apiPost`, `el`, `ok`, `err`, `esc`, and how `run.html` builds a spec). Reuse the same spec-building fields so a saved pipeline matches a Run spec.

- [ ] **Step 2: Write `gui/templates/pipelines.html`** — a page that (a) lists saved pipelines in a table with command preview + delete, and (b) a form to create one. Minimal version:

```html
{% extends "base.html" %}
{% block content %}
<div class="page-head">
  <h1><i class="fa-solid fa-cubes"></i> Pipelines</h1>
  <p>Save named, parameterized pipelines. Each becomes one asset you can wire into a <a href="/flows">Flow</a>.</p>
</div>

<div class="panel">
  <div class="panel-head"><h2>New pipeline</h2></div>
  <div class="form-row">
    <div><label>Name</label><input id="p-name" placeholder="masters-patient-incr"></div>
    <div><label>Script</label>
      <select id="p-script">
        <option value="oracle_to_iceberg">oracle_to_iceberg</option>
        <option value="dq_check">dq_check</option>
        <option value="snapshot_diff">snapshot_diff</option>
        <option value="fresh_run">fresh_run</option>
      </select>
    </div>
    <div><label>Mode</label>
      <select id="p-mode"><option>INCREMENTAL</option><option>FULL</option></select>
    </div>
  </div>
  <div class="form-row">
    <div><label>Category</label><input id="p-category" placeholder="masters / transactions / both"></div>
    <div><label>Branch(es)</label><input id="p-branches" placeholder="jazan,riyadh"></div>
    <div><label>Tables</label><input id="p-tables" placeholder="PATIENT_MASTER_DATA"></div>
  </div>
  <label>Command preview</label>
  <div id="p-preview" class="codeline">—</div>
  <div class="btn-row"><button class="btn primary" id="p-save"><i class="fa-solid fa-floppy-disk"></i> Save pipeline</button></div>
</div>

<div class="panel">
  <div class="panel-head"><h2>Saved pipelines <span id="p-count" class="tag"></span></h2><button class="btn sm ghost" id="p-refresh">↻</button></div>
  <div class="table-wrap"><table id="p-table">
    <thead><tr><th>Name</th><th>Command</th><th>Created</th><th></th></tr></thead>
    <tbody></tbody>
  </table></div>
</div>
{% endblock %}

{% block scripts %}
<script>
function buildSpec() {
  const spec = { script: el("p-script").value };
  if (spec.script === "oracle_to_iceberg") {
    spec.mode = el("p-mode").value;
    if (el("p-category").value.trim()) spec.category = el("p-category").value.trim();
  }
  if (el("p-branches").value.trim()) spec.branches = el("p-branches").value.trim();
  if (el("p-tables").value.trim()) spec.tables = el("p-tables").value.trim();
  return spec;
}
async function preview() {
  try { const r = await apiPost("/api/command/preview", buildSpec()); el("p-preview").textContent = r.preview; }
  catch (e) { el("p-preview").textContent = "⚠ " + e.message; }
}
async function load() {
  const items = await apiGet("/api/pipelines");
  el("p-count").textContent = items.length;
  $("#p-table tbody").innerHTML = items.map(p => `
    <tr><td><b>${esc(p.name)}</b></td>
        <td class="mono" style="max-width:420px;overflow:hidden;text-overflow:ellipsis" title="${esc(p.command)}">${esc(p.command)}</td>
        <td class="mono">${esc(p.created_at)}</td>
        <td><button class="btn sm bad" onclick="del('${p.id}')">Delete</button></td></tr>`
  ).join("") || `<tr><td colspan="4" class="muted">No pipelines yet.</td></tr>`;
}
async function save() {
  const name = el("p-name").value.trim();
  if (!name) return err("Name is required");
  try { await apiPost("/api/pipelines", { name, spec: buildSpec() }); ok("Saved"); el("p-name").value=""; load(); }
  catch (e) { err(e.message); }
}
async function del(id) {
  if (!confirm("Delete this pipeline?")) return;
  try { await apiDel(`/api/pipelines/${id}`); ok("Deleted"); load(); } catch (e) { err(e.message); }
}
["p-script","p-mode","p-category","p-branches","p-tables"].forEach(id => el(id).addEventListener("input", preview));
el("p-save").onclick = save;
el("p-refresh").onclick = load;
preview(); load();
</script>
{% endblock %}
```

> If `app.js` helper names differ (e.g. `apiDel` vs `apiDelete`), adjust to the actual names found in Step 1.

- [ ] **Step 3: Manual check.** Start the GUI (`.venv/Scripts/python.exe gui/app.py`), open `/pipelines`, create a pipeline, confirm it appears in `gui/state/pipelines.json` and the table; delete it.
Expected: create/list/delete all work; command preview updates live.

- [ ] **Step 4: Commit.**

```bash
git add gui/templates/pipelines.html
git commit -m "feat(gui): pipeline library page"
```

---

### Task 14: Flow builder + list page

**Files:**
- Create: `gui/templates/flows.html`

**Interfaces:**
- Consumes: `/api/flows` (GET list+pipelines+dagster), POST/PUT/DELETE, `/api/flows/<id>/run`, `/api/flows/<id>/toggle`, `/api/dagster/flow-status`, `/api/dagster/start`.

- [ ] **Step 1: Write `gui/templates/flows.html`** with two tabs — **Build** and **All flows** — mirroring `schedule.html`'s tab pattern. The Build tab: pick nodes (each node = a pipeline dropdown + an upstream-multiselect of the other nodes), cron fields + timezone, success/failure email inputs, a read-only DAG preview (text/SVG), Save. The All-flows tab: table with live status from `/api/dagster/flow-status` + Open-in-Dagster link + Run-now + enable/disable + delete. Implementation:

```html
{% extends "base.html" %}
{% block content %}
<div class="page-head">
  <h1><i class="fa-solid fa-diagram-project"></i> Flows</h1>
  <p>Compose pipelines into a DAG, wire dependencies, and schedule it. Downstream steps run only after upstream steps succeed.</p>
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
      <div><label>Timezone</label><input id="f-tz" value="Asia/Riyadh"></div>
    </div>

    <label style="margin-top:12px">Nodes</label>
    <div id="f-nodes"></div>
    <button class="btn sm" id="f-add-node"><i class="fa-solid fa-plus"></i> Add node</button>

    <label style="margin-top:16px">Schedule (cron)</label>
    <div class="cron-grid">
      <div><label>Min</label><input id="cf-min" class="mono" value="0"></div>
      <div><label>Hour</label><input id="cf-hour" class="mono" value="2"></div>
      <div><label>DoM</label><input id="cf-dom" class="mono" value="*"></div>
      <div><label>Mon</label><input id="cf-mon" class="mono" value="*"></div>
      <div><label>DoW</label><input id="cf-dow" class="mono" value="*"></div>
    </div>

    <div class="form-row" style="margin-top:12px">
      <div><label>Email on success</label><input id="f-succ" placeholder="ops@x.com, lead@x.com"></div>
      <div><label>Email on failure</label><input id="f-fail" placeholder="ops@x.com"></div>
    </div>

    <label style="margin-top:12px">DAG preview</label>
    <div id="f-preview" class="console" style="max-height:220px">—</div>

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
<script>
let PIPELINES = [], FLOWS = [], editingId = null, nodeSeq = 0;

function cron() {
  return [el("cf-min"),el("cf-hour"),el("cf-dom"),el("cf-mon"),el("cf-dow")]
    .map(e => e.value.trim() || "*").join(" ");
}
function nodeRow(n) {
  const opts = PIPELINES.map(p => `<option value="${p.id}" ${n.pipeline_id===p.id?"selected":""}>${esc(p.name)}</option>`).join("");
  return `<div class="node-row row-flex" data-node="${n.node_id}" style="gap:8px;margin:6px 0">
    <span class="mono">${n.node_id}</span>
    <select class="n-pipe">${opts}</select>
    <span class="muted">depends on:</span>
    <select class="n-deps" multiple size="2"></select>
    <button class="btn sm bad" onclick="rmNode('${n.node_id}')">✕</button></div>`;
}
function depOptions() {
  const ids = [...$$("#f-nodes .node-row")].map(r => r.dataset.node);
  $$("#f-nodes .node-row").forEach(row => {
    const self = row.dataset.node, sel = row.querySelector(".n-deps");
    const chosen = [...sel.selectedOptions].map(o => o.value);
    sel.innerHTML = ids.filter(i => i !== self)
      .map(i => `<option value="${i}" ${chosen.includes(i)?"selected":""}>${i}</option>`).join("");
  });
}
function addNode(pre) {
  nodeSeq++; const id = pre?.node_id || ("n" + nodeSeq);
  el("f-nodes").insertAdjacentHTML("beforeend", nodeRow(pre || {node_id:id, pipeline_id:(PIPELINES[0]||{}).id, deps:[]}));
  depOptions(); renderPreview();
  el("f-nodes").lastElementChild.querySelectorAll("select").forEach(s => s.addEventListener("change", () => { depOptions(); renderPreview(); }));
}
function rmNode(id) { const r = [...$$("#f-nodes .node-row")].find(x => x.dataset.node===id); if (r) r.remove(); depOptions(); renderPreview(); }
function collectNodes() {
  return [...$$("#f-nodes .node-row")].map(r => ({
    node_id: r.dataset.node,
    pipeline_id: r.querySelector(".n-pipe").value,
    deps: [...r.querySelector(".n-deps").selectedOptions].map(o => o.value),
  }));
}
function renderPreview() {
  const nodes = collectNodes();
  const byId = Object.fromEntries(PIPELINES.map(p => [p.id, p.name]));
  el("f-preview").textContent = nodes.map(n =>
    `${n.node_id} (${byId[n.pipeline_id]||"?"})` + (n.deps.length ? `  ⟵ ${n.deps.join(", ")}` : "  [root]")
  ).join("\n") || "—";
}
async function save() {
  const body = { name: el("f-name").value.trim(), nodes: collectNodes(), cron: cron(),
    timezone: el("f-tz").value.trim() || "UTC",
    email: { on_success: el("f-succ").value, on_failure: el("f-fail").value } };
  try {
    if (editingId) await apiPut(`/api/flows/${editingId}`, body);
    else await apiPost("/api/flows", body);
    ok("Saved"); resetForm(); load(); showTab("list");
  } catch (e) { err(e.message); }
}
function resetForm() { editingId=null; el("f-name").value=""; el("f-nodes").innerHTML=""; el("f-reset").hidden=true; el("f-title").textContent="New flow"; renderPreview(); }

async function load() {
  const d = await apiGet("/api/flows");
  PIPELINES = d.pipelines; FLOWS = d.flows;
  el("flow-count").textContent = FLOWS.length;
  el("dagster-banner").innerHTML = d.dagster.running
    ? `<div class="banner info">Dagster running — <a href="${d.dagster.url}" target="_blank">open UI</a></div>`
    : `<div class="banner warn">Dagster is not running. <button class="btn sm" onclick="startDagster()">Start Dagster</button></div>`;
  renderFlows();
}
async function renderFlows() {
  let status = [];
  try { status = await apiGet("/api/dagster/flow-status"); } catch (e) {}
  const byJob = Object.fromEntries(status.map(s => [s.job, s]));
  $("#flows-table tbody").innerHTML = FLOWS.map(f => {
    const st = byJob[`flow_${f.id}`] || {};
    const link = st.run_link ? `<a href="${st.run_link}" target="_blank">${esc(st.last_run_status||"—")}</a>` : (st.last_run_status||"—");
    return `<tr>
      <td><b>${esc(f.name)}</b></td>
      <td class="mono">${esc(f.cron)} <span class="muted">${esc(f.timezone)}</span></td>
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
  const f = FLOWS.find(x => x.id===id); if (!f) return;
  editingId=id; el("f-title").textContent="Edit: "+f.name; el("f-reset").hidden=false;
  el("f-name").value=f.name; el("f-tz").value=f.timezone;
  const parts=f.cron.split(/\s+/); ["cf-min","cf-hour","cf-dom","cf-mon","cf-dow"].forEach((cid,i)=>el(cid).value=parts[i]||"*");
  el("f-succ").value=(f.email.on_success||[]).join(", "); el("f-fail").value=(f.email.on_failure||[]).join(", ");
  el("f-nodes").innerHTML=""; f.nodes.forEach(n => addNode(n)); depOptions(); renderPreview();
  showTab("build"); window.scrollTo({top:0,behavior:"smooth"});
}
async function runFlow(id){ try{ const r=await apiPost(`/api/flows/${id}/run`,{}); r.ok?ok("Launched run "+r.run_id.slice(0,8)):err(r.error); setTimeout(load,1500);}catch(e){err(e.message);} }
async function toggleFlow(id,en){ try{ await apiPost(`/api/flows/${id}/toggle`,{enabled:en==="true"||en===true}); load(); }catch(e){err(e.message);} }
async function delFlow(id){ if(!confirm("Delete this flow?"))return; try{ await apiDel(`/api/flows/${id}`); ok("Deleted"); load(); }catch(e){err(e.message);} }
async function startDagster(){ try{ await apiPost("/api/dagster/start",{}); ok("Starting Dagster…"); setTimeout(load,4000);}catch(e){err(e.message);} }

function showTab(t){ $$(".tab").forEach(b=>b.classList.toggle("primary",b.dataset.tab===t)); $$("[data-panel]").forEach(p=>p.hidden=p.dataset.panel!==t); if(t==="list") renderFlows(); }
$$(".tab").forEach(b=>b.onclick=()=>showTab(b.dataset.tab));
el("f-add-node").onclick=()=>addNode();
el("f-save").onclick=save; el("f-reset").onclick=resetForm; el("f-refresh").onclick=load;
load();
</script>
{% endblock %}
```

> Reuse whatever `pill(...)` helper `schedule.html` used (it referenced `pill`); if it's defined in `app.js`, it's already global. Otherwise inline a small status-to-class map.

- [ ] **Step 2: Manual check (Dagster running).** Open `/flows`, add 2 nodes (the 2 pipelines from Task 13), make node 2 depend on node 1, set cron + a success/failure email, Save. Confirm it lands in `gui/state/flows.json`, appears under All flows, and **Run now** launches a run whose status link opens the Dagster UI. In the Dagster asset graph, confirm node 2 is downstream of node 1.
Expected: flow saves, runs in dependency order, status + deep link work.

- [ ] **Step 3: Commit.**

```bash
git add gui/templates/flows.html
git commit -m "feat(gui): flow builder + list page"
```

---

### Task 15: Navigation, retire cron scheduler, SMTP settings UI, docs

**Files:**
- Modify: `gui/templates/base.html`
- Modify: `gui/app.py` (remove `/schedule` route + schedule API)
- Delete: `gui/cron_manager.py`, `gui/templates/schedule.html`
- Modify: `gui/README.md`, `README.md` (scheduling section)

**Interfaces:**
- Produces: nav links to `/pipelines` and `/flows`; an SMTP settings panel (add to `connections.html` or a small `/flows` settings area — choose connections.html to match the "global config" grouping).

- [ ] **Step 1: Update nav in `gui/templates/base.html`.** Find the nav block (the links for dashboard/run/schedule/logs/tables/iceberg/connections). Replace the **Schedule** link with two links — **Pipelines** (`/pipelines`, icon `fa-cubes`) and **Flows** (`/flows`, icon `fa-diagram-project`) — matching the existing markup pattern and `active` highlighting.

- [ ] **Step 2: Remove the schedule route and schedule API from `gui/app.py`.** Delete `page_schedule`, the `import cron_manager`, and the entire `# Schedule API` block (`api_sched_*` handlers). In `api_overview`, remove the `"cron": cron_manager.status()["available"]` line.

- [ ] **Step 3: Delete retired files.** Run:
`git rm gui/cron_manager.py gui/templates/schedule.html`

- [ ] **Step 4: Add the SMTP settings panel** to `gui/templates/connections.html` — a small form (host, port, username, password, from, use_tls) bound to `GET/PUT /api/smtp` + a "Send test" button hitting `POST /api/smtp/test`. Follow the page's existing form/JS helpers. (Password field shows a "leave blank to keep" hint; the API redacts it.)

- [ ] **Step 5: Update docs.** In `gui/README.md` and the scheduling section of `README.md`, replace the cron-scheduler description with: the Pipeline library → Flow (DAG) builder → Dagster scheduling model; how to configure SMTP; that the GUI auto-starts Dagster (`OASIS_DAGSTER_AUTOSTART=0` to opt out, `OASIS_DAGSTER_PORT` to change the port); and the one-time `pip install -e orchestrator` step.

- [ ] **Step 6: Full regression smoke.** Run: `.venv/Scripts/python.exe -m pytest -q` then start the GUI and click through Dashboard, Run, Logs, Tables, Iceberg, Connections, Pipelines, Flows.
Expected: all tests pass; no broken nav; the old `/schedule` URL 404s; SMTP test sends.

- [ ] **Step 7: Commit.**

```bash
git add gui/templates/base.html gui/app.py gui/templates/connections.html gui/README.md README.md
git commit -m "feat(gui): nav to Pipelines/Flows, retire cron scheduler, SMTP settings"
```

---

## Phase 5 — End-to-end verification

### Task 16: End-to-end acceptance (Windows + Linux)

**Files:** none (verification only)

- [ ] **Step 1: Clean install on Windows.** From a fresh checkout: `./setup.ps1`. Confirm Dagster + orchestrator install into `.venv`, the GUI starts, and the Dagster UI is reachable at `http://127.0.0.1:3000`.

- [ ] **Step 2: Configure SMTP** in Connections → Email; click **Send test**; confirm a test email arrives.

- [ ] **Step 3: Build the scenario.** Save two pipelines (e.g. an `oracle_to_iceberg` masters incremental and a `dq_check`). Create a flow wiring `dq_check` downstream of the extract, cron a couple minutes out, set success+failure recipients.

- [ ] **Step 4: Verify dependency-on-success + success email.** Trigger **Run now**. In the Dagster UI confirm the downstream asset starts only after the upstream succeeds; confirm a **success email** arrives with a working run link.

- [ ] **Step 5: Verify failure email.** Temporarily edit a pipeline so its command fails (e.g. an invalid `--tables`), Run now, and confirm a **failure email** arrives with the error summary, and the downstream node does not run.

- [ ] **Step 6: Verify scheduling.** Leave the flow enabled and confirm the schedule fires at the cron time (check Dagster runs list + the status on the Flows page). Toggle disable/enable and confirm the schedule state changes in Dagster.

- [ ] **Step 7: Linux pass.** On a Linux host, run `./setup.sh` and repeat Steps 1, 3, 4, 6 (at least one full DAG run on schedule with emails). Confirm process start/stop and scheduling work.

- [ ] **Step 8: Final commit (docs/notes if anything was adjusted).**

```bash
git add -A
git commit -m "test: end-to-end verification notes for Dagster scheduling"
```

---

## Self-Review

**1. Spec coverage:**
- Req 1 (build DAGs from pipelines as assets, deps, schedule on-success) → Tasks 3, 4, 6, 8, 14.
- Req 2 (success + failure emails per run) → Tasks 7, 9, 15; verified in 16.
- Req 3 (app builds Dagster definitions from user params) → Tasks 5, 8 (dynamic-from-JSON).
- Req 4 (list jobs with status + Dagster hyperlink) → Tasks 11, 14.
- Req 5 (Dagster in venv, Windows + Linux) → Tasks 1, 2, 10, 16.
- SMTP global + per-DAG recipients → Tasks 4 (recipients on flow), 7/9 (global `[smtp]`).
- Retire cron scheduler → Task 15. Supervision (GUI owns Dagster) → Task 10, app autostart 12.

**2. Placeholder scan:** No "TBD/handle errors/similar to". Two places intentionally defer to live-schema/scaffold inspection (Task 2 package path, Task 11 GraphQL field names) — each has an explicit verification step and unchanged signatures, not a placeholder.

**3. Type consistency:** `asset_key`/`build_asset` (Task 6) match their uses in `build.py` (Task 8). `build_email_sensors(flow_id, flow_name, job, success_to, failure_to)` defined in Task 7 matches the call in Task 8. Store function names (`add_pipeline`, `add_flow`, `referencing_flows`, `update_flow(..., enabled=)`) match their API uses in Task 12. `service` singleton + `status/start/stop` match Task 12. `dagster_client` functions (`reload_location`, `launch_job`, `start_schedule`, `stop_schedule`, `flow_status`, `run_link`, `job_link`) match Tasks 12/14 usage. Schedule name convention `flow_<id>_schedule` consistent across Tasks 8, 11, 12.
