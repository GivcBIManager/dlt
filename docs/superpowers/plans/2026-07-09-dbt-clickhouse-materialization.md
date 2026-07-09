# dbt-core Iceberg → ClickHouse Materialization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a dbt-core materialization layer that reads the local Iceberg lake via ClickHouse's `icebergLocal(...)` and writes native ClickHouse tables — managed from a new GUI page, runnable/testable in-app, and selectable as flow nodes — with all config app-owned.

**Architecture:** Route dbt through the app's single command seam (`gui/commands.py::build_argv`, re-exported by `orchestrator/state.py`) so run-now, saved pipelines, and Dagster all reuse existing machinery. App config (`.dlt/secrets.toml [clickhouse]` + `.dlt/config.toml [dbt]`) is the source of truth; the app generates `dbt/profiles.yml`. Flow nodes gain a `kind` ("pipeline" | "dbt").

**Tech Stack:** Python 3.10+, Flask, Dagster 1.13.11, dbt-core + dbt-clickhouse, PyYAML (transitive via dbt), pytest. ClickHouse is external (24.x+).

## Global Constraints

- **Python 3.10+**, must run on **Windows and Linux** (use `pathlib`, no shell-isms).
- **Secrets never leave the server**: any handler returning ClickHouse config must redact the password (`has_password: bool`), mirroring `gui/connections.py::_safe`.
- **Config edits are in-place & comment-preserving**: edit only allowlisted keys already present in the target TOML block; keep a timestamped backup; re-parse before commit (mirror `workspace.update_etl_settings` / `connections._write`).
- **Path-traversal safety**: every dbt file operation must resolve the target and assert it stays within the dbt project dir; only `.sql`/`.yml`/`.yaml` are allowed.
- **Backward compatibility**: existing `flows.json` nodes have no `kind` — absence MUST mean `kind == "pipeline"`.
- **No new front-end CDN**: the Models page editor is a plain styled `<textarea>` (the app already loads its own `app.js`; do not add Monaco/CodeMirror).
- **`icebergLocal(...)` paths are the operator's responsibility** — the app never validates that a path exists on the ClickHouse host; surface this in UI copy, the example model, and the README.
- **dbt commands**: the command layer exposes `run, test, build, compile, debug`; **only `run, test, build`** are valid as flow-node kinds.
- **Pin**: `dbt-core>=1.9,<1.10` and `dbt-clickhouse>=1.9,<1.10` (adjust the pair together if the resolver objects; they must share a minor).

---

### Task 1: Setup, dependencies & dbt project scaffold

**Files:**
- Modify: `requirements-gui.txt`
- Modify: `setup.sh`, `setup.ps1` (add prerequisite reminder text)
- Modify: `.gitignore`
- Create: `dbt/dbt_project.yml`
- Create: `dbt/models/example_iceberg_clickhouse.sql`
- Create: `dbt/models/example_iceberg_clickhouse.yml`
- Create: `dbt/tests/.gitkeep`, `dbt/macros/.gitkeep`
- Modify: `README.md` (new "dbt → ClickHouse materialization" section)
- Test: `tests/test_dbt_scaffold.py`

**Interfaces:**
- Produces: a `dbt/` project rooted at repo root with `name: oasis`, `profile: oasis`; the one example model; ignore rules for generated artifacts.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dbt_scaffold.py
"""The dbt project scaffold exists and is coherent."""
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_dbt_project_yml_names_oasis_profile():
    text = (REPO / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert "name: 'oasis'" in text or 'name: "oasis"' in text
    assert "profile: 'oasis'" in text or 'profile: "oasis"' in text


def test_example_model_uses_iceberglocal_and_warns():
    sql = (REPO / "dbt" / "models" / "example_iceberg_clickhouse.sql").read_text(encoding="utf-8")
    assert "icebergLocal(" in sql
    assert "CLICKHOUSE" in sql.upper()  # the operator-path warning comment


def test_requirements_pin_dbt():
    reqs = (REPO / "requirements-gui.txt").read_text(encoding="utf-8")
    assert "dbt-core" in reqs and "dbt-clickhouse" in reqs


def test_gitignore_excludes_generated_dbt_artifacts():
    ig = (REPO / ".gitignore").read_text(encoding="utf-8")
    for pat in ("dbt/profiles.yml", "dbt/target/", "dbt/logs/", "dbt/dbt_packages/"):
        assert pat in ig
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dbt_scaffold.py -v`
Expected: FAIL (files do not exist).

- [ ] **Step 3: Create the scaffold and edit config files**

`dbt/dbt_project.yml`:

```yaml
name: 'oasis'
version: '1.0.0'
config-version: 2

profile: 'oasis'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  oasis:
    +materialized: table
```

`dbt/models/example_iceberg_clickhouse.sql`:

```sql
-- Example: materialize a local Iceberg table into a native ClickHouse table.
--
-- WARNING: the path in icebergLocal(...) is read by the CLICKHOUSE SERVER from
-- ITS OWN filesystem, NOT from the machine running this control panel. Use a
-- path that is valid on the ClickHouse host. This app does not validate it.
{{ config(materialized='table') }}

select *
from icebergLocal('/absolute/path/on/clickhouse/iceberg_output/oasis/product_base')
```

`dbt/models/example_iceberg_clickhouse.yml`:

```yaml
version: 2
models:
  - name: example_iceberg_clickhouse
    description: "Example Iceberg->ClickHouse materialization (edit the path)."
    columns:
      - name: product_base_id
        tests:
          - not_null
```

Create empty `dbt/tests/.gitkeep` and `dbt/macros/.gitkeep` (empty files).

Append to `requirements-gui.txt` (after the Dagster block):

```
# dbt-core materialization layer (Iceberg -> ClickHouse). Keep the pair on the
# same minor. ClickHouse itself is an EXTERNAL prerequisite (24.x+ for icebergLocal).
dbt-core>=1.9,<1.10
dbt-clickhouse>=1.9,<1.10
```

Append to `.gitignore`:

```
# dbt generated artifacts (profiles.yml is generated from app config)
dbt/profiles.yml
dbt/target/
dbt/logs/
dbt/dbt_packages/
```

In `setup.sh`, after the "Installing dependencies" echo/section, add a reminder line near the Oracle reminder:

```bash
echo "Reminder: ClickHouse (24.x+) is an EXTERNAL prerequisite for the dbt layer and"
echo "          must be able to read the iceberg_output/ path used in icebergLocal()."
```

Add the equivalent `Write-Host` lines to `setup.ps1` next to its Oracle reminder.

Add a README section (near the pipeline overview) titled "dbt → ClickHouse materialization" covering: the external ClickHouse prerequisite, the `icebergLocal` filesystem constraint, and that config lives in `.dlt/secrets.toml [clickhouse]` + `.dlt/config.toml [dbt]` with `profiles.yml` generated by the app.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dbt_scaffold.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add requirements-gui.txt setup.sh setup.ps1 .gitignore dbt/ README.md tests/test_dbt_scaffold.py
git commit -m "feat(dbt): setup deps + dbt project scaffold"
```

---

### Task 2: Config constants for dbt

**Files:**
- Modify: `gui/config.py` (after the Dagster block, ~line 47)
- Test: `tests/test_config_constants.py` (add cases; file already exists)

**Interfaces:**
- Produces: `config.DBT_DIR: Path` (= `REPO_ROOT / "dbt"`), `config.DBT_PROFILES: Path` (= `DBT_DIR / "profiles.yml"`), `config.dbt_executable() -> str` (env `OASIS_DBT` or `"dbt"`).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_config_constants.py
def test_dbt_paths_and_executable(monkeypatch):
    import config
    assert config.DBT_DIR.name == "dbt"
    assert config.DBT_DIR == config.REPO_ROOT / "dbt"
    assert config.DBT_PROFILES == config.DBT_DIR / "profiles.yml"
    monkeypatch.delenv("OASIS_DBT", raising=False)
    assert config.dbt_executable() == "dbt"
    monkeypatch.setenv("OASIS_DBT", "/opt/venv/bin/dbt")
    assert config.dbt_executable() == "/opt/venv/bin/dbt"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config_constants.py::test_dbt_paths_and_executable -v`
Expected: FAIL (`AttributeError: module 'config' has no attribute 'DBT_DIR'`).

- [ ] **Step 3: Add the constants to `gui/config.py`**

Insert after the Dagster block (after `DAGSTER_HOME = REPO_ROOT / ".dagster_home"`):

```python
# --- dbt materialization layer --------------------------------------------- #
# The dbt project lives at <repo root>/dbt. profiles.yml is GENERATED from app
# config ([clickhouse] in secrets.toml + [dbt] in config.toml); never hand-edited.
DBT_DIR = REPO_ROOT / "dbt"
DBT_PROFILES = DBT_DIR / "profiles.yml"


def dbt_executable() -> str:
    """The dbt entry point. Defaults to ``dbt`` on PATH (the active venv's)."""
    return os.environ.get("OASIS_DBT") or "dbt"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_config_constants.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gui/config.py tests/test_config_constants.py
git commit -m "feat(dbt): config constants (DBT_DIR, DBT_PROFILES, dbt_executable)"
```

---

### Task 3: ClickHouse credentials store (`secrets.toml [clickhouse]`)

**Files:**
- Create: `gui/clickhouse_config.py`
- Test: `tests/test_clickhouse_config.py`

**Interfaces:**
- Produces:
  - `clickhouse_config.get_clickhouse() -> dict` — merged with defaults, **password redacted** (`has_password: bool`, no `password` key).
  - `clickhouse_config.save_clickhouse(payload: dict) -> dict` — writes `[clickhouse]` to `secrets.toml`; preserves the stored password when `payload["password"]` is blank; returns the redacted dict.
  - `clickhouse_config._raw() -> dict` — the raw section incl. password (internal; consumed by `dbt_config.render_profiles`).
  - `clickhouse_config.test_connection() -> dict` — runs `dbt debug`; returns `{"ok": bool, "output": str}`.
- Consumes: `config.SECRETS_TOML`, `config.STATE_DIR`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_clickhouse_config.py
"""ClickHouse credential store: defaults, redaction, password preservation."""
import pytest


@pytest.fixture
def secrets(tmp_path, monkeypatch):
    import config
    p = tmp_path / "secrets.toml"
    p.write_text('[oracle_branches.jazan]\nhost = "x"\n', encoding="utf-8")
    monkeypatch.setattr(config, "SECRETS_TOML", p)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    import clickhouse_config
    monkeypatch.setattr(clickhouse_config, "SECRETS_TOML", p)
    monkeypatch.setattr(clickhouse_config, "STATE_DIR", tmp_path)
    return p


def test_defaults_when_absent(secrets):
    import clickhouse_config as cc
    got = cc.get_clickhouse()
    assert got["port"] == 8123 and got["user"] == "default"
    assert got["secure"] is False and got["has_password"] is False
    assert "password" not in got


def test_save_and_redact(secrets):
    import clickhouse_config as cc
    out = cc.save_clickhouse({"host": "ch", "port": 9000, "user": "u",
                              "password": "sekret", "database": "analytics",
                              "secure": True})
    assert out["host"] == "ch" and out["has_password"] is True
    assert "password" not in out
    assert cc._raw()["password"] == "sekret"
    # Oracle section untouched
    assert "[oracle_branches.jazan]" in secrets.read_text(encoding="utf-8")


def test_blank_password_preserves_existing(secrets):
    import clickhouse_config as cc
    cc.save_clickhouse({"host": "ch", "password": "keepme", "database": "d"})
    cc.save_clickhouse({"host": "ch2", "password": "", "database": "d"})
    assert cc._raw()["password"] == "keepme"
    assert cc._raw()["host"] == "ch2"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_clickhouse_config.py -v`
Expected: FAIL (`ModuleNotFoundError: clickhouse_config`).

- [ ] **Step 3: Implement `gui/clickhouse_config.py`**

```python
"""Create / edit the single ``[clickhouse]`` section in ``.dlt/secrets.toml``.

Mirrors ``connections.py``: surgical block-level text editing so every other
section, comment and blank line stays byte-for-byte intact. Reads parse with
tomllib; writes are validated by re-parsing and a timestamped backup is kept.
The password never leaves the server (``get_clickhouse`` redacts it).
"""
from __future__ import annotations

import re
import shutil
import subprocess
from datetime import datetime
from typing import Any

from config import SECRETS_TOML, STATE_DIR

try:  # tomllib stdlib on 3.11+, tomli backport on 3.10
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover
    import tomli as _toml  # type: ignore

_SECTION_RE = re.compile(r"^\s*\[([^\]]+)\]\s*$")

DEFAULTS: dict[str, Any] = {
    "host": "", "port": 8123, "user": "default", "database": "default",
    "secure": False, "connect_timeout": 10,
}
FIELD_ORDER = ["host", "port", "user", "password", "database", "secure", "connect_timeout"]
_NUM_FIELDS = {"port", "connect_timeout"}
_BOOL_FIELDS = {"secure"}


def _raw() -> dict[str, Any]:
    if not SECRETS_TOML.exists():
        return {}
    with SECRETS_TOML.open("rb") as fh:
        return dict(_toml.load(fh).get("clickhouse", {}))


def get_clickhouse() -> dict[str, Any]:
    sec = _raw()
    out = {k: sec.get(k, DEFAULTS[k]) for k in DEFAULTS}
    out["has_password"] = bool(sec.get("password"))
    return out


def _fmt(field: str, val: Any) -> str | None:
    if val is None or val == "":
        return None
    if field in _NUM_FIELDS:
        return str(int(val))
    if field in _BOOL_FIELDS:
        return "true" if (val is True or str(val).strip().lower() == "true") else "false"
    s = str(val).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _emit_block(data: dict[str, Any]) -> list[str]:
    lines = ["[clickhouse]"]
    for field in FIELD_ORDER:
        if field in data:
            v = _fmt(field, data[field])
            if v is not None:
                lines.append(f"{field} = {v}")
    return lines


def _find_block(lines: list[str]) -> tuple[int, int] | None:
    """(header_index, end_index) of the ``[clickhouse]`` section, or None."""
    headers = [(i, m.group(1).strip())
               for i, line in enumerate(lines) if (m := _SECTION_RE.match(line))]
    for idx, (i, name) in enumerate(headers):
        if name == "clickhouse":
            end = headers[idx + 1][0] if idx + 1 < len(headers) else len(lines)
            return i, end
    return None


def _read_lines() -> list[str]:
    if not SECRETS_TOML.exists():
        return []
    return SECRETS_TOML.read_text(encoding="utf-8").splitlines()


def _write(lines: list[str]) -> None:
    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"
    if SECRETS_TOML.exists():
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(SECRETS_TOML, STATE_DIR / f"secrets.toml.{stamp}.bak")
    tmp = SECRETS_TOML.with_suffix(".toml.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        with tmp.open("rb") as fh:
            _toml.load(fh)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ValueError(f"refused to write corrupt secrets.toml: {exc}") from exc
    tmp.replace(SECRETS_TOML)


def save_clickhouse(payload: dict[str, Any]) -> dict[str, Any]:
    if not str(payload.get("host") or "").strip():
        raise ValueError("'host' is required")
    for f in _NUM_FIELDS:
        if payload.get(f) not in (None, ""):
            try:
                int(payload[f])
            except (TypeError, ValueError):
                raise ValueError(f"'{f}' must be a whole number") from None

    merged = dict(_raw())
    for f in ("host", "port", "user", "database", "secure", "connect_timeout"):
        if f in payload and payload[f] not in (None, ""):
            merged[f] = payload[f]
    # Password only overwritten when a fresh non-blank one is supplied.
    if str(payload.get("password") or "").strip():
        merged["password"] = payload["password"]

    lines = _read_lines()
    block = _find_block(lines)
    new_block = _emit_block(merged)
    if block is None:
        new = (lines + ["", *new_block]) if lines else new_block
    else:
        h, end = block
        # trim trailing blanks inside the old block
        while end - 1 > h and lines[end - 1].strip() == "":
            end -= 1
        new = lines[:h] + new_block + lines[end:]
    _write(new)
    return get_clickhouse()


def test_connection() -> dict[str, Any]:
    """Run ``dbt debug`` against the generated profile. Never raises."""
    import config
    import dbt_config
    try:
        dbt_config.write_profiles()
    except ValueError as exc:
        return {"ok": False, "output": str(exc)}
    try:
        proc = subprocess.run(
            [config.dbt_executable(), "debug",
             "--project-dir", str(dbt_config.dbt_dir()),
             "--profiles-dir", str(dbt_config.dbt_dir())],
            capture_output=True, text=True, timeout=60,
        )
        return {"ok": proc.returncode == 0, "output": (proc.stdout or "") + (proc.stderr or "")}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "output": f"{type(exc).__name__}: {exc}"}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_clickhouse_config.py -v`
Expected: PASS (3 tests). (`test_connection` is exercised later via the API smoke test.)

- [ ] **Step 5: Commit**

```bash
git add gui/clickhouse_config.py tests/test_clickhouse_config.py
git commit -m "feat(dbt): ClickHouse credential store in secrets.toml [clickhouse]"
```

---

### Task 4: dbt settings block + reusable TOML-block editor (`config.toml [dbt]`)

**Files:**
- Modify: `gui/workspace.py` (refactor `update_etl_settings`, add `[dbt]` support)
- Modify: `.dlt/config.toml` (add a `[dbt]` block)
- Test: `tests/test_dbt_settings.py`

**Interfaces:**
- Consumes: `workspace._read_toml`, `config.CONFIG_TOML`, `config.STATE_DIR`.
- Produces:
  - `workspace._update_toml_block(section: str, allowlist: set[str], updates: dict) -> dict` — the extracted in-place editor.
  - `workspace.dbt_settings() -> dict` — the `[dbt]` block as a dict.
  - `workspace.update_dbt_settings(updates: dict) -> dict` — returns `{"applied": {...}, "backup": "..."}`.
  - `workspace.EDITABLE_DBT_KEYS = {"project_dir","target","threads","default_materialization","dbt_executable"}`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dbt_settings.py
"""[dbt] block read + in-place edit, sharing the [etl] editor internals."""
import pytest

CONFIG = """[etl]
dataset_name = "oasis"
max_branch_workers = 7

[dbt]
project_dir = "dbt"
target = "dev"
threads = 4
default_materialization = "table"
dbt_executable = "dbt"
"""


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    import config, workspace
    p = tmp_path / "config.toml"
    p.write_text(CONFIG, encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_TOML", p)
    monkeypatch.setattr(workspace, "CONFIG_TOML", p)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    monkeypatch.setattr(workspace, "STATE_DIR", tmp_path)
    return p


def test_dbt_settings_reads_block(cfg):
    import workspace
    s = workspace.dbt_settings()
    assert s["target"] == "dev" and s["threads"] == 4


def test_update_dbt_settings_in_place(cfg):
    import workspace
    res = workspace.update_dbt_settings({"threads": 8, "target": "prod"})
    assert res["applied"] == {"threads": 8, "target": "prod"}
    text = cfg.read_text(encoding="utf-8")
    assert "threads = 8" in text and 'target = "prod"' in text
    # [etl] left intact
    assert 'dataset_name = "oasis"' in text


def test_update_dbt_rejects_unlisted_key(cfg):
    import workspace
    with pytest.raises(ValueError, match="Not editable"):
        workspace.update_dbt_settings({"password": "x"})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dbt_settings.py -v`
Expected: FAIL (`AttributeError: ... 'dbt_settings'`).

- [ ] **Step 3: Refactor `workspace.py` and add the `[dbt]` block**

In `gui/workspace.py`, add near `EDITABLE_ETL_KEYS`:

```python
EDITABLE_DBT_KEYS = {
    "project_dir", "target", "threads", "default_materialization", "dbt_executable",
}
```

Replace the body of `update_etl_settings` so it delegates to a shared helper, and add the helper + dbt readers/writers. The helper is the existing loop, generalized over the section name:

```python
def _update_toml_block(section: str, allowlist: set[str], updates: dict[str, Any]) -> dict[str, Any]:
    """Edit scalar keys inside ``[section]`` of config.toml in place.

    Only keys already present in the block and on ``allowlist`` are touched;
    every other line is preserved verbatim. Keeps a timestamped backup and
    validates by re-parsing.
    """
    bad = [k for k in updates if k not in allowlist]
    if bad:
        raise ValueError(f"Not editable: {', '.join(sorted(bad))}")
    if not CONFIG_TOML.exists():
        raise FileNotFoundError("config.toml")

    lines = CONFIG_TOML.read_text(encoding="utf-8").splitlines()
    in_block = False
    applied: dict[str, Any] = {}
    for i, line in enumerate(lines):
        header = re.match(r"^\s*\[([^\]]+)\]\s*$", line)
        if header:
            in_block = header.group(1).strip() == section
            continue
        if not in_block:
            continue
        m = _ETL_KV_RE.match(line)
        if not m:
            continue
        key = m.group(2)
        if key in updates:
            new_raw = _fmt_toml_scalar(m.group(4), updates[key])
            lines[i] = f"{m.group(1)}{key}{m.group(3)}{new_raw}{m.group(5)}"
            applied[key] = updates[key]

    missing = [k for k in updates if k not in applied]
    if missing:
        raise ValueError(f"Key(s) not found in [{section}]: {', '.join(missing)}")

    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = STATE_DIR / f"config.toml.{stamp}.bak"
    shutil.copy2(CONFIG_TOML, backup)
    tmp = CONFIG_TOML.with_suffix(".toml.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        with tmp.open("rb") as fh:
            _toml.load(fh)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ValueError(f"refused to write corrupt config.toml: {exc}") from exc
    tmp.replace(CONFIG_TOML)
    return {"applied": applied, "backup": str(backup)}


def update_etl_settings(updates: dict[str, Any]) -> dict[str, Any]:
    return _update_toml_block("etl", EDITABLE_ETL_KEYS, updates)


def dbt_settings() -> dict[str, Any]:
    """The ``[dbt]`` block of config.toml (defaults applied by callers)."""
    return dict(_read_toml(CONFIG_TOML).get("dbt", {}))


def update_dbt_settings(updates: dict[str, Any]) -> dict[str, Any]:
    return _update_toml_block("dbt", EDITABLE_DBT_KEYS, updates)
```

Append a `[dbt]` block to `.dlt/config.toml` (so the in-place editor can find each key):

```toml
# --------------------------------------------------------------------------- #
# dbt-core materialization layer (Iceberg -> ClickHouse). ClickHouse connection
# creds live in .dlt/secrets.toml [clickhouse]; the app GENERATES dbt/profiles.yml
# from these two blocks. ClickHouse is an external prerequisite (24.x+).
# --------------------------------------------------------------------------- #
[dbt]
project_dir             = "dbt"     # dbt project dir, relative to repo root
target                  = "dev"     # dbt target (matches the generated profile)
threads                 = 4         # dbt threads
default_materialization = "table"   # materialize Iceberg into native CH tables
dbt_executable          = "dbt"     # dbt entry point (usually on the venv PATH)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dbt_settings.py tests/test_dq_tolerance_settings.py -v`
Expected: PASS (new dbt tests **and** the existing `[etl]` settings tests still green after the refactor).

- [ ] **Step 5: Commit**

```bash
git add gui/workspace.py .dlt/config.toml tests/test_dbt_settings.py
git commit -m "feat(dbt): [dbt] settings block + shared in-place TOML editor"
```

---

### Task 5: profiles.yml generation (`dbt_config.py`)

**Files:**
- Create: `gui/dbt_config.py`
- Test: `tests/test_dbt_config.py`

**Interfaces:**
- Consumes: `config.REPO_ROOT`, `config.DBT_DIR`, `config.dbt_executable`, `workspace.dbt_settings`, `clickhouse_config._raw`.
- Produces:
  - `dbt_config.dbt_dir() -> Path`, `dbt_config.dbt_target() -> str`, `dbt_config.dbt_threads() -> int`, `dbt_config.dbt_executable() -> str`.
  - `dbt_config.render_profiles() -> dict` — raises `ValueError("configure ClickHouse first ...")` if no host.
  - `dbt_config.write_profiles() -> Path` — writes `<dbt_dir>/profiles.yml` atomically, returns the path.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dbt_config.py
"""profiles.yml generation from [clickhouse] + [dbt]."""
import pytest
import yaml


@pytest.fixture
def wired(tmp_path, monkeypatch):
    import config, workspace, clickhouse_config, dbt_config
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    monkeypatch.setattr(config, "DBT_DIR", dbt_dir)
    monkeypatch.setattr(dbt_config, "DBT_DIR", dbt_dir)
    monkeypatch.setattr(dbt_config, "dbt_settings",
                        lambda: {"target": "dev", "threads": 6, "project_dir": "dbt"})
    return dbt_dir, monkeypatch, clickhouse_config, dbt_config


def test_render_requires_clickhouse(wired):
    _, mp, cc, dc = wired
    mp.setattr(cc, "_raw", lambda: {})
    with pytest.raises(ValueError, match="ClickHouse"):
        dc.render_profiles()


def test_render_shapes_profile(wired):
    _, mp, cc, dc = wired
    mp.setattr(cc, "_raw", lambda: {"host": "ch", "port": 8123, "user": "u",
                                    "password": "p", "database": "analytics",
                                    "secure": False, "connect_timeout": 10})
    prof = dc.render_profiles()
    out = prof["oasis"]["outputs"]["dev"]
    assert prof["oasis"]["target"] == "dev"
    assert out["type"] == "clickhouse" and out["schema"] == "analytics"
    assert out["password"] == "p" and out["threads"] == 6


def test_write_profiles_creates_file(wired):
    dbt_dir, mp, cc, dc = wired
    mp.setattr(cc, "_raw", lambda: {"host": "ch", "database": "d", "password": "p"})
    path = dc.write_profiles()
    assert path == dbt_dir / "profiles.yml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert loaded["oasis"]["outputs"]["dev"]["host"] == "ch"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dbt_config.py -v`
Expected: FAIL (`ModuleNotFoundError: dbt_config`).

- [ ] **Step 3: Implement `gui/dbt_config.py`**

```python
"""Resolve dbt settings and GENERATE dbt/profiles.yml from app config.

App config is the single source of truth: ClickHouse creds live in
``secrets.toml [clickhouse]`` and dbt knobs in ``config.toml [dbt]``. This module
renders both into a profiles.yml the dbt CLI consumes. profiles.yml is generated
(git-ignored) and never hand-edited.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

import clickhouse_config
import config
import workspace

DBT_DIR = config.DBT_DIR

_DEFAULTS = {"target": "dev", "threads": 4, "project_dir": "dbt",
             "default_materialization": "table", "dbt_executable": "dbt"}


def _settings() -> dict[str, Any]:
    s = dict(_DEFAULTS)
    s.update({k: v for k, v in workspace.dbt_settings().items() if v not in (None, "")})
    return s


def dbt_dir() -> Path:
    pd = str(_settings().get("project_dir") or "dbt")
    p = Path(pd)
    return p if p.is_absolute() else (config.REPO_ROOT / p)


def dbt_target() -> str:
    return str(_settings().get("target") or "dev")


def dbt_threads() -> int:
    try:
        return int(_settings().get("threads") or 4)
    except (TypeError, ValueError):
        return 4


def dbt_executable() -> str:
    return str(_settings().get("dbt_executable") or config.dbt_executable())


def render_profiles() -> dict[str, Any]:
    ch = clickhouse_config._raw()
    host = str(ch.get("host") or "").strip()
    if not host:
        raise ValueError("configure ClickHouse first (secrets.toml [clickhouse].host)")
    target = dbt_target()
    output = {
        "type": "clickhouse",
        "driver": "http",
        "host": host,
        "port": int(ch.get("port", 8123)),
        "user": str(ch.get("user", "default")),
        "password": str(ch.get("password", "")),
        "schema": str(ch.get("database", "default")),
        "secure": bool(ch.get("secure", False)),
        "connect_timeout": int(ch.get("connect_timeout", 10)),
        "threads": dbt_threads(),
    }
    return {"oasis": {"target": target, "outputs": {target: output}}}


def write_profiles() -> Path:
    profiles = render_profiles()
    d = dbt_dir()
    d.mkdir(parents=True, exist_ok=True)
    path = d / "profiles.yml"
    tmp = path.with_suffix(".yml.tmp")
    tmp.write_text(yaml.safe_dump(profiles, sort_keys=False), encoding="utf-8")
    tmp.replace(path)
    return path
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dbt_config.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add gui/dbt_config.py tests/test_dbt_config.py
git commit -m "feat(dbt): generate profiles.yml from [clickhouse] + [dbt]"
```

---

### Task 6: `dbt` command type in `commands.py`

**Files:**
- Modify: `gui/commands.py` (`SCRIPT_CHOICES` line 15; `build_argv` line 34)
- Test: `tests/test_commands_dbt.py`

**Interfaces:**
- Consumes: `dbt_config.dbt_dir/dbt_target/dbt_executable`.
- Produces: `build_argv({"script":"dbt", "dbt_command":..., "select":..., "full_refresh":..., "extra":...}) -> (argv, label)`. `build_argv` stays **pure** (no file writes) so `/api/command/preview` is side-effect-free. `"dbt"` added to `SCRIPT_CHOICES`. Module constant `DBT_COMMANDS = {"run","test","build","compile","debug"}`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commands_dbt.py
"""build_argv for the dbt script type."""
import pytest


def test_dbt_run_argv():
    import commands
    argv, label = commands.build_argv(
        {"script": "dbt", "dbt_command": "run", "select": "stg_products"})
    assert argv[1] == "run"
    assert "--project-dir" in argv and "--profiles-dir" in argv
    assert argv[argv.index("--select") + 1] == "stg_products"
    assert "--target" in argv
    assert label == "dbt run stg_products"


def test_dbt_full_refresh_only_on_run_build():
    import commands
    argv, _ = commands.build_argv(
        {"script": "dbt", "dbt_command": "run", "select": "m", "full_refresh": True})
    assert "--full-refresh" in argv
    argv2, _ = commands.build_argv(
        {"script": "dbt", "dbt_command": "test", "select": "m", "full_refresh": True})
    assert "--full-refresh" not in argv2


def test_dbt_debug_ignores_select():
    import commands
    argv, _ = commands.build_argv({"script": "dbt", "dbt_command": "debug", "select": "x"})
    assert argv[1] == "debug" and "--select" not in argv


def test_dbt_rejects_bad_command():
    import commands
    with pytest.raises(ValueError, match="dbt command"):
        commands.build_argv({"script": "dbt", "dbt_command": "nuke"})


def test_dbt_in_script_choices():
    import commands
    assert "dbt" in commands.SCRIPT_CHOICES
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_commands_dbt.py -v`
Expected: FAIL (`Unknown script: dbt`).

- [ ] **Step 3: Implement the dbt branch in `gui/commands.py`**

Change `SCRIPT_CHOICES` (line 15) to include `"dbt"`:

```python
SCRIPT_CHOICES = ["oracle_to_iceberg", "dq_check", "snapshot_diff", "fresh_run", "dbt", "custom"]
DBT_COMMANDS = {"run", "test", "build", "compile", "debug"}
```

In `build_argv`, add a branch **before** the `if script not in SCRIPTS:` guard (dbt is not in `SCRIPTS`):

```python
    if script == "dbt":
        return _dbt_argv(spec)
```

Add the helper (imports `dbt_config` lazily to avoid import cost on non-dbt calls):

```python
def _dbt_argv(spec: dict[str, Any]) -> tuple[list[str], str]:
    import dbt_config

    cmd = (spec.get("dbt_command") or "run").strip().lower()
    if cmd not in DBT_COMMANDS:
        raise ValueError(f"Unknown dbt command: {cmd!r} (allowed: {', '.join(sorted(DBT_COMMANDS))})")
    d = str(dbt_config.dbt_dir())
    argv = [dbt_config.dbt_executable(), cmd,
            "--project-dir", d, "--profiles-dir", d,
            "--target", dbt_config.dbt_target()]
    sel = str(spec.get("select") or "").strip()
    if sel and cmd != "debug":
        argv += ["--select", sel]
    if spec.get("full_refresh") and cmd in ("run", "build"):
        argv.append("--full-refresh")
    argv += _split(spec.get("extra"))
    label = " ".join(["dbt", cmd] + ([sel] if sel and cmd != "debug" else []))
    return argv, label
```

> Note: `build_argv` remains pure. `profiles.yml` is (re)generated at run time by the execution paths (Tasks 8 & 11), not here — so `/api/command/preview` never writes files and preview works even before ClickHouse is configured.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_commands_dbt.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add gui/commands.py tests/test_commands_dbt.py
git commit -m "feat(dbt): dbt command type in build_argv"
```

---

### Task 7: dbt project store (list / read / write / create, path-safe)

**Files:**
- Create: `gui/dbt_project_store.py`
- Test: `tests/test_dbt_project_store.py`

**Interfaces:**
- Consumes: `dbt_config.dbt_dir`, `config.dbt_executable`.
- Produces:
  - `list_models() -> list[dict]`, `list_tests() -> list[dict]` — each item `{"name","path","resource_type"}`.
  - `read_file(rel: str) -> str`, `write_file(rel: str, content: str) -> dict` (`{"path": rel}`), `delete_file(rel: str) -> bool`.
  - `create_from_template(name: str, kind: str, materialization: str = "table") -> dict` (`kind ∈ {"model","test"}`, returns `{"path": rel}`).
  - Raises `ValueError` on path escapes / disallowed extensions.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dbt_project_store.py
"""dbt project file store: listing, templates, and path-traversal safety."""
import pytest


@pytest.fixture
def proj(tmp_path, monkeypatch):
    import dbt_config, dbt_project_store as ps
    d = tmp_path / "dbt"
    (d / "models").mkdir(parents=True)
    (d / "tests").mkdir(parents=True)
    (d / "models" / "stg_a.sql").write_text("select 1", encoding="utf-8")
    (d / "tests" / "assert_x.sql").write_text("select 1 where 1=0", encoding="utf-8")
    monkeypatch.setattr(dbt_config, "DBT_DIR", d)
    monkeypatch.setattr(dbt_config, "dbt_dir", lambda: d)
    # keep tests hermetic: no live `dbt ls`
    monkeypatch.setattr(ps, "_dbt_ls", lambda rt: [])
    return d


def test_list_models_from_filesystem(proj):
    import dbt_project_store as ps
    names = {m["name"] for m in ps.list_models()}
    assert "stg_a" in names


def test_list_tests_from_filesystem(proj):
    import dbt_project_store as ps
    names = {t["name"] for t in ps.list_tests()}
    assert "assert_x" in names


def test_create_and_read_model(proj):
    import dbt_project_store as ps
    out = ps.create_from_template("stg_products", "model", "table")
    assert out["path"].endswith("stg_products.sql")
    body = ps.read_file(out["path"])
    assert "icebergLocal(" in body and "materialized='table'" in body


def test_write_and_read_roundtrip(proj):
    import dbt_project_store as ps
    ps.write_file("models/stg_a.sql", "select 42")
    assert ps.read_file("models/stg_a.sql") == "select 42"


def test_path_traversal_rejected(proj):
    import dbt_project_store as ps
    with pytest.raises(ValueError):
        ps.read_file("../../etc/passwd")
    with pytest.raises(ValueError):
        ps.write_file("models/x.py", "print(1)")   # disallowed extension
    with pytest.raises(ValueError):
        ps.write_file("/abs/models/x.sql", "select 1")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dbt_project_store.py -v`
Expected: FAIL (`ModuleNotFoundError: dbt_project_store`).

- [ ] **Step 3: Implement `gui/dbt_project_store.py`**

```python
"""Browse and edit files in the dbt project, safely.

Lists models & tests (filesystem scan, enriched best-effort by ``dbt ls``),
reads/writes/creates ``.sql``/``.yml`` files, and refuses any path that escapes
the dbt project dir or uses a disallowed extension.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import config
import dbt_config

_ALLOWED_SUFFIX = {".sql", ".yml", ".yaml"}

MODEL_TEMPLATE = """\
-- {name}: materialize a local Iceberg table into a native ClickHouse table.
--
-- WARNING: the icebergLocal(...) path is read by the CLICKHOUSE SERVER from its
-- own filesystem, not this host. Use a path valid on the ClickHouse host.
{{{{ config(materialized='{materialization}') }}}}

select *
from icebergLocal('/absolute/path/on/clickhouse/iceberg_output/oasis/CHANGE_ME')
"""

TEST_TEMPLATE = """\
-- {name}: a singular data test. It must return zero rows to pass.
select *
from {{{{ ref('CHANGE_ME') }}}}
where 1 = 0
"""


def _root() -> Path:
    return dbt_config.dbt_dir().resolve()


def _resolve(rel: str) -> Path:
    """Resolve ``rel`` under the dbt dir; raise on escape / bad extension."""
    rel = str(rel or "").strip()
    if not rel:
        raise ValueError("empty path")
    p = (_root() / rel).resolve()
    root = _root()
    if root not in p.parents and p != root:
        raise ValueError(f"path escapes the dbt project: {rel!r}")
    if p.suffix.lower() not in _ALLOWED_SUFFIX:
        raise ValueError(f"only {sorted(_ALLOWED_SUFFIX)} files are allowed")
    return p


def _rel(p: Path) -> str:
    return p.relative_to(_root()).as_posix()


def _scan(subdir: str) -> list[dict[str, Any]]:
    base = _root() / subdir
    if not base.exists():
        return []
    return [{"name": p.stem, "path": _rel(p), "resource_type": subdir.rstrip("s")}
            for p in sorted(base.rglob("*.sql"))]


def _dbt_ls(resource_type: str) -> list[dict[str, Any]]:
    """Best-effort ``dbt ls`` enrichment; returns [] on any failure."""
    d = str(dbt_config.dbt_dir())
    try:
        proc = subprocess.run(
            [config.dbt_executable(), "ls", "--resource-type", resource_type,
             "--output", "json", "--project-dir", d, "--profiles-dir", d],
            capture_output=True, text=True, timeout=60,
        )
        if proc.returncode != 0:
            return []
        out = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = obj.get("name")
            if name:
                out.append({"name": name, "path": obj.get("original_file_path", ""),
                            "resource_type": resource_type})
        return out
    except Exception:  # noqa: BLE001
        return []


def _merge(fs: list[dict[str, Any]], ls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name = {x["name"]: x for x in fs}
    for x in ls:
        by_name.setdefault(x["name"], x)
    return sorted(by_name.values(), key=lambda x: x["name"])


def list_models() -> list[dict[str, Any]]:
    return _merge(_scan("models"), _dbt_ls("model"))


def list_tests() -> list[dict[str, Any]]:
    return _merge(_scan("tests"), _dbt_ls("test"))


def read_file(rel: str) -> str:
    p = _resolve(rel)
    if not p.exists():
        raise FileNotFoundError(rel)
    return p.read_text(encoding="utf-8")


def write_file(rel: str, content: str) -> dict[str, Any]:
    p = _resolve(rel)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(content if content is not None else "", encoding="utf-8")
    tmp.replace(p)
    return {"path": _rel(p)}


def delete_file(rel: str) -> bool:
    p = _resolve(rel)
    if not p.exists():
        return False
    p.unlink()
    return True


def create_from_template(name: str, kind: str, materialization: str = "table") -> dict[str, Any]:
    stem = "".join(c for c in str(name or "").strip() if c.isalnum() or c in ("_", "-"))
    if not stem:
        raise ValueError("name must be alphanumeric / underscore")
    if kind == "model":
        rel = f"models/{stem}.sql"
        body = MODEL_TEMPLATE.format(name=stem, materialization=materialization or "table")
    elif kind == "test":
        rel = f"tests/{stem}.sql"
        body = TEST_TEMPLATE.format(name=stem)
    else:
        raise ValueError("kind must be 'model' or 'test'")
    if _resolve(rel).exists():
        raise ValueError(f"{rel} already exists")
    return write_file(rel, body)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dbt_project_store.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add gui/dbt_project_store.py tests/test_dbt_project_store.py
git commit -m "feat(dbt): path-safe dbt project file store"
```

---

### Task 8: Flask API + page route + nav

**Files:**
- Modify: `gui/app.py` (imports; new page route; new `/api/dbt/*` routes)
- Modify: `gui/templates/base.html` (nav item + title)
- Test: `tests/test_dbt_api.py`

**Interfaces:**
- Consumes: `dbt_project_store`, `clickhouse_config`, `dbt_config`, `workspace.dbt_settings/update_dbt_settings`, `runner` (`RunManager`), `commands.build_argv`.
- Produces routes:
  - `GET /models` (page), `GET /api/dbt/models`, `GET /api/dbt/tests`
  - `GET /api/dbt/file?path=…`, `PUT /api/dbt/file`, `POST /api/dbt/file`, `DELETE /api/dbt/file`
  - `GET /api/dbt/config`, `PUT /api/dbt/config`
  - `POST /api/dbt/test-connection`
  - `POST /api/dbt/run` → `{run id ...}` (reuses `runner`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dbt_api.py
"""Smoke tests for the dbt API routes (via Flask test client)."""
import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(gui_app.dbt_project_store, "list_models",
                        lambda: [{"name": "stg_a", "path": "models/stg_a.sql", "resource_type": "model"}])
    monkeypatch.setattr(gui_app.dbt_project_store, "list_tests", lambda: [])
    monkeypatch.setattr(gui_app.clickhouse_config, "get_clickhouse",
                        lambda: {"host": "ch", "port": 8123, "has_password": True})
    monkeypatch.setattr(gui_app.workspace, "dbt_settings", lambda: {"target": "dev"})
    return gui_app.app.test_client()


def test_models_route(client):
    r = client.get("/api/dbt/models")
    assert r.status_code == 200
    assert r.get_json()["models"][0]["name"] == "stg_a"


def test_config_route_redacts(client):
    r = client.get("/api/dbt/config")
    body = r.get_json()
    assert body["clickhouse"]["has_password"] is True
    assert "password" not in body["clickhouse"]
    assert body["dbt"]["target"] == "dev"


def test_models_page_renders(client):
    assert client.get("/models").status_code == 200
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dbt_api.py -v`
Expected: FAIL (routes / imports missing → 404 or AttributeError).

- [ ] **Step 3: Wire `gui/app.py` and the nav**

Add imports near the other blueprint-style imports (after `import connections`):

```python
import clickhouse_config  # noqa: E402
import dbt_config  # noqa: E402
import dbt_project_store  # noqa: E402
```

Add the page route (near the other `@app.route` pages):

```python
@app.route("/models")
def page_models():
    return render_template("dbt.html", active="models")
```

Add the API section (near the other API groups):

```python
# --------------------------------------------------------------------------- #
# dbt API
# --------------------------------------------------------------------------- #
@app.get("/api/dbt/models")
@api
def api_dbt_models():
    return jsonify({"models": dbt_project_store.list_models()})


@app.get("/api/dbt/tests")
@api
def api_dbt_tests():
    return jsonify({"tests": dbt_project_store.list_tests()})


@app.get("/api/dbt/file")
@api
def api_dbt_file_get():
    path = request.args.get("path", "")
    return jsonify({"path": path, "content": dbt_project_store.read_file(path)})


@app.put("/api/dbt/file")
@api
def api_dbt_file_put():
    b = _body()
    return jsonify(dbt_project_store.write_file(b.get("path", ""), b.get("content", "")))


@app.post("/api/dbt/file")
@api
def api_dbt_file_create():
    b = _body()
    return jsonify(dbt_project_store.create_from_template(
        b.get("name", ""), b.get("kind", "model"), b.get("materialization", "table")))


@app.delete("/api/dbt/file")
@api
def api_dbt_file_delete():
    return jsonify({"deleted": dbt_project_store.delete_file(request.args.get("path", ""))})


@app.get("/api/dbt/config")
@api
def api_dbt_config_get():
    return jsonify({"clickhouse": clickhouse_config.get_clickhouse(),
                    "dbt": workspace.dbt_settings()})


@app.put("/api/dbt/config")
@api
def api_dbt_config_put():
    b = _body()
    out = {}
    if b.get("clickhouse") is not None:
        out["clickhouse"] = clickhouse_config.save_clickhouse(b["clickhouse"])
    if b.get("dbt") is not None:
        out["dbt"] = workspace.update_dbt_settings(b["dbt"])
    # Regenerate profiles.yml from the new config (best effort).
    try:
        dbt_config.write_profiles()
    except ValueError:
        pass
    return jsonify(out)


@app.post("/api/dbt/test-connection")
@api
def api_dbt_test_connection():
    return jsonify(clickhouse_config.test_connection())


@app.post("/api/dbt/run")
@api
def api_dbt_run():
    b = _body()
    spec = {"script": "dbt", "dbt_command": b.get("dbt_command", "run"),
            "select": b.get("select", ""), "full_refresh": bool(b.get("full_refresh")),
            "extra": b.get("extra", "")}
    argv, label = commands.build_argv(spec)
    dbt_config.write_profiles()  # ensure the profile is current before running
    return jsonify(runner.start(argv, label=label))
```

In `gui/templates/base.html`, add to the `items` list (after the `flows` entry) and to `titles`:

```
    ('models',      '/models',      'Models',           'deployed_code'),
```
```
    'models': 'Models & Tests',
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dbt_api.py -v`
Expected: PASS (3 tests). (`/models` renders once Task 9 adds the template — for now it will 500 on missing template; **do Step 3 of Task 9 before running `test_models_page_renders`**, or temporarily xfail it. To keep tasks independent, create a minimal `dbt.html` stub here and flesh it out in Task 9.)

> To keep this task self-contained, also create a **minimal** `gui/templates/dbt.html` stub now: `{% extends "base.html" %}{% block content %}<div id="dbt-root"></div>{% endblock %}`. Task 9 replaces it with the full page.

- [ ] **Step 5: Commit**

```bash
git add gui/app.py gui/templates/base.html gui/templates/dbt.html tests/test_dbt_api.py
git commit -m "feat(dbt): Flask API, /models route, nav entry"
```

---

### Task 9: Models & Tests page UI (`dbt.html`)

**Files:**
- Modify: `gui/templates/dbt.html` (replace the stub with the full page)

**Interfaces:**
- Consumes: `/api/dbt/models`, `/api/dbt/tests`, `/api/dbt/file`, `/api/dbt/config`, `/api/dbt/test-connection`, `/api/dbt/run`, `/api/runs/<id>/tail`; JS helpers from `app.js` (`el`, `apiGet`, `apiPost`, `apiPut`, `apiDel`, `ok`, `err`, `esc`, `pill`).

- [ ] **Step 1: Write the full page**

Replace `gui/templates/dbt.html`:

```html
{% extends "base.html" %}
{% block content %}
<div class="page-head">
  <h1><i class="fa-solid fa-cubes"></i> Models &amp; Tests</h1>
  <p>Author dbt models that materialize the Iceberg lake into ClickHouse via <span class="mono">icebergLocal()</span>, then run &amp; test them. <b>Paths in icebergLocal() must be valid on the ClickHouse host.</b></p>
</div>

<!-- ClickHouse + dbt settings -->
<div class="panel" style="margin-bottom:18px">
  <div class="panel-head"><h2><i class="fa-solid fa-database"></i> ClickHouse &amp; dbt settings</h2>
    <div class="row-flex">
      <button class="btn sm" id="ch-test"><i class="fa-solid fa-plug"></i> Test connection</button>
      <button class="btn sm primary" id="cfg-save"><i class="fa-solid fa-floppy-disk"></i> Save</button>
    </div>
  </div>
  <div class="form-row">
    <div><label>Host</label><input id="ch-host"></div>
    <div><label>Port</label><input id="ch-port" type="number" value="8123"></div>
  </div>
  <div class="form-row">
    <div><label>User</label><input id="ch-user" value="default"></div>
    <div><label>Password <small>(blank = keep)</small></label><input id="ch-pass" type="password" placeholder="•••••"></div>
  </div>
  <div class="form-row">
    <div><label>Database</label><input id="ch-db" value="default"></div>
    <div><label>dbt target</label><input id="dbt-target" value="dev"></div>
  </div>
  <div class="checkbox"><input type="checkbox" id="ch-secure"><label for="ch-secure">secure (TLS)</label></div>
  <div id="ch-teststatus" class="hint"></div>
</div>

<div class="grid-side">
  <!-- left: model / test lists -->
  <div class="panel">
    <div class="panel-head"><h2>Models <span id="m-count" class="tag"></span></h2>
      <button class="btn sm" id="new-model"><i class="fa-solid fa-plus"></i> New model</button></div>
    <div class="table-wrap" style="max-height:240px"><table id="models-table"><tbody></tbody></table></div>

    <div class="panel-head" style="margin-top:14px"><h2>Tests <span id="t-count" class="tag"></span></h2>
      <button class="btn sm" id="new-test"><i class="fa-solid fa-plus"></i> New test</button></div>
    <div class="table-wrap" style="max-height:200px"><table id="tests-table"><tbody></tbody></table></div>
  </div>

  <!-- right: editor + actions + output -->
  <div>
    <div class="panel">
      <div class="panel-head"><h2>Editor <span id="cur-file" class="mono muted">— no file —</span></h2>
        <button class="btn sm primary" id="file-save"><i class="fa-solid fa-floppy-disk"></i> Save file</button></div>
      <textarea id="editor" class="mono" style="width:100%;min-height:320px" spellcheck="false"
        placeholder="Select a model or test on the left, or create a new one."></textarea>
      <div class="btn-row">
        <button class="btn primary" id="btn-run"><i class="fa-solid fa-play"></i> Run</button>
        <button class="btn" id="btn-test"><i class="fa-solid fa-vial"></i> Test</button>
        <button class="btn ghost" id="btn-compile">Compile</button>
        <button class="btn ghost" id="btn-debug">Debug</button>
        <button class="btn bad sm" id="btn-del"><i class="fa-solid fa-trash"></i> Delete</button>
      </div>
    </div>
    <div class="panel">
      <div class="panel-head"><h2>Output <span id="run-status"></span></h2></div>
      <div id="dbt-console" class="console">Run a model or test to see output…</div>
    </div>
  </div>
</div>
{% endblock %}

{% block scripts %}
<script>
let MODELS = [], TESTS = [], curFile = null, curSel = null, tailTimer = null, tailOffset = 0;

async function loadConfig() {
  const c = await apiGet("/api/dbt/config");
  el("ch-host").value = c.clickhouse.host || "";
  el("ch-port").value = c.clickhouse.port || 8123;
  el("ch-user").value = c.clickhouse.user || "default";
  el("ch-db").value   = c.clickhouse.database || "default";
  el("ch-secure").checked = !!c.clickhouse.secure;
  el("dbt-target").value = (c.dbt && c.dbt.target) || "dev";
  el("ch-pass").placeholder = c.clickhouse.has_password ? "•••••• (stored)" : "password";
}
async function saveConfig() {
  const body = { clickhouse: {
      host: el("ch-host").value.trim(), port: Number(el("ch-port").value) || 8123,
      user: el("ch-user").value.trim(), database: el("ch-db").value.trim(),
      secure: el("ch-secure").checked, password: el("ch-pass").value },
    dbt: {} };
  const tgt = el("dbt-target").value.trim(); if (tgt) body.dbt.target = tgt;
  if (!Object.keys(body.dbt).length) delete body.dbt;
  try { await apiPut("/api/dbt/config", body); el("ch-pass").value = ""; ok("Saved"); loadConfig(); }
  catch (e) { err(e.message); }
}
async function testConn() {
  el("ch-teststatus").textContent = "Testing…";
  try { const r = await apiPost("/api/dbt/test-connection", {});
    el("ch-teststatus").innerHTML = (r.ok ? "✅ connected" : "❌ failed") +
      `<pre class="mono" style="white-space:pre-wrap;max-height:160px;overflow:auto">${esc(r.output||"")}</pre>`;
  } catch (e) { err(e.message); }
}

async function loadLists() {
  const [m, t] = [await apiGet("/api/dbt/models"), await apiGet("/api/dbt/tests")];
  MODELS = m.models || []; TESTS = t.tests || [];
  el("m-count").textContent = MODELS.length; el("t-count").textContent = TESTS.length;
  $("#models-table tbody").innerHTML = MODELS.map(x =>
    `<tr><td><button class="btn sm ghost" onclick="openFile('${esc(x.path)}','${esc(x.name)}')">${esc(x.name)}</button></td></tr>`
  ).join("") || `<tr><td class="muted">No models yet.</td></tr>`;
  $("#tests-table tbody").innerHTML = TESTS.map(x =>
    `<tr><td><button class="btn sm ghost" onclick="openFile('${esc(x.path)}','${esc(x.name)}')">${esc(x.name)}</button></td></tr>`
  ).join("") || `<tr><td class="muted">No tests yet.</td></tr>`;
}
async function openFile(path, name) {
  try { const r = await apiGet("/api/dbt/file?path=" + encodeURIComponent(path));
    curFile = path; curSel = name; el("cur-file").textContent = path;
    el("editor").value = r.content;
  } catch (e) { err(e.message); }
}
async function saveFile() {
  if (!curFile) return err("No file open");
  try { await apiPut("/api/dbt/file", { path: curFile, content: el("editor").value }); ok("Saved " + curFile); }
  catch (e) { err(e.message); }
}
async function newFile(kind) {
  const name = prompt(`New ${kind} name (letters/digits/underscore):`); if (!name) return;
  try { const r = await apiPost("/api/dbt/file", { name, kind, materialization: "table" });
    ok("Created " + r.path); await loadLists(); openFile(r.path, name);
  } catch (e) { err(e.message); }
}
async function delFile() {
  if (!curFile || !confirm("Delete " + curFile + "?")) return;
  try { await apiDel("/api/dbt/file?path=" + encodeURIComponent(curFile)); ok("Deleted");
    curFile = null; curSel = null; el("editor").value = ""; el("cur-file").textContent = "— no file —"; loadLists();
  } catch (e) { err(e.message); }
}

async function runDbt(cmd) {
  const body = { dbt_command: cmd };
  if (cmd !== "debug" && cmd !== "compile" && curSel) body.select = curSel;
  el("dbt-console").textContent = ""; tailOffset = 0;
  try { const run = await apiPost("/api/dbt/run", body); tailRun(run.id); }
  catch (e) { err(e.message); }
}
function tailRun(id) {
  clearInterval(tailTimer);
  const poll = async () => {
    try { const r = await apiGet(`/api/runs/${id}/tail?offset=${tailOffset}`);
      if (r.chunk) { const c = el("dbt-console"); c.textContent += r.chunk; c.scrollTop = c.scrollHeight; }
      tailOffset = r.offset;
      el("run-status").innerHTML = pill(r.status) + (r.returncode != null ? ` <small>rc=${r.returncode}</small>` : "");
      if (r.status !== "running" && r.status !== "detached") clearInterval(tailTimer);
    } catch (e) { clearInterval(tailTimer); }
  };
  poll(); tailTimer = setInterval(poll, 1300);
}

el("cfg-save").onclick = saveConfig;
el("ch-test").onclick = testConn;
el("new-model").onclick = () => newFile("model");
el("new-test").onclick = () => newFile("test");
el("file-save").onclick = saveFile;
el("btn-del").onclick = delFile;
el("btn-run").onclick = () => runDbt("run");
el("btn-test").onclick = () => runDbt("test");
el("btn-compile").onclick = () => runDbt("compile");
el("btn-debug").onclick = () => runDbt("debug");
loadConfig(); loadLists();
</script>
{% endblock %}
```

- [ ] **Step 2: Verify the page renders**

Run: `python -m pytest tests/test_dbt_api.py::test_models_page_renders -v`
Expected: PASS (template renders, 200).

- [ ] **Step 3: Manual smoke (optional but recommended)**

Run the GUI (`python gui/app.py`), open `http://127.0.0.1:8765/models`, confirm: settings load, "New model" creates a file that appears in the list and opens in the editor, Save works. (ClickHouse actions need a live server.)

- [ ] **Step 4: Commit**

```bash
git add gui/templates/dbt.html
git commit -m "feat(dbt): Models & Tests page (list, edit, run/test, settings)"
```

---

### Task 10: dbt-kind flow nodes — store & validation

**Files:**
- Modify: `gui/flows_store.py` (`validate_flow` line 30; `referencing_flows` line 156)
- Test: `tests/test_flows_store.py` (add cases)

**Interfaces:**
- Consumes: nothing new.
- Produces: `validate_flow` accepts nodes where `node.get("kind","pipeline")` is `"pipeline"` (requires `pipeline_id ∈ known_pipeline_ids`) or `"dbt"` (requires `node["dbt"]["dbt_command"] ∈ {"run","test","build"}` and non-empty `node["dbt"]["select"]`). `referencing_flows` matches pipeline nodes only.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_flows_store.py
DBT_NODE = {"node_id": "d1", "kind": "dbt",
            "dbt": {"dbt_command": "run", "select": "stg_products"}, "deps": []}


def test_validate_accepts_dbt_node():
    import flows_store as fs
    fs.validate_flow([DBT_NODE], known_pipeline_ids=set())  # must not raise


def test_validate_rejects_dbt_without_select():
    import flows_store as fs
    bad = {"node_id": "d1", "kind": "dbt", "dbt": {"dbt_command": "run", "select": ""}, "deps": []}
    with pytest.raises(ValueError, match="select"):
        fs.validate_flow([bad], known_pipeline_ids=set())


def test_validate_rejects_dbt_bad_command():
    import flows_store as fs
    bad = {"node_id": "d1", "kind": "dbt", "dbt": {"dbt_command": "debug", "select": "m"}, "deps": []}
    with pytest.raises(ValueError, match="dbt command"):
        fs.validate_flow([bad], known_pipeline_ids=set())


def test_mixed_flow_and_referencing(state_dir):
    import flows_store as fs
    import pipelines_store as ps
    pa = ps.add_pipeline("a", {"script": "dq_check"})["id"]
    nodes = [{"node_id": "p1", "pipeline_id": pa, "deps": []},
             {"node_id": "d1", "kind": "dbt", "dbt": {"dbt_command": "run", "select": "m"}, "deps": ["p1"]}]
    f = fs.add_flow("mixed", nodes, "0 2 * * *", "UTC", {})
    # dbt node must not blow up referencing_flows (no pipeline_id key)
    assert [r["id"] for r in fs.referencing_flows(pa)] == [f["id"]]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_flows_store.py -v`
Expected: FAIL (dbt node rejected: `KeyError: 'pipeline_id'` / unknown pipeline).

- [ ] **Step 3: Update `gui/flows_store.py`**

Replace the per-node loop in `validate_flow` (the `for n in nodes:` body) with kind-aware validation:

```python
    _DBT_NODE_COMMANDS = {"run", "test", "build"}
    for n in nodes:
        if not _NODE_ID_RE.match(n["node_id"]):
            raise ValueError(f"node_id {n['node_id']!r} must be letters, digits or underscore")
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
        else:
            raise ValueError(f"Node {n['node_id']}: unknown kind {kind!r}")
        for d in n.get("deps", []):
            if d not in idset:
                raise ValueError(f"Node {n['node_id']} depends on unknown node {d}")
            if d == n["node_id"]:
                raise ValueError(f"Node {n['node_id']} depends on itself")
```

(Define `_DBT_NODE_COMMANDS` at module scope instead of inside the function if you prefer; keep it consistent.)

Fix `referencing_flows` (line ~156) to not assume `pipeline_id` exists:

```python
def referencing_flows(pipeline_id: str) -> list[dict[str, Any]]:
    return [f for f in _load()
            if any(n.get("pipeline_id") == pipeline_id for n in f["nodes"])]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_flows_store.py -v`
Expected: PASS (existing + 4 new).

- [ ] **Step 5: Commit**

```bash
git add gui/flows_store.py tests/test_flows_store.py
git commit -m "feat(dbt): dbt-kind flow node validation"
```

---

### Task 11: Orchestrator builds dbt-kind nodes

**Files:**
- Modify: `orchestrator/src/orchestrator/build.py` (`_build_flow` line 20)
- Modify: `orchestrator/src/orchestrator/state.py` (add `ensure_dbt_profiles`)
- Modify: `orchestrator/src/orchestrator/assets.py` (`build_asset` — generate profiles before a dbt run)
- Test: `tests/test_orchestrator_build.py` (add a case)

**Interfaces:**
- Consumes: `state.build_argv`, `assets.build_asset` (unchanged signature), `state.ensure_dbt_profiles(spec)`.
- Produces: a dbt flow node builds an asset whose spec is `{"script":"dbt","dbt_command":...,"select":...}`; `state.ensure_dbt_profiles(spec)` writes `profiles.yml` iff `spec["script"] == "dbt"`.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_orchestrator_build.py
def test_build_all_defs_handles_dbt_node(state_dir, monkeypatch):
    import json
    import config
    import dagster as dg
    from orchestrator import build, state
    monkeypatch.setattr(state._gui_config, "PIPELINES_JSON", config.PIPELINES_JSON)
    monkeypatch.setattr(state._gui_config, "FLOWS_JSON", config.FLOWS_JSON)
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
    assert dg.AssetKey(["flow_f9", "m1"]) in keys
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_orchestrator_build.py::test_build_all_defs_handles_dbt_node -v`
Expected: FAIL (`unknown pipeline` → flow skipped → asset key absent).

- [ ] **Step 3: Update the orchestrator**

In `orchestrator/build.py`, replace the node loop inside `_build_flow` so it branches on `kind`:

```python
    for node in flow["nodes"]:
        dep_keys = [asset_mod.asset_key(flow_id, d) for d in node.get("deps", [])]
        for d in node.get("deps", []):
            if d not in node_ids:
                raise ValueError(f"flow {flow_id}: unknown dep {d}")
        kind = node.get("kind", "pipeline")
        if kind == "dbt":
            dbt = node.get("dbt") or {}
            spec = {"script": "dbt", "dbt_command": dbt.get("dbt_command", "run"),
                    "select": dbt.get("select", "")}
            name = f"dbt {spec['dbt_command']} {spec['select']}".strip()
        else:
            pid = node["pipeline_id"]
            if pid not in pipelines:
                raise ValueError(f"flow {flow_id}: unknown pipeline {pid}")
            spec = pipelines[pid]["spec"]
            name = pipelines[pid].get("name", node["node_id"])
        flow_assets.append(asset_mod.build_asset(
            flow_id, node["node_id"], name, spec, dep_keys))
```

In `orchestrator/state.py`, add after the `build_argv` re-export:

```python
def ensure_dbt_profiles(spec: dict[str, Any] | None) -> None:
    """Generate dbt/profiles.yml before a dbt asset runs (no-op otherwise)."""
    if (spec or {}).get("script") == "dbt":
        import dbt_config  # gui/ is already on sys.path (see above)
        dbt_config.write_profiles()
```

In `orchestrator/assets.py`, inside `_asset`, before building argv, ensure the profile exists:

```python
    def _asset(context: dg.AssetExecutionContext) -> MaterializeResult:
        state.ensure_dbt_profiles(spec)
        argv, label = state.build_argv(spec)
        ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_orchestrator_build.py tests/test_orchestrator_assets.py -v`
Expected: PASS (new dbt-node test + existing build/asset tests).

- [ ] **Step 5: Commit**

```bash
git add orchestrator/src/orchestrator/build.py orchestrator/src/orchestrator/state.py orchestrator/src/orchestrator/assets.py tests/test_orchestrator_build.py
git commit -m "feat(dbt): orchestrator builds dbt-kind flow nodes"
```

---

### Task 12: Flow editor UI — pick a dbt model/test as a node

**Files:**
- Modify: `gui/templates/flows.html` (`nodeRow` line 67; `addNode` line 87; `collectNodes` line 94; `renderPreview` line 101; `load` line 120)

**Interfaces:**
- Consumes: `/api/dbt/models`, `/api/dbt/tests`. Produces node objects matching Task 10's schema: pipeline nodes `{node_id, kind:"pipeline", pipeline_id, deps}`; dbt nodes `{node_id, kind:"dbt", dbt:{dbt_command, select}, deps}`.

- [ ] **Step 1: Load dbt model/test lists in `load()`**

In `load()` (after `PIPELINES = d.pipelines; FLOWS = d.flows;`), add:

```javascript
  try {
    const [m, t] = [await apiGet("/api/dbt/models"), await apiGet("/api/dbt/tests")];
    DBT_MODELS = m.models || []; DBT_TESTS = t.tests || [];
  } catch (e) { DBT_MODELS = []; DBT_TESTS = []; }
```

And declare the globals at the top (line 61):

```javascript
let PIPELINES = [], FLOWS = [], editingId = null, nodeSeq = 0, DBT_MODELS = [], DBT_TESTS = [];
```

- [ ] **Step 2: Rewrite `nodeRow`, `addNode`, `collectNodes`, `renderPreview` for kinds**

Replace `nodeRow`:

```javascript
function nodeRow(n) {
  const kind = n.kind || "pipeline";
  const pipeOpts = PIPELINES.map(p => `<option value="${p.id}" ${n.pipeline_id===p.id?"selected":""}>${esc(p.name)}</option>`).join("");
  const dbt = n.dbt || {};
  const selOpts = [
    ...DBT_MODELS.map(m => `<option value="${esc(m.name)}" ${dbt.select===m.name?"selected":""}>model: ${esc(m.name)}</option>`),
    ...DBT_TESTS.map(t => `<option value="${esc(t.name)}" ${dbt.select===t.name?"selected":""}>test: ${esc(t.name)}</option>`),
  ].join("");
  const cmdOpts = ["run","test","build"].map(c => `<option ${dbt.dbt_command===c?"selected":""}>${c}</option>`).join("");
  return `<div class="node-row row-flex" data-node="${n.node_id}" data-deps="${(n.deps || []).join(',')}" style="gap:8px;margin:6px 0;flex-wrap:wrap">
    <span class="mono">${n.node_id}</span>
    <select class="n-kind">
      <option value="pipeline" ${kind==="pipeline"?"selected":""}>pipeline</option>
      <option value="dbt" ${kind==="dbt"?"selected":""}>dbt</option>
    </select>
    <select class="n-pipe" ${kind==="dbt"?"hidden":""}>${pipeOpts}</select>
    <select class="n-dbt-cmd" ${kind==="pipeline"?"hidden":""}>${cmdOpts}</select>
    <select class="n-dbt-sel" ${kind==="pipeline"?"hidden":""}>${selOpts}</select>
    <span class="muted">depends on:</span>
    <select class="n-deps" multiple size="2"></select>
    <button class="btn sm bad" onclick="rmNode('${n.node_id}')">✕</button></div>`;
}
```

Update `addNode` to wire the kind toggle (append after the existing `querySelectorAll("select")` listener line):

```javascript
function addNode(pre) {
  nodeSeq++; const id = pre?.node_id || ("n" + nodeSeq);
  el("f-nodes").insertAdjacentHTML("beforeend", nodeRow(pre || {node_id:id, kind:"pipeline", pipeline_id:(PIPELINES[0]||{}).id, deps:[]}));
  depOptions(); renderPreview();
  const row = el("f-nodes").lastElementChild;
  row.querySelectorAll("select").forEach(s => s.addEventListener("change", () => { depOptions(); renderPreview(); }));
  const kindSel = row.querySelector(".n-kind");
  kindSel.addEventListener("change", () => {
    const isDbt = kindSel.value === "dbt";
    row.querySelector(".n-pipe").hidden = isDbt;
    row.querySelector(".n-dbt-cmd").hidden = !isDbt;
    row.querySelector(".n-dbt-sel").hidden = !isDbt;
    renderPreview();
  });
}
```

Replace `collectNodes`:

```javascript
function collectNodes() {
  return [...$$("#f-nodes .node-row")].map(r => {
    const kind = r.querySelector(".n-kind").value;
    const base = { node_id: r.dataset.node, kind,
      deps: [...r.querySelector(".n-deps").selectedOptions].map(o => o.value) };
    if (kind === "dbt") {
      base.dbt = { dbt_command: r.querySelector(".n-dbt-cmd").value,
                   select: r.querySelector(".n-dbt-sel").value };
    } else {
      base.pipeline_id = r.querySelector(".n-pipe").value;
    }
    return base;
  });
}
```

Replace `renderPreview`:

```javascript
function renderPreview() {
  const nodes = collectNodes();
  const byId = Object.fromEntries(PIPELINES.map(p => [p.id, p.name]));
  el("f-preview").textContent = nodes.map(n => {
    const what = n.kind === "dbt" ? `dbt ${n.dbt.dbt_command} ${n.dbt.select||"?"}` : (byId[n.pipeline_id]||"?");
    return `${n.node_id} (${what})` + (n.deps.length ? `  ⟵ ${n.deps.join(", ")}` : "  [root]");
  }).join("\n") || "—";
}
```

- [ ] **Step 3: Verify flows page still renders**

Run: `python -m pytest tests/test_app_runs_endpoint.py -v` and manually load `/flows`: add a node, switch its kind to **dbt**, pick a model, confirm the DAG preview shows `dbt run <model>`; save and re-edit to confirm round-trip.

- [ ] **Step 4: Commit**

```bash
git add gui/templates/flows.html
git commit -m "feat(dbt): pick a dbt model/test as a flow node in the editor"
```

---

### Task 13: Full regression + docs wrap-up

**Files:**
- Modify: `README.md` (verify the dbt section is complete; add a short "select a dbt node in a Flow" note)
- (No new code)

- [ ] **Step 1: Run the whole suite**

Run: `python -m pytest -q`
Expected: PASS (all existing tests + the new dbt tests). Investigate any failure before proceeding.

- [ ] **Step 2: Sanity-launch the app**

Run: `python gui/app.py`, open `/models` and `/flows`, confirm no console errors on load. Stop with Ctrl-C.

- [ ] **Step 3: Final commit**

```bash
git add README.md
git commit -m "docs(dbt): materialization layer usage + flow-node note"
```

---

## Self-Review

**1. Spec coverage:**

- Setup installs dbt-core (req 1) → **Task 1**.
- Models & Tests page: list + create + edit (req 2) → **Tasks 7, 8, 9**.
- Run/test/debug (req 3) → **Tasks 6, 8 (`/api/dbt/run`), 9 (buttons)**.
- Select a model/test as a flow node (req 4) → **Tasks 10 (validation), 11 (orchestrator), 12 (editor UI)**.
- All dbt/ClickHouse config in global app config (req 5) → **Tasks 3 (secrets `[clickhouse]`), 4 (`[dbt]` block), 5 (profiles.yml generation), 8 (config API + page settings panel)**.
- `icebergLocal()` operator-path constraint → surfaced in **Tasks 1 (example model + README), 7 (template), 9 (page copy)**.
- profiles.yml generated, never hand-edited; git-ignored → **Tasks 1 (.gitignore), 5, 8, 11**.

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N". Every code step shows complete code. The only deferred item is the exact dbt version pin, which is an explicit, bounded range in Global Constraints (a deliberate resolver-dependent choice, not a placeholder).

**3. Type consistency:**
- Node schema is identical across Tasks 10/11/12: pipeline `{node_id, kind, pipeline_id, deps}`; dbt `{node_id, kind:"dbt", dbt:{dbt_command, select}, deps}`.
- `dbt_config` surface (`dbt_dir`, `dbt_target`, `dbt_executable`, `render_profiles`, `write_profiles`) is defined in Task 5 and consumed with those exact names in Tasks 6, 8, 11, and `clickhouse_config.test_connection` (Task 3).
- `clickhouse_config` surface (`get_clickhouse`, `save_clickhouse`, `_raw`, `test_connection`) defined in Task 3, consumed in Tasks 5 and 8 with matching names.
- `dbt_project_store` surface (`list_models`, `list_tests`, `read_file`, `write_file`, `delete_file`, `create_from_template`, `_dbt_ls`) defined in Task 7, consumed in Task 8 and monkeypatched by the same names in tests.
- `commands.build_argv` dbt spec keys (`script`, `dbt_command`, `select`, `full_refresh`, `extra`) match what Tasks 8 and 11 construct.

**Note on one intentional spec refinement:** the design spec said `build_argv` would "ensure profiles.yml is current." The plan keeps `build_argv` **pure** (so `/api/command/preview` has no side effects) and instead calls `dbt_config.write_profiles()` at the actual execution points (`/api/dbt/run`, `/api/dbt/config` save, and the Dagster asset via `state.ensure_dbt_profiles`). Same guarantee, no preview side effects.
