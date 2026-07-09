def test_dagster_defaults(monkeypatch):
    import config
    monkeypatch.delenv("OASIS_DAGSTER_PORT", raising=False)
    monkeypatch.delenv("OASIS_DAGSTER_HOST", raising=False)
    assert config.dagster_port() == 3000
    assert config.dagster_base_url() == "http://127.0.0.1:3000"
    assert config.PIPELINES_JSON.name == "pipelines.json"
    assert config.FLOWS_JSON.name == "flows.json"


def test_dbt_paths_and_executable(tmp_path, monkeypatch):
    import sys

    import config
    assert config.DBT_DIR.name == "dbt"
    assert config.DBT_DIR == config.REPO_ROOT / "dbt"
    assert config.DBT_PROFILES == config.DBT_DIR / "profiles.yml"
    # No env override and no dbt launcher next to the interpreter -> bare "dbt".
    monkeypatch.delenv("OASIS_DBT", raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "python"))
    assert config.dbt_executable() == "dbt"
    # Explicit override always wins.
    monkeypatch.setenv("OASIS_DBT", "/opt/venv/bin/dbt")
    assert config.dbt_executable() == "/opt/venv/bin/dbt"


def test_dbt_executable_resolves_next_to_interpreter(tmp_path, monkeypatch):
    """When dbt is installed next to the running interpreter (the venv's
    Scripts/bin dir), use that absolute path -- so the GUI finds dbt even when
    launched via the venv python without the venv being 'activated'."""
    import os
    import sys

    import config
    monkeypatch.delenv("OASIS_DBT", raising=False)
    scripts = tmp_path / "Scripts"
    scripts.mkdir()
    launcher = scripts / ("dbt.exe" if os.name == "nt" else "dbt")
    launcher.write_text("")
    monkeypatch.setattr(sys, "executable", str(scripts / "python"))
    assert config.dbt_executable() == str(launcher)
