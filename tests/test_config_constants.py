def test_dagster_defaults(monkeypatch):
    import config
    monkeypatch.delenv("OASIS_DAGSTER_PORT", raising=False)
    monkeypatch.delenv("OASIS_DAGSTER_HOST", raising=False)
    assert config.dagster_port() == 3000
    assert config.dagster_base_url() == "http://127.0.0.1:3000"
    assert config.PIPELINES_JSON.name == "pipelines.json"
    assert config.FLOWS_JSON.name == "flows.json"


def test_dbt_paths_and_executable(monkeypatch):
    import config
    assert config.DBT_DIR.name == "dbt"
    assert config.DBT_DIR == config.REPO_ROOT / "dbt"
    assert config.DBT_PROFILES == config.DBT_DIR / "profiles.yml"
    monkeypatch.delenv("OASIS_DBT", raising=False)
    assert config.dbt_executable() == "dbt"
    monkeypatch.setenv("OASIS_DBT", "/opt/venv/bin/dbt")
    assert config.dbt_executable() == "/opt/venv/bin/dbt"
