def test_dagster_defaults(monkeypatch):
    import config
    monkeypatch.delenv("OASIS_DAGSTER_PORT", raising=False)
    monkeypatch.delenv("OASIS_DAGSTER_HOST", raising=False)
    assert config.dagster_port() == 3000
    assert config.dagster_base_url() == "http://127.0.0.1:3000"
    assert config.PIPELINES_JSON.name == "pipelines.json"
    assert config.FLOWS_JSON.name == "flows.json"
