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
