def test_launch_argv_and_yaml(tmp_path, monkeypatch):
    import config
    import dagster_service as dsv
    monkeypatch.setattr(config, "DAGSTER_HOME", tmp_path / ".dagster_home")
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3001")

    svc = dsv.DagsterService()
    argv = svc.launch_argv()
    assert "-m" in argv and "dagster" in argv and "dev" in argv
    # Code locations come from workspace.yaml now (the orchestrator plus the
    # Fusion pipeline), not from a single -m module on the command line.
    assert "-w" in argv and argv[argv.index("-w") + 1].endswith("workspace.yaml")
    assert "3001" in argv

    home = svc.ensure_home()
    assert (home / "dagster.yaml").exists()
    text = (home / "dagster.yaml").read_text()
    # Both run-concurrency settings belong under `concurrency > runs`. A
    # top-level `run_queue:` carrying them alongside makes Dagster 1.13 refuse
    # to load the instance at all, so the generated file must not use it.
    assert "concurrency:" in text and "max_concurrent_runs" in text
    assert "run_queue" not in text


def test_status_when_not_started(monkeypatch, tmp_path):
    import config
    import dagster_service as dsv
    monkeypatch.setattr(config, "DAGSTER_HOME", tmp_path / ".dagster_home")
    svc = dsv.DagsterService()
    st = svc.status()
    assert st["running"] is False and st["url"].startswith("http://")
