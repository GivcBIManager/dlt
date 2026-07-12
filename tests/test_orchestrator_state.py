import json

import sys


def test_ensure_dbt_profiles_survives_step_worker_sys_path_reset(monkeypatch):
    """Regression: Dagster's multiprocess step worker resets sys.path and drops
    the runtime-inserted gui/ entry *before* an asset runs. ensure_dbt_profiles
    must therefore reach dbt_config through a load-time binding, not a deferred
    ``import dbt_config`` at asset-runtime -- which failed there with
    ``ModuleNotFoundError: No module named 'dbt_config'`` while the pipeline
    nodes (using already-bound build_argv) succeeded.
    """
    from orchestrator import state

    # Stub the profile write so the test needs no ClickHouse config; this is the
    # same module object state binds as ``_dbt_config``.
    import dbt_config
    monkeypatch.setattr(dbt_config, "write_profiles", lambda: None)

    # Reproduce the step-worker environment exactly as observed: gui/ absent from
    # sys.path and dbt_config no longer freshly importable from it.
    gui = str(state._gui_dir)
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != gui])
    monkeypatch.delitem(sys.modules, "dbt_config", raising=False)

    # Under the old deferred import this raised ModuleNotFoundError; the load-time
    # binding resolves without touching sys.path.
    state.ensure_dbt_profiles({"script": "dbt"})


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
