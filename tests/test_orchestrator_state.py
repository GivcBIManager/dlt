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
