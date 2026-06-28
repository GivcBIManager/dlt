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
    keys = {a.key for a in defs.resolve_all_asset_specs()}
    assert dg.AssetKey(["flow_f1", "n1"]) in keys
    assert dg.AssetKey(["flow_f1", "n2"]) in keys
    # n2 depends on n1
    spec_n2 = next(a for a in defs.resolve_all_asset_specs()
                   if a.key == dg.AssetKey(["flow_f1", "n2"]))
    assert dg.AssetKey(["flow_f1", "n1"]) in {d.asset_key for d in spec_n2.deps}
    assert defs.get_schedule_def("flow_f1_schedule").cron_schedule == "0 2 * * *"
