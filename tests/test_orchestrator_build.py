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


def _wire(monkeypatch):
    import config
    from orchestrator import state
    monkeypatch.setattr(state._gui_config, "PIPELINES_JSON", config.PIPELINES_JSON)
    monkeypatch.setattr(state._gui_config, "FLOWS_JSON", config.FLOWS_JSON)


def test_build_all_defs_uses_readable_names(state_dir, monkeypatch):
    from orchestrator import build
    _wire(monkeypatch)
    _seed(state_dir)

    defs = build.build_all_defs()
    keys = {a.key for a in defs.resolve_all_asset_specs()}
    assert dg.AssetKey(["nightly__f1", "n1"]) in keys
    assert dg.AssetKey(["nightly__f1", "n2"]) in keys
    spec_n2 = next(a for a in defs.resolve_all_asset_specs()
                   if a.key == dg.AssetKey(["nightly__f1", "n2"]))
    assert dg.AssetKey(["nightly__f1", "n1"]) in {d.asset_key for d in spec_n2.deps}
    assert spec_n2.group_name == "nightly"
    assert defs.get_schedule_def("flow_nightly__f1_schedule").cron_schedule == "0 2 * * *"
    assert defs.get_job_def("flow_nightly__f1") is not None


def test_build_all_defs_handles_dbt_node(state_dir, monkeypatch):
    from orchestrator import build
    _wire(monkeypatch)
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
    assert dg.AssetKey(["materialize__f9", "m1"]) in keys


def test_build_all_defs_handles_command_node(state_dir, monkeypatch):
    from orchestrator import build
    _wire(monkeypatch)
    (state_dir / "pipelines.json").write_text("[]")
    (state_dir / "flows.json").write_text(json.dumps([{
        "id": "f7", "name": "notify",
        "nodes": [{"node_id": "c1", "kind": "command",
                   "command": "python tools/notify.py", "deps": []}],
        "cron": "0 4 * * *", "timezone": "UTC",
        "email": {"on_success": [], "on_failure": []}, "enabled": True,
    }]))
    defs = build.build_all_defs()
    keys = {a.key for a in defs.resolve_all_asset_specs()}
    assert dg.AssetKey(["notify__f7", "c1"]) in keys
    assert defs.get_job_def("flow_notify__f7") is not None
