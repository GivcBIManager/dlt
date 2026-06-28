import pytest


def _seed_pipelines(state_dir):
    import pipelines_store as ps
    a = ps.add_pipeline("a", {"script": "dq_check"})
    b = ps.add_pipeline("b", {"script": "dq_check"})
    return a["id"], b["id"]


def test_validate_rejects_cycle():
    import flows_store as fs
    nodes = [
        {"node_id": "n1", "pipeline_id": "p", "deps": ["n2"]},
        {"node_id": "n2", "pipeline_id": "p", "deps": ["n1"]},
    ]
    with pytest.raises(ValueError, match="cycle"):
        fs.validate_flow(nodes, known_pipeline_ids={"p"})


def test_validate_rejects_unknown_dep():
    import flows_store as fs
    nodes = [{"node_id": "n1", "pipeline_id": "p", "deps": ["ghost"]}]
    with pytest.raises(ValueError):
        fs.validate_flow(nodes, known_pipeline_ids={"p"})


def test_add_flow_and_reference(state_dir):
    import flows_store as fs
    pa, pb = _seed_pipelines(state_dir)
    nodes = [
        {"node_id": "n1", "pipeline_id": pa, "deps": []},
        {"node_id": "n2", "pipeline_id": pb, "deps": ["n1"]},
    ]
    f = fs.add_flow("nightly", nodes, "0 2 * * *", "Asia/Riyadh",
                    {"on_success": ["x@y.com"], "on_failure": ["x@y.com"]})
    assert f["id"] and f["enabled"] is True
    assert [r["id"] for r in fs.referencing_flows(pa)] == [f["id"]]


def test_add_flow_rejects_bad_cron(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    with pytest.raises(ValueError):
        fs.add_flow("bad", nodes, "not a cron", "UTC", {})
