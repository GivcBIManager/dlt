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


def test_validate_rejects_bad_node_id():
    import flows_store as fs
    nodes = [{"node_id": "bad id!", "pipeline_id": "p", "deps": []}]
    import pytest
    with pytest.raises(ValueError):
        fs.validate_flow(nodes, known_pipeline_ids={"p"})


def test_add_flow_rejects_bad_cron(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    with pytest.raises(ValueError):
        fs.add_flow("bad", nodes, "not a cron", "UTC", {})


DBT_NODE = {"node_id": "d1", "kind": "dbt",
            "dbt": {"dbt_command": "run", "select": "stg_products"}, "deps": []}


def test_validate_accepts_dbt_node():
    import flows_store as fs
    fs.validate_flow([DBT_NODE], known_pipeline_ids=set())  # must not raise


def test_validate_rejects_dbt_without_select():
    import flows_store as fs
    bad = {"node_id": "d1", "kind": "dbt", "dbt": {"dbt_command": "run", "select": ""}, "deps": []}
    with pytest.raises(ValueError, match="select"):
        fs.validate_flow([bad], known_pipeline_ids=set())


def test_validate_rejects_dbt_bad_command():
    import flows_store as fs
    bad = {"node_id": "d1", "kind": "dbt", "dbt": {"dbt_command": "debug", "select": "m"}, "deps": []}
    with pytest.raises(ValueError, match="dbt command"):
        fs.validate_flow([bad], known_pipeline_ids=set())


def test_mixed_flow_and_referencing(state_dir):
    import flows_store as fs
    import pipelines_store as ps
    pa = ps.add_pipeline("a", {"script": "dq_check"})["id"]
    nodes = [{"node_id": "p1", "pipeline_id": pa, "deps": []},
             {"node_id": "d1", "kind": "dbt", "dbt": {"dbt_command": "run", "select": "m"}, "deps": ["p1"]}]
    f = fs.add_flow("mixed", nodes, "0 2 * * *", "UTC", {})
    # dbt node must not blow up referencing_flows (no pipeline_id key)
    assert [r["id"] for r in fs.referencing_flows(pa)] == [f["id"]]


def test_validate_accepts_command_node():
    import flows_store as fs
    node = {"node_id": "c1", "kind": "command", "command": "python tools/x.py", "deps": []}
    fs.validate_flow([node], known_pipeline_ids=set())  # must not raise


def test_validate_rejects_command_without_command():
    import flows_store as fs
    bad = {"node_id": "c1", "kind": "command", "command": "   ", "deps": []}
    with pytest.raises(ValueError, match="command"):
        fs.validate_flow([bad], known_pipeline_ids=set())


def test_add_flow_rejects_bad_timezone(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    with pytest.raises(ValueError, match="[Tt]imezone"):
        fs.add_flow("tzbad", nodes, "0 2 * * *", "Not/AZone", {})


def test_add_flow_accepts_valid_tz_and_stores_graph(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    graph = {"drawflow": {"Home": {"data": {"1": {"id": 1, "name": "pipeline"}}}}}
    f = fs.add_flow("tzok", nodes, "0 2 * * *", "Asia/Riyadh", {}, graph=graph)
    got = fs.get_flow(f["id"])
    assert got["timezone"] == "Asia/Riyadh"
    assert got["graph"] == graph


def test_update_flow_persists_graph(state_dir):
    import flows_store as fs
    pa, _ = _seed_pipelines(state_dir)
    nodes = [{"node_id": "n1", "pipeline_id": pa, "deps": []}]
    f = fs.add_flow("upd", nodes, "0 2 * * *", "UTC", {})
    g = {"drawflow": {"Home": {"data": {}}}}
    fs.update_flow(f["id"], graph=g)
    assert fs.get_flow(f["id"])["graph"] == g
