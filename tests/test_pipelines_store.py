import pytest


def test_add_and_get_pipeline(state_dir):
    import pipelines_store as ps
    spec = {"script": "oracle_to_iceberg", "mode": "INCREMENTAL",
            "category": "masters", "tables": "PATIENT_MASTER_DATA"}
    p = ps.add_pipeline("masters-incr", spec)
    assert p["id"] and p["name"] == "masters-incr"
    assert "oracle_to_iceberg.py" in p["command"]
    assert ps.get_pipeline(p["id"])["name"] == "masters-incr"
    assert len(ps.load_pipelines()) == 1


def test_add_rejects_bad_spec(state_dir):
    import pipelines_store as ps
    with pytest.raises(ValueError):
        ps.add_pipeline("bad", {"script": "custom", "custom": ""})


def test_add_auto_names_when_blank(state_dir):
    import pipelines_store as ps
    spec = {"script": "dq_check"}
    p1 = ps.add_pipeline("", spec)
    p2 = ps.add_pipeline("", spec)  # same label -> deduped
    assert p1["name"]  # non-empty auto name derived from the command label
    assert p2["name"] != p1["name"]
    assert p2["name"].endswith("(2)")
    assert len(ps.load_pipelines()) == 2


def test_update_and_delete(state_dir):
    import pipelines_store as ps
    p = ps.add_pipeline("a", {"script": "dq_check"})
    ps.update_pipeline(p["id"], name="renamed")
    assert ps.get_pipeline(p["id"])["name"] == "renamed"
    assert ps.delete_pipeline(p["id"]) is True
    assert ps.load_pipelines() == []
