"""dbt project file store: listing, templates, and path-traversal safety."""
import pytest


@pytest.fixture
def proj(tmp_path, monkeypatch):
    import dbt_config, dbt_project_store as ps
    d = tmp_path / "dbt"
    (d / "models").mkdir(parents=True)
    (d / "tests").mkdir(parents=True)
    (d / "models" / "stg_a.sql").write_text("select 1", encoding="utf-8")
    (d / "tests" / "assert_x.sql").write_text("select 1 where 1=0", encoding="utf-8")
    monkeypatch.setattr(dbt_config, "DBT_DIR", d)
    monkeypatch.setattr(dbt_config, "dbt_dir", lambda: d)
    # keep tests hermetic: no live `dbt ls`
    monkeypatch.setattr(ps, "_dbt_ls", lambda rt: [])
    return d


def test_list_models_from_filesystem(proj):
    import dbt_project_store as ps
    names = {m["name"] for m in ps.list_models()}
    assert "stg_a" in names


def test_list_tests_from_filesystem(proj):
    import dbt_project_store as ps
    names = {t["name"] for t in ps.list_tests()}
    assert "assert_x" in names


def test_create_and_read_model(proj):
    import dbt_project_store as ps
    out = ps.create_from_template("stg_products", "model", "table")
    assert out["path"].endswith("stg_products.sql")
    body = ps.read_file(out["path"])
    assert "icebergLocal(" in body and "materialized='table'" in body


def test_write_and_read_roundtrip(proj):
    import dbt_project_store as ps
    ps.write_file("models/stg_a.sql", "select 42")
    assert ps.read_file("models/stg_a.sql") == "select 42"


def test_path_traversal_rejected(proj):
    import dbt_project_store as ps
    with pytest.raises(ValueError):
        ps.read_file("../../etc/passwd")
    with pytest.raises(ValueError):
        ps.write_file("models/x.py", "print(1)")   # disallowed extension
    with pytest.raises(ValueError):
        ps.write_file("/abs/models/x.sql", "select 1")


def test_profiles_and_root_files_rejected(proj):
    import dbt_project_store as ps
    for op in (lambda: ps.read_file("profiles.yml"),
               lambda: ps.write_file("profiles.yml", "x"),
               lambda: ps.delete_file("profiles.yml")):
        with pytest.raises(ValueError):
            op()
    with pytest.raises(ValueError):
        ps.write_file("dbt_project.yml", "x")
    with pytest.raises(ValueError):
        ps.read_file("foo.yml")
