"""Smoke tests for the dbt API routes (via Flask test client)."""
import pytest


@pytest.fixture
def client(monkeypatch):
    import app as gui_app
    monkeypatch.setattr(gui_app.dbt_project_store, "list_models",
                        lambda: [{"name": "stg_a", "path": "models/stg_a.sql", "resource_type": "model"}])
    monkeypatch.setattr(gui_app.dbt_project_store, "list_tests", lambda: [])
    monkeypatch.setattr(gui_app.clickhouse_config, "get_clickhouse",
                        lambda: {"host": "ch", "port": 8123, "has_password": True})
    monkeypatch.setattr(gui_app.workspace, "dbt_settings", lambda: {"target": "dev"})
    return gui_app.app.test_client()


def test_models_route(client):
    r = client.get("/api/dbt/models")
    assert r.status_code == 200
    assert r.get_json()["models"][0]["name"] == "stg_a"


def test_config_route_redacts(client):
    r = client.get("/api/dbt/config")
    body = r.get_json()
    assert body["clickhouse"]["has_password"] is True
    assert "password" not in body["clickhouse"]
    assert body["dbt"]["target"] == "dev"


def test_models_page_renders(client):
    assert client.get("/models").status_code == 200


def test_template_route_renders_materialization(client):
    r = client.get("/api/dbt/template?kind=model&materialization=view&name=foo")
    assert r.status_code == 200
    assert "materialized='view'" in r.get_json()["content"]


def test_file_create_forwards_content(client, monkeypatch):
    import app as gui_app
    captured = {}

    def fake_create(name, kind, materialization="table", content=None, layer="staging"):
        captured.update(name=name, kind=kind, materialization=materialization,
                        content=content, layer=layer)
        return {"path": f"models/{layer}/{name}.sql"}

    monkeypatch.setattr(gui_app.dbt_project_store, "create_from_template", fake_create)
    r = client.post("/api/dbt/file", json={
        "name": "hand", "kind": "model", "materialization": "table",
        "content": "select 1", "layer": "marts"})
    assert r.status_code == 200
    assert captured["content"] == "select 1"
    assert captured["materialization"] == "table"
    assert captured["layer"] == "marts"


# --- dbt docs site (lineage) ------------------------------------------------ #
# /dbt-docs/ serves the static bundle `dbt docs generate` writes into target/.
# target/ also holds compiled SQL and run artifacts, so only the three docs
# files may be reachable.

@pytest.fixture
def docs_dir(tmp_path, monkeypatch):
    import app as gui_app
    d = tmp_path / "dbt"
    (d / "target").mkdir(parents=True)
    monkeypatch.setattr(gui_app.dbt_config, "dbt_dir", lambda: d)
    return d


def test_docs_index_is_served(client, docs_dir):
    (docs_dir / "target" / "index.html").write_text("<html>docs</html>", encoding="utf-8")
    r = client.get("/dbt-docs/")
    assert r.status_code == 200
    assert b"docs" in r.data
    assert r.mimetype == "text/html"


def test_docs_non_bundle_files_are_not_served(client, docs_dir):
    # run_results.json exists but is not part of the docs bundle.
    (docs_dir / "target" / "run_results.json").write_text("{}", encoding="utf-8")
    assert client.get("/dbt-docs/run_results.json").status_code == 404


def test_docs_missing_bundle_explains_itself(client, docs_dir):
    r = client.get("/dbt-docs/")
    assert r.status_code == 404
    assert "generated" in r.get_json()["error"]


def test_docs_status_reports_absence(client, docs_dir):
    assert client.get("/api/dbt/docs/status").get_json() == {"exists": False}


def test_docs_status_carries_project_name(client, docs_dir):
    # The Lineage subtab builds model.<project>.<name> deep links from this.
    (docs_dir / "target" / "index.html").write_text("<html></html>", encoding="utf-8")
    (docs_dir / "dbt_project.yml").write_text("name: 'oasis'\n", encoding="utf-8")
    body = client.get("/api/dbt/docs/status").get_json()
    assert body["exists"] is True and body["project"] == "oasis"


def test_lineage_page_redirects_to_models_subtab(client):
    r = client.get("/lineage")
    assert r.status_code == 302
    assert r.headers["Location"].endswith("/models#lineage")


def test_docs_status_dates_the_site_by_its_catalog(client, docs_dir):
    # dbt ls / run rewrite manifest.json all the time; only `docs generate`
    # writes catalog.json, so that is when the docs were generated.
    import os
    from datetime import datetime
    t = docs_dir / "target"
    for name, ts in (("index.html", 1_700_000_000), ("catalog.json", 1_700_000_500),
                     ("manifest.json", 1_800_000_000)):
        (t / name).write_text("{}", encoding="utf-8")
        os.utime(t / name, (ts, ts))
    body = client.get("/api/dbt/docs/status").get_json()
    assert body["generated"] == datetime.fromtimestamp(1_700_000_500).isoformat(timespec="seconds")
