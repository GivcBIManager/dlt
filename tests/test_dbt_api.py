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
