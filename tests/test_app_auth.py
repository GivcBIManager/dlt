"""Auth gate (login sessions) + CSRF content-type checks on the Flask app."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_GUI_USER", raising=False)
    monkeypatch.delenv("OASIS_GUI_PASSWORD", raising=False)
    monkeypatch.delenv("OASIS_ALLOW_CUSTOM_CMD", raising=False)


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


REMOTE = {"REMOTE_ADDR": "10.0.0.5"}  # simulate a non-loopback client


def _set_creds(monkeypatch):
    monkeypatch.setenv("OASIS_GUI_USER", "admin")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "s3cret")


def _login(client, username="admin", password="s3cret"):
    return client.post("/login", data={"username": username, "password": password},
                       environ_base=REMOTE)


# --- auth ------------------------------------------------------------------ #
def test_loopback_allowed_without_login(client):
    # test_client defaults to REMOTE_ADDR=127.0.0.1
    resp = client.get("/api/runs")
    assert resp.status_code == 200


def test_remote_api_blocked_without_login(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/api/runs", environ_base=REMOTE)
    assert resp.status_code == 401


def test_remote_page_redirects_to_login(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/", environ_base=REMOTE,
                      headers={"Accept": "text/html"})
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_login_page_renders_for_remote(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/login", environ_base=REMOTE)
    assert resp.status_code == 200


def test_login_wrong_credentials_rejected(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = _login(client, password="wrong")
    assert resp.status_code == 401
    # still unauthorized afterwards
    assert client.get("/api/runs", environ_base=REMOTE).status_code == 401


def test_login_success_grants_session(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = _login(client)
    assert resp.status_code == 302
    assert client.get("/api/runs", environ_base=REMOTE).status_code == 200


def test_login_rejected_when_no_credentials_configured(client):
    # No env vars set: nobody can log in remotely (loopback-only deployment).
    resp = _login(client)
    assert resp.status_code == 401


def test_logout_clears_session(client, monkeypatch):
    _set_creds(monkeypatch)
    _login(client)
    resp = client.post("/logout", environ_base=REMOTE)
    assert resp.status_code == 302
    assert client.get("/api/runs", environ_base=REMOTE).status_code == 401


# --- the old token paths must be dead -------------------------------------- #
def test_query_token_has_no_effect(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/api/runs?token=s3cret", environ_base=REMOTE)
    assert resp.status_code == 401
    resp = client.get("/api/runs?token=admin", environ_base=REMOTE)
    assert resp.status_code == 401


def test_header_token_has_no_effect(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/api/runs", environ_base=REMOTE,
                      headers={"X-Auth-Token": "s3cret"})
    assert resp.status_code == 401
    resp = client.get("/api/runs", environ_base=REMOTE,
                      headers={"Authorization": "Bearer s3cret"})
    assert resp.status_code == 401


def test_old_cookie_has_no_effect(client, monkeypatch):
    _set_creds(monkeypatch)
    client.set_cookie("oasis_token", "s3cret")
    resp = client.get("/api/runs", environ_base=REMOTE)
    assert resp.status_code == 401


def test_healthz_exempt_from_auth(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/healthz", environ_base=REMOTE)
    assert resp.status_code == 200


# --- CSRF: mutating requests must be JSON (blocks cross-site form posts) ---- #
def test_json_post_allowed(client):
    resp = client.post("/api/command/preview", json={"script": "oracle_to_iceberg"})
    assert resp.status_code == 200


def test_form_post_rejected(client):
    resp = client.post("/api/command/preview", data={"script": "oracle_to_iceberg"},
                       content_type="application/x-www-form-urlencoded")
    assert resp.status_code == 415


def test_login_form_post_not_subject_to_json_rule(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = _login(client)
    assert resp.status_code in (302, 401)  # never 415


def test_get_not_subject_to_csrf(client):
    resp = client.get("/api/runs")
    assert resp.status_code == 200
