"""Auth gate + CSRF content-type checks on the Flask app."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_GUI_TOKEN", raising=False)
    monkeypatch.delenv("OASIS_ALLOW_CUSTOM_CMD", raising=False)


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


REMOTE = {"REMOTE_ADDR": "10.0.0.5"}  # simulate a non-loopback client


# --- auth ------------------------------------------------------------------ #
def test_loopback_allowed_without_token(client):
    # test_client defaults to REMOTE_ADDR=127.0.0.1
    resp = client.get("/api/runs")
    assert resp.status_code == 200


def test_remote_blocked_without_token_when_token_configured(client, monkeypatch):
    monkeypatch.setenv("OASIS_GUI_TOKEN", "s3cret")
    resp = client.get("/api/runs", environ_base=REMOTE)
    assert resp.status_code == 401


def test_remote_allowed_with_correct_header_token(client, monkeypatch):
    monkeypatch.setenv("OASIS_GUI_TOKEN", "s3cret")
    resp = client.get("/api/runs", environ_base=REMOTE,
                      headers={"X-Auth-Token": "s3cret"})
    assert resp.status_code == 200


def test_remote_blocked_with_wrong_token(client, monkeypatch):
    monkeypatch.setenv("OASIS_GUI_TOKEN", "s3cret")
    resp = client.get("/api/runs", environ_base=REMOTE,
                      headers={"X-Auth-Token": "nope"})
    assert resp.status_code == 401


def test_healthz_exempt_from_auth(client, monkeypatch):
    monkeypatch.setenv("OASIS_GUI_TOKEN", "s3cret")
    resp = client.get("/healthz", environ_base=REMOTE)
    assert resp.status_code == 200


def test_query_token_sets_cookie(client, monkeypatch):
    monkeypatch.setenv("OASIS_GUI_TOKEN", "s3cret")
    resp = client.get("/healthz?token=s3cret", environ_base=REMOTE)
    assert resp.status_code == 200
    cookie = resp.headers.get("Set-Cookie", "")
    assert "oasis_token=s3cret" in cookie


# --- CSRF: mutating requests must be JSON (blocks cross-site form posts) ---- #
def test_json_post_allowed(client):
    resp = client.post("/api/command/preview", json={"script": "oracle_to_iceberg"})
    assert resp.status_code == 200


def test_form_post_rejected(client):
    resp = client.post("/api/command/preview", data={"script": "oracle_to_iceberg"},
                       content_type="application/x-www-form-urlencoded")
    assert resp.status_code == 415


def test_get_not_subject_to_csrf(client):
    resp = client.get("/api/runs")
    assert resp.status_code == 200
