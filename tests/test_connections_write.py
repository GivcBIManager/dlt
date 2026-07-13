"""Round-trip coverage for the connections secrets writer (via toml_edit)."""
from __future__ import annotations

import pytest


@pytest.fixture
def secrets(tmp_path, monkeypatch):
    import config
    import connections
    sec = tmp_path / "secrets.toml"
    monkeypatch.setattr(config, "SECRETS_TOML", sec)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    monkeypatch.setattr(connections, "SECRETS_TOML", sec)
    monkeypatch.setattr(connections, "STATE_DIR", tmp_path)
    return sec


def _payload(key="alpha"):
    return {"key": key, "name": "Alpha", "host": "db.example", "port": 1521,
            "username": "svc", "password": "s3cret", "database": "ORCL"}


def test_add_connection_writes_and_reads_back(secrets):
    import connections
    connections.add_connection(_payload())
    assert secrets.exists()
    got = connections.get_connection("alpha")
    assert got["host"] == "db.example" and got["username"] == "svc"
    # the written file is valid TOML with the branch section
    text = secrets.read_text(encoding="utf-8")
    assert "[oracle_branches.alpha]" in text


def test_add_two_then_delete_one(secrets):
    import connections
    connections.add_connection(_payload("alpha"))
    connections.add_connection(_payload("beta"))
    keys = {c["key"] for c in connections.list_connections()}
    assert {"alpha", "beta"} <= keys
    connections.delete_connection("alpha")
    keys = {c["key"] for c in connections.list_connections()}
    assert "alpha" not in keys and "beta" in keys


def test_corrupt_block_is_rejected_before_replacing(secrets, monkeypatch):
    import connections
    connections.add_connection(_payload("alpha"))
    before = secrets.read_text(encoding="utf-8")
    # Force an invalid emitted block; the write must refuse and keep the original.
    monkeypatch.setattr(connections, "_emit_block", lambda key, data: ["not = = valid"])
    with pytest.raises(ValueError):
        connections.add_connection(_payload("beta"))
    assert secrets.read_text(encoding="utf-8") == before
