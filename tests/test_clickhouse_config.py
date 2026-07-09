"""ClickHouse credential store: defaults, redaction, password preservation."""
import pytest


@pytest.fixture
def secrets(tmp_path, monkeypatch):
    import config
    p = tmp_path / "secrets.toml"
    p.write_text('[oracle_branches.jazan]\nhost = "x"\n', encoding="utf-8")
    monkeypatch.setattr(config, "SECRETS_TOML", p)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    import clickhouse_config
    monkeypatch.setattr(clickhouse_config, "SECRETS_TOML", p)
    monkeypatch.setattr(clickhouse_config, "STATE_DIR", tmp_path)
    return p


def test_defaults_when_absent(secrets):
    import clickhouse_config as cc
    got = cc.get_clickhouse()
    assert got["port"] == 8123 and got["user"] == "default"
    assert got["secure"] is False and got["has_password"] is False
    assert "password" not in got


def test_save_and_redact(secrets):
    import clickhouse_config as cc
    out = cc.save_clickhouse({"host": "ch", "port": 9000, "user": "u",
                              "password": "sekret", "database": "analytics",
                              "secure": True})
    assert out["host"] == "ch" and out["has_password"] is True
    assert "password" not in out
    assert cc._raw()["password"] == "sekret"
    # Oracle section untouched
    assert "[oracle_branches.jazan]" in secrets.read_text(encoding="utf-8")


def test_blank_password_preserves_existing(secrets):
    import clickhouse_config as cc
    cc.save_clickhouse({"host": "ch", "password": "keepme", "database": "d"})
    cc.save_clickhouse({"host": "ch2", "password": "", "database": "d"})
    assert cc._raw()["password"] == "keepme"
    assert cc._raw()["host"] == "ch2"
