"""profiles.yml generation from [clickhouse] + [dbt]."""
import pytest
import yaml


@pytest.fixture
def wired(tmp_path, monkeypatch):
    import config, workspace, clickhouse_config, dbt_config
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    monkeypatch.setattr(config, "DBT_DIR", dbt_dir)
    monkeypatch.setattr(dbt_config, "DBT_DIR", dbt_dir)
    monkeypatch.setattr(dbt_config, "dbt_settings",
                        lambda: {"target": "dev", "threads": 6, "project_dir": "dbt"})
    return dbt_dir, monkeypatch, clickhouse_config, dbt_config


def test_render_requires_clickhouse(wired):
    _, mp, cc, dc = wired
    mp.setattr(cc, "_raw", lambda: {})
    with pytest.raises(ValueError, match="ClickHouse"):
        dc.render_profiles()


def test_render_shapes_profile(wired):
    _, mp, cc, dc = wired
    mp.setattr(cc, "_raw", lambda: {"host": "ch", "port": 8123, "user": "u",
                                    "password": "p", "database": "analytics",
                                    "secure": False, "connect_timeout": 10})
    prof = dc.render_profiles()
    out = prof["oasis"]["outputs"]["dev"]
    assert prof["oasis"]["target"] == "dev"
    assert out["type"] == "clickhouse" and out["schema"] == "analytics"
    assert out["password"] == "p" and out["threads"] == 6


def test_write_profiles_creates_file(wired):
    dbt_dir, mp, cc, dc = wired
    mp.setattr(cc, "_raw", lambda: {"host": "ch", "database": "d", "password": "p"})
    path = dc.write_profiles()
    assert path == dbt_dir / "profiles.yml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert loaded["oasis"]["outputs"]["dev"]["host"] == "ch"
