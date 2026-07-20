from etl import config


def test_load_postgres_config_parses_section():
    raw = {"host": "db", "port": 5432, "database": "oasis_meta",
           "username": "u", "password": "p"}
    pg = config.load_postgres_config(raw)
    assert pg.host == "db"
    assert pg.port == 5432
    assert pg.schema == "etl_meta"
    assert pg.sqlalchemy_url() == "postgresql+psycopg2://u:p@db:5432/oasis_meta"


def test_load_postgres_config_none_when_absent(monkeypatch):
    # Explicit empty dict -> None (pure, no ambient dependency).
    assert config.load_postgres_config({}) is None
    # The None-arg path reads ambient dlt.secrets; force it empty so this test
    # does not depend on the host's .dlt/secrets.toml (which has a [postgres]
    # section on configured hosts).
    import dlt
    monkeypatch.setattr(dlt.secrets, "get", lambda *a, **k: None)
    assert config.load_postgres_config(None) is None
