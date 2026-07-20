from etl import config


def test_load_postgres_config_parses_section():
    raw = {"host": "db", "port": 5432, "database": "oasis_meta",
           "username": "u", "password": "p"}
    pg = config.load_postgres_config(raw)
    assert pg.host == "db"
    assert pg.port == 5432
    assert pg.schema == "etl_meta"
    assert pg.sqlalchemy_url() == "postgresql+psycopg2://u:p@db:5432/oasis_meta"


def test_load_postgres_config_none_when_absent():
    assert config.load_postgres_config(None) is None
    assert config.load_postgres_config({}) is None
