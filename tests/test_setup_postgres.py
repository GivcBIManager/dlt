"""Target extraction for the setup.sh/setup.ps1 Postgres provisioning step.

Pure parsing only — nothing here connects to a server.
"""
import sys
from pathlib import Path

# setup_postgres.py lives at the repo root, next to oracle_to_iceberg.py.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import setup_postgres as sp  # noqa: E402


def test_metastore_target_from_postgres_section():
    t = sp._target_from_postgres_section({"postgres": {
        "host": "db", "port": 5432, "database": "oasis_meta",
        "username": "u", "password": "p"}})
    assert (t["host"], t["port"], t["database"]) == ("db", 5432, "oasis_meta")
    assert t["schema"] == "etl_meta"          # default, mirrors PostgresConfig


def test_metastore_target_absent_section():
    assert sp._target_from_postgres_section({}) is None


def test_catalog_target_decodes_credentials():
    # A password with an '@' is percent-encoded in the SQLAlchemy URL; psycopg2
    # takes the raw value, so it has to come back out decoded.
    t = sp._target_from_catalog_uri({"iceberg_catalog": {"iceberg_catalog_config": {
        "uri": "postgresql+psycopg2://u%40corp:p%40ss@host:5433/oasis_catalog"}}})
    assert t["username"] == "u@corp"
    assert t["password"] == "p@ss"
    assert (t["host"], t["port"], t["database"]) == ("host", 5433, "oasis_catalog")
    # pyiceberg owns this database's tables — no schema DDL for us to apply.
    assert t["schema"] is None


def test_catalog_target_defaults_port():
    t = sp._target_from_catalog_uri({"iceberg_catalog": {"iceberg_catalog_config": {
        "uri": "postgresql://u:p@host/oasis_catalog"}}})
    assert t["port"] == 5432


def test_catalog_target_skips_non_postgres_backend():
    # A sqlite (or any non-Postgres) catalog is not ours to CREATE DATABASE.
    assert sp._target_from_catalog_uri({"iceberg_catalog": {
        "iceberg_catalog_config": {"uri": "sqlite:///catalog.db"}}}) is None


def test_catalog_target_absent_or_empty():
    assert sp._target_from_catalog_uri({}) is None
    assert sp._target_from_catalog_uri({"iceberg_catalog": {}}) is None
    # URI without a database path -> nothing to create.
    assert sp._target_from_catalog_uri({"iceberg_catalog": {
        "iceberg_catalog_config": {"uri": "postgresql://u:p@host:5432/"}}}) is None


def test_not_configured_exit_code(tmp_path, monkeypatch, capsys):
    """A host with no [postgres]/catalog block is 'skip' (2), not a failure."""
    secrets = tmp_path / "secrets.toml"
    secrets.write_text('[oracle_branches.x]\nhost = "h"\n')
    monkeypatch.setattr(sp, "SECRETS_TOML", secrets)
    monkeypatch.setattr(sys, "argv", ["setup_postgres.py"])
    assert sp.main() == sp.NOT_CONFIGURED

    # Missing file entirely -> same outcome.
    monkeypatch.setattr(sp, "SECRETS_TOML", tmp_path / "absent.toml")
    assert sp.main() == sp.NOT_CONFIGURED
    assert "skipping" in capsys.readouterr().out
