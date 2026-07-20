"""Pytest shared fixtures: import paths + isolated state dir."""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "gui"))
sys.path.insert(0, str(REPO_ROOT / "orchestrator" / "src"))
sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    """Point config's JSON state at a temp dir so stores never touch real state."""
    import config

    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    monkeypatch.setattr(config, "PIPELINES_JSON", tmp_path / "pipelines.json")
    monkeypatch.setattr(config, "FLOWS_JSON", tmp_path / "flows.json")
    return tmp_path


@pytest.fixture
def pg_meta():
    """A MetaStore pointed at a throwaway schema in the test Postgres.

    Skips when OASIS_TEST_PG_DSN is unset. DSN form:
    postgresql+psycopg2://user:pass@host:5432/dbname
    """
    dsn = os.environ.get("OASIS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("OASIS_TEST_PG_DSN not set; skipping Postgres metastore test")
    from etl import config, metastore
    from sqlalchemy.engine import make_url

    url = make_url(dsn)
    schema = f"etl_meta_test_{uuid.uuid4().hex[:8]}"
    cfg = config.PostgresConfig(
        host=url.host, port=url.port or 5432, database=url.database,
        username=url.username, password=url.password, schema=schema)
    store = metastore.MetaStore(cfg)
    store.ensure_schema()
    try:
        yield store
    finally:
        with store.engine.begin() as conn:
            from sqlalchemy import text
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        store.engine.dispose()
