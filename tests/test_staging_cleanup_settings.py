"""Config + CLI plumbing for the staged-parquet cleanup setting."""
from __future__ import annotations

from etl import config
from etl.config import Settings


def test_settings_defaults_cleanup_on():
    assert Settings().cleanup_staging_after_load is True


def test_load_settings_override_cleanup():
    s = config.load_settings({"cleanup_staging_after_load": False})
    assert s.cleanup_staging_after_load is False


def test_load_settings_reads_etl_key(monkeypatch):
    orig = config._cfg
    monkeypatch.setattr(
        config, "_cfg",
        lambda key, default: False if key == "etl.cleanup_staging_after_load" else orig(key, default),
    )
    s = config.load_settings()
    assert s.cleanup_staging_after_load is False
