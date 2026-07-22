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


import oracle_to_iceberg as o2i


def test_keep_staging_flag_disables_cleanup():
    args = o2i.parse_args(["--keep-staging"])
    assert o2i.build_overrides(args)["cleanup_staging_after_load"] is False


def test_no_keep_staging_flag_leaves_default():
    args = o2i.parse_args([])
    # Absent from overrides -> the config default (on) is used.
    assert "cleanup_staging_after_load" not in o2i.build_overrides(args)
