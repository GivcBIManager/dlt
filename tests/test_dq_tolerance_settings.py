"""Config + GUI plumbing for the DQ hash-delta tolerance setting."""
from __future__ import annotations

from etl import config
from etl.config import Settings


def test_settings_has_default_tolerance():
    assert Settings().dq_hash_delta_tolerance_pct == 10.0


def test_load_settings_override_tolerance():
    s = config.load_settings({"dq_hash_delta_tolerance_pct": 5.0})
    assert s.dq_hash_delta_tolerance_pct == 5.0


def test_load_settings_reads_etl_key(monkeypatch):
    orig = config._cfg
    monkeypatch.setattr(
        config, "_cfg",
        lambda key, default: 7.5 if key == "etl.dq_hash_delta_tolerance_pct" else orig(key, default),
    )
    s = config.load_settings()
    assert s.dq_hash_delta_tolerance_pct == 7.5


def test_tolerance_key_is_editable(tmp_path, monkeypatch):
    import workspace
    cfg = tmp_path / "config.toml"
    cfg.write_text("[etl]\ndq_hash_delta_tolerance_pct = 10.0\n", encoding="utf-8")
    monkeypatch.setattr(workspace, "CONFIG_TOML", cfg)
    monkeypatch.setattr(workspace, "STATE_DIR", tmp_path)
    assert "dq_hash_delta_tolerance_pct" in workspace.EDITABLE_ETL_KEYS
    res = workspace.update_etl_settings({"dq_hash_delta_tolerance_pct": 5.0})
    assert res["applied"]["dq_hash_delta_tolerance_pct"] == 5.0
    assert "dq_hash_delta_tolerance_pct = 5.0" in cfg.read_text(encoding="utf-8")
