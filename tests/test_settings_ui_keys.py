"""Every runtime-tunable [etl] key is manageable from the Settings page.

The GUI's edit path requires three things per key: presence in the config.toml
[etl] block (update_etl_settings only rewrites existing lines), membership in
workspace.EDITABLE_ETL_KEYS (server allowlist), and membership in the page's
SET_EDITABLE set (client). Host-specific keys (thick_mode,
oracle_client_lib_dir) are deliberately view-only.
"""
from __future__ import annotations

from pathlib import Path

import workspace

SETTINGS_HTML = (Path(__file__).resolve().parents[1]
                 / "gui" / "templates" / "settings.html").read_text(encoding="utf-8")

# Keys the UI must be able to edit (superset of the historical allowlist).
UI_EDITABLE = {
    "dataset_name", "pipeline_name", "max_branch_workers", "max_table_workers",
    "pool_min", "pool_max", "pool_increment", "pool_acquire_timeout_s",
    "pool_acquire_attempts", "pool_backoff_base_s", "pool_backoff_cap_s",
    "max_retries", "retry_interval_s", "dsn_mode",
    "snapshot_maintenance", "snapshot_expire_days", "snapshot_min_to_keep",
    "progress_enabled", "progress_interval_s",
    "load_batch_rows", "load_group_max_bytes", "load_commit_timeout_s",
    "load_workers",
    "cleanup_staging_after_load", "dq_hash_delta_tolerance_pct",
}

# Host-specific: shown on the page but locked (edit the file directly).
UI_LOCKED = {"thick_mode", "oracle_client_lib_dir"}


def test_editable_allowlist_covers_ui_keys():
    assert UI_EDITABLE <= workspace.EDITABLE_ETL_KEYS
    assert not (UI_LOCKED & workspace.EDITABLE_ETL_KEYS)


def test_settings_page_declares_every_editable_key():
    for key in UI_EDITABLE:
        assert f'"{key}"' in SETTINGS_HTML, f"{key} missing from settings.html"


def test_settings_page_curates_new_load_keys():
    # New keys must have curated rows (k: "...") -- not fall into "Other".
    for key in ("load_group_max_bytes", "load_batch_rows", "load_commit_timeout_s",
                "load_workers",
                "cleanup_staging_after_load", "progress_enabled",
                "progress_interval_s", "pool_backoff_base_s", "pool_backoff_cap_s"):
        assert f'k: "{key}"' in SETTINGS_HTML, f"{key} has no curated row"


def _cfg_file(tmp_path, body: str) -> Path:
    cfg = tmp_path / "config.toml"
    cfg.write_text(f"[etl]\n{body}", encoding="utf-8")
    return cfg


def test_new_numeric_keys_round_trip(tmp_path, monkeypatch):
    cfg = _cfg_file(tmp_path, "load_group_max_bytes = 268435456\n"
                              "load_batch_rows = 50000\n"
                              "load_commit_timeout_s = 900\n")
    monkeypatch.setattr(workspace, "CONFIG_TOML", cfg)
    monkeypatch.setattr(workspace, "STATE_DIR", tmp_path)
    res = workspace.update_etl_settings({
        "load_group_max_bytes": 134217728,
        "load_batch_rows": 100000,
        "load_commit_timeout_s": 600,
    })
    assert res["applied"] == {"load_group_max_bytes": 134217728,
                             "load_batch_rows": 100000,
                             "load_commit_timeout_s": 600}
    text = cfg.read_text(encoding="utf-8")
    assert "load_group_max_bytes = 134217728" in text
    assert "load_batch_rows = 100000" in text
    assert "load_commit_timeout_s = 600" in text


def test_boolean_keys_round_trip_as_bare_toml_bools(tmp_path, monkeypatch):
    cfg = _cfg_file(tmp_path, "snapshot_maintenance = true\n"
                              "progress_enabled = true\n"
                              "cleanup_staging_after_load = true\n")
    monkeypatch.setattr(workspace, "CONFIG_TOML", cfg)
    monkeypatch.setattr(workspace, "STATE_DIR", tmp_path)
    # The page's <select> submits strings; the writer must emit bare booleans.
    workspace.update_etl_settings({
        "snapshot_maintenance": "false",
        "progress_enabled": "false",
        "cleanup_staging_after_load": "false",
    })
    text = cfg.read_text(encoding="utf-8")
    assert "snapshot_maintenance = false" in text
    assert "progress_enabled = false" in text
    assert "cleanup_staging_after_load = false" in text
    assert '"false"' not in text
    assert workspace.etl_settings()["progress_enabled"] is False


def test_host_specific_keys_rejected(tmp_path, monkeypatch):
    import pytest

    cfg = _cfg_file(tmp_path, "thick_mode = true\n")
    monkeypatch.setattr(workspace, "CONFIG_TOML", cfg)
    monkeypatch.setattr(workspace, "STATE_DIR", tmp_path)
    with pytest.raises(ValueError, match="Not editable"):
        workspace.update_etl_settings({"thick_mode": "false"})
