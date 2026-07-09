"""[dbt] block read + in-place edit, sharing the [etl] editor internals."""
import pytest

CONFIG = """[etl]
dataset_name = "oasis"
max_branch_workers = 7

[dbt]
project_dir = "dbt"
target = "dev"
threads = 4
default_materialization = "table"
dbt_executable = "dbt"
"""


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    import config, workspace
    p = tmp_path / "config.toml"
    p.write_text(CONFIG, encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_TOML", p)
    monkeypatch.setattr(workspace, "CONFIG_TOML", p)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    monkeypatch.setattr(workspace, "STATE_DIR", tmp_path)
    return p


def test_dbt_settings_reads_block(cfg):
    import workspace
    s = workspace.dbt_settings()
    assert s["target"] == "dev" and s["threads"] == 4


def test_update_dbt_settings_in_place(cfg):
    import workspace
    res = workspace.update_dbt_settings({"threads": 8, "target": "prod"})
    assert res["applied"] == {"threads": 8, "target": "prod"}
    text = cfg.read_text(encoding="utf-8")
    assert "threads = 8" in text and 'target = "prod"' in text
    # [etl] left intact
    assert 'dataset_name = "oasis"' in text


def test_update_dbt_rejects_unlisted_key(cfg):
    import workspace
    with pytest.raises(ValueError, match="Not editable"):
        workspace.update_dbt_settings({"password": "x"})
