"""Staging-layer table deletion: path safety, system-table rules, watermarks."""
from __future__ import annotations

import json

import pytest


def _mk_table(root, name, rows=42, size=1000):
    meta_dir = root / name / "metadata"
    meta_dir.mkdir(parents=True)
    (meta_dir / "00001-a.metadata.json").write_text(json.dumps({
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "summary": {
            "total-records": rows, "total-files-size": size}}],
    }), encoding="utf-8")


@pytest.fixture
def lake(tmp_path, monkeypatch):
    import iceberg_browser as ib

    root = tmp_path / "oasis"
    root.mkdir()
    control = tmp_path / "control_state.json"
    monkeypatch.setattr(ib, "ICEBERG_ROOT", root)
    monkeypatch.setattr(ib, "CONTROL_STATE", control)
    return root, control


def test_delete_table_removes_dir_and_watermark(lake):
    import iceberg_browser as ib

    root, control = lake
    _mk_table(root, "patient_ad")
    control.write_text(json.dumps({"patient_ad": {"x": 1}, "other": {}}), encoding="utf-8")

    out = ib.delete_table("patient_ad")

    assert out["deleted"] == ["patient_ad"]
    assert out["watermarks_cleared"] == ["patient_ad"]
    assert out["rows"] == 42
    assert not (root / "patient_ad").exists()
    assert json.loads(control.read_text(encoding="utf-8")) == {"other": {}}


def test_delete_table_without_control_entry(lake):
    import iceberg_browser as ib

    root, _control = lake
    _mk_table(root, "etl_control")
    out = ib.delete_table("etl_control")  # system tables deletable by name
    assert out["deleted"] == ["etl_control"]
    assert out["watermarks_cleared"] == []


@pytest.mark.parametrize("bad", ["_dlt_loads", "_dlt_version", "..", "a/b", "a\\b", ""])
def test_delete_table_rejects_protected_and_unsafe_names(lake, bad):
    import iceberg_browser as ib

    root, _ = lake
    _mk_table(root, "_dlt_loads")
    with pytest.raises(ValueError):
        ib.delete_table(bad)
    assert (root / "_dlt_loads").exists()


def test_delete_table_unknown_is_not_found(lake):
    import iceberg_browser as ib

    with pytest.raises(FileNotFoundError):
        ib.delete_table("nope")


def test_delete_all_skips_system_unless_included(lake):
    import iceberg_browser as ib

    root, control = lake
    for n in ("patient_ad", "doc", "etl_control", "etl_run_log", "_dlt_loads"):
        _mk_table(root, n)
    control.write_text(json.dumps({"patient_ad": {}, "doc": {}}), encoding="utf-8")

    out = ib.delete_all_tables(include_system=False)
    assert sorted(out["deleted"]) == ["doc", "patient_ad"]
    assert (root / "etl_control").exists()
    assert (root / "_dlt_loads").exists()
    assert sorted(out["watermarks_cleared"]) == ["doc", "patient_ad"]

    out2 = ib.delete_all_tables(include_system=True)
    assert sorted(out2["deleted"]) == ["etl_control", "etl_run_log"]
    assert (root / "_dlt_loads").exists()  # never deletable
