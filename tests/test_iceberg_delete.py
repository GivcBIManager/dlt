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
def lake(tmp_path, monkeypatch, pg_meta):
    """A temp Iceberg root plus watermarks sourced from a throwaway Postgres.

    ``_clear_control_state`` now deletes from ``etl_meta.control_state`` in
    Postgres (not a local JSON file), so the fixture repoints
    ``metastore_read.open_metastore`` at the ``pg_meta`` store. Monkeypatching it
    also guarantees deletion tests can never touch the real metastore.
    """
    import iceberg_browser as ib
    import metastore_read

    root = tmp_path / "oasis"
    root.mkdir()
    monkeypatch.setattr(ib, "ICEBERG_ROOT", root)
    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)
    return root, pg_meta


def test_delete_table_removes_dir_and_watermark(lake):
    import iceberg_browser as ib
    import metastore_read

    root, store = lake
    _mk_table(root, "patient_ad")
    store.upsert_control_state([
        {"table_name": "patient_ad", "branch_id": "x", "status": "SUCCESS"},
        {"table_name": "other", "branch_id": "1", "status": "SUCCESS"},
    ])

    out = ib.delete_table("patient_ad")

    assert out["deleted"] == ["patient_ad"]
    assert out["watermarks_cleared"] == ["patient_ad"]
    assert out["rows"] == 42
    assert not (root / "patient_ad").exists()
    remaining = {r["table_name"] for r in metastore_read.read_table_rows("control_state")}
    assert remaining == {"other"}


def test_delete_table_without_control_entry(lake):
    import iceberg_browser as ib

    root, _store = lake
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

    root, store = lake
    for n in ("patient_ad", "doc", "etl_control", "etl_run_log", "_dlt_loads"):
        _mk_table(root, n)
    store.upsert_control_state([
        {"table_name": "patient_ad", "branch_id": "1", "status": "SUCCESS"},
        {"table_name": "doc", "branch_id": "1", "status": "SUCCESS"},
    ])

    out = ib.delete_all_tables(include_system=False)
    assert sorted(out["deleted"]) == ["doc", "patient_ad"]
    assert (root / "etl_control").exists()
    assert (root / "_dlt_loads").exists()
    assert sorted(out["watermarks_cleared"]) == ["doc", "patient_ad"]

    out2 = ib.delete_all_tables(include_system=True)
    assert sorted(out2["deleted"]) == ["etl_control", "etl_run_log"]
    assert (root / "_dlt_loads").exists()  # never deletable
