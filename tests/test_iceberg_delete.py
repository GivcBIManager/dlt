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

    ``_drop_from_catalog`` is also monkeypatched to a recorder: the real
    implementation opens the production Postgres Iceberg catalog, which these
    tests must never touch. Its own behavior is covered separately (and safely,
    via mocks) in ``tests/test_catalog_drop.py``.
    """
    import iceberg_browser as ib
    import metastore_read

    root = tmp_path / "oasis"
    root.mkdir()
    monkeypatch.setattr(ib, "ICEBERG_ROOT", root)
    monkeypatch.setattr(metastore_read, "open_metastore", lambda: pg_meta)

    catalog_calls: list[list[str]] = []

    def fake_drop_from_catalog(names: list[str]) -> list[str]:
        dropped = list(names)
        catalog_calls.append(dropped)
        return dropped

    monkeypatch.setattr(ib, "_drop_from_catalog", fake_drop_from_catalog)
    return root, pg_meta, catalog_calls


def test_delete_table_removes_dir_and_watermark(lake):
    import iceberg_browser as ib
    import metastore_read

    root, store, catalog_calls = lake
    _mk_table(root, "patient_ad")
    store.upsert_control_state([
        {"table_name": "patient_ad", "branch_id": "x", "status": "SUCCESS"},
        {"table_name": "other", "branch_id": "1", "status": "SUCCESS"},
    ])

    out = ib.delete_table("patient_ad")

    assert out["deleted"] == ["patient_ad"]
    assert catalog_calls == [["patient_ad"]]  # _drop_from_catalog invoked with the deleted table
    assert out["catalog_dropped"] == ["patient_ad"]  # == the recorder's return value
    assert out["watermarks_cleared"] == ["patient_ad"]
    assert out["rows"] == 42
    assert not (root / "patient_ad").exists()
    remaining = {r["table_name"] for r in metastore_read.read_table_rows("control_state")}
    assert remaining == {"other"}


def test_delete_table_without_control_entry(lake):
    import iceberg_browser as ib

    root, _store, catalog_calls = lake
    _mk_table(root, "etl_control")
    out = ib.delete_table("etl_control")  # system tables deletable by name
    assert out["deleted"] == ["etl_control"]
    assert catalog_calls == [["etl_control"]]
    assert out["catalog_dropped"] == ["etl_control"]
    assert out["watermarks_cleared"] == []


@pytest.mark.parametrize("bad", ["_dlt_loads", "_dlt_version", "..", "a/b", "a\\b", ""])
def test_delete_table_rejects_protected_and_unsafe_names(lake, bad):
    import iceberg_browser as ib

    root, _, _catalog_calls = lake
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

    root, store, catalog_calls = lake
    for n in ("patient_ad", "doc", "etl_control", "etl_run_log", "_dlt_loads"):
        _mk_table(root, n)
    store.upsert_control_state([
        {"table_name": "patient_ad", "branch_id": "1", "status": "SUCCESS"},
        {"table_name": "doc", "branch_id": "1", "status": "SUCCESS"},
    ])

    out = ib.delete_all_tables(include_system=False)
    assert sorted(out["deleted"]) == ["doc", "patient_ad"]
    assert catalog_calls == [["doc", "patient_ad"]]  # single _drop_from_catalog call for the whole sweep
    assert sorted(out["catalog_dropped"]) == ["doc", "patient_ad"]
    assert (root / "etl_control").exists()
    assert (root / "_dlt_loads").exists()
    assert sorted(out["watermarks_cleared"]) == ["doc", "patient_ad"]

    out2 = ib.delete_all_tables(include_system=True)
    assert sorted(out2["deleted"]) == ["etl_control", "etl_run_log"]
    assert catalog_calls[-1] == ["etl_control", "etl_run_log"]
    assert sorted(out2["catalog_dropped"]) == ["etl_control", "etl_run_log"]
    assert (root / "_dlt_loads").exists()  # never deletable
