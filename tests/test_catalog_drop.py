"""``iceberg_browser._drop_from_catalog``: Postgres Iceberg catalog purge.

These tests never touch the real Iceberg catalog -- ``dlt.common.libs.pyiceberg``'s
``get_catalog`` / ``drop_iceberg_table`` are monkeypatched to a sentinel + recorder
so the real production catalog/warehouse is never opened.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def fake_pyiceberg(monkeypatch):
    """Monkeypatch the dlt catalog helpers ``iceberg_browser`` imports lazily.

    ``_drop_from_catalog`` does ``from dlt.common.libs.pyiceberg import
    get_catalog, drop_iceberg_table`` at call time, so patching the attributes
    on the real ``dlt.common.libs.pyiceberg`` module is what actually takes
    effect -- no real catalog connection is ever attempted because
    ``get_catalog`` never runs its real body.
    """
    import dlt.common.libs.pyiceberg as real_pyiceberg

    sentinel = object()
    calls: list[tuple] = []

    def fake_get_catalog(*args, **kwargs):
        return sentinel

    def fake_drop_iceberg_table(catalog, table_id, purge):
        calls.append((catalog, table_id, purge))
        # "bar" simulates NoSuchTableError -> drop_iceberg_table returns False
        return not table_id.endswith(".bar")

    monkeypatch.setattr(real_pyiceberg, "get_catalog", fake_get_catalog)
    monkeypatch.setattr(real_pyiceberg, "drop_iceberg_table", fake_drop_iceberg_table)
    return sentinel, calls


def test_drop_from_catalog_calls_drop_per_table(fake_pyiceberg):
    import iceberg_browser as ib

    sentinel, calls = fake_pyiceberg
    result = ib._drop_from_catalog(["foo", "bar"])

    assert calls == [
        (sentinel, "oasis.foo", True),
        (sentinel, "oasis.bar", True),
    ]
    assert result == ["foo"]  # only "foo" simulated a real drop; "bar" was not registered


def test_drop_from_catalog_empty_list_short_circuits(monkeypatch):
    import dlt.common.libs.pyiceberg as real_pyiceberg
    import iceberg_browser as ib

    def boom(*a, **k):
        raise AssertionError("get_catalog must not be called for an empty table list")

    monkeypatch.setattr(real_pyiceberg, "get_catalog", boom)

    assert ib._drop_from_catalog([]) == []


def test_drop_from_catalog_get_catalog_failure_is_best_effort(monkeypatch):
    import dlt.common.libs.pyiceberg as real_pyiceberg
    import iceberg_browser as ib

    def raising_get_catalog(*args, **kwargs):
        raise RuntimeError("catalog unreachable")

    monkeypatch.setattr(real_pyiceberg, "get_catalog", raising_get_catalog)

    assert ib._drop_from_catalog(["foo"]) == []


def test_drop_from_catalog_isolates_per_table_failure(monkeypatch):
    """One table's ``drop_iceberg_table`` raising must not abort the others --
    the loop's ``try/except Exception: pass`` should skip it and keep going."""
    import dlt.common.libs.pyiceberg as real_pyiceberg
    import iceberg_browser as ib

    sentinel = object()

    def fake_get_catalog(*args, **kwargs):
        return sentinel

    def fake_drop_iceberg_table(catalog, table_id, purge):
        if table_id.endswith(".raising"):
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(real_pyiceberg, "get_catalog", fake_get_catalog)
    monkeypatch.setattr(real_pyiceberg, "drop_iceberg_table", fake_drop_iceberg_table)

    result = ib._drop_from_catalog(["raising", "ok"])

    assert result == ["ok"]
