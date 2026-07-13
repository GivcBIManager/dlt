"""dbt ls enrichment is cached on the project's models/tests mtime signature.

Opening the Models page must not spawn a fresh `dbt ls` subprocess on every
request when nothing changed on disk.
"""
from __future__ import annotations

import os
import time

import pytest


@pytest.fixture
def proj(tmp_path, monkeypatch):
    import dbt_config
    import dbt_project_store as ps
    d = tmp_path / "dbt"
    (d / "models").mkdir(parents=True)
    (d / "tests").mkdir(parents=True)
    (d / "models" / "m.sql").write_text("select 1", encoding="utf-8")
    monkeypatch.setattr(dbt_config, "dbt_dir", lambda: d)
    ps._LS_CACHE.clear()
    return d


def test_dbt_ls_not_rerun_when_unchanged(proj, monkeypatch):
    import dbt_project_store as ps
    calls = {"n": 0}

    def counting_ls(rt):
        calls["n"] += 1
        return []

    monkeypatch.setattr(ps, "_dbt_ls", counting_ls)
    ps.list_models()
    ps.list_models()
    assert calls["n"] == 1, "second identical request must hit the cache"


def test_dbt_ls_rerun_when_file_changes(proj, monkeypatch):
    import dbt_project_store as ps
    calls = {"n": 0}

    def counting_ls(rt):
        calls["n"] += 1
        return []

    monkeypatch.setattr(ps, "_dbt_ls", counting_ls)
    ps.list_models()
    future = time.time() + 30
    os.utime(proj / "models" / "m.sql", (future, future))
    ps.list_models()
    assert calls["n"] == 2, "a changed project file must invalidate the cache"


def test_models_and_tests_cached_separately(proj, monkeypatch):
    import dbt_project_store as ps
    seen = []
    monkeypatch.setattr(ps, "_dbt_ls", lambda rt: seen.append(rt) or [])
    ps.list_models()
    ps.list_tests()
    ps.list_models()
    ps.list_tests()
    assert seen == ["model", "test"], "each resource type cached under its own key"
