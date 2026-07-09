"""Tests for the GUI's 1000-record load caps (data-table pagination)."""
from __future__ import annotations

import iceberg_browser as ib


def test_sample_endpoint_caps_limit_at_1000(monkeypatch):
    import app as gui_app
    seen = {}

    def fake_sample(table, limit=50, **kw):
        seen["limit"] = limit
        return {"columns": [], "rows": [], "snapshot_id": None}

    monkeypatch.setattr(gui_app.iceberg_browser, "sample_rows", fake_sample)
    resp = gui_app.app.test_client().get("/api/iceberg/tables/foo/sample?limit=5000")
    assert resp.status_code == 200
    assert seen["limit"] == 1000
