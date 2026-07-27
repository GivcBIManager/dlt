"""Load dispatch: N workers overlap distinct tables, each through its own
per-table pipeline; load_workers=1 keeps loads strictly serial."""
from __future__ import annotations

import threading
import time

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef
from etl.iceberg_load import ControlStore, TableLoadPlan
from etl.oracle_extract import ExtractResult


class _FakeStore:
    def upsert_control_state(self, rows):
        pass


def _tdef(name):
    return TableDef(
        table=f"OASIS.{name}", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _drive(monkeypatch, tables, load_workers, body):
    """Run load_and_record with _load_one_table stubbed to `body(tdef)`."""
    built = []

    def fake_build(settings, pipelines_dir=None, pipeline_name=None):
        built.append(pipeline_name)
        return object()

    def fake_load(pipeline, tdef, batch, settings, control, total_branches,
                  branches_in_run, monitor):
        body(tdef)
        plan = TableLoadPlan(tdef=tdef, success=[], failed=[])
        plan.load_status = "SUCCESS"
        return plan

    monkeypatch.setattr(iceberg_load, "build_pipeline", fake_build)
    monkeypatch.setattr(iceberg_load, "clear_pending_packages", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_load_one_table", fake_load)
    monkeypatch.setattr(iceberg_load, "_write_observability", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "apply_snapshot_retention", lambda *a, **k: None)

    def run_extraction(on_table_done):
        for t in tables:
            on_table_done(ExtractResult(table_def=t, branch="b1", branch_id=1,
                                        status="SUCCESS", row_count=1,
                                        staged_path=None))

    summary = iceberg_load.load_and_record(
        run_extraction_fn=run_extraction, tables=tables,
        settings=Settings(mode=MODE_INCREMENTAL, progress_enabled=False,
                          load_workers=load_workers),
        control=ControlStore(_FakeStore()), run_id="r",
        total_branches=1, branches_in_run=1)
    return summary, built


def test_two_workers_overlap_two_tables(monkeypatch):
    barrier = threading.Barrier(2, timeout=10)

    summary, built = _drive(
        monkeypatch, [_tdef("A"), _tdef("B")], load_workers=2,
        body=lambda tdef: barrier.wait())   # both must be in flight at once

    assert sorted(built) == ["oracle_to_iceberg__a", "oracle_to_iceberg__b"]
    assert [p.load_status for p in summary.plans] == ["SUCCESS", "SUCCESS"]
    assert summary.extraction_error is None


def test_single_worker_is_strictly_serial(monkeypatch):
    state = {"active": 0, "peak": 0}
    lock = threading.Lock()

    def body(tdef):
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        time.sleep(0.05)
        with lock:
            state["active"] -= 1

    summary, _ = _drive(monkeypatch, [_tdef("A"), _tdef("B"), _tdef("C")],
                        load_workers=1, body=body)

    assert state["peak"] == 1
    assert len(summary.plans) == 3


def test_zero_workers_clamps_to_serial(monkeypatch):
    state = {"active": 0, "peak": 0}
    lock = threading.Lock()

    def body(tdef):
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        time.sleep(0.02)
        with lock:
            state["active"] -= 1

    summary, _ = _drive(monkeypatch, [_tdef("A"), _tdef("B")],
                        load_workers=0, body=body)

    assert state["peak"] == 1
    assert len(summary.plans) == 2
