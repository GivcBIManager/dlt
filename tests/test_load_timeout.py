"""Watchdog around each blocking Iceberg commit (``pipeline.run``).

A hung pyiceberg commit never returns and never raises, so the per-table
``except`` recovery can't fire and the whole run deadlocks. ``_run_with_timeout``
converts a hang into a ``TimeoutError`` -- promptly, without waiting out the
hang -- so the existing recovery marks the table FAILED and the run proceeds.
"""
from __future__ import annotations

import threading
import time

import pytest

from etl.iceberg_load import _run_with_timeout


def test_returns_result_when_fn_completes_in_time():
    assert _run_with_timeout(lambda: 42, timeout_s=5, label="t") == 42


def test_raises_timeout_when_fn_hangs():
    started = threading.Event()

    def hang():
        started.set()
        time.sleep(30)  # simulates the never-returning commit

    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        _run_with_timeout(hang, timeout_s=0.2, label="hanging-table")
    elapsed = time.monotonic() - t0
    assert started.is_set()
    # Must give up near the timeout, not wait out the full 30s hang.
    assert elapsed < 5


def test_propagates_fn_exception():
    def boom():
        raise ValueError("commit failed")

    with pytest.raises(ValueError, match="commit failed"):
        _run_with_timeout(boom, timeout_s=5, label="t")


def test_zero_timeout_runs_inline_without_watchdog():
    # 0/None disables the watchdog: fn runs on the calling thread and returns.
    assert _run_with_timeout(lambda: "ok", timeout_s=0, label="t") == "ok"
    assert _run_with_timeout(lambda: "ok", timeout_s=None, label="t") == "ok"


# --------------------------------------------------------------------------- #
# Timeout recovery: a hung commit poisons the pipeline; the orchestrator must
# abandon it (per-table pipelines: nothing rebuilds, nothing else uses it)
# rather than clear/reuse it (a zombie is stuck inside).
# --------------------------------------------------------------------------- #
import dlt  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from etl import iceberg_load  # noqa: E402
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef  # noqa: E402
from etl.oracle_extract import ExtractResult  # noqa: E402
from etl.progress import PipelineMonitor  # noqa: E402


def _pipeline(base):
    return dlt.pipeline(
        pipeline_name="timeout_test",
        pipelines_dir=str(base / "pipelines"),
        destination=dlt.destinations.filesystem(bucket_url=str(base / "bucket")),
        dataset_name="ds",
    )


def _merge_tdef():
    return TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _staged(tmp_path):
    p = tmp_path / "staged.parquet"
    pq.write_table(pa.table({"ID": pa.array([1, 2], pa.int64())}), p)
    return p


def test_timed_out_commit_marks_plan_poisoned_and_skips_cleanup(tmp_path, monkeypatch, pg_meta):
    tdef = _merge_tdef()
    result = ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                           status="SUCCESS", row_count=2, staged_path=_staged(tmp_path))

    # A hung commit surfaces as TimeoutError from inside the load body.
    monkeypatch.setattr(iceberg_load, "_existing_insert_at",
                        lambda *a, **k: (_ for _ in ()).throw(TimeoutError("hung 900s")))
    cleared = []
    monkeypatch.setattr(iceberg_load, "clear_pending_packages",
                        lambda *a, **k: cleared.append(a))
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)

    plan = iceberg_load._load_one_table(
        _pipeline(tmp_path), tdef, [result], Settings(mode=MODE_INCREMENTAL),
        iceberg_load.ControlStore(pg_meta), 2, 1,
        PipelineMonitor(total_units=1, total_tables=1, enabled=False))

    assert plan.load_status == "FAILED"
    assert plan.load_timed_out is True
    # A poisoned pipeline (zombie still inside it) must NOT be touched by cleanup.
    assert cleared == []


class _FakeStore:
    def upsert_control_state(self, rows):
        pass


def test_load_and_record_continues_past_a_timed_out_table(tmp_path, monkeypatch):
    """Table A's hung commit must not stop table B: each table has its own
    pipeline, so A is FAILED+timed-out and simply abandoned, B succeeds."""
    built = []

    def fake_build(settings, pipelines_dir=None, pipeline_name=None):
        built.append(pipeline_name)
        return _pipeline(tmp_path / (pipeline_name or "p"))

    monkeypatch.setattr(iceberg_load, "build_pipeline", fake_build)
    monkeypatch.setattr(iceberg_load, "_write_observability", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "apply_snapshot_retention", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "_coerce_unified_nulls", lambda s, t, sc: sc)
    monkeypatch.setattr(iceberg_load, "_widen_schema_to_destination",
                        lambda s, t, sc: sc)
    monkeypatch.setattr(iceberg_load, "_table_is_hash_ready", lambda *a, **k: False)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)

    t_hang = _merge_tdef()                                   # -> "foo"
    t_ok = TableDef(table="OASIS.BAR", unique_key="ID",
                    cdc_column="AMEND_LAST_DATE", where_date_column=None,
                    where_operator=None, where_value_of_initial_run=None,
                    category=CATEGORY_MASTER)

    def fake_eia(settings, tdef, *a, **k):
        if tdef.dataset_table_name == "foo":
            raise TimeoutError("hung 900s")
        return None

    monkeypatch.setattr(iceberg_load, "_existing_insert_at", fake_eia)

    def run_extraction(on_table_done):
        for tdef in (t_hang, t_ok):
            on_table_done(ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                                        status="SUCCESS", row_count=2,
                                        staged_path=_staged(tmp_path)))

    summary = iceberg_load.load_and_record(
        run_extraction_fn=run_extraction, tables=[t_hang, t_ok],
        settings=Settings(mode=MODE_INCREMENTAL, progress_enabled=False,
                          snapshot_maintenance=False, load_workers=1),
        control=iceberg_load.ControlStore(_FakeStore()),
        run_id="r", total_branches=2, branches_in_run=1)

    by_name = {p.tdef.dataset_table_name: p for p in summary.plans}
    assert by_name["foo"].load_timed_out is True
    assert by_name["foo"].load_status == "FAILED"
    assert by_name["bar"].load_status == "SUCCESS"
    assert built == ["oracle_to_iceberg__foo", "oracle_to_iceberg__bar"]
