"""A failed table load must not leave pending dlt packages behind.

dlt completes any pending (extracted/normalized-but-not-loaded) package before
extracting new data on every ``pipeline.run``. Because all tables share one
pipeline, a single poisoned package -- e.g. a bad unique_key that fails at
normalize -- would otherwise be retried and fail again on every later table's
run in the same (and the next) execution, blocking them all.
"""
from __future__ import annotations

import dlt
import pyarrow as pa
import pyarrow.parquet as pq

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, MODE_INCREMENTAL, Settings, TableDef
from etl.oracle_extract import ExtractResult
from etl.progress import PipelineMonitor


def _pipeline(tmp_path):
    return dlt.pipeline(
        pipeline_name="pending_pkg_test",
        pipelines_dir=str(tmp_path / "pipelines"),
        destination=dlt.destinations.filesystem(
            bucket_url=str(tmp_path / "bucket")
        ),
        dataset_name="ds",
    )


def test_clear_pending_packages_drops_stuck_package(tmp_path):
    pipeline = _pipeline(tmp_path)
    # extract() without normalize/load leaves the package pending, exactly the
    # state a normalize/load failure leaves behind.
    pipeline.extract([{"id": 1}], table_name="foo")
    assert pipeline.has_pending_data

    iceberg_load.clear_pending_packages(pipeline, context="test")

    assert not pipeline.has_pending_data


def test_clear_pending_packages_noop_on_clean_pipeline(tmp_path):
    pipeline = _pipeline(tmp_path)

    iceberg_load.clear_pending_packages(pipeline, context="test")

    assert not pipeline.has_pending_data


def test_clear_pending_packages_never_raises():
    class BrokenPipeline:
        @property
        def has_pending_data(self):
            raise RuntimeError("storage broken")

    # Cleanup is best-effort: a failure here must not mask the original error.
    iceberg_load.clear_pending_packages(BrokenPipeline(), context="test")


def test_failed_table_load_drops_pending_packages(tmp_path, monkeypatch):
    """A table whose load fails must leave the shared pipeline clean."""
    pipeline = _pipeline(tmp_path)
    # Pre-existing pending package standing in for whatever debris the failing
    # run itself produced (extract succeeds even when normalize/load will not).
    pipeline.extract([{"id": 1}], table_name="leftover")
    assert pipeline.has_pending_data

    staged = tmp_path / "staged.parquet"
    pq.write_table(pa.table({"ID": pa.array([1, 2], pa.int64())}), staged)
    tdef = TableDef(
        table="OASIS.FOO",
        unique_key="ID",
        cdc_column="AMEND_LAST_DATE",
        where_date_column=None,
        where_operator=None,
        where_value_of_initial_run=None,
        category=CATEGORY_MASTER,
    )
    result = ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                           status="SUCCESS", row_count=2, staged_path=staged)

    def boom(*args, **kwargs):
        raise RuntimeError("simulated load failure")

    # INCREMENTAL + branch subset -> merge path, which enters the load via
    # _existing_insert_at; failing there exercises the except handler.
    monkeypatch.setattr(iceberg_load, "_existing_insert_at", boom)

    plan = iceberg_load._load_one_table(
        pipeline, tdef, [result], Settings(mode=MODE_INCREMENTAL),
        iceberg_load.ControlStore(tmp_path / "control.json"),
        2, 1,
        PipelineMonitor(total_units=1, total_tables=1, enabled=False),
    )

    assert plan.load_status == "FAILED"
    assert not pipeline.has_pending_data


def test_load_and_record_starts_with_clean_pipeline(tmp_path, monkeypatch):
    """Debris left by a crashed previous run is swept before any table loads."""
    pipeline = _pipeline(tmp_path)
    pipeline.extract([{"id": 1}], table_name="leftover")
    assert pipeline.has_pending_data

    monkeypatch.setattr(iceberg_load, "build_pipeline", lambda settings: pipeline)
    # No observability/retention against the throwaway destination.
    monkeypatch.setattr(iceberg_load, "_write_observability",
                        lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "apply_snapshot_retention",
                        lambda *a, **k: None)

    settings = Settings(mode=MODE_INCREMENTAL, progress_enabled=False)
    iceberg_load.load_and_record(
        run_extraction_fn=lambda on_table_done: None,
        tables=[],
        settings=settings,
        control=iceberg_load.ControlStore(tmp_path / "control.json"),
        run_id="test-run",
        total_branches=1,
        branches_in_run=1,
    )

    assert not pipeline.has_pending_data
