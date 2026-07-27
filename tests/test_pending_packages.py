"""A failed table load must not leave pending dlt packages behind.

dlt completes any pending (extracted/normalized-but-not-loaded) package before
extracting new data on every ``pipeline.run``. Each table now has its own
pipeline, so debris can only ever block that table; it is swept just before
the table loads.
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


def test_failed_table_load_drops_pending_packages(tmp_path, monkeypatch, pg_meta):
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
    monkeypatch.setattr(iceberg_load, "_open_dest_table", lambda *a, **k: None)

    plan = iceberg_load._load_one_table(
        pipeline, tdef, [result], Settings(mode=MODE_INCREMENTAL),
        iceberg_load.ControlStore(pg_meta),
        2, 1,
        PipelineMonitor(total_units=1, total_tables=1, enabled=False),
    )

    assert plan.load_status == "FAILED"
    assert not pipeline.has_pending_data


def test_each_table_sweeps_its_own_pending_debris_before_loading(tmp_path, monkeypatch, pg_meta):
    """Crash debris in a table's own pipeline is swept just before that table
    loads (the per-table replacement for the old startup sweep)."""
    pipeline = _pipeline(tmp_path)
    pipeline.extract([{"id": 1}], table_name="leftover")
    assert pipeline.has_pending_data

    built = []

    def fake_build(settings, pipelines_dir=None, pipeline_name=None):
        built.append(pipeline_name)
        return pipeline

    monkeypatch.setattr(iceberg_load, "build_pipeline", fake_build)
    monkeypatch.setattr(iceberg_load, "_write_observability", lambda *a, **k: None)
    monkeypatch.setattr(iceberg_load, "apply_snapshot_retention", lambda *a, **k: None)

    tdef = TableDef(table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
                    where_date_column=None, where_operator=None,
                    where_value_of_initial_run=None, category=CATEGORY_MASTER)
    staged = tmp_path / "staged.parquet"
    pq.write_table(pa.table({"ID": pa.array([], pa.int64())}), staged)

    def run_extraction(on_table_done):
        on_table_done(ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                                    status="SUCCESS", row_count=0,
                                    staged_path=staged))

    summary = iceberg_load.load_and_record(
        run_extraction_fn=run_extraction, tables=[tdef],
        settings=Settings(mode=MODE_INCREMENTAL, progress_enabled=False,
                          snapshot_maintenance=False),
        control=iceberg_load.ControlStore(pg_meta), run_id="test-run",
        total_branches=1, branches_in_run=1)

    assert built == ["oracle_to_iceberg__foo"]
    assert not pipeline.has_pending_data          # swept before the load
    assert summary.plans[0].load_status == "SUCCESS"   # 0-row skip path
