"""Per-table pipeline names: stable, deterministic, derived from settings."""
from __future__ import annotations

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, Settings, TableDef


def _tdef():
    return TableDef(
        table="OASIS.APPOINTMENTS", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def test_table_pipeline_name_is_stable_and_derived():
    assert (iceberg_load._table_pipeline_name(Settings(), _tdef())
            == "oracle_to_iceberg__appointments")


def test_build_pipeline_honors_name_override(tmp_path):
    p = iceberg_load.build_pipeline(
        Settings(destination_bucket_url=str(tmp_path / "bucket")),
        pipelines_dir=str(tmp_path / "pipes"),
        pipeline_name="oracle_to_iceberg__appointments")
    assert p.pipeline_name == "oracle_to_iceberg__appointments"


def test_build_pipeline_defaults_to_settings_name(tmp_path):
    p = iceberg_load.build_pipeline(
        Settings(destination_bucket_url=str(tmp_path / "bucket")),
        pipelines_dir=str(tmp_path / "pipes"))
    assert p.pipeline_name == "oracle_to_iceberg"
