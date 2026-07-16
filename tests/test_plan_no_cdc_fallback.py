"""No-CDC tables fall back to a full replace instead of an incremental merge.

A table with neither its own ``cdc_column`` nor a helper has no watermark to
filter on, so it re-extracts in full every run (``build_query`` behaves as
INITIAL). Writing that full extract as a ``merge`` upserts it in 1,000-row
chunks -- the catastrophic commit storm. Since the data is a full snapshot
anyway, we write it as a single bulk ``replace`` -- but only when the run covers
every branch (a ``replace`` overwrites the whole table, so a branch subset would
clobber the branches it isn't loading).
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from etl import iceberg_load, oracle_extract
from etl.config import (
    CATEGORY_MASTER,
    HelperJoin,
    MODE_INCREMENTAL,
    MODE_INITIAL,
    Settings,
    TableDef,
)
from etl.oracle_extract import ExtractResult


def _tdef(cdc_column=None, helper=None) -> TableDef:
    return TableDef(
        table="OASIS.NOCDC",
        unique_key="ID",
        cdc_column=cdc_column,
        where_date_column=None,
        where_operator=None,
        where_value_of_initial_run=None,
        category=CATEGORY_MASTER,
        helper=helper,
    )


def _result(tdef, tmp_path, settings) -> ExtractResult:
    staged = tmp_path / "b1.parquet"
    table = oracle_extract.inject_columns(
        pa.table({"ID": pa.array([1, 2], pa.int64())}),
        branch_id=1, settings=settings, tdef=tdef)
    pq.write_table(table, staged)
    return ExtractResult(table_def=tdef, branch="b1", branch_id=1,
                         status="SUCCESS", row_count=2, staged_path=staged)


def _plan(tdef, settings, tmp_path, total_branches, branches_in_run):
    r = _result(tdef, tmp_path, settings)
    return iceberg_load._plan_table(
        tdef, [r], settings, total_branches=total_branches,
        branches_in_run=branches_in_run)


def test_no_cdc_incremental_full_coverage_is_replace(tmp_path):
    # No cdc_column, no helper -> nothing to filter on -> full replace.
    plan = _plan(_tdef(), Settings(mode=MODE_INCREMENTAL), tmp_path,
                 total_branches=1, branches_in_run=1)
    assert plan.disposition == "replace"


def test_no_cdc_incremental_branch_subset_stays_merge(tmp_path):
    # A replace would clobber the branches not in this run, so a subset run of a
    # no-CDC table must stay merge (Phase 2 makes that a single commit).
    plan = _plan(_tdef(), Settings(mode=MODE_INCREMENTAL), tmp_path,
                 total_branches=3, branches_in_run=1)
    assert plan.disposition == "merge"


def test_with_cdc_incremental_stays_merge(tmp_path):
    # A table WITH a cdc_column still does a real incremental merge.
    plan = _plan(_tdef(cdc_column="AMEND_LAST_DATE"),
                 Settings(mode=MODE_INCREMENTAL), tmp_path,
                 total_branches=1, branches_in_run=1)
    assert plan.disposition == "merge"


def test_helper_driven_incremental_stays_merge(tmp_path):
    # A helper supplies CDC, so the table is NOT treated as no-CDC.
    helper = HelperJoin(
        table="OASIS.PARENT",
        join_keys=(("PARENT_ID", "ID"),),
        cdc_column="AMEND_LAST_DATE",
        where_date_column=None,
    )
    plan = _plan(_tdef(helper=helper), Settings(mode=MODE_INCREMENTAL), tmp_path,
                 total_branches=1, branches_in_run=1)
    assert plan.disposition == "merge"
