"""Full rebuilds/appends pack branches into size-budgeted groups: one commit each."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from etl import iceberg_load
from etl.config import CATEGORY_MASTER, Settings, TableDef
from etl.oracle_extract import ExtractResult


def _tdef():
    return TableDef(
        table="OASIS.FOO", unique_key="ID", cdc_column="AMEND_LAST_DATE",
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=CATEGORY_MASTER)


def _staged_parquet(base, tdef, branch, rows=2):
    d = base / tdef.dataset_table_name
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{branch}.parquet"
    pq.write_table(pa.table({"ID": pa.array(list(range(rows)), pa.int64())}), p)
    return p


def _result(tdef, branch, branch_id, staged, rows=2):
    return ExtractResult(table_def=tdef, branch=branch, branch_id=branch_id,
                         status="SUCCESS", row_count=rows, staged_path=staged)


def _three_results(tmp_path):
    tdef = _tdef()
    return tdef, [
        _result(tdef, f"b{i}", i, _staged_parquet(tmp_path, tdef, f"b{i}"))
        for i in (1, 2, 3)
    ]


class _FakeControl:
    def __init__(self):
        self.advanced = []

    def advance(self, r):
        self.advanced.append(r)

    def save(self):
        pass


def _record_runs(monkeypatch):
    """Stub the dlt run; record (paths, disposition) per _iceberg_resource call."""
    calls = []

    def fake_resource(plan, settings, paths, disposition, **kw):
        calls.append((list(paths), disposition))
        return None

    monkeypatch.setattr(iceberg_load, "_iceberg_resource", fake_resource)
    monkeypatch.setattr(iceberg_load, "_run_pipeline", lambda *a, **k: None)
    return calls


# ------------------------- _group_by_staged_bytes ------------------------- #

def test_grouping_collapses_small_branches_into_one_group(tmp_path):
    _, results = _three_results(tmp_path)
    groups = iceberg_load._group_by_staged_bytes(results, max_bytes=10**9)
    assert groups == [results]


def test_grouping_splits_when_budget_exceeded(tmp_path):
    _, results = _three_results(tmp_path)
    groups = iceberg_load._group_by_staged_bytes(results, max_bytes=1)
    assert groups == [[r] for r in results]      # every branch oversized: solo


def test_grouping_preserves_branch_order(tmp_path):
    _, results = _three_results(tmp_path)
    size = results[0].staged_path.stat().st_size  # all three files identical
    groups = iceberg_load._group_by_staged_bytes(results, max_bytes=2 * size)
    assert [r for g in groups for r in g] == results
    assert groups == [results[:2], results[2:]]   # packed 2 + 1


def test_grouping_isolates_missing_staged_file(tmp_path):
    tdef = _tdef()
    ok1 = _result(tdef, "b1", 1, _staged_parquet(tmp_path, tdef, "b1"))
    gone = _result(tdef, "b2", 2, tmp_path / tdef.dataset_table_name / "gone.parquet")
    ok2 = _result(tdef, "b3", 3, _staged_parquet(tmp_path, tdef, "b3"))
    groups = iceberg_load._group_by_staged_bytes([ok1, gone, ok2], max_bytes=10**9)
    assert groups == [[ok1], [gone], [ok2]]       # unknown size: quarantined


# --------------------------- grouped run loops ---------------------------- #

def test_rebuild_small_table_is_single_replace_run(tmp_path, monkeypatch):
    calls = _record_runs(monkeypatch)
    tdef, results = _three_results(tmp_path)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=results, failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_rebuild(None, plan, Settings(), control)

    assert len(calls) == 1                        # ONE commit, not 3
    paths, disposition = calls[0]
    assert paths == [r.staged_path for r in results]
    assert disposition == "replace"
    assert control.advanced == results
    assert all(not r.staged_path.exists() for r in results)


def test_rebuild_over_budget_is_replace_then_appends(tmp_path, monkeypatch):
    calls = _record_runs(monkeypatch)
    tdef, results = _three_results(tmp_path)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=results, failed=[])

    iceberg_load._run_per_branch_rebuild(
        None, plan, Settings(load_group_max_bytes=1), _FakeControl())

    assert [d for _, d in calls] == ["replace", "append", "append"]
    assert [p for p, _ in calls] == [[r.staged_path] for r in results]


def test_snapshot_append_small_table_is_single_append_run(tmp_path, monkeypatch):
    calls = _record_runs(monkeypatch)
    tdef, results = _three_results(tmp_path)
    plan = iceberg_load.TableLoadPlan(tdef=tdef, success=results, failed=[])
    control = _FakeControl()

    iceberg_load._run_per_branch_append(None, plan, Settings(), control)

    assert len(calls) == 1
    assert calls[0][1] == "append"                # never replace: history kept
    assert control.advanced == results


def test_settings_default_group_budget():
    assert Settings().load_group_max_bytes == 256 * 1024 * 1024
