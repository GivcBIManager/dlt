"""DQ scoping: identifier normalization, helper coverage, snapshot pinning.

Each of these guards a defect where DQ compared the wrong row set and reported
the difference as drift:

* ``_norm`` diverging from dlt silently dropped a column from the comparison,
* a helper-driven table was compared against its whole source table rather than
  the subset the pipeline loads through the helper join,
* an append-only snapshot table was compared against every generation the lake
  had accumulated rather than the newest one.
"""
from __future__ import annotations

import datetime as dt

import pytest
from dlt.common.normalizers.naming.snake_case import NamingConvention

from etl import dq_check
from etl.config import CATEGORY_MASTER, CATEGORY_TRANSACTION, TableDef, _parse_helper


# --------------------------------------------------------------------------- #
# _norm parity with the lake's own normalizer
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("source_name", [
    "STAFF_NAME_1B", "COL2A", "ADDRESS1", "MAPS006", "XY_IOS",
    "REVIEWED_FLAG", "AMEND_LAST_DATE", "WASFATY_IOS",
])
def test_norm_matches_dlt_naming_convention(source_name):
    assert dq_check._norm(source_name) == NamingConvention().normalize_identifier(source_name)


def test_norm_inserts_separator_at_digit_letter_boundary():
    # The regression: a hand-rolled "non-alphanumeric -> _" pass produced
    # 'staff_name_1b', a name present on neither side, so the column dropped
    # out of the comparison entirely.
    assert dq_check._norm("STAFF_NAME_1B") == "staff_name_1_b"
    assert dq_check._norm("COL2A") == "col2_a"


# --------------------------------------------------------------------------- #
# Helper-driven coverage predicates
# --------------------------------------------------------------------------- #
def _helper_tdef(**over) -> TableDef:
    entry = {
        "table": "OASIS.AUTHORISATIONS",
        "unique_key": "AUTHORISATION_NO",
        "cdc_column": None,
        "where_date_column": None,
        "where_operator": ">=",
        "where_value_of_initial_run": "2022-01-01",
        "helper": {
            "table": "OASIS.AUTHORISATIONS_MASTER",
            "cdc_column": "AMEND_LAST_DATE",
            "where_date_column": "REQUEST_DATE",
            "join": [["REQUEST_NO", "REQUEST_NO"]],
        },
    }
    entry.update(over)
    return TableDef(
        table=entry["table"], unique_key=entry["unique_key"],
        cdc_column=entry["cdc_column"], where_date_column=entry["where_date_column"],
        where_operator=entry["where_operator"],
        where_value_of_initial_run=entry["where_value_of_initial_run"],
        category=CATEGORY_TRANSACTION, helper=_parse_helper(entry))


_ENTRY = {"last_cdc": {"value": "2026-09-02 11:54:57.000000", "kind": "datetime"}}


def test_helper_source_query_joins_the_parent():
    tdef = _helper_tdef()
    win = dq_check._make_window(tdef, _ENTRY, dt.date(2026, 1, 1), None)
    coverage, note = dq_check._coverage_predicates(tdef, _ENTRY)
    sql = dq_check._oracle_count_sql(tdef, win, coverage)

    assert "JOIN OASIS.AUTHORISATIONS_MASTER h ON t.REQUEST_NO = h.REQUEST_NO" in sql
    # the parent's floor and the branch's watermark both bound the source
    assert "h.REQUEST_DATE >= TO_DATE('2022-01-01', 'YYYY-MM-DD')" in sql
    assert "h.AMEND_LAST_DATE <= TO_DATE('2026-09-02 11:54:57'" in sql
    assert "AUTHORISATIONS_MASTER" in note


def test_helper_child_window_is_aliased_to_the_child():
    # DOCL has its own date column; it must be qualified t. (the child), while
    # the helper predicates stay on h.
    tdef = _helper_tdef(
        table="DEVDBA.DOCL", unique_key="LINE_ID", where_date_column="DOC_DATE",
        helper={"table": "DEVDBA.DOC", "cdc_column": "AMEND_LAST_DATE",
                "where_date_column": "DOC_DATE", "join": [["DOC_ID", "DOC_ID"]]})
    win = dq_check._make_window(tdef, _ENTRY, dt.date(2026, 1, 1), None)
    coverage, _ = dq_check._coverage_predicates(tdef, _ENTRY)
    sql = dq_check._oracle_count_sql(tdef, win, coverage)

    assert "t.DOC_DATE >= TO_DATE('2026-01-01', 'YYYY-MM-DD')" in sql
    assert "h.AMEND_LAST_DATE <= " in sql
    # the watermark is the helper's column, so it never bounds the child's
    assert "t.DOC_DATE <= " not in sql


def test_plain_table_gets_no_coverage_predicates():
    tdef = TableDef(
        table="OASIS.CONTRACTS", unique_key="CONTRACT_NO",
        cdc_column="AMEND_LAST_DATE", where_date_column=None,
        where_operator=None, where_value_of_initial_run=None,
        category=CATEGORY_MASTER, helper=None)
    coverage, note = dq_check._coverage_predicates(tdef, _ENTRY)
    assert coverage == [] and note is None
    win = dq_check._make_window(tdef, _ENTRY, dt.date(2026, 1, 1), None)
    assert dq_check._oracle_count_sql(tdef, win, coverage) == (
        "SELECT COUNT(*) FROM OASIS.CONTRACTS t")


def test_master_helper_table_gets_no_initial_floor():
    # build_query only applies the configured range filter to non-masters.
    tdef = _helper_tdef()
    tdef = TableDef(
        table=tdef.table, unique_key=tdef.unique_key, cdc_column=tdef.cdc_column,
        where_date_column=tdef.where_date_column, where_operator=tdef.where_operator,
        where_value_of_initial_run=tdef.where_value_of_initial_run,
        category=CATEGORY_MASTER, helper=tdef.helper)
    coverage, _ = dq_check._coverage_predicates(tdef, _ENTRY)
    assert not any("REQUEST_DATE" in p for p in coverage)
    assert any("AMEND_LAST_DATE <=" in p for p in coverage)


def test_coverage_upper_bound_absent_without_a_watermark():
    tdef = _helper_tdef()
    coverage, _ = dq_check._coverage_predicates(tdef, {})
    assert any("REQUEST_DATE" in p for p in coverage)
    assert not any("<=" in p for p in coverage)


# --------------------------------------------------------------------------- #
# Snapshot version pinning
# --------------------------------------------------------------------------- #
class _FakeField:
    def __init__(self, name):
        self.name = name


class _FakeSchema:
    def __init__(self, names):
        self.fields = [_FakeField(n) for n in names]


class _FakeSnapshotTable:
    """Minimal StaticTable stand-in: one branch, three generations."""

    COLUMNS = ["product_code", "branch_id", "version", "version_date"]

    def __init__(self):
        self.filters = []

    def schema(self):
        return _FakeSchema(self.COLUMNS)

    @property
    def inspect(self):
        raise RuntimeError("no partition summaries")  # force the scan fallback

    def scan(self, row_filter=None, selected_fields=()):
        self.filters.append(row_filter)
        return _FakeScan(row_filter, selected_fields)


class _FakeScan:
    VERSIONS = [dt.datetime(2026, 8, 31, 23, 59), dt.datetime(2026, 9, 1, 23, 59),
                dt.datetime(2026, 9, 2, 11, 16)]

    def __init__(self, row_filter, selected_fields):
        self.row_filter, self.selected_fields = row_filter, selected_fields

    def to_arrow_batch_reader(self):
        import pyarrow as pa
        from pyiceberg.expressions import And

        pinned = None
        expr = self.row_filter
        while isinstance(expr, And):
            for side in (expr.left, expr.right):
                val = getattr(side, "literal", None)
                if val is not None and isinstance(getattr(val, "value", None), dt.datetime):
                    pinned = val.value
            expr = expr.left
        versions = [pinned] * 5 if pinned else list(self.VERSIONS)
        cols = {n: pa.array(versions if n == "version" else [1] * len(versions))
                for n in self.selected_fields}
        yield from pa.table(cols).to_batches()


def test_latest_snapshot_version_picks_the_newest_generation(monkeypatch):
    from etl import config

    settings = config.Settings()
    table = _FakeSnapshotTable()
    scope = dq_check._latest_snapshot_version(table, 5, settings)
    assert scope is not None
    assert scope.version == dt.datetime(2026, 9, 2, 11, 16)
    assert scope.field == "version"


def test_snapshot_scope_narrows_the_lake_scan():
    table = _FakeSnapshotTable()
    scope = dq_check._SnapshotScope(
        field="version", version=dt.datetime(2026, 9, 2, 11, 16), date_field="version_date")
    list(dq_check._lake_scan_batches(table, 5, ["product_code"], scope))
    rendered = str(table.filters[-1])
    assert "version" in rendered and "branch_id" in rendered


def test_no_snapshot_scope_leaves_the_scan_on_the_branch_only():
    table = _FakeSnapshotTable()
    list(dq_check._lake_scan_batches(table, 5, ["product_code"], None))
    rendered = str(table.filters[-1])
    assert "branch_id" in rendered and "version" not in rendered


# --------------------------------------------------------------------------- #
# Window pushdown into the Iceberg scan
# --------------------------------------------------------------------------- #
class _FakeWindowTable(_FakeSnapshotTable):
    COLUMNS = ["line_id", "branch_id", "doc_date"]


def _win(lower=None, upper=None, date_col="DOC_DATE"):
    return dq_check._Window(date_col=date_col, ice_lower=lower, ice_upper=upper)


def test_pad_bound_widens_outward():
    assert dq_check._pad_bound(dt.datetime(2026, 1, 1), -1) == dt.datetime(2025, 12, 31)
    assert dq_check._pad_bound(dt.datetime(2026, 1, 1), +1) == dt.datetime(2026, 1, 2)
    assert dq_check._pad_bound(2461042, -1) == 2461041      # numeric (Julian) date
    assert dq_check._pad_bound(2461252.0, +1) == 2461253.0


def _epoch_us(d: dt.datetime) -> int:
    # pyiceberg renders a timestamp literal as microseconds since the epoch.
    return int(d.replace(tzinfo=dt.timezone.utc).timestamp() * 1_000_000)


def test_window_pushdown_is_looser_than_the_exact_window():
    # The pushdown may only prune; it must never exclude a row the exact Arrow
    # filter would keep, so its bounds sit outside the real ones.
    lower, upper = dt.datetime(2026, 1, 1), dt.datetime(2026, 6, 30)
    rendered = str(dq_check._window_row_filter(_FakeWindowTable(), _win(lower, upper)))
    assert str(_epoch_us(lower - dt.timedelta(days=1))) in rendered
    assert str(_epoch_us(upper + dt.timedelta(days=1))) in rendered
    assert str(_epoch_us(lower)) not in rendered
    assert str(_epoch_us(upper)) not in rendered


@pytest.mark.parametrize("win", [
    None,
    _win(None, None),                                   # no bounds resolved
    _win(dt.datetime(2026, 1, 1), None, date_col=None),  # master: no date column
    _win(dt.datetime(2026, 1, 1), None, date_col="ABSENT_COL"),  # not in the lake
])
def test_window_pushdown_absent_when_it_cannot_apply(win):
    assert dq_check._window_row_filter(_FakeWindowTable(), win) is None


def test_one_sided_window_pushes_only_that_side():
    lower = dt.datetime(2026, 1, 1)
    rendered = str(dq_check._window_row_filter(_FakeWindowTable(), _win(lower, None)))
    assert str(_epoch_us(lower - dt.timedelta(days=1))) in rendered
    assert "LessThan" not in rendered


def test_scan_filter_carries_branch_and_window():
    table = _FakeWindowTable()
    list(dq_check._lake_scan_batches(
        table, 3, ["line_id"], None, _win(dt.datetime(2026, 1, 1), None)))
    rendered = str(table.filters[-1])
    assert "branch_id" in rendered and "doc_date" in rendered


def test_scan_filter_is_branch_only_without_a_window():
    table = _FakeWindowTable()
    list(dq_check._lake_scan_batches(table, 3, ["line_id"], None, None))
    rendered = str(table.filters[-1])
    assert "branch_id" in rendered and "doc_date" not in rendered


def test_pushdown_failure_falls_back_to_the_branch_scan(monkeypatch):
    # A predicate pyiceberg cannot build must degrade to the old behaviour,
    # never fail the unit.
    monkeypatch.setattr(dq_check, "_window_row_filter",
                        lambda *a, **k: (_ for _ in ()).throw(TypeError("bad literal")))
    table = _FakeWindowTable()
    list(dq_check._lake_scan_batches(
        table, 3, ["line_id"], None, _win(dt.datetime(2026, 1, 1), None)))
    assert "branch_id" in str(table.filters[-1])
