"""DQ delta tolerance: status classification + reporting surfaces."""
from __future__ import annotations

from etl import dq_check
from etl.dq_check import HashDelta, DqResult, classify_status


def _hash(matched=0, oo=0, oi=0, mm=0, ora=0, ice=0):
    return HashDelta(matched=matched, only_in_oracle=oo, only_in_iceberg=oi,
                     mismatch=mm, oracle_rows=ora, iceberg_rows=ice)


def test_zero_delta_is_ok():
    assert classify_status(
        0, 100, _hash(matched=100, ora=100, ice=100), 10.0) == ("OK", 0.0, 0.0)


def test_within_tolerance():
    status, cnt_pct, pct = classify_status(
        0, 1000, _hash(matched=992, oo=8, ora=1000, ice=1000), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert cnt_pct == 0.0
    assert round(pct, 4) == 0.8


def test_boundary_exactly_at_tolerance_is_within():
    # delta 100 / 1000 = 10.0% == tolerance -> WITHIN_TOLERANCE (<=)
    status, _, pct = classify_status(
        0, 1000, _hash(matched=900, oo=100, ora=1000, ice=1000), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert round(pct, 2) == 10.0


def test_over_tolerance_is_mismatch():
    status, _, pct = classify_status(
        0, 1000, _hash(matched=850, oo=150, ora=1000, ice=1000), 10.0)
    assert status == "MISMATCH"
    assert round(pct, 2) == 15.0


def test_row_count_delta_within_tolerance():
    # 5 rows short of 1000 = 0.5% -> tolerated, like a hash delta of the same size
    status, cnt_pct, pct = classify_status(
        5, 1000, _hash(matched=1000, ora=1000, ice=1000), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert round(cnt_pct, 2) == 0.5
    assert pct == 0.0


def test_row_count_delta_over_tolerance_is_mismatch():
    status, cnt_pct, _ = classify_status(
        150, 1000, _hash(matched=1000, ora=1000, ice=1000), 10.0)
    assert status == "MISMATCH"
    assert round(cnt_pct, 2) == 15.0


def test_row_count_surplus_uses_absolute_delta():
    # more rows in the lake than in Oracle: the sign must not hide the drift
    status, cnt_pct, _ = classify_status(-150, 1000, None, 10.0)
    assert status == "MISMATCH"
    assert round(cnt_pct, 2) == 15.0


def test_larger_of_the_two_deltas_decides():
    # counts drift 0.5% (fine) but the hash drifts 15% -> MISMATCH
    status, cnt_pct, pct = classify_status(
        5, 1000, _hash(matched=850, oo=150, ora=1000, ice=1000), 10.0)
    assert status == "MISMATCH"
    assert round(cnt_pct, 2) == 0.5
    assert round(pct, 2) == 15.0


def test_zero_tolerance_keeps_any_drift_a_mismatch():
    assert classify_status(1, 1000, None, 0.0)[0] == "MISMATCH"
    assert classify_status(
        0, 1000, _hash(matched=999, oo=1, ora=1000, ice=1000), 0.0)[0] == "MISMATCH"


def test_zero_oracle_rows_with_only_lake_rows_is_not_drift():
    # Was MISMATCH (undefined ratio). With 0 Oracle rows the only possible hash
    # difference is lake-only rows -- deleted in Oracle -- which are no longer
    # drift. The undefined-ratio rule still holds for count drift (next test).
    # (row-count delta = oracle - iceberg = 0 - 50, as check_unit computes it)
    status, _, pct = classify_status(-50, 0, _hash(oi=50, ora=0, ice=50), 10.0)
    assert status == "OK"
    assert pct == 0.0


def test_zero_oracle_rows_with_count_delta_is_mismatch():
    status, cnt_pct, _ = classify_status(-50, 0, None, 10.0)
    assert status == "MISMATCH"
    assert cnt_pct is None


def test_no_hash_is_ok_when_count_clean():
    assert classify_status(0, 10, None, 10.0) == ("OK", 0.0, None)
    # an unmeasured count (None) is not drift
    assert classify_status(None, None, None, 10.0) == ("OK", None, None)


def _res(status, pct, table="t", branch="b", cnt_pct=0.0):
    return DqResult(
        table=table, source_table="OASIS.T", branch=branch,
        oracle_row_count=1000, iceberg_row_count=1000,
        hash=_hash(matched=992, oo=8, ora=1000, ice=1000),
        hash_delta_pct=pct, row_count_delta_pct=cnt_pct, status=status)


def test_render_summary_has_pct_columns_and_tally():
    out = dq_check.render_summary(
        [_res("WITHIN_TOLERANCE", 0.8), _res("OK", 0.0, table="u")], do_hash=True)
    assert "HASH%" in out
    assert "CNT%" in out
    assert "0.80%" in out
    assert "1 WITHIN_TOLERANCE" in out


def test_render_summary_hash_pct_dropped_without_hash():
    out = dq_check.render_summary([_res("OK", None)], do_hash=False)
    assert "HASH%" not in out   # only shown with the hash columns
    assert "CNT%" in out        # the count check always runs


def test_result_rows_includes_both_pcts():
    from etl.config import Settings
    rows = dq_check._result_rows(
        [_res("WITHIN_TOLERANCE", 0.8, cnt_pct=0.5)], Settings(), "run1")
    assert rows[0]["hash_delta_pct"] == 0.8
    assert rows[0]["row_count_delta_pct"] == 0.5
    assert rows[0]["status"] == "WITHIN_TOLERANCE"


def test_dq_hints_has_pct_doubles():
    assert dq_check._DQ_HINTS["hash_delta_pct"] == {"data_type": "double"}
    assert dq_check._DQ_HINTS["row_count_delta_pct"] == {"data_type": "double"}

# --- rows only in Iceberg are not drift ---------------------------------------- #
# The load never deletes: a row deleted in Oracle stays in the lake by design, so
# lake-only rows are reported (hash_only_in_iceberg, hash_total_delta) but do not
# count toward the status -- neither through the hash delta nor through the
# row-count delta they cause.
def test_lake_only_rows_alone_are_ok():
    status, cnt_pct, pct = classify_status(
        -50, 1000, _hash(matched=1000, oi=50, ora=1000, ice=1050), 10.0)
    assert (status, cnt_pct, pct) == ("OK", 0.0, 0.0)


def test_lake_only_rows_do_not_inflate_real_drift():
    status, _, pct = classify_status(
        0, 1000, _hash(matched=850, mm=150, oi=500, ora=1000, ice=1500), 10.0)
    assert status == "MISMATCH"
    assert round(pct, 2) == 15.0            # 150 changed, not 650


def test_count_delta_from_lake_only_rows_is_discounted():
    # 50 missing from the lake, 500 deleted in Oracle: count delta -450 is
    # really +50 once the deleted rows are set aside -> 5% -> within 10%.
    status, cnt_pct, pct = classify_status(
        -450, 1000, _hash(matched=950, oo=50, oi=500, ora=1000, ice=1450), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert round(cnt_pct, 2) == 5.0 and round(pct, 2) == 5.0


def test_only_deleted_rows_and_no_oracle_rows_is_ok():
    # ar_stat_of_invoices/abha: Oracle 0 rows in the window, lake 1 (deleted).
    assert classify_status(-1, 0, _hash(oi=1, ora=0, ice=1), 10.0)[0] == "OK"


def test_lake_duplicates_still_count():
    # Duplicated lake rows are not lake-only keys (keys are deduped before the
    # compare), so the surplus stays a count drift.
    status, cnt_pct, _ = classify_status(
        -150, 1000, _hash(matched=1000, ora=1000, ice=1150), 10.0)
    assert status == "MISMATCH" and round(cnt_pct, 2) == 15.0


def test_counts_only_mode_is_unchanged():
    # Without the hash pass lake-only rows cannot be told apart from duplicates.
    assert classify_status(-500, 1000, None, 10.0)[0] == "MISMATCH"


def test_reported_total_delta_still_includes_lake_only_rows():
    assert _hash(oo=1, oi=2, mm=3).total_delta == 6
