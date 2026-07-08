"""DQ hash-delta tolerance: status classification + reporting surfaces."""
from __future__ import annotations

from etl import dq_check
from etl.dq_check import HashDelta, DqResult, classify_status


def _hash(matched=0, oo=0, oi=0, mm=0, ora=0, ice=0):
    return HashDelta(matched=matched, only_in_oracle=oo, only_in_iceberg=oi,
                     mismatch=mm, oracle_rows=ora, iceberg_rows=ice)


def test_zero_delta_is_ok():
    assert classify_status(0, _hash(matched=100, ora=100, ice=100), 10.0) == ("OK", 0.0)


def test_within_tolerance():
    status, pct = classify_status(0, _hash(matched=992, oo=8, ora=1000, ice=1000), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert round(pct, 4) == 0.8


def test_boundary_exactly_at_tolerance_is_within():
    # delta 100 / 1000 = 10.0% == tolerance -> WITHIN_TOLERANCE (<=)
    status, pct = classify_status(0, _hash(matched=900, oo=100, ora=1000, ice=1000), 10.0)
    assert status == "WITHIN_TOLERANCE"
    assert round(pct, 2) == 10.0


def test_over_tolerance_is_mismatch():
    status, pct = classify_status(0, _hash(matched=850, oo=150, ora=1000, ice=1000), 10.0)
    assert status == "MISMATCH"
    assert round(pct, 2) == 15.0


def test_row_count_delta_is_hard_mismatch():
    # hash is clean but the row-count delta is nonzero -> MISMATCH regardless
    status, pct = classify_status(5, _hash(matched=1000, ora=1000, ice=1000), 10.0)
    assert status == "MISMATCH"
    assert pct == 0.0


def test_zero_oracle_rows_with_delta_is_mismatch():
    status, pct = classify_status(0, _hash(oi=50, ora=0, ice=50), 10.0)
    assert status == "MISMATCH"
    assert pct is None


def test_no_hash_is_ok_when_count_clean():
    assert classify_status(0, None, 10.0) == ("OK", None)
    assert classify_status(None, None, 10.0) == ("OK", None)


def _res(status, pct, table="t", branch="b"):
    return DqResult(
        table=table, source_table="OASIS.T", branch=branch,
        oracle_row_count=1000, iceberg_row_count=1000,
        hash=_hash(matched=992, oo=8, ora=1000, ice=1000),
        hash_delta_pct=pct, status=status)


def test_render_summary_has_tol_column_and_tally():
    out = dq_check.render_summary(
        [_res("WITHIN_TOLERANCE", 0.8), _res("OK", 0.0, table="u")], do_hash=True)
    assert "TOL%" in out
    assert "0.80%" in out
    assert "1 WITHIN_TOLERANCE" in out


def test_render_summary_tol_dash_without_hash():
    out = dq_check.render_summary([_res("OK", None)], do_hash=False)
    assert "TOL%" not in out  # TOL% only shown with the hash columns


def test_result_rows_includes_hash_delta_pct():
    from etl.config import Settings
    rows = dq_check._result_rows([_res("WITHIN_TOLERANCE", 0.8)], Settings(), "run1")
    assert rows[0]["hash_delta_pct"] == 0.8
    assert rows[0]["status"] == "WITHIN_TOLERANCE"


def test_dq_hints_has_hash_delta_pct_double():
    assert dq_check._DQ_HINTS["hash_delta_pct"] == {"data_type": "double"}
