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
