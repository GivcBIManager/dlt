"""Canonicalization parity: the vectorized decimal path must match _canon_decimal."""
from __future__ import annotations

from decimal import Decimal

import pyarrow as pa

from etl import dq_check
from etl.dq_check import _NULL


def _canon(values, arrow_type):
    col = pa.array(values, arrow_type)
    return dq_check._canon_array(col).to_pylist()


def test_decimal_scale2_strips_trailing_zeros():
    out = _canon(
        [Decimal("123.40"), Decimal("100.00"), Decimal("0.00"),
         Decimal("-5.50"), Decimal("0.10"), None],
        pa.decimal128(12, 2))
    assert out == ["123.4", "100", "0", "-5.5", "0.1", _NULL]


def test_decimal_scale0_unchanged():
    out = _canon([Decimal("123"), Decimal("0"), Decimal("-7"), None],
                 pa.decimal128(12, 0))
    assert out == ["123", "0", "-7", _NULL]


def test_decimal_matches_scalar_reference():
    # cross-check the vectorized array against the per-value reference impl
    vals = [Decimal("0.000"), Decimal("12.300"), Decimal("-0.500"),
            Decimal("999.999"), Decimal("1000.000")]
    ref = [dq_check._canon_decimal(v) for v in vals]
    out = _canon(vals, pa.decimal128(12, 3))
    assert out == ref
