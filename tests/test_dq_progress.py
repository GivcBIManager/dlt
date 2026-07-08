"""DQ live-progress emitter: line formats + counters."""
from __future__ import annotations

import logging

from etl.dq_check import DqResult, HashDelta, _DqProgress, _fmt_elapsed


def _unit(status, table="appointments", branch="jazan", ora=2000, ice=2000,
          matched=1992, oo=8, pct=0.40):
    return DqResult(
        table=table, source_table="OASIS.APPT", branch=branch,
        oracle_row_count=ora, iceberg_row_count=ice,
        hash=HashDelta(matched=matched, only_in_oracle=oo, oracle_rows=ora, iceberg_rows=ice),
        hash_delta_pct=pct, status=status)


def test_fmt_elapsed():
    assert _fmt_elapsed(72) == "0:01:12"
    assert _fmt_elapsed(3661) == "1:01:01"


def test_unit_line_format():
    p = _DqProgress(total=3, enabled=False)
    line = p._unit_line(_unit("WITHIN_TOLERANCE"))
    assert line == ("DQ-UNIT appointments/jazan | ora=2000 ice=2000 cnt=0 | "
                    "match=1992 delta=8 pct=0.40 | WITHIN_TOLERANCE")


def test_unit_line_handles_missing_hash():
    p = _DqProgress(total=1, enabled=False)
    res = DqResult(table="m", source_table="OASIS.M", branch="b",
                   oracle_row_count=10, iceberg_row_count=10, hash=None,
                   hash_delta_pct=None, status="OK")
    assert p._unit_line(res) == "DQ-UNIT m/b | ora=10 ice=10 cnt=0 | match=- delta=- pct=- | OK"


def test_record_counts_and_heartbeat(caplog):
    p = _DqProgress(total=3, enabled=False)
    p.start()
    with caplog.at_level(logging.INFO, logger="etl.dq"):
        p.record(_unit("WITHIN_TOLERANCE"))
        p.record(_unit("OK", table="staff", ora=500, ice=500, matched=500, oo=0, pct=0.0))
    assert "DQ-UNIT appointments/jazan" in caplog.text
    assert p._heartbeat_line(10) == "DQ-PROGRESS 0:00:10 | units 2/3 | ok 1 tol 1 mismatch 0 err 0"
    p.stop()
