"""Peak/heartbeat labels track the SET of in-flight table loads."""
from __future__ import annotations

from etl.progress import PipelineMonitor


def _mon():
    return PipelineMonitor(total_units=1, total_tables=1, enabled=False)


def test_label_is_phase_when_no_loads_active():
    m = _mon()
    m.set_activity("extract")
    assert m._activity_label() == "extract"


def test_label_lists_active_loads_in_start_order():
    m = _mon()
    m.set_activity("extract")
    m.begin_load("appointments")
    m.begin_load("claims")
    assert m._activity_label() == "load[2]:appointments,claims"
    m.end_load("appointments")
    assert m._activity_label() == "load[1]:claims"
    m.end_load("claims")
    assert m._activity_label() == "extract"


def test_end_load_of_unknown_table_is_noop():
    m = _mon()
    m.end_load("never-started")
    assert m._activity_label() == "starting"


def test_peak_attribution_names_the_inflight_load():
    m = _mon()
    m.begin_load("big_table")
    m._refresh_peaks()
    report = m.stop()
    assert "big_table" in (report.rss_peak_activity or "")
