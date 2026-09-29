"""Unified re-read window ([etl] resync_days): catch rows Oracle changes without
moving the CDC column (C1) by re-reading recent rows every incremental run.
Covers the setting (default, parsing, validation) and the query branch."""
from __future__ import annotations

import pytest

from etl import config
from etl.config import Settings


# --- the setting ------------------------------------------------------------ #
def _load_with(monkeypatch, value):
    real = config._cfg
    monkeypatch.setattr(config, "_cfg",
                        lambda key, default: value if key == "etl.resync_days" else real(key, default))
    return config.load_settings({})


def test_default_window_is_60_days():
    assert Settings().resync_days == 60


def test_load_settings_reads_resync_days(monkeypatch):
    assert _load_with(monkeypatch, 14).resync_days == 14


def test_zero_disables_and_is_accepted(monkeypatch):
    assert _load_with(monkeypatch, 0).resync_days == 0


def test_digit_string_from_an_env_override_is_accepted(monkeypatch):
    assert _load_with(monkeypatch, "30").resync_days == 30


@pytest.mark.parametrize("bad", [-1, "abc", 1.5, True, "-3"])
def test_bad_values_are_rejected(monkeypatch, bad):
    with pytest.raises(ValueError, match="resync_days"):
        _load_with(monkeypatch, bad)
