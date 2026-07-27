"""load_workers: parallel load slots, default 2, never below 1."""
from __future__ import annotations

from etl.config import Settings


def test_default_load_workers_is_two():
    assert Settings().load_workers == 2


def test_load_workers_clamped_to_at_least_one():
    assert Settings(load_workers=0).load_workers == 1
    assert Settings(load_workers=-3).load_workers == 1


def test_load_workers_accepts_higher_values():
    assert Settings(load_workers=4).load_workers == 4


def test_load_workers_override_is_clamped_via_load_settings():
    from etl import config
    assert config.load_settings({"load_workers": 0}).load_workers == 1
