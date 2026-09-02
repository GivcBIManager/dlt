"""The flow builder must let a dbt node mean "the whole project".

The JS is not exercised by the Python suite, and the "all models" choice was
blocked in three separate places -- the store, the node-collect default, and a
client-side guard in save(). These assertions pin the two that live only in the
template, so removing one and missing the other cannot happen again silently.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

TEMPLATE = Path(__file__).resolve().parents[1] / "gui" / "templates" / "flows.html"


@pytest.fixture(scope="module")
def html() -> str:
    return TEMPLATE.read_text(encoding="utf-8")


def test_model_dropdown_offers_all_models(html):
    assert re.search(r'<option value=""[^>]*>all models</option>', html), \
        "the dbt model dropdown needs an empty-valued 'all models' option"


def test_save_does_not_reject_a_dbt_node_without_a_selection(html):
    # The guard read:
    #   if (n.kind === "dbt" && !String((n.dbt||{}).select || "").trim())
    #     problems.push(`Node ${i+1} (dbt) has no model/test selected`);
    guard = re.search(r'n\.kind === "dbt".{0,120}problems\.push', html, re.S)
    assert guard is None, \
        f"save() still blocks a whole-project dbt node: {guard.group(0)!r}"


def test_collect_does_not_substitute_a_default_model(html):
    # `select: data.select || DBT_MODELS[0].name` silently narrowed an
    # "all models" node to one arbitrary model on save.
    assert not re.search(r'select:\s*data\.select\s*\|\|\s*\(DBT_MODELS', html), \
        "collectNodes() must keep an empty select empty"
    assert re.search(r'select:\s*data\.select\s*\|\|\s*""', html)


def test_other_node_kinds_are_still_guarded(html):
    # Removing the dbt guard must not have removed the ones that still matter.
    assert 'has no command' in html
    assert 'has no pipeline selected' in html
