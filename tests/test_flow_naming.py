import pytest


def test_slugify_basic():
    from flow_naming import slugify
    assert slugify("Nightly Masters") == "nightly_masters"


def test_slugify_collapses_and_strips_punctuation():
    from flow_naming import slugify
    assert slugify("  Nightly  Masters -> DQ!!  ") == "nightly_masters_dq"


def test_slugify_never_produces_double_underscore():
    from flow_naming import slugify
    assert "__" not in slugify("a --- b @@@ c ___ d")


def test_slugify_empty_falls_back_to_flow():
    from flow_naming import slugify
    assert slugify("") == "flow"
    assert slugify("→→→") == "flow"


def test_name_builders():
    import flow_naming as fn
    flow = {"id": "a1b2c3d4", "name": "Nightly Masters"}
    assert fn.base_name(flow) == "nightly_masters__a1b2c3d4"
    assert fn.job_name(flow) == "flow_nightly_masters__a1b2c3d4"
    assert fn.schedule_name(flow) == "flow_nightly_masters__a1b2c3d4_schedule"
    assert fn.group_name(flow) == "nightly_masters"
    assert fn.asset_key_prefix(flow) == "nightly_masters__a1b2c3d4"


def test_flow_id_round_trips_through_job_name():
    import flow_naming as fn
    flow = {"id": "deadbeef", "name": "Weird -- Name __ x"}
    assert fn.flow_id_from_job(fn.job_name(flow)) == "deadbeef"
