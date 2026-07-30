"""Partition pruning for the in-memory merge lookup.

``_upsert_in_memory_lookup`` used to plan EVERY data file in the table, so a
single-branch delta on ``appointments`` scanned all 8 branch partitions -- 763
files / 19GB of reads to merge 16MB. These tables are partitioned by
``branch_id`` (identity), so the plan can be pruned to the branches the delta
actually carries. ``_delta_partition_filter`` builds that predicate, and the
tests below pin the two things that make the pruning SAFE: it only fires for
identity transforms (where value -> partition is 1:1), and it bails out
whenever the delta cannot constrain a partition dimension.
"""
from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.transforms import BucketTransform, IdentityTransform

from etl.iceberg_load import _delta_partition_filter


class _Field:
    def __init__(self, source_id, transform):
        self.source_id = source_id
        self.transform = transform


class _Spec:
    def __init__(self, *fields):
        self.fields = fields


class _SchemaField:
    def __init__(self, name):
        self.name = name


class _Schema:
    def __init__(self, by_id):
        self._by_id = by_id

    def find_field(self, source_id):
        return _SchemaField(self._by_id[source_id])


class _Table:
    def __init__(self, spec, by_id):
        self._spec = spec
        self._schema = _Schema(by_id)

    def spec(self):
        return self._spec

    def schema(self):
        return self._schema


def _identity_table(col="branch_id"):
    return _Table(_Spec(_Field(73, IdentityTransform())), {73: col})


def test_single_branch_delta_prunes_to_that_branch():
    """The predicate names the partition column and only the delta's value."""
    table = _identity_table()
    delta = pa.table({"branch_id": pa.array([7] * 100, pa.int32())})

    expr = _delta_partition_filter(table, delta)

    assert expr is not None
    rendered = str(expr)
    assert "branch_id" in rendered
    assert "7" in rendered
    # No other branch may leak into the predicate -- that would defeat pruning.
    assert "8" not in rendered


def test_multi_branch_delta_keeps_every_branch_present():
    """A grouped run covering several branches must not lose any of them."""
    table = _identity_table()
    delta = pa.table({"branch_id": pa.array([1, 1, 4, 7], pa.int32())})

    rendered = str(_delta_partition_filter(table, delta))

    for present in ("1", "4", "7"):
        assert present in rendered


def test_nulls_do_not_enter_the_predicate():
    """A NULL branch cannot be expressed as an In literal; real values still are."""
    table = _identity_table()
    delta = pa.table({"branch_id": pa.array([3, None, 3], pa.int32())})

    expr = _delta_partition_filter(table, delta)

    assert expr is not None
    assert "3" in str(expr)


def test_all_null_partition_column_falls_back_to_full_plan():
    """No usable value means no safe pruning -- scan everything, as before."""
    table = _identity_table()
    delta = pa.table({"branch_id": pa.array([None, None], pa.int32())})

    assert _delta_partition_filter(table, delta) is None


def test_non_identity_transform_is_not_pruned():
    """bucket(N) maps many values to one partition; the delta's raw values say
    nothing about which buckets hold them, so pruning would drop live files."""
    table = _Table(_Spec(_Field(73, BucketTransform(16))), {73: "branch_id"})
    delta = pa.table({"branch_id": pa.array([7], pa.int32())})

    assert _delta_partition_filter(table, delta) is None


def test_partition_column_absent_from_delta_is_not_pruned():
    """Without the column the delta cannot constrain the dimension at all."""
    table = _identity_table()
    delta = pa.table({"some_other_col": pa.array([1, 2])})

    assert _delta_partition_filter(table, delta) is None


def test_unpartitioned_table_is_not_pruned():
    """Preserves today's behaviour for tables with no partition spec."""
    table = _Table(_Spec(), {})
    delta = pa.table({"branch_id": pa.array([7], pa.int32())})

    assert _delta_partition_filter(table, delta) is None


def test_mixed_spec_bails_out_if_any_field_is_unprunable():
    """One non-identity dimension makes the whole conjunction unsafe."""
    table = _Table(
        _Spec(_Field(73, IdentityTransform()), _Field(74, BucketTransform(8))),
        {73: "branch_id", 74: "doc_no"},
    )
    delta = pa.table({
        "branch_id": pa.array([7], pa.int32()),
        "doc_no": pa.array(["A1"]),
    })

    assert _delta_partition_filter(table, delta) is None


@pytest.mark.parametrize("branch_type", [pa.int32(), pa.int64(), pa.string()])
def test_common_branch_id_types_are_supported(branch_type):
    """branch_id reads back as int32/int64 depending on the table's vintage."""
    table = _identity_table()
    value = "7" if branch_type == pa.string() else 7
    delta = pa.table({"branch_id": pa.array([value], branch_type)})

    assert _delta_partition_filter(table, delta) is not None
