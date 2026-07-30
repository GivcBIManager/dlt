"""The surgical merge rewrite must conserve rows exactly.

``_upsert_in_memory_lookup`` no longer delegates the delete to
``tx.overwrite(rows, In(changed_keys))``: that re-reads every planned file on
top of the read the lookup already did, and its pruning collapses above
IN_PREDICATE_LIMIT, so doc's 1.02M scattered changes re-read 7.6GB and blew the
commit watchdog. Each matched file is now rewritten from the copy already in
memory and the commit swaps just those files.

That moves file content under our control, so the tests here are about DATA
SAFETY rather than speed: every row of a rewritten file must survive exactly
once, updated rows must carry the delta's values, untouched rows must keep
theirs, and a file whose matched rows are all unchanged must not be rewritten
at all. They run against a real Iceberg table on local disk (SQL catalog +
temp warehouse), so the rewrite, the snapshot swap and the readback are the
genuine code paths, not stubs.
"""
from __future__ import annotations

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")

from etl.iceberg_load import _upsert_in_memory_lookup  # noqa: E402

KEY = "merge_hash"


@pytest.fixture
def table(tmp_path):
    """A tiny partitioned Iceberg table, written as several data files."""
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "t",
        uri=f"sqlite:///{tmp_path}/cat.db",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("ns")

    schema = pa.schema([
        pa.field(KEY, pa.int64(), nullable=False),
        pa.field("branch_id", pa.int32(), nullable=False),
        pa.field("payload", pa.string()),
        pa.field("n", pa.int64()),
    ])
    tbl = catalog.create_table("ns.t", schema=schema)

    # Three separate appends => at least three data files.
    for lo in (0, 10, 20):
        tbl.append(pa.table({
            KEY: pa.array(range(lo, lo + 10), pa.int64()),
            "branch_id": pa.array([1] * 10, pa.int32()),
            "payload": pa.array([f"v{i}" for i in range(lo, lo + 10)], pa.string()),
            "n": pa.array(range(lo, lo + 10), pa.int64()),
        }, schema=schema))
    return tbl


def _rows(tbl) -> dict[int, tuple]:
    t = tbl.scan().to_arrow()
    return {
        r[KEY]: (r["payload"], r["n"])
        for r in t.select([KEY, "payload", "n"]).to_pylist()
    }


def _delta(tbl, rows: list[dict]) -> pa.Table:
    from pyiceberg.io.pyarrow import schema_to_pyarrow
    return pa.table(
        {k: [r[k] for r in rows] for k in (KEY, "branch_id", "payload", "n")},
        schema=schema_to_pyarrow(tbl.schema()),
    )


def test_updates_apply_and_no_rows_are_lost(table):
    before = _rows(table)
    delta = _delta(table, [
        {KEY: 3, "branch_id": 1, "payload": "CHANGED-3", "n": 300},
        {KEY: 15, "branch_id": 1, "payload": "CHANGED-15", "n": 1500},
    ])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    after = _rows(table)
    assert len(after) == len(before)          # nothing lost, nothing duplicated
    assert after[3] == ("CHANGED-3", 300)
    assert after[15] == ("CHANGED-15", 1500)
    for k, v in before.items():
        if k not in (3, 15):
            assert after[k] == v              # untouched rows keep their values


def test_inserts_are_appended(table):
    before = _rows(table)
    delta = _delta(table, [
        {KEY: 999, "branch_id": 1, "payload": "new", "n": 9},
    ])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    after = _rows(table)
    assert len(after) == len(before) + 1
    assert after[999] == ("new", 9)


def test_mixed_update_and_insert(table):
    before = _rows(table)
    delta = _delta(table, [
        {KEY: 5, "branch_id": 1, "payload": "upd", "n": 50},
        {KEY: 777, "branch_id": 1, "payload": "ins", "n": 7},
    ])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    after = _rows(table)
    assert len(after) == len(before) + 1
    assert after[5] == ("upd", 50)
    assert after[777] == ("ins", 7)


def test_identical_delta_commits_nothing(table):
    """Unchanged-row elision: no snapshot, and no file rewritten."""
    before_snapshots = len(table.metadata.snapshots)
    unchanged = _delta(table, [{KEY: 4, "branch_id": 1, "payload": "v4", "n": 4}])

    _upsert_in_memory_lookup(table, unchanged, KEY, update_matched=True, label="t")

    table.refresh()
    assert len(table.metadata.snapshots) == before_snapshots


def test_update_touches_only_the_file_holding_the_key(table):
    """The point of the rewrite: files without a changed row are carried
    forward by reference, so their data files keep their identity."""
    paths_before = {t.file.file_path for t in table.scan().plan_files()}
    delta = _delta(table, [{KEY: 1, "branch_id": 1, "payload": "only-one", "n": 1}])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    table.refresh()
    paths_after = {t.file.file_path for t in table.scan().plan_files()}
    # Exactly one original file was replaced; the others are the same objects.
    assert len(paths_before - paths_after) == 1
    assert len(paths_before & paths_after) == len(paths_before) - 1


def test_every_row_of_a_rewritten_file_survives(table):
    """Change one row in a 10-row file; the other 9 must come back intact."""
    before = _rows(table)
    delta = _delta(table, [{KEY: 12, "branch_id": 1, "payload": "X", "n": -1}])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    after = _rows(table)
    assert len(after) == len(before)
    for k in range(10, 20):
        assert k in after, f"row {k} vanished from the rewritten file"
    assert after[12] == ("X", -1)


def test_all_rows_of_one_file_updated(table):
    """Whole-file replacement: survivors is empty, upd carries every row."""
    before = _rows(table)
    delta = _delta(table, [
        {KEY: k, "branch_id": 1, "payload": f"all{k}", "n": k * 2}
        for k in range(20, 30)
    ])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    after = _rows(table)
    assert len(after) == len(before)
    for k in range(20, 30):
        assert after[k] == (f"all{k}", k * 2)
    assert after[0] == before[0]  # other files untouched


def test_updates_spanning_multiple_files(table):
    before = _rows(table)
    delta = _delta(table, [
        {KEY: 2, "branch_id": 1, "payload": "a", "n": 1},
        {KEY: 14, "branch_id": 1, "payload": "b", "n": 2},
        {KEY: 26, "branch_id": 1, "payload": "c", "n": 3},
    ])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    after = _rows(table)
    assert len(after) == len(before)
    assert (after[2], after[14], after[26]) == (("a", 1), ("b", 2), ("c", 3))


def test_null_payload_round_trips(table):
    """A NULL in the delta must be written as NULL, not dropped."""
    delta = _delta(table, [{KEY: 8, "branch_id": 1, "payload": None, "n": None}])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")

    assert _rows(table)[8] == (None, None)


def test_duplicate_delta_keys_are_refused(table):
    delta = _delta(table, [
        {KEY: 1, "branch_id": 1, "payload": "a", "n": 1},
        {KEY: 1, "branch_id": 1, "payload": "b", "n": 2},
    ])

    with pytest.raises(ValueError, match="Duplicate rows found in source dataset"):
        _upsert_in_memory_lookup(table, delta, KEY, update_matched=True, label="t")


def test_insert_only_leaves_matched_rows_alone(table):
    """update_matched=False must not rewrite anything, only append new keys."""
    before = _rows(table)
    delta = _delta(table, [
        {KEY: 1, "branch_id": 1, "payload": "IGNORED", "n": -99},
        {KEY: 555, "branch_id": 1, "payload": "new", "n": 5},
    ])

    _upsert_in_memory_lookup(table, delta, KEY, update_matched=False, label="t")

    after = _rows(table)
    assert after[1] == before[1]      # matched row untouched
    assert after[555] == ("new", 5)   # unmatched row inserted


# --------------------------------------------------------------------------- #
# Mixed per-file Arrow widths
# --------------------------------------------------------------------------- #
# ArrowScan reports each data file's OWN Arrow width, so one Iceberg `binary`
# key reads back as `binary` from some files and `large_binary` from others
# (files written by different dlt/pyarrow versions). The lookup collects the
# matched keys of every file into one chunked_array, which demands a single
# type -- doc and docl died on "Array chunks must all be same type", retried as
# transient until max_retry_count was exhausted. The int64 key above cannot
# diverge, so these use a binary key written at both widths.

BKEY = "merge_hash"


@pytest.fixture
def binary_key_table(tmp_path):
    """Binary-keyed table whose data files disagree on the key's Arrow width."""
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "t", uri=f"sqlite:///{tmp_path}/cat.db", warehouse=f"file://{warehouse}")
    catalog.create_namespace("ns")

    schema = pa.schema([
        pa.field(BKEY, pa.binary(), nullable=False),
        pa.field("branch_id", pa.int32(), nullable=False),
        pa.field("payload", pa.string()),
    ])
    tbl = catalog.create_table("ns.b", schema=schema)

    def _file(lo, key_type):
        keys = pa.array([bytes([i]) * 16 for i in range(lo, lo + 5)], pa.binary())
        return pa.table(
            {
                BKEY: keys.cast(key_type),
                "branch_id": pa.array([1] * 5, pa.int32()),
                "payload": pa.array([f"v{i}" for i in range(lo, lo + 5)], pa.string()),
            },
            schema=pa.schema([schema.field(BKEY).with_type(key_type),
                              schema.field("branch_id"), schema.field("payload")]),
        )

    tbl.append(_file(0, pa.binary()))          # narrow file
    tbl.append(_file(10, pa.large_binary()))   # wide file -- the divergence
    return tbl


def _brows(tbl) -> dict[bytes, str]:
    t = tbl.scan().to_arrow()
    return {r[BKEY]: r["payload"] for r in t.select([BKEY, "payload"]).to_pylist()}


def _bdelta(tbl, rows: list[tuple[bytes, str]], key_type) -> pa.Table:
    """A delta shaped like the table, with the key forced to ``key_type``."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    schema = schema_to_pyarrow(tbl.schema())
    schema = schema.set(schema.get_field_index(BKEY),
                        schema.field(BKEY).with_type(key_type))
    return pa.table(
        {
            BKEY: pa.array([k for k, _ in rows], pa.binary()).cast(key_type),
            "branch_id": pa.array([1] * len(rows), pa.int32()),
            "payload": pa.array([p for _, p in rows], pa.string()),
        },
        schema=schema,
    )


@pytest.mark.parametrize("delta_key_type", [pa.binary(), pa.large_binary()])
def test_update_spanning_files_of_differing_key_width(binary_key_table, delta_key_type):
    """Matched keys from a narrow and a wide file must collect into one array.

    Parametrized on the delta's own width too: whichever side the delta comes
    in as, the stored keys have to be normalized before they are chunked.
    """
    tbl = binary_key_table
    before = _brows(tbl)
    k_narrow, k_wide = bytes([1]) * 16, bytes([11]) * 16
    delta = _bdelta(tbl, [(k_narrow, "UPD-narrow"), (k_wide, "UPD-wide")],
                     delta_key_type)

    _upsert_in_memory_lookup(tbl, delta, BKEY, update_matched=True, label="b")

    after = _brows(tbl)
    assert len(after) == len(before)          # no rows lost across either file
    assert after[k_narrow] == "UPD-narrow"
    assert after[k_wide] == "UPD-wide"
    for k, v in before.items():
        if k not in (k_narrow, k_wide):
            assert after[k] == v


def test_insert_split_is_correct_across_differing_key_width(binary_key_table):
    """The matched/unmatched split drives inserts, so it must survive the cast."""
    tbl = binary_key_table
    before = _brows(tbl)
    # Match in BOTH files, so the split is computed from the mixed-width
    # collection rather than from a single file's chunks.
    k_narrow, k_wide, k_new = bytes([2]) * 16, bytes([12]) * 16, bytes([99]) * 16
    delta = _bdelta(tbl, [(k_narrow, "UPD-n"), (k_wide, "UPD-w"), (k_new, "INS")],
                    pa.binary())

    _upsert_in_memory_lookup(tbl, delta, BKEY, update_matched=True, label="b")

    after = _brows(tbl)
    assert len(after) == len(before) + 1      # exactly one insert, not three
    assert after[k_narrow] == "UPD-n"
    assert after[k_wide] == "UPD-w"
    assert after[k_new] == "INS"
