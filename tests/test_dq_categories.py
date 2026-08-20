"""dq_check --category: table-type selection (masters / transactions / snapshots)."""
from __future__ import annotations

import pytest

import dq_check as dq_cli
from etl import config


def _tdef(name: str, category: str) -> config.TableDef:
    return config.TableDef(
        table=f"OASIS.{name}", unique_key="ID", cdc_column=None,
        where_date_column=None, where_operator=None,
        where_value_of_initial_run=None, category=category)


ALL = [_tdef("STAFF", config.CATEGORY_MASTER),
       _tdef("APPOINTMENTS", config.CATEGORY_TRANSACTION),
       _tdef("STOCK", config.CATEGORY_SNAPSHOT)]


def test_default_is_every_category():
    assert dq_cli._parse_categories(None) == {
        config.CATEGORY_MASTER, config.CATEGORY_TRANSACTION, config.CATEGORY_SNAPSHOT}
    assert dq_cli._parse_categories("") == dq_cli._parse_categories("all")


def test_comma_and_space_separated():
    assert dq_cli._parse_categories("masters,snapshots") == {"masters", "snapshots"}
    assert dq_cli._parse_categories("masters transactions") == {"masters", "transactions"}


def test_both_matches_the_ingest_meaning():
    # oracle_to_iceberg --category both = masters + transactions (no snapshots)
    assert dq_cli._parse_categories("both") == {"masters", "transactions"}


def test_case_insensitive():
    assert dq_cli._parse_categories("Masters") == {"masters"}


def test_unknown_type_raises():
    with pytest.raises(ValueError, match="unknown table type"):
        dq_cli._parse_categories("mastres")


def test_filters_the_table_list():
    cats = dq_cli._parse_categories("masters,snapshots")
    assert [t.object_name for t in ALL if t.category in cats] == ["STAFF", "STOCK"]


def test_parse_args_accepts_category():
    args = dq_cli.parse_args(["--category", "snapshots"])
    assert args.category == "snapshots"
    assert dq_cli.parse_args([]).category is None


# --- GUI command builder ---------------------------------------------------- #

def test_build_argv_passes_category():
    import commands
    argv, label = commands.build_argv(
        {"script": "dq_check", "categories": ["masters", "snapshots"]})
    assert argv[argv.index("--category") + 1] == "masters,snapshots"
    assert "masters,snapshots" in label


def test_build_argv_omits_a_full_selection():
    import commands
    argv, _ = commands.build_argv(
        {"script": "dq_check", "categories": list(commands.DQ_CATEGORIES)})
    assert "--category" not in argv
    argv2, _ = commands.build_argv({"script": "dq_check", "categories": []})
    assert "--category" not in argv2
    argv3, _ = commands.build_argv({"script": "dq_check", "categories": "all"})
    assert "--category" not in argv3


def test_build_argv_accepts_a_comma_string():
    import commands
    argv, _ = commands.build_argv(
        {"script": "dq_check", "category": "transactions"})
    assert argv[argv.index("--category") + 1] == "transactions"


def test_ingest_category_is_untouched():
    import commands
    argv, _ = commands.build_argv({"script": "oracle_to_iceberg", "category": "masters"})
    assert argv[argv.index("--category") + 1] == "masters"
