"""build_argv for the dbt script type."""
import pytest


def test_dbt_run_argv():
    import commands
    argv, label = commands.build_argv(
        {"script": "dbt", "dbt_command": "run", "select": "stg_products"})
    assert argv[1] == "run"
    assert "--project-dir" in argv and "--profiles-dir" in argv
    assert argv[argv.index("--select") + 1] == "stg_products"
    assert "--target" in argv
    assert label == "dbt run stg_products"


def test_dbt_full_refresh_only_on_run_build():
    import commands
    argv, _ = commands.build_argv(
        {"script": "dbt", "dbt_command": "run", "select": "m", "full_refresh": True})
    assert "--full-refresh" in argv
    argv2, _ = commands.build_argv(
        {"script": "dbt", "dbt_command": "test", "select": "m", "full_refresh": True})
    assert "--full-refresh" not in argv2


def test_dbt_debug_ignores_select():
    import commands
    argv, _ = commands.build_argv({"script": "dbt", "dbt_command": "debug", "select": "x"})
    assert argv[1] == "debug" and "--select" not in argv


def test_dbt_rejects_bad_command():
    import commands
    with pytest.raises(ValueError, match="dbt command"):
        commands.build_argv({"script": "dbt", "dbt_command": "nuke"})


def test_dbt_in_script_choices():
    import commands
    assert "dbt" in commands.SCRIPT_CHOICES
