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


# --- whole-project dbt nodes ------------------------------------------------ #
# A dbt command with no --select runs every model (or every test). The flow
# builder can now emit that, so the argv and the label must both be right.
@pytest.mark.parametrize("cmd", ["run", "test", "build"])
def test_empty_select_omits_the_flag_entirely(cmd):
    import commands
    argv, label = commands.build_argv({"script": "dbt", "dbt_command": cmd, "select": ""})
    assert "--select" not in argv
    assert label == f"dbt {cmd} all models"


def test_missing_select_key_behaves_like_an_empty_one():
    import commands
    argv, label = commands.build_argv({"script": "dbt", "dbt_command": "run"})
    assert "--select" not in argv and label == "dbt run all models"


def test_whitespace_only_select_is_not_passed_through():
    import commands
    argv, _ = commands.build_argv(
        {"script": "dbt", "dbt_command": "run", "select": "   "})
    assert "--select" not in argv


def test_a_real_selection_still_narrows_the_run():
    import commands
    argv, label = commands.build_argv(
        {"script": "dbt", "dbt_command": "run", "select": "stg_products"})
    assert argv[argv.index("--select") + 1] == "stg_products"
    assert label == "dbt run stg_products"


def test_debug_never_gets_a_scope():
    import commands
    argv, label = commands.build_argv({"script": "dbt", "dbt_command": "debug"})
    assert "--select" not in argv and label == "dbt debug"


def test_full_refresh_still_applies_to_a_whole_project_run():
    import commands
    argv, _ = commands.build_argv(
        {"script": "dbt", "dbt_command": "run", "select": "", "full_refresh": True})
    assert "--full-refresh" in argv and "--select" not in argv
