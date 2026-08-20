"""The dbt integration is wired up.

The dbt project itself is per-host working state and is not in the repo (see
.gitignore): models, tests and macros are authored through the GUI's Models
page. Only ``dbt_project.yml`` is tracked, so that is the only project file
these tests can assume exists.
"""
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_dbt_project_yml_names_oasis_profile():
    text = (REPO / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert "name: 'oasis'" in text or 'name: "oasis"' in text
    assert "profile: 'oasis'" in text or 'profile: "oasis"' in text


def test_requirements_pin_dbt():
    reqs = (REPO / "requirements-gui.txt").read_text(encoding="utf-8")
    assert "dbt-core" in reqs and "dbt-clickhouse" in reqs


def test_gitignore_excludes_the_dbt_project_but_keeps_dbt_project_yml():
    """dbt_project.yml must survive the blanket ignore: dbt will not start
    without it and, unlike profiles.yml, nothing generates it."""
    ig = (REPO / ".gitignore").read_text(encoding="utf-8").splitlines()
    # `dbt/*`, not `dbt/`: git does not descend into an excluded directory, so a
    # negation nested inside one would never be reached.
    assert "dbt/*" in ig
    assert "!dbt/dbt_project.yml" in ig
    assert "dbt/" not in ig, "a bare `dbt/` would make the negation unreachable"
