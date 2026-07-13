"""Shared TOML surgical-write helper used by the secret/config editors."""
from __future__ import annotations

import pytest


def test_read_lines_missing_is_empty(tmp_path):
    import toml_edit
    assert toml_edit.read_lines(tmp_path / "nope.toml") == []


def test_read_lines_returns_file_lines(tmp_path):
    import toml_edit
    p = tmp_path / "secrets.toml"
    p.write_text("[a]\nx = 1\n", encoding="utf-8")
    assert toml_edit.read_lines(p) == ["[a]", "x = 1"]


def test_write_lines_replaces_atomically_and_backs_up(tmp_path):
    import toml_edit
    p = tmp_path / "secrets.toml"
    p.write_text("[a]\nx = 1\n", encoding="utf-8")
    backup = toml_edit.write_lines(
        p, ["[a]", "x = 2"], backup_dir=tmp_path, backup_prefix="secrets.toml")
    assert p.read_text(encoding="utf-8") == "[a]\nx = 2\n"
    assert backup is not None and backup.exists()
    assert backup.read_text(encoding="utf-8") == "[a]\nx = 1\n"


def test_write_lines_first_write_has_no_backup(tmp_path):
    import toml_edit
    p = tmp_path / "secrets.toml"
    backup = toml_edit.write_lines(
        p, ["[a]", "x = 1"], backup_dir=tmp_path, backup_prefix="secrets.toml")
    assert backup is None
    assert p.exists()


def test_write_lines_rejects_corrupt_toml_and_preserves_original(tmp_path):
    import toml_edit
    p = tmp_path / "secrets.toml"
    p.write_text("[a]\nx = 1\n", encoding="utf-8")
    with pytest.raises(ValueError):
        toml_edit.write_lines(
            p, ["[a]", "x = = broken"], backup_dir=tmp_path, backup_prefix="secrets.toml")
    assert p.read_text(encoding="utf-8") == "[a]\nx = 1\n"
    assert not (tmp_path / "secrets.toml.tmp").exists()
