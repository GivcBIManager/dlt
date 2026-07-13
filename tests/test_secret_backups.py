"""Secret backup hardening: prune old copies, restrict permissions."""
from __future__ import annotations

import os

import pytest


def _make(dir_, name):
    p = dir_ / name
    p.write_text("secret", encoding="utf-8")
    return p


def test_prune_backups_keeps_newest_n(tmp_path):
    import security
    # timestamped names sort chronologically by name
    for stamp in ("20260101-000000", "20260102-000000", "20260103-000000",
                  "20260104-000000", "20260105-000000"):
        _make(tmp_path, f"secrets.toml.{stamp}.bak")
    security.prune_backups(tmp_path, "secrets.toml.*.bak", keep=2)
    remaining = sorted(p.name for p in tmp_path.glob("secrets.toml.*.bak"))
    assert remaining == ["secrets.toml.20260104-000000.bak",
                         "secrets.toml.20260105-000000.bak"]


def test_prune_backups_ignores_other_files(tmp_path):
    import security
    _make(tmp_path, "secrets.toml.20260101-000000.bak")
    keep_me = _make(tmp_path, "secrets.toml")  # the live file, not a backup
    tmp_file = _make(tmp_path, "secrets.toml.tmp")
    security.prune_backups(tmp_path, "secrets.toml.*.bak", keep=0)
    assert keep_me.exists() and tmp_file.exists()
    assert not list(tmp_path.glob("secrets.toml.*.bak"))


def test_prune_backups_noop_when_under_limit(tmp_path):
    import security
    _make(tmp_path, "secrets.toml.20260101-000000.bak")
    security.prune_backups(tmp_path, "secrets.toml.*.bak", keep=10)
    assert len(list(tmp_path.glob("secrets.toml.*.bak"))) == 1


def test_harden_file_does_not_raise_and_keeps_readable(tmp_path):
    import security
    p = _make(tmp_path, "secrets.toml")
    security.harden_file(p)  # must not raise on any OS
    assert p.read_text(encoding="utf-8") == "secret"


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits only")
def test_harden_file_sets_owner_only_mode_on_posix(tmp_path):
    import security
    p = _make(tmp_path, "secrets.toml")
    security.harden_file(p)
    assert (p.stat().st_mode & 0o777) == 0o600
