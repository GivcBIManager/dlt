"""Shared surgical-edit helpers for the TOML secret/config files.

Each editor (connections / smtp / clickhouse / workspace) rewrites only the
block it owns and leaves every other line intact. This module centralises the
identical read + backup + validate + atomic-replace + prune machinery so the
durability and (for secrets) hardening behaviour stays consistent in one place.
"""
from __future__ import annotations

import re
import shutil
from datetime import datetime
from pathlib import Path

try:
    import tomllib as _toml
except ModuleNotFoundError:  # Python < 3.11
    import tomli as _toml  # type: ignore

import security

# ``[section]`` header line, capturing the section name.
SECTION_RE = re.compile(r"^\s*\[([^\]]+)\]")


def read_lines(path: Path) -> list[str]:
    """The file's lines (empty list when the file is absent)."""
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def write_lines(path: Path, lines: list[str], *, backup_dir: Path,
                backup_prefix: str, harden: bool = False) -> Path | None:
    """Atomically rewrite ``path`` from ``lines``, keeping a pruned backup.

    Writes to a temp file, re-parses it as TOML to refuse corrupt output, then
    atomically replaces the target. ``harden=True`` restricts the file and its
    backup to owner-only (for secret files). Returns the backup path, or ``None``
    when the target did not exist yet.
    """
    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"

    backup: Path | None = None
    if path.exists():
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = backup_dir / f"{backup_prefix}.{stamp}.bak"
        shutil.copy2(path, backup)
        if harden:
            security.harden_file(backup)

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    # Validate before committing; never leave a corrupt secrets/config file.
    try:
        with tmp.open("rb") as fh:
            _toml.load(fh)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ValueError(f"refused to write corrupt {path.name}: {exc}") from exc
    tmp.replace(path)

    if harden:
        security.harden_file(path)
    security.prune_backups(backup_dir, f"{backup_prefix}.*.bak")
    return backup
