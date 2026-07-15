"""Writable staging-lake maintenance: snapshot expiry + orphan file cleanup.

``iceberg_browser`` stays read-only (StaticTable cannot commit); anything that
rewrites table metadata lives here. Tables are opened writable through the ETL
pipeline's Iceberg catalog — the same commit path the loader's own
``apply_snapshot_retention`` uses — so the new metadata files follow dlt's
naming and are picked up by both the loader and the browser.

pyiceberg's ``expire_snapshots`` only removes snapshot entries from metadata;
the data/manifest files they referenced stay on disk. ``expire_snapshots``
here therefore finishes with an orphan sweep: every file under the table's
``data/`` and ``metadata/`` dirs that no remaining snapshot references is
deleted (``*.metadata.json`` and ``version-hint.text`` are always kept).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from config import ICEBERG_ROOT, REPO_ROOT

# metadata jsons + version hint must survive any sweep: they are the table.
_ALWAYS_KEEP = re.compile(r"(\.metadata\.json|^version-hint\.text)$")


def _validated_dir(table: str) -> Path:
    """Reject path tricks / ``_dlt*`` names; require an existing table dir."""
    safe = Path(table).name
    if not safe or safe != table or safe in (".", "..") or safe.startswith("_dlt"):
        raise ValueError(f"snapshots not expirable for table: {table!r}")
    tdir = ICEBERG_ROOT / safe
    if not (tdir / "metadata").is_dir():
        raise FileNotFoundError(table)
    return tdir


def _writable_table(table: str):
    """Open the staging table writable via the ETL pipeline's catalog."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from dlt.common.libs.pyiceberg import get_iceberg_tables

    from etl.config import load_settings
    from etl.iceberg_load import build_pipeline

    pipeline = build_pipeline(load_settings())
    try:
        return get_iceberg_tables(pipeline, table)[table]
    except ValueError as exc:  # unknown to the pipeline schema
        raise FileNotFoundError(table) from exc


def _local_path(uri: str) -> Path | None:
    """Local Path for a lake URI; None when the lake is remote (s3/az/...)."""
    parsed = urlparse(str(uri))
    if parsed.scheme == "file":
        s = unquote(parsed.path)
    elif not parsed.scheme or len(parsed.scheme) == 1:  # bare path / drive letter
        s = str(uri)
    else:
        return None
    if re.match(r"^/[A-Za-z]:", s):  # file:///D:/... -> D:/...
        s = s[1:]
    return Path(s).resolve()


def _expire_to_latest(tbl) -> int:
    """Expire every unprotected snapshot; returns how many were expired."""
    meta = tbl.metadata
    protected = {ref.snapshot_id for ref in meta.refs.values()}
    if meta.current_snapshot_id is not None:
        protected.add(meta.current_snapshot_id)
    ids = [s.snapshot_id for s in meta.snapshots if s.snapshot_id not in protected]
    if ids:
        tbl.maintenance.expire_snapshots().by_ids(ids).commit()
    return len(ids)


def _referenced_files(tbl) -> set[Path] | None:
    """Every local file the remaining snapshots still reference.

    Returns None when any reference is non-local — then the lake is remote
    and the filesystem sweep must be skipped entirely.
    """
    meta, io = tbl.metadata, tbl.io
    uris: list[str] = []
    for snap in meta.snapshots:
        uris.append(snap.manifest_list)
        for manifest in snap.manifests(io):
            uris.append(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(io, discard_deleted=False):
                uris.append(entry.data_file.file_path)
    for stat in list(meta.statistics or []) + list(meta.partition_statistics or []):
        path = getattr(stat, "statistics_path", None)
        if path:
            uris.append(path)
    keep: set[Path] = set()
    for uri in uris:
        local = _local_path(uri)
        if local is None:
            return None
        keep.add(local)
    return keep


def _delete_orphans(tdir: Path, keep: set[Path]) -> tuple[int, int, dict[str, str]]:
    """Delete unreferenced files under data/ + metadata/; prune empty dirs."""
    deleted = 0
    freed = 0
    errors: dict[str, str] = {}
    for sub in ("data", "metadata"):
        base = tdir / sub
        if not base.is_dir():
            continue
        for f in sorted(base.rglob("*")):
            if not f.is_file() or _ALWAYS_KEEP.search(f.name) or f.resolve() in keep:
                continue
            try:
                size = f.stat().st_size
                f.unlink()
                deleted += 1
                freed += size
            except OSError as exc:
                errors[f.relative_to(tdir).as_posix()] = str(exc)
        # deepest-first so emptied partition dirs collapse upwards
        for d in sorted((p for p in base.rglob("*") if p.is_dir()), reverse=True):
            try:
                d.rmdir()
            except OSError:
                pass  # not empty
    return deleted, freed, errors


def expire_snapshots(table: str) -> dict[str, Any]:
    """Expire all snapshots except the latest and sweep orphaned files."""
    tdir = _validated_dir(table)
    tbl = _writable_table(table)
    expired = _expire_to_latest(tbl)
    result: dict[str, Any] = {
        "table": table,
        "expired": expired,
        "remaining": len(tbl.metadata.snapshots),
        "orphans_deleted": 0,
        "bytes_freed": 0,
        "errors": {},
    }
    keep = _referenced_files(tbl)
    if keep is None:
        result["errors"]["cleanup"] = "lake is not on the local filesystem; orphan sweep skipped"
        return result
    deleted, freed, errors = _delete_orphans(tdir, keep)
    result.update(orphans_deleted=deleted, bytes_freed=freed, errors=errors)
    return result
