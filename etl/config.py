"""Configuration objects and loaders.

Three sources of truth feed the pipeline:

* ``tables.json``            - per-table definitions (key, cdc column, load filters)
* ``.dlt/secrets.toml``      - the ``[oracle_branches.*]`` connection sections
* ``.dlt/config.toml``       - the Iceberg/filesystem destination and ``[etl]`` tuning

This module turns all of that into typed dataclasses the rest of the pipeline
consumes, so nothing downstream pokes at raw dicts or TOML.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import dlt

log = logging.getLogger("etl.config")

# Two load modes drive everything (query building + write disposition).
MODE_INITIAL = "INITIAL"
MODE_INCREMENTAL = "INCREMENTAL"

CATEGORY_MASTER = "masters"
CATEGORY_TRANSACTION = "transactions"
# Append-only "snapshot" tables: the whole table is pulled from every branch on
# every run and appended (never merged/replaced), so each run accumulates a full
# historical copy stamped with a single shared ``version`` timestamp.
CATEGORY_SNAPSHOT = "snapshots"

# --------------------------------------------------------------------------- #
# Timestamps
#
# Every timestamp the pipeline *generates* (the injected ``insert_at`` /
# ``Recorded_updated_at`` data columns plus the etl_control / etl_run_log
# observability columns) uses the **server's local** wall-clock time -- i.e.
# whatever timezone the host running the ETL is configured for, not a fixed zone.
#
# We want these stored as tz-less Iceberg ``timestamp`` so the naive local value
# reads back as that same local wall-clock in any reader. dlt does NOT give us
# that for free: it defaults timestamp columns to ``timezone=True`` and, at write
# time, tags our naive value as UTC *without shifting it* (pyarrow assume_timezone)
# -- so a local 17:00 lands as the instant 17:00Z and a UTC+3 reader renders 20:00.
# We force the tz-less behavior with an explicit ``timezone: False`` column hint on
# every generated time column (see ``iceberg_load._naive_ts_hint`` and the
# etl_control / etl_run_log / etl_dq_results hint maps). (This does NOT touch
# source CDC/date columns or the Iceberg snapshot-expiry cutoff, which stays UTC
# to match Iceberg's own snapshot clock.)
# --------------------------------------------------------------------------- #
def now_local() -> dt.datetime:
    """Naive 'now' in the server's local wall-clock time (for tz-less columns)."""
    return dt.datetime.now()

# Default Oracle fetch/round-trip size used when a branch section does not set
# its own ``fetch_batch_size`` (tuning is per-branch, see BranchConfig).
DEFAULT_FETCH_BATCH_SIZE = 50_000

# --------------------------------------------------------------------------- #
# Helper-driven CDC (tables with no CDC column of their own)
#
# A table that lacks a usable CDC/date column can borrow one from a parent/helper
# table over a declared join (e.g. ORDER_LINES -> ORDERS_MASTER on
# MASTER_ORDER_NO, using ORDERS_MASTER.AMEND_LAST_DATE / ORDER_DATE). The
# helper's cdc/date columns are projected into the SELECT under these reserved
# aliases so the normal watermark-from-parquet logic captures them; they are
# stripped before the Iceberg write so they never land in the lake. The aliases
# start with a letter, so they are valid *unquoted* Oracle identifiers.
# --------------------------------------------------------------------------- #
HELPER_CDC_ALIAS = "ETL_HELPER_CDC"
HELPER_DATE_ALIAS = "ETL_HELPER_DATE"
HELPER_RESERVED_COLUMNS = {HELPER_CDC_ALIAS, HELPER_DATE_ALIAS}

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")


def _normalize_name(raw: str) -> str:
    """Lower-snake a name so it is a stable, filesystem-safe dataset/table id."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", raw).strip("_").lower()


@dataclass(frozen=True)
class HelperJoin:
    """A parent/helper table that supplies CDC for a column-less child table.

    The child is INNER-joined to the helper on ``join_keys`` (a list of
    ``(child_column, helper_column)`` equi-join pairs, supporting composite keys),
    and the helper's ``cdc_column`` / ``where_date_column`` drive the incremental
    filter and watermark in place of columns the child does not have.
    """

    table: str                              # helper table, e.g. "OASIS.ORDERS_MASTER"
    join_keys: tuple[tuple[str, str], ...]  # ((child_col, helper_col), ...)
    cdc_column: str                         # helper column for "updated" rows
    where_date_column: Optional[str]        # helper column for "new" rows (optional)


@dataclass(frozen=True)
class TableDef:
    """A single source table and how it should be loaded."""

    table: str                              # e.g. "OASIS.STAFF_MASTER_DATA"
    unique_key: Optional[str]               # column, "A,B", SQL expression, or None (snapshots)
    cdc_column: Optional[str]               # column used for INCREMENTAL "updated" rows
    where_date_column: Optional[str]        # column used for INCREMENTAL "new" rows
    where_operator: Optional[str]           # operator for the INITIAL range filter
    where_value_of_initial_run: Optional[str]  # value/expression for the INITIAL range
    category: str                           # CATEGORY_MASTER | CATEGORY_TRANSACTION
    helper: Optional[HelperJoin] = None     # parent table supplying CDC (no-CDC tables)
    # Optional *upper* bound on ``where_date_column``, turning the open-ended
    # ``date >= floor`` filter into a bounded window ``floor <= date <= ceiling``.
    # Needed for tables whose date column is a *future* (scheduled) date -- e.g.
    # APPOINTMENTS.JULIAN_DATE -- where ``>= today`` would otherwise pull the
    # entire forward booking book. Applies to both the INITIAL filter and the
    # INCREMENTAL "new rows" branch (the value is usually a rolling SYSDATE
    # expression so the ceiling moves forward each run).
    where_value_max: Optional[str] = None     # upper-bound value/expression (window ceiling)
    where_operator_max: Optional[str] = None  # operator for the upper bound (default "<=")
    # Explicit Iceberg table name. Only meaningful (and required) when ``table``
    # is an inline-view subquery source -- plain tables keep deriving their name
    # from the OWNER.TABLE identifier.
    name: Optional[str] = None

    # ----- derived identifiers ------------------------------------------------
    @property
    def is_query(self) -> bool:
        """True when ``table`` is an inline-view subquery, not an identifier."""
        return self.table.lstrip().startswith("(")

    @property
    def owner(self) -> str:
        if self.is_query:
            return ""
        return self.table.split(".", 1)[0] if "." in self.table else ""

    @property
    def object_name(self) -> str:
        if self.is_query:
            return self.name or ""
        return self.table.split(".", 1)[1] if "." in self.table else self.table

    @property
    def dataset_table_name(self) -> str:
        """Normalized name used as the Iceberg table / dlt resource name."""
        return _normalize_name(self.object_name)

    @property
    def is_master(self) -> bool:
        return self.category == CATEGORY_MASTER

    @property
    def is_snapshot(self) -> bool:
        """True for append-only snapshot tables (full copy stamped per run)."""
        return self.category == CATEGORY_SNAPSHOT

    # ----- helper-driven CDC --------------------------------------------------
    @property
    def is_helper_driven(self) -> bool:
        """True when CDC/date are borrowed from a parent table via a join."""
        return self.helper is not None

    @property
    def cdc_capture_column(self) -> Optional[str]:
        """Parquet column the CDC watermark is read back from.

        For a helper-driven table the helper's cdc is projected under
        ``HELPER_CDC_ALIAS``; otherwise it is the table's own ``cdc_column``.
        """
        if self.helper is not None:
            return HELPER_CDC_ALIAS
        return self.cdc_column

    @property
    def date_capture_column(self) -> Optional[str]:
        """Parquet column the date watermark is read back from (or None)."""
        if self.helper is not None:
            return HELPER_DATE_ALIAS if self.helper.where_date_column else None
        return self.where_date_column

    # ----- key handling -------------------------------------------------------
    @property
    def key_is_expression(self) -> bool:
        """True when unique_key is a SQL expression rather than plain column(s)."""
        if not self.unique_key:
            return False
        parts = [p.strip() for p in self.unique_key.split(",")]
        return not all(_IDENT_RE.match(p) for p in parts)

    @property
    def derived_key_alias(self) -> str:
        return "DERIVED_KEY"

    @property
    def key_columns(self) -> list[str]:
        """Physical key column names used for merge upserts.

        Expression keys are projected into a single ``DERIVED_KEY`` column in the
        SELECT, so the merge key is that alias; simple keys split on commas.
        Snapshot (append-only) tables have no merge key, so this is empty.
        """
        if not self.unique_key:
            return []
        if self.key_is_expression:
            return [self.derived_key_alias]
        return [p.strip() for p in self.unique_key.split(",") if p.strip()]


@dataclass(frozen=True)
class BranchConfig:
    """Connection details for one OASIS Oracle branch."""

    key: str            # section key, e.g. "alrabwah" (what --branch matches)
    name: str
    id: int             # numeric branch id stamped into the BRANCH_ID column
    host: str
    port: int
    username: str
    password: str
    database: str       # service name or SID depending on Settings.dsn_mode

    # Oracle fetch/round-trip size, tuned per branch (high-latency links want a
    # bigger batch to amortize the round trip). Falls back to
    # DEFAULT_FETCH_BATCH_SIZE when the branch section omits it.
    fetch_batch_size: int = DEFAULT_FETCH_BATCH_SIZE

    def dsn(self, dsn_mode: str) -> str:
        import oracledb

        if dsn_mode == "sid":
            return oracledb.makedsn(self.host, self.port, sid=self.database)
        return oracledb.makedsn(self.host, self.port, service_name=self.database)


@dataclass
class Settings:
    """Runtime tuning + destination configuration."""

    mode: str = MODE_INCREMENTAL

    # concurrency: outer pool over branches, inner pool over tables per branch
    max_branch_workers: int = 7
    max_table_workers: int = 3

    # Oracle connection pool (per branch) + acquire backoff
    pool_min: int = 1
    pool_max: int = 4
    pool_increment: int = 1
    pool_acquire_timeout_s: int = 30          # wait per acquire before giving up
    pool_acquire_attempts: int = 5            # acquire retries on pool exhaustion
    pool_backoff_base_s: float = 2.0          # exponential backoff base
    pool_backoff_cap_s: float = 60.0

    # connection-failure retry policy (the "5 retries every 5 minutes" rule)
    max_retries: int = 5
    retry_interval_s: int = 300

    dsn_mode: str = "service_name"            # "service_name" | "sid"

    # Oracle 11g requires python-oracledb *thick* mode (thin needs 12.1+).
    thick_mode: bool = True
    oracle_client_lib_dir: Optional[str] = None

    # destination
    destination_bucket_url: str = ""
    dataset_name: str = "oasis"
    pipeline_name: str = "oracle_to_iceberg"

    # injected business columns
    branch_id_column: str = "BRANCH_ID"
    recorded_ts_column: str = "Recorded_updated_at"   # ETL last-load time (updates every run)
    inserted_ts_column: str = "insert_at"             # ETL first-load time (preserved across updates)
    merge_hash_column: str = "merge_hash"             # single-column merge key derived from PK+BRANCH_ID

    # snapshot (append-only) tables: a single per-run timestamp stamped into every
    # record of every branch (``version``), plus a derived date used purely as the
    # snapshot-date partition column. ``snapshot_ts`` is set once per run (see
    # oracle_to_iceberg.main) so all branches share the exact same value.
    snapshot_version_column: str = "version"          # run timestamp, identical across all branches
    snapshot_date_column: str = "version_date"        # date(version), snapshot-date partition
    snapshot_ts: Optional[dt.datetime] = None

    # Iceberg snapshot retention (keep this many days; min always retained)
    snapshot_maintenance: bool = True
    snapshot_expire_days: int = 7
    snapshot_min_to_keep: int = 1

    # live progress heartbeat + peak-memory probe (background daemon thread)
    progress_enabled: bool = True
    progress_interval_s: float = 5.0

    # load streaming: rows per Arrow batch when reading staged parquet into the
    # Iceberg load. Caps load-time memory vs reading a whole branch file at once.
    load_batch_rows: int = 50_000

    # Watchdog on each blocking Iceberg commit (``pipeline.run``). A pyiceberg
    # commit can hang forever with no error, deadlocking the whole run; if a
    # single commit exceeds this many seconds it is abandoned and that table is
    # marked FAILED so the run proceeds. Generous enough that a legitimate
    # (even minutes-long) large-branch commit never trips it. 0 disables it.
    load_commit_timeout_s: int = 900

    # DQ: tolerate row-hash drift up to this percent of a (table, branch)'s
    # Oracle hashed rows before flagging MISMATCH; at or below it the status is
    # WITHIN_TOLERANCE. Row-count drift is always a hard MISMATCH.
    dq_hash_delta_tolerance_pct: float = 10.0

    # local working state
    staging_dir: Path = field(default_factory=lambda: Path("_staging"))
    control_state_path: Path = field(default_factory=lambda: Path("control_state.json"))

    self_test: bool = False

    def __post_init__(self):
        # merge_hash is an internal derived column; force its name lowercase so it
        # always matches dlt's normalized Iceberg field across the write, readiness,
        # merge-join, and carry-forward paths.
        self.merge_hash_column = self.merge_hash_column.lower()

    @property
    def control_table_name(self) -> str:
        return "etl_control"

    @property
    def log_table_name(self) -> str:
        return "etl_run_log"


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #
def _parse_helper(entry: dict) -> Optional[HelperJoin]:
    """Build a HelperJoin from a table entry's optional ``helper`` block.

    Expects ``{"table", "join": [[child, helper], ...], "cdc_column",
    "where_date_column"?}``. Validates the shape so a misconfigured join fails
    loudly at load time rather than producing broken SQL.
    """
    raw = entry.get("helper")
    if not raw:
        return None

    table_name = entry.get("table", "<unknown>")
    helper_table = str(raw.get("table") or "").strip()
    cdc_column = str(raw.get("cdc_column") or "").strip()
    pairs_raw = raw.get("join") or raw.get("join_keys") or []

    if not helper_table:
        raise ValueError(f"helper for {table_name} is missing 'table'")
    if not cdc_column:
        raise ValueError(f"helper for {table_name} is missing 'cdc_column'")
    if not pairs_raw:
        raise ValueError(f"helper for {table_name} is missing 'join' key pairs")

    join_keys: list[tuple[str, str]] = []
    for pair in pairs_raw:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise ValueError(
                f"helper for {table_name}: each 'join' entry must be a "
                f"[child_column, helper_column] pair, got {pair!r}"
            )
        child_col, helper_col = str(pair[0]).strip(), str(pair[1]).strip()
        if not child_col or not helper_col:
            raise ValueError(
                f"helper for {table_name}: empty column in join pair {pair!r}")
        join_keys.append((child_col, helper_col))

    if entry.get("cdc_column"):
        log.warning(
            "Table %s declares both its own cdc_column (%s) and a helper; "
            "the helper (%s.%s) takes precedence for CDC.",
            table_name, entry.get("cdc_column"), helper_table, cdc_column,
        )

    where_date = raw.get("where_date_column")
    return HelperJoin(
        table=helper_table,
        join_keys=tuple(join_keys),
        cdc_column=cdc_column,
        where_date_column=str(where_date).strip() if where_date else None,
    )


def load_table_defs(path: Path) -> list[TableDef]:
    """Parse tables.json into TableDef objects (masters + transactions)."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    defs: list[TableDef] = []
    for category in (CATEGORY_MASTER, CATEGORY_TRANSACTION, CATEGORY_SNAPSHOT):
        for entry in data.get(category, []):
            tdef = TableDef(
                table=entry["table"],
                unique_key=entry.get("unique_key"),
                cdc_column=entry.get("cdc_column"),
                where_date_column=entry.get("where_date_column"),
                where_operator=entry.get("where_operator"),
                where_value_of_initial_run=entry.get("where_value_of_initial_run"),
                category=category,
                helper=_parse_helper(entry),
                where_value_max=entry.get("where_value_max"),
                where_operator_max=entry.get("where_operator_max"),
                name=entry.get("name"),
            )
            if tdef.is_query and not (tdef.name or "").strip():
                raise ValueError(
                    f"{category} entry uses a subquery source and requires a "
                    f"'name' (the Iceberg table name): {entry['table'][:80]}"
                )
            defs.append(tdef)
    if not defs:
        raise ValueError(f"No table definitions found in {path}")
    return defs


def load_branches(
    default_fetch_batch_size: int = DEFAULT_FETCH_BATCH_SIZE,
) -> dict[str, BranchConfig]:
    """Read every ``[oracle_branches.*]`` section from .dlt/secrets.toml.

    ``fetch_batch_size`` is tuned per branch: each section may set its own value
    (high-latency branches benefit from a larger round trip); sections that omit
    it fall back to ``default_fetch_batch_size``.
    """
    raw = dlt.secrets.get("oracle_branches") or {}
    branches: dict[str, BranchConfig] = {}
    for key, sec in raw.items():
        branches[key] = BranchConfig(
            key=key,
            name=str(sec.get("name", key)),
            id=int(sec["id"]),
            host=str(sec["host"]),
            port=int(sec["port"]),
            username=str(sec["username"]),
            password=str(sec["password"]),
            database=str(sec["database"]),
            fetch_batch_size=int(sec.get("fetch_batch_size", default_fetch_batch_size)),
        )
    if not branches:
        raise ValueError("No [oracle_branches.*] sections found in .dlt/secrets.toml")
    return branches


def _cfg(key: str, default: Any) -> Any:
    val = dlt.config.get(key)
    return default if val is None else val


# --------------------------------------------------------------------------- #
# Cross-platform path/location resolution (Windows <-> Linux)
# --------------------------------------------------------------------------- #
def _repo_root() -> Path:
    """Project root (the dir holding tables.json / .dlt). Used to resolve a
    scheme-less destination path the same way regardless of OS or CWD."""
    return Path(__file__).resolve().parent.parent


def resolve_bucket_url(raw: Optional[str]) -> str:
    """Make the Iceberg destination location portable across Windows and Linux.

    Priority: ``$OASIS_BUCKET_URL`` > the configured value > the default
    ``<repo_root>/iceberg_output``. A value that already carries a scheme
    (``file://``, ``s3://``, ...) is used verbatim; a plain or relative path is
    resolved against the repo root and emitted as a ``file://`` URI via
    ``Path.as_uri()``, so the *same* config.toml yields a valid URL on either OS.
    """
    raw = os.environ.get("OASIS_BUCKET_URL") or raw or "iceberg_output"
    if "://" in raw:
        return raw
    p = Path(raw)
    if not p.is_absolute():
        p = _repo_root() / p
    return p.resolve().as_uri()


def resolve_oracle_client_lib_dir(configured: Optional[str]) -> Optional[str]:
    """Pick an Instant Client dir that actually exists on *this* host.

    Priority: ``$ORACLE_CLIENT_LIB_DIR`` > the configured value > ``None``. A
    path that does not exist on the current OS (e.g. a Windows path seen on
    Linux) is dropped so thick-mode init falls back to the system loader path
    (``PATH`` on Windows, ``LD_LIBRARY_PATH`` on Linux) instead of crashing.
    """
    lib = os.environ.get("ORACLE_CLIENT_LIB_DIR") or configured
    if lib and not Path(lib).is_dir():
        return None
    return lib or None


def load_settings(overrides: Optional[dict[str, Any]] = None) -> Settings:
    """Build Settings from .dlt/config.toml ``[etl]`` section, then CLI overrides."""
    bucket_url = resolve_bucket_url(dlt.config.get("destination.filesystem.bucket_url"))

    s = Settings(
        max_branch_workers=int(_cfg("etl.max_branch_workers", 7)),
        max_table_workers=int(_cfg("etl.max_table_workers", 3)),
        pool_min=int(_cfg("etl.pool_min", 1)),
        pool_max=int(_cfg("etl.pool_max", 4)),
        pool_increment=int(_cfg("etl.pool_increment", 1)),
        pool_acquire_timeout_s=int(_cfg("etl.pool_acquire_timeout_s", 30)),
        pool_acquire_attempts=int(_cfg("etl.pool_acquire_attempts", 5)),
        pool_backoff_base_s=float(_cfg("etl.pool_backoff_base_s", 2.0)),
        pool_backoff_cap_s=float(_cfg("etl.pool_backoff_cap_s", 60.0)),
        max_retries=int(_cfg("etl.max_retries", 5)),
        retry_interval_s=int(_cfg("etl.retry_interval_s", 300)),
        dsn_mode=str(_cfg("etl.dsn_mode", "service_name")),
        thick_mode=bool(_cfg("etl.thick_mode", True)),
        oracle_client_lib_dir=resolve_oracle_client_lib_dir(
            _cfg("etl.oracle_client_lib_dir", None)),
        destination_bucket_url=str(bucket_url),
        dataset_name=str(_cfg("etl.dataset_name", "oasis")),
        pipeline_name=str(_cfg("etl.pipeline_name", "oracle_to_iceberg")),
        snapshot_maintenance=bool(_cfg("etl.snapshot_maintenance", True)),
        snapshot_expire_days=int(_cfg("etl.snapshot_expire_days", 7)),
        snapshot_min_to_keep=int(_cfg("etl.snapshot_min_to_keep", 1)),
        progress_enabled=bool(_cfg("etl.progress_enabled", True)),
        progress_interval_s=float(_cfg("etl.progress_interval_s", 5.0)),
        load_batch_rows=int(_cfg("etl.load_batch_rows", 50_000)),
        load_commit_timeout_s=int(_cfg("etl.load_commit_timeout_s", 900)),
        dq_hash_delta_tolerance_pct=float(_cfg("etl.dq_hash_delta_tolerance_pct", 10.0)),
    )

    for key, value in (overrides or {}).items():
        if value is None:
            continue
        if not hasattr(s, key):
            raise AttributeError(f"Unknown setting override: {key}")
        setattr(s, key, value)

    # normalize Path-typed fields that may arrive as strings
    s.staging_dir = Path(s.staging_dir)
    s.control_state_path = Path(s.control_state_path)
    return s
