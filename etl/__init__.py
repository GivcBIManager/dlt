"""Production-grade dlt pipeline for Oracle 11g multi-branch ETL into Iceberg.

Modules
-------
config        - configuration objects + loaders (tables.json, dlt secrets/config)
types_map     - Oracle -> Arrow type mapping and cross-branch schema unification
oracle_extract- connection pooling, query building, threaded extraction, retries
iceberg_load  - dlt pipeline, write strategy, control + log Iceberg tables
progress      - live progress heartbeat + peak-memory probe (low overhead)

The CLI entry point lives in ``oracle_to_iceberg.py`` at the project root.
"""

__all__ = ["config", "types_map", "oracle_extract", "iceberg_load", "progress"]
