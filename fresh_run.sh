#!/usr/bin/env bash
#
# Clear every runtime artifact so the next pipeline run starts completely fresh.
#
# Removes:  Iceberg output, staged parquet, the local watermark store, dlt
#           pipeline working state, and Python bytecode caches.
# Keeps:    all source code, tables.json, and .dlt/{config,secrets}.toml.
#
# Usage:  ./fresh_run.sh
set -euo pipefail

# Run from the repo root regardless of where the script is invoked.
cd "$(dirname "$0")"

# Must match [etl].pipeline_name in .dlt/config.toml.
PIPELINE_NAME="oracle_to_iceberg"

echo "Clearing runtime artifacts for a fresh run..."

# --- destination data + intermediate state -------------------------------- #
rm -rf  iceberg_output
rm -rf  _staging
rm -f   control_state.json
rm -f   ./*.duckdb

# --- dlt pipeline working state ------------------------------------------- #
# dlt keeps load packages / schema state under its data dir. Clear the local
# project copy, the default (~/.dlt), and a custom $DLT_DATA_DIR if set.
rm -rf  ".dlt/pipelines/${PIPELINE_NAME}"
rm -rf  "${HOME}/.dlt/pipelines/${PIPELINE_NAME}"
if [ -n "${DLT_DATA_DIR:-}" ]; then
  rm -rf "${DLT_DATA_DIR%/}/pipelines/${PIPELINE_NAME}"
fi

# --- Python bytecode caches ----------------------------------------------- #
find . -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
find . -type f -name '*.pyc' -delete 2>/dev/null || true

echo "Done. Fresh run ready."
