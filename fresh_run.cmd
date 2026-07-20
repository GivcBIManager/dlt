@echo off
REM ==========================================================================
REM Clear every runtime artifact so the next pipeline run starts completely
REM fresh.
REM
REM Removes:  Iceberg output, staged parquet, the Postgres app metastore
REM           (etl_meta schema) + Iceberg catalog tables, dlt pipeline working
REM           state, and Python bytecode caches.
REM Keeps:    all source code, tables.json, and .dlt\{config,secrets}.toml.
REM
REM Usage:  fresh_run.cmd
REM ==========================================================================
setlocal

REM Run from the repo root regardless of where the script is invoked.
cd /d "%~dp0"

REM Must match [etl].pipeline_name in .dlt\config.toml.
set "PIPELINE_NAME=oracle_to_iceberg"

echo Clearing runtime artifacts for a fresh run...

REM --- destination data + intermediate state -------------------------------
if exist "iceberg_output"     rmdir /s /q "iceberg_output"
if exist "_staging"           rmdir /s /q "_staging"
del /q *.duckdb 2>nul

REM --- Postgres app metastore + Iceberg catalog reset -----------------------
REM Requires psql on PATH and %OASIS_META_DSN% / %OASIS_CATALOG_DSN% (libpq URLs).
if defined OASIS_META_DSN (
  psql "%OASIS_META_DSN%" -c "DROP SCHEMA IF EXISTS etl_meta CASCADE;"
)
if defined OASIS_CATALOG_DSN (
  psql "%OASIS_CATALOG_DSN%" -c "DROP TABLE IF EXISTS iceberg_tables, iceberg_namespace_properties CASCADE;"
)

REM --- dlt pipeline working state ------------------------------------------
REM Clear the local project copy, the default (%USERPROFILE%\.dlt), and a
REM custom %DLT_DATA_DIR% if set.
if exist ".dlt\pipelines\%PIPELINE_NAME%" rmdir /s /q ".dlt\pipelines\%PIPELINE_NAME%"
if exist "%USERPROFILE%\.dlt\pipelines\%PIPELINE_NAME%" rmdir /s /q "%USERPROFILE%\.dlt\pipelines\%PIPELINE_NAME%"
if defined DLT_DATA_DIR if exist "%DLT_DATA_DIR%\pipelines\%PIPELINE_NAME%" rmdir /s /q "%DLT_DATA_DIR%\pipelines\%PIPELINE_NAME%"

REM --- Python bytecode caches ---------------------------------------------
for /d /r %%d in (__pycache__) do @if exist "%%d" rmdir /s /q "%%d"
del /s /q *.pyc 2>nul

echo Done. Fresh run ready.
endlocal
