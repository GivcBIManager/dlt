#!/usr/bin/env bash
#
# One-shot setup + launcher for the OASIS control panel (Ubuntu/Linux/macOS).
#
#   ./setup.sh                 # create/refresh .venv, install deps, provision
#                              # Postgres, start the GUI
#   ./setup.sh --no-start      # install only, don't launch
#   ./setup.sh --no-postgres   # skip the Postgres provisioning step
#   PYTHON=python3.12 ./setup.sh
#   OASIS_GUI_PORT=9000 ./setup.sh
#
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3}"
VENV="${VENV:-.venv}"
START=1
POSTGRES=1
[[ -n "${OASIS_SKIP_POSTGRES:-}" ]] && POSTGRES=0
for arg in "$@"; do
  case "$arg" in
    --no-start)    START=0 ;;
    --no-postgres) POSTGRES=0 ;;
    *) echo "ERROR: unknown option '$arg' (use --no-start / --no-postgres)" >&2; exit 1 ;;
  esac
done

if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "ERROR: '$PYTHON' not found. Install Python 3.10+ (set PYTHON=... to override)." >&2
  exit 1
fi

# A $VENV dir without bin/activate is not a usable Linux venv — typically a
# Windows venv that came along with a folder copy (Scripts/ instead of bin/) or
# the debris of a venv creation that died halfway (missing python3-venv).
# Rebuild it rather than failing at the source line below.
if [[ -d "$VENV" && ! -f "$VENV/bin/activate" ]]; then
  echo "==> $VENV exists but has no bin/activate (Windows copy or broken venv) — recreating"
  rm -rf "$VENV"
fi

if [[ ! -d "$VENV" ]]; then
  echo "==> Creating virtual environment at $VENV"
  if ! "$PYTHON" -m venv "$VENV"; then
    rm -rf "$VENV"   # don't leave a half-built venv for the next run to trip on
    echo "ERROR: venv creation failed. On Ubuntu/Debian install the venv module first:" >&2
    echo "       sudo apt install ${PYTHON}-venv   (e.g. python3.12-venv)" >&2
    exit 1
  fi
fi

# shellcheck disable=SC1091
source "$VENV/bin/activate"

echo "==> Upgrading pip"
python -m pip install --upgrade pip >/dev/null

echo "==> Installing dependencies (pipeline + GUI)"
python -m pip install -r requirements-gui.txt

echo "==> Installing the orchestrator code location (editable)"
python -m pip install -e orchestrator

# Postgres (external server, app-owned databases). Runs after the installs
# because it needs psycopg2 + the etl package from the venv. Non-fatal: an
# unreachable/unconfigured server must not abort setup — the GUI still runs and
# the operator can fix secrets.toml and re-run `python setup_postgres.py`.
PG_STATUS=""
if [[ "$POSTGRES" == "1" ]]; then
  echo "==> Provisioning Postgres (oasis_catalog + oasis_meta)"
  set +e
  python setup_postgres.py
  rc=$?
  set -e
  case "$rc" in
    0) PG_STATUS="" ;;
    2) PG_STATUS="Reminder: Postgres is NOT configured in .dlt/secrets.toml — the pipeline
          cannot run without it. Add [postgres] + [iceberg_catalog.iceberg_catalog_config]
          (see README.md 'Postgres'), then re-run: python setup_postgres.py" ;;
    *) PG_STATUS="WARNING: Postgres provisioning failed (see the error above). Fix the
          server/credentials, then re-run: python setup_postgres.py" ;;
  esac
else
  echo "==> Skipping Postgres provisioning (--no-postgres)"
fi

echo
echo "Setup complete. Virtualenv: $VENV"
if [[ -n "$PG_STATUS" ]]; then
  echo "$PG_STATUS"
fi
echo "Reminder: Oracle 11g needs the Instant Client (thick mode) on this host —"
echo "          see README.md 'Oracle Instant Client'. The GUI itself runs without it,"
echo "          but launching real (non --self-test) extractions does not."
echo "Reminder: ClickHouse (24.x+) is an EXTERNAL prerequisite for the dbt layer and"
echo "          must be able to read the iceberg_output/ path used in icebergLocal()."
echo

if [[ "$START" == "1" ]]; then
  PORT="${OASIS_GUI_PORT:-8765}"
  echo "==> Starting OASIS control panel on http://127.0.0.1:${PORT}"
  exec python gui/app.py
else
  echo "Run the GUI later with:  source $VENV/bin/activate && python gui/app.py"
fi
