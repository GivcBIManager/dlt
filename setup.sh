#!/usr/bin/env bash
#
# One-shot setup + launcher for the OASIS control panel (Ubuntu/Linux/macOS).
#
#   ./setup.sh              # create/refresh .venv, install deps, start the GUI
#   ./setup.sh --no-start   # install only, don't launch
#   PYTHON=python3.12 ./setup.sh
#   OASIS_GUI_PORT=9000 ./setup.sh
#
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3}"
VENV="${VENV:-.venv}"
START=1
[[ "${1:-}" == "--no-start" ]] && START=0

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

echo
echo "Setup complete. Virtualenv: $VENV"
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
