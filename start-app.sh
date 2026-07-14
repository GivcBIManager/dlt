#!/usr/bin/env bash
#
# Start the OASIS / HNH ETL control panel (Flask GUI + embedded Dagster).
#
#   ./start-app.sh                    # development mode, foreground
#   ./start-app.sh prod               # production mode, background (detached)
#   ./start-app.sh dev  --background
#   ./start-app.sh prod --foreground
#
# Environments (presets, only applied when not already set in the environment):
#   dev  -> host 127.0.0.1, debug on,  binds to localhost only
#   prod -> host 0.0.0.0,   debug off, listens on every interface
#
# Override any setting with env vars before launching:
#   OASIS_GUI_HOST, OASIS_GUI_PORT (8765), OASIS_GUI_DEBUG,
#   OASIS_GUI_USER / OASIS_GUI_PASSWORD (required for any non-loopback bind;
#     prompted for interactively when missing),
#   OASIS_ALLOW_CUSTOM_CMD (1 to permit the free-form 'custom' run script),
#   OASIS_DAGSTER_AUTOSTART (1), OASIS_DAGSTER_PORT (3000)
#
set -euo pipefail
cd "$(dirname "$0")"

ENVIRONMENT="${1:-dev}"
MODE="${2:-}"

VPY=".venv/bin/python"
if [[ ! -x "$VPY" ]]; then
  echo "ERROR: virtualenv not found at $VPY. Run ./setup.sh first." >&2
  exit 1
fi

case "$ENVIRONMENT" in
  prod)
    : "${OASIS_GUI_HOST:=0.0.0.0}"
    : "${OASIS_GUI_DEBUG:=0}"
    # The panel can launch processes and edit config; a public interface needs
    # login credentials (the app enforces this too). Prompt for any that are
    # missing when a terminal is attached; otherwise fail closed.
    if [[ "$OASIS_GUI_HOST" != "127.0.0.1" && "$OASIS_GUI_HOST" != "::1" \
          && ( -z "${OASIS_GUI_USER:-}" || -z "${OASIS_GUI_PASSWORD:-}" ) ]]; then
      if [[ -t 0 ]]; then
        echo "==> Login credentials required for public bind on $OASIS_GUI_HOST"
        if [[ -z "${OASIS_GUI_USER:-}" ]]; then
          read -r -p "    GUI username: " OASIS_GUI_USER
        fi
        if [[ -z "${OASIS_GUI_PASSWORD:-}" ]]; then
          read -r -s -p "    GUI password: " OASIS_GUI_PASSWORD; echo
        fi
        if [[ -z "$OASIS_GUI_USER" || -z "$OASIS_GUI_PASSWORD" ]]; then
          echo "ERROR: username and password must be non-empty." >&2
          exit 1
        fi
        export OASIS_GUI_USER OASIS_GUI_PASSWORD
      else
        echo "ERROR: refusing to start prod on $OASIS_GUI_HOST without authentication." >&2
        echo "       Set login credentials first, e.g.:" >&2
        echo "         export OASIS_GUI_USER='admin' OASIS_GUI_PASSWORD='<secret>'" >&2
        echo "       Then sign in at  http://<host>:${OASIS_GUI_PORT:-8765}/login" >&2
        echo "       (or bind 127.0.0.1 behind a reverse proxy that handles auth)." >&2
        exit 1
      fi
    fi
    ;;
  dev)
    : "${OASIS_GUI_HOST:=127.0.0.1}"
    : "${OASIS_GUI_DEBUG:=1}"
    ;;
  *)
    echo "ERROR: unknown environment '$ENVIRONMENT' (use 'dev' or 'prod')." >&2
    exit 1
    ;;
esac
: "${OASIS_GUI_PORT:=8765}"
export OASIS_GUI_HOST OASIS_GUI_DEBUG OASIS_GUI_PORT

RUN_DIR="run_logs"
mkdir -p "$RUN_DIR"
PID_FILE="$RUN_DIR/gui-app.pid"
OUT_LOG="gui-server.log"

# Refuse to start a second instance on top of a live one.
if [[ -f "$PID_FILE" ]]; then
  OLD="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$OLD" ]] && kill -0 "$OLD" 2>/dev/null; then
    echo "ERROR: app already running (PID $OLD). Stop it with ./stop-app.sh." >&2
    exit 1
  fi
  rm -f "$PID_FILE"
fi

# prod -> background by default; dev -> foreground. Explicit flags win.
BACKGROUND=0
[[ "$ENVIRONMENT" == "prod" ]] && BACKGROUND=1
[[ "$MODE" == "--background" ]] && BACKGROUND=1
[[ "$MODE" == "--foreground" ]] && BACKGROUND=0

echo "==> Starting control panel [$ENVIRONMENT] on http://${OASIS_GUI_HOST}:${OASIS_GUI_PORT}"

if [[ "$BACKGROUND" == "1" ]]; then
  setsid "$VPY" gui/app.py >>"$OUT_LOG" 2>&1 &
  echo $! >"$PID_FILE"
  echo "    running in background (PID $(cat "$PID_FILE")); logs -> $OUT_LOG"
  echo "    stop with:  ./stop-app.sh"
else
  echo "    running in foreground (Ctrl+C to stop)"
  exec "$VPY" gui/app.py
fi
