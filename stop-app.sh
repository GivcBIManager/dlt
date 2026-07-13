#!/usr/bin/env bash
#
# Stop the OASIS / HNH ETL control panel started by start-app.sh.
#
# Kills the background Flask GUI process group and the embedded Dagster
# webserver + daemon (which runs in its own session).
#
#   ./stop-app.sh
#
set -uo pipefail
cd "$(dirname "$0")"

PID_FILE="run_logs/gui-app.pid"

stop_pid() {
  local pid="$1"
  if kill -0 "$pid" 2>/dev/null; then
    echo "==> Stopping control panel (PID $pid)..."
    kill -TERM "-$pid" 2>/dev/null || true   # process group
    kill -TERM "$pid" 2>/dev/null || true
    sleep 2
    kill -KILL "-$pid" 2>/dev/null || true
    kill -KILL "$pid" 2>/dev/null || true
  fi
}

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  [[ -n "$PID" ]] && stop_pid "$PID"
  rm -f "$PID_FILE"
else
  echo "No PID file found ($PID_FILE)."
fi

# The embedded Dagster runs in its own session; clean it up too.
if pkill -f "dagster dev -m orchestrator.definitions" 2>/dev/null; then
  echo "==> Stopped embedded Dagster."
fi

echo "    done."
