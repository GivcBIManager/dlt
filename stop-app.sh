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

# The embedded Dagster runs in its own session (see gui/dagster_service.py), so
# the GUI's process group never covers it. TERMing only the `dagster dev`
# supervisor is not enough either: its webserver / daemon / code-server / grpc
# children are re-parented to init and keep running, holding port 3000 and
# heartbeating into the same instance. Kill the whole session, then sweep.
VPY_ABS="$PWD/.venv/bin/python"

dagster_pids() {
  # Scoped to this checkout's venv so a Dagster elsewhere on the box is safe.
  pgrep -f "^${VPY_ABS} -m dagster[ ._]" 2>/dev/null || true
}

signal_dagster() {
  local sig="$1"; shift
  local pid pgid
  for pid in "$@"; do
    # `dagster dev` is its own session leader, so its PGID covers the whole
    # tree; signal the group first, then the PID in case it has already gone.
    pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')"
    [[ -n "$pgid" ]] && kill -"$sig" "-$pgid" 2>/dev/null
    kill -"$sig" "$pid" 2>/dev/null
  done
}

stop_dagster() {
  local pids waited
  pids="$(dagster_pids)"
  [[ -z "$pids" ]] && return 0
  echo "==> Stopping embedded Dagster (SIGTERM): $(echo $pids | tr '\n' ' ')"
  signal_dagster TERM $pids
  # Give it a real grace period: `dagster dev` takes a few seconds to shut its
  # children down, and a run the daemon launched is a separate process we would
  # rather see exit on its own than orphan.
  for ((waited = 0; waited < 15; waited++)); do
    sleep 1
    [[ -z "$(dagster_pids)" ]] && return 0
  done
  pids="$(dagster_pids)"
  echo "==> Still up after 15s, escalating to SIGKILL: $(echo $pids | tr '\n' ' ')"
  signal_dagster KILL $pids
  sleep 2
}

stop_dagster
LEFTOVER="$(dagster_pids)"
if [[ -n "$LEFTOVER" ]]; then
  echo "WARNING: Dagster processes survived SIGKILL: $(echo $LEFTOVER | tr '\n' ' ')" >&2
  echo "         Port 3000 may still be held; investigate before starting again." >&2
else
  echo "    Dagster stopped."
fi

echo "    done."
