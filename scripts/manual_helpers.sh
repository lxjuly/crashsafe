#!/usr/bin/env bash

# Shared HTTP and polling helpers for the focused manual checks.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_URL="${CRASHSAFE_API_URL:-http://127.0.0.1:8000}"
TOOL_URL="${CRASHSAFE_TOOL_URL:-http://127.0.0.1:8001}"
STATE_DIR="${CRASHSAFE_STATE_DIR:-$ROOT_DIR/.crashsafe}"
WAIT_SECONDS="${CRASHSAFE_MANUAL_TIMEOUT:-30}"
if [[ "$STATE_DIR" != /* ]]; then
  STATE_DIR="$ROOT_DIR/$STATE_DIR"
fi

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_services() {
  require_command curl
  require_command jq
  curl -fsS "$API_URL/healthz" >/dev/null || {
    echo "Crashsafe API is not available at $API_URL" >&2
    exit 1
  }
  curl -fsS "$TOOL_URL/healthz" >/dev/null || {
    echo "Mock tool is not available at $TOOL_URL" >&2
    exit 1
  }
}

create_workflow_run() {
  curl -fsS -X POST "$API_URL/workflow_runs" \
    -H 'content-type: application/json' \
    --data-binary "@$1"
}

wait_for_json() {
  local url="$1"
  local expression="$2"
  local description="$3"
  local deadline=$((SECONDS + WAIT_SECONDS))
  local response

  while ((SECONDS < deadline)); do
    if response="$(curl -fsS "$url" 2>/dev/null)" &&
      jq -e "$expression" >/dev/null 2>&1 <<<"$response"; then
      return 0
    fi
    sleep 0.2
  done

  echo "Timed out waiting for $description" >&2
  return 1
}

wait_for_process_exit() {
  local pid="$1"
  local deadline=$((SECONDS + WAIT_SECONDS))
  while ((SECONDS < deadline)); do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  echo "Timed out waiting for worker $pid to exit" >&2
  return 1
}

worker_pid() {
  local pid_file="$STATE_DIR/worker.pid"
  [[ -s "$pid_file" ]] || {
    echo "Worker PID file not found at $pid_file" >&2
    echo "Start the stack with uv run crashsafe-stack, not only crashsafe-api." >&2
    exit 1
  }
  tr -d '[:space:]' <"$pid_file"
}

print_result() {
  local title="$1"
  local url="$2"
  echo
  echo "$title"
  curl -fsS "$url" | jq .
}
