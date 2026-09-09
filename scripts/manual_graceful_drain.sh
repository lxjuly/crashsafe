#!/usr/bin/env bash

# Proves SIGTERM drains an in-flight attempt: the worker remains alive long
# enough to persist completion, exits, and the supervisor starts a replacement.
#
# Start a fresh stack first:
#   rm -rf .crashsafe/manual-drain
#   CRASHSAFE_STATE_DIR=.crashsafe/manual-drain \
#   CRASHSAFE_WORKERS=1 CRASHSAFE_FLAKY_RATE=0 \
#   CRASHSAFE_DELAY_AFTER_TOOL_COMMIT=charge CRASHSAFE_COMMIT_DELAY=5 \
#   CRASHSAFE_REQUEST_TIMEOUT=10 uv run crashsafe-stack
#
# Then:
#   CRASHSAFE_STATE_DIR=.crashsafe/manual-drain scripts/manual_graceful_drain.sh

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual_helpers.sh"

require_services
pid="$(worker_pid)"
baseline_charges="$(curl -fsS "$TOOL_URL/ledger" | jq -r '.charges')"
expected_charges=$((baseline_charges + 1))

echo "Submitting paid onboarding workflow"
created="$(submit_workflow "$ROOT_DIR/examples/paid-onboarding.json")"
jq . <<<"$created"
workflow_id="$(jq -er '.id' <<<"$created")"

echo
echo "Waiting until the charge commits while the response remains in flight"
wait_for_json "$TOOL_URL/ledger" ".charges == $expected_charges" "one new charge"

echo "Sending SIGTERM to worker $pid"
kill -TERM "$pid"
sleep 0.2
if ! kill -0 "$pid" 2>/dev/null; then
  echo "FAIL: worker exited immediately instead of draining its in-flight attempt" >&2
  exit 1
fi
echo "Worker is still alive and draining the current attempt"

wait_for_process_exit "$pid"
echo "Original worker exited cleanly; waiting for workflow completion"
wait_for_json "$API_URL/workflows/$workflow_id" '.status == "completed"' "workflow completion"

events="$(curl -fsS "$API_URL/workflows/$workflow_id/events")"
charge_attempts="$(jq '[.[] | select(.event_type == "StepAttemptStarted" and .step_id == "charge")] | length' <<<"$events")"
final_charges="$(curl -fsS "$TOOL_URL/ledger" | jq -r '.charges')"

print_result "History-derived timeline" "$API_URL/workflows/$workflow_id/timeline"
print_result "Durable side-effect ledger" "$TOOL_URL/ledger"

if ((charge_attempts != 1)) || ((final_charges != expected_charges)); then
  echo "FAIL: expected the in-flight charge to finish once without retry" >&2
  exit 1
fi

echo
echo "PASS: SIGTERM drained the in-flight attempt and the workflow completed exactly once."
