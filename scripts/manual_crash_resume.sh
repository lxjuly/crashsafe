#!/usr/bin/env bash

# Proves recovery from the ambiguous outcome: the tool commits, the worker is
# SIGKILLed before recording completion, and the retry reuses the same key.
#
# Start a fresh stack first:
#   rm -rf .crashsafe/manual-crash
#   CRASHSAFE_STATE_DIR=.crashsafe/manual-crash \
#   CRASHSAFE_WORKERS=1 CRASHSAFE_FLAKY_RATE=0 \
#   CRASHSAFE_DELAY_AFTER_TOOL_COMMIT=charge CRASHSAFE_COMMIT_DELAY=10 \
#   CRASHSAFE_REQUEST_TIMEOUT=20 uv run crashsafe-stack
#
# In another terminal, use the same state directory:
#   CRASHSAFE_STATE_DIR=.crashsafe/manual-crash scripts/manual_crash_resume.sh

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
echo "Waiting until the mock tool has durably committed the charge"
wait_for_json "$TOOL_URL/ledger" ".charges == $expected_charges" "one new charge"
curl -fsS "$TOOL_URL/ledger" | jq .

echo
echo "Sending kill -9 to worker $pid while its response is delayed"
kill -9 "$pid"
wait_for_process_exit "$pid"

echo "Waiting for the supervisor's replacement worker to resume the workflow"
wait_for_json "$API_URL/workflows/$workflow_id" '.status == "completed"' "workflow completion"

events="$(curl -fsS "$API_URL/workflows/$workflow_id/events")"
charge_attempts="$(jq '[.[] | select(.event_type == "StepAttemptStarted" and .step_id == "charge")] | length' <<<"$events")"
unique_keys="$(jq '[.[] | select(.event_type == "StepAttemptStarted" and .step_id == "charge") | .payload.operation_key] | unique | length' <<<"$events")"
final_charges="$(curl -fsS "$TOOL_URL/ledger" | jq -r '.charges')"

print_result "History-derived timeline" "$API_URL/workflows/$workflow_id/timeline"
print_result "Durable side-effect ledger" "$TOOL_URL/ledger"
echo
echo "Charge retry evidence"
jq '[.[] | select(.event_type == "StepAttemptStarted" and .step_id == "charge") | {attempt, worker_id: .payload.worker_id, fence_token: .payload.fence_token, operation_key: .payload.operation_key}]' <<<"$events"

if ((charge_attempts < 2)) || ((unique_keys != 1)) || ((final_charges != expected_charges)); then
  echo "FAIL: expected 2+ requests, one stable key, and exactly one new charge" >&2
  exit 1
fi

echo
echo "PASS: the resumed charge used one key and produced exactly one side effect."
