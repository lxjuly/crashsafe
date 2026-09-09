#!/usr/bin/env bash

# Proves a deterministic HTTP 429 is retried using the provider's Retry-After.
#
# Start a fresh stack first:
#   rm -rf .crashsafe/manual-retry
#   CRASHSAFE_STATE_DIR=.crashsafe/manual-retry \
#   CRASHSAFE_FLAKY_RATE=0 CRASHSAFE_FAIL_FIRST_N=1 \
#   CRASHSAFE_RETRY_AFTER=2 uv run crashsafe-stack
#
# Then:
#   CRASHSAFE_STATE_DIR=.crashsafe/manual-retry scripts/manual_safe_retries.sh

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual_helpers.sh"

require_services
baseline_charges="$(curl -fsS "$TOOL_URL/ledger" | jq -r '.charges')"
expected_charges=$((baseline_charges + 1))

echo "Submitting a workflow; the first tool request should receive HTTP 429"
created="$(submit_workflow "$ROOT_DIR/examples/paid-onboarding.json")"
jq . <<<"$created"
workflow_id="$(jq -er '.id' <<<"$created")"

wait_for_json "$API_URL/workflows/$workflow_id/events" '[.[] | select(.event_type == "StepRetryScheduled")] | length >= 1' "a persisted retry"
echo
echo "Persisted retry event"
curl -fsS "$API_URL/workflows/$workflow_id/events" |
  jq '[.[] | select(.event_type == "StepRetryScheduled") | {step_id, attempt, error: .payload.error, next_attempt_at: .payload.next_attempt_at}]'

echo
echo "Waiting for the Retry-After window and workflow completion"
wait_for_json "$API_URL/workflows/$workflow_id" '.status == "completed"' "workflow completion"

events="$(curl -fsS "$API_URL/workflows/$workflow_id/events")"
retries="$(jq '[.[] | select(.event_type == "StepRetryScheduled")] | length' <<<"$events")"
final_charges="$(curl -fsS "$TOOL_URL/ledger" | jq -r '.charges')"

print_result "History-derived timeline" "$API_URL/workflows/$workflow_id/timeline"
print_result "Durable side-effect ledger" "$TOOL_URL/ledger"

if ((retries < 1)) || ((final_charges != expected_charges)); then
  echo "FAIL: expected a persisted retry and exactly one new charge" >&2
  exit 1
fi

echo
echo "PASS: the 429 produced a persisted retry, then completed without duplicating the charge."
