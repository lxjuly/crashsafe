# Feature testing manual

Run the setup once from the repository root:

```bash
make setup
```

## 1. Crash safety and one external charge

```bash
make demo
```

The demo starts isolated engine and tool databases, waits until the charge is
durably committed, sends the worker `kill -9`, and starts a replacement worker.
Verify the final output shows:

```text
engine charge state: intent_recorded
charge attempts: [1, 2]
operation keys: ['<same key>', '<same key>']
workflow status: completed
event/projection audit consistent: True
final durable ledger: {'charges': 1, 'provisions': 1, 'notifications': 1}
PASS
```

This proves durable recovery and at-least-once requests. The single charge is
provided by the mock tool's durable idempotency transaction, not by claiming
general exactly-once execution in the engine.

## 2. Safe retries

```bash
.venv/bin/pytest -q \
  tests/test_engine.py::test_retry_schedule_and_idempotency_key_survive_retry \
  tests/test_engine.py::test_committed_retry_time_does_not_change_with_configuration
```

Expected result: `2 passed`. These checks force a retryable HTTP-style failure
and verify that the engine persists `retry_wait`, the absolute
`next_attempt_at`, and the unchanged operation key. Reopening storage with a
different backoff configuration does not alter the committed retry time.

## 3. Graceful drain

```bash
.venv/bin/pytest -q \
  tests/test_process_recovery.py::test_sigterm_drains_in_flight_step_then_restart_resumes
```

Expected result: `1 passed`. This launches real processes, sends `SIGTERM` while
the mock tool is delaying a committed charge response, and verifies that the
worker:

1. finishes and commits only the in-flight charge;
2. exits with status 0 without starting provision; and
3. resumes after restart with exactly one charge, provision, and notification.

The deployment termination grace period must exceed the configured tool-request
timeout. If the process is forcibly killed instead, crash-safe recovery applies.

## Complete verification

```bash
make test
make lint
```

Expected results: 18 tests pass; Ruff and strict mypy report no errors.
