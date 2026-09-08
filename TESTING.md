# Feature testing manual

From the repository root:

```bash
make setup
```

## Crash-safe resume and exactly one external charge

```bash
make demo
```

The script waits until the mock tool has durably committed charge, prints and
executes `kill -9` against worker 1, and starts worker 2. Verify the output shows:

```text
engine charge state: intent_recorded
charge attempts: [1, 2]
operation keys: ['<same key>', '<same key>']
workflow status: completed
event/projection audit consistent: True
final durable ledger: {'charges': 1, 'provisions': 1, 'notifications': 1}
```

The timeline should attribute the attempts to different workers and increasing
fence tokens. This proves crash recovery plus at-least-once requests. The one
charge comes from the mock tool's durable idempotency transaction.

## Concurrent workers, adaptive backoff, drain, and timeline

```bash
make demo-features
```

This creates two workflows, launches `worker-1` and `worker-2`, and forces the
first fresh tool request to return 429 with `Retry-After: 0.5`. Verify:

- both worker IDs appear in the history-derived timelines;
- exactly one `StepRetryScheduled` displays a 500 ms planned wait;
- both workers report a clean drain;
- both histories pass their projection audit; and
- the final ledger contains two charges, provisions, and notifications.

The first two requests may already overlap before the 429 is learned. Once the
cooldown transaction commits, no newly claimed same-tool request is eligible
until its durable deadline.

## Focused automated checks

Safe retries and durable provider cooldown:

```bash
.venv/bin/pytest -q tests/test_engine.py tests/test_adaptive_backoff.py
```

Lease contention, renewal, expiry, fencing, and parallel workflows:

```bash
.venv/bin/pytest -q tests/test_concurrency.py
```

Real `SIGKILL`, transaction rollback, deterministic crash points, and graceful
`SIGTERM` drain:

```bash
.venv/bin/pytest -q tests/test_process_recovery.py
```

Event reconstruction and read-only observability:

```bash
.venv/bin/pytest -q tests/test_history.py tests/test_observability.py tests/test_api.py
```

## Run the pool manually

Terminal 1:

```bash
CRASHSAFE_WORKERS=2 CRASHSAFE_FLAKY_RATE=0 make run
```

Terminal 2:

```bash
curl -sS -X POST http://127.0.0.1:8000/workflows \
  -H 'content-type: application/json' \
  -d '{"customer_id":"manual","amount_cents":4200,"email":"manual@example.com"}'
```

Copy the returned `id`, then inspect durable state and the event-derived view:

```bash
curl -sS http://127.0.0.1:8000/workflows/WORKFLOW_ID
curl -sS http://127.0.0.1:8000/workflows/WORKFLOW_ID/timeline
curl -sS http://127.0.0.1:8001/ledger
```

Press Ctrl-C in terminal 1. Any in-flight attempt completes; no worker accepts a
new workflow after receiving the signal.

## Complete verification

```bash
make test
make lint
```

Expected: 28 tests pass; Ruff and strict mypy report no errors.
