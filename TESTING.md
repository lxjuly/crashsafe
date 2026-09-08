# Feature testing manual

From the repository root, run `uv sync` once.

## Reviewable end-to-end demo

```bash
uv run python scripts/demo.py
```

The one demo covers two JSON workflows, two workers, a live `kill -9`, expired
lease takeover, stable-key reuse, history-derived timelines, and the final
ledger `{charges: 1, provisions: 2, notifications: 3}`.

## Crash safety and graceful drain

Run every real-process crash boundary, including the ambiguous committed charge
and an event/projection transaction interrupted by `SIGKILL`:

```bash
uv run pytest -q tests/test_process_recovery.py
```

The same file sends `SIGTERM` while a committed charge response is delayed. The
worker finishes only that in-flight step, exits cleanly, and a restarted worker
completes the workflow with one side effect per step.

## Safe retries and adaptive backoff

```bash
uv run pytest -q tests/test_engine.py tests/test_adaptive_backoff.py
```

These checks prove absolute retry eligibility and the stable operation key
survive restart, a 429's `Retry-After` creates a persisted tool-wide cooldown,
and non-429 failures do not throttle unrelated workflows.

## DAG validation and scheduling

```bash
uv run pytest -q tests/test_api.py tests/test_storage.py tests/test_history.py
```

These cover malformed operation requests, unknown dependencies, cycles,
duplicate IDs, forbidden fields, dependency readiness, deterministic branch
ordering, immutable plan persistence, append-only history, and exact projection
reconstruction.

## Concurrent ownership and timeline

```bash
uv run pytest -q tests/test_concurrency.py tests/test_observability.py
```

These exercise atomic claims, renewable leases, increasing fences, stale-owner
rejection, simultaneous progress across workflows, and read-only timelines.

## Manual API run

Terminal 1:

```bash
CRASHSAFE_WORKERS=2 CRASHSAFE_FLAKY_RATE=0 uv run crashsafe-stack
```

Terminal 2:

```bash
curl -sS -X POST http://127.0.0.1:8000/workflows \
  -H 'content-type: application/json' \
  --data-binary @examples/paid-onboarding.json
```

Copy the returned ID:

```bash
curl -sS http://127.0.0.1:8000/workflows/WORKFLOW_ID/events
curl -sS http://127.0.0.1:8000/workflows/WORKFLOW_ID/timeline
curl -sS http://127.0.0.1:8001/ledger
```

Press Ctrl-C in terminal 1 to gracefully drain the pool.

## Complete verification

```bash
uv run pytest
uv run ruff check .
uv run mypy crashsafe
```
