# Crashsafe

Crashsafe is a small Python/FastAPI durable workflow engine. A client submits a
fully materialized JSON DAG; local workers execute its allowlisted tool calls
while SQLite preserves enough history to resume after arbitrary process death.

The guarantee is intentionally precise: Crashsafe provides durable recovery and
**at-least-once tool requests**. An external effect happens exactly once only
when the tool durably deduplicates Crashsafe's stable idempotency key.

## What I built—and cut

Built:

- Crash-safe resume after real `kill -9`, including the ambiguous window where
  the tool committed but the worker did not record the response.
- Safe retries with persisted exponential backoff and provider `Retry-After`.
- A configurable local worker pool with renewable workflow leases and monotonic
  fence tokens (`CRASHSAFE_WORKERS=2` in the demo).
- Graceful `SIGTERM`/`SIGINT` drain: stop claiming, finish one in-flight
  attempt, commit it, then exit.
- An append-only typed event history and atomically updated scheduling
  projections, plus history-derived timelines.
- A separate flaky mock tool whose side effect and idempotency result commit in
  one durable SQLite transaction.
- Validated JSON DAGs with `charge`, `provision`, and `notify` operations,
  explicit dependencies, deterministic ready-step ordering, and fail-fast runs.

Cut: arbitrary workflow code/replay, templates and expression parsing, dynamic
fan-out, intra-workflow parallelism, compensation, cancellation, distributed
consensus/storage, remote queues, authentication, tracing infrastructure, and a
UI. One workflow lease intentionally serializes ready branches; workers still
process different workflows concurrently. Temporal is discussed only as design
context—it is not a dependency or experiment in this submission.

## Key decisions

### Execution model

```text
POST materialized DAG ──► FastAPI ──► engine.db ◄── fenced workers (1..N)
                                      history +             │
                                      projections           │ HTTP + stable key
                                                            ▼
                                                      mock tool ──► tools.db
```

`POST /workflows` validates the entire concrete plan with Pydantic and graph
checks. In one `BEGIN IMMEDIATE` transaction it appends `WorkflowCreated` v3
and inserts workflow, step, and dependency projections. The event contains the
workflow name, ordered steps, dependencies, concrete requests, tool identity,
and server-generated keys. Recovery therefore needs no definition registry.

Workers atomically claim a workflow, then handle one synchronous tool attempt.
Different workflows can overlap. Reacquiring an expired lease
increments its fence token; every worker-originated state commit checks the
current owner and token. SQLite uses WAL mode and `synchronous=FULL` on every
connection, and no database transaction spans network I/O.

### State machine and invariants

```text
pending ──persist intent──► intent_recorded ──success──► completed
                                │
                                ├──retryable──► retry_wait ──deadline──┐
                                └──permanent/exhausted──► failed       │
                                              ▲────────────────────────┘
```

Core invariants:

1. Intent, concrete request, attempt number, and stable operation key commit
   before a request leaves the worker.
2. A key is always `{workflow_id}:{client_step_id}` and never changes across
   retry, restart, or lease takeover.
3. A step is claimable only after every declared dependency completed; array
   position deterministically breaks ties between ready steps.
4. Each transition event and its mutable projection update share one SQLite
   transaction. Ordered history can reconstruct the projection exactly.
5. The final step completion and `WorkflowCompleted` commit atomically.
6. Only the current unexpired fence can commit worker results. This guarantees
   one valid committing owner, not exactly-once compute.
7. The tool atomically records its side effect and cached idempotency response.
   A same-key/different-payload request is rejected.

The calls I am least sure about are wall-clock leases (appropriate locally, not
a multi-host consensus substitute) and a conservative tool-wide cooldown (one
429 temporarily gates every workflow using that tool).

## Failure modes

| Worker dies exactly… | Durable state and recovery | Why correct |
|---|---|---|
| Before the intent transaction commits | Step remains `pending`; another owner starts attempt 1. | No request was authorized by visible durable state. |
| After intent commits, before HTTP send | An unmatched attempt remains; after lease expiry, a higher fence records another attempt with the same key. | A needless repeat is harmless at the idempotent tool. |
| After the tool commits charge, before the response is recorded | `tools.db` has one effect and cached result; engine history has an unmatched attempt. Recovery repeats the same key and receives that result. | Effect and idempotency result committed atomically. This is the demo's `kill -9` point. |
| After the old lease expires while its call is still running | A replacement owns a higher fence. The stale engine commit is rejected. | Fencing protects engine state; the stable key protects the external effect. |
| During or after the completion transaction | An uncommitted event/projection pair rolls back together; a committed pair is skipped on restart. | No half-transition is visible, and completed steps never reopen. |

An unmatched attempt means the outcome is **unknown**, not that delivery did or
did not happen. Idempotency makes both possible worlds safe.

## API and workflow definition

```bash
curl -sS -X POST http://127.0.0.1:8000/workflows \
  -H 'content-type: application/json' \
  --data-binary @examples/paid-onboarding.json
```

```json
{
  "name": "paid-onboarding",
  "steps": [
    {
      "id": "charge",
      "operation": "charge",
      "depends_on": [],
      "request": {"customer_id": "paid-customer", "amount_cents": 4200}
    },
    {
      "id": "provision",
      "operation": "provision",
      "depends_on": ["charge"],
      "request": {"customer_id": "paid-customer", "plan": "standard"}
    }
  ]
}
```

The repository includes complete
[`paid-onboarding`](examples/paid-onboarding.json) and
[`trial-activation`](examples/trial-activation.json) examples. Endpoints:

```text
POST /workflows
GET  /workflows
GET  /workflows/{id}
GET  /workflows/{id}/events
GET  /workflows/{id}/timeline
```

## Demo

Requires Python 3.9+:

```bash
uv sync
uv run python scripts/demo.py
```

The sole demo submits both example workflows, starts two workers, waits for the
tool to durably charge, prints and executes a real `kill -9` against worker 1,
and lets worker 2 take over with a higher fence and the same key. It prints both
history-derived timelines and passes only when both workflows complete and the
ledger contains exactly **one charge, two provisions, and three notifications**.

[Watch the terminal demo](crashsafe-demo.mov). Focused retry, drain, concurrency,
and crash commands are in [TESTING.md](TESTING.md).

## AI usage

AI helped enumerate crash windows, scaffold typed boundaries, draft the reducer
and process harnesses, and generate adversarial tests. Its suggestions were not
treated as proof; claims were checked with strict typing, repeated tests,
database inspection, deterministic barriers/clocks, and real process signals.

It was wrong in several useful ways. It initially interpreted project memory as
a runtime CRUD service; rereading the scope removed that work. It emitted Python
3.10 union syntax despite Python 3.9 support; the end-to-end run caught it. It
first set `synchronous=FULL` only during setup, but review caught that the pragma
is connection-local. It initially deleted released lease rows, which could reuse
fence tokens; a stale-owner test led to retained, monotonic fences. Finally, it
overbuilt separate demos and a Temporal experiment; review consolidated the
submission to this engine, one script, and one recording.
