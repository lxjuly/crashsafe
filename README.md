# Crashsafe

Crashsafe is a small Python/FastAPI durable workflow engine. A client submits a
fully materialized JSON workflow definition to create a run; local workers
execute its allowlisted tool calls while SQLite preserves enough history to
resume the run after arbitrary process death.

The guarantee is intentionally precise: Crashsafe provides durable recovery and
**at-least-once tool requests**. An external effect happens exactly once only
when the tool durably deduplicates Crashsafe's stable idempotency key.

## What I built—and cut

Built:

- Crash-safe resume after real `kill -9`, including the ambiguous window where
  the tool committed but the worker did not record the response.
- Safe retries with persisted exponential backoff and provider `Retry-After`.
- A configurable local worker pool with renewable run leases and monotonic
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
UI. One run lease intentionally serializes ready branches; workers still
process different runs concurrently. Temporal is discussed only as design
context—it is not a dependency or experiment in this submission.

## AI usage

AI performed the implementation: it wrote the application and tests, enumerated
crash windows, scaffolded the typed boundaries, built the event reducer and
process harnesses, and drafted the documentation. The most important human work
was steering. Left unguided, AI repeatedly made plausible local choices that
pulled the project away from the right submission:

- **Scope:** it initially treated project memory as a runtime CRUD feature and
  later expanded into experiments that did not strengthen the take-home.
- **Event history:** it needed direction on why the append-only history is
  the durable source of truth, which facts belong in it, and where a projection
  is sufficient instead of more infrastructure.
- **Workflow definition:** it moved between a hard-coded sequence, a Python-like
  dynamic model, and a general DAG before human review selected a small,
  validated JSON definition that demonstrates two useful workflows without
  making parsing the project.
- **Demo:** it overbuilt multiple demos and a Temporal experiment before review
  focused the submission on one terminal recording that visibly proves
  ambiguous-outcome recovery and a deduplicated side effect.
- **Build tooling:** it retained a conventional `pip`/Make-based workflow and
  `Makefile` until human review chose `uv` for a faster, smaller setup.

AI also introduced implementation defects: Python 3.10 union syntax despite
Python 3.9 support, a connection-local SQLite durability pragma applied only at
setup, and lease deletion that could reuse fencing tokens. These were caught by
rereading the assignment, reviewing the data model and transaction boundaries,
running strict typing and repeated tests, inspecting SQLite state, using
deterministic barriers and clocks, exercising stale-owner cases, and sending real
process signals.

AI was extremely helpful: its raw implementation speed made the breadth of this
project possible within the available time. Human supervision is what made that
speed effective. It was needed to define the actual problem, reject attractive
extra scope, choose the durability model and interface, and decide what evidence
would make the guarantee credible. The result came from combining rapid AI
execution with deliberate human product and architectural judgment.

## Key decisions

### Execution model

```text
POST definition ──► FastAPI ──► workflow run in engine.db ◄── fenced workers
                                 history + projections          │
                                                                │ HTTP + stable key
                                                                ▼
                                                          mock tool ──► tools.db
```

`POST /workflow_runs` validates the entire concrete definition with Pydantic and
graph checks. In one `BEGIN IMMEDIATE` transaction it appends
`WorkflowRunCreated` v3 and inserts the run, step, and dependency projections.
The event snapshots the definition name, ordered steps, dependencies, concrete
requests, tool identity, and server-generated keys. Recovery therefore needs no
definition registry or source JSON file.

Workers atomically claim a run, then handle one synchronous tool attempt.
Different runs can overlap. Reacquiring an expired lease
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
2. A key is always `{run_id}:{client_step_id}` and never changes across
   retry, restart, or lease takeover.
3. A step is claimable only after every declared dependency completed; array
   position deterministically breaks ties between ready steps.
4. Each transition event and its mutable projection update share one SQLite
   transaction. Ordered history can reconstruct the projection exactly.
5. The final step completion and `WorkflowRunCompleted` commit atomically.
6. Only the current unexpired fence can commit worker results. This guarantees
   one valid committing owner, not exactly-once compute.
7. The tool atomically records its side effect and cached idempotency response.
   A same-key/different-payload request is rejected.

The calls I am least sure about are wall-clock leases (appropriate locally, not
a multi-host consensus substitute) and a conservative tool-wide cooldown (one
429 temporarily gates every run using that tool).

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
curl -sS -X POST http://127.0.0.1:8000/workflow_runs \
  -H 'content-type: application/json' \
  --data-binary @workflows/paid-onboarding.json
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
[`paid-onboarding`](workflows/paid-onboarding.json) and
[`trial-activation`](workflows/trial-activation.json) definitions. These files
are authoring examples, not a server-side registry; every created run owns an
immutable snapshot of the submitted definition. Endpoints:

```text
POST /workflow_runs
GET  /workflow_runs
GET  /workflow_runs/{run_id}
GET  /workflow_runs/{run_id}/events
GET  /workflow_runs/{run_id}/timeline
```

## Demo

Requires Python 3.9+:

```bash
uv sync
uv run python scripts/demo.py
```

The sole demo creates two runs. Worker 1 starts a charge and remains blocked
after the tool durably commits it. While that request is still in flight,
worker 2 starts the independent trial run, receives HTTP 429, and durably
schedules the provider's two-second `Retry-After`. The demo then executes a real
`kill -9` against worker 1.

Immediately after SIGKILL, it prints both history-derived timelines: the paid
run has an unmatched charge attempt with an unknown outcome, while the trial
run has a persisted retry deadline. It then starts one replacement worker,
prints both completed timelines, and passes only when the retry waited as
directed and the ledger contains exactly **one charge, two provisions, and three
notifications**. The brief overlap between workers 1 and 2 also exercises
leased concurrent execution without making concurrency a separate demo.

[Watch the terminal demo](crashsafe-demo.mov).

## Focused manual check: graceful drain

The crash-resume, safe-retry, concurrency, and observability properties are
covered together by the canonical demo. Graceful drain is mutually exclusive
with SIGKILL, so it has one separate curl-based script. Run `uv sync` once and
install `jq` before using it. Engine and tool state stay
in `.crashsafe/engine.db` and `.crashsafe/tools.db` across server restarts. The
script compares against existing ledger counts, never deletes either database,
and always prints the run timeline.

### Start the server

In Terminal 1, delay the committed charge response long enough to signal the
worker while its attempt is in flight:

```bash
CRASHSAFE_WORKERS=1 CRASHSAFE_FLAKY_RATE=0 \
CRASHSAFE_DELAY_AFTER_TOOL_COMMIT=charge CRASHSAFE_COMMIT_DELAY=5 \
CRASHSAFE_REQUEST_TIMEOUT=10 uv run crashsafe-stack
```

In Terminal 2, send `SIGTERM`, verify that the worker stays alive to commit the
in-flight attempt, and confirm it exits cleanly with one charge attempt and one
side effect:

```bash
scripts/manual_graceful_drain.sh
```

The script exits nonzero if the drain invariant is not observed.
