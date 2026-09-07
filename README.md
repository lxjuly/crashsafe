# Crashsafe

Crashsafe is a small Python workflow engine built for the
[Stack AI durable-execution take-home](https://stack-ai.notion.site/Senior-Software-Engineer-Take-Home-Durable-Execution-Engine-3a7974ca255c811d9ffac55e5ad72a4a).
It favors a narrow, testable durability guarantee over distributed scheduling
features.

## Problem statement

Execute the ordered workflow `charge → provision → notify` despite flaky tools
and worker termination at any instruction boundary. The difficult case is an
ambiguous outcome: the tool commits a charge, but the worker dies before saving
the response.

Crashsafe guarantees durable recovery and **at-least-once tool requests**. The
mock tool guarantees **one side effect per stable idempotency key**. The engine
does not claim general exactly-once execution; a real tool must provide the same
durable idempotency contract.

## Functional requirements

- Create and inspect the seeded workflow through FastAPI.
- Execute steps in order with one polling worker.
- Persist intent before each request and reuse one operation key across retries.
- Persist retry decisions and resume after restart.
- Record immutable workflow events and audit the scheduling projection by
  rebuilding it from history.
- Gracefully drain on `SIGTERM`/`SIGINT` by finishing the in-flight attempt and
  admitting no next step.
- Exercise random HTTP 429s and deterministic hard-crash points through a
  separately durable mock tool.

The API exposes:

```text
POST /workflows
GET  /workflows
GET  /workflows/{id}
GET  /workflows/{id}/events
GET  /workflows/{id}/audit
```

## Non-functional requirements

- **Crash durability:** SQLite WAL mode, `synchronous=FULL`, and short
  `BEGIN IMMEDIATE` write transactions.
- **Atomic history:** an event append and its `workflows`/`steps` projection
  update commit in the same transaction.
- **Recoverability:** a pure reducer reconstructs operational state from ordered
  history.
- **Testability:** real child processes, `SIGKILL`, deterministic commit signals,
  and an automated ambiguous-outcome demo.
- **Maintainability:** Python type annotations, Pydantic contracts, strict mypy,
  Ruff, and separated API, engine, storage, worker, and tool layers.
- **Simple operation:** one local machine, one worker, and two SQLite databases.

## Architecture

```text
Client ──► FastAPI ──► engine.db
                         ├─ workflow_events  (authoritative history)
                         └─ workflows/steps  (scheduling projection)
                                ▲
                                │ atomic event + projection transaction
                          single worker
                                │ at-least-once HTTP
                                │ stable Idempotency-Key
                                ▼
                       mock tool API ──► tools.db
                                          side effect + cached result
                                          in one transaction
```

`engine.db` and `tools.db` are intentionally separate. A shared transaction
would hide the network ambiguity that the exercise is meant to handle.

## Core data model and event-history invariants

`workflow_events` stores a global row ID, workflow ID, per-workflow sequence,
event type and schema version, optional step/attempt identifiers, canonical JSON
payload, and committed timestamp. `(workflow_id, sequence)` is unique.

The event vocabulary is deliberately small:

| Event | Durable fact |
|---|---|
| `WorkflowCreated` | Input and immutable ordered step definitions, requests, and operation keys |
| `StepAttemptStarted` | Intent, attempt number, request, and stable key were committed before network I/O |
| `StepRetryScheduled` | Error and absolute `next_attempt_at` chosen by retry policy |
| `StepCompleted` | Tool output was committed for the current attempt |
| `StepFailed` | The current attempt became terminally failed |
| `WorkflowCompleted` / `WorkflowFailed` | Explicit terminal workflow outcome |

The model enforces six core invariants:

1. SQLite triggers reject event updates and deletes.
2. Sequence—not wall-clock time—defines a contiguous order per workflow.
3. Events and their mutable projections commit atomically.
4. Event payloads contain enough information to reconstruct every scheduling
   field with a typed reducer.
5. Operation keys are immutable and repeated across attempts.
6. An unmatched `StepAttemptStarted` means **outcome unknown**. It does not claim
   that a request was or was not delivered.

Normal scheduling reads indexed `workflows` and `steps` projections. The audit
endpoint folds history and compares the rebuilt state with those projections;
the storage layer can replace a corrupted projection from the same history.

The crash boundary is therefore:

| Worker dies… | Last committed engine fact | Recovery |
|---|---|---|
| Before attempt transaction | No attempt event | Start normally |
| After attempt commit, before send | Unmatched attempt | Retry with the stable key |
| After tool commit, before response | Unmatched attempt; effect is ambiguous | Retry; tool returns its cached result |
| During completion transaction | Neither completion event nor projection commits | Retry with the stable key |
| After completion transaction | `StepCompleted` and completed projection | Skip the step |

The event history makes committed engine decisions immutable and reconstructable.
Exactly one external charge still depends on atomic idempotency in `tools.db`.

## State machine

```text
pending ──StepAttemptStarted──► intent_recorded
                                   │          │
                    StepCompleted  │          │ StepRetryScheduled
                                   ▼          ▼
                               completed   retry_wait
                                                │
                                                └──StepAttemptStarted──► intent_recorded

intent_recorded ──StepFailed──► failed
```

Steps are selected by position only after every predecessor is completed. The
final `StepCompleted` and `WorkflowCompleted` commit together. `retry_wait`
stores the selected absolute eligibility time, so a restart or later retry-policy
configuration change cannot alter the committed wait.

## Demo

Requires Python 3.9+.

[Watch the 24-second recorded crash-recovery demo](docs/crashsafe-demo.mov). It
shows the live `kill -9`, replacement worker, repeated charge request with one
stable key, exactly one durable charge, and the central architecture decision.

```bash
make setup
make test
make demo
```

`make test` runs unit, API, reducer, SQLite atomicity, graceful-drain, and real
process-recovery tests. `make demo` uses isolated state under `.crashsafe/demo`,
disables random flakiness, and performs the required ambiguous-charge scenario.

The demo prints four recording-friendly checkpoints:

1. It starts the API and independently durable mock tool, creates a workflow,
   and prints the stable charge key.
2. It waits for the tool's charge transaction to commit, shows one ledger
   charge, and sends the worker `SIGKILL` before completion is saved.
3. It shows history containing attempt 1 without `StepCompleted`, restarts the
   worker, and shows attempt 2 using the identical key.
4. It verifies a completed workflow, a consistent event/projection audit, and
   the final ledger `{charges: 1, provisions: 1, notifications: 1}`.

For the submission video, record one terminal while running the three commands
above. Briefly narrate the stable key at checkpoint 1, the ambiguous state at
checkpoint 2, the repeated request at checkpoint 3, and the three final
assertions at checkpoint 4. The automated process test remains the executable
evidence behind the recording.

To reproduce the browser-formatted recording view, run
`.venv/bin/python scripts/video_demo_console.py` and open
<http://127.0.0.1:8099/auto>. The page drives real local processes and refuses
to print `PASS` unless it observes at least two charge attempts sharing one key
and exactly one ledger charge.

For ordinary exploration, run `make run`, open
<http://127.0.0.1:8000/docs>, and use the endpoints above. State is stored under
`.crashsafe/`. If an older development database predates event history, back it
up if needed and run `make clean` once.

### Optional Temporal comparison

With `uv` and the Temporal CLI installed, run the same ambiguous tool outcome
through a persistent local Temporal server and one Temporal worker:

```bash
make demo-temporal
```

This experiment uses the same mock tool and stable operation key. It kills the
Temporal worker after the charge commits, displays Temporal's history before and
after recovery, and verifies that the retried Activity receives the cached
charge result. The comparison is isolated under `experiments/`; Temporal is not
a Crashsafe runtime dependency. See the
[observed comparison](experiments/temporal_compare/README.md) for the exact
failure sequence and semantic differences.

## Scope and limitations

Consciously cut: concurrent workers, claims, leases, fencing, shards, history
trees, fan-out, cancellation, arbitrary workflow definitions, authentication,
distributed storage, and a UI. These solve coordination, scale, or product
surface rather than this single-worker durability proof. Numeric `Retry-After`
is supported, but retry policy is configuration; durability comes from storing
the resulting absolute retry time.

## AI usage

AI helped enumerate crash windows, scaffold typed boundaries, and build the
process-test harness. Verification caught and corrected generated-code risks:
the required Python/FastAPI stack was confirmed from the assignment, SQLite's
connection-local `synchronous=FULL` setting was applied on every connection,
idempotency cache hits validate canonical request hashes, and Python 3.9 runtime
typing compatibility is tested. The durability claims rely on executable crash
tests and event/projection audits, not on generated-code confidence.
