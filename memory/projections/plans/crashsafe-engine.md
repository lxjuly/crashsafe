# Crashsafe Engine — Full Implementation Plan

Status: implementation, README, and video completed
Baseline implemented: 2026-09-06  
Plan revised: 2026-09-07
Assignment: Stack AI Senior Software Engineer Take-Home — Durable Execution Engine

## 1. Objective

Build a small workflow runtime that executes an ordered
`charge → provision → notify` workflow and remains correct when the worker dies
at any instruction boundary. Demonstrate the hardest ambiguous outcome: the
charge commits at the external tool, the worker dies before recording success,
and recovery finishes without a second charge.

The engine must be straightforward to run, typed throughout, and divided into
engine, persistence, API, worker, and mock-tool boundaries.

The next durability milestone is an append-only workflow event history. It will
be the authoritative logical record of what the engine committed. The existing
`workflows` and `steps` rows remain the efficient scheduling projection, but
must be reproducible by folding the event stream.

## 2. Success criteria

1. Workflow and step state survive worker restart.
2. A completed step is never selected again.
3. Before every tool request, the engine durably stores the request payload,
   operation intent, attempt count, and stable idempotency key.
4. Retries and restarts reuse the same operation key.
5. Retry schedules survive restart. Interpreting `Retry-After` is retry-policy
   configuration, not part of the durability guarantee.
6. The mock tool atomically stores its side effect and idempotency result.
7. A duplicate request returns the original reference without repeating the
   side effect; conflicting reuse of a key is rejected.
8. `SIGKILL` recovery is demonstrated with real child processes.
9. `SIGTERM`/`SIGINT` drains the current attempt, admits no next step, and exits
   cleanly.
10. One setup command, one run command, and one deterministic crash demo are
    documented and reproducible.
11. Every committed workflow transition is represented by immutable, ordered
    events, and the event append and projection update commit together.
12. Folding a workflow's event history reconstructs the same workflow and step
    state used by the worker.

## 3. Chosen scope

### Implemented assignment features

- Crash-safe resume — mandatory.
- Safe retries — selected additional feature.
- Graceful drain — added after the initial engine implementation.

### Supporting capabilities

- Persisted exponential backoff.
- Numeric `Retry-After` support for an individual throttled request.
- Random HTTP 429 behavior in the mock tool.
- Deterministic hard-crash points.
- A supervisor that restarts a killed worker.
- Read-only workflow state through FastAPI and OpenAPI documentation.

### Durability core

- SQLite WAL and `synchronous=FULL` provide physical durability for committed
  database transactions.
- An append-only event stream provides logical durability: it records why the
  workflow is in its current state and can rebuild the scheduling projection.
- Stable operation keys bridge engine recovery to external idempotency.
- Persisted `next_attempt_at` preserves a committed retry decision across
  restart.

### Policy, not stronger guarantees

- Exponential delay parameters, maximum attempts, jitter, and interpretation of
  `Retry-After` decide *when* another attempt is made.
- They do not change the at-least-once request or external-idempotency guarantee.
- Crashsafe may retain numeric `Retry-After` support, but the event schema stores
  the resulting retry decision rather than coupling history to an HTTP header.

### Explicitly deferred

- Concurrent workers, claims, leases, and fencing.
- Shards, history trees/branches, task queues, and ownership transfer.
- Fan-out, child workflows, and joins.
- General workflow definitions, versioning, and replay.
- Cancellation and mid-request interruption.
- A timeline UI or metrics system.
- Tool-wide adaptive throttling.
- Authentication, multi-tenancy, migrations, and distributed storage.

These cuts keep the correctness argument centered on the network gap between
two independent durable systems. Sharding, branching, leases, and fencing solve
distribution, concurrency, and scale. They are not required to demonstrate
single-worker crash durability.

## 4. Architecture

```text
Client
  │
  ▼
FastAPI workflow API ───────► engine.db
                                  │
                                  ├─ workflow_events (authority)
                                  └─ workflows + steps (projection)
                                  ▲
                                  │ atomic event + projection commits
                              single worker
                                  │ at-least-once HTTP + stable key
                                  ▼
                         flaky mock tool API ───────► tools.db
                                                     side effect +
                                                     idempotency result
```

### Components

- `crashsafe/models.py` — typed workflow, step, request, and response models.
- `crashsafe/storage.py` — SQLite schema and atomic state transitions.
- `crashsafe/engine.py` — selection, execution, retry policy, and crash hooks.
- `crashsafe/api.py` — workflow creation and inspection endpoints.
- `crashsafe/worker.py` — single polling worker and graceful-drain lifecycle.
- `crashsafe/mock_tool.py` — flaky external boundary and durable deduplication.
- `crashsafe/stack.py` — local process supervisor.
- `scripts/demo_ambiguous_charge.py` — deterministic video/demo scenario.

The engine and tool use separate SQLite databases. A shared transaction would
remove the ambiguity the assignment is intended to test and would not represent
a real external side-effect boundary.

## 5. Durable data model

### Workflow event history — authoritative

```sql
CREATE TABLE workflow_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    step_id TEXT,
    attempt INTEGER,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    UNIQUE (workflow_id, sequence)
);

CREATE INDEX idx_workflow_events_order
    ON workflow_events (workflow_id, sequence);
```

`sequence`, not time or global `id`, defines order within a workflow. Sequence
numbers are allocated inside the same `BEGIN IMMEDIATE` transaction as the
projection update. The global `id` exists only for efficient inspection and
pagination.

There is intentionally no foreign key from `workflow_events` to `workflows`.
The event stream is the authority and must be able to outlive and recreate its
projection. A `WorkflowCreated` event establishes the logical workflow.

SQLite triggers reject `UPDATE` and `DELETE` against `workflow_events`. Events
are corrected only by appending a new fact; they are never edited in place.

Minimum event vocabulary:

| Event | Durable fact and required payload |
|---|---|
| `WorkflowCreated` | Workflow input plus ordered immutable step definitions, including IDs, positions, requests, and operation keys |
| `StepAttemptStarted` | Step ID/name, attempt number, stored request, and stable operation key; committed before network I/O |
| `StepRetryScheduled` | Attempt, normalized error, and absolute `next_attempt_at` chosen by policy |
| `StepCompleted` | Attempt number and stored tool result |
| `StepFailed` | Attempt number and terminal error |
| `WorkflowCompleted` | Explicit terminal workflow fact |
| `WorkflowFailed` | Explicit terminal workflow fact and cause |

The name `StepAttemptStarted` is deliberate. The engine can prove that it
committed intent before calling the tool, but it cannot atomically prove that a
packet left the process. An unmatched attempt-start event therefore means
“outcome unknown” and recovery must retry with the same operation key.

`occurred_at` records the committed transition time and may be used to rebuild
projection timestamps, but never determines event order. `payload_json` is
canonical JSON; `schema_version` makes later event upcasting explicit.

Drain signals are process-lifecycle observations, not workflow state changes,
so `WorkerDrainRequested` is not part of the workflow history.

### Scheduling projection — rebuildable

#### Workflow

- `id`
- `status`: `running | completed | failed`
- immutable input JSON
- created, updated, and completed timestamps

#### Step

- `id`, `workflow_id`, ordered `position`, and operation `name`
- `status`: `pending | intent_recorded | retry_wait | completed | failed`
- immutable request JSON
- stable, unique operation key
- persisted output JSON
- attempt count
- next eligible attempt timestamp
- last error
- created, updated, and completed timestamps

#### Mock-tool ledger

- An idempotency row keyed by operation key, containing operation, canonical
  request hash, and the serialized original response.
- A side-effect row keyed uniquely by operation key.
- Both rows are inserted in the same SQLite transaction.

SQLite uses WAL mode, `BEGIN IMMEDIATE` for writes, a busy timeout, foreign-key
enforcement where applicable, and `synchronous=FULL` on every connection. WAL is
the database's physical write-ahead log; `workflow_events` is the engine's
domain-level history. They serve different layers of the durability argument.

## 6. State machine

```text
pending ──record attempt and intent──► intent_recorded
                                         │       │
                              success    │       │ retryable failure
                                         ▼       ▼
                                     completed  retry_wait
                                                     │
                                                     │ next_attempt_at reached
                                                     └──► intent_recorded

intent_recorded or retry_wait ──retry budget exhausted──► failed
```

Steps execute strictly by position. A step is selectable only when all earlier
steps are `completed`. A workflow becomes `completed` in the same transaction
that completes its final step. A permanent failure or exhausted retry budget
makes the step and workflow terminally `failed`.

## 7. Correctness and transaction boundaries

### Event-history invariants

1. **Append-only:** application code exposes append/read operations only, while
   database triggers reject event updates and deletes.
2. **Per-workflow total order:** `(workflow_id, sequence)` is unique and
   contiguous; timestamps never determine semantic order.
3. **Atomic projection:** all events describing a transition and all resulting
   `workflows`/`steps` changes occur in one SQLite transaction. Neither side can
   commit alone.
4. **Reducer completeness:** event payloads contain every durable input needed
   to reconstruct workflow status, step status, attempt count, output, error,
   retry time, request, and operation key.
5. **External ambiguity is explicit:** an attempt-start event without a later
   completion or retry event records an unknown outcome, not proof that the
   tool did or did not execute.
6. **Idempotency identity is immutable:** the operation key originates in
   `WorkflowCreated`, is repeated in attempt-start events for auditability, and
   never changes across retries or recovery.

Normal scheduling reads the projection; it need not fold the full stream on
every poll. A pure reducer folds events for audit and rebuild. Startup may audit
the projection, and tests must compare it with the folded result after each
failure scenario.

### Boundary A — workflow creation

One transaction inserts the `WorkflowCreated` event and projects the workflow
and all three ordered steps, including their immutable payloads and stable
operation keys. A partially seeded workflow cannot become visible. Projection
rows may be written before the event within the transaction, but no observer can
see that internal ordering.

### Boundary B — durable intent

Before network I/O, one transaction appends `StepAttemptStarted`, changes the
step to `intent_recorded`, increments the attempt count, and clears obsolete
retry state. Only after this commit may the HTTP request leave the process.

### Boundary C — external effect

The engine sends the stored request with its stored idempotency key. The mock
tool transaction inserts both the side-effect row and idempotency result. The
tool returns only after that transaction commits.

### Boundary D — step completion

After a successful response, one transaction appends `StepCompleted`, stores
the response, and marks the step `completed`. If it is the final step, the same
transaction also appends `WorkflowCompleted` and marks the workflow completed.

### Boundary E — retry scheduling

A retryable failure appends `StepRetryScheduled` and projects `retry_wait`, the
error, and `next_attempt_at` in one transaction. Restart cannot erase or shorten
the chosen wait. The event stores the chosen absolute time—not the HTTP header
or backoff formula that happened to produce it. An `intent_recorded` step left
by a hard crash is eligible immediately because its outcome is unknown.

### Boundary F — terminal failure

Exhausted retries or a permanent error append `StepFailed` and
`WorkflowFailed`, then project both terminal states in the same transaction.

### Recovery interpretation

After restart, the worker uses the projection to select work:

- `completed` steps are skipped.
- `retry_wait` steps remain ineligible until their persisted timestamp.
- `intent_recorded` plus an unmatched `StepAttemptStarted` means the request may
  have executed, so the worker makes another at-least-once request with the
  original key.
- A projection/history mismatch is corruption or a programming defect, not a
  normal recovery state; the reducer provides a deterministic repair source.

### How the schema satisfies the durability requirement

| Crash point | Last committed history | Rebuilt state | Safe recovery action |
|---|---|---|---|
| Before attempt transaction | No `StepAttemptStarted` | Step remains pending or waiting | Start the attempt normally |
| After attempt commit, before send | Unmatched `StepAttemptStarted` | `intent_recorded`, stable key retained | Retry with the same key |
| Tool commits, worker misses response | Unmatched `StepAttemptStarted` | Same unknown-outcome state | Retry; tool returns its durable cached result |
| Response received, before completion commit | Unmatched `StepAttemptStarted` | Same unknown-outcome state | Retry with the same key |
| During event/projection completion transaction | Transaction has neither committed event nor projected state | Unknown-outcome state | Retry with the same key |
| After completion transaction | `StepCompleted` and completed projection | Step completed with stored output | Skip the step |

The history does not make an external side effect exactly once, nor does it make
an uncommitted transaction durable. It makes every *committed engine decision*
immutable and reconstructable. Atomic tool-side idempotency still closes the
ambiguous network gap.

### Guarantee boundary

The engine guarantees durable recovery and at-least-once HTTP requests. It
cannot guarantee exactly-once effects for an arbitrary external system. The
demo's one-charge result depends on the mock tool's durable atomic idempotency
contract, which a production provider would also need to supply.

## 8. Phased implementation plan and outcome

### Phase 0 — inspect and scope

Planned:

- Read the complete assignment.
- Select the required stack and strongest additional feature.
- Write down what “completed” means and where transactions end.

Outcome:

- Selected Python, FastAPI, Pydantic, SQLite, one worker, safe retries, and a
  separate durable mock tool.
- Rejected worker pools, fan-out, replay, and UI for this submission.

### Phase 1 — project and typed contracts

Planned:

- Create packaging, settings, enums, Pydantic models, commands, and test setup.
- Centralize status names, ports, retry limits, and timeouts.

Outcome:

- Added Python 3.9-compatible typed models and a minimal install/run toolchain.

### Phase 2 — durable workflow storage

Planned:

- Create workflow and step tables.
- Seed all steps atomically.
- Implement ordered selection and explicit transition methods.

Outcome:

- Storage owns every state mutation and exposes no network behavior.
- Final-step and workflow completion share one transaction.

### Phase 3 — durable mock tool

Planned:

- Expose charge, provision, and notify operations.
- Return random 429 responses before committing a side effect.
- Deduplicate by stable operation key and validate the request hash.

Outcome:

- Tool state survives worker termination in an independent database.
- A post-commit delay and signal file make the ambiguous window observable.

### Phase 4 — engine and retry policy

Planned:

- Persist intent before send.
- Execute HTTP outside database transactions.
- Persist success, permanent failure, or retry scheduling.
- Reuse the original operation key on every attempt.

Outcome:

- Implemented exponential backoff, numeric `Retry-After`, retry exhaustion,
  and immediate recovery of unknown `intent_recorded` outcomes.

### Phase 5 — API, worker, and local startup

Planned:

- Add create/get/list workflow endpoints.
- Add a single polling worker.
- Add a one-command local supervisor.

Outcome:

- `make run` starts the workflow API, mock tool, and worker; the supervisor
  restarts a worker that exits unexpectedly.

### Phase 6 — deterministic failure injection

Planned:

- Provide hard exits before request, after response, and after completion.
- Add a distinct ambiguous-outcome hook after tool commit but before response.

Outcome:

- Tests use `os._exit` or real `SIGKILL`; they do not simulate process death by
  throwing a recoverable application exception.

### Phase 7 — graceful drain

Planned:

- Close admission on `SIGTERM`/`SIGINT`.
- Make idle poll waits interruptible.
- Finish the in-flight attempt and its state transaction before exit.

Outcome:

- Implemented and verified with a real signal during a delayed committed charge
  response. See `memory/projections/plans/graceful-drain.md`.

### Phase 8 — append-only event history

Status: completed.

#### 8.1 Schema and typed events

- Add `workflow_events`, its ordering index, and append-only update/delete
  triggers to `engine.db`.
- Define a closed `EventType` enum and versioned Pydantic payload model for each
  event. Reject payloads that do not match their event type.
- Serialize canonical JSON and parse timestamps consistently in UTC.
- Treat the current development databases as disposable schema version 1 data;
  do not invent historical events for pre-history projection rows. Fail fast
  with a documented reset instruction if a nonempty legacy database is opened.

Acceptance: malformed event payloads cannot enter through the storage API;
direct event mutation is rejected by SQLite.

#### 8.2 Pure reducer

- Implement a side-effect-free reducer from an ordered event list to typed
  workflow and step projection state.
- Validate sequence contiguity, legal transitions, immutable step definitions,
  stable keys, monotonic attempts, and exactly one workflow creation event.
- Make invalid or incomplete histories fail with a specific integrity error.

Acceptance: folding only `WorkflowCreated` produces the initial projection;
folding each terminal history reproduces all stored projection fields.

#### 8.3 Atomic transition writer

- Centralize each storage transition so it accepts the expected prior state,
  appends one or more events, and updates its projection inside one
  `BEGIN IMMEDIATE` transaction.
- Allocate the next per-workflow sequence in that transaction. The single-writer
  design is the concurrency assumption; do not introduce leases or shards.
- Refactor workflow creation, attempt start, retry scheduling, step completion,
  and terminal failure through these transition functions.

Acceptance: injected exceptions after event insertion and before projection
update—and in the reverse internal order—roll back the entire transaction.

#### 8.4 History inspection and audit

- Add a read-only ordered history query, exposed either under the existing
  workflow detail response or as `GET /workflows/{id}/events`.
- Add an audit operation that folds history and compares the result with the
  stored projection without mutating either.
- Keep rebuild as an explicit storage/admin operation, not part of every worker
  poll. The worker continues using indexed projections for scheduling.

Acceptance: the API returns strictly ordered typed events, and audit reports no
differences for every normal and crash-recovery scenario.

#### 8.5 Failure and reconstruction tests

- Re-run every existing process-kill scenario and compare folded history with
  the stored projection after recovery.
- Kill a child process during a deliberately paused event/projection transaction
  and prove SQLite exposes neither half after restart.
- In the ambiguous-charge test, require history to show attempt 1 started with
  no completion, attempt 2 using the same operation key, then one completion;
  require the tool ledger to contain exactly one charge.
- Test that retry events preserve the absolute eligible time even when retry
  configuration changes before restart.
- Retain Ruff, strict mypy, all baseline tests, and the one-command demo.

Acceptance: event history explains every state reached, projection rebuild is
equal, and no regression weakens the existing one-charge proof.

Outcome:

- Added typed, versioned events, append-only SQLite triggers, per-workflow
  sequencing, a pure integrity-checking reducer, and atomic transition writers.
- Added read-only event and audit endpoints plus explicit projection rebuild.
- Added exception rollback and real-`SIGKILL` transaction tests; all crash paths
  audit equal to their reconstructed projections.

### Phase 9 — README, reproducible demo, and video

Status: completed.

Rewrite `README.md` as the final submission write-up. Keep it succinct and use
this order:

1. **Problem statement** — the ordered workflow, ambiguous network outcome, and
   narrow guarantee boundary.
2. **Functional requirements** — workflow creation/inspection, ordered steps,
   retries, recovery, event-history inspection, and graceful drain.
3. **Non-functional requirements** — crash durability, stable idempotency,
   deterministic failure testing, type safety, and simple local startup.
4. **Architecture** — FastAPI, single worker, `engine.db`, independent mock tool
   and `tools.db`, with no shared transaction.
5. **Core data model and event-history invariants** — append-only ordered events,
   atomic projections, reducer reconstruction, immutable operation keys, and
   the external exactly-once boundary.
6. **State machine** — the minimal step states and the event emitted for each
   committed transition.
7. **Demo** — exact commands, expected checkpoints, and final assertions.

Keep conscious cuts, limitations, and honest AI usage brief, either alongside
the relevant section or in short closing notes.

The README demo must be directly followable:

1. Run `make setup` and `make test`.
2. Run `make demo` with deterministic random flakiness disabled.
3. Show that the charge commits in `tools.db`, then that the worker is killed
   before `StepCompleted` is committed in `engine.db`.
4. Show the pre-recovery event history: attempt 1 has
   `StepAttemptStarted` but no matching `StepCompleted`.
5. Let the supervisor restart the worker and show attempt 2 reusing the same
   operation key.
6. Show the completed `charge → provision → notify` workflow, a consistent
   folded projection, and exactly one charge in the durable ledger.

Adjust `make demo` so these checkpoints and identifiers are printed clearly
enough to read in a screen recording. Record a short terminal video following
the same sequence; the video is submission evidence, not a substitute for the
automated process-recovery test.

Acceptance: a reviewer can clone the repository, follow only the README, run
the tests and demo, and see the claimed failure boundary and one-charge result
without inspecting implementation details first.

Outcome:

- Rewrote `README.md` in the agreed problem/requirements/architecture/data
  model/invariants/state-machine/demo format.
- Updated `make demo` with four readable checkpoints and assertions for event
  history, same-key recovery, projection consistency, and the durable ledger.
- Recorded `docs/crashsafe-demo.mov`: a 23.99-second live `SIGKILL` recovery
  with assertion-gated history, ledger, and guarantee-boundary evidence.

## 10. Verification matrix

| Property | Evidence |
|---|---|
| Ordered workflow seeding and selection | Storage tests |
| Atomic final-step/workflow completion | Storage tests |
| Persisted retry schedule and stable key reuse | Engine retry test |
| Durable tool deduplication after reopen | Mock-tool storage test |
| Conflicting key/payload rejection | Mock-tool storage test |
| Crash after intent, before request | Hard-exit process test |
| Crash after response, before engine commit | Hard-exit process test |
| Crash after engine completion commit | Hard-exit process test |
| Tool committed, worker killed before response | Real `SIGKILL` process test and `make demo` |
| Graceful signal during in-flight response | Real `SIGTERM` process test |
| Static quality | Ruff and strict mypy |
| Append-only enforcement | Direct SQL update/delete rejection tests |
| Event/projection transaction atomicity | Exception rollback and real-process `SIGKILL` tests |
| Per-workflow event ordering | Unique, contiguous-sequence tests |
| Projection reconstruction | Reducer audit and explicit rebuild tests |
| Ambiguous outcome represented faithfully | History assertion plus one-entry tool ledger |
| Retry decision independent of later config | Persisted timestamp/restart test |

Final verified state at the time of this projection: 18 tests passed, Ruff
passed, strict mypy passed, and the recording-oriented ambiguous-charge demo
passed.

## 11. Risks discovered and corrections

1. The first scaffold targeted TypeScript before the assignment was accessible.
   Reading the full page corrected the implementation to required Python and
   FastAPI before that scaffold was committed.
2. SQLite `synchronous=FULL` was initially applied only during schema creation.
   Because the setting is connection-local, it was moved to every connection.
3. Cached idempotency lookup initially risked accepting a reused key with
   different input. Canonical request hashing and conflict rejection fixed it.
4. Automated formatting introduced Python 3.10 union annotations. The
   end-to-end demo on Python 3.9 exposed Pydantic's runtime evaluation failure;
   `Optional[...]` restored the advertised compatibility.
5. The memory request was initially over-interpreted as a new service. It was
   reduced to Git-tracked Chronelle-style project memory, keeping the take-home
   focused on durable workflows.

## 12. Completion boundary and later work

The core durability implementation is complete: append-only event history,
atomic projection updates, reducer reconstruction, and failure evidence all
pass. Concurrency remains intentionally outside the submission.

Phase 9 turned the verified implementation into the succinct README,
reproducible demo, and linked video artifact. The submission scope is complete.
Optional usability work could include richer event inspection, selective
operator tooling, or metrics. Retry-policy tuning remains configuration work.

A production distributed engine would separately require concurrent ownership,
claims, renewable leases, fencing, shards, task queues, history partitioning,
definition versioning, migrations, authentication, and multi-tenant isolation.
Those solve a different problem and are outside Crashsafe's durability proof.
