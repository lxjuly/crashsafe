# Crashsafe Engine — Full Implementation Plan

Status: current engine complete; materialized JSON DAG revision accepted and pending
Baseline implemented: 2026-09-06  
Plan revised: 2026-09-08
Assignment: Stack AI Senior Software Engineer Take-Home — Durable Execution Engine

## 1. Objective

Build a small workflow runtime whose `POST /workflows` endpoint accepts a fully
materialized JSON execution plan and that remains correct when the worker dies
at any instruction boundary. Preserve the hardest ambiguous outcome: a charge
commits at the external tool, the worker dies before recording success, and
recovery finishes without a second charge.

The engine must be straightforward to run, typed throughout, and divided into
engine, persistence, API, worker, and mock-tool boundaries.

The durability, leased concurrency, graceful drain, timeline, and adaptive
backoff baseline is complete. Append-only workflow history remains the
authoritative logical record and `workflows`/`steps` remain its rebuildable
scheduling projection. The accepted revision replaces hardcoded workflow
seeding with a deliberately small, validated JSON DAG submitted per run and
demonstrates two different workflows without moving the correctness boundary
away from SQLite.

## 2. Success criteria

1. Workflow and step state survive worker restart.
2. A completed step is never selected again.
3. Before every tool request, the engine durably stores the request payload,
   operation intent, attempt count, and stable idempotency key.
4. Retries and restarts reuse the same operation key.
5. Retry schedules survive restart. A tool's `Retry-After` also advances a
   persisted tool-wide throttle so other workflows do not immediately call the
   same throttled service.
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
13. `CRASHSAFE_WORKERS` controls the local worker count; the default remains one
    and the concurrency demo runs two.
14. An atomic renewable workflow lease gives only one worker a valid claim at a
    time; a monotonically increasing fence token rejects stale commits.
15. Two workers execute different workflows concurrently, while a killed lease
    owner is recovered after expiry using the original operation key.
16. A read-only timeline shows ordered steps, attempts, retries, persisted waits,
    worker identity, and terminal outcome directly from durable events.
17. The demo ends with the timeline and summary counts, making the recovery and
    retry behavior understandable without reading raw JSON.
18. `POST /workflows` accepts immutable workflow metadata and fully materialized
    steps containing dependencies, allowlisted operations, and concrete request
    payloads.
19. Two distinct JSON workflows can complete through the same API and worker
    runtime without Python workflow code changes.
20. DAG readiness is deterministic: a step becomes runnable only after all of
    its declared dependencies have committed `StepCompleted`.
21. Pydantic and graph validation reject cycles, unknown dependencies,
    duplicate step IDs, unsupported operations, and invalid operation-specific
    requests.
22. Public audit models, endpoints, and demo fields are removed; reducer replay
    remains mandatory internal verification for every transition and failure
    test.

## 3. Chosen scope

### Baseline implemented assignment features

- Crash-safe resume — mandatory.
- Safe retries — selected additional feature.
- Graceful drain — added after the initial engine implementation.

### Implemented additional assignment features

- Concurrent workers — configurable pool, with two workers in the demo and an
  exclusive renewable workflow lease plus fencing.
- Observability — a history-derived timeline and concise per-run summary.
- Adaptive backoff — persisted service-wide throttle extension from
  `Retry-After`, shared by all workers and workflows.

### Accepted product capability

- Per-run JSON DAG definitions — the complete concrete plan is validated by
  Pydantic at `POST /workflows` and snapshotted into `WorkflowCreated`.
- Dependency-aware scheduling — deterministic ready-node selection without
  intra-workflow parallel execution.
- No binding language — every submitted step already contains its final request
  body; Crashsafe only validates and persists it.
- Two example payloads — the original paid onboarding flow and a branching paid
  activation flow whose post-charge provision and receipt notification are
  independently ready.

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
- The event schema stores the selected absolute retry time, while a separate
  durable tool throttle stores the maximum service-wide blocked-until time.

### Explicitly deferred

- Shards, history trees/branches, remote task queues, and distributed ownership
  transfer.
- Parallel execution of nodes inside one workflow, dynamic fan-out, child
  workflows, conditions, loops, compensation, and partial-success semantics.
- Arbitrary Python workflow code, stored definition CRUD, registries, templates,
  output references, and expression evaluation.
- Cancellation and mid-request interruption.
- A graphical UI, tracing backend, or production metrics stack.
- Rate estimation, token buckets, and cross-tool adaptive control beyond a
  persisted `Retry-After` cooldown per tool.
- Authentication, multi-tenancy, a general schema-migration framework, and
  distributed storage.
- Idempotent workflow creation across repeated client `POST` requests; the
  exactly-once boundary remains scoped to side effects within one workflow ID.

These cuts keep the correctness argument centered on the network gap between
two independent durable systems. The posted JSON is a complete execution plan,
not an embedded programming language or reusable-definition service. A workflow
lease still serializes nodes within one run, while multiple workers may execute
different runs concurrently.

## 4. Architecture

```text
Client
  │
  ▼
FastAPI workflow API ───────► Pydantic + graph validation
                                  │ complete immutable plan
                                  ▼
                              engine.db
                                  │
                                  ├─ workflow_events (authority)
                                  ├─ workflows + steps (projection)
                                  ├─ workflow_leases (owner + fence)
                                  └─ tool_throttles (shared cooldown)
                                  ▲
                                  │ fenced atomic transitions
                         configurable worker pool
                              worker-1  worker-2
                                  │ at-least-once HTTP + stable key
                                  ▼
                         flaky mock tool API ───────► ledger.db
                                                     side effect +
                                                     idempotency result
```

### Components

- `crashsafe/models.py` — typed workflow, response, and discriminated
  operation-specific step models plus workflow-level dependency validation.
- `crashsafe/storage.py` — SQLite schema and atomic state transitions.
- `crashsafe/engine.py` — selection, execution, retry policy, and crash hooks.
- `crashsafe/api.py` — workflow creation and inspection endpoints.
- `crashsafe/worker.py` — polling worker, lease renewal, fencing, and
  graceful-drain lifecycle.
- `crashsafe/mock_tool.py` — flaky external boundary and durable deduplication.
- `crashsafe/supervisor.py` — local process supervisor and configurable worker pool.
- `crashsafe/observability.py` — event-to-timeline projection and
  summary formatting.
- `scripts/demo.py` — planned single canonical deterministic demo scenario;
  replaces the two current demo scripts during Phase 15.

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
| `WorkflowCreated` | Complete immutable submitted plan: workflow name plus step IDs, array order, operations, dependencies, concrete requests, tool keys, and generated operation keys |
| `StepAttemptStarted` | Step ID/name, attempt number, stored request, stable operation key, worker ID, and lease fence token; committed before network I/O |
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

Concurrency will introduce a version-2 `StepAttemptStarted` payload containing
`worker_id` and `lease_token`. The reducer will continue to accept version 1 and
upcast its missing observability metadata, so existing histories remain valid.

Drain signals are process-lifecycle observations, not workflow state changes,
so `WorkerDrainRequested` is not part of the workflow history.

### Scheduling projection — rebuildable

#### Workflow

- `id`
- immutable submitted workflow name and definition JSON
- `status`: `running | completed | failed`
- created, updated, and completed timestamps

#### Step

- internal `id`, `workflow_id`, stable client step ID, deterministic order, and
  allowlisted operation name
- declared dependency node IDs, normalized into `step_dependencies`
- `status`: `pending | intent_recorded | retry_wait | completed | failed`
- immutable request JSON
- stable, unique operation key
- persisted output JSON
- attempt count
- next eligible attempt timestamp
- last error
- created, updated, and completed timestamps

#### Workflow lease — operational control state

- `workflow_id` — one lease row per workflow
- `owner_id` — unique worker-process identity
- `fence_token` — monotonically increasing on every successful acquisition
- `lease_expires_at` — absolute UTC expiry
- `updated_at` — inspection and renewal timestamp

Lease acquisition runs in `BEGIN IMMEDIATE`: select an eligible workflow whose
lease is absent, expired, or already owned by this worker; atomically assign the
owner, increment the token, and return the workflow. Every worker-originated
attempt, retry, completion, and failure transaction must validate both owner and
fence token. The lease is deliberately not part of the reconstructable workflow
state: it is transient coordination state, while accepted fenced transitions
remain in authoritative history.

The model guarantees one **valid** owner at a time, not exactly-once compute. If
an old worker pauses beyond lease expiry, a replacement may repeat its request;
the old worker's later database commit is fenced out and the stable tool key
makes overlapping external attempts safe.

#### Tool throttle — shared retry control state

- `tool_key` — stable service identity, initially `mock-tool`
- `blocked_until` — maximum absolute retry time observed for that tool
- `reason` and `updated_at` — inspection metadata

Steps gain an immutable `tool_key`. On HTTP 429, one transaction schedules the
current step retry and advances `blocked_until` using
`max(existing, now + Retry-After)`. Candidate selection excludes every step for
that tool until the persisted cooldown expires. A shorter concurrent response
cannot reduce an existing throttle.

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

The current implementation executes strictly by position. The accepted DAG
scheduler instead selects a step only when every declared dependency is
`completed`, breaking ties by deterministic definition order. A workflow lease
still permits only one valid worker to advance a particular run, so ready
branches execute sequentially; the DAG models dependency semantics, not
intra-workflow parallelism. A workflow becomes `completed` in the same
transaction that completes its last unfinished node. A permanent failure or
exhausted retry budget remains fail-fast for the entire workflow.

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
   retry time, request, operation key, submitted workflow name, deterministic
   step order, and dependency graph.
5. **External ambiguity is explicit:** an attempt-start event without a later
   completion or retry event records an unknown outcome, not proof that the
   tool did or did not execute.
6. **Idempotency identity is immutable:** the operation key originates in
   `WorkflowCreated`, is repeated in attempt-start events for traceability, and
   never changes across retries or recovery.
7. **Fenced ownership:** only the current lease owner and fence token may append
   a worker-originated transition; stale workers may compute but cannot commit.
8. **Monotonic tool cooldown:** a 429 may extend a tool's persisted
   `blocked_until`, never shorten it, and all workers consult it before claim.

Normal scheduling reads the projection; it need not fold the full stream on
every poll. A pure reducer folds events for reconstruction and correctness
tests. Projection equality remains an internal invariant checked after failure
scenarios, but is no longer presented as an assignment feature or public API.

### Boundary A — workflow creation

One transaction inserts the `WorkflowCreated` event and projects the submitted
workflow, all DAG steps, and their dependency edges, including concrete
requests and stable operation keys. The committed event contains the complete
submitted plan, so restart needs no external definition source. A partially
seeded workflow cannot become visible. Projection rows may be written before
the event within the transaction, but no observer can see that internal
ordering.

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

### Boundary G — workflow lease

A worker must acquire or renew the workflow lease before recording an attempt.
The returned fence token is carried through that attempt. Renewal changes only
the expiry for the same owner/token; reacquisition after expiry increments the
token. Every later transition validates the token in its write transaction, so
an expired worker cannot overwrite state accepted from its replacement.

Lease renewal continues while a synchronous tool request is in flight. During
graceful drain the worker keeps renewing until the current result is committed,
then releases the lease and exits without claiming another workflow.

### Boundary H — adaptive tool throttle

When the gateway receives HTTP 429, retry scheduling and extension of the
tool-wide `blocked_until` occur in one fenced transaction. The current workflow
records its absolute retry time in history; the shared throttle is operational
scheduling state. Other workers atomically consult that gate before acquiring
work for the same tool.

### Recovery interpretation

After restart, the worker uses the projection to select work:

- `completed` steps are skipped.
- `retry_wait` steps remain ineligible until their persisted timestamp.
- steps for a throttled tool remain ineligible until its persisted
  `blocked_until`, even when their own workflow has not received a 429.
- `intent_recorded` plus an unmatched `StepAttemptStarted` means the request may
  have executed; after the dead owner's lease expires, a new fenced owner makes
  another at-least-once request with the original key.
- a live worker presenting an old fence token cannot append completion, retry,
  or failure after ownership has moved.
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
  response. See `.memory/projections/plans/graceful-drain.md`.

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

This baseline submission phase was completed before the scope expansion. The
README was subsequently reorganized around the evaluator's four requested
sections: built/cut scope, key decisions and uncertainty, failure modes, and AI
usage. Phase 13 will update it again only for the additional implemented
features and evidence.

Rewrite `README.md` as the final submission write-up. Keep it succinct and use
this order:

1. **Problem statement** — the ordered workflow, ambiguous network outcome, and
   narrow guarantee boundary.
2. **Functional requirements** — workflow creation/inspection, ordered steps,
   retries, recovery, event-history inspection, and graceful drain.
3. **Non-functional requirements** — crash durability, stable idempotency,
   deterministic failure testing, type safety, and simple local startup.
4. **Architecture** — FastAPI, single worker, `engine.db`, independent mock tool
   and `ledger.db`, with no shared transaction.
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
3. Show that the charge commits in `ledger.db`, then that the worker is killed
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
- Recorded root-level `crashsafe-demo.mp4`: a cropped plain-terminal
  `SIGKILL` recovery with same-key attempts, history audit, one-charge evidence,
  and the final history-derived timeline.

### Phase 10 — configurable leased worker pool

Status: complete.

Implementation:

1. Add `CRASHSAFE_WORKERS` (default `1`), `CRASHSAFE_LEASE_TTL`, and
   `CRASHSAFE_LEASE_RENEW_INTERVAL` settings. Reject invalid combinations such
   as a renewal interval greater than or equal to the TTL.
2. Add `workflow_leases` with owner, expiry, and monotonically increasing fence
   token. Keep lease state outside the event reducer because it is operational
   ownership, not workflow business state.
3. Replace `next_runnable_step()` with an atomic claim operation. Under
   `BEGIN IMMEDIATE`, select an eligible workflow whose lease is absent or
   expired, exclude tool-throttled candidates, acquire it, and return the step
   plus fence token.
4. Require owner and fence token on every worker-originated storage transition.
   A stale owner receives a typed `LeaseLostError`, discards its result, and
   performs no further workflow write.
5. Renew the lease in a small background heartbeat while the worker is blocked
   in synchronous HTTP. Never hold a SQLite transaction during network I/O.
6. Release after a committed attempt outcome when no immediately runnable step
   remains, and on graceful drain after the in-flight attempt commits. A hard
   crash relies only on expiry.
7. Make the supervisor start and maintain the configured number of uniquely
   identified worker processes. PID files and logs must include worker IDs.

Concurrency model:

- Different workflows may execute in parallel.
- One unexpired lease is the only valid claim for a workflow.
- Lease expiry permits at-least-once overlapping compute; fencing guarantees
  that only the current owner can advance engine state.
- External duplicates remain safe only through the stable tool idempotency key.
- SQLite still serializes short write transactions; concurrency is gained across
  the HTTP-bound execution intervals, not by parallel database writers.

Verification and acceptance:

- Two workers claim different workflows and overlap delayed tool calls.
- Repeated concurrent claim attempts yield one owner/token for a workflow.
- Renewal prevents takeover during a healthy long request.
- Killing the owner leaves the workflow unavailable until TTL, then another
  worker claims it with a higher token and completes with the same key.
- A deliberately paused stale worker cannot commit after the higher token is
  issued.
- Graceful drain finishes, releases its lease, and claims no new workflow.

### Phase 11 — adaptive tool-wide backoff

Status: complete.

Implementation:

1. Add immutable `tool_key` to step definitions and projections; version/upcast
   existing `WorkflowCreated` payloads with the default `mock-tool` identity.
2. Add `tool_throttles(tool_key, blocked_until, reason, updated_at)`.
3. On HTTP 429, parse numeric `Retry-After`, compute one absolute UTC deadline,
   and atomically append `StepRetryScheduled`, update the step projection, and
   extend the tool throttle with `max(existing, deadline)`.
4. Make claim selection exclude every workflow whose next step targets a tool
   with `blocked_until > now`. All workers use the same durable gate.
5. Preserve exponential per-step backoff when no valid `Retry-After` is present;
   do not turn malformed headers into permanent failures.
6. Use an injectable clock in storage/engine tests so throttle eligibility is
   deterministic.

This is intentionally a cooldown, not a full rate controller. It reacts to an
explicit provider instruction and persists that instruction across worker and
process restarts. It does not estimate capacity, implement a token bucket, or
claim fairness across tools.

Verification and acceptance:

- A 429 for workflow A prevents workflow B from calling the same tool before
  the shared deadline.
- A later, longer `Retry-After` extends the gate; a shorter one cannot reduce it.
- Restarting all workers during the wait does not reset the deadline.
- A different `tool_key` remains eligible.
- At expiry, only normally leased work resumes; idempotency keys are unchanged.

### Phase 12 — history-derived observability

Status: complete.

Implementation:

1. Add typed timeline and summary response models and
   `GET /workflows/{id}/timeline`.
2. Derive the timeline from ordered workflow events rather than introducing a
   second mutable source of truth. Include sequence, relative time, event,
   step, attempt, worker ID, fence token, selected wait, and outcome.
3. Add version-2 attempt metadata for worker ID/fence token while keeping the
   reducer backward compatible with version-1 histories.
4. Derive summary values: workflow duration, per-step duration, attempt and
   retry counts, planned wait time, terminal status, and projection-audit result.
5. Add a compact plain-text formatter used by demos. Unknown outcomes appear as
   an unmatched attempt rather than as an invented crash event.

Verification and acceptance:

- Timeline ordering always follows per-workflow sequence, never wall-clock sort.
- Retry waits and repeated same-key attempts are represented correctly.
- Timeline generation is read-only and leaves event/projection state unchanged.
- Version-1 histories remain viewable with absent worker metadata.
- The final demo timeline makes the killed attempt, replacement worker, retry,
  and terminal outcome understandable in one screen.

### Phase 13 — integrated verification, README, and demos

Status: complete.

1. Keep `make demo` focused on the mandatory proof, but run a two-worker pool:
   commit the charge, kill its lease owner, wait for expiry/reclaim, recover with
   the same key, and print the durable timeline plus one-charge ledger.
2. Add `make demo-features` for multiple workflows and a deterministic 429. Show
   two workers processing separate workflows, the persisted tool-wide cooldown,
   delayed eligibility of another workflow, and final timelines.
3. Extend the process suite with real two-worker claim, expiry, stale-fence,
   renewal, adaptive-throttle, restart, and timeline tests.
4. Run the full suite repeatedly to expose timing flakes; keep injected clocks,
   barriers, and commit files for assertions instead of fixed sleeps.
5. Revise README built/cut scope, concurrency model, uncertainty, and failure
   table. Add lease-expiry/stale-owner failure modes without weakening the
   external idempotency boundary.
6. Update `TESTING.md`, record a replacement terminal video, and report the new
   verified test count only after every acceptance criterion passes.

Phase 13 is complete only when a reviewer can see both claims independently:
two workers increase throughput across workflows, and ownership failure still
reduces to fenced database state plus safe at-least-once tool requests.

### Phase 14 — materialized JSON DAG workflows

Status: complete.

#### 14.1 Minimal request contract

Change `POST /workflows` so its body is the complete concrete execution plan:

```json
{
  "name": "paid-activation",
  "steps": [
    {
      "id": "charge",
      "operation": "charge",
      "depends_on": [],
      "request": {
        "customer_id": "customer-123",
        "amount_cents": 4200
      }
    },
    {
      "id": "provision",
      "operation": "provision",
      "depends_on": ["charge"],
      "request": {
        "customer_id": "customer-123",
        "plan": "standard"
      }
    },
    {
      "id": "receipt",
      "operation": "notify",
      "depends_on": ["charge"],
      "request": {
        "customer_id": "customer-123",
        "email": "customer@example.com",
        "message": "Payment received. Activation is in progress."
      }
    }
  ]
}
```

There is no separate input object, registry, version resolver, file loader,
templating, interpolation, output reference, or expression language. Each
request is already fully materialized when submitted. The engine generates the
workflow ID and operation keys; clients cannot supply either.

Use a Pydantic discriminated union keyed by `operation` so `charge`,
`provision`, and `notify` retain their existing typed request validation. A
workflow-level model validator additionally enforces:

- one to 32 steps, step IDs matching `[A-Za-z0-9_-]{1,64}`, and the existing
  request-field bounds;
- unique, nonempty step IDs;
- only the three allowlisted operations;
- existing dependency targets, with no self-edge or duplicate edge;
- acyclicity using a deterministic topological sort;
- `extra="forbid"` at workflow, step, and operation-request boundaries.

Array order is the deterministic tie-breaker when multiple steps are ready; it
does not create an implicit dependency. Dependencies must be explicit.

Acceptance: FastAPI returns `422` for invalid request shape, operation payload,
dependency, or cycle; a valid body becomes durable without workflow-specific
Python branching.

#### 14.2 Immutable history and schema cutover

- Extend `WorkflowCreated` to schema version 3 with the workflow name and the
  complete validated step list: client step ID, array order, operation,
  dependencies, concrete request, `tool_key`, and generated operation key.
- Generate each key as `"{workflow_id}:{step_id}"`; retries and lease takeovers
  reuse it unchanged.
- Replace the workflow projection's specialized input with `name` and the
  submitted definition JSON. Add stable client step ID and array order to the
  step projection.
- Add normalized
  `step_dependencies(workflow_id, step_id, dependency_step_id)` rows so
  readiness is queryable without parsing JSON during worker polls.
- Commit `WorkflowCreated`, the workflow projection, all step projections, and
  every dependency edge in one `BEGIN IMMEDIATE` transaction.
- Treat current development databases as a legacy schema at this boundary.
  Detect a nonempty pre-DAG database and fail with the existing documented
  instruction to remove `.crashsafe/` and restart rather than inventing
  dependencies or rewriting old history. No database file is part of the
  submitted repository.

Acceptance: once `POST /workflows` returns `201`, the entire executable plan is
recoverable from history alone and no external definition source is required.

#### 14.3 API surface

- Keep one creation endpoint: `POST /workflows` with the materialized JSON body.
- Keep `GET /workflows`, `GET /workflows/{id}`,
  `GET /workflows/{id}/events`, and `GET /workflows/{id}/timeline`.
- Return the workflow name and each step's ID, operation, dependencies, concrete
  request, generated operation key, and execution state from workflow reads.
- Remove `GET /workflows/{id}/audit`; add no definition discovery or CRUD
  endpoints.
- Update all demos and API tests to submit complete workflow definitions. Since
  there are no released consumers, use one clear request shape rather than a
  legacy-body union.

Acceptance: two different JSON bodies execute through the same endpoint,
storage, reducer, scheduler, and workers.

#### 14.4 Dependency-aware scheduling

- Replace the earlier-position predicate with: a candidate step is runnable
  only if no dependency row points to a step whose status is not `completed`.
- Retain `pending`, `intent_recorded`, and eligible `retry_wait` handling,
  tool-wide cooldown checks, and atomic workflow lease acquisition.
- When multiple steps are ready, select the lowest submitted array order. A
  workflow-level lease means they execute sequentially within one run.
- Complete the workflow atomically with the step completion that leaves no
  unfinished steps. Do not assume a particular step is “last.”
- Preserve fail-fast semantics: one terminal step failure appends
  `StepFailed`/`WorkflowFailed`; downstream steps remain unexecuted.
- Keep workers generic by dispatching `step.operation` through the existing
  allowlisted tool client.

Acceptance: no step runs before every dependency completes; two ready branches
run in deterministic order; independent workflows still run concurrently on
different workers.

#### 14.5 Reducer correctness, with public audit removed

- Extend the pure reducer to reconstruct workflow metadata, concrete steps,
  dependencies, attempts, outputs, and workflow terminal state from history.
- Validate acyclicity, immutable graph structure, stable step keys, legal
  dependency completion, monotonic attempts, and exactly one creation event.
- Keep reducer-versus-projection comparison in storage and process tests and
  retain explicit projection rebuild as an internal recovery/development tool.
- Remove `WorkflowAudit`, `GET /workflows/{id}/audit`,
  `TimelineSummary.audit_consistent`, demo `audit` output, and README language
  that presents audit as an assignment feature.
- Keep `GET /workflows/{id}/events` and the timeline. They expose durable facts;
  they do not claim a separate audit product.

This cuts API surface and demo noise, not correctness. Every transition still
commits its event and projection atomically, and every failure test still folds
history and asserts exact projection equality.

Acceptance: corrupt or illegal histories fail the reducer; all valid histories
reconstruct exactly; the public API and demo contain no audit feature.

### Phase 15 — verification, repository cleanup, write-up, and one demo

Status: complete.

1. Add request-model tests for duplicate steps, missing dependencies, cycles,
   unknown operations, invalid operation requests, size limits, forbidden
   fields, and stable topological order.
2. Add storage/engine tests proving dependency readiness, deterministic branch
   ordering, join-by-workflow-completion, fail-fast behavior, lease recovery,
   retry persistence, and stable keys derived from client step IDs rather than
   array position.
3. Re-run every real-process crash point against the paid-onboarding JSON,
   especially the ambiguous charge result, and assert reducer/projection
   equality inside the test rather than printing “audit.”
4. Add `examples/paid-onboarding.json` and `examples/trial-activation.json`.
   The paid workflow retains the ambiguous charge; the trial workflow branches
   after provisioning into customer and internal notifications without adding a
   second charge.
5. Replace `scripts/demo_ambiguous_charge.py` and
   `scripts/demo_extended_features.py` with one reviewable `scripts/demo.py`,
   run as `uv run python scripts/demo.py`. Its single deterministic narrative
   must:
   - submit both JSON workflows;
   - start two workers so different workflows can progress concurrently;
   - kill the paid workflow's owner after the tool commits its only charge but
     before engine completion;
   - recover under a higher fence with the same operation key;
   - complete both workflows and print their history-derived timelines;
   - finish with exactly one charge, two provisions, and three notifications.
6. Keep adaptive backoff and graceful-drain demonstrations in focused tests and
   `TESTING.md`; do not add alternate demo modes or scripts merely to display
   every implemented feature.
7. Remove the entire `experiments/` tree, all Make targets, and every
   README/manual/memory reference that suggests the Temporal experiment is part
   of the submission. Temporal may remain a brief architectural comparison in
   prose, not executable repository scope.
8. Use uv as the only project interface: move development dependencies to the
   `dev` dependency group, commit `uv.lock`, remove the Makefile, and document
   `uv sync` plus `uv run ...` commands. Remove obsolete demo targets, helper
   scripts, stale recordings, generated state, caches, and superseded
   documentation. After cleanup, `scripts/` contains only `demo.py`, the root
   contains only the current `crashsafe-demo.mp4`, and `rg --files` shows no
   generated databases, logs, virtual environments, or Python caches.
9. Rewrite README built/cut scope, execution model, failure table, request
   example, and demo commands. State explicitly that Crashsafe executes a
   submitted materialized DAG; it does not parse templates or replay arbitrary
   workflow code like Temporal.
10. Update `TESTING.md` around one canonical demo plus focused feature tests.
    Refresh `crashsafe-demo.mp4` because the request body, step IDs, consolidated
    flow, and removal of public audit output make the current video outdated.
11. Run the entire suite at least three times, then run Ruff, strict mypy, and
    the sole `uv run python scripts/demo.py` command before marking either phase
    complete.

Phase 15 is complete only when two submitted JSON workflows execute through the
same generic engine, the original kill/recovery guarantee is unchanged, the
public submission no longer markets audit as a feature, and a reviewer sees one
demo script, one demo command, one video, and no experimental implementation.

## 16. Verification matrix

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
| Tool committed, worker killed before response | Real `SIGKILL` process test and canonical uv demo |
| Graceful signal during in-flight response | Real `SIGTERM` process test |
| Static quality | Ruff and strict mypy |
| Append-only enforcement | Direct SQL update/delete rejection tests |
| Event/projection transaction atomicity | Exception rollback and real-process `SIGKILL` tests |
| Per-workflow event ordering | Unique, contiguous-sequence tests |
| Projection reconstruction | Internal reducer equality and explicit rebuild tests |
| Ambiguous outcome represented faithfully | History assertion plus one-entry tool ledger |
| Retry decision independent of later config | Persisted timestamp/restart test |
| Atomic workflow claim under contention | Two-worker lease contention test |
| Lease renewal and expiry recovery | Delayed-call heartbeat and killed-owner process tests |
| Stale-owner fencing | Higher-token commit rejection test |
| Parallel progress across workflows | Barrier-controlled two-worker test |
| Tool-wide `Retry-After` propagation | Two-workflow shared-throttle test |
| Throttle persistence across restart | Reopened-storage cooldown test with injected clock |
| Timeline correctness and read-only behavior | Event-derived timeline and API tests |
| Canonical two-workflow crash demo | Sole `uv run python scripts/demo.py` assertions |
| Adaptive backoff and graceful drain | Focused engine and real-process tests documented in `TESTING.md` |
| JSON workflow validation | Pydantic and graph validation tests |
| DAG dependency readiness | Storage tests with blocked and ready nodes |
| Complete plan persistence | History-only reconstruction and restart tests |
| Generic execution of two workflows | Canonical uv demo and API tests |
| Reducer correctness without public audit | Failure tests plus removed endpoint/summary assertions |
| Reviewable repository surface | Verified inventory: one demo script, one video, no `experiments/` or generated state |

Verified after the expansion: 28 tests passed, Ruff passed, strict mypy passed,
and both terminal demos passed. The mandatory demo transfers ownership from
worker 1 to worker 2 after `SIGKILL`; the feature demo runs two workers and two
workflows concurrently while forcing one durable shared cooldown.

## 17. Risks discovered and corrections

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

## 18. Completion boundary and later work

The current durability implementation is complete: append-only event history,
atomic projection updates, reducer reconstruction, graceful drain, and the
ambiguous-outcome evidence all pass.

The expanded submission implements the leased configurable worker pool, shared
adaptive cooldown, history-derived timeline, and two-worker demos. The
durability guarantee did not move: SQLite transactions define accepted engine
state, fence tokens reject stale owners, and external exactly-once effects still
depend on durable tool idempotency.

The accepted next boundary is a materialized JSON DAG submitted with each run,
not a reusable-definition service or general workflow language. It adds an
immutable graph snapshot, dependency-aware readiness, and two example payloads
while preserving the same history, transaction, fencing, and idempotency
guarantees. Public audit presentation is removed, while reducer reconstruction
stays in the correctness test suite. The submission surface is reduced to one
canonical demo script, one demo command, one current recording, and no
experimental implementation tree.

Even after this revision, a production distributed engine would separately
require remote ownership consensus, shards, durable task queues, cross-node
clock assumptions, history partitioning, reusable definition lifecycle,
arbitrary workflow versioning, authentication, and multi-tenant isolation. The
worker pool remains bounded local concurrency, and the DAG remains a bounded
concrete execution plan.
