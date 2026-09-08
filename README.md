# Crashsafe

Crashsafe is a small durable workflow engine for the fixed sequence
`charge → provision → notify`, built for the
[Stack AI take-home](https://stack-ai.notion.site/Senior-Software-Engineer-Take-Home-Durable-Execution-Engine-3a7974ca255c811d9ffac55e5ad72a4a).
It is written in Python with FastAPI, Pydantic, and SQLite.

Its core guarantee is deliberately narrow: workflow state survives arbitrary
worker termination, and incomplete external calls are retried **at least once**.
Exactly one external side effect is possible only when the tool durably honors
the stable idempotency key supplied by Crashsafe.

## 1. What I built and what I cut

### Built

- **Crash-safe resume.** One worker persists workflow input, ordered steps,
  request payloads, operation keys, outputs, and retry eligibility in SQLite.
- **Append-only event history.** Typed workflow events are the logical record;
  indexed workflow and step rows are rebuildable scheduling projections. Each
  event and its projection update commit in one transaction.
- **At-least-once requests with idempotent effects.** Intent is committed before
  HTTP I/O. A retry reuses the same operation key. The mock tool atomically
  stores its side effect and cached response in a separate SQLite database.
- **Safe retries.** The mock tool can return HTTP 429. Crashsafe persists the
  selected absolute `next_attempt_at`, including numeric `Retry-After`, so a
  restart does not reset or recalculate an existing wait.
- **Graceful drain.** `SIGTERM` and `SIGINT` stop admission of new work, allow
  the current synchronous attempt and state transaction to finish, then exit.
- **Inspectable evidence.** FastAPI exposes workflows, history, and a reducer
  audit. Tests use real child processes, deterministic crash hooks, and durable
  commit signals rather than simulated exceptions alone.

### Cut

I intentionally skipped concurrent workers, leases and fencing, distributed
storage, shards/history trees, fan-out, cancellation, compensation, arbitrary
workflow definitions, authentication, and a UI. Those features primarily solve
coordination, scale, or product breadth. Adding them would obscure the requested
single-worker durability proof and introduce a different class of correctness
problems.

Temporal is not a runtime dependency. A small comparison under
`experiments/temporal_compare/` checks the same ambiguous charge with one
Temporal worker, but it is kept outside the engine.

## 2. Key decisions

### Execution model and state machine

A single polling worker selects the first eligible step whose predecessors are
complete. It performs one attempt synchronously and then polls again. `engine.db`
contains history and scheduling state; the independently durable mock tool owns
`tools.db`.

```text
pending ──attempt committed──► intent_recorded
                                   │       │
                         success   │       │ retryable failure
                                   ▼       ▼
                              completed  retry_wait
                                             │ eligible time reached
                                             └────────► intent_recorded

intent_recorded ──permanent failure / exhausted budget──► failed
```

The final step completion and workflow completion commit together. Steps are
never selected out of order.

### Why resume stays correct

1. Workflow creation fixes every step's request and operation key.
2. Before a request, `StepAttemptStarted` and the `intent_recorded` projection
   commit with SQLite WAL mode and `synchronous=FULL`.
3. If completion is missing after a restart, the outcome is treated as unknown
   and the request is repeated with the same key.
4. The mock tool either has no record and applies the effect, or returns the
   response atomically cached with the earlier effect. Reusing a key with a
   different operation or payload is rejected.
5. `StepCompleted` and its projection commit atomically. A committed step is
   skipped; a partially written completion transaction is rolled back.
6. Folding the ordered event stream must equal the stored projections; the
   audit endpoint makes that invariant observable.

The engine therefore guarantees durable recovery and at-least-once requests.
The tool's idempotency transaction—not the workflow event log—provides one
charge in the ambiguous-response case.

### Decisions I am least sure about

- **Keeping both history and projections.** A carefully designed step-state
  table is enough for this fixed workflow. I kept the event stream because it
  makes crash boundaries auditable and projections reconstructable, but it adds
  schema evolution and transition-writing cost that may be excessive here.
- **Finishing the in-flight call during drain.** This makes shutdown behavior
  easy to reason about, but deployment grace must exceed the request timeout.
  An alternative is to cancel immediately and recover the unknown outcome with
  the same key; that is faster but adds cancellation races without improving
  the hard-crash guarantee.

## 3. Failure modes

| Exact worker death point | Durable state after restart | Recovery behavior | Why it remains correct |
|---|---|---|---|
| Before the attempt transaction commits | Step is `pending`; no `StepAttemptStarted` | Start attempt 1 normally | No request was authorized by durable state and no partial transaction is visible. |
| After attempt commit, before the HTTP request | Step is `intent_recorded`; attempt 1 has no completion | Start attempt 2 with the same request and key | The first request was not sent, so the retry creates the effect once. An unnecessary duplicate request would still be safe. |
| After the tool commits the charge, before the worker receives the response | Engine has only the unmatched attempt; `tools.db` has the effect and cached result | Retry with the same key; tool returns the original receipt | Side effect and idempotency result committed atomically, so the charge is not repeated. This is the primary demo. |
| After the response, while the completion event/projection transaction is uncommitted | Engine still has only the unmatched attempt; tool result is durable | SQLite rolls back the whole incomplete transaction; retry retrieves the cached result | The event append and projection update cannot become visible independently. |
| After the completion transaction commits | Step is `completed` with output; terminal workflow state may also be committed | Skip the completed step and run only the next eligible step | Completion is a durable scheduling fact; recovery never reopens it. |

An unmatched `StepAttemptStarted` intentionally means **outcome unknown**. It
does not claim the network request was delivered. Retrying with a stable key is
what makes both possible worlds safe.

## 4. AI usage

AI helped enumerate instruction-level crash windows, scaffold typed storage and
API boundaries, write the event reducer, and build real-process recovery tests.
It was most useful as a fast adversarial checklist generator; none of its
durability claims were accepted without an executable failure test.

It was also wrong in concrete ways:

- It initially interpreted “project memory” as a runtime CRUD subsystem. Reading
  the request again exposed the scope error, and that API, database, entry point,
  and its tests were removed.
- It generated Python 3.10 union syntax that Pydantic evaluated at runtime even
  though Python 3.9 was supported. The end-to-end run failed; the annotations
  were replaced with compatible `Optional[...]` forms.
- An early pass treated SQLite durability settings too globally. Review showed
  `synchronous=FULL` is connection-local, so every correctness-sensitive
  connection now configures it explicitly.
- It overbuilt the first video as a browser console. Reviewing the actual
  submission need led to deleting that harness and recording the real terminal
  flow instead.

These errors were caught through requirement rereads, strict mypy and Ruff,
end-to-end execution, event/projection audits, and process tests that send real
`SIGKILL` and `SIGTERM` signals. The current suite has 18 tests, including the
ambiguous charge, an interrupted SQLite transition, deterministic crash points,
persisted retries, and graceful drain.

## Run and verify

Requires Python 3.9+.

```bash
make setup
make test
make lint
make demo
```

`make demo` creates isolated engine and tool databases, waits for the charge to
commit, sends the worker `kill -9` before completion is saved, starts a
replacement worker, and verifies two same-key attempts but one durable charge.

[Watch the 13-second plain-Terminal demo](crashsafe-demo.mov).

For focused crash-safety, retry, and graceful-drain checks, follow
[TESTING.md](TESTING.md). To explore the API, run `make run` and open
<http://127.0.0.1:8000/docs>.

The API surface is intentionally small:

```text
POST /workflows
GET  /workflows
GET  /workflows/{id}
GET  /workflows/{id}/events
GET  /workflows/{id}/audit
```
