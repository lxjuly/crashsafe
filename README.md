# Crashsafe

Crashsafe is a small durable workflow engine for the fixed sequence
`charge → provision → notify`, built for the
[Stack AI take-home](https://stack-ai.notion.site/Senior-Software-Engineer-Take-Home-Durable-Execution-Engine-3a7974ca255c811d9ffac55e5ad72a4a).
It uses Python, FastAPI, Pydantic, and SQLite.

The guarantee is deliberately narrow: engine state survives arbitrary worker
termination and incomplete external calls are retried **at least once**. Exactly
one external side effect requires the called tool to durably honor Crashsafe's
stable idempotency key.

## 1. What I built and what I cut

### Built

- **Crash-safe resume.** Workflow input, ordered steps, request intent, stable
  operation keys, outputs, and retry eligibility survive `kill -9`.
- **Append-only event history.** Typed per-workflow events are the logical
  record. Mutable workflow and step rows are rebuildable scheduling projections;
  an event and its projection update commit in one SQLite transaction.
- **Safe retries and idempotent effects.** The mock tool atomically stores each
  side effect and its idempotency result in a separate durable database. A lost
  response is recovered with the same key and original result.
- **Concurrent workers.** `CRASHSAFE_WORKERS` defaults to one and can run a local
  pool. A renewable workflow lease prevents simultaneous valid ownership;
  monotonically increasing fence tokens reject a stale owner's state writes.
- **Graceful drain.** `SIGTERM`/`SIGINT` stops admission, finishes the current
  synchronous attempt and state commit, releases its lease, and exits.
- **Adaptive backoff.** A 429 persists both the step's absolute retry time and a
  maximum tool-wide cooldown. Every worker consults that gate, so another
  workflow cannot start a new request to the same tool before `Retry-After`.
- **Observability.** `GET /workflows/{id}/timeline` folds immutable history into
  sequence, relative time, step, attempt, worker/fence, waits, outcomes, and an
  audit summary. Both demos print this view.
- **Failure evidence.** Deterministic 429 and hard-crash injection, real
  `SIGKILL`/`SIGTERM` process tests, history reconstruction, and two runnable
  terminal demos exercise the boundaries above.

### Cut

I skipped distributed storage and consensus, shards/history trees, remote task
queues, fan-out/DAGs, cancellation and compensation, arbitrary workflow code,
authentication, tracing infrastructure, and a graphical UI. The local lease
model adds bounded concurrency without pretending SQLite is a distributed
coordinator. The adaptive policy is a persisted provider-directed cooldown, not
a token bucket or capacity estimator.

Temporal is not a runtime dependency. `experiments/temporal_compare/` contains
an optional one-worker comparison of the same ambiguous-charge scenario.

## 2. Key decisions

### Execution model and architecture

The FastAPI service only creates and reads workflows. Independently polling
worker processes execute one synchronous attempt at a time. `engine.db` owns
history, projections, leases, and tool cooldowns; the mock service independently
owns `tools.db`, including its side-effect ledger and idempotency responses.

```text
FastAPI ──create/read──► engine.db ◄──claim/fenced commits── workers (1..N)
                           │                                  │
                           │ append-only history              │ HTTP + stable key
                           │ + rebuildable projections         ▼
                           └────────────────────────────── mock tool ──► tools.db
```

SQLite uses WAL mode and `synchronous=FULL` on every connection. Write
transactions are short and never span network I/O. SQLite serializes those
writes, while workers overlap the slower HTTP calls for different workflows.

### State machine

```text
pending ──attempt committed──► intent_recorded
                                   │       │
                         success   │       │ retryable failure
                                   ▼       ▼
                              completed  retry_wait
                                             │ deadline reached
                                             └────────► intent_recorded

intent_recorded ──permanent failure / exhausted budget──► failed
```

The final `StepCompleted`, projection update, `WorkflowCompleted`, and workflow
projection update share one transaction. Predecessors must be complete before a
later step can be claimed.

### How resume and concurrent ownership stay correct

1. Creation fixes each step's request, tool identity, and operation key.
2. A worker atomically acquires a workflow lease. Reacquisition increments its
   fence token; healthy calls renew the lease in a background heartbeat.
3. Before HTTP I/O, `StepAttemptStarted` and `intent_recorded` commit with the
   worker ID, fence token, request, and operation key.
4. If completion is absent after restart, the outcome is unknown. A replacement
   waits for lease expiry, receives a higher token, and repeats the same request
   and key.
5. The tool either applies and records the effect atomically, or returns the
   result cached by the earlier effect. A key reused for different input is
   rejected.
6. Every worker-originated state transition validates unexpired ownership and
   the exact fence token. A stale result cannot advance engine state.
7. Folding ordered history must reproduce the scheduling projection; the audit
   endpoint and timeline expose that invariant.

This provides one valid **committing** owner, not exactly-once compute. A paused
worker can overlap a replacement after expiry. Fencing protects engine state;
the stable key and tool transaction protect the external effect.

### Decisions I am least sure about

- **Wall-clock leases on one host.** They are compact and adequate for this
  local SQLite design, but require sensible TTL/renewal settings and a monotonic
  fence. A production multi-host engine should use a remote consensus-backed
  coordinator and define clock assumptions explicitly.
- **One shared cooldown per tool.** It faithfully honors the mock provider's
  `Retry-After` and survives restart, but is intentionally conservative: one
  throttled request pauses every workflow targeting that tool.

## 3. Failure modes

| Exact worker death or ownership point | Durable state | Recovery | Why it remains correct |
|---|---|---|---|
| Before the attempt transaction commits | Step is `pending`; no attempt event | Another worker claims it and starts attempt 1 | No request was authorized by durable state; a partial transaction is invisible. |
| After intent commits, before HTTP send | Step is `intent_recorded`; lease eventually expires | Replacement records attempt 2 with a higher fence and the same key | The first request was not sent; even an unnecessary duplicate would be idempotent. |
| After the tool commits charge, before the worker receives its response | Engine has an unmatched attempt; `tools.db` has one effect and cached result | Same-key retry returns the original receipt | Effect and idempotency response committed atomically. This is the primary demo. |
| Old worker returns after its lease expired and a replacement acquired the workflow | Replacement owns a higher fence token | Old completion is rejected; replacement retries or commits | Only the current owner can mutate engine state. Possible external duplicates still share one key. |
| After response while completion transaction is uncommitted, or after it commits | Before commit: unmatched attempt. After commit: completed step and output | SQLite rolls back the incomplete transaction and retries, or skips the committed step | Event and projection never become visible separately; committed completion is never reopened. |

An unmatched `StepAttemptStarted` means **outcome unknown**. It does not prove a
request was delivered. The stable key makes both possible worlds safe.

## 4. AI usage

AI helped enumerate instruction-level crash windows, scaffold typed boundaries,
write the reducer and process harness, and generate adversarial checks. It was
most useful as a fast hypothesis generator; durability claims were accepted only
after executable tests or direct inspection.

It was also wrong in specific, useful ways:

- It first interpreted “project memory” as a runtime CRUD service. Rereading the
  request caught the scope error and that code was removed.
- It produced Python 3.10 union syntax despite advertised Python 3.9 support.
  The end-to-end run failed, and compatible `Optional[...]` annotations fixed it.
- It initially configured `synchronous=FULL` only during schema setup. Review
  caught that the setting is connection-local, so every connection applies it.
- The first lease implementation deleted a released lease row, which would
  reset its fence counter. An adversarial stale-owner review caught the token
  reuse; released rows now retain and monotonically advance the fence.
- It initially overbuilt a browser demo. Matching the submission requirement
  led to a short recording of the real terminal and literal `kill -9` instead.

These mistakes were caught with requirement rereads, strict mypy, Ruff, event
audits, deterministic clocks/barriers, and real child-process signals. The final
suite has 28 tests.

## Run and verify

Requires Python 3.9+.

```bash
make setup
make test
make lint
make demo
make demo-features
```

- `make demo` kills worker 1 after the tool commits charge but before the engine
  saves completion. Worker 2 resumes with a higher fence and the same operation
  key. The final ledger contains one charge.
- `make demo-features` starts two workers and two workflows, forces one 429,
  shows the shared persisted wait, drains both workers, and prints timelines.

[Watch the plain-Terminal crash-recovery demo](crashsafe-demo.mov).

For focused manual checks, see [TESTING.md](TESTING.md). Run `make run` and open
<http://127.0.0.1:8000/docs> to explore:

```text
POST /workflows
GET  /workflows
GET  /workflows/{id}
GET  /workflows/{id}/events
GET  /workflows/{id}/audit
GET  /workflows/{id}/timeline
```
