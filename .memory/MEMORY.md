# Crashsafe Memory

Updated: 2026-09-09T03:45:00Z

## Claims

- [settled] assignment-purpose: Crashsafe is a senior software engineer take-home for a small durable execution engine.
- [settled] required-stack: The assignment requires Python, FastAPI, Pydantic, type annotations, separated engine/storage/API layers, and restart-safe persistence.
- [settled] workflow-shape: A submitted JSON definition contains an allowlisted,
  ordered DAG; the repository examples include paid onboarding and trial activation.
- [settled] correctness-boundary: The engine guarantees durable recovery and at-least-once tool requests; exactly-once side effects require durable idempotency at the tool boundary.
- [settled] implemented-scope: The repository implements a configurable local SQLite-backed worker pool, renewable workflow leases and fencing, append-only typed event history, rebuildable scheduling projections, persisted per-step and tool-wide retry scheduling, graceful drain, timeline observability, deterministic failures, and real process recovery tests.
- [settled] ambiguous-charge-proof: The demo kills the worker after the mock tool commits a charge but before the worker persists completion, then recovers with one charge in the durable ledger.
- [settled] graceful-drain-verified: A real-process test sends SIGTERM during a delayed committed charge response; the worker commits only the in-flight step, exits cleanly, and a restart finishes with one of each side effect.
- [settled] full-plan-projected: The completed baseline and extension plan through
  Phase 15 is stored at `.memory/projections/plans/crashsafe-engine.md`.
- [settled] durability-scope: Shards and history trees remain deferred; a bounded local lease and fence token are now planned specifically to add safe multi-worker ownership without changing the SQLite durability boundary.
- [settled] event-history-implemented: Phase 8 added an append-only, per-run event
  stream that commits atomically with rebuildable `workflow_runs` and `steps`
  projections.
- [settled] retry-policy-boundary: Persisting `next_attempt_at` is durable per-step state; the expansion also plans a persisted monotonic tool-wide `blocked_until` derived from `Retry-After`.
- [superseded] final-verification: The earlier 28-test verification was superseded
  by the expanded suite and workflow-run terminology migration.
- [settled] readme-completed: The final README follows the agreed problem, requirements, architecture, data/invariants, state-machine, and demo structure.
- [settled] temporal-comparison-verified: An isolated persistent Temporal server with one worker reproduces the ambiguous charge outcome; Activity attempt 2 reuses the application key, returns the cached tool result, and completes with exactly one charge.
- [settled] temporal-history-difference: In the observed run, Temporal represented the lost first execution through Activity attempt 2 with the prior Start-To-Close timeout, while Crashsafe retains two explicit `StepAttemptStarted` events.
- [superseded] original-submission-video: The earlier 7.9-second fixed-workflow recording was replaced after the JSON DAG and consolidated-demo revision.
- [superseded] feature-test-manual: Separate crash and retry scripts were folded
  into the canonical demo; graceful drain remains the only focused manual check.
- [settled] handoff-ready-memory: Project memory lives under `.memory/`; the current projection and dated journal identify the verified implementation and completed Git handoff.
- [settled] evaluator-focused-readme: The README now follows the requested review structure: built/cut scope, key decisions and uncertainties, five precise failure modes, and concrete AI usage mistakes and verification.
- [settled] extension-scope-complete: Concurrent workers, observability, and adaptive backoff are implemented and verified without changing the core durability boundary.
- [settled] concurrency-model-implemented: `CRASHSAFE_WORKERS` defaults to one and supports two for demos; workflow-level renewable leases plus monotonic fence tokens permit one valid committing owner while preserving safe at-least-once recovery after expiry.
- [settled] observability-model-implemented:
  `GET /workflow_runs/{run_id}/timeline` and the demo formatter derive sequence,
  relative timing, attempts, worker/fence ownership, waits, and outcomes from
  ordered history.
- [settled] adaptive-backoff-model-implemented: A durable per-tool cooldown propagates the maximum `Retry-After` deadline across workflows, workers, and restarts.
- [superseded] deterministic-feature-demo: The former Make-based feature demo was
  replaced by one canonical uv-driven demo and one focused graceful-drain script.
- [settled] expanded-final-verification: The 28-test suite passed three consecutive runs; Ruff, strict mypy, `make demo`, and `make demo-features` then passed again.
- [settled] workflow-run-contract: JSON definitions live under `workflows/` as
  authoring examples, not registered server resources. Each
  `POST /workflow_runs` validates and snapshots one definition into a new durable
  run identified by `run_id`.
- [settled] dag-scope-boundary: The accepted DAG contains allowlisted operations, concrete request bodies, explicit dependencies, and deterministic array order; it excludes registries, templates, separate inputs, output references, expressions, Python workflow code, definition CRUD, conditions, loops, dynamic fan-out, and intra-workflow parallel execution.
- [accepted] audit-feature-cut: Remove the public audit model, endpoint, timeline flag, demo output, and feature language while preserving the reducer, atomic event/projection commits, internal reconstruction, and equality assertions in failure tests.
- [accepted] phase-15-repository-cleanup: Remove `experiments/`, consolidate the two original demo scripts into one `scripts/demo.py` invoked through uv, retain broader feature evidence in tests and README-linked manual checks, and keep only the current root video.
- [accepted] uv-only-build: Phase 15 removes the Makefile, commits `uv.lock`, uses a standard `dev` dependency group, and documents only `uv sync` and `uv run ...` commands.
- [settled] json-dag-implemented: `POST /workflow_runs` accepts a fully
  materialized, validated JSON DAG; `WorkflowRunCreated` v3 durably snapshots the
  concrete graph, and dependency projections drive deterministic readiness.
- [settled] public-audit-removed: The audit model and endpoint are removed while `projection_matches_history`, reducer tests, and projection rebuild retain the underlying correctness check internally.
- [settled] repository-surface-cleaned: The tracked execution surface has one
  canonical `scripts/demo.py`, one self-contained `scripts/graceful_drain.py`, two
  JSON definitions, and `uv.lock`; the shell scripts and helper were removed.
- [settled] self-contained-drain-demo: The graceful-drain check starts isolated
  API and tool services, signals an in-flight worker, prints the drained timeline,
  starts a replacement, prints the completed timeline, and asserts exactly one
  charge from one `uv run` command.
- [settled] drain-startup-robustness: The self-contained graceful-drain script
  allocates free loopback ports instead of assuming 8020/8021 are available and
  reports captured service startup output when a child exits early.
- [settled] consolidated-demo-verified: While worker 1 is blocked after the paid
  charge commits, worker 2 receives a targeted HTTP 429 for the trial run and
  persists its two-second Retry-After. The demo then SIGKILLs worker 1, prints
  both partial timelines, starts one replacement, prints both completed
  timelines, and verifies one charge plus ledger counts `1/2/3`.
- [settled] run-terminology-migration: Database schema v4 uses `workflow_runs`,
  `workflow_run_events`, `workflow_run_leases`, and `run_id`; startup losslessly
  migrates nonempty v3 databases and maps the three workflow-level event names.
- [settled] run-api: The public API is `POST/GET /workflow_runs` plus
  `GET /workflow_runs/{run_id}`, `/events`, and `/timeline`; response models expose
  `run_id` consistently.
- [settled] targeted-demo-failure: `CRASHSAFE_FAIL_FIRST_OPERATION` deterministically
  injects one pre-side-effect 429 for a selected operation so the core demo can
  order concurrent crash and retry evidence without timing luck.
- [settled] current-verification: All 40 tests, Ruff, strict mypy, shell syntax,
  and the combined canonical demo pass.
- [settled] phase-15-verification: The expanded 38-test suite passed three consecutive uv runs; Ruff, strict mypy, and the canonical uv demo also passed.
- [superseded] refreshed-submission-video: The existing root recording predates
  the combined 429/crash timeline story and must be rerecorded before submission.

## Commitments

- [accepted] prioritize-correctness: Keep the expansion bounded to local SQLite coordination: leased workers, a history-derived timeline, and a persisted provider-directed cooldown; continue to exclude fan-out, general replay, distributed shards, and UI infrastructure.
- [accepted] stable-operation-keys: Persist intent, request payload, and a stable idempotency key before every external tool request; reuse the key across retries and restarts.
- [accepted] separate-durability-domains: Keep engine state and mock-tool idempotency state in separate SQLite databases so no shared transaction hides the network ambiguity.
- [accepted] memory-is-documentation: Keep project memory as Git-tracked files under `.memory/`; do not add a memory API, database, or CRUD subsystem to the take-home.
- [accepted] graceful-drain-contract: On SIGTERM or SIGINT, admit no new step, allow the current attempt and its state transaction to finish, then exit; forced termination continues to use crash-safe recovery.
- [accepted] expanded-submission-readiness: Phases 10–13 passed the repeated verification gate and the conventional implementation, observability, and documentation commits were pushed to `origin/main` over SSH.
- [accepted] plan-projection-scope: Treat the graceful-drain plan as a supporting sub-plan; use the Crashsafe engine plan as the full implementation projection.
- [accepted] final-submission-phase: After event history and its failure tests pass, finish with a succinct README, followable deterministic demo, and recorded terminal video.
- [accepted] event-history-authority: Treat immutable workflow events as the logical authority and mutable workflow/step rows as the scheduling projection; verify their equality with a pure reducer.
- [accepted] event-projection-atomicity: Append transition events and update their projections in one SQLite transaction so a crash cannot expose either half.
- [superseded] concurrency-remains-cut: This constrained Phase 8 and is superseded for the extension by the bounded leased worker-pool plan; shards and branches remain cut.
- [accepted] fenced-worker-pool: Atomically claim one workflow with a renewable lease, increment its fence token on reacquisition, and require that token for all worker-originated commits.
- [accepted] honest-concurrency-boundary: Promise one valid committing owner, not exactly-once compute; an expired worker may overlap a replacement but is fenced from state and external duplicates use the stable key.
- [accepted] tool-wide-cooldown: On 429, atomically persist both the step retry decision and the maximum tool-level blocked-until deadline consulted by all workers.
- [accepted] timeline-is-derived: Build observability from immutable events rather than a second mutable truth, and show unmatched attempts honestly as unknown outcomes.
- [accepted] record-submission-video: Keep the plain-Terminal `crashsafe-demo.mov` at the repository root and link it from the README.
- [superseded] temporal-remains-experiment: The earlier isolated Temporal comparison is superseded by Phase 15 repository cleanup; remove `experiments/temporal_compare/` and retain only concise prose comparison where useful.
- [accepted] immutable-submitted-plan: Validate and persist the complete submitted
  definition, dependency edges, concrete requests, and server-generated stable
  per-step keys atomically in `WorkflowRunCreated`; recovery requires no external
  definition source.
- [accepted] deterministic-dag-scheduling: A step is ready only when all declared dependencies are completed; ties use submitted array order, and a workflow-level lease keeps ready branches sequential within one run.
- [accepted] one-reviewable-demo: The canonical demo combines two-run concurrency,
  targeted 429 handling, persisted Retry-After, live SIGKILL takeover, partial
  timelines, final timelines, and exactly one charge. Graceful drain alone stays
  separate because SIGTERM and SIGKILL are mutually exclusive scenarios.
