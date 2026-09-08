# Crashsafe Memory

Updated: 2026-09-08T22:05:00Z

## Claims

- [settled] assignment-purpose: Crashsafe is a senior software engineer take-home for a small durable execution engine.
- [settled] required-stack: The assignment requires Python, FastAPI, Pydantic, type annotations, separated engine/storage/API layers, and restart-safe persistence.
- [settled] workflow-shape: The seeded ordered workflow is charge → provision → notify.
- [settled] correctness-boundary: The engine guarantees durable recovery and at-least-once tool requests; exactly-once side effects require durable idempotency at the tool boundary.
- [settled] implemented-scope: The repository implements a configurable local SQLite-backed worker pool, renewable workflow leases and fencing, append-only typed event history, rebuildable scheduling projections, persisted per-step and tool-wide retry scheduling, graceful drain, timeline observability, deterministic failures, and real process recovery tests.
- [settled] ambiguous-charge-proof: The demo kills the worker after the mock tool commits a charge but before the worker persists completion, then recovers with one charge in the durable ledger.
- [settled] graceful-drain-verified: A real-process test sends SIGTERM during a delayed committed charge response; the worker commits only the in-flight step, exits cleanly, and a restart finishes with one of each side effect.
- [settled] full-plan-projected: The full baseline and extension plan, including completed Phases 0–13 and accepted pending JSON-workflow Phases 14–15, is stored at `.memory/projections/plans/crashsafe-engine.md`.
- [settled] durability-scope: Shards and history trees remain deferred; a bounded local lease and fence token are now planned specifically to add safe multi-worker ownership without changing the SQLite durability boundary.
- [settled] event-history-implemented: Phase 8 added an append-only, per-workflow event stream that commits atomically with rebuildable `workflows` and `steps` projections.
- [settled] retry-policy-boundary: Persisting `next_attempt_at` is durable per-step state; the expansion also plans a persisted monotonic tool-wide `blocked_until` derived from `Retry-After`.
- [settled] final-verification: Twenty-eight tests, Ruff, strict mypy, the ambiguous-charge demo, and the two-worker feature demo pass.
- [settled] readme-completed: The final README follows the agreed problem, requirements, architecture, data/invariants, state-machine, and demo structure.
- [settled] temporal-comparison-verified: An isolated persistent Temporal server with one worker reproduces the ambiguous charge outcome; Activity attempt 2 reuses the application key, returns the cached tool result, and completes with exactly one charge.
- [settled] temporal-history-difference: In the observed run, Temporal represented the lost first execution through Activity attempt 2 with the prior Start-To-Close timeout, while Crashsafe retains two explicit `StepAttemptStarted` events.
- [superseded] original-submission-video: The earlier 7.9-second fixed-workflow recording was replaced after the JSON DAG and consolidated-demo revision.
- [settled] feature-test-manual: `TESTING.md` provides concise, reproducible checks for crash recovery, safe retries, and graceful drain.
- [settled] handoff-ready-memory: Project memory lives under `.memory/`; the current projection and dated journal identify the verified implementation and completed Git handoff.
- [settled] evaluator-focused-readme: The README now follows the requested review structure: built/cut scope, key decisions and uncertainties, five precise failure modes, and concrete AI usage mistakes and verification.
- [settled] extension-scope-complete: Concurrent workers, observability, and adaptive backoff are implemented and verified without changing the core durability boundary.
- [settled] concurrency-model-implemented: `CRASHSAFE_WORKERS` defaults to one and supports two for demos; workflow-level renewable leases plus monotonic fence tokens permit one valid committing owner while preserving safe at-least-once recovery after expiry.
- [settled] observability-model-implemented: `GET /workflows/{id}/timeline` and the demo formatter derive sequence, relative timing, attempts, worker/fence ownership, waits, outcomes, and audit summary from ordered history.
- [settled] adaptive-backoff-model-implemented: A durable per-tool cooldown propagates the maximum `Retry-After` deadline across workflows, workers, and restarts.
- [settled] deterministic-feature-demo: `make demo-features` starts two workers and two workflows, forces exactly one 429, shows the shared 500 ms wait and both worker IDs, drains cleanly, and finishes with two of each side effect.
- [settled] expanded-final-verification: The 28-test suite passed three consecutive runs; Ruff, strict mypy, `make demo`, and `make demo-features` then passed again.
- [accepted] json-dag-plan: Phases 14–15 replace hardcoded seeding with a complete materialized JSON DAG submitted directly to `POST /workflows`, validated by Pydantic and graph checks, persisted in `WorkflowCreated`, and demonstrated with two different payloads.
- [settled] dag-scope-boundary: The accepted DAG contains allowlisted operations, concrete request bodies, explicit dependencies, and deterministic array order; it excludes registries, templates, separate inputs, output references, expressions, Python workflow code, definition CRUD, conditions, loops, dynamic fan-out, and intra-workflow parallel execution.
- [accepted] audit-feature-cut: Remove the public audit model, endpoint, timeline flag, demo output, and feature language while preserving the reducer, atomic event/projection commits, internal reconstruction, and equality assertions in failure tests.
- [accepted] phase-15-repository-cleanup: Remove `experiments/`, consolidate the two current demo scripts into one `scripts/demo.py` invoked through uv, retain broader feature evidence in tests and `TESTING.md`, and keep only the current root video.
- [accepted] uv-only-build: Phase 15 removes the Makefile, commits `uv.lock`, uses a standard `dev` dependency group, and documents only `uv sync` and `uv run ...` commands.
- [settled] json-dag-implemented: `POST /workflows` now accepts a fully materialized, validated JSON DAG; `WorkflowCreated` v3 durably snapshots the concrete graph, and dependency projections drive deterministic readiness.
- [settled] public-audit-removed: The audit model and endpoint are removed while `projection_matches_history`, reducer tests, and projection rebuild retain the underlying correctness check internally.
- [settled] repository-surface-cleaned: The Temporal experiment, Makefile, two old demo scripts, and alternate demo targets are removed; the tracked surface has one `scripts/demo.py`, two example JSON definitions, and `uv.lock`.
- [settled] consolidated-demo-verified: The uv-run demo submits two workflows, starts two workers, kills the paid owner after charge commit, resumes at fence 2 with the same key, and finishes with one charge, two provisions, and three notifications.
- [settled] phase-15-verification: The expanded 38-test suite passed three consecutive uv runs; Ruff, strict mypy, and the canonical uv demo also passed.
- [settled] refreshed-submission-video: The root `crashsafe-demo.mov` is a 1920×1080 H.264 plain-Terminal recording of the uv demo. Frames at 1, 6, 12, 18, and 23 seconds were inspected and show the live kill, unknown engine outcome, fenced recovery, exact ledger, timelines, and PASS line.

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
- [accepted] immutable-submitted-plan: Validate and persist the complete submitted workflow, dependency edges, concrete requests, and server-generated stable per-step keys atomically in `WorkflowCreated`; recovery requires no external definition source.
- [accepted] deterministic-dag-scheduling: A step is ready only when all declared dependencies are completed; ties use submitted array order, and a workflow-level lease keeps ready branches sequential within one run.
- [accepted] one-reviewable-demo: The final repository exposes one canonical demo script and one demo command that submit two JSON workflows, exercise concurrent workers and live crash takeover, and finish with exactly one charge; adaptive backoff and graceful drain remain independently testable rather than adding alternate demo harnesses.
