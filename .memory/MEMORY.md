# Crashsafe Memory

Updated: 2026-09-08T00:38:26Z

## Claims

- [settled] assignment-purpose: Crashsafe is a senior software engineer take-home for a small durable execution engine.
- [settled] required-stack: The assignment requires Python, FastAPI, Pydantic, type annotations, separated engine/storage/API layers, and restart-safe persistence.
- [settled] workflow-shape: The seeded ordered workflow is charge → provision → notify.
- [settled] correctness-boundary: The engine guarantees durable recovery and at-least-once tool requests; exactly-once side effects require durable idempotency at the tool boundary.
- [settled] implemented-scope: The repository implements one SQLite-backed worker, append-only typed event history, rebuildable scheduling projections, persisted retry scheduling, graceful drain, random 429 failures, deterministic crash injection, and real process recovery tests.
- [settled] ambiguous-charge-proof: The demo kills the worker after the mock tool commits a charge but before the worker persists completion, then recovers with one charge in the durable ledger.
- [settled] graceful-drain-verified: A real-process test sends SIGTERM during a delayed committed charge response; the worker commits only the in-flight step, exits cleanly, and a restart finishes with one of each side effect.
- [settled] full-plan-projected: The completed implementation plan, including event-history outcomes and the final README/demo phase, is stored at `.memory/projections/plans/crashsafe-engine.md`.
- [settled] durability-scope: Shards, history trees, leases, and fencing address distribution and concurrent ownership; they are outside the single-worker crash-durability proof.
- [settled] event-history-implemented: Phase 8 added an append-only, per-workflow event stream that commits atomically with rebuildable `workflows` and `steps` projections.
- [settled] retry-policy-boundary: Persisting the selected `next_attempt_at` is durable state; deriving it from `Retry-After`, exponential backoff, jitter, or other configuration is policy.
- [settled] final-verification: Eighteen tests, Ruff, strict mypy, and the recording-oriented ambiguous-charge demo pass.
- [settled] readme-completed: The final README follows the agreed problem, requirements, architecture, data/invariants, state-machine, and demo structure.
- [settled] temporal-comparison-verified: An isolated persistent Temporal server with one worker reproduces the ambiguous charge outcome; Activity attempt 2 reuses the application key, returns the cached tool result, and completes with exactly one charge.
- [settled] temporal-history-difference: In the observed run, Temporal represented the lost first execution through Activity attempt 2 with the prior Start-To-Close timeout, while Crashsafe retains two explicit `StepAttemptStarted` events.
- [settled] submission-video-recorded: The repository root contains `crashsafe-demo.mov`, a verified 13-second plain-Terminal recording showing a real worker `SIGKILL`, two charge attempts with one key, and one durable charge.
- [settled] feature-test-manual: `TESTING.md` provides concise, reproducible checks for crash recovery, safe retries, and graceful drain.
- [settled] handoff-ready-memory: Project memory now lives under `.memory/`; the current projection and dated journal identify the verified state and outstanding Git handoff action.

## Commitments

- [accepted] prioritize-correctness: Prefer a small, defensible failure model over worker pools, leases, fan-out, general replay, or UI surface area.
- [accepted] stable-operation-keys: Persist intent, request payload, and a stable idempotency key before every external tool request; reuse the key across retries and restarts.
- [accepted] separate-durability-domains: Keep engine state and mock-tool idempotency state in separate SQLite databases so no shared transaction hides the network ambiguity.
- [accepted] memory-is-documentation: Keep project memory as Git-tracked files under `.memory/`; do not add a memory API, database, or CRUD subsystem to the take-home.
- [accepted] graceful-drain-contract: On SIGTERM or SIGINT, admit no new step, allow the current attempt and its state transaction to finish, then exit; forced termination continues to use crash-safe recovery.
- [accepted] submission-readiness: The README and one-command crash demonstration are complete and verified.
- [accepted] plan-projection-scope: Treat the graceful-drain plan as a supporting sub-plan; use the Crashsafe engine plan as the full implementation projection.
- [accepted] final-submission-phase: After event history and its failure tests pass, finish with a succinct README, followable deterministic demo, and recorded terminal video.
- [accepted] event-history-authority: Treat immutable workflow events as the logical authority and mutable workflow/step rows as the scheduling projection; verify their equality with a pure reducer.
- [accepted] event-projection-atomicity: Append transition events and update their projections in one SQLite transaction so a crash cannot expose either half.
- [accepted] concurrency-remains-cut: Do not introduce shards, branches, claims, leases, worker pools, or fencing as part of the event-history implementation.
- [accepted] record-submission-video: Keep the plain-Terminal `crashsafe-demo.mov` at the repository root and link it from the README.
- [accepted] temporal-remains-experiment: Keep Temporal and its Python 3.12 SDK outside Crashsafe's runtime dependencies under `experiments/temporal_compare/`.
