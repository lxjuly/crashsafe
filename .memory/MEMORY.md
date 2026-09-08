# Crashsafe Memory

Updated: 2026-09-08T06:03:08Z

## Claims

- [settled] assignment-purpose: Crashsafe is a senior software engineer take-home for a small durable execution engine.
- [settled] required-stack: The assignment requires Python, FastAPI, Pydantic, type annotations, separated engine/storage/API layers, and restart-safe persistence.
- [settled] workflow-shape: The seeded ordered workflow is charge → provision → notify.
- [settled] correctness-boundary: The engine guarantees durable recovery and at-least-once tool requests; exactly-once side effects require durable idempotency at the tool boundary.
- [settled] implemented-scope: The repository implements a configurable local SQLite-backed worker pool, renewable workflow leases and fencing, append-only typed event history, rebuildable scheduling projections, persisted per-step and tool-wide retry scheduling, graceful drain, timeline observability, deterministic failures, and real process recovery tests.
- [settled] ambiguous-charge-proof: The demo kills the worker after the mock tool commits a charge but before the worker persists completion, then recovers with one charge in the durable ledger.
- [settled] graceful-drain-verified: A real-process test sends SIGTERM during a delayed committed charge response; the worker commits only the in-flight step, exits cleanly, and a restart finishes with one of each side effect.
- [settled] full-plan-projected: The full baseline and extension plan, including completed event-history outcomes and planned Phases 10–13, is stored at `.memory/projections/plans/crashsafe-engine.md`.
- [settled] durability-scope: Shards and history trees remain deferred; a bounded local lease and fence token are now planned specifically to add safe multi-worker ownership without changing the SQLite durability boundary.
- [settled] event-history-implemented: Phase 8 added an append-only, per-workflow event stream that commits atomically with rebuildable `workflows` and `steps` projections.
- [settled] retry-policy-boundary: Persisting `next_attempt_at` is durable per-step state; the expansion also plans a persisted monotonic tool-wide `blocked_until` derived from `Retry-After`.
- [settled] final-verification: Twenty-eight tests, Ruff, strict mypy, the ambiguous-charge demo, and the two-worker feature demo pass.
- [settled] readme-completed: The final README follows the agreed problem, requirements, architecture, data/invariants, state-machine, and demo structure.
- [settled] temporal-comparison-verified: An isolated persistent Temporal server with one worker reproduces the ambiguous charge outcome; Activity attempt 2 reuses the application key, returns the cached tool result, and completes with exactly one charge.
- [settled] temporal-history-difference: In the observed run, Temporal represented the lost first execution through Activity attempt 2 with the prior Start-To-Close timeout, while Crashsafe retains two explicit `StepAttemptStarted` events.
- [settled] submission-video-recorded: The repository root contains `crashsafe-demo.mov`, a 13-second plain-Terminal recording showing a real worker `SIGKILL`, two charge attempts with one key, and one durable charge; macOS Quick Look successfully rendered its terminal content during final artifact verification.
- [settled] feature-test-manual: `TESTING.md` provides concise, reproducible checks for crash recovery, safe retries, and graceful drain.
- [settled] handoff-ready-memory: Project memory now lives under `.memory/`; the current projection and dated journal identify the verified state and outstanding Git handoff action.
- [settled] evaluator-focused-readme: The README now follows the requested review structure: built/cut scope, key decisions and uncertainties, five precise failure modes, and concrete AI usage mistakes and verification.
- [settled] extension-scope-complete: Concurrent workers, observability, and adaptive backoff are implemented and verified without changing the core durability boundary.
- [settled] concurrency-model-implemented: `CRASHSAFE_WORKERS` defaults to one and supports two for demos; workflow-level renewable leases plus monotonic fence tokens permit one valid committing owner while preserving safe at-least-once recovery after expiry.
- [settled] observability-model-implemented: `GET /workflows/{id}/timeline` and the demo formatter derive sequence, relative timing, attempts, worker/fence ownership, waits, outcomes, and audit summary from ordered history.
- [settled] adaptive-backoff-model-implemented: A durable per-tool cooldown propagates the maximum `Retry-After` deadline across workflows, workers, and restarts.
- [settled] deterministic-feature-demo: `make demo-features` starts two workers and two workflows, forces exactly one 429, shows the shared 500 ms wait and both worker IDs, drains cleanly, and finishes with two of each side effect.
- [settled] expanded-final-verification: The 28-test suite passed three consecutive runs; Ruff, strict mypy, `make demo`, and `make demo-features` then passed again.

## Commitments

- [accepted] prioritize-correctness: Keep the expansion bounded to local SQLite coordination: leased workers, a history-derived timeline, and a persisted provider-directed cooldown; continue to exclude fan-out, general replay, distributed shards, and UI infrastructure.
- [accepted] stable-operation-keys: Persist intent, request payload, and a stable idempotency key before every external tool request; reuse the key across retries and restarts.
- [accepted] separate-durability-domains: Keep engine state and mock-tool idempotency state in separate SQLite databases so no shared transaction hides the network ambiguity.
- [accepted] memory-is-documentation: Keep project memory as Git-tracked files under `.memory/`; do not add a memory API, database, or CRUD subsystem to the take-home.
- [accepted] graceful-drain-contract: On SIGTERM or SIGINT, admit no new step, allow the current attempt and its state transaction to finish, then exit; forced termination continues to use crash-safe recovery.
- [accepted] expanded-submission-readiness: Phases 10–13 are implemented; final Git/video handoff follows the repeated verification gate.
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
- [accepted] temporal-remains-experiment: Keep Temporal and its Python 3.12 SDK outside Crashsafe's runtime dependencies under `experiments/temporal_compare/`.
