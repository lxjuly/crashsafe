# Graceful Drain — Retrospective Implementation Plan

Status: completed  
Implemented: 2026-09-06  
Source primitives: `graceful-drain-contract`, `graceful-drain-verified`

## Objective

Add the assignment's graceful-drain feature without weakening crash recovery:
after a termination request, admit no new step, durably settle the current
attempt, exit cleanly, and let the next worker resume from that boundary.

## Planned sequence and actual outcome

1. **Define the shutdown boundary — completed.** Chose finish-current-attempt
   semantics rather than request cancellation. A later `SIGKILL` remains safe
   under the existing idempotent recovery model.
2. **Add an admission gate — completed.** Replaced the worker's boolean flag
   with a thread-safe event checked before selecting every step.
3. **Make idle shutdown prompt — completed.** Replaced polling `sleep` calls
   with interruptible event waits, so an idle worker does not wait out the poll
   interval after receiving a signal.
4. **Drain in-flight work — completed.** `SIGTERM` and `SIGINT` set the event;
   the synchronous tool request and its completion-or-retry transaction finish,
   and the loop exits before selecting another step.
5. **Verify the real boundary — completed.** Added a process test that delays a
   committed charge response, sends `SIGTERM`, checks clean exit and durable
   charge completion, restarts the worker, and asserts one charge, provision,
   and notification.
6. **Document operational limits — completed.** The README now states that this
   is drain, not cancellation, and that orchestrator termination grace must be
   at least the tool request timeout.

## Evidence

- `tests/test_process_recovery.py::test_sigterm_drains_in_flight_step_then_restart_resumes`
- Full suite: 10 tests passed.
- Ruff and strict mypy passed.

## Deliberate limits

- A hung request can delay graceful exit until the configured HTTP timeout.
- There is no per-workflow admission control because the worker is single-threaded.
- A forced kill during drain is handled as crash recovery, not graceful shutdown.

