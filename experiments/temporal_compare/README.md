# One-worker Temporal comparison

This experiment runs Crashsafe's `charge → provision → notify` workflow through
a persistent local Temporal server and one Temporal worker. It reuses
Crashsafe's mock tool and `tools.db`, including the atomic idempotency ledger.

```bash
make demo-temporal
```

Prerequisites are `uv` and the Temporal CLI. The command uses Python 3.12 and
Temporal Python SDK 1.18.2 in an isolated environment; neither is added to the
Crashsafe runtime.

## Equivalent failure

1. Temporal schedules the charge Activity with a stable application key.
2. The mock tool commits the charge and delays its HTTP response.
3. The demo sends the only Temporal worker `SIGKILL`.
4. The Activity reaches its Start-To-Close timeout.
5. A replacement worker receives Activity attempt 2 with the same key.
6. The tool returns its cached result and Temporal completes the workflow.

The final ledger must contain one charge, one provision, and one notification.

## Observed comparison

| Concern | Crashsafe | Temporal experiment |
|---|---|---|
| Durable owner | `engine.db` | Temporal server SQLite |
| Worker count | One | One, then one replacement |
| Unknown charge outcome | Unmatched `StepAttemptStarted` | Activity retry after Start-To-Close timeout |
| Retry evidence | Two explicit attempt-start events | Completed retry recorded as Activity attempt 2 with the prior timeout |
| Stable key | Generated while seeding steps | Derived from workflow ID, position, and operation |
| One charge effect | Mock-tool idempotency | The same mock-tool idempotency |

Temporal owns workflow replay, task delivery, durable timers, and retry
scheduling. Crashsafe implements only the small state machine necessary for the
take-home. Neither engine alone can make an arbitrary external side effect
exactly once.

References: [Temporal Python Activity retry sample](https://github.com/temporalio/samples-python/blob/main/hello/hello_activity_retry.py),
[Temporal Activity idempotency guidance](https://docs.temporal.io/activity-definition#idempotency).
