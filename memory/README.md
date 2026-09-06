# Project memory

This directory follows the lightweight current interface described by
[Chronelle](https://github.com/chronelle/chronelle):

- `MEMORY.md` is the compact current-state projection an agent reads first.
- `journal/YYYY-MM-DD.md` is the detailed dated paper trail.
- `projections/` contains derived views such as implementation plans; these are
  backed by Claims and Commitments in current memory or the journal.

Memory uses two primitives:

- **Claim** — truth-apt state: facts, assumptions, questions, and observations.
- **Commitment** — will-apt state: decisions, goals, constraints, and next actions.

This is deliberately not a service, database, or CRUD system. Git is the ledger.
Update `MEMORY.md` when current state changes and append the supporting context
to the day's journal.
