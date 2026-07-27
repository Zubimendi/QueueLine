# Architecture: principles → code

Same format as Dispatcher and SlotForge: each section is one principle,
what it means for a job queue specifically, and exactly where it's
implemented.

## System shape

```
                 ┌──────────────┐        ┌──────────────┐
  producers  ───▶│  cmd/api      │◀──────▶│  PostgreSQL   │
 (any language)  │  (REST)       │        │  jobs table   │
                 └───────┬───────┘        │  (source of   │
                         │                 │   truth)      │
                         │ same REST API   └───────┬───────┘
                         ▼                          │
                 ┌──────────────┐                    │
  workers    ───▶│  pkg/client   │                    │
 (Go, via SDK)   │  or raw HTTP  │                    │
                 └──────────────┘                    │
                                                       │
                 ┌──────────────┐                    │
                 │  cmd/reaper   │────────────────────┘
                 │ (independent  │  sweeps expired leases
                 │  process)     │
                 └──────────────┘
```

---

## 1. Concurrency control — the centerpiece

This is the project's core lesson, so it gets the most detail.

**`FOR UPDATE SKIP LOCKED` for claiming.** `Engine.Claim`
(`internal/queue/engine.go`) selects the highest-priority, oldest, due
`PENDING` job with `FOR UPDATE SKIP LOCKED`. Any number of concurrent
callers — many worker processes, in any language, all hitting the same
`/v1/queues/{queue}/lease` endpoint simultaneously — can never claim the
same row: Postgres's row-level locking guarantees it, atomically, without
QueueLine's application code needing to implement any locking logic of
its own. `test/integration/concurrency_test.go`'s
`TestConcurrentClaims_NeverDoubleClaim` proves this with 10 concurrent
workers draining a 50-job queue and asserting every job was claimed
exactly once.

**Fencing tokens for lease safety.** This is the subtler, more important
half of the story. `SKIP LOCKED` guarantees two workers can't claim the
*same row at the same instant* — but it says nothing about what happens
when a worker claims a job, stalls (a GC pause, a network partition, a
container being throttled), has its lease expire and reclaimed by the
reaper, and then *"wakes up"* and tries to report success on a job it no
longer owns. Without a defense against this, that stale completion could
silently mark a job "done" when a second worker is mid-way through
actually doing it again — a genuine, classic distributed-systems bug
(see Martin Kleppmann's treatment of fencing tokens in *Designing
Data-Intensive Applications*, which is exactly what this schema
implements).

The defense: every successful `Claim` mints a **brand new** `lease_id`
(migration `0001_init.sql`). `Heartbeat`, `Complete`, and `Fail` all
require the caller to present the `lease_id` they were given, and the
`UPDATE ... WHERE id = $1 AND lease_id = $2` clause means the write
only succeeds if that lease is still the job's *current* one. A stale
worker's request affects zero rows, and `Engine` translates that into
`ErrLeaseMismatch` rather than silently succeeding.
`test/integration/concurrency_test.go`'s
`TestStaleLeaseCannotCompleteAfterReclaim` proves this end-to-end: claim
a job, force its lease to expire, let the reaper reclaim it, have a
second worker claim it fresh, then prove the *original* worker's stale
completion attempt is rejected while the second worker's legitimate one
succeeds.

## 2. Idempotency

**Where:** `Engine.Enqueue` relies on a partial unique index
(`idx_jobs_queue_dedup`, migration `0001_init.sql`) on
`(queue, dedup_key) WHERE dedup_key IS NOT NULL` — a real database
constraint, not an application-level check-then-insert (which has a race
window between two concurrent enqueue calls with the same key). On a
`23505` unique violation, `Enqueue` fetches and returns the *original*
job with `ErrDuplicate` rather than erroring — a producer that retries a
timed-out enqueue call gets the same job both times, never a duplicate.

## 3. Explicit state machines

**Where:** `internal/queue/state_machine.go`'s `CanTransition` is the one
sanctioned way to reason about whether a `Status` transition is legal.
Every write to `jobs.status` across `Claim`, `Complete`, `Fail`, and
`Reap` (which calls `Fail` internally, reusing the same transition
logic) moves along a legal edge in that table.
`state_machine_test.go` pins down both legal paths and specific illegal
ones — a `COMPLETED` job cannot go back to `LEASED`, a `PENDING` job
cannot skip straight to `COMPLETED` without ever being claimed.

## 4. Resilience against worker failure — the explicit failure path

**Where:** `cmd/reaper` is a deliberately separate, independently
deployable process from `cmd/api`. Its only job is `Engine.Reap`: find
`LEASED` jobs whose `lease_expires_at` has passed, and treat each one
exactly like an explicit `Fail` call (same backoff scheduling, same
dead-lettering-when-exhausted logic — `Reap` literally calls `Fail`
internally, so there's exactly one place that logic lives). Running this
as a separate process, not a goroutine inside `cmd/api`, means a bug or
outage in the reaper can never affect the latency of a healthy worker's
claim/complete calls, and an API server restart never accidentally
pauses lease reclamation.

## 5. Retries with exponential backoff and jitter

**Where:** `internal/queue/backoff.go`'s `BackoffWithJitter`, used by
`Engine.Fail` when rescheduling a job (`2s * 2^(attempt-1)`, capped at
10 minutes, with randomized jitter). This is the same shape used in
Dispatcher, for the same reason: many jobs failing around the same
moment (a downstream dependency blipping) must not all retry in
lockstep the instant their window opens. `backoff_test.go` asserts both
the cap and the jitter (repeated calls at the same attempt number
produce varying durations).

## 6. Dead-letter handling as a first-class citizen

**Where:** `dead_letter_jobs` is a separate table (migration
`0001_init.sql`), not just a status flag on `jobs` — so operational
queries against the hot `jobs` table never scan long-dead rows, and a
dead-lettered job's full failure history is preserved independent of
whatever retention policy might later apply to `jobs` itself.
`Engine.RequeueDeadLetter` creates a **new** job rather than resurrecting
the old row in place, specifically so the original dead-letter record
stays an untouched historical artifact for post-incident review.

## 7. Server-side validation

**Where:** every handler in `internal/api/handlers.go` validates its
input before it ever reaches `Engine` — an empty payload, a negative
delay, a missing `leaseId` on heartbeat/complete/fail all get a clean
`400` with a machine-readable `code` field, never a confusing downstream
database error.

## 8. Observability

**Where:** `internal/observability/observability.go` — Prometheus
counters for enqueues, claims, and reaps, all labeled by queue name, plus
a queue-depth gauge updated on every `/stats` call. `/healthz` (process
up) vs. `/readyz` (process up AND can reach Postgres) are separate
endpoints in `internal/api/server.go`, the same liveness/readiness split
used in Dispatcher and SlotForge — deliberately, so this project follows
the same operational conventions as the rest of the portfolio, not a new
one invented from scratch each time.

## 9. Resilience as a design property

**Where:** `cmd/api/main.go` handles `SIGTERM` with a graceful HTTP
server shutdown (drains in-flight requests). `pkg/client.RunWorker`
respects context cancellation in its polling loop, and
`cmd/worker-example` wires that to `signal.NotifyContext` — a worker
process stopping cleanly on `SIGTERM` rather than abandoning a job
mid-flight (and relying on the reaper to clean up after it) is the
correct default, with the reaper as the backstop for the cases where a
clean shutdown genuinely isn't possible (a hard crash, an OOM kill).

## What's deliberately simplified for a v1 reference implementation

- **A single Postgres table as the queue backend**, not a dedicated
  message broker. Correct and simple at this portfolio's scale; a
  genuinely high-throughput use case (tens of thousands of jobs/sec)
  would eventually want a log-based broker instead — see `docs/PRD.md`
  §7 for the explicit tradeoff.
- **Polling, not push, for job pickup.** `RunWorker`'s loop checks every
  `pollInterval` (500ms default) rather than being notified the instant
  a job becomes claimable — simple and correct, at the cost of a small,
  bounded latency. `LISTEN`/`NOTIFY` on the `jobs` table is a documented,
  contained future improvement (see `docs/CURSOR_CONTEXT.md`).
- **No auth on the REST API.** v1 assumes QueueLine runs on a trusted
  internal network, not exposed to untrusted clients — a real
  multi-tenant deployment would need API keys and per-tenant rate
  limiting before that assumption could be relaxed.
- **No Python/TypeScript client SDK yet** — the REST API is a complete
  contract for those languages today, just without the convenience
  wrapper Go gets from `pkg/client`.
