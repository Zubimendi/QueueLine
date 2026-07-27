# QueueLine — Product Requirements Document

## 1. Problem

Almost every backend service eventually needs to do work outside the
request/response cycle: send an email, process an uploaded file, call a
slow third-party API, run a report. The naive version — do it inline,
in the handler, before responding — makes the user wait on work they
didn't ask to wait on, and a failure in that work (a downstream timeout,
a transient error) becomes a failure of the entire request, even though
the actual user-facing action (e.g. "create my account") succeeded.

The next naive version — "just fire a goroutine/promise and don't await
it" — is worse: if the process crashes or restarts before that
work finishes, it's silently lost, with no record it was ever supposed to
happen and no retry.

Most teams eventually build some version of a job queue to solve this.
Most build it under deadline pressure, once, with the specific gaps that
matter revealed only in production: no dead-letter handling (a
permanently-broken job retries forever, or is silently dropped after one
failure), no protection against a crashed worker leaving a job stuck
"in progress" forever, and no idempotent enqueue (a retried API call
creates a duplicate job).

## 2. Goal

Build a **correct, reusable, language-agnostic job queue** — correct
specifically meaning: no job is ever lost, no job is ever processed by
two workers as if both owned it simultaneously, and a worker crash is an
explicitly handled case, not an assumption that never happens. Reusable
meaning: expose it as a REST API from day one, not a Go-only library, so
every future project in this portfolio (regardless of language) can
depend on it instead of rebuilding this exact piece of infrastructure
each time.

## 3. Users

- **Producers**: any service (in any language) that has work it needs
  done asynchronously — enqueues a job via `POST /v1/queues/{queue}/jobs`.
- **Workers**: processes (in any language) that claim jobs, do the work,
  and report success or failure — via `POST /v1/queues/{queue}/lease`
  and the completion/failure endpoints, or via `pkg/client` for Go
  workers specifically.
- **Operators**: whoever runs QueueLine — needs to see queue depth,
  failure rates, and dead-lettered jobs at a glance, and needs a way to
  inspect and retry dead-lettered jobs without touching the database
  directly.

## 4. Scope

### In scope (v1)
- Enqueue with priority, delay (`run_after`), configurable max attempts,
  and idempotent submission via an optional dedup key.
- Claim via `FOR UPDATE SKIP LOCKED`, safe across any number of
  concurrent worker processes in any language.
- Fencing-token leases (a fresh `lease_id` per claim) so a worker that
  stalls past its lease expiry can never corrupt a job that's since been
  reclaimed and re-leased to someone else.
- Heartbeating for long-running jobs, so a legitimately slow handler
  doesn't lose its lease mid-work.
- Exponential backoff with jitter on failure, up to `max_attempts`, then
  automatic dead-lettering.
- A standalone reaper process that reclaims jobs whose lease expired
  without any Complete/Fail/Heartbeat call — the explicit answer to "the
  worker process died."
- Dead-letter inspection and manual requeue via the REST API.
- A Go client SDK (`pkg/client`) with a `RunWorker` helper that handles
  polling, automatic heartbeating, and graceful shutdown, so a future Go
  project's worker code is a five-line handler function, not
  boilerplate.
- Prometheus metrics, structured logging, liveness/readiness health
  checks.

### Explicitly out of scope (v1)
- Client SDKs for Python or TypeScript — the REST API is the contract
  for non-Go consumers in v1; a thin SDK in each language is a natural,
  contained future addition once a Python or NestJS project in the
  roadmap actually needs one (see `docs/CURSOR_CONTEXT.md`).
- Job scheduling beyond simple delay (`run_after`) — no cron-expression-
  based recurring jobs. (A distributed cron service is its own, separate
  roadmap project — week 9's CronMesh — deliberately not duplicated
  here.)
- Multi-tenancy / per-tenant auth on the queue API — v1 assumes trusted,
  internal callers (other services in the same deployment), not
  untrusted public clients.
- Message ordering guarantees beyond priority + FIFO-within-priority —
  no strict per-key ordering (e.g. "all jobs for user X must process in
  submission order").
- Horizontal partitioning/sharding of the jobs table — v1's single-table
  design is correct and simple; sharding is a documented future
  consideration if queue depth ever demands it, not a v1 requirement.

## 5. Success criteria

1. Under N concurrent workers claiming from the same queue, every job is
   claimed by exactly one worker at a time — proven by
   `test/integration/concurrency_test.go` with 10 workers and 50 jobs.
2. A worker that stalls past its lease expiry and then attempts to
   report completion is rejected (`ErrLeaseMismatch`), not silently
   accepted — proven by `TestStaleLeaseCannotCompleteAfterReclaim`.
3. A duplicate enqueue request (same `dedupKey`) never creates two jobs.
4. A job that fails `max_attempts` times lands in the dead-letter queue
   automatically, with its full history preserved, and can be requeued
   with a single API call.
5. The API and reaper are independently deployable and independently
   restartable — killing either one doesn't corrupt the other's view of
   queue state, because all state lives in Postgres, not in either
   process's memory.

## 6. Non-functional requirements

- **Correctness over throughput** — same philosophy as every project in
  this roadmap: this queue is designed to be provably correct under
  concurrency and worker failure first. Very high throughput (tens of
  thousands of jobs/sec) would eventually call for a log-based broker
  instead of a single Postgres table — documented as a deliberate,
  explained v1 tradeoff, not an oversight (see `docs/ARCHITECTURE.md`).
- **Zero paid dependencies** — Postgres only, runs entirely via Docker
  Compose.
- **Cross-language from day one** — the REST API is not an afterthought
  bolted onto a Go-only library; it's the primary interface, with the Go
  SDK as a convenience layer on top of it.

## 7. Risks / open questions

- A single Postgres table as the queue backing store has a throughput
  ceiling well below a dedicated broker (Kafka, NATS JetStream, SQS) —
  fine for this portfolio's scale, worth naming explicitly as the
  tradeoff it is.
- No per-tenant auth means this should not be exposed outside a trusted
  network as-is; flagged in `docs/CURSOR_CONTEXT.md` as a needed
  addition before any project treats it as a public-facing dependency.
- `RunWorker`'s polling loop (versus long-polling or a push-based
  notification) trades a small amount of latency (up to `pollInterval`)
  for significant simplicity — worth revisiting if a future project
  needs near-real-time job pickup.
