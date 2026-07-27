# Agent context: resuming work on QueueLine

Read this file first, then `docs/ARCHITECTURE.md`, before changing
anything.

## Project identity

- **Name:** QueueLine — a Postgres-backed distributed job queue with a
  REST API.
- **Stack:** Go 1.22, PostgreSQL 16, `chi` router, Docker Compose.
- **Place in the larger plan:** this is **Week 1, Project 1** of a
  30-project, 15-week backend engineering roadmap (see
  `30-backend-project-roadmap.md` and `backend-engineering-playbook.md`
  from the same portfolio effort — if those files aren't in this repo,
  they exist in the broader project context and should be requested).
  QueueLine is deliberately built as **reusable infrastructure** — the
  plan is for later weeks' projects (in any of Go/Python/NestJS) to
  depend on this via its REST API instead of rebuilding job-queue logic
  from scratch each time. Keep that reusability goal in mind for any
  change: a change that makes QueueLine harder to depend on from another
  project works against the whole roadmap's design.
- **Companion projects so far:** Dispatcher (Go/GraphQL webhook
  delivery), SlotForge (NestJS/Postgres booking engine). Same underlying
  documentation and principles discipline, applied here to a third
  domain and (partially) a third consumption model — REST API as a
  cross-language dependency, not just a single service's own interface.

## Current state (as of this handoff)

### Done
- Full schema (`internal/db/migrations/0001_init.sql`): `jobs` (with
  fencing-token `lease_id`, priority, delay via `run_after`, dedup key)
  and `dead_letter_jobs`.
- `internal/queue/engine.go` — the core: `Enqueue`, `Claim`, `Heartbeat`,
  `Complete`, `Fail`, `Reap`, `GetJob`, `Stats`, `ListDeadLetters`,
  `RequeueDeadLetter`. All the correctness logic (SKIP LOCKED, fencing
  tokens, backoff, dead-lettering) lives here.
- `internal/queue/state_machine.go` — explicit legal `Status`
  transitions.
- `internal/queue/backoff.go` — exponential backoff with jitter, same
  shape as Dispatcher's.
- `internal/api/` — REST API (chi router): enqueue, lease, heartbeat,
  complete, fail, get job, stats, dead-letter list/requeue. Structured
  logging and panic-recovery middleware.
- `pkg/client/` — Go SDK with `Enqueue` and `RunWorker` (automatic
  polling, automatic heartbeating at half the lease interval, clean
  context-cancellation shutdown).
- `cmd/api`, `cmd/reaper`, `cmd/worker-example` — three entrypoints, API
  and reaper both with graceful shutdown / clean loop termination.
- Unit tests: state machine, backoff (`internal/queue/*_test.go`).
- **The two most important tests in the repo**:
  `test/integration/concurrency_test.go`'s
  `TestConcurrentClaims_NeverDoubleClaim` (10 workers, 50 jobs, proves no
  double-claim) and `TestStaleLeaseCannotCompleteAfterReclaim` (proves
  fencing tokens actually reject a stale worker's completion attempt).
- `docker-compose.yml` (Postgres only — this project has no other
  infra dependency, deliberately minimal), `Makefile`, `.env.example`.
- Docs: `README.md`, `docs/PRD.md`, `docs/ARCHITECTURE.md`,
  `docs/TESTING.md`, `docs/STORY.md`, this file.

### NOT done — verify before assuming it works

- **This code has not been compiled or run.** Written without network
  access (no `go mod tidy`, no live Postgres to test against). First
  task in a new session:
  ```bash
  go mod tidy
  go build ./...
  go vet ./...
  make up
  make test
  make test-integration
  ```
  Likely trouble spots: (a) the `isUniqueViolation` helper in
  `engine.go` uses a structural interface check (`SQLState() string`)
  rather than importing `pgconn.PgError` directly — this was written to
  minimize import surface but should be double-checked against the
  actual pgx v5 error type; a direct `errors.As(err, &pgErr)` against
  `*pgconn.PgError` (the pattern Dispatcher used) may be more robust and
  is the first thing to try if the duplicate-detection path doesn't work
  as expected; (b) the chi router version pinned in `go.mod` should be
  confirmed current; (c) `pkg/client`'s `RunWorker` swallows lease/claim
  errors into a sleep-and-retry rather than surfacing them — reasonable
  for a first pass, but worth adding a max-consecutive-errors circuit
  breaker so a persistently misconfigured worker doesn't spin silently
  forever (flagged, not fixed, in this handoff).
- **No Python or TypeScript client SDK.** The REST API is a complete,
  documented contract for those languages already (see README's curl
  examples), but no convenience wrapper exists yet. Build one the first
  time a Python or NestJS project in the roadmap actually needs to
  consume QueueLine — don't build it speculatively before there's a real
  consumer to validate the ergonomics against.
- **No auth on the REST API.** Fine for local, trusted-network use
  across this portfolio's projects; would need API keys (and probably
  per-caller rate limiting) before being exposed any more broadly. Not
  built because no current consumer needs it yet — add it when one does.
- **No `LISTEN`/`NOTIFY`-based push for job pickup** — `RunWorker` polls
  every `pollInterval` (500ms). Simple and correct; a documented future
  improvement if a later project's use case genuinely needs lower
  pickup latency than that.
- **No load test characterizing the actual claim-throughput ceiling** —
  `docs/ARCHITECTURE.md` names "a single Postgres table won't scale to
  extreme throughput" as a known tradeoff but this hasn't been measured.
  A k6 or vegeta-based benchmark against a queue with, say, 100k pending
  jobs and an increasing number of concurrent claimers would turn that
  from an assumption into a measured, citable number — genuinely useful
  for an interview answer ("I measured X claims/sec before latency
  degraded, here's why").

## Design decisions already made — don't relitigate without reason

1. **REST API as the primary interface, Go SDK as a convenience layer on
   top of it** — not the reverse. This is what makes QueueLine usable
   from Python/NestJS projects later in the roadmap without them needing
   a Go dependency. Don't refactor this so the REST API becomes a thin
   wrapper around Go-specific internals that leak through.
2. **Fencing tokens (`lease_id`), not just a "who owns this" worker-ID
   field.** A worker-ID field alone doesn't distinguish "you're still
   the current owner" from "you WERE the owner, but that claim has been
   superseded" — see `docs/ARCHITECTURE.md` §1 and
   `docs/STORY.md` for the full reasoning. This is the project's core
   lesson; don't simplify it away to reduce API surface.
3. **A single Postgres table, not a dedicated broker.** Correct at this
   portfolio's scale, and keeps the whole project deployable with zero
   extra infrastructure beyond Postgres (which every other project in
   the roadmap already needs). Revisit only if a specific future
   project's load genuinely demands more throughput than a single table
   comfortably provides — and measure first (see the load-test item
   above) rather than assuming.
4. **The reaper is a separate binary/process from the API**, mirroring
   Dispatcher's `cmd/reconciler` pattern — deliberately, so a bug or
   slowdown in lease-reclamation logic can never affect claim/complete
   latency for healthy workers, and so the two can be scaled and deployed
   independently.

## Suggested next-session priorities, in order

1. Get it compiling and passing both unit and integration tests (see
   "NOT done" above) — this project's two integration tests are the
   actual proof of its core claims, so getting them running for real is
   higher priority here than in most projects.
2. Walk through `docs/TESTING.md` end-to-end by hand once, including the
   manual reaper/crash-simulation test.
3. Run the load test described above and update `docs/ARCHITECTURE.md`
   with a measured throughput ceiling instead of a qualitative claim.
4. When Week 3 or later's Python/NestJS project first needs background
   job processing, build that language's client SDK against the REST
   API at that point — informed by a real consumer, not speculatively.

## How to give a fresh agent session everything it needs

Point it at, in this order: this file → `docs/ARCHITECTURE.md` →
`docs/PRD.md`. Tell it explicitly: "this codebase has never been
compiled — verify and fix before adding features," and "the fencing-
token mechanism in Claim/Heartbeat/Complete/Fail is the point of this
project — don't simplify it away to fix a build error without
understanding why it's there first."
