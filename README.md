# QueueLine

A distributed job queue with a REST API, built in Go on top of
PostgreSQL. Week 1, Project 1 of a 15-week backend engineering roadmap
(see `docs/CURSOR_CONTEXT.md` for the full context) — and deliberately
built as **reusable infrastructure**, not a one-off demo: the plan is to
depend on this from later projects in the roadmap instead of rebuilding
job-queue logic in every one of them.

## What it does

- Any service — Go, Python, NestJS, doesn't matter, it's a REST API —
  enqueues jobs onto a named queue, optionally delayed, prioritized, and
  deduplicated via an idempotency key.
- Workers claim jobs (`FOR UPDATE SKIP LOCKED`, safe across any number of
  concurrent worker processes), do the work, and report completion or
  failure.
- Failed jobs retry with exponential backoff and jitter, up to a
  configurable limit, then land in a dead-letter queue for inspection and
  manual requeue.
- A standalone reaper process reclaims jobs whose worker died mid-flight
  without ever reporting success or failure — using **fencing tokens**
  (a fresh lease ID minted on every claim) so a worker that stalls past
  its lease and "wakes up late" can never corrupt a job another worker
  has since picked up. This is the single most important correctness
  property in the codebase — see `docs/ARCHITECTURE.md` §4 and the test
  that proves it, `test/integration/concurrency_test.go`.

## Quickstart

Requires Go 1.22+, Docker, and Docker Compose.

```bash
git clone <your-fork-url> queueline && cd queueline
cp .env.example .env
go mod tidy          # needs network access
make up               # Postgres + migrations
make api               # terminal 1: REST API on :8080
make reaper             # terminal 2: lease reaper
make worker-example      # terminal 3: enqueues + processes one demo job end-to-end
```

`worker-example` is a complete, runnable demonstration of the whole
lifecycle — read `cmd/worker-example/main.go` first if you want to see
the pattern for consuming QueueLine from a Go project, then copy that
shape into whatever needs a background job in a future week.

For a non-Go project (Python, NestJS), skip the Go SDK and talk to the
REST API directly:

```bash
# Enqueue
curl -X POST localhost:8080/v1/queues/welcome-emails/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload": {"userEmail": "test@example.com"}, "maxAttempts": 3}'

# Lease (as a worker would)
curl -X POST localhost:8080/v1/queues/welcome-emails/lease \
  -H 'Content-Type: application/json' -d '{"leaseSeconds": 30}'
# -> {"id": "...", "leaseId": "...", "payload": {...}, ...}

# Complete (using the leaseId from above)
curl -X POST localhost:8080/v1/jobs/<id>/complete \
  -H 'Content-Type: application/json' -d '{"leaseId": "<leaseId>"}'
```

## Documentation

- [`docs/PRD.md`](docs/PRD.md) — the problem, users, and scope.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — every design decision
  mapped to the principle it demonstrates, including a full explanation
  of fencing tokens and why they matter.
- [`docs/TESTING.md`](docs/TESTING.md) — how to run it, including the
  test that proves 10 concurrent workers never double-claim a job.
- [`docs/STORY.md`](docs/STORY.md) — narrative for LinkedIn/Medium and
  interview talking points.
- [`docs/CURSOR_CONTEXT.md`](docs/CURSOR_CONTEXT.md) — full handoff
  context, including this project's place in the larger 30-project
  roadmap.

## Repo layout

```
internal/queue/    the engine: Enqueue, Claim, Heartbeat, Complete, Fail, Reap
internal/api/       REST API wrapping the engine - the cross-language surface
internal/db/        Postgres connection + migrations
pkg/client/         Go SDK for future Go projects to import directly
cmd/api/            the HTTP server
cmd/reaper/          standalone lease-reclaiming process
cmd/worker-example/   reference pattern for consuming the queue
test/integration/    the concurrency + fencing-token proof
docs/
```

## Status

Built as reusable portfolio infrastructure, not hardened for arbitrary
production traffic as-is. See `docs/CURSOR_CONTEXT.md` for what's done,
what's stubbed, and how later projects in the roadmap are expected to
depend on this one.

## License

MIT — see `LICENSE`.
