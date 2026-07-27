# Testing QueueLine

## 1. Unit tests

No Docker required.

```bash
go mod tidy   # first time only, needs network
make test
```

Covers the state machine (`state_machine_test.go`: legal and illegal
transitions) and backoff/jitter (`backoff_test.go`: growth, cap, and
variance across calls).

## 2. The concurrency + fencing-token tests — the most important thing to run

```bash
make up             # Postgres + migrations
make test-integration
```

This runs `test/integration/concurrency_test.go` against a real
Postgres. Two tests:

**`TestConcurrentClaims_NeverDoubleClaim`** — enqueues 50 jobs, starts 10
goroutines all calling `Claim` in a tight loop against the same queue
until it's drained, and asserts every one of the 50 jobs was claimed by
exactly one of them. This is the test that would catch a regression
immediately if `FOR UPDATE SKIP LOCKED` were ever accidentally changed to
a plain `SELECT` (try it yourself: remove `FOR UPDATE SKIP LOCKED` from
`Engine.Claim`'s query, re-run this test, and watch it fail with jobs
claimed more than once).

**`TestStaleLeaseCannotCompleteAfterReclaim`** — claims a job with an
intentionally tiny lease TTL, lets it expire, runs the reaper to reclaim
it, has a second `Claim` pick it up fresh, and then proves two things at
once: the *original* worker's stale `Complete` call (using its old,
superseded `lease_id`) is rejected with `ErrLeaseMismatch`, and the
*second* worker's `Complete` call (using the current `lease_id`)
succeeds. This is the test that proves fencing tokens actually work, not
just that the code compiles.

## 3. End-to-end manual walkthrough

```bash
cp .env.example .env
go mod tidy
make up
make api            # terminal 1
make reaper           # terminal 2
```

Enqueue and process a job by hand:

```bash
curl -X POST localhost:8080/v1/queues/demo/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload": {"hello": "world"}, "maxAttempts": 3}'
# -> {"id": "...", "status": "PENDING", ...}

curl -X POST localhost:8080/v1/queues/demo/lease \
  -H 'Content-Type: application/json' -d '{"leaseSeconds": 30}'
# -> {"id": "...", "leaseId": "...", "status": "LEASED", "attempts": 1, ...}

curl -X POST localhost:8080/v1/jobs/<id>/complete \
  -H 'Content-Type: application/json' -d '{"leaseId": "<leaseId>"}'
# -> 204 No Content

curl localhost:8080/v1/queues/demo/stats
# -> {"queue":"demo","pending":0,"leased":0,"completed24h":1,"deadLettered":0}
```

Or just run `make worker-example` — it does all of the above
automatically and logs each step.

## 4. Proving retries and backoff

```bash
JOB=$(curl -s -X POST localhost:8080/v1/queues/demo/jobs \
  -H 'Content-Type: application/json' -d '{"payload": {"x":1}, "maxAttempts": 3}' | jq -r .id)

LEASE=$(curl -s -X POST localhost:8080/v1/queues/demo/lease \
  -H 'Content-Type: application/json' -d '{"leaseSeconds": 30}' | jq -r .leaseId)

curl -X POST localhost:8080/v1/jobs/$JOB/fail \
  -H 'Content-Type: application/json' -d "{\"leaseId\": \"$LEASE\", \"error\": \"simulated failure\"}"
```

Check the job's `run_after` moved into the future (backoff scheduled)
and `attempts` incremented:

```bash
curl localhost:8080/v1/jobs/$JOB
```

Repeat the fail cycle 3 times total (`maxAttempts`), and confirm the job
now shows up in the dead-letter list instead of being claimable:

```bash
curl localhost:8080/v1/queues/demo/dead-letters
```

Requeue it and confirm it's claimable again:

```bash
DLID=$(curl -s localhost:8080/v1/queues/demo/dead-letters | jq -r '.[0].id')
curl -X POST localhost:8080/v1/dead-letters/$DLID/requeue
```

## 5. Proving the reaper (worker-crash simulation)

1. Enqueue and lease a job with a short lease
   (`{"leaseSeconds": 3}`).
2. Do **not** call complete, fail, or heartbeat — simulating a worker
   that crashed mid-job.
3. Wait past the lease TTL and one reaper tick (`REAPER_INTERVAL`,
   default 10s).
4. Confirm via `GET /v1/jobs/<id>` that the job is back to `PENDING`
   (or `DEAD_LETTERED` if it had already exhausted `max_attempts`) and
   `attempts` incremented — the reaper treated the silent worker death
   exactly like an explicit failure report.

## 6. Load-testing claim throughput (optional)

To get a feel for how many concurrent workers a single Postgres instance
comfortably supports before `Claim` latency degrades, run
`worker-example` multiple times in parallel against a queue pre-seeded
with a few thousand jobs, and watch `queueline_jobs_claimed_total` in
`/metrics` climb. This is a good exercise for building intuition about
the throughput ceiling named as a tradeoff in `docs/ARCHITECTURE.md`,
rather than just taking the doc's word for it.

## Known gaps in test coverage

- No test yet for the idempotent-enqueue (`dedupKey`) path under genuine
  concurrent load (two simultaneous enqueue calls with the same key) —
  worth adding alongside the two existing integration tests.
- No load test characterizing the actual claim-throughput ceiling
  mentioned in `docs/ARCHITECTURE.md` — see §6 above for a manual
  starting point; a proper k6/vegeta-based benchmark is a good next
  addition.
