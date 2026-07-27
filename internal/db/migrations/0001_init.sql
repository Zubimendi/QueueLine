-- QueueLine schema. Full reasoning in docs/ARCHITECTURE.md; the two
-- ideas worth flagging up front:
--
--   1. `lease_id` is a fencing token, not just a "who owns this" marker.
--      Every successful Claim mints a NEW lease_id. Heartbeat/Complete/
--      Fail must present the lease_id they were given, and it must match
--      the job's CURRENT lease_id. This is what prevents a classic
--      distributed-systems bug: a worker that stalls past its lease
--      expiry (GC pause, network partition, whatever), gets its job
--      reclaimed and re-leased to a second worker, then "wakes up" and
--      tries to report success on a job it no longer owns. Without
--      fencing, that stale completion could corrupt the second worker's
--      in-flight attempt. With it, the stale request is rejected with a
--      clean 409 - see Martin Kleppmann's writing on fencing tokens for
--      the canonical explanation of why this matters (Designing
--      Data-Intensive Applications, ch. 8/9 - this schema is a direct,
--      practical application of that idea).
--
--   2. `dedup_key` + a partial unique index gives idempotent enqueue:
--      the same logical job submitted twice (a caller retrying a timed-
--      out enqueue request) creates one job, not two.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue            TEXT NOT NULL,
    payload          JSONB NOT NULL,
    priority         INT NOT NULL DEFAULT 0,          -- higher claims first
    status           TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING','LEASED','COMPLETED','FAILED','DEAD_LETTERED')),
    attempts         INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 5,
    run_after        TIMESTAMPTZ NOT NULL DEFAULT now(),  -- delayed jobs / backoff schedule
    lease_id         UUID,                              -- fencing token, see above
    lease_expires_at TIMESTAMPTZ,
    dedup_key        TEXT,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent enqueue: NULL dedup_key means "no dedup requested", and
-- Postgres treats every NULL as distinct for uniqueness purposes, so
-- this only constrains callers who actually opt in.
CREATE UNIQUE INDEX idx_jobs_queue_dedup ON jobs (queue, dedup_key) WHERE dedup_key IS NOT NULL;

-- The index that makes Claim's query fast: only PENDING, due jobs are
-- ever candidates, ordered by priority then age.
CREATE INDEX idx_jobs_claimable ON jobs (queue, priority DESC, run_after)
    WHERE status = 'PENDING';

-- The index the reaper sweeps with: only LEASED jobs whose lease expired.
CREATE INDEX idx_jobs_lease_expiry ON jobs (lease_expires_at)
    WHERE status = 'LEASED';

CREATE INDEX idx_jobs_queue_status ON jobs (queue, status);

-- Terminal home for jobs that exhausted their retries. Kept as a
-- separate table (not just a status on `jobs`) so operational queries
-- against the hot `jobs` table never have to scan long-dead rows, and
-- so a dead-lettered job's full history (why, when, how many attempts)
-- is preserved even if someone wants to prune old `jobs` rows later.
CREATE TABLE dead_letter_jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_job_id  UUID NOT NULL,
    queue            TEXT NOT NULL,
    payload          JSONB NOT NULL,
    attempts         INT NOT NULL,
    last_error       TEXT,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dead_letter_queue ON dead_letter_jobs (queue, dead_lettered_at);
