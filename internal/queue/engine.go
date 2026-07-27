// Package queue is QueueLine's core: a Postgres-backed job queue using
// SELECT ... FOR UPDATE SKIP LOCKED for claim-safety across any number
// of concurrent workers (the same technique Dispatcher used for webhook
// delivery jobs, generalized here into standalone, reusable
// infrastructure any future project - Go, Python, or NestJS, via the
// HTTP API in internal/api - can depend on instead of rebuilding).
//
// Every exported method here maps to one principle from the playbook:
//   Enqueue          -> idempotency (dedup key), transactional write
//   Claim             -> concurrency control (SKIP LOCKED), explicit state machine
//   Heartbeat         -> long-running job support without losing the lease
//   Complete / Fail   -> fencing tokens (lease_id) prevent stale writes
//   Reap              -> explicit failure path for "the worker process died"
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound      = errors.New("job not found")
	ErrLeaseMismatch = errors.New("lease_id does not match the job's current lease (it may have expired and been reclaimed)")
	ErrDuplicate     = errors.New("a job with this dedup_key already exists in this queue")
)

type Engine struct {
	pool *pgxpool.Pool
}

func NewEngine(pool *pgxpool.Pool) *Engine {
	return &Engine{pool: pool}
}

type EnqueueInput struct {
	Queue       string
	Payload     json.RawMessage
	Priority    int
	DelaySec    int
	MaxAttempts int
	DedupKey    *string
}

// Enqueue is a straightforward insert, but two things make it more than
// that: dedup_key gives the caller idempotent submission (retry-safe),
// and run_after (computed from DelaySec) is what makes delayed jobs a
// first-class feature rather than something bolted on with a separate
// scheduler.
func (e *Engine) Enqueue(ctx context.Context, in EnqueueInput) (Job, error) {
	if in.MaxAttempts <= 0 {
		in.MaxAttempts = 5
	}
	runAfter := time.Now().Add(time.Duration(in.DelaySec) * time.Second)

	var job Job
	row := e.pool.QueryRow(ctx, `
		INSERT INTO jobs (queue, payload, priority, max_attempts, run_after, dedup_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, queue, payload, priority, status, attempts, max_attempts, run_after, created_at, updated_at
	`, in.Queue, in.Payload, in.Priority, in.MaxAttempts, runAfter, in.DedupKey)

	err := row.Scan(&job.ID, &job.Queue, &job.Payload, &job.Priority, &job.Status,
		&job.Attempts, &job.MaxAttempts, &job.RunAfter, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			existing, ferr := e.findByDedupKey(ctx, in.Queue, *in.DedupKey)
			if ferr != nil {
				return job, fmt.Errorf("fetch existing job after dedup conflict: %w", ferr)
			}
			return existing, ErrDuplicate // caller decides whether this is an error or a no-op
		}
		return job, fmt.Errorf("insert job: %w", err)
	}
	return job, nil
}

func (e *Engine) findByDedupKey(ctx context.Context, queueName, dedupKey string) (Job, error) {
	var job Job
	err := e.pool.QueryRow(ctx, `
		SELECT id, queue, payload, priority, status, attempts, max_attempts, run_after, created_at, updated_at
		FROM jobs WHERE queue = $1 AND dedup_key = $2
	`, queueName, dedupKey).Scan(&job.ID, &job.Queue, &job.Payload, &job.Priority, &job.Status,
		&job.Attempts, &job.MaxAttempts, &job.RunAfter, &job.CreatedAt, &job.UpdatedAt)
	return job, err
}

// Claim atomically finds the highest-priority, oldest, due PENDING job
// in the given queue, locks it with FOR UPDATE SKIP LOCKED (so any
// number of concurrent callers - one process, many processes, many
// languages via the HTTP API - never claim the same row), mints a fresh
// lease_id (the fencing token), and marks it LEASED. Returns (nil, nil)
// when the queue is empty - the normal, expected steady state, not an
// error condition.
func (e *Engine) Claim(ctx context.Context, queueName string, leaseTTL time.Duration) (*Job, error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var job Job
	err = tx.QueryRow(ctx, `
		SELECT id, queue, payload, priority, status, attempts, max_attempts, run_after, created_at, updated_at
		FROM jobs
		WHERE queue = $1 AND status = $2 AND run_after <= now()
		ORDER BY priority DESC, run_after ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, queueName, StatusPending).Scan(&job.ID, &job.Queue, &job.Payload, &job.Priority, &job.Status,
		&job.Attempts, &job.MaxAttempts, &job.RunAfter, &job.CreatedAt, &job.UpdatedAt)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claim job: %w", err)
	}

	leaseID := uuid.NewString()
	leaseExpiresAt := time.Now().Add(leaseTTL)

	_, err = tx.Exec(ctx, `
		UPDATE jobs
		SET status = $1, attempts = attempts + 1, lease_id = $2, lease_expires_at = $3, updated_at = now()
		WHERE id = $4
	`, StatusLeased, leaseID, leaseExpiresAt, job.ID)
	if err != nil {
		return nil, fmt.Errorf("lease job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	job.Status = StatusLeased
	job.Attempts++
	job.LeaseID = &leaseID
	job.LeaseExpiresAt = &leaseExpiresAt
	return &job, nil
}

// Heartbeat extends a job's lease - for handlers whose work legitimately
// takes longer than the initial lease TTL (a large file conversion, a
// slow report generation). Requires the caller's lease_id to match
// exactly; a worker whose lease already expired and was reclaimed by the
// reaper gets ErrLeaseMismatch, not a silently-accepted heartbeat on a
// job it no longer owns.
func (e *Engine) Heartbeat(ctx context.Context, jobID, leaseID string, extend time.Duration) error {
	tag, err := e.pool.Exec(ctx, `
		UPDATE jobs SET lease_expires_at = now() + $1::interval, updated_at = now()
		WHERE id = $2 AND lease_id = $3 AND status = $4
	`, extend.String(), jobID, leaseID, StatusLeased)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// Complete is the same fencing-token check as Heartbeat: the lease_id
// presented must match the job's CURRENT lease_id. This is the guard
// against the "stale worker completes a job it no longer owns" race
// described in migration 0001's comments.
func (e *Engine) Complete(ctx context.Context, jobID, leaseID string) error {
	tag, err := e.pool.Exec(ctx, `
		UPDATE jobs SET status = $1, lease_id = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $2 AND lease_id = $3 AND status = $4
	`, StatusCompleted, jobID, leaseID, StatusLeased)
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// Fail records a failed attempt. If attempts are exhausted, the job is
// moved to dead_letter_jobs (in the same transaction as removing it from
// the hot `jobs` table's active working set) rather than just flagged -
// see migration 0001 for why that's a separate table. Otherwise it's
// rescheduled with exponential backoff.
func (e *Engine) Fail(ctx context.Context, jobID, leaseID, errMsg string) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var job Job
	err = tx.QueryRow(ctx, `
		SELECT id, queue, payload, attempts, max_attempts
		FROM jobs WHERE id = $1 AND lease_id = $2 AND status = $3
		FOR UPDATE
	`, jobID, leaseID, StatusLeased).Scan(&job.ID, &job.Queue, &job.Payload, &job.Attempts, &job.MaxAttempts)
	if err != nil {
		if err == pgx.ErrNoRows {
			return ErrLeaseMismatch
		}
		return fmt.Errorf("load job for fail: %w", err)
	}

	if job.Attempts >= job.MaxAttempts {
		return e.deadLetter(ctx, tx, job, errMsg)
	}

	nextRun := time.Now().Add(BackoffWithJitter(job.Attempts))
	_, err = tx.Exec(ctx, `
		UPDATE jobs
		SET status = $1, run_after = $2, lease_id = NULL, lease_expires_at = NULL,
		    last_error = $3, updated_at = now()
		WHERE id = $4
	`, StatusPending, nextRun, errMsg, jobID)
	if err != nil {
		return fmt.Errorf("reschedule job: %w", err)
	}

	return tx.Commit(ctx)
}

func (e *Engine) deadLetter(ctx context.Context, tx pgx.Tx, job Job, errMsg string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO dead_letter_jobs (original_job_id, queue, payload, attempts, last_error)
		VALUES ($1, $2, $3, $4, $5)
	`, job.ID, job.Queue, job.Payload, job.Attempts, errMsg)
	if err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE jobs SET status = $1, lease_id = NULL, lease_expires_at = NULL, last_error = $2, updated_at = now()
		WHERE id = $3
	`, StatusDeadLettered, errMsg, job.ID)
	if err != nil {
		return fmt.Errorf("mark dead lettered: %w", err)
	}
	return tx.Commit(ctx)
}

// Reap is QueueLine's explicit failure path for "the worker process
// itself died" (crashed, OOM-killed, network partition) without ever
// calling Complete or Fail. Run on a schedule (see cmd/reaper) as an
// entirely separate deployable process, deliberately, so a bug or outage
// in the reaper can never take down job claiming/completion, and vice
// versa. Jobs whose lease expired are treated exactly like a Fail call -
// same backoff, same dead-lettering-when-exhausted logic - because from
// the queue's point of view, an unrenewed expired lease and an explicit
// failure report both mean "this attempt did not succeed."
func (e *Engine) Reap(ctx context.Context) (int, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT id, lease_id FROM jobs WHERE status = $1 AND lease_expires_at < now()
	`, StatusLeased)
	if err != nil {
		return 0, fmt.Errorf("find expired leases: %w", err)
	}
	type expired struct{ id, leaseID string }
	var stale []expired
	for rows.Next() {
		var x expired
		if err := rows.Scan(&x.id, &x.leaseID); err != nil {
			rows.Close()
			return 0, err
		}
		stale = append(stale, x)
	}
	rows.Close()

	reaped := 0
	for _, x := range stale {
		if err := e.Fail(ctx, x.id, x.leaseID, "lease expired: worker did not complete, fail, or heartbeat in time"); err != nil {
			continue // best-effort; next reaper tick will retry any that failed to update
		}
		reaped++
	}
	return reaped, nil
}

func (e *Engine) GetJob(ctx context.Context, jobID string) (Job, error) {
	var job Job
	err := e.pool.QueryRow(ctx, `
		SELECT id, queue, payload, priority, status, attempts, max_attempts, run_after,
		       lease_id, lease_expires_at, last_error, created_at, updated_at
		FROM jobs WHERE id = $1
	`, jobID).Scan(&job.ID, &job.Queue, &job.Payload, &job.Priority, &job.Status, &job.Attempts,
		&job.MaxAttempts, &job.RunAfter, &job.LeaseID, &job.LeaseExpiresAt, &job.LastError,
		&job.CreatedAt, &job.UpdatedAt)
	if err == pgx.ErrNoRows {
		return job, ErrNotFound
	}
	return job, err
}

func (e *Engine) Stats(ctx context.Context, queueName string) (QueueStats, error) {
	stats := QueueStats{Queue: queueName}
	err := e.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'PENDING'),
			COUNT(*) FILTER (WHERE status = 'LEASED'),
			COUNT(*) FILTER (WHERE status = 'COMPLETED' AND updated_at > now() - interval '24 hours')
		FROM jobs WHERE queue = $1
	`, queueName).Scan(&stats.Pending, &stats.Leased, &stats.Completed24h)
	if err != nil {
		return stats, fmt.Errorf("stats query: %w", err)
	}
	err = e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dead_letter_jobs WHERE queue = $1`, queueName).Scan(&stats.DeadLettered)
	return stats, err
}

func (e *Engine) ListDeadLetters(ctx context.Context, queueName string, limit int) ([]DeadLetterJob, error) {
	rows, err := e.pool.Query(ctx, `
		SELECT id, original_job_id, queue, payload, attempts, last_error, dead_lettered_at
		FROM dead_letter_jobs WHERE queue = $1
		ORDER BY dead_lettered_at DESC LIMIT $2
	`, queueName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeadLetterJob
	for rows.Next() {
		var d DeadLetterJob
		if err := rows.Scan(&d.ID, &d.OriginalJobID, &d.Queue, &d.Payload, &d.Attempts, &d.LastError, &d.DeadLetteredAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// RequeueDeadLetter gives a dead-lettered job a fresh start: a brand new
// PENDING job with attempts reset to zero. Deliberately creates a NEW
// job row rather than resurrecting the old one in place - the old
// dead_letter_jobs row stays as an untouched historical record of the
// original failure, which matters for post-incident review.
func (e *Engine) RequeueDeadLetter(ctx context.Context, deadLetterID string) (Job, error) {
	var dl DeadLetterJob
	err := e.pool.QueryRow(ctx, `
		SELECT id, original_job_id, queue, payload, attempts, last_error, dead_lettered_at
		FROM dead_letter_jobs WHERE id = $1
	`, deadLetterID).Scan(&dl.ID, &dl.OriginalJobID, &dl.Queue, &dl.Payload, &dl.Attempts, &dl.LastError, &dl.DeadLetteredAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return Job{}, ErrNotFound
		}
		return Job{}, err
	}

	return e.Enqueue(ctx, EnqueueInput{
		Queue:       dl.Queue,
		Payload:     dl.Payload,
		MaxAttempts: 5,
	})
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
