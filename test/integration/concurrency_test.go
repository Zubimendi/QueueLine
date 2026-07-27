// This is the single most important test in the repo: it proves the
// core correctness claim - that FOR UPDATE SKIP LOCKED means N
// concurrent workers claiming from the same queue never claim the same
// job twice - under real concurrent load against a real Postgres.
// Requires `make up && make migrate` first (see docs/TESTING.md).
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yourname/queueline/internal/config"
	"github.com/yourname/queueline/internal/db"
	"github.com/yourname/queueline/internal/queue"
)

func TestConcurrentClaims_NeverDoubleClaim(t *testing.T) {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Skipf("skipping: could not connect to Postgres (%v) - run `make up` first", err)
	}
	defer pool.Close()

	engine := queue.NewEngine(pool)
	queueName := fmt.Sprintf("concurrency-test-%d", time.Now().UnixNano())

	const numJobs = 50
	const numWorkers = 10

	for i := 0; i < numJobs; i++ {
		payload, _ := json.Marshal(map[string]int{"n": i})
		if _, err := engine.Enqueue(context.Background(), queue.EnqueueInput{
			Queue: queueName, Payload: payload, MaxAttempts: 3,
		}); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	claimed := make(chan string, numJobs*2)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				job, err := engine.Claim(context.Background(), queueName, 30*time.Second)
				if err != nil {
					t.Errorf("worker %d: claim error: %v", workerID, err)
					return
				}
				if job == nil {
					return // queue drained
				}
				claimed <- job.ID
				if err := engine.Complete(context.Background(), job.ID, *job.LeaseID); err != nil {
					t.Errorf("worker %d: complete error: %v", workerID, err)
				}
			}
		}(w)
	}

	wg.Wait()
	close(claimed)

	seen := map[string]int{}
	for id := range claimed {
		seen[id]++
	}

	if len(seen) != numJobs {
		t.Fatalf("expected exactly %d unique jobs claimed, got %d", numJobs, len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("job %s was claimed %d times - two workers claimed the same job", id, count)
		}
	}
}

// Proves the fencing token: a worker whose lease has (conceptually)
// already been reclaimed cannot complete the job out from under whoever
// claimed it next.
func TestStaleLeaseCannotCompleteAfterReclaim(t *testing.T) {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Skipf("skipping: could not connect to Postgres (%v) - run `make up` first", err)
	}
	defer pool.Close()

	engine := queue.NewEngine(pool)
	queueName := fmt.Sprintf("fencing-test-%d", time.Now().UnixNano())

	payload, _ := json.Marshal(map[string]string{"x": "y"})
	if _, err := engine.Enqueue(context.Background(), queue.EnqueueInput{Queue: queueName, Payload: payload, MaxAttempts: 3}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Claim with a lease so short it's already "expired" by the time we reap.
	job, err := engine.Claim(context.Background(), queueName, 1*time.Millisecond)
	if err != nil || job == nil {
		t.Fatalf("claim: %v", err)
	}
	staleLeaseID := *job.LeaseID

	time.Sleep(10 * time.Millisecond)
	reaped, err := engine.Reap(context.Background())
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("expected reaper to reclaim 1 job, got %d", reaped)
	}

	// A second worker claims the now-PENDING job, getting a NEW lease.
	second, err := engine.Claim(context.Background(), queueName, 30*time.Second)
	if err != nil || second == nil {
		t.Fatalf("second claim: %v", err)
	}
	if *second.LeaseID == staleLeaseID {
		t.Fatal("second claim reused the stale lease ID - fencing is broken")
	}

	// The original, stale worker "wakes up" and tries to complete using
	// its old lease. This must fail.
	err = engine.Complete(context.Background(), job.ID, staleLeaseID)
	if err != queue.ErrLeaseMismatch {
		t.Fatalf("expected ErrLeaseMismatch for a stale lease completion, got: %v", err)
	}

	// The second (legitimate) worker's completion must succeed.
	if err := engine.Complete(context.Background(), second.ID, *second.LeaseID); err != nil {
		t.Fatalf("legitimate completion with current lease should succeed, got: %v", err)
	}
}
