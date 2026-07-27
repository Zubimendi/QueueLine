// Same exponential-backoff-with-full-jitter shape used in the Dispatcher
// project, for the same reason: many jobs failing around the same
// moment (a downstream dependency blipping) must not all retry in
// lockstep the instant their window opens.
package queue

import (
	"math"
	"math/rand"
	"time"
)

const (
	baseBackoff = 2 * time.Second
	maxBackoff  = 10 * time.Minute
)

func BackoffWithJitter(attempt int) time.Duration {
	backoff := time.Duration(math.Min(
		float64(baseBackoff)*math.Pow(2, float64(attempt-1)),
		float64(maxBackoff),
	))
	jitter := time.Duration(rand.Int63n(int64(backoff)/2 + 1))
	return backoff/2 + jitter
}
