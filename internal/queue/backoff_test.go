package queue

import (
	"testing"
	"time"
)

func TestBackoffWithJitter_GrowsAndCaps(t *testing.T) {
	for attempt := 1; attempt <= 12; attempt++ {
		d := BackoffWithJitter(attempt)
		if d < 0 {
			t.Fatalf("attempt %d: backoff must not be negative, got %v", attempt, d)
		}
		if d > maxBackoff {
			t.Fatalf("attempt %d: backoff exceeded cap: %v", attempt, d)
		}
	}
}

func TestBackoffWithJitter_Varies(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 20; i++ {
		seen[BackoffWithJitter(3)] = true
	}
	if len(seen) < 2 {
		t.Fatal("expected jitter to produce varying durations across calls")
	}
}
