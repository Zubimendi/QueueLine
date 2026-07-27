package queue

import "testing"

func TestCanTransition_LegalPaths(t *testing.T) {
	cases := []struct {
		from, to Status
		want     bool
	}{
		{StatusPending, StatusLeased, true},
		{StatusLeased, StatusCompleted, true},
		{StatusLeased, StatusFailed, true},
		{StatusLeased, StatusPending, true}, // reaper reclaim
		{StatusFailed, StatusPending, true},
		{StatusFailed, StatusDeadLettered, true},
		// illegal
		{StatusCompleted, StatusLeased, false},
		{StatusDeadLettered, StatusPending, false},
		{StatusPending, StatusCompleted, false}, // cannot skip leasing
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}
