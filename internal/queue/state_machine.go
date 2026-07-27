// "If a workflow modifies state, model it as explicit state transitions."
// Every legal move a Job can make. Nothing in this codebase writes to
// `jobs.status` without going through a path that respects this table -
// Claim only ever moves PENDING->LEASED, Complete only ever moves
// LEASED->COMPLETED, and so on. This is what makes "a completed job gets
// silently re-run" or "a dead-lettered job comes back to life on its
// own" structurally impossible to write by accident.
package queue

var transitions = map[Status][]Status{
	StatusPending:      {StatusLeased},
	StatusLeased:       {StatusCompleted, StatusFailed, StatusPending}, // last: reaper reclaim
	StatusFailed:       {StatusPending, StatusDeadLettered},
	StatusCompleted:    {},
	StatusDeadLettered: {},
}

func CanTransition(from, to Status) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}
