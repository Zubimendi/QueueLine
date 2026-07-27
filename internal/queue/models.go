package queue

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusPending      Status = "PENDING"
	StatusLeased       Status = "LEASED"
	StatusCompleted    Status = "COMPLETED"
	StatusFailed       Status = "FAILED" // transient: about to retry
	StatusDeadLettered Status = "DEAD_LETTERED"
)

type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Status         Status          `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"maxAttempts"`
	RunAfter       time.Time       `json:"runAfter"`
	LeaseID        *string         `json:"leaseId,omitempty"`
	LeaseExpiresAt *time.Time      `json:"leaseExpiresAt,omitempty"`
	LastError      *string         `json:"lastError,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type DeadLetterJob struct {
	ID             string          `json:"id"`
	OriginalJobID  string          `json:"originalJobId"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Attempts       int             `json:"attempts"`
	LastError      *string         `json:"lastError,omitempty"`
	DeadLetteredAt time.Time       `json:"deadLetteredAt"`
}

type QueueStats struct {
	Queue          string `json:"queue"`
	Pending        int64  `json:"pending"`
	Leased         int64  `json:"leased"`
	Completed24h   int64  `json:"completed24h"`
	DeadLettered   int64  `json:"deadLettered"`
}
