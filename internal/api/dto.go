package api

import "encoding/json"

// "If the client provides business-critical data, validate it on the
// server." Every request DTO here is validated in handlers.go before
// ever reaching the queue engine - malformed input never becomes a bad
// database row.

type enqueueRequest struct {
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	DelaySec    int             `json:"delaySeconds"`
	MaxAttempts int             `json:"maxAttempts"`
	DedupKey    *string         `json:"dedupKey,omitempty"`
}

type leaseRequest struct {
	LeaseSeconds int `json:"leaseSeconds"`
}

type heartbeatRequest struct {
	LeaseID       string `json:"leaseId"`
	ExtendSeconds int    `json:"extendSeconds"`
}

type completeRequest struct {
	LeaseID string `json:"leaseId"`
}

type failRequest struct {
	LeaseID string `json:"leaseId"`
	Error   string `json:"error"`
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}
