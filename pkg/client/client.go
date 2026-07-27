// Package client is QueueLine's Go SDK: what a future Go project in this
// portfolio imports to enqueue and consume jobs, instead of talking to
// the REST API by hand or, worse, reimplementing a job queue from
// scratch again. A Python or NestJS project would call the same REST API
// (see internal/api) directly with a plain HTTP client - this package is
// the Go-specific convenience layer on top of it.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 10 * time.Second}}
}

type Job struct {
	ID       string          `json:"id"`
	Queue    string          `json:"queue"`
	Payload  json.RawMessage `json:"payload"`
	Attempts int             `json:"attempts"`
	LeaseID  *string         `json:"leaseId,omitempty"`
}

type EnqueueOptions struct {
	Priority    int
	DelaySeconds int
	MaxAttempts int
	DedupKey    string
}

func (c *Client) Enqueue(ctx context.Context, queueName string, payload interface{}, opts EnqueueOptions) (*Job, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	reqBody := map[string]interface{}{
		"payload": json.RawMessage(body), "priority": opts.Priority,
		"delaySeconds": opts.DelaySeconds, "maxAttempts": opts.MaxAttempts,
	}
	if opts.DedupKey != "" {
		reqBody["dedupKey"] = opts.DedupKey
	}

	var job Job
	if err := c.post(ctx, fmt.Sprintf("/v1/queues/%s/jobs", queueName), reqBody, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// Handler is what a worker registers to process one job. Returning an
// error triggers a retry (with backoff) or dead-lettering, per
// QueueLine's usual rules - the handler doesn't need to know or care
// which one happens.
type Handler func(ctx context.Context, job *Job) error

// RunWorker polls `queueName` in a loop, dispatching each claimed job to
// handler, and heartbeats automatically at half the lease interval so a
// handler that's still legitimately working never loses its lease. Stops
// cleanly when ctx is cancelled - the intended pattern is to cancel ctx
// on SIGTERM and let any in-flight handler finish before returning; see
// cmd/worker-example for the full graceful-shutdown wiring.
func (c *Client) RunWorker(ctx context.Context, queueName string, leaseSeconds int, handler Handler) error {
	if leaseSeconds <= 0 {
		leaseSeconds = 30
	}
	pollInterval := 500 * time.Millisecond
	consecutiveErrors := 0
	maxConsecutiveErrors := 10

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		job, err := c.lease(ctx, queueName, leaseSeconds)
		if err != nil {
			consecutiveErrors++
			if consecutiveErrors >= maxConsecutiveErrors {
				return fmt.Errorf("worker circuit breaker tripped: %d consecutive lease errors, last error: %w", consecutiveErrors, err)
			}
			time.Sleep(pollInterval)
			continue
		}
		consecutiveErrors = 0

		if job == nil {
			time.Sleep(pollInterval)
			continue
		}

		c.processWithHeartbeat(ctx, job, leaseSeconds, handler)
	}
}

func (c *Client) processWithHeartbeat(ctx context.Context, job *Job, leaseSeconds int, handler Handler) {
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()

	go func() {
		ticker := time.NewTicker(time.Duration(leaseSeconds) * time.Second / 2)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				_ = c.heartbeat(heartbeatCtx, job.ID, *job.LeaseID, leaseSeconds)
			}
		}
	}()

	err := handler(ctx, job)
	if err != nil {
		_ = c.fail(ctx, job.ID, *job.LeaseID, err.Error())
		return
	}
	_ = c.complete(ctx, job.ID, *job.LeaseID)
}

func (c *Client) lease(ctx context.Context, queueName string, leaseSeconds int) (*Job, error) {
	resp, err := c.doRequest(ctx, http.MethodPost, fmt.Sprintf("/v1/queues/%s/lease", queueName),
		map[string]int{"leaseSeconds": leaseSeconds})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	var job Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (c *Client) heartbeat(ctx context.Context, jobID, leaseID string, extendSeconds int) error {
	_, err := c.doRequest(ctx, http.MethodPost, fmt.Sprintf("/v1/jobs/%s/heartbeat", jobID),
		map[string]interface{}{"leaseId": leaseID, "extendSeconds": extendSeconds})
	return err
}

func (c *Client) complete(ctx context.Context, jobID, leaseID string) error {
	_, err := c.doRequest(ctx, http.MethodPost, fmt.Sprintf("/v1/jobs/%s/complete", jobID),
		map[string]string{"leaseId": leaseID})
	return err
}

func (c *Client) fail(ctx context.Context, jobID, leaseID, errMsg string) error {
	_, err := c.doRequest(ctx, http.MethodPost, fmt.Sprintf("/v1/jobs/%s/fail", jobID),
		map[string]string{"leaseId": leaseID, "error": errMsg})
	return err
}

func (c *Client) post(ctx context.Context, path string, body, out interface{}) error {
	resp, err := c.doRequest(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNoContent {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("queueline: %s %s -> %d: %s", method, path, resp.StatusCode, string(b))
	}
	return resp, nil
}
