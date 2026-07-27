// QueueLine's REST API. This is what makes the queue usable from any
// language - a Python or NestJS project in a later week doesn't need a
// Go client, it just needs an HTTP client, which is why this exists
// alongside pkg/client (the Go-native SDK for Go-based projects).
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/yourname/queueline/internal/observability"
	"github.com/yourname/queueline/internal/queue"
)

type Handlers struct {
	engine          *queue.Engine
	log             *zap.Logger
	defaultLeaseTTL time.Duration
	maxLeaseTTL     time.Duration
}

func NewHandlers(engine *queue.Engine, log *zap.Logger, defaultLeaseTTL, maxLeaseTTL time.Duration) *Handlers {
	return &Handlers{engine: engine, log: log, defaultLeaseTTL: defaultLeaseTTL, maxLeaseTTL: maxLeaseTTL}
}

func (h *Handlers) Enqueue(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "queue")
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_BODY", "request body must be valid JSON")
		return
	}
	if len(req.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "MISSING_PAYLOAD", "payload is required")
		return
	}
	if req.DelaySec < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_DELAY", "delaySeconds must not be negative")
		return
	}

	job, err := h.engine.Enqueue(r.Context(), queue.EnqueueInput{
		Queue: queueName, Payload: req.Payload, Priority: req.Priority,
		DelaySec: req.DelaySec, MaxAttempts: req.MaxAttempts, DedupKey: req.DedupKey,
	})
	if err != nil && err != queue.ErrDuplicate {
		h.log.Error("enqueue failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to enqueue job")
		return
	}

	observability.JobsEnqueued.WithLabelValues(queueName).Inc()
	status := http.StatusCreated
	if err == queue.ErrDuplicate {
		status = http.StatusOK // idempotent replay, not a new creation
	}
	writeJSON(w, status, job)
}

func (h *Handlers) Lease(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "queue")
	var req leaseRequest
	_ = json.NewDecoder(r.Body).Decode(&req) // empty body is valid; falls back to default TTL

	ttl := h.defaultLeaseTTL
	if req.LeaseSeconds > 0 {
		ttl = time.Duration(req.LeaseSeconds) * time.Second
		if ttl > h.maxLeaseTTL {
			writeError(w, http.StatusBadRequest, "LEASE_TOO_LONG", "leaseSeconds exceeds the configured maximum")
			return
		}
	}

	job, err := h.engine.Claim(r.Context(), queueName, ttl)
	if err != nil {
		h.log.Error("claim failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to claim a job")
		return
	}
	if job == nil {
		w.WriteHeader(http.StatusNoContent) // queue empty - normal, not an error
		return
	}

	observability.JobsClaimed.WithLabelValues(queueName).Inc()
	writeJSON(w, http.StatusOK, job)
}

func (h *Handlers) Heartbeat(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_LEASE_ID", "leaseId is required")
		return
	}
	extend := h.defaultLeaseTTL
	if req.ExtendSeconds > 0 {
		extend = time.Duration(req.ExtendSeconds) * time.Second
	}

	if err := h.engine.Heartbeat(r.Context(), jobID, req.LeaseID, extend); err != nil {
		h.respondLeaseError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) Complete(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	var req completeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_LEASE_ID", "leaseId is required")
		return
	}

	if err := h.engine.Complete(r.Context(), jobID, req.LeaseID); err != nil {
		h.respondLeaseError(w, err)
		return
	}
	// Note: JobOutcomes isn't incremented here because this route only
	// has the job ID, not its queue name, and the metric is labeled by
	// queue - see docs/CURSOR_CONTEXT.md for the documented follow-up
	// (have Complete/Fail return the job's queue so this can be fixed
	// without an extra query on the hot path).
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) Fail(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	var req failRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_LEASE_ID", "leaseId is required")
		return
	}
	if req.Error == "" {
		req.Error = "worker reported failure with no message"
	}

	if err := h.engine.Fail(r.Context(), jobID, req.LeaseID, req.Error); err != nil {
		h.respondLeaseError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	job, err := h.engine.GetJob(r.Context(), jobID)
	if err != nil {
		if err == queue.ErrNotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handlers) Stats(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "queue")
	stats, err := h.engine.Stats(r.Context(), queueName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load stats")
		return
	}
	observability.QueueDepth.WithLabelValues(queueName).Set(float64(stats.Pending))
	writeJSON(w, http.StatusOK, stats)
}

func (h *Handlers) ListDeadLetters(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "queue")
	items, err := h.engine.ListDeadLetters(r.Context(), queueName, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list dead letters")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handlers) RequeueDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := h.engine.RequeueDeadLetter(r.Context(), id)
	if err != nil {
		if err == queue.ErrNotFound {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "dead letter job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to requeue")
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (h *Handlers) respondLeaseError(w http.ResponseWriter, err error) {
	if err == queue.ErrLeaseMismatch {
		// 409, not 500 or 404: the request was well-formed, but the
		// world moved on underneath it - the correct signal for "your
		// lease was reclaimed, stop working on this job."
		writeError(w, http.StatusConflict, "LEASE_MISMATCH", err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: message, Code: code})
}
