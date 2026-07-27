package api

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/yourname/queueline/internal/observability"
	"github.com/yourname/queueline/internal/queue"
)

func NewRouter(engine *queue.Engine, pool *pgxpool.Pool, log *zap.Logger, defaultLeaseTTL, maxLeaseTTL time.Duration) http.Handler {
	h := NewHandlers(engine, log, defaultLeaseTTL, maxLeaseTTL)

	r := chi.NewRouter()
	r.Use(RecoverMiddleware(log))
	r.Use(LoggingMiddleware(log))

	// Liveness: is the process up. Readiness: is it up AND can it reach
	// Postgres. An orchestrator should stop routing traffic to an
	// instance that fails readiness without necessarily restarting it.
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, map[string]string{"status": "ok"}) })
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "database is not reachable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Handle("/metrics", observability.MetricsHandler())

	r.Route("/v1", func(r chi.Router) {
		r.Route("/queues/{queue}", func(r chi.Router) {
			r.Post("/jobs", h.Enqueue)
			r.Post("/lease", h.Lease)
			r.Get("/stats", h.Stats)
			r.Get("/dead-letters", h.ListDeadLetters)
		})
		r.Route("/jobs/{id}", func(r chi.Router) {
			r.Get("/", h.GetJob)
			r.Post("/heartbeat", h.Heartbeat)
			r.Post("/complete", h.Complete)
			r.Post("/fail", h.Fail)
		})
		r.Post("/dead-letters/{id}/requeue", h.RequeueDeadLetter)
	})

	return r
}
