package api

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Structured request logging, independent of any framework default -
// same rationale as Dispatcher and SlotForge: consistent, greppable
// request logs across every project in this portfolio, not whatever a
// framework happens to print by default.
func LoggingMiddleware(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: 200}
			next.ServeHTTP(sw, r)
			log.Info("request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", sw.status),
				zap.Duration("duration", time.Since(start)),
			)
		})
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// RecoverMiddleware ensures a panic in one handler (e.g. a malformed
// payload tripping an unexpected nil dereference) returns a clean 500
// instead of crashing the whole process - "a system that cannot recover
// gracefully from failure isn't production-ready" applies at the level
// of a single request too, not just whole-service crashes.
func RecoverMiddleware(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic recovered", zap.Any("panic", rec), zap.String("path", r.URL.Path))
					writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
