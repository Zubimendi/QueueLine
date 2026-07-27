package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queueline_jobs_enqueued_total",
		Help: "Jobs enqueued, by queue.",
	}, []string{"queue"})

	JobsClaimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queueline_jobs_claimed_total",
		Help: "Jobs claimed by a worker, by queue.",
	}, []string{"queue"})

	JobOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queueline_job_outcomes_total",
		Help: "Job outcomes, by queue and result (completed, failed, dead_lettered).",
	}, []string{"queue", "outcome"})

	JobsReaped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queueline_jobs_reaped_total",
		Help: "Jobs reclaimed by the reaper after an expired lease.",
	}, []string{"queue"})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queueline_queue_depth",
		Help: "Current PENDING job count, by queue.",
	}, []string{"queue"})
)

func NewLogger() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	return logger
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
