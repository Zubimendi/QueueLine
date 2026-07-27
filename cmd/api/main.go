// cmd/api is QueueLine's HTTP server. Stateless - all coordination state
// (which job is claimed by whom, lease expiry) lives in Postgres, so any
// number of replicas can run behind a load balancer with zero
// coordination between them.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/yourname/queueline/internal/api"
	"github.com/yourname/queueline/internal/config"
	"github.com/yourname/queueline/internal/db"
	"github.com/yourname/queueline/internal/observability"
	"github.com/yourname/queueline/internal/queue"
)

func main() {
	log := observability.NewLogger()
	defer log.Sync()

	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	cancel()
	if err != nil {
		log.Fatal("db connect failed", zap.Error(err))
	}
	defer pool.Close()

	engine := queue.NewEngine(pool)
	router := api.NewRouter(engine, pool, log, cfg.DefaultLeaseTTL, cfg.MaxLeaseTTL)

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Info("queueline api starting", zap.String("port", cfg.HTTPPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", zap.Error(err))
	}
}
