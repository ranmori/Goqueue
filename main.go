package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	var store Store
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL != "" {
		pgStore, err := NewPostgresStore(databaseURL)
		if err != nil {
			logger.Error("failed to connect to postgres", "error", err)
			os.Exit(1)
		}
		store = pgStore
		logger.Info("using PostgreSQL store")
	} else {
		store = NewInMemoryStore()
		logger.Warn("DATABASE_URL is not set, using in-memory persistence only")
	}

	queue := NewQueue(store, 5)
	queue.SetLogger(logger)

	// Example: cap "email" at 10 concurrent sends regardless of worker
	// count, so a burst of emails can't starve webhook/report jobs.
	queue.SetConcurrencyLimit("email", 10)

	queue.RegisterHandler("email", func(ctx context.Context, job *Job) error {
		logger.Info("sending email", "job_id", job.ID, "to", job.Payload["to"])
		select {
		case <-time.After(time.Duration(rand.Intn(2000)) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	queue.RegisterHandler("webhook", func(ctx context.Context, job *Job) error {
		logger.Info("firing webhook", "job_id", job.ID, "url", job.Payload["url"])
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		if rand.Float32() < 0.3 {
			return fmt.Errorf("webhook endpoint returned 500")
		}
		return nil
	})

	queue.RegisterHandler("report", func(ctx context.Context, job *Job) error {
		logger.Info("generating report", "job_id", job.ID, "report_name", job.Payload["report_name"])
		select {
		case <-time.After(3 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	scheduler := NewScheduler(queue.release, 200*time.Millisecond)
	queue.SetScheduler(scheduler)
	scheduler.Start()

	queue.Start()

	handlers := &Handlers{queue: queue, store: store, apiKey: os.Getenv("API_KEY")}
	if handlers.apiKey == "" {
		logger.Warn("API_KEY is not set — write endpoints (POST /jobs, DELETE /jobs/{id}, POST /dlq/{id}/replay) are unauthenticated")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", handlers.rootHandler)
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/jobs/", handlers.jobHandler)
	mux.HandleFunc("/jobs", handlers.jobsHandler)
	mux.HandleFunc("/stats", handlers.getStats)
	mux.HandleFunc("/dlq", handlers.dlqHandler)
	mux.HandleFunc("/dlq/", handlers.dlqReplayHandler)
	mux.HandleFunc("/metrics", queue.Metrics().ServeHTTP)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		logger.Info("shutdown signal received")

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			logger.Error("server shutdown error", "error", err)
		}
		scheduler.Stop()
		queue.Stop()
	}()

	logger.Info("GoQueue running", "addr", ":8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}

	logger.Info("GoQueue stopped")
}
