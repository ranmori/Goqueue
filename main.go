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

	handlers := &Handlers{queue: queue, store: store, apiKey: os.Getenv("API_KEY")}

	// With ETCD_ENDPOINTS set, several instances share one Postgres and
	// elect a single dispatcher through etcd. Without it, this process is
	// the only one and dispatches in-memory exactly as before.
	var node *haNode
	var scheduler *Scheduler
	if endpoints := splitEndpoints(os.Getenv("ETCD_ENDPOINTS")); len(endpoints) > 0 {
		claimStore, ok := store.(ClaimStore)
		if !ok {
			logger.Error("ETCD_ENDPOINTS is set but the store can't be shared between instances; set DATABASE_URL")
			os.Exit(1)
		}
		queue.UseStoreDispatch()
		elector, err := NewElector(LeaderConfig{
			Endpoints: endpoints,
			ID:        instanceID(),
			TTL:       envInt("ETCD_SESSION_TTL", 5),
		}, logger)
		if err != nil {
			logger.Error("failed to join leader election", "error", err)
			os.Exit(1)
		}
		node = &haNode{elector: elector, queue: queue, store: claimStore, log: logger}
		handlers.leader = elector.Status
		queue.Metrics().SetLeaderFunc(elector.IsLeader)
	} else {
		scheduler = NewScheduler(queue.release, 200*time.Millisecond)
		queue.SetScheduler(scheduler)
		scheduler.Start()
		queue.Start()
	}

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
	mux.HandleFunc("/leader", handlers.leaderHandler)

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("GoQueue running", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	haCtx, haCancel := context.WithCancel(context.Background())
	defer haCancel()
	haDone := make(chan error, 1)
	if node != nil {
		go func() { haDone <- node.run(haCtx) }()
	}

	select {
	case sig := <-stop:
		logger.Info("shutdown signal received", "signal", sig.String())
	case err := <-serverErr:
		logger.Error("server error", "error", err)
		os.Exit(1)
	case err := <-haDone:
		// run only returns before shutdown if leadership or the etcd
		// session was lost. Exit rather than demote in place; the
		// supervisor restarts us and we rejoin as a fresh follower.
		logger.Error("leader election failed, exiting", "error", err)
		os.Exit(1)
	}

	if node != nil {
		// Stop claiming first, then finish in-flight work, then resign —
		// in that order, so the next leader never sees a job we're still
		// running as orphaned unless the grace period runs out.
		haCancel()
		<-haDone
		node.shutdown(10 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}
	if scheduler != nil {
		scheduler.Stop()
		queue.Stop()
	}

	logger.Info("GoQueue stopped")
}
