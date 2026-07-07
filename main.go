package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	var store Store
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL != "" {
		pgStore, err := NewPostgresStore(databaseURL)
		if err != nil {
			log.Fatalf("failed to connect to postgres: %v", err)
		}
		store = pgStore
		log.Println("✅ Using PostgreSQL store")
	} else {
		store = NewInMemoryStore()
		log.Println("⚠️  DATABASE_URL is not set. Using in-memory persistence only.")
	}

	queue := NewQueue(store, 5)

	queue.RegisterHandler("email", func(ctx context.Context, job *Job) error {
		log.Printf("📧 Sending email to %s", job.Payload["to"])
		select {
		case <-time.After(time.Duration(rand.Intn(2000)) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	queue.RegisterHandler("webhook", func(ctx context.Context, job *Job) error {
		log.Printf("🌐 Firing webhook to %s", job.Payload["url"])
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
		log.Printf("📊 Generating report: %s", job.Payload["report_name"])
		select {
		case <-time.After(3 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	queue.Start()

	handlers := &Handlers{queue: queue, store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/jobs/", handlers.jobHandler)
	mux.HandleFunc("/jobs", handlers.jobsHandler)
	mux.HandleFunc("/stats", handlers.getStats)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		log.Println("⏳ shutdown signal received")

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("server shutdown error: %v", err)
		}
		queue.Stop()
	}()

	log.Println("⚡ GoQueue running on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}

	log.Println("GoQueue stopped")
}
