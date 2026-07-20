package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEnqueueRejectedWithoutAPIKey(t *testing.T) {
	q := newTestQueue(t)
	h := &Handlers{queue: q, store: q.store, apiKey: "secret123"}

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"noop"}`))
	rec := httptest.NewRecorder()
	h.enqueueJob(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no API key, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEnqueueSucceedsWithCorrectAPIKey(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("noop", func(ctx context.Context, job *Job) error { return nil })
	h := &Handlers{queue: q, store: q.store, apiKey: "secret123"}

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"noop"}`))
	req.Header.Set("X-API-Key", "secret123")
	rec := httptest.NewRecorder()
	h.enqueueJob(rec, req)

	if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("expected success with correct API key, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestReadEndpointsWorkWithoutAPIKey(t *testing.T) {
	q := newTestQueue(t)
	h := &Handlers{queue: q, store: q.store, apiKey: "secret123"}

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()
	h.listJobs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected GET /jobs to work without a key, got %d", rec.Code)
	}
}

func TestNoAPIKeyConfiguredAllowsAllRequests(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("noop", func(ctx context.Context, job *Job) error { return nil })
	h := &Handlers{queue: q, store: q.store, apiKey: ""}

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"noop"}`))
	rec := httptest.NewRecorder()
	h.enqueueJob(rec, req)

	if rec.Code == http.StatusUnauthorized {
		t.Fatal("expected requests to be allowed when no API key is configured")
	}
}
