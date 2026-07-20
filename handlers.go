package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "ok"})
}

func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}

type Handlers struct {
	queue  *Queue
	store  Store
	apiKey string // empty means no auth required (e.g. local dev)
}

// authorized checks X-API-Key for mutating endpoints. Returns false (and
// has already written the 401 response) if the request should be
// rejected — callers should return immediately when it does.
func (h *Handlers) authorized(w http.ResponseWriter, r *http.Request) bool {
	if h.apiKey == "" {
		return true
	}
	if r.Header.Get("X-API-Key") != h.apiKey {
		respondError(w, http.StatusUnauthorized, "missing or invalid X-API-Key header")
		return false
	}
	return true
}

type enqueueRequest struct {
	Type       string            `json:"type"`
	Payload    map[string]string `json:"payload"`
	Priority   JobPriority       `json:"priority"`
	MaxRetries int               `json:"max_retries"`

	// RunAt, if set, delays dispatch until this time (RFC3339). Omitted
	// or in the past means "run as soon as a worker is free".
	RunAt *time.Time `json:"run_at,omitempty"`

	// TimeoutSeconds bounds a single execution attempt. 0 means no
	// per-attempt deadline.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`

	// DedupeKey/DedupeWindowSeconds suppress accepting another job with
	// the same key while one is still within its window. Window 0 with
	// a non-empty key falls back to a 1-minute default (see deduper).
	DedupeKey           string `json:"dedupe_key,omitempty"`
	DedupeWindowSeconds int    `json:"dedupe_window_seconds,omitempty"`

	WebhookURL string       `json:"webhook_url,omitempty"`
	OnSuccess  *JobTemplate `json:"on_success,omitempty"`
	OnFailure  *JobTemplate `json:"on_failure,omitempty"`
}

func (h *Handlers) jobsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.enqueueJob(w, r)
	case http.MethodGet:
		h.listJobs(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handlers) jobHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" || strings.Contains(id, "/") {
		respondError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getJob(w, r, id)
	case http.MethodDelete:
		h.cancelJob(w, r, id)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handlers) enqueueJob(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(w, r) {
		return
	}
	var body enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.Type == "" {
		respondError(w, http.StatusBadRequest, "job type is required")
		return
	}
	if body.MaxRetries < 0 {
		respondError(w, http.StatusBadRequest, "max_retries must be 0 or greater")
		return
	}
	if body.TimeoutSeconds < 0 {
		respondError(w, http.StatusBadRequest, "timeout_seconds must be 0 or greater")
		return
	}

	job := &Job{
		ID:         fmt.Sprintf("job_%d", time.Now().UnixNano()),
		Type:       body.Type,
		Payload:    body.Payload,
		Priority:   body.Priority,
		Status:     StatusPending,
		MaxRetries: body.MaxRetries,
		CreatedAt:  time.Now().UTC(),
	}
	if body.RunAt != nil {
		job.RunAt = body.RunAt.UTC()
	}
	if body.TimeoutSeconds > 0 {
		job.Timeout = time.Duration(body.TimeoutSeconds) * time.Second
	}
	job.DedupeKey = body.DedupeKey
	if body.DedupeWindowSeconds > 0 {
		job.DedupeWindow = time.Duration(body.DedupeWindowSeconds) * time.Second
	}
	job.WebhookURL = body.WebhookURL
	job.OnSuccess = body.OnSuccess
	job.OnFailure = body.OnFailure

	if err := h.queue.Enqueue(job); err != nil {
		if errors.Is(err, ErrDuplicateJob) {
			respondError(w, http.StatusConflict, err.Error())
			return
		}
		respondError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	respond(w, http.StatusCreated, job)
}

func (h *Handlers) getJob(w http.ResponseWriter, r *http.Request, id string) {
	job, err := h.store.Get(id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, job)
}

func (h *Handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	filter := JobFilter{}
	query := r.URL.Query()

	if jobType := query.Get("type"); jobType != "" {
		filter.Type = jobType
	}
	if statusParam := query.Get("status"); statusParam != "" {
		status, err := ParseJobStatus(statusParam)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		filter.Status = &status
	}
	if limitParam := query.Get("limit"); limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err != nil || limit < 0 {
			respondError(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		filter.Limit = limit
	}
	if offsetParam := query.Get("offset"); offsetParam != "" {
		offset, err := strconv.Atoi(offsetParam)
		if err != nil || offset < 0 {
			respondError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		filter.Offset = offset
	}

	jobs, err := h.store.List(filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respond(w, http.StatusOK, jobs)
}

func (h *Handlers) rootHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
		return
	}
	respond(w, http.StatusOK, map[string]any{
		"service": "GoQueue",
		"description": "A background job processor with priority queues, retries, " +
			"scheduling, a dead-letter queue, rate limiting, deduplication, job " +
			"chaining, webhooks, and Prometheus metrics.",
		"endpoints": map[string]string{
			"GET  /health":          "liveness check",
			"POST /jobs":            "enqueue a job (requires X-API-Key if configured)",
			"GET  /jobs":            "list jobs (?type=&status=&limit=&offset=)",
			"GET  /jobs/{id}":       "get a single job",
			"DELETE /jobs/{id}":     "cancel a pending job (requires X-API-Key if configured)",
			"GET  /stats":           "job counts by status",
			"GET  /dlq":             "list dead-lettered jobs",
			"POST /dlq/{id}/replay": "re-enqueue a dead-lettered job (requires X-API-Key if configured)",
			"GET  /metrics":         "Prometheus-format metrics",
		},
		"source": "https://github.com/ranmori/Goqueue",
	})
}

func (h *Handlers) cancelJob(w http.ResponseWriter, r *http.Request, id string) {
	if !h.authorized(w, r) {
		return
	}
	if err := h.store.Cancel(id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (h *Handlers) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.store.Stats()
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respond(w, http.StatusOK, stats)
}

func (h *Handlers) dlqHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	entries, err := h.store.ListDeadLetters()
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respond(w, http.StatusOK, entries)
}

// dlqReplayHandler handles POST /dlq/{id}/replay — it reconstructs a
// fresh job from the dead letter's original type/payload/priority,
// re-enqueues it, and removes the dead letter entry on success. The
// dead letter is left in place if re-enqueueing fails, so nothing is
// lost if e.g. the queue happens to be full at that moment.
func (h *Handlers) dlqReplayHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !h.authorized(w, r) {
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/dlq/")
	id = strings.TrimSuffix(id, "/replay")
	if id == "" {
		respondError(w, http.StatusBadRequest, "invalid dead letter id")
		return
	}

	entries, err := h.store.ListDeadLetters()
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var entry *DeadLetter
	for _, e := range entries {
		if e.ID == id {
			entry = e
			break
		}
	}
	if entry == nil {
		respondError(w, http.StatusNotFound, fmt.Sprintf("dead letter %s not found", id))
		return
	}

	job := NewJob(entry.Type, entry.Payload, entry.Priority, entry.MaxRetries)
	if err := h.queue.Enqueue(job); err != nil {
		respondError(w, http.StatusServiceUnavailable, fmt.Sprintf("replay failed, dead letter kept: %v", err))
		return
	}
	if err := h.store.DeleteDeadLetter(entry.ID); err != nil {
		// Job is already back in the queue at this point; a failed
		// cleanup just means the DLQ entry lingers, not a lost job.
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("job replayed as %s but failed to clear dead letter: %v", job.ID, err))
		return
	}

	respond(w, http.StatusOK, job)
}
