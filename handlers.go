package main

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	queue *Queue
	store Store
}

type enqueueRequest struct {
	Type       string            `json:"type"`
	Payload    map[string]string `json:"payload"`
	Priority   JobPriority       `json:"priority"`
	MaxRetries int               `json:"max_retries"`
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

	job := &Job{
		ID:         fmt.Sprintf("job_%d", time.Now().UnixNano()),
		Type:       body.Type,
		Payload:    body.Payload,
		Priority:   body.Priority,
		Status:     StatusPending,
		MaxRetries: body.MaxRetries,
		CreatedAt:  time.Now().UTC(),
	}

	if err := h.queue.Enqueue(job); err != nil {
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

	jobs, err := h.store.List(filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respond(w, http.StatusOK, jobs)
}

func (h *Handlers) cancelJob(w http.ResponseWriter, r *http.Request, id string) {
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
