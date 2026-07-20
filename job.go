package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func generateID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("job_%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:])
}

type JobStatus int

const (
	StatusPending JobStatus = iota
	StatusRunning
	StatusDone
	StatusFailed
	StatusCancelled
)

func (s JobStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusDone:
		return "done"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

func (s JobStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func ParseJobStatus(value string) (JobStatus, error) {
	switch strings.ToLower(value) {
	case "pending":
		return StatusPending, nil
	case "running":
		return StatusRunning, nil
	case "done":
		return StatusDone, nil
	case "failed":
		return StatusFailed, nil
	case "cancelled":
		return StatusCancelled, nil
	default:
		return StatusPending, fmt.Errorf("unknown status: %s", value)
	}
}

type JobPriority int

const (
	PriorityLow    JobPriority = 0
	PriorityNormal JobPriority = 5
	PriorityHigh   JobPriority = 10
)

func (p JobPriority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	default:
		return "unknown"
	}
}

type Job struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Payload    map[string]string `json:"payload"`
	Priority   JobPriority       `json:"priority"`
	Status     JobStatus         `json:"status"`
	Retries    int               `json:"retries"`
	MaxRetries int               `json:"max_retries"`
	Error      string            `json:"error,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	StartedAt  *time.Time        `json:"started_at,omitempty"`
	FinishedAt *time.Time        `json:"finished_at,omitempty"`

	// RunAt schedules the job for future dispatch. Zero value means
	// "eligible immediately". Jobs with a future RunAt are held by the
	// Scheduler and only handed to a worker once that time has passed.
	RunAt time.Time `json:"run_at,omitempty"`

	// Timeout bounds how long a single execution attempt may run before
	// it is cancelled and treated as a failed attempt (subject to the
	// same retry/backoff policy as any other handler error). Zero means
	// no per-attempt deadline is enforced.
	Timeout time.Duration `json:"timeout_ns,omitempty"`

	// DedupeKey, if set, suppresses new enqueues carrying the same key
	// while a prior job with that key is still within DedupeWindow of
	// its own enqueue time. Empty key means no deduplication.
	DedupeKey    string        `json:"dedupe_key,omitempty"`
	DedupeWindow time.Duration `json:"dedupe_window_ns,omitempty"`

	// WebhookURL, if set, receives a POST with the job's final status
	// once it reaches Done or Failed. Best-effort — delivery failures
	// are logged, not retried, and never affect the job's own status.
	WebhookURL string `json:"webhook_url,omitempty"`

	// OnSuccess/OnFailure describe a follow-up job to enqueue once this
	// job finishes. Only one fires per job, matching its actual outcome.
	OnSuccess *JobTemplate `json:"on_success,omitempty"`
	OnFailure *JobTemplate `json:"on_failure,omitempty"`
}

// JobTemplate is a lightweight description of a job to enqueue later —
// used for chaining (Job.OnSuccess / Job.OnFailure) instead of embedding
// a full *Job, since a template has no ID, status, or timestamps of its
// own until it's actually turned into one.
type JobTemplate struct {
	Type       string            `json:"type"`
	Payload    map[string]string `json:"payload"`
	Priority   JobPriority       `json:"priority"`
	MaxRetries int               `json:"max_retries"`
}

// ToJob turns a template into a freshly-minted, enqueue-ready Job.
func (t *JobTemplate) ToJob() *Job {
	return NewJob(t.Type, t.Payload, t.Priority, t.MaxRetries)
}

func NewJob(jobType string, payload map[string]string, priority JobPriority, maxRetries int) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:         generateID(),
		Type:       jobType,
		Payload:    payload,
		Priority:   priority,
		Status:     StatusPending,
		MaxRetries: maxRetries,
		CreatedAt:  now,
	}
}

// IsDue reports whether the job's scheduled time has arrived (or it was
// never delayed in the first place).
func (j *Job) IsDue(now time.Time) bool {
	return j.RunAt.IsZero() || !j.RunAt.After(now)
}
