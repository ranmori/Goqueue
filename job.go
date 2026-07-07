package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

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
}

func NewJob(jobType string, payload map[string]string, priority JobPriority, maxRetries int) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:         fmt.Sprintf("job_%d", now.UnixNano()),
		Type:       jobType,
		Payload:    payload,
		Priority:   priority,
		Status:     StatusPending,
		MaxRetries: maxRetries,
		CreatedAt:  now,
	}
}
