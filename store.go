package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type JobFilter struct {
	Type   string
	Status *JobStatus
}

type Store interface {
	Save(job *Job) error
	Update(job *Job) error
	Get(id string) (*Job, error)
	List(filter JobFilter) ([]*Job, error)
	Cancel(id string) error
	Stats() (map[string]int, error)
}

type InMemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{jobs: make(map[string]*Job)}
}

func (s *InMemoryStore) Save(job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	return nil
}

func (s *InMemoryStore) Update(job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[job.ID]; !exists {
		return fmt.Errorf("job %s not found", job.ID)
	}
	s.jobs[job.ID] = job
	return nil
}

func (s *InMemoryStore) Get(id string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[id]
	if !exists {
		return nil, fmt.Errorf("job %s not found", id)
	}
	return job, nil
}

func (s *InMemoryStore) List(filter JobFilter) ([]*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		if filter.Type != "" && job.Type != filter.Type {
			continue
		}
		if filter.Status != nil && job.Status != *filter.Status {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (s *InMemoryStore) Cancel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, exists := s.jobs[id]
	if !exists {
		return fmt.Errorf("job %s not found", id)
	}
	if job.Status != StatusPending {
		return fmt.Errorf("can only cancel pending jobs, current status: %s", job.Status)
	}
	job.Status = StatusCancelled
	s.jobs[id] = job
	return nil
}

func (s *InMemoryStore) Stats() (map[string]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats := map[string]int{
		StatusPending.String():   0,
		StatusRunning.String():   0,
		StatusDone.String():      0,
		StatusFailed.String():    0,
		StatusCancelled.String(): 0,
	}
	for _, job := range s.jobs {
		stats[job.Status.String()]++
	}
	return stats, nil
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(dsn string) (Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	store := &PostgresStore{db: db}
	if err := store.ensureSchema(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *PostgresStore) ensureSchema() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NULL,
    priority INT NOT NULL,
    status INT NOT NULL,
    retries INT NOT NULL,
    max_retries INT NOT NULL,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NULL,
    finished_at TIMESTAMPTZ NULL
)
`)
	return err
}

func (s *PostgresStore) Save(job *Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`
INSERT INTO jobs (
    id, type, payload, priority, status, retries, max_retries, error, created_at, started_at, finished_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
) ON CONFLICT (id) DO UPDATE SET
    type = EXCLUDED.type,
    payload = EXCLUDED.payload,
    priority = EXCLUDED.priority,
    status = EXCLUDED.status,
    retries = EXCLUDED.retries,
    max_retries = EXCLUDED.max_retries,
    error = EXCLUDED.error,
    created_at = EXCLUDED.created_at,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at
`, job.ID, job.Type, payload, int(job.Priority), int(job.Status), job.Retries, job.MaxRetries, job.Error, job.CreatedAt, nullTime(job.StartedAt), nullTime(job.FinishedAt))
	return err
}

func nullTime(value *time.Time) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func (s *PostgresStore) Update(job *Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}

	result, err := s.db.Exec(`
UPDATE jobs SET
    type = $2,
    payload = $3,
    priority = $4,
    status = $5,
    retries = $6,
    max_retries = $7,
    error = $8,
    created_at = $9,
    started_at = $10,
    finished_at = $11
WHERE id = $1
`, job.ID, job.Type, payload, int(job.Priority), int(job.Status), job.Retries, job.MaxRetries, job.Error, job.CreatedAt, nullTime(job.StartedAt), nullTime(job.FinishedAt))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("job %s not found", job.ID)
	}
	return nil
}

func (s *PostgresStore) Get(id string) (*Job, error) {
	row := s.db.QueryRow(`
SELECT id, type, payload, priority, status, retries, max_retries, error, created_at, started_at, finished_at
FROM jobs
WHERE id = $1
`, id)
	return s.scanJob(row)
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func (s *PostgresStore) scanJob(scanner rowScanner) (*Job, error) {
	var payloadBytes []byte
	var errorText sql.NullString
	var startedAt sql.NullTime
	var finishedAt sql.NullTime
	var priority int
	var status int

	job := &Job{}
	if err := scanner.Scan(
		&job.ID,
		&job.Type,
		&payloadBytes,
		&priority,
		&status,
		&job.Retries,
		&job.MaxRetries,
		&errorText,
		&job.CreatedAt,
		&startedAt,
		&finishedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job %s not found", job.ID)
		}
		return nil, err
	}

	job.Priority = JobPriority(priority)
	job.Status = JobStatus(status)
	if errorText.Valid {
		job.Error = errorText.String
	}
	if len(payloadBytes) > 0 {
		_ = json.Unmarshal(payloadBytes, &job.Payload)
	}
	if job.Payload == nil {
		job.Payload = map[string]string{}
	}
	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		job.FinishedAt = &finishedAt.Time
	}

	return job, nil
}

func (s *PostgresStore) List(filter JobFilter) ([]*Job, error) {
	query := `
SELECT id, type, payload, priority, status, retries, max_retries, error, created_at, started_at, finished_at
FROM jobs`

	var conditions []string
	var args []interface{}

	if filter.Type != "" {
		conditions = append(conditions, fmt.Sprintf("type = $%d", len(args)+1))
		args = append(args, filter.Type)
	}
	if filter.Status != nil {
		conditions = append(conditions, fmt.Sprintf("status = $%d", len(args)+1))
		args = append(args, int(*filter.Status))
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*Job, 0)
	for rows.Next() {
		job, err := s.scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *PostgresStore) Cancel(id string) error {
	result, err := s.db.Exec(`
UPDATE jobs SET status = $1
WHERE id = $2 AND status = $3
`, int(StatusCancelled), id, int(StatusPending))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("job %s not found or cannot be cancelled", id)
	}
	return nil
}

func (s *PostgresStore) Stats() (map[string]int, error) {
	rows, err := s.db.Query(`
SELECT status, count(*)
FROM jobs
GROUP BY status
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := map[string]int{
		StatusPending.String():   0,
		StatusRunning.String():   0,
		StatusDone.String():      0,
		StatusFailed.String():    0,
		StatusCancelled.String(): 0,
	}

	for rows.Next() {
		var status int
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats[JobStatus(status).String()] = count
	}
	return stats, rows.Err()
}
