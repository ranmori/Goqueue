package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type JobFilter struct {
	Type   string
	Status *JobStatus

	// Limit/Offset paginate List results. Limit <= 0 means "no limit".
	Limit  int
	Offset int
}

// DeadLetter records a job that exhausted its retries. Kept separately
// from the main jobs table/map so a growing backlog of permanent
// failures doesn't clutter normal job listings, while still being fully
// inspectable and replayable.
type DeadLetter struct {
	ID                  string            `json:"id"`
	JobID               string            `json:"job_id"`
	Type                string            `json:"type"`
	Payload             map[string]string `json:"payload"`
	Priority            JobPriority       `json:"priority"`
	MaxRetries          int               `json:"max_retries"`
	Error               string            `json:"error"`
	FailedAt            time.Time         `json:"failed_at"`
	OriginallyCreatedAt time.Time         `json:"originally_created_at"`
}

type Store interface {
	Save(job *Job) error
	Update(job *Job) error
	Get(id string) (*Job, error)
	List(filter JobFilter) ([]*Job, error)
	Cancel(id string) error
	Stats() (map[string]int, error)

	SaveDeadLetter(entry *DeadLetter) error
	GetDeadLetter(id string) (*DeadLetter, error)
	ListDeadLetters() ([]*DeadLetter, error)
	DeleteDeadLetter(id string) error
}

type InMemoryStore struct {
	mu          sync.RWMutex
	jobs        map[string]*Job
	deadLetters map[string]*DeadLetter
	dlqSeq      int64
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		jobs:        make(map[string]*Job),
		deadLetters: make(map[string]*DeadLetter),
	}
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

	// Match PostgresStore's ORDER BY created_at DESC so pagination behaves
	// the same regardless of which Store implementation is in use.
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})

	if filter.Offset > 0 {
		if filter.Offset >= len(jobs) {
			return []*Job{}, nil
		}
		jobs = jobs[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(jobs) {
		jobs = jobs[:filter.Limit]
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

func (s *InMemoryStore) SaveDeadLetter(entry *DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlqSeq++
	if entry.ID == "" {
		entry.ID = fmt.Sprintf("dlq_%d", s.dlqSeq)
	}
	s.deadLetters[entry.ID] = entry
	return nil
}

func (s *InMemoryStore) GetDeadLetter(id string) (*DeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, exists := s.deadLetters[id]
	if !exists {
		return nil, fmt.Errorf("dead letter %s not found", id)
	}
	return entry, nil
}

func (s *InMemoryStore) ListDeadLetters() ([]*DeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*DeadLetter, 0, len(s.deadLetters))
	for _, d := range s.deadLetters {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FailedAt.After(out[j].FailedAt) })
	return out, nil
}

func (s *InMemoryStore) DeleteDeadLetter(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.deadLetters[id]; !exists {
		return fmt.Errorf("dead letter %s not found", id)
	}
	delete(s.deadLetters, id)
	return nil
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

// schemaLockKey is an arbitrary constant for pg_advisory_xact_lock.
const schemaLockKey = 0x676f7175657565 // "goqueue"

func (s *PostgresStore) ensureSchema() error {
	// Several instances starting at once (docker compose up) would race
	// on CREATE TABLE IF NOT EXISTS, which isn't concurrency-safe in
	// Postgres — the loser fails on pg_type's unique index. Serialise
	// migrations behind an advisory lock held for the transaction.
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT pg_advisory_xact_lock($1)`, int64(schemaLockKey)); err != nil {
		return err
	}
	if err := migrate(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func migrate(tx *sql.Tx) error {
	_, err := tx.Exec(`
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
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS dead_letters (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload JSONB NULL,
    priority INT NOT NULL,
    max_retries INT NOT NULL,
    error TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    originally_created_at TIMESTAMPTZ NOT NULL
)
`)
	if err != nil {
		return err
	}

	// Columns needed once jobs are dispatched from the store rather than
	// handed over in memory (leader election mode): the dispatcher only
	// sees what is persisted here. claimed_epoch records which leader
	// claimed a running job, for debugging stuck or duplicated work.
	_, err = tx.Exec(`
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS run_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS timeout_ns BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS webhook_url TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS on_success JSONB NULL,
    ADD COLUMN IF NOT EXISTS on_failure JSONB NULL,
    ADD COLUMN IF NOT EXISTS claimed_epoch BIGINT NOT NULL DEFAULT 0
`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`CREATE INDEX IF NOT EXISTS jobs_dispatch_idx ON jobs (status, priority DESC, created_at)`)
	if err != nil {
		return err
	}

	// leader_fence holds a single row: the epoch of the most recent
	// leader to take over. See TakeOver and ClaimDue.
	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS leader_fence (
    id INT PRIMARY KEY,
    epoch BIGINT NOT NULL
);
INSERT INTO leader_fence (id, epoch) VALUES (1, 0) ON CONFLICT (id) DO NOTHING
`)
	return err
}

const jobColumns = `id, type, payload, priority, status, retries, max_retries, error, created_at, started_at, finished_at,
    run_at, timeout_ns, webhook_url, on_success, on_failure`

// jobArgs returns the values for jobColumns, in order.
func jobArgs(job *Job) ([]interface{}, error) {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return nil, err
	}
	onSuccess, err := nullJSON(job.OnSuccess)
	if err != nil {
		return nil, err
	}
	onFailure, err := nullJSON(job.OnFailure)
	if err != nil {
		return nil, err
	}
	var runAt *time.Time
	if !job.RunAt.IsZero() {
		runAt = &job.RunAt
	}
	return []interface{}{
		job.ID, job.Type, payload, int(job.Priority), int(job.Status), job.Retries, job.MaxRetries, job.Error,
		job.CreatedAt, nullTime(job.StartedAt), nullTime(job.FinishedAt),
		nullTime(runAt), int64(job.Timeout), job.WebhookURL, onSuccess, onFailure,
	}, nil
}

func nullJSON(t *JobTemplate) (interface{}, error) {
	if t == nil {
		return nil, nil
	}
	return json.Marshal(t)
}

// TakeOver implements ClaimStore. Both statements run in one
// transaction, and the fence row stays locked until commit, so a stale
// leader's in-flight ClaimDue either commits first (and its claims are
// requeued here) or blocks and then sees the new epoch and claims nothing.
func (s *PostgresStore) TakeOver(ctx context.Context, epoch int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `UPDATE leader_fence SET epoch = $1 WHERE id = 1 AND epoch < $1`, epoch)
	if err != nil {
		return 0, err
	}
	if n, err := result.RowsAffected(); err != nil {
		return 0, err
	} else if n == 0 {
		return 0, ErrFenced
	}

	// Only the leader ever runs jobs, so anything still "running" belongs
	// to a previous leader that died or gave up before finishing it.
	result, err = tx.ExecContext(ctx, `
UPDATE jobs SET status = $1, started_at = NULL
WHERE status = $2
`, int(StatusPending), int(StatusRunning))
	if err != nil {
		return 0, err
	}
	requeued, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return requeued, tx.Commit()
}

// ClaimDue implements ClaimStore. FOR SHARE on the fence row is what
// serialises it against TakeOver; SKIP LOCKED only matters if two
// claimers ever overlap (e.g. a stale leader during the fence handoff).
func (s *PostgresStore) ClaimDue(ctx context.Context, epoch int64, limit int) ([]*Job, error) {
	rows, err := s.db.QueryContext(ctx, `
WITH fence AS (
    SELECT epoch FROM leader_fence WHERE id = 1 AND epoch = $1 FOR SHARE
), picked AS (
    SELECT j.id FROM jobs j
    WHERE j.status = $2
      AND (j.run_at IS NULL OR j.run_at <= now())
      AND EXISTS (SELECT 1 FROM fence)
    ORDER BY j.priority DESC, j.created_at
    LIMIT $3
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs SET status = $4, started_at = now(), claimed_epoch = $1
FROM picked
WHERE jobs.id = picked.id
RETURNING `+qualifiedJobColumns("jobs"), epoch, int(StatusPending), limit, int(StatusRunning))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*Job, 0, limit)
	for rows.Next() {
		job, err := s.scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// RETURNING order is unspecified; restore dispatch order.
	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Priority != jobs[j].Priority {
			return jobs[i].Priority > jobs[j].Priority
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})
	return jobs, nil
}

func qualifiedJobColumns(table string) string {
	cols := strings.Split(jobColumns, ",")
	for i, c := range cols {
		cols[i] = table + "." + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}

func (s *PostgresStore) Save(job *Job) error {
	args, err := jobArgs(job)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`
INSERT INTO jobs (`+jobColumns+`) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
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
    finished_at = EXCLUDED.finished_at,
    run_at = EXCLUDED.run_at,
    timeout_ns = EXCLUDED.timeout_ns,
    webhook_url = EXCLUDED.webhook_url,
    on_success = EXCLUDED.on_success,
    on_failure = EXCLUDED.on_failure
`, args...)
	return err
}

func nullTime(value *time.Time) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func (s *PostgresStore) Update(job *Job) error {
	args, err := jobArgs(job)
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
    finished_at = $11,
    run_at = $12,
    timeout_ns = $13,
    webhook_url = $14,
    on_success = $15,
    on_failure = $16
WHERE id = $1
`, args...)
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
	row := s.db.QueryRow(`SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
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
	var runAt sql.NullTime
	var timeoutNs int64
	var onSuccess, onFailure []byte

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
		&runAt,
		&timeoutNs,
		&job.WebhookURL,
		&onSuccess,
		&onFailure,
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
	if runAt.Valid {
		job.RunAt = runAt.Time.UTC()
	}
	job.Timeout = time.Duration(timeoutNs)
	if len(onSuccess) > 0 {
		_ = json.Unmarshal(onSuccess, &job.OnSuccess)
	}
	if len(onFailure) > 0 {
		_ = json.Unmarshal(onFailure, &job.OnFailure)
	}

	return job, nil
}

func (s *PostgresStore) List(filter JobFilter) ([]*Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs`

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

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, filter.Limit)
	}
	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", len(args)+1)
		args = append(args, filter.Offset)
	}

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

func (s *PostgresStore) SaveDeadLetter(entry *DeadLetter) error {
	payload, err := json.Marshal(entry.Payload)
	if err != nil {
		return err
	}
	if entry.ID == "" {
		entry.ID = fmt.Sprintf("dlq_%s", entry.JobID)
	}
	_, err = s.db.Exec(`
INSERT INTO dead_letters (
    id, job_id, type, payload, priority, max_retries, error, failed_at, originally_created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (id) DO NOTHING
`, entry.ID, entry.JobID, entry.Type, payload, int(entry.Priority), entry.MaxRetries, entry.Error, entry.FailedAt, entry.OriginallyCreatedAt)
	return err
}

func (s *PostgresStore) GetDeadLetter(id string) (*DeadLetter, error) {
	row := s.db.QueryRow(`
SELECT id, job_id, type, payload, priority, max_retries, error, failed_at, originally_created_at
FROM dead_letters
WHERE id = $1
`, id)
	var d DeadLetter
	var payloadBytes []byte
	var priority int
	if err := row.Scan(&d.ID, &d.JobID, &d.Type, &payloadBytes, &priority, &d.MaxRetries, &d.Error, &d.FailedAt, &d.OriginallyCreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("dead letter %s not found", id)
		}
		return nil, err
	}
	d.Priority = JobPriority(priority)
	if len(payloadBytes) > 0 {
		_ = json.Unmarshal(payloadBytes, &d.Payload)
	}
	return &d, nil
}

func (s *PostgresStore) ListDeadLetters() ([]*DeadLetter, error) {
	rows, err := s.db.Query(`
SELECT id, job_id, type, payload, priority, max_retries, error, failed_at, originally_created_at
FROM dead_letters
ORDER BY failed_at DESC
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*DeadLetter, 0)
	for rows.Next() {
		var d DeadLetter
		var payloadBytes []byte
		var priority int
		if err := rows.Scan(&d.ID, &d.JobID, &d.Type, &payloadBytes, &priority, &d.MaxRetries, &d.Error, &d.FailedAt, &d.OriginallyCreatedAt); err != nil {
			return nil, err
		}
		d.Priority = JobPriority(priority)
		if len(payloadBytes) > 0 {
			_ = json.Unmarshal(payloadBytes, &d.Payload)
		}
		out = append(out, &d)
	}
	return out, rows.Err()
}

func (s *PostgresStore) DeleteDeadLetter(id string) error {
	result, err := s.db.Exec(`DELETE FROM dead_letters WHERE id = $1`, id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("dead letter %s not found", id)
	}
	return nil
}
