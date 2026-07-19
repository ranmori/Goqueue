package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"
)

type JobHandler func(ctx context.Context, job *Job) error

type Queue struct {
	highQueue   chan *Job
	normalQueue chan *Job
	lowQueue    chan *Job
	handlers    map[string]JobHandler
	store       Store
	numWorkers  int
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	mu          sync.Mutex
	accepting   bool
	scheduler   *Scheduler

	limiter *concurrencyLimiter
	dedupe  *deduper
	metrics *Metrics
	log     *slog.Logger

	webhookClient *http.Client
}

// SetScheduler wires a Scheduler to hold jobs with a future RunAt until
// they're due. Optional — a Queue with no scheduler simply rejects
// delayed jobs (see Enqueue), so wiring this up is only required if the
// caller intends to use delayed/scheduled jobs at all.
func (q *Queue) SetScheduler(s *Scheduler) {
	q.scheduler = s
}

// SetConcurrencyLimit caps how many jobs of jobType may run at once
// across all workers, independent of total worker count. A limit <= 0
// removes any existing cap for that type.
func (q *Queue) SetConcurrencyLimit(jobType string, limit int) {
	q.limiter.SetLimit(jobType, limit)
}

// Metrics returns the Queue's metrics collector, for mounting its
// ServeHTTP as a /metrics endpoint.
func (q *Queue) Metrics() *Metrics {
	return q.metrics
}

// SetLogger overrides the default logger (slog.Default()).
func (q *Queue) SetLogger(l *slog.Logger) {
	q.log = l
}

// QueueDepths reports the current number of jobs waiting in each
// priority lane. Used by /stats and by the metrics exporter.
func (q *Queue) QueueDepths() map[string]int {
	return map[string]int{
		PriorityHigh.String():   len(q.highQueue),
		PriorityNormal.String(): len(q.normalQueue),
		PriorityLow.String():    len(q.lowQueue),
	}
}

const (
	highQueueSize   = 1000
	normalQueueSize = 1000
	lowQueueSize    = 1000
)

func NewQueue(store Store, numWorkers int) *Queue {
	q := &Queue{
		highQueue:   make(chan *Job, highQueueSize),
		normalQueue: make(chan *Job, normalQueueSize),
		lowQueue:    make(chan *Job, lowQueueSize),
		handlers:    make(map[string]JobHandler),
		store:       store,
		numWorkers:  numWorkers,
		stopCh:      make(chan struct{}),
		accepting:   true,
		limiter:     newConcurrencyLimiter(),
		dedupe:      newDeduper(),
		metrics:     NewMetrics(),
		log:         slog.Default(),
		webhookClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	q.metrics.SetQueueDepthFunc(q.QueueDepths)
	q.metrics.SetScheduledFunc(func() int {
		if q.scheduler == nil {
			return 0
		}
		return q.scheduler.Len()
	})
	return q
}

func (q *Queue) RegisterHandler(jobType string, handler JobHandler) {
	q.handlers[jobType] = handler
}

func (q *Queue) Start() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.accepting {
		return
	}

	for i := 0; i < q.numWorkers; i++ {
		q.wg.Add(1)
		go q.runWorker(i + 1)
	}

	q.log.Info("workers started", "count", q.numWorkers)
}

// ErrDuplicateJob is returned by Enqueue when a job's DedupeKey matches
// one already accepted within its DedupeWindow.
var ErrDuplicateJob = fmt.Errorf("duplicate job suppressed")

func (q *Queue) Enqueue(job *Job) error {
	q.mu.Lock()
	accepting := q.accepting
	q.mu.Unlock()

	if !accepting {
		return fmt.Errorf("queue is shutting down")
	}
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	now := time.Now().UTC()
	if !q.dedupe.tryAcquire(job.DedupeKey, job.DedupeWindow, now) {
		return fmt.Errorf("%w: dedupe_key %q already accepted within its window", ErrDuplicateJob, job.DedupeKey)
	}

	job.Status = StatusPending
	job.CreatedAt = now

	if err := q.store.Save(job); err != nil {
		return err
	}

	if !job.IsDue(now) {
		if q.scheduler == nil {
			return fmt.Errorf("job has a future run_at but no scheduler is configured")
		}
		q.scheduler.Schedule(job)
		return nil
	}

	return q.release(job)
}

// release hands a job directly to its priority channel, skipping the
// scheduling check in Enqueue. Used for jobs that are immediately due,
// and by the Scheduler once a delayed job's RunAt has arrived.
func (q *Queue) release(job *Job) error {
	select {
	case q.priorityChannel(job.Priority) <- job:
		return nil
	default:
		return fmt.Errorf("queue is full")
	}
}

func (q *Queue) Stop() {
	q.stopOnce.Do(func() {
		q.mu.Lock()
		q.accepting = false
		q.mu.Unlock()
		close(q.stopCh)
	})
	q.wg.Wait()
}

func (q *Queue) runWorker(id int) {
	defer q.wg.Done()
	q.log.Info("worker ready", "worker_id", id)

	for {
		job := q.fetchJob()
		if job == nil {
			q.log.Info("worker shutting down", "worker_id", id)
			return
		}
		q.processJob(job)
	}
}

func (q *Queue) fetchJob() *Job {
	for {
		select {
		case job := <-q.highQueue:
			return job
		default:
		}

		select {
		case job := <-q.normalQueue:
			return job
		default:
		}

		select {
		case job := <-q.lowQueue:
			return job
		default:
		}

		select {
		case <-q.stopCh:
			return nil
		case job := <-q.highQueue:
			return job
		case job := <-q.normalQueue:
			return job
		case job := <-q.lowQueue:
			return job
		}
	}
}

func (q *Queue) priorityChannel(priority JobPriority) chan *Job {
	switch priority {
	case PriorityHigh:
		return q.highQueue
	case PriorityNormal:
		return q.normalQueue
	default:
		return q.lowQueue
	}
}

func (q *Queue) processJob(job *Job) {
	if job == nil {
		return
	}

	savedJob, err := q.store.Get(job.ID)
	if err == nil && savedJob.Status == StatusCancelled {
		q.log.Info("job was cancelled before processing", "job_id", job.ID)
		return
	}

	release := q.limiter.acquire(job.Type)
	defer release()

	processingStart := time.Now().UTC()

	now := processingStart
	job.StartedAt = &now
	job.Status = StatusRunning
	_ = q.store.Update(job)

	handler, exists := q.handlers[job.Type]
	if !exists {
		job.Status = StatusFailed
		job.Error = fmt.Sprintf("no handler for job type: %s", job.Type)
		job.FinishedAt = timePtr(time.Now().UTC())
		_ = q.store.Update(job)
		q.finish(job, processingStart)
		return
	}

	for attempt := 0; attempt <= job.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			q.log.Info("job retrying", "job_id", job.ID, "backoff", backoff, "attempt", attempt)
			time.Sleep(backoff)
		}

		attemptCtx := context.Background()
		var cancel context.CancelFunc
		if job.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(attemptCtx, job.Timeout)
		}

		err := handler(attemptCtx, job)

		if err == nil {
			if cancel != nil {
				cancel()
			}
			finished := time.Now().UTC()
			job.FinishedAt = &finished
			job.Status = StatusDone
			job.Error = ""
			_ = q.store.Update(job)
			q.log.Info("job done", "job_id", job.ID, "type", job.Type, "attempts", attempt+1)
			q.finish(job, processingStart)
			return
		}

		if attemptCtx.Err() == context.DeadlineExceeded {
			err = fmt.Errorf("attempt timed out after %s: %w", job.Timeout, attemptCtx.Err())
		}
		if cancel != nil {
			cancel()
		}

		job.Retries = attempt + 1
		job.Error = err.Error()
		_ = q.store.Update(job)
		q.log.Warn("job attempt failed", "job_id", job.ID, "type", job.Type, "attempt", attempt+1, "error", err)
	}

	finished := time.Now().UTC()
	job.FinishedAt = &finished
	job.Status = StatusFailed
	_ = q.store.Update(job)
	q.log.Error("job failed permanently", "job_id", job.ID, "type", job.Type, "retries", job.MaxRetries)

	q.moveToDeadLetter(job)
	q.finish(job, processingStart)
}

// finish runs everything that should happen once a job reaches a
// terminal status, regardless of which one: metrics, the webhook
// notification, and firing whichever chained job matches the outcome.
func (q *Queue) finish(job *Job, processingStart time.Time) {
	q.metrics.RecordCompletion(job.Type, job.Status, job.Retries, time.Since(processingStart))
	q.notifyWebhook(job)

	var next *JobTemplate
	if job.Status == StatusDone {
		next = job.OnSuccess
	} else if job.Status == StatusFailed {
		next = job.OnFailure
	}
	if next == nil {
		return
	}
	chained := next.ToJob()
	if err := q.Enqueue(chained); err != nil {
		q.log.Error("failed to enqueue chained job", "parent_job_id", job.ID, "error", err)
	}
}

// moveToDeadLetter archives a permanently-failed job so it can be
// inspected or replayed later, without cluttering normal job listings.
func (q *Queue) moveToDeadLetter(job *Job) {
	entry := &DeadLetter{
		JobID:               job.ID,
		Type:                job.Type,
		Payload:             job.Payload,
		Priority:            job.Priority,
		MaxRetries:          job.MaxRetries,
		Error:               job.Error,
		FailedAt:            time.Now().UTC(),
		OriginallyCreatedAt: job.CreatedAt,
	}
	if err := q.store.SaveDeadLetter(entry); err != nil {
		q.log.Error("failed to save dead letter", "job_id", job.ID, "error", err)
	}
}

// notifyWebhook fires a best-effort POST with the job's final status.
// It runs in its own goroutine so a slow or unreachable endpoint can
// never delay worker throughput; delivery failures are logged only.
func (q *Queue) notifyWebhook(job *Job) {
	if job.WebhookURL == "" {
		return
	}
	body, err := json.Marshal(map[string]any{
		"job_id":  job.ID,
		"type":    job.Type,
		"status":  job.Status.String(),
		"error":   job.Error,
		"retries": job.Retries,
	})
	if err != nil {
		q.log.Error("failed to marshal webhook payload", "job_id", job.ID, "error", err)
		return
	}

	go func() {
		resp, err := q.webhookClient.Post(job.WebhookURL, "application/json", bytes.NewReader(body))
		if err != nil {
			q.log.Warn("webhook delivery failed", "job_id", job.ID, "url", job.WebhookURL, "error", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			q.log.Warn("webhook endpoint returned non-2xx", "job_id", job.ID, "url", job.WebhookURL, "status", resp.StatusCode)
		}
	}()
}

func timePtr(t time.Time) *time.Time {
	return &t
}
