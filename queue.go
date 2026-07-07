package main

import (
	"context"
	"fmt"
	"log"
	"math"
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
}

const (
	highQueueSize   = 1000
	normalQueueSize = 1000
	lowQueueSize    = 1000
)

func NewQueue(store Store, numWorkers int) *Queue {
	return &Queue{
		highQueue:   make(chan *Job, highQueueSize),
		normalQueue: make(chan *Job, normalQueueSize),
		lowQueue:    make(chan *Job, lowQueueSize),
		handlers:    make(map[string]JobHandler),
		store:       store,
		numWorkers:  numWorkers,
		stopCh:      make(chan struct{}),
		accepting:   true,
	}
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

	log.Printf("🚀 %d workers started", q.numWorkers)
}

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

	job.Status = StatusPending
	job.CreatedAt = time.Now().UTC()

	if err := q.store.Save(job); err != nil {
		return err
	}

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
	log.Printf("Worker %d ready", id)

	for {
		job := q.fetchJob()
		if job == nil {
			log.Printf("Worker %d shutting down", id)
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
		log.Printf("Job %s was cancelled before processing", job.ID)
		return
	}

	now := time.Now().UTC()
	job.StartedAt = &now
	job.Status = StatusRunning
	_ = q.store.Update(job)

	handler, exists := q.handlers[job.Type]
	if !exists {
		job.Status = StatusFailed
		job.Error = fmt.Sprintf("no handler for job type: %s", job.Type)
		job.FinishedAt = timePtr(time.Now().UTC())
		_ = q.store.Update(job)
		return
	}

	for attempt := 0; attempt <= job.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			log.Printf("Job %s retrying in %v (attempt %d)", job.ID, backoff, attempt)
			time.Sleep(backoff)
		}

		err := handler(context.Background(), job)
		if err == nil {
			finished := time.Now().UTC()
			job.FinishedAt = &finished
			job.Status = StatusDone
			job.Error = ""
			_ = q.store.Update(job)
			log.Printf("✅ Job %s done", job.ID)
			return
		}

		job.Retries = attempt + 1
		job.Error = err.Error()
		_ = q.store.Update(job)
		log.Printf("❌ Job %s failed (attempt %d): %v", job.ID, attempt+1, err)
	}

	finished := time.Now().UTC()
	job.FinishedAt = &finished
	job.Status = StatusFailed
	_ = q.store.Update(job)
	log.Printf("🚨 Job %s failed after %d retries", job.ID, job.MaxRetries)
}

func timePtr(t time.Time) *time.Time {
	return &t
}
