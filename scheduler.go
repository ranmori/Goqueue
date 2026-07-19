package main

import (
	"container/heap"
	"sync"
	"time"
)

// jobHeap is a min-heap of jobs ordered by RunAt, used internally by
// Scheduler to always release the soonest-due job first.
type jobHeap []*Job

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return h[i].RunAt.Before(h[j].RunAt) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*Job))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Release hands a due job off to whatever dispatches jobs to workers
// (normally Queue.release). It mirrors the signature workers expect so
// Scheduler doesn't need to know anything about Queue internals.
type Release func(job *Job) error

// Scheduler holds jobs whose RunAt is in the future and releases each one
// as soon as its time arrives. Delayed jobs live here instead of sitting
// in a worker-facing channel, so they don't consume queue capacity or
// require a worker to poll them individually.
type Scheduler struct {
	mu       sync.Mutex
	pending  jobHeap
	release  Release
	interval time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewScheduler creates a Scheduler that checks for due jobs every
// pollInterval. A pollInterval of 0 defaults to 200ms, which bounds
// worst-case dispatch latency for a delayed job without busy-looping.
func NewScheduler(release Release, pollInterval time.Duration) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}
	return &Scheduler{
		release:  release,
		interval: pollInterval,
		stopCh:   make(chan struct{}),
	}
}

// Schedule adds a job to be released once its RunAt arrives. The caller
// is responsible for having already persisted the job — Scheduler never
// touches the store directly.
func (s *Scheduler) Schedule(job *Job) {
	s.mu.Lock()
	heap.Push(&s.pending, job)
	s.mu.Unlock()
}

func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.run()
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

func (s *Scheduler) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.releaseDue(time.Now().UTC())
		}
	}
}

// releaseDue pops and releases every job at the front of the heap whose
// RunAt has passed. If release fails (e.g. the target queue is full or
// shutting down), the job is pushed back so it's retried on the next
// tick instead of being silently dropped.
func (s *Scheduler) releaseDue(now time.Time) {
	for {
		s.mu.Lock()
		if len(s.pending) == 0 || s.pending[0].RunAt.After(now) {
			s.mu.Unlock()
			return
		}
		job := heap.Pop(&s.pending).(*Job)
		s.mu.Unlock()

		if err := s.release(job); err != nil {
			s.mu.Lock()
			heap.Push(&s.pending, job)
			s.mu.Unlock()
			return
		}
	}
}

// Len reports how many jobs are currently waiting for their RunAt.
// Exposed mainly for tests and for surfacing in /stats.
func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}
