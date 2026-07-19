package main

import "sync"

// concurrencyLimiter caps how many jobs of a given type may execute at
// once, independent of the worker pool size. Without this, a burst of
// slow jobs of one type (e.g. "email") can occupy every worker and starve
// every other job type even though the queue itself isn't full.
type concurrencyLimiter struct {
	mu    sync.Mutex
	sems  map[string]chan struct{}
	limit map[string]int
}

func newConcurrencyLimiter() *concurrencyLimiter {
	return &concurrencyLimiter{
		sems:  make(map[string]chan struct{}),
		limit: make(map[string]int),
	}
}

// SetLimit caps job type jobType to at most `limit` concurrent executions
// across all workers. Must be called before jobs of that type start
// processing to take effect — changing it mid-flight is not supported,
// matching the simplicity of the rest of this package.
func (c *concurrencyLimiter) SetLimit(jobType string, limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if limit <= 0 {
		delete(c.sems, jobType)
		delete(c.limit, jobType)
		return
	}
	c.limit[jobType] = limit
	c.sems[jobType] = make(chan struct{}, limit)
}

// acquire blocks until a slot for jobType is free, and returns the
// release function to call when the job is done. If no limit was
// configured for jobType, it returns immediately with a no-op release.
func (c *concurrencyLimiter) acquire(jobType string) (release func()) {
	c.mu.Lock()
	sem, limited := c.sems[jobType]
	c.mu.Unlock()

	if !limited {
		return func() {}
	}

	sem <- struct{}{}
	return func() { <-sem }
}

// InFlight reports how many jobs of jobType are currently holding a slot.
// Useful for metrics/tests; returns 0 for unlimited types.
func (c *concurrencyLimiter) InFlight(jobType string) int {
	c.mu.Lock()
	sem, limited := c.sems[jobType]
	c.mu.Unlock()
	if !limited {
		return 0
	}
	return len(sem)
}
