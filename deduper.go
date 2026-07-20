package main

import (
	"sync"
	"time"
)

// deduper suppresses enqueuing a job whose DedupeKey matches one already
// accepted within its DedupeWindow — e.g. "don't send the same email to
// the same recipient twice within 30s" even if two callers race to
// enqueue it. It only needs to remember when each key expires; the first
// job to claim a key is the one that runs.
type deduper struct {
	mu      sync.Mutex
	expires map[string]time.Time
}

func newDeduper() *deduper {
	return &deduper{expires: make(map[string]time.Time)}
}

// tryAcquire reports whether key is free to use right now. If it is (or
// key is empty, meaning dedupe isn't requested), it also reserves key
// until now+window and returns true. Otherwise it returns false and
// leaves the existing reservation untouched.
func (d *deduper) tryAcquire(key string, window time.Duration, now time.Time) bool {
	if key == "" {
		return true
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.sweepLocked(now)

	if expiry, exists := d.expires[key]; exists && expiry.After(now) {
		return false
	}

	if window <= 0 {
		window = time.Minute // sane default so a key isn't reserved forever by accident
	}
	d.expires[key] = now.Add(window)
	return true
}

// sweepLocked drops expired entries so the map doesn't grow unbounded.
// Caller must hold d.mu.
func (d *deduper) sweepLocked(now time.Time) {
	for k, exp := range d.expires {
		if !exp.After(now) {
			delete(d.expires, k)
		}
	}
}

// release removes a key's reservation so the same key can be used again
// immediately — called when an enqueued job couldn't be dispatched (e.g.
// queue full) and the caller should be allowed to retry.
func (d *deduper) release(key string) {
	if key == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.expires, key)
}
