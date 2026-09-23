package main

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// ErrFenced is returned when a newer leader epoch has already been
// recorded in the store, i.e. this instance's leadership is stale.
var ErrFenced = errors.New("fenced: a newer leader epoch is already recorded")

// ClaimStore is the extra contract a Store must satisfy to be shared by
// several GoQueue instances under leader election. Only PostgresStore
// implements it — an in-memory store is private to one process, so there
// is nothing for a follower to take over.
type ClaimStore interface {
	// TakeOver records epoch as the current leader epoch (failing with
	// ErrFenced if a newer one is already recorded) and returns every
	// job the previous leader had claimed but not finished to pending.
	TakeOver(ctx context.Context, epoch int64) (requeued int64, err error)

	// ClaimDue atomically marks up to limit due, pending jobs as running
	// and returns them, highest priority first. It claims nothing if
	// epoch is no longer the recorded leader epoch.
	ClaimDue(ctx context.Context, epoch int64, limit int) ([]*Job, error)
}

// Dispatcher is the leader-only loop that feeds the local worker pool
// from the shared store. Followers never run one; they only persist jobs
// that arrive over their HTTP API as pending rows.
type Dispatcher struct {
	queue    *Queue
	store    ClaimStore
	epoch    int64
	interval time.Duration
	log      *slog.Logger
}

func NewDispatcher(queue *Queue, store ClaimStore, epoch int64, interval time.Duration, log *slog.Logger) *Dispatcher {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &Dispatcher{queue: queue, store: store, epoch: epoch, interval: interval, log: log}
}

// Run polls until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	d.log.Info("dispatcher started", "epoch", d.epoch)
	defer d.log.Info("dispatcher stopped", "epoch", d.epoch)

	for {
		d.dispatchOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// dispatchOnce claims only as many jobs as there are idle-ish workers to
// take them. Keeping the local buffer at most numWorkers deep bounds how
// much work is stranded in "running" if this leader dies, and how much a
// graceful shutdown has to hand back.
func (d *Dispatcher) dispatchOnce(ctx context.Context) {
	for ctx.Err() == nil {
		free := d.queue.numWorkers - d.queue.buffered()
		if free <= 0 {
			return
		}
		jobs, err := d.store.ClaimDue(ctx, d.epoch, free)
		if err != nil {
			if ctx.Err() == nil {
				d.log.Error("claiming due jobs failed", "epoch", d.epoch, "error", err)
			}
			return
		}
		for _, job := range jobs {
			if err := d.queue.release(job); err != nil {
				d.queue.handBack(job)
			}
		}
		if len(jobs) < free {
			return
		}
	}
}
