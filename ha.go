package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// haNode runs one GoQueue instance under etcd leader election. Every
// instance serves the HTTP API; only the elected leader runs workers and
// the Dispatcher.
type haNode struct {
	elector *Elector
	queue   *Queue
	store   ClaimStore
	log     *slog.Logger

	electedAt time.Time
}

// run campaigns and, once elected, dispatches until ctx is cancelled
// (graceful shutdown — returns nil) or leadership is lost (returns an
// error; the caller should exit and let its supervisor restart it).
func (n *haNode) run(ctx context.Context) error {
	epoch, err := n.elector.Campaign(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("campaign: %w", err)
	}
	n.electedAt = time.Now()

	// Fence out the previous leader before touching any jobs, then
	// recover whatever it had claimed but not finished.
	requeued, err := n.store.TakeOver(ctx, epoch)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, ErrFenced) {
			return fmt.Errorf("elected with epoch %d but store already fenced to a newer leader: %w", epoch, err)
		}
		return fmt.Errorf("take over as leader: %w", err)
	}
	n.log.Info("became leader",
		"instance_id", n.elector.cfg.ID, "epoch", epoch, "requeued_orphaned_jobs", requeued)

	n.queue.Start()

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	dispatched := make(chan struct{})
	go func() {
		NewDispatcher(n.queue, n.store, epoch, 200*time.Millisecond, n.log).Run(dispatchCtx)
		close(dispatched)
	}()

	select {
	case <-ctx.Done():
		cancel()
		<-dispatched
		return nil
	case <-n.elector.Lost():
		// Stop claiming first; the fence would reject our claims anyway,
		// but there's no reason to keep hammering the store.
		cancel()
		<-dispatched
		return fmt.Errorf("leadership lost: %s", n.elector.LostReason())
	}
}

// shutdown is the graceful half of failover, called after run has
// returned for a SIGTERM. The Dispatcher has already stopped claiming;
// here we let in-flight jobs finish (bounded by grace), hand back
// anything claimed but unstarted, and then resign explicitly so a
// follower is elected immediately instead of after the lease TTL.
func (n *haNode) shutdown(grace time.Duration) {
	wasLeader := n.elector.IsLeader()
	start := time.Now()

	if wasLeader {
		done := make(chan struct{})
		go func() {
			n.queue.Stop()
			close(done)
		}()
		select {
		case <-done:
			n.log.Info("in-flight jobs finished", "took", time.Since(start).Round(time.Millisecond).String())
		case <-time.After(grace):
			n.log.Warn("grace period elapsed with jobs still running; resigning anyway — the next leader will requeue them",
				"grace", grace.String())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resignStart := time.Now()
	if err := n.elector.Resign(ctx); err != nil {
		n.log.Error("resign failed; followers will take over when the lease expires", "error", err)
		return
	}
	if wasLeader {
		n.log.Info("resigned leadership",
			"instance_id", n.elector.cfg.ID,
			"held_for", time.Since(n.electedAt).Round(time.Second).String(),
			"resign_took", time.Since(resignStart).Round(time.Millisecond).String())
	} else {
		n.log.Info("left leader election", "instance_id", n.elector.cfg.ID)
	}
}

func instanceID() string {
	if id := os.Getenv("INSTANCE_ID"); id != "" {
		return id
	}
	host, err := os.Hostname()
	if err != nil {
		host = "goqueue"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func splitEndpoints(s string) []string {
	var out []string
	for _, ep := range strings.Split(s, ",") {
		if ep = strings.TrimSpace(ep); ep != "" {
			out = append(out, ep)
		}
	}
	return out
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}
