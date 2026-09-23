package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// These tests need a real etcd cluster and skip without one:
//
//	ETCD_ENDPOINTS=127.0.0.1:2379 go test -run Election -v
//
// docker compose up etcd1 etcd2 etcd3 provides one.

func testElector(t *testing.T, prefix, id string, ttl int) *Elector {
	t.Helper()
	endpoints := splitEndpoints(os.Getenv("ETCD_ENDPOINTS"))
	if len(endpoints) == 0 {
		t.Skip("ETCD_ENDPOINTS not set")
	}
	e, err := NewElector(LeaderConfig{Endpoints: endpoints, ID: id, Prefix: prefix, TTL: ttl},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewElector(%s): %v", id, err)
	}
	return e
}

func uniquePrefix(t *testing.T) string {
	return fmt.Sprintf("/goqueue-test/%s/%d", t.Name(), time.Now().UnixNano())
}

type campaignResult struct {
	epoch int64
	err   error
	at    time.Time
}

func campaignAsync(e *Elector) <-chan campaignResult {
	out := make(chan campaignResult, 1)
	go func() {
		epoch, err := e.Campaign(context.Background())
		out <- campaignResult{epoch, err, time.Now()}
	}()
	return out
}

func TestElectionOnlyOneLeader(t *testing.T) {
	prefix := uniquePrefix(t)
	a := testElector(t, prefix, "a", 5)
	defer a.Resign(context.Background())
	b := testElector(t, prefix, "b", 5)
	defer b.Resign(context.Background())

	if _, err := a.Campaign(context.Background()); err != nil {
		t.Fatalf("a campaign: %v", err)
	}
	bRes := campaignAsync(b)

	select {
	case r := <-bRes:
		t.Fatalf("b was elected while a still leads: %+v", r)
	case <-time.After(1 * time.Second):
	}
	if !a.IsLeader() || b.IsLeader() {
		t.Fatalf("want a leader and b follower, got a=%v b=%v", a.IsLeader(), b.IsLeader())
	}
	waitFor(t, 2*time.Second, func() bool { return b.Status().Leader == "a" })
}

func TestElectionFailoverOnResign(t *testing.T) {
	prefix := uniquePrefix(t)
	a := testElector(t, prefix, "a", 10)
	b := testElector(t, prefix, "b", 10)
	defer b.Resign(context.Background())

	aEpoch, err := a.Campaign(context.Background())
	if err != nil {
		t.Fatalf("a campaign: %v", err)
	}
	bRes := campaignAsync(b)
	time.Sleep(300 * time.Millisecond) // let b register as a waiter

	resignedAt := time.Now()
	if err := a.Resign(context.Background()); err != nil {
		t.Fatalf("resign: %v", err)
	}

	select {
	case r := <-bRes:
		if r.err != nil {
			t.Fatalf("b campaign: %v", r.err)
		}
		took := r.at.Sub(resignedAt)
		t.Logf("failover after resign took %s (session TTL is 10s)", took)
		// Resign must hand over immediately, not after the lease TTL.
		if took > 2*time.Second {
			t.Fatalf("failover after resign took %s; resign should not wait for the TTL", took)
		}
		if r.epoch <= aEpoch {
			t.Fatalf("new epoch %d must exceed old epoch %d for fencing", r.epoch, aEpoch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("b not elected within 5s of a resigning")
	}
}

func TestElectionFailoverOnCrash(t *testing.T) {
	const ttl = 2
	prefix := uniquePrefix(t)
	a := testElector(t, prefix, "a", ttl)
	b := testElector(t, prefix, "b", ttl)
	defer b.Resign(context.Background())

	if _, err := a.Campaign(context.Background()); err != nil {
		t.Fatalf("a campaign: %v", err)
	}
	bRes := campaignAsync(b)
	time.Sleep(300 * time.Millisecond)

	// Simulate kill -9: drop the connection without resigning or revoking
	// the lease. etcd only learns a is gone when the lease runs out.
	crashedAt := time.Now()
	a.observeCancel()
	a.client.Close()

	select {
	case r := <-bRes:
		if r.err != nil {
			t.Fatalf("b campaign: %v", r.err)
		}
		took := r.at.Sub(crashedAt)
		t.Logf("failover after crash took %s (session TTL %ds)", took, ttl)
		if took < 500*time.Millisecond {
			t.Fatalf("b elected after %s — before a's lease could have expired", took)
		}
	case <-time.After(time.Duration(ttl+5) * time.Second):
		t.Fatalf("b not elected within TTL+5s of a crashing")
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

// The fencing tests need a disposable Postgres database — they wipe the
// jobs and leader_fence tables:
//
//	TEST_DATABASE_URL=postgres://.../goqueue_test?sslmode=disable go test -run Fence -v

func testPostgres(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	s, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	pg := s.(*PostgresStore)
	if _, err := pg.db.Exec(`DELETE FROM jobs; UPDATE leader_fence SET epoch = 0`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	t.Cleanup(func() { pg.db.Close() })
	return pg
}

func TestFenceRejectsStaleLeader(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()

	if _, err := s.TakeOver(ctx, 10); err != nil {
		t.Fatalf("take over as 10: %v", err)
	}
	job := NewJob("email", map[string]string{"to": "a@example.com"}, PriorityNormal, 0)
	if err := s.Save(job); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimDue(ctx, 10, 5)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("leader 10 claim: got %d jobs, err %v", len(claimed), err)
	}

	// Leader 10 "dies" holding the job; leader 20 takes over.
	requeued, err := s.TakeOver(ctx, 20)
	if err != nil {
		t.Fatalf("take over as 20: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("want the orphaned job requeued, got %d", requeued)
	}

	// A zombie leader 10 that hasn't noticed it lost must claim nothing.
	if stale, err := s.ClaimDue(ctx, 10, 5); err != nil || len(stale) != 0 {
		t.Fatalf("stale leader claimed %d jobs (err %v); fence failed", len(stale), err)
	}
	if fresh, err := s.ClaimDue(ctx, 20, 5); err != nil || len(fresh) != 1 || fresh[0].ID != job.ID {
		t.Fatalf("new leader should reclaim the orphaned job, got %d (err %v)", len(fresh), err)
	}

	// Epochs only move forward.
	if _, err := s.TakeOver(ctx, 15); !errors.Is(err, ErrFenced) {
		t.Fatalf("take over with older epoch: want ErrFenced, got %v", err)
	}
}

func TestClaimDueRespectsPriorityRunAtAndPersistsFields(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	if _, err := s.TakeOver(ctx, 1); err != nil {
		t.Fatal(err)
	}

	low := NewJob("email", nil, PriorityLow, 0)
	high := NewJob("email", nil, PriorityHigh, 2)
	high.Timeout = 30 * time.Second
	high.WebhookURL = "https://example.com/hook"
	high.OnSuccess = &JobTemplate{Type: "report", Priority: PriorityNormal}
	later := NewJob("email", nil, PriorityHigh, 0)
	later.RunAt = time.Now().UTC().Add(time.Hour)
	for _, j := range []*Job{low, high, later} {
		if err := s.Save(j); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ClaimDue(ctx, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 due jobs (future run_at held back), got %d", len(got))
	}
	if got[0].ID != high.ID || got[1].ID != low.ID {
		t.Fatalf("want high before low, got %v then %v", got[0].Priority, got[1].Priority)
	}
	h := got[0]
	if h.Status != StatusRunning || h.Timeout != 30*time.Second || h.WebhookURL != high.WebhookURL ||
		h.OnSuccess == nil || h.OnSuccess.Type != "report" || h.MaxRetries != 2 {
		t.Fatalf("claimed job lost fields: %+v", h)
	}
}
