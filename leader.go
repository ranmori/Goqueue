package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// LeaderConfig configures an Elector. Only Endpoints is required.
type LeaderConfig struct {
	Endpoints []string

	// ID identifies this instance in the election value, and is what
	// followers report as the current leader. Must be unique per process.
	ID string

	// Prefix is the etcd key prefix all candidates campaign under.
	// Defaults to "/goqueue/leader".
	Prefix string

	// TTL is the session lease TTL in seconds. It bounds how long a
	// crashed leader (kill -9, network partition) keeps the lock before
	// etcd expires it and a follower takes over. Defaults to 5.
	TTL int
}

// LeaderStatus is what GET /leader reports.
type LeaderStatus struct {
	Mode     string `json:"mode"`
	ID       string `json:"id"`
	IsLeader bool   `json:"is_leader"`
	Leader   string `json:"leader"`
	Epoch    int64  `json:"epoch,omitempty"`
}

// Elector wraps etcd's concurrency.Election. Every GoQueue instance
// campaigns on the same prefix; etcd grants the lock to the candidate
// whose key has the lowest create revision, and hands it to the next
// candidate the moment that key disappears — either because the leader
// resigned, or because its session lease expired.
//
// Elector deliberately does not implement demotion (leader -> follower
// within the same process). When leadership is lost involuntarily, Lost
// fires and the process is expected to exit and be restarted by its
// supervisor, rejoining as a fresh candidate. That keeps the state
// machine to two states and avoids reasoning about a half-torn-down
// worker pool that still thinks it owns jobs.
type Elector struct {
	cfg      LeaderConfig
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
	log      *slog.Logger

	observeCancel context.CancelFunc

	mu        sync.Mutex
	isLeader  bool
	epoch     int64
	leaderID  string
	resigning bool

	lost     chan struct{}
	lostOnce sync.Once
	lostWhy  string
}

// NewElector connects to etcd and opens a session. It does not campaign
// yet — call Campaign for that.
func NewElector(cfg LeaderConfig, log *slog.Logger) (*Elector, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("leader election: no etcd endpoints configured")
	}
	if cfg.ID == "" {
		return nil, errors.New("leader election: instance ID is required")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "/goqueue/leader"
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 5
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("leader election: connect to etcd: %w", err)
	}

	// Grant the lease ourselves so the call is bounded by a timeout. (A
	// timeout context passed via concurrency.WithContext would also own
	// the session's keepalives, ending the session when it expired.)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	lease, err := client.Grant(ctx, int64(cfg.TTL))
	cancel()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("leader election: grant lease: %w", err)
	}

	// The session keeps the lease alive in the background. If keepalives
	// stop reaching a quorum for TTL seconds, the lease expires
	// server-side and session.Done() closes client-side.
	session, err := concurrency.NewSession(client, concurrency.WithLease(lease.ID))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("leader election: open session: %w", err)
	}

	e := &Elector{
		cfg:      cfg,
		client:   client,
		session:  session,
		election: concurrency.NewElection(session, cfg.Prefix),
		log:      log,
		lost:     make(chan struct{}),
	}

	observeCtx, observeCancel := context.WithCancel(context.Background())
	e.observeCancel = observeCancel
	go e.observe(observeCtx)
	go e.watchSession()

	log.Info("joined leader election",
		"instance_id", cfg.ID, "prefix", cfg.Prefix,
		"lease_id", fmt.Sprintf("%x", session.Lease()), "ttl_seconds", cfg.TTL)
	return e, nil
}

// Campaign blocks until this instance is elected leader, ctx is
// cancelled, or the session expires. On success it returns the fencing
// epoch: the create revision of our election key, which is strictly
// greater than that of every previous leader.
func (e *Elector) Campaign(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-e.session.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	e.log.Info("campaigning for leadership, standing by as follower", "instance_id", e.cfg.ID)
	if err := e.election.Campaign(ctx, e.cfg.ID); err != nil {
		select {
		case <-e.session.Done():
			return 0, errors.New("etcd session expired while campaigning")
		default:
			return 0, err
		}
	}

	epoch := e.election.Rev()
	e.mu.Lock()
	e.isLeader = true
	e.epoch = epoch
	e.leaderID = e.cfg.ID
	e.mu.Unlock()
	return epoch, nil
}

// Lost is closed if this instance loses leadership (or, as a follower,
// its session) without having asked to. Callers should stop dispatching
// immediately and exit.
func (e *Elector) Lost() <-chan struct{} { return e.lost }

// LostReason explains why Lost fired.
func (e *Elector) LostReason() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lostWhy
}

func (e *Elector) markLost(reason string) {
	e.mu.Lock()
	if e.resigning {
		e.mu.Unlock()
		return
	}
	e.isLeader = false
	e.lostWhy = reason
	e.mu.Unlock()
	e.lostOnce.Do(func() { close(e.lost) })
}

// watchSession treats lease expiry as fatal for leaders and followers
// alike: a leader whose lease expired no longer holds the lock, and a
// follower whose lease expired is no longer a candidate (its key is gone,
// so its Campaign would wait forever).
func (e *Elector) watchSession() {
	<-e.session.Done()
	e.markLost("etcd session lease expired")
}

// observe keeps leaderID current (for GET /leader on followers) and
// detects the leader key changing out from under us, e.g. an operator
// running `etcdctl del` on it.
func (e *Elector) observe(ctx context.Context) {
	for {
		for resp := range e.election.Observe(ctx) {
			if len(resp.Kvs) == 0 {
				continue
			}
			kv := resp.Kvs[0]
			e.mu.Lock()
			prev := e.leaderID
			e.leaderID = string(kv.Value)
			// Any real successor was created after our key, so a lower
			// revision is just a stale event from before we were elected.
			supersededUs := e.isLeader && kv.CreateRevision > e.epoch
			e.mu.Unlock()

			if prev != string(kv.Value) {
				e.log.Info("observed leader", "leader", string(kv.Value), "epoch", kv.CreateRevision, "instance_id", e.cfg.ID)
			}
			if supersededUs {
				e.markLost(fmt.Sprintf("leader key now held by %q", kv.Value))
			}
		}
		if ctx.Err() != nil {
			return
		}
		// Observe's channel closes on transient watch failures too; back
		// off briefly and re-subscribe instead of going blind.
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Status reports this instance's view of the election for GET /leader.
func (e *Elector) Status() LeaderStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := LeaderStatus{Mode: "ha", ID: e.cfg.ID, IsLeader: e.isLeader, Leader: e.leaderID}
	if e.isLeader {
		s.Epoch = e.epoch
	}
	return s
}

// IsLeader reports whether this instance currently holds leadership.
func (e *Elector) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.isLeader
}

// Resign gives up leadership explicitly and closes the session, so the
// next candidate is elected as soon as etcd processes the delete instead
// of after the lease TTL runs out. Safe to call on a follower.
func (e *Elector) Resign(ctx context.Context) error {
	e.mu.Lock()
	e.resigning = true
	wasLeader := e.isLeader
	e.isLeader = false
	e.mu.Unlock()

	var err error
	if wasLeader {
		err = e.election.Resign(ctx)
	}
	e.observeCancel()
	// Closing the session revokes the lease, which also removes a
	// follower's candidate key so it doesn't linger in the queue of
	// waiters for TTL seconds.
	if cerr := e.session.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if cerr := e.client.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}
