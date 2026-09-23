# GoQueue

A background job processor with a REST API, written in Go. Priority queues,
retries with exponential backoff, delayed/scheduled jobs, per-job timeouts,
a dead-letter queue with replay, per-type rate limiting, deduplication,
job chaining, webhook notifications, and a Prometheus-compatible metrics
endpoint — backed by either an in-memory store or PostgreSQL.

## Why this exists

A learning project turned into something closer to what a real job queue
needs to do in production. Every feature below started from a concrete
question ("what happens if a handler hangs forever?", "what happens to a
job that fails for good?") rather than being added for its own sake — see
[Design notes](#design-notes) for the reasoning behind each one.

## Architecture

```mermaid
graph TB
    subgraph "HTTP API (:8080)"
        REST["REST Endpoints<br/>POST /jobs · GET /jobs · DELETE /jobs/{id}<br/>/stats · /dlq · /metrics · /health"]
    end

    subgraph "Queue Engine"
        DEDUP{"Deduplication<br/>Key + Window"}
        SCH["Scheduler<br/>(min-heap by RunAt)"]
        HIGH[/"High Lane<br/>(cap 1000)"/]
        NORM[/"Normal Lane<br/>(cap 1000)"/]
        LOW[/"Low Lane<br/>(cap 1000)"/]
    end

    subgraph "Worker Pool"
        W1["Worker 1"]
        W2["Worker 2"]
        WN["Worker N"]
    end

    subgraph "Per-Type Rate Limiting"
        LIM["Concurrency Limiter<br/>(buffered-channel semaphore)"]
    end

    subgraph "Storage"
        STORE[("Store Interface<br/>InMemory · PostgreSQL")]
        DLQ[("Dead Letter<br/>Queue")]
    end

    subgraph "Post-Processing"
        MET["Metrics<br/>(Prometheus /metrics)"]
        WH["Webhooks<br/>(fire-and-forget POST)"]
        CHAIN["Job Chaining<br/>on_success / on_failure"]
    end

    REST -->|enqueue| DEDUP
    DEDUP -->|new key| SCH
    DEDUP -->|immediate| HIGH
    DEDUP -->|immediate| NORM
    DEDUP -->|immediate| LOW
    SCH -->|RunAt due| HIGH
    SCH -->|RunAt due| NORM
    SCH -->|RunAt due| LOW

    HIGH & NORM & LOW --> W1 & W2 & WN
    W1 & W2 & WN --> LIM
    LIM -->|acquire slot| STORE
    LIM -->|release slot| HIGH & NORM & LOW

    W1 & W2 & WN -->|terminal status| MET
    W1 & W2 & WN -->|webhook_url set| WH
    W1 & W2 & WN -->|on_success/on_failure| CHAIN
    CHAIN -->|re-enqueue| DEDUP
    W1 & W2 & WN -->|retries exhausted| DLQ

    STORE --- DLQ
```

### Priority queue internals

```mermaid
sequenceDiagram
    participant C as Client
    participant API as HTTP Handler
    participant D as Deduper
    participant Q as Priority Channels
    participant S as Scheduler
    participant W as Worker Pool
    participant L as Concurrency Limiter
    participant Store as Store

    C->>API: POST /jobs {type, priority, ...}
    API->>D: tryAcquire(key, window)
    alt duplicate key
        D-->>API: false
        API-->>C: 409 Conflict
    else new key
        D-->>API: true
        API->>Store: Save(job)
        alt future RunAt
            API->>S: Schedule(job)
            Note over S: holds in min-heap
            S->>Q: release(job) when RunAt due
        else immediate
            API->>Q: release(job)
            alt channel full
                Q-->>API: error
                API->>Store: Update(status=cancelled)
                API->>D: release(key)
                API-->>C: 503 Queue Full
            else accepted
                Q-->>API: ok
                API-->>C: 201 Created
            end
        end
    end

    loop fetchJob() — priority order
        W->>Q: non-blocking read high → normal → low
        alt all empty
            W->>Q: blocking select (+ 1ms backoff)
        end
    end

    W->>L: acquire(type)
    alt concurrency limit hit
        Note over L: blocks until slot free
    end
    L-->>W: slot acquired

    W->>Store: Update(status=running)
    W->>W: handler(ctx, job)
    alt handler success
        W->>Store: Update(status=done)
        W->>W: metrics + webhook + chain
    else handler error + retries left
        W->>Store: Update(retries++, error)
        Note over W: sleep 2^(attempt-1)s
        W->>W: retry
    else retries exhausted
        W->>Store: Update(status=failed)
        W->>Store: SaveDeadLetter(entry)
        W->>W: metrics + webhook + on_failure chain
    end

    W->>L: release(slot)
```

## Features

- **Priority queues** — high/normal/low lanes, always drained in that order.
- **Retries with exponential backoff** — configurable `max_retries` per job.
- **Delayed / scheduled jobs** — set `run_at` and a heap-based scheduler
  holds the job until it's due, without occupying a worker-facing channel.
- **Per-job timeouts** — `timeout_seconds` bounds a single execution
  attempt via `context.WithTimeout`; a hung handler is cancelled and
  treated as a failed attempt, not left running forever.
- **Dead-letter queue** — a job that exhausts all retries is archived
  (not just marked `failed`) and can be inspected or replayed later.
- **Per-job-type rate limiting** — cap concurrent executions of one job
  type so a burst of slow jobs can't starve every other type.
- **Deduplication** — a `dedupe_key` + `dedupe_window_seconds` suppress
  a duplicate enqueue (e.g. the same email to the same recipient twice
  within 30 seconds).
- **Job chaining** — `on_success` / `on_failure` templates enqueue a
  follow-up job matching the actual outcome.
- **Webhook notifications** — best-effort POST to a URL when a job
  reaches a terminal status.
- **Metrics** — `/metrics` in Prometheus text format: job counts by
  type/status, retry counts, a duration histogram, live queue depth.
- **Two storage backends** — in-memory (default) or PostgreSQL (set
  `DATABASE_URL`), behind the same `Store` interface.
- **Structured logging** — JSON logs via the standard library's
  `log/slog`.
- **High availability** — run several instances against one Postgres.
  They elect a single dispatcher through etcd, and a follower takes over
  when it dies. See [High availability](#high-availability-leader-election).

## Running it

```bash
go run .
# or, with Postgres:
DATABASE_URL="postgres://user:pass@localhost:5432/goqueue?sslmode=disable" go run .
# or, the full HA stack (3-node etcd + Postgres + 2 GoQueue instances):
docker compose up -d --build --wait
```

The server listens on `:8080` (override with `ADDR`). Three example
handlers are registered at startup: `email`, `webhook`, and `report`
(see `main.go`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | unset (in-memory) | Postgres DSN; required for HA |
| `ETCD_ENDPOINTS` | unset (single-node) | comma-separated etcd endpoints; enables leader election |
| `ETCD_SESSION_TTL` | `5` | lease TTL in seconds, which bounds failover time after a crash |
| `INSTANCE_ID` | `<hostname>-<pid>` | this instance's name in the election |
| `ADDR` | `:8080` | HTTP listen address |
| `API_KEY` | unset | required `X-API-Key` for mutating endpoints |

## High availability (leader election)

A job queue with one dispatcher is a single point of failure. Running
two dispatchers naively is worse: both pull the same pending jobs and
run everything twice. GoQueue runs **N instances with exactly one
active dispatcher**, elected through etcd.

```mermaid
graph LR
    C[Clients] --> LB{any instance}
    LB --> G1["goqueue-1<br/><b>leader</b><br/>API + dispatcher + workers"]
    LB --> G2["goqueue-2<br/>follower<br/>API only"]
    G1 -- "campaign / keepalive<br/>(lease TTL 5s)" --> E[("etcd<br/>3 members")]
    G2 -- "campaign (blocked)<br/>observe leader key" --> E
    G1 -- "ClaimDue(epoch)" --> PG[("Postgres<br/>jobs + leader_fence")]
    G2 -- "INSERT pending" --> PG
```

**I didn't implement consensus.** etcd already runs Raft, and its
`concurrency` package provides election on top of it. Each instance
opens a `concurrency.Session`, which is a lease kept alive in the
background, and calls `Election.Campaign`. Campaign writes a key under
`/goqueue/leader` tied to that lease. The key with the lowest create
revision leads; everyone else blocks. The work here is wiring it up
correctly ([leader.go](leader.go), [ha.go](ha.go),
[dispatcher.go](dispatcher.go)):

- **Every instance serves the API.** Followers persist jobs as
  `pending`. Only the leader runs workers, plus a dispatcher that claims
  due jobs with `UPDATE … FOR UPDATE SKIP LOCKED`. It claims at most as
  many as it has workers free, which bounds how much work is stranded if
  it dies.
- **Crash failover (kill -9, partition):** the dead leader's lease stops
  being renewed. After `ETCD_SESSION_TTL` seconds etcd deletes its key,
  and the next candidate's Campaign returns. The new leader requeues
  every job still marked `running`: only a leader runs jobs, so those
  belong to the dead one.
- **Graceful failover (SIGTERM):** stop claiming → let in-flight jobs
  finish (10s grace) → hand unstarted claims back to `pending` →
  `Election.Resign()`. The follower takes over as soon as etcd processes
  the delete, not after the TTL. That order is deliberate: resigning
  first would be faster, but the new leader would requeue jobs we're
  still running.
- **Fencing against a zombie leader.** Election alone isn't enough. A
  leader stuck in a long GC pause, or partitioned away from etcd, can
  keep going for a moment after etcd has already elected someone else.
  So the election key's create revision (the **epoch**, which only
  increases) is written to a `leader_fence` row on takeover. Every claim
  checks it under `FOR SHARE`, so a stale leader's claims match nothing.
  The fence row lock also serialises the new leader's requeue against a
  claim the old leader has in flight.
- **Losing leadership exits the process.** If the session expires or
  another key takes over, the instance stops dispatching and exits
  non-zero. Its supervisor (`restart: on-failure` in compose,
  Kubernetes, systemd) restarts it as a fresh follower. That's the same
  choice kube-controller-manager makes. Demoting in place would mean
  reasoning about a half-stopped worker pool that still believes it owns
  jobs.
- **Why 3 etcd members.** etcd commits nothing without a majority, and
  that includes lease expiry and a new leader's key. Three members
  tolerate one failure. A leader on the minority side of a partition
  can't renew its lease, so it steps down, and no second leader can be
  elected there. A single-node etcd would itself be the single point of
  failure this was meant to remove.

**Trade-off: at-least-once.** A job that was mid-flight when its leader
died runs again on the new leader. Exactly-once would need handlers to
commit their side effects and the job's completion atomically, which a
generic queue can't do for you. So handlers should be idempotent (the
`dedupe_key` pattern helps).

**Demo:** [docs/failover-demo.md](docs/failover-demo.md) has the log
output from `kill -9`-ing a leader with two jobs mid-flight. The
follower took over in 5.24s (TTL 5s), logged `requeued_orphaned_jobs: 2`,
and finished both. To reproduce it: `./scripts/failover-demo.sh` (or
`./scripts/failover-demo.sh stop` for the SIGTERM path).

## API

Set `API_KEY` in the environment to require an `X-API-Key` header on
mutating requests (`POST /jobs`, `DELETE /jobs/{id}`, `POST /dlq/{id}/replay`).
Read endpoints stay open either way. If `API_KEY` is unset, all endpoints
are open — fine for local dev, not for anything public-facing (see
`DEPLOY.md`).

### `POST /jobs` — enqueue a job

```json
{
  "type": "email",
  "payload": { "to": "user@example.com" },
  "priority": 10,
  "max_retries": 3,
  "run_at": "2026-08-01T09:00:00Z",
  "timeout_seconds": 30,
  "dedupe_key": "email:user@example.com",
  "dedupe_window_seconds": 30,
  "webhook_url": "https://example.com/hooks/jobs",
  "on_success": { "type": "log_success", "priority": 5 },
  "on_failure": { "type": "alert_oncall", "priority": 10 }
}
```

Only `type` is required. `priority` is `0` (low), `5` (normal), or `10`
(high). A duplicate `dedupe_key` within its window returns `409 Conflict`.

### `GET /jobs?type=&status=&limit=&offset=` — list jobs

Paginated, newest first.

### `GET /jobs/{id}` — get a single job

### `DELETE /jobs/{id}` — cancel a pending job

Only jobs still in `pending` status can be cancelled.

### `GET /stats` — job counts by status

### `GET /dlq` — list dead-lettered jobs

### `POST /dlq/{id}/replay` — re-enqueue a dead-lettered job

Reconstructs a fresh job from the dead letter's original type/payload/
priority, enqueues it, and removes the dead letter entry once the
re-enqueue succeeds. If re-enqueueing fails (e.g. queue full), the dead
letter is left in place rather than lost.

### `GET /metrics` — Prometheus-format metrics

### `GET /health` — liveness check

### `GET /leader` — leader-election status

```json
{"mode":"ha","id":"goqueue-2","is_leader":false,"leader":"goqueue-1"}
```

In single-node mode it returns `{"mode":"single-node","is_leader":true}`.
`/metrics` also exports it as the `goqueue_is_leader` gauge.

## Design notes

**Why a hand-rolled metrics exporter instead of `client_golang`?**
`github.com/prometheus/client_golang`'s registry and histogram types pull
in a chain of transitive dependencies (`client_model`, `common`,
`google.golang.org/protobuf`) that's disproportionate to what a handful
of counters and one histogram need here. `metrics.go` renders the same
wire format by hand — any real Prometheus can scrape `/metrics` directly
— with zero external dependencies.

**Why does `Timeout` matter?** The original version of `processJob`
called `handler(context.Background(), job)` unconditionally — a job's
declared timeout was accepted over the API but silently never enforced,
so a stuck handler could hang a worker forever. `processJob` now builds
`context.WithTimeout` per attempt when `job.Timeout` is set, and
`queue_test.go` proves it: a handler that would take 2 seconds, given a
50ms timeout, fails at ~50ms instead of running to completion.

**Why a separate dead-letter queue instead of just leaving jobs
`Failed`?** A `Failed` status mixed into the main job list makes it hard
to see, at a glance, what actually needs a human to look at it — and
there was no way to retry a fixed bug without re-submitting the job by
hand. The DLQ keeps permanent failures inspectable and one API call away
from being replayed.

**Why deduplication is enqueue-time, not handler-time.** Suppressing a
duplicate inside the handler means the duplicate already consumed a
worker slot and a retry budget before being thrown away. Checking at
`Enqueue` rejects it before it ever enters a queue.

## Testing

```bash
go test ./... -v          # all tests
go test ./... -race       # race detector (clean)
gofmt -l .                # formatting
go vet ./...              # static analysis
```

20 tests cover priority ordering, retry/backoff, timeout enforcement,
delayed scheduling, rate limiting, deduplication (including window
expiry), success/failure chaining, dead-letter archival and replay,
pagination, and metrics output. Two more cover store-dispatch mode:
followers only persist jobs, and a resigning leader hands unstarted
claims back.

The HA tests need real infrastructure and skip without it:

```bash
docker compose up -d etcd1 etcd2 etcd3 postgres --wait
ETCD_ENDPOINTS=127.0.0.1:2379 \
TEST_DATABASE_URL="postgres://goqueue:goqueue@localhost:5432/goqueue_test?sslmode=disable" \
  go test -run 'Election|Fence|ClaimDue' -v   # TEST_DATABASE_URL gets wiped: use a scratch DB
```

- **Election:** only one leader at a time; failover after `Resign` is
  immediate (not TTL-bound); failover after a simulated crash waits out
  the lease.
- **Fencing:** a stale epoch claims nothing after takeover, orphaned
  jobs are requeued, and epochs can't go backwards.
- **Claims:** priority order, future `run_at` held back, and every job
  field survives the store round-trip.

## Known limitations

- The concurrency limiter's per-type limit must be set before jobs of
  that type start processing; changing it mid-flight isn't supported.
- Webhook delivery is fire-and-forget — a failed delivery is logged, not
  retried.
- The in-memory store's dead-letter IDs are a simple incrementing
  sequence; the Postgres store derives an ID from the job ID. Neither is
  guaranteed globally unique across store swaps.
- In HA mode, delivery is at-least-once (see
  [High availability](#high-availability-leader-election)). A requeued
  job also starts its retry budget over.
- In HA mode, deduplication is still per-instance memory. Two requests
  with the same `dedupe_key` that hit different instances are both
  accepted. Making it cluster-wide would need a unique constraint in
  Postgres or a key in etcd.
- Fencing covers claims, not every later write. A zombie leader that
  finishes a job after being fenced can still record its result.
