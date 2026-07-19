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

## Running it

```bash
go run .
# or, with Postgres:
DATABASE_URL="postgres://user:pass@localhost:5432/goqueue?sslmode=disable" go run .
```

The server listens on `:8080`. Three example handlers are registered at
startup: `email`, `webhook`, and `report` (see `main.go`).

## API

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

16 tests cover priority ordering, retry/backoff, timeout enforcement,
delayed scheduling, rate limiting, deduplication (including window
expiry), success/failure chaining, dead-letter archival and replay,
pagination, and metrics output.

## Known limitations

- The concurrency limiter's per-type limit must be set before jobs of
  that type start processing; changing it mid-flight isn't supported.
- Webhook delivery is fire-and-forget — a failed delivery is logged, not
  retried.
- The in-memory store's dead-letter IDs are a simple incrementing
  sequence; the Postgres store derives an ID from the job ID. Neither is
  guaranteed globally unique across store swaps.
