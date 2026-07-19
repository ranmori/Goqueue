package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	store := NewInMemoryStore()
	q := NewQueue(store, 1) // single worker: makes ordering assertions deterministic
	return q
}

func TestPriorityOrdering(t *testing.T) {
	q := newTestQueue(t)

	low := NewJob("noop", nil, PriorityLow, 0)
	normal := NewJob("noop", nil, PriorityNormal, 0)
	high := NewJob("noop", nil, PriorityHigh, 0)

	// Enqueue in low->normal->high order; fetchJob should still return
	// high first regardless of arrival order.
	for _, j := range []*Job{low, normal, high} {
		if err := q.Enqueue(j); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	got := q.fetchJob()
	if got.ID != high.ID {
		t.Fatalf("expected high priority job first, got priority %v", got.Priority)
	}
	got = q.fetchJob()
	if got.ID != normal.ID {
		t.Fatalf("expected normal priority job second, got priority %v", got.Priority)
	}
	got = q.fetchJob()
	if got.ID != low.ID {
		t.Fatalf("expected low priority job third, got priority %v", got.Priority)
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	q := newTestQueue(t)

	var attempts int32
	q.RegisterHandler("flaky", func(ctx context.Context, job *Job) error {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			return errors.New("simulated transient failure")
		}
		return nil
	})

	job := NewJob("flaky", nil, PriorityNormal, 5)
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	q.processJob(job)

	if job.Status != StatusDone {
		t.Fatalf("expected job to finish Done, got %v (error: %s)", job.Status, job.Error)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", attempts)
	}
}

func TestRetriesExhausted(t *testing.T) {
	q := newTestQueue(t)

	q.RegisterHandler("always_fails", func(ctx context.Context, job *Job) error {
		return errors.New("permanent failure")
	})

	job := NewJob("always_fails", nil, PriorityNormal, 2)
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	q.processJob(job)

	if job.Status != StatusFailed {
		t.Fatalf("expected job to end Failed, got %v", job.Status)
	}
	if job.Retries != job.MaxRetries+1 {
		t.Fatalf("expected %d recorded attempts, got %d", job.MaxRetries+1, job.Retries)
	}
}

func TestPerJobTimeoutIsEnforced(t *testing.T) {
	q := newTestQueue(t)

	q.RegisterHandler("slow", func(ctx context.Context, job *Job) error {
		select {
		case <-time.After(2 * time.Second):
			return nil // would eventually succeed if not cancelled
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	job := NewJob("slow", nil, PriorityNormal, 0)
	job.Timeout = 50 * time.Millisecond

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	start := time.Now()
	q.processJob(job)
	elapsed := time.Since(start)

	if job.Status != StatusFailed {
		t.Fatalf("expected job to fail due to timeout, got %v", job.Status)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("job took %v — timeout was not enforced (handler ran to completion)", elapsed)
	}
}

func TestScheduledJobIsHeldUntilDue(t *testing.T) {
	q := newTestQueue(t)
	scheduler := NewScheduler(q.release, 20*time.Millisecond)
	q.SetScheduler(scheduler)
	scheduler.Start()
	defer scheduler.Stop()

	job := NewJob("noop", nil, PriorityNormal, 0)
	job.RunAt = time.Now().UTC().Add(150 * time.Millisecond)

	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Immediately after enqueue, the job must NOT be sitting in a
	// worker-facing channel yet — it should still be held by the scheduler.
	select {
	case <-q.normalQueue:
		t.Fatal("delayed job was dispatched before its RunAt")
	default:
	}
	if scheduler.Len() != 1 {
		t.Fatalf("expected scheduler to be holding 1 job, got %d", scheduler.Len())
	}

	time.Sleep(250 * time.Millisecond)

	select {
	case got := <-q.normalQueue:
		if got.ID != job.ID {
			t.Fatalf("dispatched wrong job")
		}
	default:
		t.Fatal("job was never released to the queue after its RunAt passed")
	}
}

func TestEnqueueRejectsDelayedJobWithoutScheduler(t *testing.T) {
	q := newTestQueue(t) // no scheduler wired up

	job := NewJob("noop", nil, PriorityNormal, 0)
	job.RunAt = time.Now().UTC().Add(time.Hour)

	if err := q.Enqueue(job); err == nil {
		t.Fatal("expected an error enqueueing a delayed job with no scheduler configured")
	}
}

func TestFailedJobMovesToDeadLetter(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("always_fails", func(ctx context.Context, job *Job) error {
		return errors.New("boom")
	})

	job := NewJob("always_fails", map[string]string{"x": "1"}, PriorityNormal, 0)
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	q.processJob(job)

	entries, err := q.store.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(entries))
	}
	if entries[0].JobID != job.ID {
		t.Fatalf("dead letter references wrong job id: got %s want %s", entries[0].JobID, job.ID)
	}
	if entries[0].Payload["x"] != "1" {
		t.Fatalf("dead letter did not preserve payload")
	}
}

func TestDeadLetterReplay(t *testing.T) {
	q := newTestQueue(t)
	var calls int32
	q.RegisterHandler("flaky_then_ok", func(ctx context.Context, job *Job) error {
		return errors.New("still failing")
	})

	job := NewJob("flaky_then_ok", nil, PriorityNormal, 0)
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	q.processJob(job)

	entries, _ := q.store.ListDeadLetters()
	if len(entries) != 1 {
		t.Fatalf("expected 1 dead letter before replay, got %d", len(entries))
	}

	// Simulate the operator fixing the downstream issue, then replaying.
	q.handlers["flaky_then_ok"] = func(ctx context.Context, job *Job) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}

	replayed := NewJob(entries[0].Type, entries[0].Payload, entries[0].Priority, entries[0].MaxRetries)
	if err := q.Enqueue(replayed); err != nil {
		t.Fatalf("replay enqueue failed: %v", err)
	}
	q.processJob(replayed)

	if replayed.Status != StatusDone {
		t.Fatalf("expected replayed job to succeed, got %v", replayed.Status)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected the fixed handler to run once, got %d", calls)
	}
}

func TestConcurrencyLimitIsEnforced(t *testing.T) {
	q := newTestQueue(t)
	q.SetConcurrencyLimit("limited", 2)

	var inFlight int32
	var maxObserved int32
	release := make(chan struct{})
	var wg sync.WaitGroup

	q.RegisterHandler("limited", func(ctx context.Context, job *Job) error {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxObserved)
			if n <= old || atomic.CompareAndSwapInt32(&maxObserved, old, n) {
				break
			}
		}
		<-release
		atomic.AddInt32(&inFlight, -1)
		return nil
	})

	jobs := make([]*Job, 5)
	for i := range jobs {
		jobs[i] = NewJob("limited", nil, PriorityNormal, 0)
		if err := q.Enqueue(jobs[i]); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	for _, j := range jobs {
		wg.Add(1)
		go func(job *Job) {
			defer wg.Done()
			q.processJob(job)
		}(j)
	}

	// Give the limited handlers time to pile up against the semaphore.
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&maxObserved); got > 2 {
		t.Fatalf("concurrency limit of 2 was exceeded: observed %d simultaneous executions", got)
	}
	close(release)
	wg.Wait()
}

func TestDeduplicationSuppressesRepeat(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("noop", func(ctx context.Context, job *Job) error { return nil })

	first := NewJob("noop", nil, PriorityNormal, 0)
	first.DedupeKey = "email:alice@example.com"
	first.DedupeWindow = time.Minute

	second := NewJob("noop", nil, PriorityNormal, 0)
	second.DedupeKey = "email:alice@example.com"
	second.DedupeWindow = time.Minute

	if err := q.Enqueue(first); err != nil {
		t.Fatalf("first enqueue should have succeeded: %v", err)
	}
	err := q.Enqueue(second)
	if err == nil {
		t.Fatal("expected second enqueue with the same dedupe key to be rejected")
	}
	if !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("expected ErrDuplicateJob, got: %v", err)
	}

	// A different key must not be affected by someone else's dedupe window.
	unrelated := NewJob("noop", nil, PriorityNormal, 0)
	unrelated.DedupeKey = "email:bob@example.com"
	unrelated.DedupeWindow = time.Minute
	if err := q.Enqueue(unrelated); err != nil {
		t.Fatalf("unrelated dedupe key should not be blocked: %v", err)
	}
}

func TestDeduplicationExpiresAfterWindow(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("noop", func(ctx context.Context, job *Job) error { return nil })

	first := NewJob("noop", nil, PriorityNormal, 0)
	first.DedupeKey = "sms:+1555"
	first.DedupeWindow = 50 * time.Millisecond
	if err := q.Enqueue(first); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	second := NewJob("noop", nil, PriorityNormal, 0)
	second.DedupeKey = "sms:+1555"
	second.DedupeWindow = 50 * time.Millisecond
	if err := q.Enqueue(second); err != nil {
		t.Fatalf("expected dedupe window to have expired, got: %v", err)
	}
}

func TestOnSuccessChainFires(t *testing.T) {
	q := newTestQueue(t)

	followUpDone := make(chan struct{})
	q.RegisterHandler("step_one", func(ctx context.Context, job *Job) error { return nil })
	q.RegisterHandler("step_two", func(ctx context.Context, job *Job) error {
		close(followUpDone)
		return nil
	})

	job := NewJob("step_one", nil, PriorityNormal, 0)
	job.OnSuccess = &JobTemplate{Type: "step_two", Priority: PriorityNormal}
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	dispatched := q.fetchJob()
	if dispatched == nil || dispatched.ID != job.ID {
		t.Fatal("failed to fetch the originally enqueued job")
	}
	q.processJob(dispatched)
	chained := q.fetchJob()
	if chained == nil || chained.Type != "step_two" {
		t.Fatal("expected the OnSuccess job to have been enqueued")
	}
	q.processJob(chained)

	select {
	case <-followUpDone:
	default:
		t.Fatal("chained step_two handler never ran")
	}
}

func TestOnFailureChainFiresNotOnSuccess(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("will_fail", func(ctx context.Context, job *Job) error {
		return errors.New("nope")
	})
	q.RegisterHandler("alert", func(ctx context.Context, job *Job) error { return nil })

	job := NewJob("will_fail", nil, PriorityNormal, 0)
	job.OnSuccess = &JobTemplate{Type: "should_not_run", Priority: PriorityNormal}
	job.OnFailure = &JobTemplate{Type: "alert", Priority: PriorityNormal}
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	dispatched := q.fetchJob()
	if dispatched == nil || dispatched.ID != job.ID {
		t.Fatal("failed to fetch the originally enqueued job")
	}
	q.processJob(dispatched)

	chained := q.fetchJob()
	if chained == nil {
		t.Fatal("expected an OnFailure job to be enqueued")
	}
	if chained.Type != "alert" {
		t.Fatalf("expected OnFailure job (alert), got %s — OnSuccess must not fire on a failed job", chained.Type)
	}
}

func TestWebhookNotifiesOnCompletion(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("noop", func(ctx context.Context, job *Job) error { return nil })

	received := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- "called"
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	job := NewJob("noop", nil, PriorityNormal, 0)
	job.WebhookURL = srv.URL
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	q.processJob(job)

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("webhook was never called after job completion")
	}
}

func TestPaginationLimitAndOffset(t *testing.T) {
	store := NewInMemoryStore()
	for i := 0; i < 5; i++ {
		j := NewJob("noop", nil, PriorityNormal, 0)
		j.ID = "job_" + strconv.Itoa(i)
		j.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Second) // ascending creation time
		_ = store.Save(j)
	}

	// Newest first (CreatedAt desc), page size 2.
	page1, err := store.List(JobFilter{Limit: 2})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 results, got %d", len(page1))
	}
	if page1[0].ID != "job_4" || page1[1].ID != "job_3" {
		t.Fatalf("expected newest-first ordering [job_4, job_3], got [%s, %s]", page1[0].ID, page1[1].ID)
	}

	page2, err := store.List(JobFilter{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(page2) != 2 || page2[0].ID != "job_2" {
		t.Fatalf("expected second page to start at job_2, got %+v", page2)
	}

	beyondEnd, err := store.List(JobFilter{Limit: 2, Offset: 10})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(beyondEnd) != 0 {
		t.Fatalf("expected empty result past the end, got %d", len(beyondEnd))
	}
}

func TestMetricsExposesJobCounts(t *testing.T) {
	q := newTestQueue(t)
	q.RegisterHandler("noop", func(ctx context.Context, job *Job) error { return nil })
	q.RegisterHandler("bad", func(ctx context.Context, job *Job) error { return errors.New("x") })

	ok := NewJob("noop", nil, PriorityNormal, 0)
	_ = q.Enqueue(ok)
	q.processJob(ok)

	bad := NewJob("bad", nil, PriorityNormal, 0)
	_ = q.Enqueue(bad)
	q.processJob(bad)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	q.Metrics().ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `goqueue_jobs_total{type="noop",status="done"} 1`) {
		t.Fatalf("missing expected done counter, got:\n%s", body)
	}
	if !strings.Contains(body, `goqueue_jobs_total{type="bad",status="failed"} 1`) {
		t.Fatalf("missing expected failed counter, got:\n%s", body)
	}
	if !strings.Contains(body, "goqueue_job_duration_seconds_bucket") {
		t.Fatalf("missing duration histogram, got:\n%s", body)
	}
}
