package main

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Metrics is a small, dependency-free counter/histogram store that
// renders in the standard Prometheus text exposition format.
//
// Deliberately not using github.com/prometheus/client_golang here: its
// registry/histogram machinery pulls in a chain of transitive
// dependencies (client_model, common, google.golang.org/protobuf) that's
// disproportionate to what a handful of counters and one histogram
// actually need for a service this size. This produces the same wire
// format with zero external dependencies — any real Prometheus can
// scrape /metrics directly.
type Metrics struct {
	mu sync.Mutex

	jobsTotal    map[metricKey]int64
	retriesTotal map[string]int64

	durationBuckets map[string][]int64
	durationSum     map[string]float64
	durationCount   map[string]int64

	queueDepthFn func() map[string]int
	scheduledFn  func() int
}

type metricKey struct {
	jobType string
	status  string
}

var histogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

func NewMetrics() *Metrics {
	return &Metrics{
		jobsTotal:       make(map[metricKey]int64),
		retriesTotal:    make(map[string]int64),
		durationBuckets: make(map[string][]int64),
		durationSum:     make(map[string]float64),
		durationCount:   make(map[string]int64),
	}
}

// SetQueueDepthFunc wires in a callback invoked at scrape time so queue
// depth is always current rather than a stale snapshot from whenever it
// last changed.
func (m *Metrics) SetQueueDepthFunc(fn func() map[string]int) {
	m.queueDepthFn = fn
}

func (m *Metrics) SetScheduledFunc(fn func() int) {
	m.scheduledFn = fn
}

// RecordCompletion should be called once per job, when it reaches a
// terminal status (Done or Failed).
func (m *Metrics) RecordCompletion(jobType string, status JobStatus, retries int, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobsTotal[metricKey{jobType, status.String()}]++
	if retries > 0 {
		m.retriesTotal[jobType] += int64(retries)
	}

	buckets, ok := m.durationBuckets[jobType]
	if !ok {
		buckets = make([]int64, len(histogramBuckets))
		m.durationBuckets[jobType] = buckets
	}
	seconds := duration.Seconds()
	for i, le := range histogramBuckets {
		if seconds <= le {
			buckets[i]++
		}
	}
	m.durationSum[jobType] += seconds
	m.durationCount[jobType]++
}

// ServeHTTP renders all metrics in Prometheus text exposition format.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder

	b.WriteString("# HELP goqueue_jobs_total Total jobs processed, by type and final status.\n")
	b.WriteString("# TYPE goqueue_jobs_total counter\n")
	keys := make([]metricKey, 0, len(m.jobsTotal))
	for k := range m.jobsTotal {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].jobType != keys[j].jobType {
			return keys[i].jobType < keys[j].jobType
		}
		return keys[i].status < keys[j].status
	})
	for _, k := range keys {
		fmt.Fprintf(&b, "goqueue_jobs_total{type=%q,status=%q} %d\n", k.jobType, k.status, m.jobsTotal[k])
	}

	b.WriteString("# HELP goqueue_retries_total Total retry attempts, by job type.\n")
	b.WriteString("# TYPE goqueue_retries_total counter\n")
	types := make([]string, 0, len(m.retriesTotal))
	for t := range m.retriesTotal {
		types = append(types, t)
	}
	sort.Strings(types)
	for _, t := range types {
		fmt.Fprintf(&b, "goqueue_retries_total{type=%q} %d\n", t, m.retriesTotal[t])
	}

	b.WriteString("# HELP goqueue_job_duration_seconds Job execution duration in seconds, by type.\n")
	b.WriteString("# TYPE goqueue_job_duration_seconds histogram\n")
	durTypes := make([]string, 0, len(m.durationBuckets))
	for t := range m.durationBuckets {
		durTypes = append(durTypes, t)
	}
	sort.Strings(durTypes)
	for _, t := range durTypes {
		buckets := m.durationBuckets[t]
		for i, le := range histogramBuckets {
			fmt.Fprintf(&b, "goqueue_job_duration_seconds_bucket{type=%q,le=\"%g\"} %d\n", t, le, buckets[i])
		}
		fmt.Fprintf(&b, "goqueue_job_duration_seconds_bucket{type=%q,le=\"+Inf\"} %d\n", t, m.durationCount[t])
		fmt.Fprintf(&b, "goqueue_job_duration_seconds_sum{type=%q} %g\n", t, m.durationSum[t])
		fmt.Fprintf(&b, "goqueue_job_duration_seconds_count{type=%q} %d\n", t, m.durationCount[t])
	}

	if m.queueDepthFn != nil {
		b.WriteString("# HELP goqueue_queue_depth Current number of jobs waiting, by priority lane.\n")
		b.WriteString("# TYPE goqueue_queue_depth gauge\n")
		depths := m.queueDepthFn()
		lanes := make([]string, 0, len(depths))
		for lane := range depths {
			lanes = append(lanes, lane)
		}
		sort.Strings(lanes)
		for _, lane := range lanes {
			fmt.Fprintf(&b, "goqueue_queue_depth{priority=%q} %d\n", lane, depths[lane])
		}
	}

	if m.scheduledFn != nil {
		b.WriteString("# HELP goqueue_scheduled_jobs Jobs currently held by the scheduler awaiting their run_at.\n")
		b.WriteString("# TYPE goqueue_scheduled_jobs gauge\n")
		fmt.Fprintf(&b, "goqueue_scheduled_jobs %d\n", m.scheduledFn())
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
