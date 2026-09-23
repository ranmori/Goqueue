# Failover demo: `kill -9` the leader

Two GoQueue instances share one Postgres and campaign for leadership on
a 3-member etcd cluster (session TTL 5s). I sent 12 jobs through the
**follower**, then killed the **leader** with SIGKILL while two of those
jobs were still running on it. It got no chance to resign, flush, or say
goodbye.

To reproduce it: `./scripts/failover-demo.sh` (Docker Compose), or
`./scripts/failover-demo.sh stop` for the graceful SIGTERM path.

> Recorded 2026-09-22 on Windows 11 with native etcd v3.5.21 binaries
> (three members on localhost) and Postgres 18. The Docker Compose script
> runs the same sequence. Logs are trimmed to the relevant lines, and
> times are local (UTC+3).

## Timeline

| time         | instance  | event |
|--------------|-----------|-------|
| 19:04:56.915 | goqueue-1 | `became leader` epoch **28**, `requeued_orphaned_jobs: 0` |
| 19:04:56.992 | goqueue-2 | `observed leader` goqueue-1, standing by |
| 19:05:01.134 | goqueue-1 | starts job `job_4efe58a1…` (webhook #11) |
| 19:05:01.171 | goqueue-1 | starts job `job_8713fee9…` (webhook #12) |
| **19:05:01.564** | goqueue-1 | **`kill -9`**: process gone, both jobs mid-flight |
| 19:05:06.800 | goqueue-2 | `observed leader` goqueue-2, epoch **29** |
| 19:05:06.807 | goqueue-2 | `became leader` epoch 29, **`requeued_orphaned_jobs: 2`** |
| 19:05:06.812 | goqueue-2 | re-runs #12, then #11. Both `job done` |

**The follower took over 5.24s after the kill**, which is the 5s lease
TTL plus the poll loop. No job was lost: `/stats` afterwards showed
`{"done":7,"failed":5,"pending":0,"running":0}`. All 12 jobs reached a
terminal state; the 5 failures are the demo `webhook` handler's
deliberate 30% error rate, not failover.

## Script output

```
--- /leader
{"mode":"ha","id":"goqueue-1","is_leader":true,"leader":"goqueue-1","epoch":28}
{"mode":"ha","id":"goqueue-2","is_leader":false,"leader":"goqueue-1"}

--- killing leader goqueue-1 with SIGKILL-equivalent (TerminateProcess)
16:05:01.564
demo.sh: line 18:  2104 Killed   INSTANCE_ID=$1 ADDR=:$2 goqueue.exe
--- goqueue-2 is leader after 5255ms
{"mode":"ha","id":"goqueue-2","is_leader":true,"leader":"goqueue-2","epoch":29}

--- /stats
{"cancelled":0,"done":7,"failed":5,"pending":0,"running":0}
```

## Leader (goqueue-1): last lines before it died

```json
{"time":"19:04:56.908","msg":"joined leader election","instance_id":"goqueue-1","lease_id":"426ba0c9d9082b1c","ttl_seconds":5}
{"time":"19:04:56.915","msg":"became leader","instance_id":"goqueue-1","epoch":28,"requeued_orphaned_jobs":0}
{"time":"19:04:56.915","msg":"dispatcher started","epoch":28}
  … 10 jobs processed …
{"time":"19:05:01.134","msg":"firing webhook","job_id":"job_4efe58a1-82af-4b64-bc07-47c65b946da2","url":"https://example.com/11"}
{"time":"19:05:01.171","msg":"firing webhook","job_id":"job_8713fee9-6620-42be-8466-05967baacf43","url":"https://example.com/12"}
{"time":"19:05:01.365","msg":"job failed permanently","job_id":"job_af680666-…","type":"webhook","retries":0}
                                   ← SIGKILL at 19:05:01.564; no further output
```

## Follower (goqueue-2): standby, then takeover

```json
{"time":"19:04:56.989","msg":"joined leader election","instance_id":"goqueue-2","lease_id":"426ba0c9d9082b1f","ttl_seconds":5}
{"time":"19:04:56.989","msg":"campaigning for leadership, standing by as follower","instance_id":"goqueue-2"}
{"time":"19:04:56.991","msg":"observed leader","leader":"goqueue-1","epoch":28,"instance_id":"goqueue-2"}
{"time":"19:05:06.800","msg":"observed leader","leader":"goqueue-2","epoch":29,"instance_id":"goqueue-2"}
{"time":"19:05:06.807","msg":"became leader","instance_id":"goqueue-2","epoch":29,"requeued_orphaned_jobs":2}
{"time":"19:05:06.807","msg":"dispatcher started","epoch":29}
{"time":"19:05:06.812","msg":"firing webhook","job_id":"job_8713fee9-6620-42be-8466-05967baacf43","url":"https://example.com/12"}
{"time":"19:05:06.957","msg":"firing webhook","job_id":"job_4efe58a1-82af-4b64-bc07-47c65b946da2","url":"https://example.com/11"}
{"time":"19:05:07.313","msg":"job done","job_id":"job_8713fee9-6620-42be-8466-05967baacf43","type":"webhook","attempts":1}
{"time":"19:05:07.460","msg":"job done","job_id":"job_4efe58a1-82af-4b64-bc07-47c65b946da2","type":"webhook","attempts":1}
```

## What to notice

- **Why ~5s and not instant:** a SIGKILLed process can't tell etcd it's
  leaving. etcd only knows once the lease stops being renewed for TTL
  seconds, and then it deletes the leader key. The follower is watching
  that key and wins the moment it disappears.
- **The epoch went 28 → 29.** It's the etcd create revision of each
  leader's election key, so it only moves forward. goqueue-2 wrote it to
  Postgres (`leader_fence`) *before* touching any jobs. From then on, a
  zombie goqueue-1 that somehow came back could claim nothing: its
  claims carry epoch 28 and the fence rejects them.
  `TestFenceRejectsStaleLeader` covers that path.
- **`requeued_orphaned_jobs: 2`** is jobs #11 and #12. Only the leader
  runs jobs, so anything still `running` at takeover belongs to a dead
  leader and goes back to `pending`. That makes the queue
  **at-least-once**: #11 and #12 started twice. Handlers must be
  idempotent.

## Graceful path (SIGTERM)

With `docker stop` (or Ctrl-C) the leader stops claiming, lets in-flight
jobs finish (10s grace), hands any unstarted claims back to `pending`,
and calls `election.Resign()`. The follower is elected as soon as etcd
processes the delete, without waiting out the TTL. In
`TestElectionFailoverOnResign` that took **7ms with a 10s TTL**:

```
=== RUN   TestElectionFailoverOnResign
    leader_test.go:100: failover after resign took 7.4234ms (session TTL is 10s)
--- PASS: TestElectionFailoverOnResign (0.36s)
=== RUN   TestElectionFailoverOnCrash
    leader_test.go:138: failover after crash took 1.9474345s (session TTL 2s)
--- PASS: TestElectionFailoverOnCrash (2.28s)
```
