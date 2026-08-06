# Running BatchFlow in production

The page for whoever is on call. The [Guide](Guide.md) explains the concepts;
this one assembles the operational facts that are otherwise scattered across
rustdoc.

---

## 1. Choosing a metadata store

The metadata store **is** the exactly-once guarantee. Restart is emergent from
what was recorded, so a lost `StepExecution` commit means a restarted job
re-does or skips work that had already committed.

| Store | Use it when | Non-negotiable configuration |
|---|---|---|
| `InMemoryJobRepository` | Tests, examples, a single-shot script | Nothing is durable. A restart across processes is impossible. |
| `batchflow-postgres` | **The default recommendation** | Nothing special. `Tx` is a real `sqlx` transaction, so a chunk's rows, counters and bookmark commit together. |
| `batchflow-redis` | You already run Redis and accept the constraints below | `appendonly yes`, `appendfsync always`, `maxmemory-policy noeviction` |

### Redis, specifically

Three settings, and none of them is a preference:

```
appendonly yes
appendfsync always
maxmemory-policy noeviction
```

- **`appendfsync always`.** The default RDB snapshotting can lose the last
  seconds of writes. For most Redis workloads that is a fine trade; for this
  one it is data loss, because the write that was lost is the bookmark.
- **`maxmemory-policy noeviction`.** Under `allkeys-lru` Redis will evict *any*
  key under memory pressure, including a step execution. From v0.1.1 an evicted
  record is detected and reported rather than read back as "this step has never
  run" — but the run still fails. Eviction of batch metadata has no safe
  interpretation.

**Redis Cluster is not supported.** The Lua scripts declare keys that hash to
different slots and construct further keys inside the script, both of which
Cluster rejects. Use a single Redis instance, or Postgres.

If you cannot meet all three, use `batchflow-postgres`. That is the
recommendation, not a footnote.

---

## 2. Sizing the commit interval

`chunk_size` is three decisions at once, and only the first is about speed.

**Throughput.** Measured framework overhead is ~3.9 ns per item plus ~102 ns per
chunk ([Performance](Performance.md)). The curve flattens by about 1,000: going
from 1 to 100 is a 21× improvement, from 1,000 to 10,000 is 1.02×. Since a
Postgres `COMMIT` is 100 µs–1 ms, **the framework is never the reason to raise
the chunk size** past a few hundred; the commit is.

**Memory.** Peak is roughly

```
chunk_size × (size_of::<ReaderItem> + size_of::<ProcessorOutput>)
```

plus whatever those items own on the heap. A chunk of 10,000 wide rows is
10,000 wide rows resident, twice. The framework does not bound this — the
commit interval is the only knob, and it controls memory and durability
together.

**Blast radius.** A failed chunk rolls back entirely and is re-read on restart.
A larger interval means more work redone per failure, and a longer wait for a
[graceful stop](#4-stopping-a-job) to take effect — a stop lands at the next
commit boundary, so a ten-minute chunk means a ten-minute shutdown.

**A reasonable default is 100–1,000.** Start at 500, measure, and move it for
one of the three reasons above rather than out of optimism.

---

## 3. What the metadata store costs

Every chunk commit issues one `UPDATE step_execution` inside the chunk's
transaction. That is one row version per chunk, forever.

A 10M-row job at `chunk_size = 100` writes **100,000 row versions** for a single
`step_execution` row. Autovacuum's defaults (`scale_factor = 0.2` of a tiny
table) will not collect them promptly.

```sql
-- Recommended for any deployment running more than a handful of jobs a day.
ALTER TABLE step_execution SET (fillfactor = 70);
ALTER TABLE step_execution SET (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 1000
);
```

`fillfactor` leaves room on the page so the updates are HOT — the same page,
no index churn. It works here because no indexed column changes during a chunk
commit.

**There is no retention API yet.** The tables grow without bound; pruning is
currently your `DELETE`, and there is one rule that is easy to get wrong:

> Deleting a `Completed` execution while keeping its `job_instance` row makes
> that instance launchable again, because the FR-4.4 gate reads
> `last_execution`. Delete the instance too, or keep its terminal execution.

Tracked as PROD-3 in [the audit](audit/FINDINGS.md).

---

## 4. Stopping a job

**Do not just drop the job's future.** The unwind skips the write that records
a terminal status, leaving `job_execution.status = 'STARTED'` — and every later
launch of that instance is then refused with `JobExecutionAlreadyRunning`
naming a process that has already exited.

Use a `StopSignal`. The job finishes the chunk in flight, commits it, records
`Stopped`, and returns `BatchError::Stopped`. `Stopped` is a restartable status,
so the next launch resumes from the bookmark with no operator action.

```rust
use batchflow::{JobLauncher, StopSignal};

let stop = StopSignal::new();
let launcher = JobLauncher::new(repository).with_stop_signal(stop.clone());

tokio::spawn({
    let stop = stop.clone();
    async move {
        let mut term = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )?;
        term.recv().await;
        stop.request();
        Ok::<_, std::io::Error>(())
    }
});

match launcher.run(&mut job, &parameters).await {
    Ok(execution)              => { /* completed */ }
    Err(BatchError::Stopped)   => { /* clean shutdown; exit 0 */ }
    Err(error)                 => { /* real failure; exit non-zero */ }
}
```

**Set `terminationGracePeriodSeconds` above your worst-case chunk duration**, or
Kubernetes will SIGKILL before the stop lands and you are back to the stale
`STARTED` row.

---

## 5. Recovering a wedged instance

A job whose process died without recording a terminal status — SIGKILL, a
machine loss, an OOM kill, or a panic under `panic = "abort"` — leaves an
execution at `STARTED`. Clearing it is an operator decision:

```rust
launcher.repository().abandon_execution(execution_id).await?;
```

`abandon_execution` asserts the process is dead. **It does not verify that**, and
if you are wrong the old process keeps writing alongside the new one. Confirm
the process is gone first.

`Completed` executions cannot be abandoned; that refusal is what stops a
finished instance from being made relaunchable in two calls.

An ordinary panic in user code does *not* need this — the engine catches it at
its step and job boundaries and records `Failed`, which restarts normally.

### Finding the candidates

Every record carries a heartbeat, so a stale one is a query rather than a guess:

```sql
SELECT e.id, e.instance_id, e.created_at, e.last_updated, now() - e.last_updated AS silent_for
  FROM job_execution e
 WHERE e.status IN ('STARTING', 'STARTED')
   AND e.last_updated < now() - interval '15 minutes'
 ORDER BY e.last_updated;
```

A chunk commit writes its step execution, and the trigger moves `last_updated`
with it — so a healthy step keeps its heartbeat moving and a dead process does
not. Size the interval above your worst-case chunk duration, or you will abandon
a job that is merely slow.

**This still does not verify the process is gone.** It narrows the candidates;
the judgement is yours. An automatic reaper is future work — see
[PROD-4](audit/FINDINGS.md).

---

## 6. What to alert on

The engine emits through the `metrics` facade and records nothing until you
install a recorder. `batchflow-metrics::install()` is that recorder, with
histogram buckets that aggregate across processes.

```rust
let handle = batchflow_metrics::install()?;   // once, at startup
// serve handle.render() from your existing HTTP stack
```

| Signal | Query | Why it matters |
|---|---|---|
| **Jobs stuck in flight** | `sum(batchflow_jobs_started_total) - sum(batchflow_jobs_finished_total)` | Should return to 0 between runs. A permanent floor of 1 is a wedged instance — see §5. |
| **Failure rate** | `rate(batchflow_jobs_finished_total{status="failed"}[1h])` | `status="stopped"` is a deliberate shutdown and must **not** be in the same alert. |
| **Chunk scanning** | `rate(batchflow_chunk_scans_total[15m]) > 0` | Chunks are failing on a single bad row, and every good item in them is being written twice. Worth an alert, not just a graph. |
| **Skip rate** | `rate(batchflow_items_skipped_total[15m])` | Data quality. Break out by `phase` — a bad input row (`read`) and a bad transform (`process`) are different incidents. |
| **Retry rate** | `rate(batchflow_chunk_retries_total[15m])` | Backend contention. A sustained rise usually precedes failures. |
| **Chunk latency** | `histogram_quantile(0.99, rate(batchflow_chunk_duration_seconds_bucket[5m]))` | Also your shutdown time — see §4. |
| **Overlapping ticks** | `rate(batchflow_triggers_total{outcome="already_running"}[1d])` | One overlap is a slow night. A run of them means the job no longer fits its schedule, and it is invisible in the job's own metrics because nothing ran. |

Counters are only published for work that committed, so
`sum(batchflow_items_written_total)` reconciles with `sum(write_count)` in the
metadata store. If they disagree, trust the store.

### Asking the store directly

Metrics are process-local and disappear with the process; the metadata store
outlives it. Since PROD-2 it can answer the questions that actually get asked
after an incident:

```sql
-- What ran last night, how long did it take, and why did anything fail?
SELECT i.job_name, i.parameters, e.status, e.created_at,
       e.ended_at - e.created_at AS duration, e.exit_message
  FROM job_execution e
  JOIN job_instance i ON i.id = e.instance_id
 WHERE e.created_at >= current_date - 1
 ORDER BY e.created_at DESC;

-- Which step was it, and where did its bookmark get to?
SELECT step_name, status, read_count, write_count, skip_count,
       ended_at - created_at AS duration, exit_message, execution_context
  FROM step_execution
 WHERE job_execution_id = $1
 ORDER BY id;
```

For correlating one specific run, use tracing rather than metrics: the `job` and
`step` spans carry `instance_id`, `execution_id` and `step_execution_id`, which
are deliberately absent from every metric label (one label value per run would
mint one time series per run, kept forever).

---

## 7. Writing readers and writers that behave

**Flush in `close`.** `ItemWriter::close` is called once when the step ends, on
both the success and failure paths, and an error there fails the step. A
buffered writer that flushes only on `Drop` reports success and writes a
truncated file.

**Do not block the runtime.** `ItemProcessor::process` runs on a worker thread.
CPU-bound transforms, synchronous I/O and C libraries must go through
`tokio::task::spawn_blocking`, or they stall every other task on that worker —
including the metadata store's connections.

**Record a bookmark, or you are not restartable.** `ItemReader::open` and
`update` have default bodies that do nothing, which is a valid choice and a
silent one. A reader that does not override them restarts from the beginning.

**Prefer a transactional writer.** `Unmanaged<W>` is an explicit acceptance of
at-least-once for that step. It is the right answer for S3 or a CSV; it is not
the right answer for the database the metadata already lives in.

**Enable `scan_on_write_failure` deliberately.** On a write failure the chunk is
re-written one item at a time to find the bad row, then the survivors are
written again — `N + 1` transactions, every good item written twice, on the
failure path only. With an `Unmanaged` writer the identifying pass really
delivers, so a 1,000-item chunk with one bad row delivers roughly 2,000 items.

---

## 8. Running more than one replica

**Safe.** The launch gate is atomic: `JobRepository::start_execution` decides
whether an instance may run *and* opens the execution as one indivisible
operation, so two replicas racing the same instance produce exactly one run and
the loser is refused with `JobExecutionAlreadyRunning` (or
`JobInstanceAlreadyComplete`, if the winner finished first).

How each backend achieves it:

| Backend | Mechanism |
|---|---|
| Postgres | `SELECT … FOR UPDATE` on the `job_instance` row, with the gate and the insert in one transaction. The lock is per instance, so unrelated jobs never contend. |
| Redis | One Lua script; Redis's single-threaded execution does the rest. |
| In-memory | One `Mutex` acquisition with no `.await` inside it. |

The conformance suite pins this for every backend
(`only_one_of_two_concurrent_launches_wins`), so a third-party `JobRepository`
that gets it wrong fails its test run rather than duplicating work in
production.

What this does **not** give you is parallelism *within* one instance: the loser
does not queue, it is refused. Two replicas are a redundancy and failover
arrangement, not a way to make one job go faster.

---

## 9. Scheduling

`JobParameters` is what makes a schedule idempotent. Derive them from the tick —
`date=2026-08-06` for a nightly job, `hour=2026-08-06T14` for an hourly one — so
that a re-fired tick resolves to the same instance and is refused, while the
next tick resolves to a new one and runs.

- **Constant parameters** give a job that runs exactly once, ever.
- **Parameters containing a timestamp** give a job with no deduplication and no
  restart, because every attempt is a new instance.

Neither is usually what a schedule wants.

Use `batchflow_scheduler::trigger`, which turns the launcher's two *refusal*
errors into an `Outcome` and leaves every real error alone. Under an external
scheduler, map a refusal to exit 0 — a `CronJob` that exits non-zero here is
restarted by the controller into the same refusal.

---

## 10. Known gaps

Current as of 0.1.1. The full list with severities and proposed fixes is in
[`docs/audit/FINDINGS.md`](audit/FINDINGS.md); these are the ones that change
what an operator should do.

- **No automatic reaper.** The heartbeat makes stale executions findable (§5),
  but nothing acts on it: `abandon_execution` is still a human decision.
- **No timeouts.** A writer that hangs hangs the job. Give your clients their
  own timeouts.
- **No retention API.** See §3.
- **No parallel or partitioned steps.** One job is one task, steps run in
  sequence.
