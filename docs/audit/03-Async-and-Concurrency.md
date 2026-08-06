# Pass 5 — Async Review

Reviewed: tokio usage, `spawn`/`spawn_blocking`, cancellation, graceful
shutdown, back-pressure, timeouts, task leaks, deadlocks, cancellation safety,
structured concurrency, async trait design, blocking inside async.

## Baseline

Several things are right and should not be changed:

- **`Send` is asserted, not assumed.** `launcher_run_future_is_send` and
  `job_run_future_is_send` are compile-time tests. Auto-traits are semver-visible
  and lost silently; testing for them is correct and rare.
- **The backoff sleeps with `tokio::time::sleep`**, with a comment explaining
  that `std::thread::sleep` would stall every unrelated task on the worker. The
  `tokio` dependency in core is confined to `features = ["time"]` for exactly
  this.
- **Rollback happens before the backoff sleep** (`chunk.rs:329-340`), so the
  delay does not hold row locks and a pooled connection. This is the mistake
  nearly every hand-rolled retry loop makes, and the comment shows it was a
  deliberate ordering.
- **No `.await` while holding a `std::sync::Mutex` guard** anywhere. Twelve
  lock sites in `memory.rs`, all synchronous-only.
- **`start_paused = true` on the timing tests**, so backoff assertions cost no
  wall-clock. Correct use of tokio's test clock.

---

<a id="async-1"></a>
### ASYNC-1 — There is no cancellation path, and cancelling mid-chunk corrupts nothing but wedges everything

**Severity:** Critical · **Effort:** M (2–3 days) · **Files:** `chunk.rs`, `job.rs`, `launcher.rs`, `tasklet.rs`

Nothing in the crate accepts a cancellation token, a `select!`, a shutdown
channel or a stop flag. `grep -r 'CancellationToken\|shutdown\|select!' crates/*/src`
returns nothing.

**What actually happens when a caller drops the future** (SIGTERM handler,
`tokio::select!` with a shutdown branch, a test timeout, `JoinHandle::abort`):

| Where the drop lands | Consequence |
|---|---|
| Inside `reader.read()` | Nothing committed since the last chunk is lost. **Correct** — this is what the bookmark is for. |
| Inside `writer.write()` | `tx` is dropped. sqlx rolls back on drop. **Correct.** |
| Inside `commit.commit()` | The `COMMIT` may or may not have reached the server. **Ambiguous, and unavoidable** — this is the standard distributed-commit problem. |
| **After any of the above** | `Job::run` never resumes, so `update_step_execution(Failed)` never runs; `JobLauncher::run` never resumes, so `update_execution(Failed)` never runs. |

That last row is the finding. The metadata store is left with
`job_execution.status = 'STARTED'` and `step_execution.status = 'STARTED'`
**permanently**. On the next launch, `launcher.rs:78-83` refuses with
`JobExecutionAlreadyRunning`, and the only way out is a human calling
`abandon_execution`.

So: **a graceful pod shutdown produces a job that can never run again without
manual intervention.** In a Kubernetes deployment that rolls nightly, this
fires on the first roll that overlaps a batch window.

**Why it is Critical rather than High:** the failure is silent at the moment it
occurs and only surfaces at the *next* scheduled run, by which point the
operator is debugging "why did last night's job not run" rather than "why did
the deploy break the job".

**Recommendation.** A cooperative stop signal checked at the commit boundary —
not a `select!` on user futures, which would reintroduce cancellation-safety
questions the current design avoids entirely.

```rust
/// A cooperative request to stop after the current chunk commits.
///
/// Checked only at commit boundaries, never mid-chunk: a chunk that has been
/// written but not committed must roll back cleanly, and interrupting a user's
/// `write` would make cancellation safety that user's problem.
#[derive(Debug, Clone, Default)]
pub struct StopSignal(Arc<AtomicBool>);

impl StopSignal {
    pub fn stop(&self) { self.0.store(true, Ordering::Relaxed); }
    pub fn is_stopped(&self) -> bool { self.0.load(Ordering::Relaxed) }
}
```

Threaded through `JobLauncher::new(repo).with_stop_signal(signal)`, and checked
in three places:

```rust
// chunk.rs, after a successful commit, before reading the next chunk:
if stop.is_stopped() {
    tracing::info!("stop requested; ending the step at a committed boundary");
    return Err(BatchError::Stopped);   // new variant
}

// job.rs, between steps.
// tasklet.rs, after each Continuable pass commits.
```

`JobLauncher::run` maps `BatchError::Stopped` to `BatchStatus::Stopped` rather
than `Failed`, which finally gives that variant meaning — see
[CONC-4](#conc-4).

The restart path then works unchanged: `Stopped` is already in the launcher's
"terminal but unsuccessful" arm (`launcher.rs:85`), so a relaunch resumes from
the bookmark. **The whole feature is ~80 lines because the restart machinery
already exists.**

**Also required:** document the SIGTERM recipe in the guide —

```rust
let stop = StopSignal::default();
tokio::spawn({ let stop = stop.clone(); async move {
    tokio::signal::ctrl_c().await.ok();
    stop.stop();
}});
launcher.run(&mut job, &params).await   // finishes the current chunk, then stops
```

**Benefit:** turns "the process must be killed and then manually unblocked"
into "the job stops at a chunk boundary and restarts where it left off". This
is the single highest-impact change in the audit.

---

<a id="async-2"></a>
### ASYNC-2 — No timeouts anywhere; a hung writer hangs the job forever

**Severity:** High · **Effort:** S (1 day) · **Files:** `chunk.rs:319, 374`, `fault.rs`

`writer.write(&mut tx, &items).await` has no deadline. Neither does
`commit.commit(...)`, `reader.read()`, or `processor.process()`. A user's HTTP
writer against an endpoint that accepts the connection and never responds
produces a job that sits at `STARTED` indefinitely, holding an open transaction
and a pooled connection.

`RetryPolicy` has `min_delay` and `max_delay` but no `attempt_timeout` — so the
retry budget bounds *attempts*, never *time*. `RetryPolicy::attempts(3)` with a
writer that hangs is a job that never returns.

**Recommendation.** Add a per-attempt deadline to `RetryPolicy`, where the
policy already lives:

```rust
impl RetryPolicy {
    /// How long one write-and-commit attempt may take before it is abandoned
    /// and treated as a failure.
    ///
    /// `None` (the default) means no deadline, which is what a writer with its
    /// own client-side timeout wants. Set it when the writer has none: without
    /// a bound, a hung backend is indistinguishable from slow progress and the
    /// step never fails.
    #[must_use]
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self { /* ... */ }
}
```

Applied in `run_step`:

```rust
let outcome = match config.fault.attempt_timeout() {
    Some(limit) => tokio::time::timeout(limit, writer.write(&mut tx, &items))
        .await
        .unwrap_or_else(|_| Err(BatchError::write("write attempt timed out"))),
    None => writer.write(&mut tx, &items).await,
};
```

**Cancellation-safety note that must be documented:** a timed-out `write` drops
the user's future mid-flight. The transaction is then rolled back, so the
*database* is consistent — but a writer with external side effects (an HTTP
POST already sent) has the same at-least-once exposure `Unmanaged` already
warns about. Say so in the rustdoc; do not leave it to be discovered.

**Benefit:** converts an unbounded hang into a classified, retryable,
observable failure.

---

<a id="async-3"></a>
### ASYNC-3 — No guidance on blocking work inside `ItemProcessor`

**Severity:** Medium · **Effort:** XS (docs) · **Files:** `item.rs:104-117`, `docs/Guide.md`

`ItemProcessor::process` is an `async fn` that users will fill with whatever
transforms their data — including CPU-bound parsing, compression, or a
synchronous library call. On a multi-threaded runtime that blocks a worker
thread; the chunk loop then stalls every other task scheduled there, including
the metadata store's connections.

This is the single most common way an async framework is misused, and the docs
say nothing about it. The framework's own docs correctly warn about
`std::thread::sleep` in the backoff — the same reasoning applies to user code
and is not stated where users will read it.

**Recommendation.** A short section in `docs/Guide.md` and a paragraph on the
`ItemProcessor` rustdoc:

> `process` runs on the runtime's worker thread. Work that blocks for more than
> a few microseconds — CPU-bound transforms, synchronous I/O, a C library —
> must go through `tokio::task::spawn_blocking`, or it stalls every other task
> on that worker, including the metadata store's connections. If most of your
> processing is CPU-bound, consider whether the transform belongs in the
> reader's query instead.

**Benefit:** prevents the most common performance complaint a framework like
this receives, at the cost of one paragraph.

---

<a id="async-4"></a>
### ASYNC-4 — The `cron` adapter swallows every runtime failure by design, and the design is stated but not defended

**Severity:** Medium · **Files:** `crates/batchflow-scheduler/src/cron.rs:63-88`

```rust
Err(error) => tracing::error!(error = %error, "scheduled job failed; no caller will see this error"),
```

The doc comment is admirably explicit — *"an unmonitored in-process schedule can
fail every night in complete silence"*. That is the right disclosure. What is
missing is a mitigation for users who take the adapter anyway.

**Recommendation.** Offer a failure hook, so the adapter is usable without a
metrics stack:

```rust
pub fn into_cron_job_with(
    self,
    schedule: &str,
    on_outcome: impl Fn(&Result<Outcome, BatchError>) + Send + Sync + 'static,
) -> Result<CronJob, JobSchedulerError>
```

Ten lines, and it lets a user page, exit the process, or increment their own
counter. `into_cron_job` becomes `into_cron_job_with(schedule, |_| {})`.

**Benefit:** the difference between "documented footgun" and "documented
footgun with a guard rail".

---

<a id="async-5"></a>
### ASYNC-5 — Redis `commit` does not verify what `EXEC` returned

**Severity:** Medium · **Effort:** S · **Files:** `crates/batchflow-redis/src/lib.rs:318-320`

```rust
async fn commit(&self, tx: Self::Tx) -> Result<(), BatchError> {
    tx.query_async::<()>(&mut self.conn()).await.map_err(re)
}
```

The pipeline is `.atomic()`, so this is `MULTI … EXEC`. Two cases are not
covered by any test:

1. **A queued command that fails at `EXEC` time** (wrong type, OOM). Redis
   returns an array in which individual elements are errors. Whether
   `query_async::<()>` surfaces that as `Err` depends on redis-rs's response
   parsing for the unit type — and the audit could not establish it by
   inspection.
2. **`EXEC` returning nil.** That happens only with `WATCH`, which this code
   does not use, so it should be unreachable — but "should be unreachable" is
   the kind of claim that deserves a test rather than an argument.

**Why it matters here specifically:** this is the commit that makes a chunk's
counters and bookmark durable. If a failed `EXEC` reads as `Ok(())`, the chunk
loop believes it committed, publishes its metrics, advances its in-memory
counters and moves on — and a restart resumes from a bookmark that was never
written. Silent duplicate writes, which is the exact failure the framework
exists to prevent.

**Recommendation.** Do not reason about it — assert it:

```rust
async fn commit(&self, tx: Self::Tx) -> Result<(), BatchError> {
    // Typed as Vec<Value> rather than (): EXEC returns one reply per queued
    // command, and an error in any of them must not read as success.
    let replies: Vec<redis::Value> = tx.query_async(&mut self.conn()).await.map_err(re)?;
    // ... check for ServerError entries
}
```

plus a test that queues a command guaranteed to fail inside `EXEC` (e.g. `HSET`
against a key holding a list) and asserts `commit` returns `Err`.

**Benefit:** closes the only path in the Redis backend where a failed commit
could be mistaken for a successful one.

---

<a id="async-6"></a>
### ASYNC-6 — No task leaks, no deadlocks, no `spawn` — verified

**Severity:** none — recorded as a negative result.

- **No `tokio::spawn` in any library crate.** The framework never spawns; it
  runs on the caller's task. That is the correct default for a library and it
  makes task-leak analysis trivial: there are none.
- **No structured-concurrency primitives needed**, because there is no
  concurrency. `JoinHandle` appears nowhere in `src/`.
- **Deadlock analysis:** one lock in the entire workspace
  (`InMemoryJobRepository::inner`). Single lock, no ordering to get wrong, never
  held across an `.await`. `last_step_execution` nests two iterators over
  `inner` fields *under one guard*, which is fine (no re-entrant lock).
- **Back-pressure:** implicit and correct. The chunk loop is a sequential
  pipeline with no queues, so a slow writer back-pressures the reader by simply
  not being awaited. No unbounded channel exists to grow.

---

# Pass 8 — Concurrency Review

<a id="conc-1"></a>
### CONC-1 — The launcher's FR-4.4 gate is a check-then-act race

**Severity:** High · **Effort:** M (2 days) · **Files:** `launcher.rs:70-91`

Known and documented in the CHANGELOG, which is to the project's credit. It is
listed here because it is the framework's headline promise and the fix is
tractable.

```rust
if let Some(last) = self.repository.last_execution(instance.id()).await? {
    match last.status() { /* refuse if Completed / Starting / Started */ }
}
let mut execution = self.repository.create_execution(instance.id()).await?;
```

Two statements, no transaction, no lock. Two processes interleave as:

```
P1: last_execution(7) -> None
P2: last_execution(7) -> None
P1: create_execution(7) -> id 100, STARTING
P2: create_execution(7) -> id 101, STARTING
P1, P2: both run the same job instance concurrently
```

**Impact.** Both launch. Both readers open at the same bookmark. Both write the
same rows. For a billing or ledger job that is a duplicated financial effect,
and it happens exactly when it is most likely to: two replicas of the same
CronJob, or a scheduler retrying a tick it thinks was missed.

The CHANGELOG's framing — *"two processes racing an instance that has no prior
execution can both launch"* — slightly understates it: the same race exists
when the prior execution is `Failed`, which is the restart path, and restarts
are precisely when an operator is likely to be poking the system by hand while
a scheduler also fires.

**Recommendation — Postgres.** Push the whole decision into one statement, so
the database arbitrates:

```sql
-- 0003_live_execution.sql
-- At most one non-terminal execution per instance. A partial index, so
-- terminal rows are unconstrained and an instance may accumulate attempts.
CREATE UNIQUE INDEX job_execution_one_live
    ON job_execution (instance_id)
 WHERE status IN ('STARTING', 'STARTED');
```

```sql
-- create_execution becomes conditional:
INSERT INTO job_execution (instance_id, status, execution_context)
SELECT $1, $2, $3
 WHERE NOT EXISTS (
     SELECT 1 FROM job_execution
      WHERE instance_id = $1 AND status = 'COMPLETED'
 )
RETURNING id
```

The `WHERE NOT EXISTS` closes the completed-instance race; the partial unique
index closes the concurrent-launch race by making the second insert fail with
`23505`, which the repository translates to
`BatchError::JobExecutionAlreadyRunning`. The loser learns it lost from the
database rather than from a stale read.

This does change the `JobRepository` contract: `create_execution` gains the
ability to refuse, so the gate logic moves from `JobLauncher` into the
repository. That is the right place for it — the launcher cannot make an atomic
decision and the store can.

**Recommendation — Redis.** The same check-and-insert as one Lua script, which
the backend already does for four other operations. A `live:{instance_id}` key
set with `SET … NX` inside the script.

**Recommendation — in-memory.** Trivially correct once the decision is inside
the single `Mutex`.

**Conformance case that must accompany it:**

```rust
pub async fn only_one_of_two_concurrent_launches_wins<R: JobRepository>(repository: &R) {
    let instance = repository.find_or_create_instance("nightly", &params(&[("d", "1")])).await.unwrap();
    let (a, b) = tokio::join!(
        repository.create_execution(instance.id()),
        repository.create_execution(instance.id()),
    );
    assert_eq!(a.is_ok() as u8 + b.is_ok() as u8, 1, "exactly one launch may win");
}
```

Note this test would currently **fail** against all three backends, which is
the point: the contract is not tested because it is not enforced.

**Benefit:** makes exactly-once actually exactly-once under the deployment
topology (multiple replicas) that every Kubernetes user has by default.

---

<a id="conc-2"></a>
### CONC-2 — No fencing token, so `abandon_execution` can resurrect a live process

**Severity:** Medium · **Effort:** M · **Files:** `repository.rs:100-103`, `launcher.rs`

`abandon_execution` is documented as an operator assertion that the process is
dead — *"which the repository cannot verify"*. Honest, and correct as far as it
goes. The gap is what happens if the operator is wrong:

```
P1 is alive but slow (GC pause, network partition, paused container)
Operator: abandon_execution(100)      -> instance released
P2 launches, gets execution 101, starts writing
P1 resumes, finishes its chunk, calls update_step_execution_in(...) and commits
```

P1's writes land after P2 started. Both are writing the same rows from the same
bookmark. The metadata store now has two executions interleaving updates to
their own `step_execution` rows — but the *data* they write collides.

This is the classic problem a fencing token solves: every write carries the
execution id it belongs to, and the store rejects writes from an execution that
has been abandoned.

**Recommendation.** Add an epoch check to the chunk commit path:

```sql
-- update_step_execution_in gains a guard on its parent's status
UPDATE step_execution s
   SET ...
  FROM job_execution e
 WHERE s.id = $1
   AND e.id = s.job_execution_id
   AND e.status IN ('STARTING', 'STARTED')   -- refuse if abandoned
```

Zero rows affected → `BatchError::Repository("this execution was abandoned")`
→ the step fails, the zombie process stops. The check is free: it is a
join on a primary key inside a transaction the chunk is already opening.

**Tradeoff.** It does not make abandonment safe in general — P1 may have
already committed a chunk before the abandon landed, and P2 will re-do it.
But that case is exactly the at-least-once the framework already documents
under `Unmanaged`, whereas *unbounded* interleaving is not. The fence bounds
the damage to one chunk.

**Benefit:** makes the documented escape hatch safe to actually use, which
today it is not.

---

<a id="conc-3"></a>
### CONC-3 — `find_or_create_instance` is atomic; the rest of the metadata lifecycle is not

**Severity:** Low · **Files:** `postgres/src/lib.rs:202-227` vs. the rest

Worth recording as an observation rather than a defect: the codebase clearly
understands TOCTOU — `find_or_create_instance` is one `INSERT … ON CONFLICT`
with a comment explaining exactly why, and `abandon_execution` uses a
`FOR UPDATE` CTE for the same reason.

Both of those are single-repository-method races, and both are closed. The
races that remain ([CONC-1](#conc-1), [CONC-2](#conc-2)) are *cross-method*
races, which the `JobRepository` trait has no vocabulary for: every method is
its own round trip and the trait offers no way for a caller to compose two of
them atomically.

**Recommendation (design note, not an action).** When CONC-1 is fixed, the fix
should be to move the *decision* into the repository rather than to give the
launcher a transaction to compose in. Exposing `begin`/`commit` to the launcher
would let a user hold a metadata transaction across a whole job run, which is
precisely the "one transaction per step" anti-pattern the chunk loop exists to
avoid.

---

<a id="conc-4"></a>
### CONC-4 — `BatchStatus::Stopped` is written by nothing

**Severity:** Low (becomes moot with [ASYNC-1](#async-1)) · **Files:** `execution.rs:20-21`

`grep -rn 'BatchStatus::Stopped' crates --include='*.rs'` finds five hits:
the launcher's terminal-status match arm, and two `status_name`/`status_from`
pairs in the backends. **Nothing ever sets it.** The only way an execution
reaches `STOPPED` is a user writing the row by hand.

It is a published enum variant, a stored string constant in two backends, and a
metric label value (`status_label(Stopped) => "stopped"`, with a test pinning
the string) — for a state the engine cannot produce.

**Recommendation.** Fixing [ASYNC-1](#async-1) gives it meaning and this
finding disappears. If graceful stop is deferred, document `Stopped` as
"reserved; not currently produced by the engine" so the next reader does not
spend an hour looking for the transition.
