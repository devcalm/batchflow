# BatchFlow guide

Concepts first, then recipes. If you want running code instead, start with
[Examples.md](Examples.md) — five programs, four of which need no database.

- [The model](#the-model)
- [Your first job](#your-first-job)
- [The chunk loop](#the-chunk-loop)
- [Tasklets](#tasklets)
- [Restart](#restart)
- [Transactions](#transactions)
- [Retry and skip](#retry-and-skip)
- [PostgreSQL](#postgresql)
- [Scheduling](#scheduling)
- [Observability](#observability)
- [Dashboards and alerts](#dashboards-and-alerts)
- [Choosing a backend](#choosing-a-backend)
- [Writing your own backend](#writing-your-own-backend)
- [Mistakes worth avoiding](#mistakes-worth-avoiding)

## The model

Five nouns, split into two groups. The split is what makes restart possible, so
it is worth ten seconds.

**What you write** — immutable definitions:

| | |
|---|---|
| `Job` | An ordered list of steps, with a name. |
| `Step` | One unit of work. `ChunkStep` is the one you will use. |
| `ItemReader` / `ItemProcessor` / `ItemWriter` | The three halves of a chunk step. |

**What the engine writes** — execution records, in the `JobRepository`:

| | |
|---|---|
| `JobInstance` | Identified by `(job_name, JobParameters)`. "The nightly job for 2026-08-05." |
| `JobExecution` | *One attempt* at an instance. A restart is a second execution of the same instance. |
| `StepExecution` | One step's attempt: status and counters. |
| `ExecutionContext` | A reader's bookmark. Serializable, and committed with the data. |

The distinction that carries everything: **a `JobInstance` is the logical run, a
`JobExecution` is one try at it.** Parameters identify the instance, so
launching "nightly" with `date=2026-08-05` twice resolves to the *same*
instance — which is how the framework knows the second launch is a restart and
not a fresh run. Change any parameter and you get a different instance, free to
run from the start.

A completed instance cannot be launched again. That is the guard that stops a
cron job from double-billing when it fires twice.

## Your first job

```rust
use batchflow::{
    BatchError, ChunkStep, InMemoryJobRepository, ItemProcessor, ItemReader, ItemWriter,
    Job, JobLauncher, JobParameter, JobParameters, Unmanaged,
};
use std::num::NonZeroUsize;

struct Counter { next: u32, last: u32 }

impl ItemReader for Counter {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        if self.next > self.last {
            return Ok(None);          // end of input
        }
        let item = self.next;
        self.next += 1;
        Ok(Some(item))
    }
}

struct Double;

impl ItemProcessor for Double {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        if item % 3 == 0 {
            return Ok(None);          // filtered out, not failed
        }
        Ok(Some(item * 2))
    }
}

struct Stdout;

impl ItemWriter for Stdout {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        println!("chunk: {items:?}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BatchError> {
    let launcher = JobLauncher::new(InMemoryJobRepository::new());

    let step = ChunkStep::new(
        "double",
        Counter { next: 1, last: 10 },
        Double,
        Unmanaged(Stdout),
        NonZeroUsize::new(4).expect("4 is not zero"),
    );

    let mut job = Job::builder("hello").step(step).build();
    let parameters = JobParameters::new()
        .with("run", JobParameter::String("demo".into()));

    let execution = launcher.run(&mut job, &parameters).await?;
    println!("{:?} finished as {:?}", execution.id(), execution.status());
    Ok(())
}
```

Three things in that snippet are deliberate and easy to miss.

`Ok(None)` from a **reader** means end of input; from a **processor** it means
this item is filtered out. Filtered and skipped are different counters: a
filter is a decision you made, a skip is a failure you tolerated.

`NonZeroUsize` on the commit interval exists so that "chunk size 0", which
would write nothing and report success, cannot be written down.

`Job::builder("hello").step(step).build()` will not compile without at least
one `.step(..)`. The builder changes type on the first step, which is also why
you cannot drive it from a loop — use `Job::new(name, steps)` if you need to
build a step list dynamically.

## The chunk loop

```
loop {
    read  chunk_size items      (or fewer, at end of input)
    process each item
    ─── begin transaction ───
    write the chunk
    fold the counters
    save the reader's bookmark
    ─── commit ───
}
```

Processing happens **outside** the transaction, because holding locks across
slow user code is how a batch job becomes an incident. Writing, counting and
bookmarking happen **inside** one transaction, because a bookmark that outruns
the data it describes is a duplicate on the next restart.

The commit interval is therefore a real trade-off, not a tuning knob:

| Bigger `chunk_size` | Smaller `chunk_size` |
|---|---|
| Fewer transactions, less DB overhead | Less work re-done after a crash |
| More memory held live | Lower peak memory |
| More work lost on failure | More commits to pay for |

BatchFlow's own per-chunk cost is about 102 ns, so **the framework is never the
reason to raise your chunk size — your database is.** Measurements and the full
curve are in [Performance.md](Performance.md). A few hundred to a few thousand
suits most jobs.

## Tasklets

Not every step reads items. Truncating a staging table, archiving yesterday's
file, calling a stored procedure — one unit of work, no commit interval to size.
That is a `Tasklet`, and it is a peer of `ChunkStep`, not a lesser form of one:
both become a `Step` and a `Job` cannot tell them apart.

```rust
use batchflow::{
    BatchError, ExecutionContext, RepeatStatus, StepContribution, Tasklet,
    TaskletStep, Unmanaged,
};

struct TruncateStaging;

impl Tasklet for TruncateStaging {
    async fn execute(
        &mut self,
        _context: &mut ExecutionContext,
        contribution: &mut StepContribution,
    ) -> Result<RepeatStatus, BatchError> {
        // ... do the work ...
        contribution.increment_write(1);
        Ok(RepeatStatus::Finished)
    }
}

let job: Job = Job::builder("nightly")
    .step(TaskletStep::new("truncate", Unmanaged(TruncateStaging)))
    .step(load_step)
    .build();
```

The `: Job` is load-bearing. Anything wrapped in `Unmanaged` implements its step
trait for *every* transaction type, so nothing in that expression pins one —
naming the job's type is what chooses it. `Job` is `Job<()>`; a job whose steps
enlist in a Postgres transaction is `Job<PgTx>`, by the same one word.

`Unmanaged` means the same thing it means for a writer: *this does not enlist in
the step's transaction*, stated at the call site rather than inferred. A tasklet
that does want the transaction implements `TransactionalTasklet<Tx>` instead and
receives `&mut Tx`, which is how you get "delete the staging rows and record
that you did" as one atomic act.

**`RepeatStatus` is the interesting part.** Returning `Continuable` runs
`execute` again in a *fresh transaction*, so a long tasklet gets a commit point
per pass and — if it leaves a bookmark in the context — restarts from where it
stopped:

```rust
let done = context.get_long("archived")?.unwrap_or(0);
archive_one(done)?;
context.put("archived", ContextValue::Long(done + 1));
contribution.increment_write(1);

Ok(if done + 1 == self.total { RepeatStatus::Finished } else { RepeatStatus::Continuable })
```

Loop inside a single `execute` instead and you commit once at the very end,
which means a crash loses all of it. The trade is that **nothing bounds a
`Continuable` loop** — there is no item count to bound it with, the way a skip
limit bounds a non-advancing reader — so a tasklet that never returns `Finished`
runs until the process is killed. That is your obligation, not the engine's.

Two things a tasklet deliberately does not get: **retry and skip.** Skip has no
meaning with no items to drop, and retry would re-call `execute` on a tasklet
that has already changed its own state — the "must be idempotent" obligation
this framework is careful not to impose on your processor. If you want a
tasklet to retry, do it inside `execute`, where you can see what you already
did.

## Restart

Restart is not a mode and there is no `if restarting` anywhere. A fresh run
takes exactly the same path, with every lookup returning `None`.

To make a step restartable, your reader must record where it got to. Two hooks
on `ItemReader`, both with default bodies that do nothing — **a reader that
overrides neither is simply not restartable**, which is honest rather than
hidden:

```rust
use batchflow::{ContextValue, ExecutionContext};

const POSITION: &str = "position";

impl ItemReader for Rows {
    type Item = Row;

    async fn read(&mut self) -> Result<Option<Row>, BatchError> {
        let row = self.rows.get(self.pos).cloned();
        if row.is_some() {
            self.pos += 1;
        }
        Ok(row)
    }

    // Called once before the loop. May seek a file or reposition a cursor,
    // so it is async and fallible.
    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        if let Some(position) = context.get_long(POSITION)? {
            self.pos = usize::try_from(position)
                .map_err(|_| BatchError::read(format!("negative bookmark {position}")))?;
        }
        Ok(())
    }

    // Called at every commit point. Writing your own position into a map
    // cannot fail, so this is sync and infallible.
    fn update(&self, context: &mut ExecutionContext) {
        context.put(POSITION, ContextValue::Long(self.pos as i64));
    }
}
```

`get_long` returns `Result<Option<i64>, _>` and the difference matters:
`Ok(None)` is a fresh start, `Err` is a *malformed* bookmark. Collapsing them
lets a garbled bookmark silently restart from zero and rewrite every committed
item. Use `try_from`, never `as` — `as` turns a corrupt `-1` into
`usize::MAX`, and the reader then seeks past the end and reports success having
processed nothing.

On restart, a step that previously completed is skipped entirely and gets no
new record. So `step_executions(execution_id)` on a restart lists only what
actually ran; "what did this instance do across all attempts?" is an
*instance* question, answered by `executions(instance_id)`.

## Transactions

A writer that can join the step's transaction implements `TransactionalWriter`:

```rust
use batchflow::TransactionalWriter;
use sqlx::{Postgres, Transaction};

type PgTx = Transaction<'static, Postgres>;

impl TransactionalWriter<PgTx> for Rows {
    type Item = Row;

    async fn write(&mut self, tx: &mut PgTx, items: &[Row]) -> Result<(), BatchError> {
        for item in items {
            sqlx::query("INSERT INTO target (value) VALUES ($1)")
                .bind(item.value)
                .execute(&mut **tx)
                .await
                .map_err(BatchError::write)?;
        }
        Ok(())
    }
}
```

A writer that *cannot* — stdout, S3, a CSV file — keeps implementing plain
`ItemWriter` and is wrapped in `Unmanaged`:

```rust
ChunkStep::new("export", reader, processor, Unmanaged(CsvWriter::new(path)), chunk_size)
```

`Unmanaged` is required rather than automatic, and that is the point:
non-transactional writing means at-least-once delivery for that step, and it
should be visible at the call site rather than inferred. A blanket impl is also
literally impossible — Rust cannot prove a type is *not* an `ItemWriter`.

Note that `write` takes `&[Item]` — a whole chunk, not one item. That is what
lets a backend issue one `COPY` or one multi-row `INSERT` per commit interval
instead of a round trip per row.

## Retry and skip

Failures are classified, not caught. You write a `Classifier` over your own
error type:

```rust
use batchflow::{Classifier, ErrorAction, BatchError, FaultTolerance, RetryPolicy};
use std::num::NonZeroU32;

struct FeedClassifier;

impl Classifier for FeedClassifier {
    fn classify(&self, error: &BatchError) -> ErrorAction {
        // Walk the source chain: your error is nested inside BatchError.
        let mut source = Some(error as &(dyn std::error::Error + 'static));
        while let Some(current) = source {
            if let Some(feed) = current.downcast_ref::<FeedError>() {
                return match feed {
                    FeedError::MalformedRow(_) => ErrorAction::Skip,
                    FeedError::Throttled       => ErrorAction::Retry,
                    FeedError::Unauthorized    => ErrorAction::Fail,
                };
            }
            source = current.source();
        }
        ErrorAction::Fail
    }
}

let fault = FaultTolerance::new()
    .classifier(FeedClassifier)
    .retry(RetryPolicy::attempts(NonZeroU32::new(3).unwrap()))
    .skip_limit(50);

let step = ChunkStep::new("load", reader, processor, writer, chunk_size)
    .with_fault_tolerance(fault);
```

The three actions:

- **`Retry`** re-attempts the *chunk*, always in a fresh transaction, with
  exponential backoff. The default policy is no retries.
- **`Skip`** drops the offending *item* and keeps going, up to `skip_limit`.
  Exceeding the limit fails the step with `BatchError::SkipLimitExceeded`,
  whose source chain still carries the original failure.
- **`Fail`** stops the step. This is the default for everything
  (`FailFast`), which is the right default for infrastructure errors — a
  connection failure is not a bad row.

**Retry is per chunk; skip is per item.** That is a consequence of the trait
signatures, not a policy choice: `read` and `process` are per item, so the
failing item is known and can be dropped. `write(&[Item])` is per chunk, so a
write error names N items rather than one.

To skip on the *write* side you have to find which item is bad, which is what
chunk scanning does — off by default:

```rust
let fault = FaultTolerance::new()
    .classifier(FeedClassifier)
    .skip_limit(50)
    .scan_on_write_failure(true);
```

On a write failure the chunk is re-written one item at a time in throwaway
transactions that are always rolled back, and the survivors are then written
once for real. It costs `N + 1` transactions and writes every good item twice —
on the failure path only.

**Think before enabling it with an `Unmanaged` writer.** The identifying pass
rolls back, which for a writer that cannot enlist in a transaction means
nothing: its rows were already sent. A thousand-item chunk with one bad row
delivers roughly two thousand items. `Unmanaged` already means at-least-once, so
nothing is promised and then broken — but that is a lot of duplicates to
discover by accident.

Note that scanning applies to a failed *write*, not a failed *commit*. A commit
error names the transaction, not a row, so there is nothing in it to isolate.

The `skip_limit` is **step-wide, not per chunk**: one bad row in each of a
thousand chunks is a broken input file, and a per-chunk limit would call it
healthy. It also bounds the read loop, which matters more than it sounds —
a skipping reader **must advance past the item it failed on**, or the engine
hands it the same item forever. The engine cannot enforce that, because `read`
is opaque to it. The skip limit is the only thing standing between a
non-advancing reader and an infinite loop.

A skipped read does not consume a chunk slot, so dirty input cannot silently
shrink the transaction your commit interval promised.

## PostgreSQL

```rust
use batchflow_postgres::{PostgresClassifier, PostgresJobRepository};
use sqlx::PgPool;

let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
let repository = PostgresJobRepository::new(pool);
repository.migrate().await?;                 // embedded migrations

let launcher = JobLauncher::new(repository);
```

`PostgresClassifier` maps SQLSTATE to an action, so core never learns what a
SQLSTATE is:

- Retry on `40001` (serialization failure), `40P01` (deadlock), `55P03` (lock
  not available) — enumerated, deliberately *not* the whole class 40, because
  `40003` means the outcome is unknown and retrying may double-write.
- Skip on class `22` (data exception) and `23` (integrity violation) — both say
  "this row is wrong", never "the system is unwell".
- Fail on everything else, including connection failures.

Classifiers compose by delegation: handle your own errors, defer the rest.

```rust
impl Classifier for LoaderClassifier {
    fn classify(&self, error: &BatchError) -> ErrorAction {
        match my_own_verdict(error) {
            Some(action) => action,
            None => PostgresClassifier.classify(error),
        }
    }
}
```

`Job` becomes `Job<PgTx>` once a transactional writer is involved. The typestate
builder still works — annotating the target is enough
(`fn build_job() -> Job<PgTx>`).

## Scheduling

BatchFlow has no cron engine and will not grow one. What it has is the three-way
classification a schedule needs, in `batchflow-scheduler`:

```rust
use batchflow_scheduler::{Outcome, trigger};

match trigger(&launcher, &mut nightly(), &run_key(today)).await? {
    Outcome::Ran(execution)          => println!("ran: {:?}", execution.status()),
    Outcome::AlreadyComplete { .. }  => println!("already done today"),
    Outcome::AlreadyRunning { .. }   => println!("last night's run is still going"),
    _ => {}
}
```

`JobLauncher::run` reports those last two as errors, which is right for a caller
that asked for a run and wrong for a schedule, to which both mean the system is
working. Everything genuinely wrong — a failed step, an unreachable metadata
store — still propagates, because a scheduler that swallowed those would report
a healthy nightly run that never happened.

**The run key is the whole design.** Derive `JobParameters` from the tick:

```rust
fn run_key(date: &str) -> JobParameters {
    JobParameters::new().with("date", JobParameter::String(date.into()))
}
```

A re-fired tick resolves to the same `JobInstance` and is refused; tomorrow's
tick resolves to a new one and runs. Two mistakes to avoid, both of which look
fine until they do not:

| Run key | What you get |
|---|---|
| `date=2026-08-06` | ✅ idempotent per tick, restartable within it |
| constant | a job that runs exactly **once, ever** |
| contains a timestamp | **no deduplication and no restart** — every attempt is a new instance |

**Build a fresh `Job` per tick.** A step owns its reader, so reusing one job
hands the second firing a reader already at end of input. `ScheduledJob` takes a
closure precisely so this cannot be got wrong:

```rust
let nightly = ScheduledJob::new(launcher, move || Due {
    job: build_job(),
    parameters: run_key(&today()),
});
```

**Which trigger to use.** Under Kubernetes, systemd or system cron, call
`trigger` from `main` and exit — a refusal is exit 0, since a container that
exits non-zero here is restarted straight back into the same refusal. For a
long-lived service, enable the `cron` feature and hand `ScheduledJob` to
`tokio-cron-scheduler`:

```rust
scheduler.add(nightly.into_cron_job("0 0 2 * * * *")?).await?;  // 02:00 daily, UTC
```

Note what that costs in observability: the cron callback returns `()`, so it is
the end of the line for both outcomes and failures. It logs and emits
`batchflow_triggers_total`, and there is nowhere else for them to go — an
unmonitored in-process schedule can fail every night in silence. Under an
external scheduler you get a failing exit code for free.

Overlapping ticks are refused by the *metadata store*, not by the scheduler, so
the refusal holds across replicas: two processes on one store cannot both run an
instance.

## Observability

Both subsystems are inert until you install something, so a library user pays
nothing by default.

**Tracing.** `job` and `step` spans nest, so a skip event carries the job name,
instance and execution without the chunk loop naming any of them. Span and
field names are published constants in `batchflow_core::tracing`, so a dashboard
query is a compile-checked reference rather than a string.

```rust
tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
```

**Metrics.** Items read/written/filtered/skipped, chunks committed, retries,
and durations. `batchflow-metrics` installs a Prometheus exporter:

```rust
batchflow_metrics::builder().install()?;
```

`sum(items_written_total)` reconciles with `sum(write_count)` from the metadata
store, because both are published only after the chunk commits. Retries are the
deliberate exception — counted as they happen, so a chunk that retried five
times and then failed still reports them.

## Dashboards and alerts

The metric names are chosen so the useful queries are short. These are the ones
worth having before a job goes to production; all of them are labelled by `job`
and most by `step`, so each is a panel with a legend rather than one panel per
job.

**Is it running at all?**

```promql
# Runs in flight. Should return to 0 between schedules; a floor that creeps up
# means executions are being started and never finished.
sum by (job) (batchflow_jobs_started_total) - sum by (job) (batchflow_jobs_finished_total)

# Firings that did no work. Silence here is *not* good news: a refused schedule
# emits no jobs_started at all, so without this panel a job that has been
# skipping for a week looks exactly like a healthy one.
sum by (job, outcome) (rate(batchflow_triggers_total[1d]))
```

**Is it healthy?**

```promql
# Success rate. Every series exists from step start, so a job that has never
# failed reads 0 rather than being absent — which is why this does not go blank
# for your healthiest jobs.
sum by (job) (rate(batchflow_jobs_finished_total{status="completed"}[1d]))
  / sum by (job) (rate(batchflow_jobs_finished_total[1d]))

# Skips by phase. The `phase` label is the diagnosis: `read` is a broken input
# file, `process` is a bad assumption in your code, `write` is a constraint,
# `tasklet` is whatever the tasklet decided.
sum by (job, step, phase) (rate(batchflow_items_skipped_total[5m]))
```

**Is it fast enough?**

```promql
# p99 chunk latency. A histogram, not a summary, precisely so this aggregates
# across replicas — quantiles computed per process cannot be summed.
histogram_quantile(0.99, sum by (le, job, step) (rate(batchflow_chunk_duration_seconds_bucket[5m])))

# Throughput, in items per second.
sum by (job, step) (rate(batchflow_items_written_total[5m]))
```

**Three alerts worth paging on**, and the reasoning behind each:

| Alert | Expression | Why |
|---|---|---|
| Chunk scanning is happening | `rate(batchflow_chunk_scans_total[15m]) > 0` | Every good item in a scanned chunk is written twice. With an `Unmanaged` writer that is real duplicate delivery, and it will not stop on its own |
| Retries without progress | `rate(batchflow_chunk_retries_total[15m]) > 0 and rate(batchflow_chunks_committed_total[15m]) == 0` | Retrying and committing is a contended database; retrying and *not* committing is a stuck job |
| A schedule stopped running | `rate(batchflow_triggers_total{outcome="ran"}[2d]) == 0` | Catches a job whose run key stopped changing — the failure mode that produces no errors at all |

Two things the metrics deliberately cannot tell you, so do not build a panel
trying:

- **Which run.** No execution id is ever a label; one label value per run mints
  one time series per run, written once and kept forever. Correlating a specific
  execution is tracing's job, where high cardinality is the point — the `job` and
  `step` spans carry `instance_id` and `execution_id`.
- **When the skips happened.** A `skip_count` of 400 cannot say whether they were
  one contiguous block (a corrupt input segment) or evenly scattered (a
  systematic parse bug), and those have opposite remedies. That is a question
  about ordering in time, which a counter structurally cannot hold; the skip
  events in the trace can.

## Choosing a backend

| | `Tx` | Rollback | Use when |
|---|---|---|---|
| `InMemoryJobRepository` | `()` | none — at-least-once | tests, experiments |
| `PostgresJobRepository` | `sqlx::Transaction` | real | the default choice |
| `RedisJobRepository` | `MULTI`/`EXEC` pipeline | discard-before-send | metadata already lives in Redis |

Redis buffers commands client-side and sends them only at commit, so a
rolled-back chunk was never sent — but it has no read-your-writes inside a
transaction and no conflict detection, and **it is only correct with
`appendonly yes` and `appendfsync always`**. The metadata store is the
exactly-once guarantee; a store that can lose the last seconds of writes makes
restart probabilistic. If in doubt, use PostgreSQL.

## Writing your own backend

`JobRepository` is the main extension point. Its contract is published as an
executable test suite rather than as prose, so you can check your
implementation against the same list the shipped backends pass:

```toml
[dev-dependencies]
batchflow-core = { version = "0.1", features = ["conformance"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
// tests/conformance.rs
async fn setup() -> ((), MyRepository) {
    ((), MyRepository::new(/* ... */))
}

batchflow_core::job_repository_conformance!(setup());
```

That generates one `#[tokio::test]` per property — instance identity, execution
ordering, the abandon rules, step-execution scoping, bookmark round-tripping.
The setup expression is evaluated once per test and returns `(guard,
repository)`, where the guard is whatever must stay alive for the repository to
work (a container handle, a temp directory) or `()`.

Two things the suite deliberately does **not** assert, both worth understanding
before you implement:

**Rollback is not part of the contract.** A store with no transactions sets
`Tx = ()` and degrades to at-least-once — that is what `InMemoryJobRepository`
does, and it is honest rather than hidden. If your backend has real
transactions, test rollback yourself; it is a promise you are making, not one
the trait makes.

**Ids are opaque.** The suite only ever compares them for equality, because the
in-memory store draws from one counter while PostgreSQL uses a sequence per
table. Do not assume ids are dense, ordered across tables, or meaningful.

## Mistakes worth avoiding

**Assuming a reader is restartable.** `open`/`update` have default bodies that
do nothing. Nothing warns you. Test it by running the job twice with a failure
injected in between, and assert on the *exact* output — a reader resuming at
the wrong offset produces a duplicate-free but incomplete result, which a
"no duplicates" assertion happily passes.

**Using `as` on a bookmark.** See [Restart](#restart). `try_from` or a corrupt
bookmark becomes a silent no-op run.

**Treating a filter as a skip.** `Ok(None)` from a processor is a decision;
an `Err` that the classifier tolerates is a skip. They are separate counters
because they mean different things to whoever reads them at 3am.

**Expecting `Unmanaged` to be transactional.** It ignores the transaction. That
step is at-least-once, and a chunk that fails after writing has already
written.

**Reaching for a bigger chunk size for speed.** Measure first. The framework
costs ~3.9 ns/item and its per-chunk cost is flat above about 1,000; if your
job is slow, it is almost certainly your I/O.

**Sharing a reader between steps for parallelism.** `read` takes `&mut self`, so
a reader cannot be shared. Parallelism comes from partitioning — running
several steps over disjoint ranges — which is not yet implemented.
