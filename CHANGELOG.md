# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

All six crates — `batchflow`, `batchflow-core`, `batchflow-postgres`,
`batchflow-redis`, `batchflow-metrics` and `batchflow-scheduler` — share one
version number and are released together.

## [Unreleased]

### Added

**From the engineering audit (`docs/audit/`), phase 1**

- **`JobRepository::start_execution` — the launch gate is now atomic.** It
  decides whether an instance may run *and* opens the execution as one
  indivisible operation, returning it already `Started`. This closes the race
  the 0.1.0 CHANGELOG listed under "known limitations": the gate used to be a
  read, a decision and a write in `JobLauncher`, which two processes could
  interleave — both read "no live execution", both inserted, and **one job
  instance ran twice**. For a billing or ledger job that is a duplicated
  financial effect, and it happened exactly when it was most likely to: two
  replicas of one `CronJob`, or an operator relaunching by hand while a
  schedule fired. Running more than one replica is now safe.

  The decision moved into the repository because only the store can make it
  indivisible — `JobLauncher` has no way to make two calls atomic, and the trait
  deliberately offers no cross-method transaction. Postgres takes a
  `SELECT … FOR UPDATE` row lock on the instance and runs the gate and the
  insert in one transaction (per instance, so unrelated jobs never contend);
  Redis uses one Lua script; the in-memory store one `Mutex` acquisition.
  `create_execution` is unchanged and remains the unconditional primitive.

  Six conformance cases cover it, including
  `only_one_of_two_concurrent_launches_wins` — so a third-party backend that
  gets this wrong fails its test run rather than duplicating work in
  production.
- **Graceful stop** (`StopSignal`, `JobLauncher::with_stop_signal`,
  `StepCommit::stop_requested`, `BatchError::Stopped`). A raised signal ends the
  step at its next *committed* chunk boundary — never mid-chunk — records
  `BatchStatus::Stopped`, and leaves the bookmark just past the last committed
  work. `Stopped` was already a restartable status in the launcher's gate, so
  the resume path is the restart path unchanged. Until now the only way to end
  a running job was to drop its future, which skipped the terminal status write
  and left `job_execution.status = 'STARTED'` forever: every later launch of
  that instance was refused with `JobExecutionAlreadyRunning` naming a dead
  process, recoverable only by a manual `abandon_execution`. A rolling deploy
  landing mid-job did exactly that. `BatchStatus::Stopped` is no longer a
  variant nothing writes.
- **`ItemReader::close` and `ItemWriter::close`**, provided methods called once
  when the step ends, on both the success and the failure path, paired with
  `open`. An error fails the step. Without them a buffered writer — `BufWriter`,
  a batching client, `csv::Writer` — still held part of the last chunk when
  `write` returned and flushed only on `Drop`, where the `io::Error` is
  unobservable: a full disk produced a job that reported success and wrote a
  truncated file.
- **A panic boundary** around `Step::run` and `Job::run`. A panic in a user's
  reader, processor, writer or tasklet becomes `BatchError::Panic` and the
  execution is recorded `Failed`, so the instance restarts normally. Previously
  the unwind skipped both terminal status writes and wedged the instance until
  an operator intervened — turning the single most common bug in Rust
  application code into an incident needing database access. Inert under
  `panic = "abort"`, which is documented.
- `ExecutionContext::{len, remove, iter}`, so a reader can clear a finished
  bookmark and a diagnostic can enumerate what a step recorded.
- `impl JobRepository for Arc<R>`, so a store that is not `Clone` — notably
  `InMemoryJobRepository` — can still be shared behind a launcher.
- `docs/Operations.md`: choosing a backend, sizing the commit interval as a
  memory *and* durability decision, what the per-chunk metadata write costs,
  stopping a job, recovering a wedged instance, and what to alert on.
- `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, issue and pull
  request templates. `CONTRIBUTING` records the two prerequisites that were
  written down nowhere: the backend suites need Docker, and `DATABASE_URL` must
  stay unset so `sqlx` validates against the committed `.sqlx/` cache — which
  also means reindenting a query invalidates it, since `sqlx` hashes the
  literal.
- CI: `cargo-deny` (advisories, bans, licences, sources) on pull requests and
  weekly; a `publish --dry-run` packaging lane; a guard that fails the build if
  a crate README claims the crate is an unreleased placeholder; Dependabot for
  Cargo and Actions, made safe to automate by the existing MSRV matrix.

### Changed

- **`batchflow` re-exports `batchflow_core` at its root**, so imports read
  `use batchflow::{Job, JobLauncher};` rather than
  `use batchflow::batchflow_core::{…}`. The old path still resolves. The
  facade's crate docs are now its README, which makes the quickstart a compiled
  doctest; the two structural doctests it carried moved to
  `crates/batchflow/tests/facade.rs`, where they run rather than merely compile
  and the one-crate-deep dependency graph still guarantees a user could write
  them.
- `Job::run` takes a `&StopSignal`. `JobLauncher::run` passes its own, so only
  callers driving `Job::run` directly are affected.
- **`batchflow-redis` rejects a metadata record that Redis no longer has.**
  `HGETALL` on an evicted key returns an empty hash, and every field lookup had
  a default — so an evicted step execution read back as a pristine `STARTING`
  with an empty bookmark, indistinguishable from a step that had never run. The
  restart then re-read the input from the beginning and re-wrote every
  committed item, silently. It now fails loudly and names `maxmemory-policy`.
  The crate docs additionally state that **Redis Cluster is not supported** —
  structurally, not merely untested: the Lua scripts declare cross-slot keys and
  build further keys from `ARGV`.
- `batchflow-postgres` writes counters through a checked `i64::try_from` rather
  than `as`, mirroring the read path — `as` reinterprets a large `usize` as
  negative, which the non-negative `CHECK` constraint would then reject with a
  message claiming corruption. Its two identical `UPDATE step_execution`
  statements are now one function generic over `sqlx::Executor`.
- Redis Lua scripts are `LazyLock<Script>` rather than rebuilt per call, so each
  is SHA-1 hashed once instead of on every repository operation.
- `InMemoryJobRepository` has one lock helper instead of twelve copies of the
  same `map_err`, and no longer stringifies a `PoisonError` — which rendered the
  poisoning rather than the panic that caused it.
- `PostgresClassifier` derives `Debug`, `Clone`, `Copy` and `Default`; every
  crate root now warns on `missing_debug_implementations`.
- The chunk-loop benchmark declares `Throughput::Elements`, so criterion reports
  the ns/item figure `docs/Performance.md` quotes instead of it being arithmetic
  done by hand outside the harness. The two `TODO(you)` prompts it shipped with
  are resolved.

**Earlier in this cycle**

- **`batchflow-scheduler`** — a sixth crate, closing the last untouched phase.
  `trigger` turns the launcher's two *refusal* errors into an `Outcome`
  (`Ran` / `AlreadyComplete` / `AlreadyRunning`) and leaves every other error
  alone, which is the whole of what a schedule needs; per ADR-006 there is still
  no cron engine here. `ScheduledJob` binds a launcher to a per-tick job
  factory, and the optional `cron` feature adapts it to `tokio-cron-scheduler`
  (37 → 66 dependencies, hence the flag). New metric
  `batchflow_triggers_total{job,outcome}`, because a refusal creates no
  `JobExecution` and is therefore invisible in the core vocabulary.
- **Tasklets** (`Tasklet`, `TransactionalTasklet<Tx>`, `TaskletStep`,
  `RepeatStatus`) — the second kind of step FR-1.2 always named. One transaction
  per `execute` call, repeated while the tasklet returns `Continuable`, so a long
  tasklet gets a commit point and a durable bookmark per pass. Adapted to any
  `Tx` by the existing `Unmanaged<T>` newtype. No retry or skip, deliberately.
- **Property tests for the chunk loop** (`proptest`, dev-dependency): counter
  partition, `ceil(n / chunk_size)` commits, chunk size not changing the result,
  exact output ordering, and bookmark coverage.
- `metrics::describe()` now covers `batchflow_chunk_scans_total`, which shipped
  in 0.1.0 without help text.

### Fixed

- **Skips in the tail of the input were silently dropped.** A step whose last
  rows are all skippable finished with those skips missing from `skip_count` and
  from `batchflow_items_skipped_total`, and with its bookmark short of them — so
  a restart re-read rows already known to be poison. The empty chunk that ends
  the loop now commits its trailing skips and the bookmark past them. Found by
  the new property test; no example-based test had put a poison row last.

### Changed

- CI runs `--all-features` for test, clippy and doc, and the 1.85 MSRV lane now
  covers `batchflow-scheduler` including its `cron` feature. Without it a
  feature-gated half of the workspace would ship uncompiled.

## [0.1.0]

The first usable release. `0.0.0` reserved the name and shipped nothing; every
entry below is new relative to it, so this section describes the framework
rather than a diff.

### Added

**Execution model**
- `Job`, `Step` and a typestate `JobBuilder` in which a job with no steps is a
  compile error rather than a runtime one.
- `ChunkStep`: the chunk-oriented step, driving `ItemReader` → `ItemProcessor` →
  `ItemWriter` in commit-interval sized chunks. `chunk_size` is a
  `NonZeroUsize`, so the silent "commit interval of zero writes nothing and
  reports success" failure is unrepresentable.
- `JobLauncher`, which resolves a `JobInstance` from `(job_name, JobParameters)`
  and decides whether a job may run at all.
- The framework is `Send` end to end, so `tokio::spawn(job.run(..))` compiles.

**Metadata and restart**
- `JobRepository`: instances, executions, step executions, counters and
  bookmarks. `InMemoryJobRepository` ships in `batchflow-core`.
- `ExecutionContext` — a reader's durable bookmark, written in the same
  transaction as the chunk's data and counters.
- Restart: a relaunched job skips steps that already completed and reopens its
  reader at the last committed chunk. Restart is not a mode; a fresh run takes
  the same path with every lookup returning `None`.
- `JobRepository::abandon_execution`, the escape hatch that releases an
  instance whose process died, shipped together with the guard that needs it.

**Transactions**
- `StepCommit` and `JobRepository::{begin, commit, rollback}`: the commit
  interval is the transaction boundary, so a chunk's rows, its counters and its
  bookmark become durable together or not at all.
- `TransactionalWriter<Tx>` for writers that enlist, and `Unmanaged<W>` — an
  explicit, visible acceptance of at-least-once for writers that cannot.

**Fault tolerance**
- `Classifier` and `ErrorAction::{Retry, Skip, Fail}`; `FailFast` is the
  default.
- Retry with exponential backoff (`RetryPolicy`), scoped to a chunk and always
  in a fresh transaction — reusing a rolled-back one fails to compile.
- Skip with a step-wide `skip_limit`, counted in `skip_count` and persisted.
- Chunk scanning (`FaultTolerance::scan_on_write_failure`), off by default: on a
  write failure the chunk is re-written one item at a time to isolate the bad
  row, then the survivors are committed. Costs `N + 1` transactions and writes
  every good item twice, on the failure path only — and with an `Unmanaged`
  writer the identifying pass really delivers, so read its docs before enabling
  it.

**Observability**
- `tracing`: `job` and `step` spans, plus events for skips, retries and failed
  cleanup. Span and field names are published constants.
- `metrics`: item, chunk, retry and skip counters plus durations.
  `batchflow-metrics` provides a Prometheus exporter.

**Backends**
- A shared conformance suite behind `batchflow-core`'s `conformance` feature:
  `job_repository_conformance!(setup())` generates one test per property of the
  `JobRepository` contract. Both shipped backends run the identical list, and a
  third-party backend gets the contract as executable tests rather than prose.
- `batchflow-redis`: a `RedisJobRepository` whose `Tx` is a `MULTI`/`EXEC`
  pipeline, so a rolled-back chunk was never sent. Check-then-act operations
  are Lua scripts. **Requires Redis with `appendonly yes` and
  `appendfsync always`** — see its docs; weaker persistence makes restart
  semantics probabilistic.
- `batchflow-postgres`: a `PostgresJobRepository` with embedded migrations, and
  `PostgresClassifier`, which maps SQLSTATE to a retry/skip/fail decision
  without core ever learning what a SQLSTATE is.

### Notes

- MSRV is per crate: **1.85** for `batchflow`, `batchflow-core` and
  `batchflow-metrics`; **1.88** for `batchflow-redis` (redis 1.5); **1.94** for
  `batchflow-postgres` (sqlx 0.9). A user on 1.85 can still take the facade
  with the in-memory store.
- `BatchError` and the public enums that will grow are `#[non_exhaustive]`.
- Measured framework overhead is ~3.9 ns per item plus ~102 ns per chunk, and
  allocation is a function of the chunk count rather than the item count. See
  [docs/Performance.md](docs/Performance.md) for the method and its caveats.

### Known limitations

- ~~**The launcher's gate is not race-free.**~~ — closed by
  `JobRepository::start_execution`, see Unreleased.
- ~~**No scheduling adapters**~~ — shipped as `batchflow-scheduler`, see
  Unreleased.
- Parallel and partitioned steps are not implemented. A reader is `&mut self`,
  so parallelism comes from partitioning, which is future work.
- No built-in readers or writers (CSV, JSON, SQL). FR-3.4 is open; the traits
  are small enough to implement in a few lines, which is what every example
  does.

[Unreleased]: https://github.com/devcalm/batchflow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/devcalm/batchflow/releases/tag/v0.1.0
