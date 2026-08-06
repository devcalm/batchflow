# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

All five crates — `batchflow`, `batchflow-core`, `batchflow-postgres`,
`batchflow-redis` and `batchflow-metrics` — share one version number and are
released together.

## [Unreleased]

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

- **The launcher's gate is not race-free.** Instance identity is enforced by a
  unique constraint in Postgres, but reading the last execution and creating
  the next one are two statements outside a transaction, so two processes
  racing an instance that has no prior execution can both launch.
- **No scheduling adapters** (planned; additive).
- Parallel and partitioned steps are not implemented. A reader is `&mut self`,
  so parallelism comes from partitioning, which is future work.

[Unreleased]: https://github.com/devcalm/batchflow/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/devcalm/batchflow/releases/tag/v0.1.0
