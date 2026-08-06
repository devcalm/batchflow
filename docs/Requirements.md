# BatchFlow — Requirements

> Status: **Living document.** Decisions marked `[DECIDED]`, `[BUILT]`, `[OPEN]`, or `[FUTURE]`.
> `[BUILT]` means implemented *and* covered by a test that fails when the behaviour is removed.
> Last updated: 2026-07-30

BatchFlow is an idiomatic, production-grade batch-processing framework for Rust,
inspired by Spring Batch. It owns the **orchestration layer** (execution engine,
fault tolerance, restartability, metadata) and integrates mature Rust crates for
everything else (async runtime, DB, serialization, tracing, metrics).

---

## 1. Functional Requirements

### FR-1 — Job & Step model
- FR-1.1 A **Job** is an ordered graph of **Steps**. `[DECIDED]` Start linear; DAG/branching `[FUTURE]`.
- FR-1.2 A **Step** is either a **chunk-oriented step** or a **tasklet** (single unit of work).
- FR-1.3 Jobs and Steps are defined in code (builders), not config/reflection. `[DECIDED]`

### FR-2 — Chunk-oriented processing
- FR-2.1 A chunk step runs a `read → process → write` loop with a configurable **commit interval** N.
- FR-2.2 The **writer is chunk-oriented**: it receives a slice of N items in one call (enables batched I/O).
- FR-2.3 The **processor may filter** an item (return `None`), dropping it from the chunk.
- FR-2.4 One chunk = one transaction: business data + metadata counters + reader bookmark commit atomically.

### FR-3 — Reader / Processor / Writer abstractions
- FR-3.1 `ItemReader<Item>` — pull model; `None` signals exhaustion, `Err` signals failure.
- FR-3.2 `ItemProcessor<In, Out>` — transform; `None` filters the item.
- FR-3.3 `ItemWriter<Item>` — consume a chunk (`&[Item]`).
- FR-3.4 Built-in impls (phased): CSV, JSON, SQL. `[FUTURE]` Kafka, S3.
- FR-3.5 Composite processors (chaining) and validating/filtering processors.

### FR-4 — Metadata & persistence (JobRepository)
- FR-4.1 Persist `JobInstance`, `JobExecution`, `StepExecution`, `ExecutionContext`.
- FR-4.2 `JobParameters` form the **identity key** of a `JobInstance` (dedup / idempotency).
- FR-4.3 Storage is **trait-based**; swappable backends. Order: InMemory → Postgres → Redis.
- FR-4.4 A completed JobInstance cannot be re-run with identical parameters (matches Spring Batch).

### FR-5 — Restartability
- FR-5.1 A failed JobExecution can be **restarted**: completed steps are skipped, the failed step resumes.
- FR-5.2 Resume uses the persisted `ExecutionContext` bookmark; the reader seeks forward.
- FR-5.3 Restart must not duplicate already-committed items (guaranteed by FR-2.4 atomicity).

### FR-6 — Fault tolerance
- FR-6.1 **Retry policy**: re-attempt the **write and commit** on classified *transient* errors, up to a limit, with backoff. `[BUILT]`
  Scope narrowed during Phase 10b, and the narrowing is forced rather than chosen: `ItemProcessor::process` takes its
  item **by value**, so a second attempt has no input left to re-process, whereas `ItemWriter::write` borrows a slice
  and can be re-issued freely. Retrying the processor would require `P::In: Clone` threaded through `ChunkStep`,
  `Step` and `Job`. Spring re-runs the processor and pays for it with a documented idempotency obligation on the user.
  **Each attempt opens a fresh transaction** — a rolled-back one cannot be reused, which the type system enforces
  because `StepCommit::rollback`/`commit` take `Tx` by value. The commit itself is inside the retry scope
  (Postgres raises `40001` *at* `COMMIT`).
- FR-6.2 **Skip policy**: tolerate classified *bad-item* errors on **read and process** up to a step-wide skip limit;
  increment `skip_count`. `[BUILT]`
  Read and process are per-item, so the failing item is known. A **write** error names a whole chunk, not an item —
  see FR-6.4. Exceeding the limit fails the step with `BatchError::SkipLimitExceeded`, carrying the item error as its
  source: "one odd row" and "this input file is wrong" are different pages for an operator.
- FR-6.3 **Error classification** via a trait (Rust replacement for exception hierarchies). `[BUILT]`
  `Classifier::classify(&self, &BatchError) -> ErrorAction{Retry, Skip, Fail}`; default `FailFast`, so fault tolerance
  is opt-in per step. Requires errors to *carry* their cause: the wrapping `BatchError` variants hold a boxed
  `Cause`, since a stringified `sqlx::Error` cannot be classified. `PostgresClassifier` maps SQLSTATE and lives in
  `batchflow-postgres` — core never learns a SQLSTATE.
- FR-6.4 **Chunk-scanning** on write failure to isolate a poison item. `[BUILT]` — inherited, opt-in via
  `FaultTolerance::scan_on_write_failure(true)`, off by default.
  A write error names N items, so `ErrorAction::Skip` could not apply to one. The scan is **two passes, and the split
  is forced rather than chosen**: each item is written alone in a throwaway transaction that is *always rolled back*,
  then the survivors are written once through the ordinary commit point. Committing item by item would be cheaper and
  wrong — `ItemReader::update` reports a position *past the whole chunk*, so there is no way to express "bookmark
  after item three", and a crash midway would leave the bookmark behind items already durable. Cost: `N + 1`
  transactions and every good item written twice, on the failure path only. With an `Unmanaged` writer the
  identifying pass really delivers, so a 1000-item chunk with one bad row sends ~2000 items — no promise is broken
  (`Unmanaged` is already at-least-once) but it is documented as a reason to opt in deliberately.
- FR-6.5 Dead-letter routing for skipped items. `[FUTURE]`
  Note the gap this leaves today: a skipped item is counted and then **gone**. `skip_count` says how many rows were
  discarded and nothing says which.

### FR-7 — Listeners / lifecycle hooks
- FR-7.1 Hooks: before/after job, step, chunk, read, process, write, on-error, on-skip.
- FR-7.2 Idiomatic form `[OPEN]`: trait objects vs. typed callbacks vs. an event stream.

### FR-8 — Observability
- FR-8.1 **Metrics** `[DONE 2026-07-31]`: jobs/steps started/finished by status, items read/written/filtered/skipped (by phase), chunks committed, retries, chunk and step duration. Emitted through the `metrics` facade from `batchflow-core`; Prometheus exporter in `batchflow-metrics`. Throughput is deliberately *not* a metric — it is `rate(batchflow_items_written_total[5m])`, a question for the query language rather than a pre-averaged number the library computes at one window it chose.
- FR-8.2 **Tracing**: spans per job/step/chunk; correlation IDs; OpenTelemetry export.

### FR-9 — Scheduling (integration, not engine)
- FR-9.1 Provide a clean launch API so external schedulers (cron, k8s CronJob, tokio-cron-scheduler) can trigger jobs.
- FR-9.2 BatchFlow does **not** implement its own scheduling engine. `[DECIDED]`

### FR-10 — Scaling
- FR-10.1 **Partitioning**: split input into partitions, run step instances in parallel (local). `[FUTURE-ish]`
- FR-10.2 Multi-threaded single step. `[OPEN]` — interacts with `&mut` reader constraint.
- FR-10.3 Remote chunking / distributed. `[FUTURE]`

---

## 2. Non-Functional Requirements

- NFR-1 **Idiomatic Rust**: traits, generics, ownership, `Result`, async-first. No reflection/hidden magic/global state.
- NFR-2 **Correctness first**: restart and transaction semantics are the framework's reason to exist.
- NFR-3 **Modularity**: Cargo workspace; core has minimal deps; backends are opt-in crates/features.
- NFR-4 **Async-first**: `async` traits; Tokio the reference runtime, but avoid leaking Tokio into core traits where feasible.
- NFR-5 **Zero warnings**: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test` all clean. No dead code, no needless clones/allocs.
- NFR-6 **API stability**: follow Rust API Guidelines + SemVer; `#[non_exhaustive]` on public enums that will grow (e.g. `BatchError`).
- NFR-7 **Testability**: InMemory backend enables fast deterministic tests; property + failure-injection tests for the engine.
- NFR-8 **Documentation**: every public item documented; doctests compile; runnable examples.

---

## 3. User Stories

- **US-1** As a data engineer, I define a job that reads a 10M-row CSV, transforms rows, and bulk-inserts to Postgres, with a tunable commit interval.
- **US-2** As an operator, when a job crashes at row 4M, I restart it and it resumes near where it stopped — no duplicated inserts.
- **US-3** As a developer, I skip malformed rows (bad-item errors) while failing fast on infrastructure errors (DB down).
- **US-4** As a developer, transient DB deadlocks are retried transparently with backoff.
- **US-5** As an SRE, I scrape Prometheus metrics for throughput and see per-step spans in my tracing backend.
- **US-6** As a platform team, I trigger jobs from a k8s CronJob without BatchFlow owning scheduling.
- **US-7** As a framework user, I implement a custom `ItemReader` for a proprietary source by implementing one trait.

---

## 4. Constraints

- **C-1** Rust edition 2024, recent stable toolchain (native `async fn` in traits, ≥1.75).
- **C-2** Own only orchestration; integrate crates for runtime/DB/serde/tracing/metrics.
- **C-3** Chunk step contract fixed: item reader/processor + **chunk** writer; commit interval = transaction boundary.
- **C-4** `&mut self` readers ⇒ readers are not `Sync`-shareable; parallelism requires partitioning, not sharing.
- **C-5** No exceptions: control flow via `Result` + explicit classification.

---

## 5. Performance Goals `[OPEN — to benchmark, not guess]`

- **P-1** Chunk-loop overhead negligible vs. actual read/process/write I/O.
- **P-2** Commit interval amortizes transaction + metadata cost; per-item commits are a non-goal (anti-pattern).
- **P-3** Batched writer I/O (one round-trip per chunk).
- **P-4** No per-item heap allocation beyond what the item type inherently needs.
- **P-5** Establish criterion benchmarks in Phase 17 before claiming any number.

---

## 6. Security Goals

- **S-1** No secrets in metadata/`ExecutionContext` snapshots (or explicit redaction hooks).
- **S-2** Parameterized SQL only in built-in SQL reader/writer.
- **S-3** Backend credentials via config crates / env, never hard-coded.
- **S-4** `ExecutionContext` serialization must not deserialize untrusted arbitrary types (avoid Java-style gadget risk).

---

## 7. Future Requirements `[FUTURE]`

- DAG/branching job flows, conditional transitions on exit status.
- Additional backends: SQLite, DynamoDB, MongoDB.
- Additional readers/writers: Kafka, S3, Parquet.
- Remote partitioning / remote chunking (distributed execution).
- Dead-letter queue integration.
- A CLI (`clap`) to launch/inspect jobs.
- A `JobExplorer` read-side API for dashboards.
