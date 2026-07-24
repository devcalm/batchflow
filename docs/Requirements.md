# BatchFlow — Requirements

> Status: **Living document.** Phase 0 (research). Decisions marked `[DECIDED]`, `[OPEN]`, or `[FUTURE]`.
> Last updated: 2026-07-24

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
- FR-6.1 **Retry policy**: re-attempt process/write on classified *transient* errors, up to a limit, with backoff.
- FR-6.2 **Skip policy**: tolerate classified *bad-item* errors up to a skip limit; increment `skipCount`.
- FR-6.3 **Error classification** via a trait (Rust replacement for exception hierarchies). `[DECIDED]`
- FR-6.4 Chunk-scanning on write failure to isolate a poison item. `[OPEN]` — decide whether to inherit.
- FR-6.5 Dead-letter routing for skipped items. `[FUTURE]`

### FR-7 — Listeners / lifecycle hooks
- FR-7.1 Hooks: before/after job, step, chunk, read, process, write, on-error, on-skip.
- FR-7.2 Idiomatic form `[OPEN]`: trait objects vs. typed callbacks vs. an event stream.

### FR-8 — Observability
- FR-8.1 **Metrics**: jobs/steps started/completed/failed, throughput, chunk duration, retries, skips.
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
