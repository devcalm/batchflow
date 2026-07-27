# BatchFlow — Architecture

> Status: **Living document.** Phase 0. Records the model, the crate layout, technology choices, and ADRs.
> Last updated: 2026-07-24

---

## 1. Architecture Overview

BatchFlow is **a durable state machine with a transactional inner loop**. Two things bolted together:

1. **Execution engine** — walks a Job's Steps; inside a chunk step runs a tight `read → process → write` loop.
2. **Metadata store (JobRepository)** — records every step of that walk so the machine can be killed and resumed.

Almost every "feature" is emergent from these two interacting:
- **Restartability** = the engine consulting persisted state before each move.
- **Fault tolerance** = policies perturbing the inner loop.
- **Observability** = instrumentation wrapped around the loop.

### The two families of domain objects

**Definitions (blueprint, immutable, in-memory, author-written):**
`Job`, `Step`, `ItemReader<I>`, `ItemProcessor<I,O>`, `ItemWriter<O>`.

**Executions (runtime facts, mutable, persisted, engine-written):**
`JobInstance` (identity = hash of `JobParameters`), `JobExecution` (one attempt),
`StepExecution` (one step attempt + counters), `ExecutionContext` (serializable bookmark bag).

> The **JobInstance vs JobExecution** distinction is what makes restart possible.
> `JobParameters` are the identity key: they decide "new instance" vs "restart of an existing one."

---

## 2. Execution Flow — the chunk loop (spine)

Spring Batch splits this across `TaskletStep` / `RepeatTemplate` / `ChunkOrientedTasklet` /
`ChunkProvider` / `ChunkProcessor`. BatchFlow collapses the Java loop-abstractions (`RepeatTemplate`)
into plain Rust `loop`s and keeps the meaningful seams (provide vs process, TX ownership).

```
Step (owns the transaction + StepExecution):
  loop (chunk loop):
    tx = repository.begin()                       # BEGIN
    // ---- read side (ChunkProvider) ----
    chunk = []
    for _ in 0..commit_interval:
        match reader.read().await? {              # None → exhausted; Err → rollback
            Some(i) => chunk.push(i),
            None    => break,
        }
    if chunk.is_empty(): tx.commit(); return DONE
    // ---- process + write side (ChunkProcessor) ----
    out = []
    for item in chunk:
        if let Some(o) = processor.process(item).await? { out.push(o) }  # None → filtered
    writer.write(&out).await?                      # ONE call, whole chunk
    // ---- persist, atomically with data ----
    let contribution = StepContribution { read, write, filter, ... }  # pending deltas
    step_execution.apply(contribution)
    reader.update(&mut step_execution.execution_context)  # bookmark
    repository.update(&step_execution, &tx).await?
    tx.commit().await?                             # COMMIT: data + metadata together
    # on any `?` error → tx.rollback(); apply skip/retry policy or fail the step
```

**Load-bearing invariants:**
- **TX boundary = commit interval.** Bigger N ⇒ fewer commits, faster, but more re-done on crash + more memory/chunk.
- **StepContribution = pending deltas.** Counters fold in only just before commit, so rollback discards them cleanly. In Rust this is a plain owned struct — no shared mutable state.
- **Atomic bookmark.** Reader position is snapshotted into `ExecutionContext` in the *same* transaction as data + counters ⇒ restart cannot duplicate committed items.

### Restart
Not special code. Load prior `JobExecution`; skip `COMPLETED` steps; hand the failed step's persisted
`ExecutionContext` back to its reader; resume the same loop. Engine can't distinguish "fresh at row 4000"
from "restart at row 4000" — the goal property.

### Fault tolerance as loop perturbations
- **Retry**: wrap process/write; re-attempt on classified transient error up to a limit + backoff.
  Write-failure of a chunk can't tell which item is poison ⇒ optional **chunk scanning** (one-at-a-time). `[OPEN]`
- **Skip**: catch classified bad-item error, `skip_count += 1`, continue up to skip limit.
- Both are **error-classification policies** — driven by `Result<T,E>` + a classifier trait, not exceptions.

---

## 3. Core Traits (target shape)

```rust
// RPITIT with an explicit `+ Send` bound — see ADR-002a.
// Implementors still write plain `async fn`.
trait ItemReader    { type Item; fn read(&mut self) -> impl Future<Output = Result<Option<Self::Item>, BatchError>> + Send; }
trait ItemProcessor { type In; type Out; fn process(&mut self, item: Self::In) -> impl Future<Output = Result<Option<Self::Out>, BatchError>> + Send; }
trait ItemWriter    { type Item; fn write(&mut self, items: &[Self::Item]) -> impl Future<Output = Result<(), BatchError>> + Send; }
```

- `&mut self` reader — stateful cursor ⇒ **not shareable across threads** (parallelism ⇒ partition, don't share). Compile-enforced.
- Writer takes `&[Item]` — least-privilege; writer that needs ownership clones (its cost, not everyone's).
- Processor takes `item: In` by value — it transforms/consumes.

### Storage abstraction (trait-first, phased impls)
`JobRepository`, `ExecutionContextStore`, `CheckpointStore`, `LockProvider`, `MetricsExporter` — all traits.
Impl order: **InMemory → Postgres → Redis**. `[FUTURE]` SQLite, DynamoDB, Mongo.

**Where does the transaction live** in an async Rust `JobRepository`? `[DECIDED 2026-07-27 — see ADR-007]`
Answer: the repository owns it (`type Tx` + `begin`/`commit`/`rollback`), and writers *opt in* to enlisting via
`TransactionalWriter<Tx>`; plain `ItemWriter` impls are adapted with an `Unmanaged<W>` wrapper. Implemented in
Phase 11 against real Postgres, not against the InMemory fake.

---

## 4. Workspace Organization

Cargo workspace; core stays dependency-light, backends/observability are separate opt-in crates.

| Crate | Purpose | Why separate |
|---|---|---|
| `batchflow-core` | Traits, domain types, `BatchError`, chunk-loop engine | Stable heart; minimal deps; what users import |
| `batchflow-memory` | InMemory `JobRepository` etc. | Zero-dep testing + reference impl |
| `batchflow-postgres` | Postgres backend (sqlx) | Heavy dep (sqlx) must be opt-in |
| `batchflow-redis` | Redis backend | Opt-in |
| `batchflow-metrics` | `metrics`/Prometheus exporter | Optional observability |
| `batchflow-tracing` | `tracing`/OpenTelemetry wiring | Optional observability |
| `batchflow-scheduler` | Adapters to external schedulers | Integration, not engine |
| `batchflow-io` | CSV/JSON/SQL readers & writers | Keeps I/O deps out of core |
| `batchflow-testing` | Test harness, fakes, failure injection | Test-only helpers |
| `examples/` | Runnable end-to-end examples | Docs that compile |

> Today the repo is a single crate (`batchflow`). Split into the workspace at **Phase 1**.

---

## 5. Technology Evaluation

Principle: **own orchestration, integrate everything else.** Selections and one-line rationale.

| Category | Selected | Alternatives considered | Rationale |
|---|---|---|---|
| Async runtime | **tokio** | async-std (waning), smol | De-facto standard, ecosystem gravity, mature. Keep it out of *core trait* signatures where possible. |
| Error (lib) | **thiserror** | anyhow (apps only), snafu | Libraries expose typed errors; `anyhow` is for bins. `thiserror` derives our `BatchError`. |
| Async traits | **native `async fn` in traits** | `async-trait` macro | Native is stable + zero-cost; use `async-trait` only where we need `dyn` object safety. See ADR-002. |
| Serialization | **serde** | — | Universal. Backs `ExecutionContext` + params. |
| DB | **sqlx** | diesel (sync/macro-heavy), sea-orm | Async-native, compile-checked queries, Postgres-first. |
| CSV | **csv** | — | Standard, fast, serde-integrated. |
| Filesystem | **tokio::fs** | std::fs (blocking) | Async-consistent. |
| CPU parallelism | **rayon** | — | For CPU-bound processors; bridge to async carefully. |
| Async streams | **futures / tokio-stream** | — | Reader-as-stream adapters. |
| Retry | **backon** | tokio-retry (less active) | Ergonomic backoff builder, async-first, maintained. We own *classification*; delegate *backoff*. |
| Scheduling | **integrate**: tokio-cron-scheduler / k8s CronJob / system cron | building our own | Non-goal to build a scheduler. |
| Metrics | **metrics** (+ Prometheus exporter) | prometheus crate directly | Facade decouples us from exporter choice. |
| Tracing | **tracing** | log | Spans model job/step/chunk naturally. |
| OpenTelemetry | **opentelemetry** (+ tracing-opentelemetry) | — | Standard distributed tracing export. |
| Config | **figment** or **config** | — | `[OPEN]` — decide at Phase 14/scheduler. |
| CLI | **clap** | — | Standard; only for `examples`/tooling. |
| Time | **chrono** | time | `[OPEN]` — chrono for breadth; revisit. |
| UUID | **uuid** | — | Execution IDs. |
| Testing | **rstest**, **proptest**, **testcontainers** | — | Params, property tests, real Postgres in integration. |
| Benchmarking | **criterion** | — | Statistically sound; Phase 17. |

---

## 6. ADRs (Architecture Decision Records)

### ADR-001 — Collapse `RepeatTemplate`; the loop is a `loop`
**Context:** Spring Batch abstracts loops into `RepeatTemplate` objects (pre-lambda Java couldn't pass loop bodies).
**Decision:** Use plain Rust `loop`/`for`. Keep only the meaningful seams (read side vs process/write side, TX ownership).
**Consequences:** Less indirection, clearer control flow. We lose a pluggable "completion policy" object — acceptable; reintroduce as a small enum if needed.

### ADR-002 — Native `async fn` in traits; `async-trait` only for `dyn`
**Context:** Core traits are async. Native async-fn-in-traits is stable but has `dyn`/object-safety + auto-trait-bound caveats.
**Decision:** Prefer native for generic/`impl Trait` paths. Introduce `async-trait` (or manual boxing) only where trait objects are required (e.g. heterogeneous step lists, plugin readers).
**Consequences:** Zero-cost in the hot path; localized macro use where dynamism is genuinely needed. Revisit if RPITIT ergonomics change.

#### ADR-002a — Amendment: the framework is `Send` `[DECIDED 2026-07-27]`
**Context:** The original decision used `#[async_trait(?Send)]` for `Step`, which made `Job::run()`'s future non-`Send`. Consequence discovered by compiling an assertion: **`tokio::spawn(job.run())` did not compile** — jobs were locked to a current-thread runtime. `#[tokio::test]` defaults to current-thread, so no test caught it.

**Decision:** `Send` is a framework-wide requirement.
1. `Step: Send` supertrait + `#[async_trait]` (not `?Send`). `Box<dyn Step>` is then `Send` for free — no `+ Send` at use sites.
2. The three core traits keep native async but declare the bound explicitly via **RPITIT**: `fn read(&mut self) -> impl Future<Output = ..> + Send`. The `async fn` sugar desugars to an *unnameable* associated future type, so `R: ItemReader + Send` cannot constrain it — writing the return type by hand is the only way to say `+ Send`. Implementors are unaffected and still write `async fn`.
3. `#[allow(async_fn_in_trait)]` is removed from all three traits — that lint exists precisely to flag the unbounded-future hole this closes.
4. The same rule applies to every future trait, notably `JobRepository` (Phase 7).

**Consequences:** Jobs are spawnable and multi-threaded-runtime-safe. Cost: implementors' futures **must** be `Send` — an `Rc`/`RefCell`-based reader will not compile. Accepted deliberately; if a non-`Send` variant is ever needed, generate both with the `trait_variant` crate rather than forking the traits by hand. Auto-traits are SemVer-visible and silently lost, so `job_run_future_is_send` asserts this at compile time in the test suite.

### ADR-003 — Associated types over generic type params on core traits `[DECIDED 2026-07-24]`
**Context:** `ItemReader<I>` (generic) vs `trait ItemReader { type Item; }` (associated).
**Rule applied:** Generic param = *input* type the caller chooses, multiple coexist per impl (`From<T>`). Associated type = *output* type the impl determines uniquely, functional dependency `Self → Item` (`Iterator::Item`). A reader/processor/writer produces/consumes exactly one type per impl ⇒ output ⇒ associated.
**Decision:** Associated types on all three core traits. `ItemProcessor` gets two (`In`, `Out`) — both are outputs of the impl.
**Consequences:** (1) Compiler enforces "a reader reads one thing" (second impl = conflict error). (2) Inference flows: `read_chunk<R: ItemReader>() -> Vec<R::Item>` needs no turbofish. (3) Step type-wiring is 3 equality bounds (`P: ItemProcessor<In = R::Item>`, `W: ItemWriter<Item = P::Out>`) instead of 4 viral generic params threaded everywhere. (4) Write-anything polymorphism (e.g. JSON) lives on the concrete type via `impl<T: Serialize> ItemWriter for JsonWriter<T> { type Item = T; }` + `PhantomData<T>`, NOT on the trait — keeps the guarantee and the ergonomics.

### ADR-004 — Errors: typed `BatchError` via thiserror, `#[non_exhaustive]`
**Context:** Need retry/skip classification without exceptions.
**Decision:** `BatchError` enum, `#[non_exhaustive]`, derived with thiserror; classification via a trait mapping errors → {Retryable, Skippable, Fatal}.
**Consequences:** Explicit, SemVer-safe growth. Classification is user-overridable.

### ADR-005 — Storage is trait-first; InMemory is the reference impl
**Context:** Must not couple to Postgres.
**Decision:** Define `JobRepository` (+ friends) as traits; ship `batchflow-memory` first, then Postgres, then Redis.
**Consequences:** Fast tests, clean SPI. The hard open problem is transaction ownership across writer + repository (ADR pending, Phase 7).

### ADR-006 — Scheduling is integration, not an engine `[DECIDED]`
BatchFlow exposes a launch API; external schedulers trigger it. No home-grown cron engine.

### ADR-007 — Transaction ownership: opt-in transactional writer `[DECIDED 2026-07-27]`
**Context:** §2's chunk loop promises data + metadata + bookmark commit atomically (FR-2.4), which is what makes restart non-duplicating (FR-5.3). But `ItemWriter::write(&mut self, items)` has no access to a transaction, so as written the promise is unachievable — the writer commits independently of the repository.

**Options considered:**
- **(A) `tx` in every writer** — `write(&mut self, items, tx: &mut Tx)`. Atomic by construction, but `Tx` becomes a viral generic across `ItemWriter`/`ChunkStep`/`Step`/`Job`, and non-transactional writers (CSV, S3, stdout) must accept a transaction they cannot use.
- **(B) Repository owns its own tx** — simplest trait, but data and metadata commit separately ⇒ at-least-once only; FR-2.4/FR-5.3 downgrade from guarantees to best-effort.
- **(C) Opt-in transactional writer** — chosen.

**Decision:** `JobRepository` gains an associated `type Tx` plus `begin`/`commit`/`rollback`. Writers that *can* enlist in the step's transaction implement `TransactionalWriter<Tx>`; plain `ItemWriter` impls are unaffected and are adapted explicitly via an `Unmanaged<W>` wrapper. Exactly-once where the backend supports it, honest degradation where it doesn't.

**Consequences:**
- Two writer traits instead of one. The obvious blanket `impl<W: ItemWriter, Tx> TransactionalWriter<Tx> for W` is **not possible** — it would overlap with any direct impl, and Rust cannot prove a type doesn't implement `ItemWriter` (no negative reasoning without specialization). Hence the explicit `Unmanaged<W>` newtype: different `Self` type ⇒ no coherence conflict.
- `JobRepository` is used as a *generic* parameter (`R: JobRepository`), not `Box<dyn>`. There is exactly one repository per job — heterogeneity is what justifies `dyn` (as for `Step`), and it is absent here. This also sidesteps having to name `Tx` in a trait-object type.
- Per ADR-002a, `JobRepository: Send + Sync` and its futures are `+ Send`.

**Implementation is deferred to Phase 11**, when a real sqlx backend exists. Rationale: an InMemory repository has no transactions and will satisfy *any* `begin`/`commit` shape, including a wrong one — validating a transaction abstraction against a fake is how it ends up wrong. Phase 7b therefore ships metadata CRUD + identity dedup only, with no `Tx` in the trait yet. Adding it is a breaking change, which is free pre-0.1.0.

**Amends ADR-005:** the InMemory repository lives in `batchflow-core` rather than a separate `batchflow-memory` crate — it needs zero extra dependencies and core's own tests depend on it, which a separate crate would turn into a dev-dependency cycle. Revisit if it grows non-trivial.

---

## 7. Extensibility (SPI surface)

Users/extenders implement: `ItemReader`, `ItemProcessor`, `ItemWriter`, `JobRepository` (+ stores),
error `Classifier`, listeners. Everything else is internal orchestration. New backends/readers = new crates,
no core changes. This is the "own the orchestration, not every component" principle in practice.
