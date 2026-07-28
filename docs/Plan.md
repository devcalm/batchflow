# BatchFlow — Implementation Plan

> Status: **Living roadmap.** Phased. Each phase: goals · learning objectives · tasks · rationale · acceptance · testing · docs.
> Mentoring cadence per phase: **concept → Rust lens → minimal sketch → you implement → I review → improve.**
> Last updated: 2026-07-28

**Legend:** ☐ not started · ◐ in progress · ☑ done

---

## Phase 0 — Research & Architecture ◐
- **Goals:** Understand Spring Batch as a running machine; pick the Rust ecosystem; write these docs.
- **Learning objectives:** JobInstance vs JobExecution; the chunk loop; TX boundary = commit interval; StepContribution; atomic bookmark = restart.
- **Tasks:** ☑ Spring Batch execution-model deep dive · ☑ Requirements.md · ☑ Architecture.md · ☑ Plan.md · ☐ finalize tech eval open items.
- **Acceptance:** Docs exist and capture the model + crate choices + ADRs.
- **Testing:** n/a. **Docs:** the three files.

## Phase 1 — Workspace ☑
- **Goals:** Convert single crate → Cargo workspace per Architecture §4.
- **Learning:** Workspace layout, feature flags, dependency hygiene, why core stays thin.
- **Tasks:** Create `batchflow-core` + placeholder crates; wire workspace `Cargo.toml`; CI-style lint gate.
- **Rationale:** Boundaries before code prevents dep leakage (sqlx/tokio into core).
- **Acceptance:** `cargo build` workspace clean; core has minimal deps.
- **Testing:** build only. **Docs:** README workspace map.

## Phase 2 — Core Traits ☑
- **Goals:** `ItemReader`/`ItemProcessor`/`ItemWriter`, `BatchError`, `read_chunk`.
- **Learning:** associated types vs generics (ADR-003); `Option`=exhaust/filter; `Result`=fail; `&mut self` reader ⇒ not `Sync`; async-fn-in-traits caveats.
- **Tasks:** Define 3 traits; `BatchError` (thiserror, `#[non_exhaustive]`); `read_chunk`; a fake reader for tests.
- **Acceptance:** compiles; `clippy -D warnings` clean; user can defend the assoc-type-vs-generic call.
- **Testing:** unit test `read_chunk` (full chunk, partial/EOF, error short-circuit). **Docs:** trait rustdoc + doctest.

## Phase 3 — Job Model ☑ (basic: `Job` holds `Vec<Box<dyn Step>>`, runs in order, fail-fast; builders/DAG deferred)
- **Goals:** `Job`, `Step` definitions + builders; linear step ordering.
- **Learning:** builder pattern in Rust; typestate for build-time validation; DAG deferred.
- **Tasks:** `Job`/`Step` types; `JobBuilder`/`StepBuilder`; heterogeneous step list (trait objects, ADR-002).
- **Acceptance:** define a 2-step job in code. **Testing:** builder unit tests. **Docs:** example.

## Phase 4 — Step Model ◐ (`StepContribution` + `StepExecution` done in 7c-2; dedicated tasklet trait pending)
- **Goals:** Chunk-step vs tasklet-step; `StepContribution`; `StepExecution` counters.
- **Learning:** pending-delta pattern; step status lifecycle.
- **Tasks:** ☑ `StepExecution` (id/job_execution_id/step_name/status/counters) · ☑ `StepContribution` (private fields, `increment_*` only, `apply` folds) · ☑ status lifecycle (`Starting`→`Started`→`Completed`/`Failed`) · ☐ tasklet trait (today a tasklet is a hand-written `Step` impl that ignores its contribution).
- **Acceptance:** ☑ counters fold in correctly. Rollback-discards-deltas is structural today (an errored chunk yields no contribution to fold) and becomes a real transaction in Phase 11. **Testing:** unit. **Docs:** rustdoc.

## Phase 5 — Execution Engine ☑ (basic: `run_step` chunk loop + `Step`/`ChunkStep`; no TX/persistence yet)
- **Goals:** Drive a Job through its Steps; the chunk loop (no persistence/TX yet).
- **Learning:** ownership of reader/processor/writer during a run; where `?` triggers failure.
- **Tasks:** step executor running `read_chunk` → process → write with in-memory counters.
- **Acceptance:** end-to-end in-memory job runs to completion. **Testing:** integration w/ fakes. **Docs:** flow diagram.

## Phase 6 — Chunk Processing (full) ◐ (semantics + `StepContribution` integration done; property tests + tuning guide pending)
- **Goals:** commit interval, filtering, empty-chunk termination, chunk-oriented writer semantics.
- **Learning:** memory vs throughput tradeoff of N; batched writes.
- **Tasks:** ☑ chunk semantics (`NonZeroUsize` interval, empty chunk terminates, `write(&[O])`) · ☑ `StepContribution` integration — `process_chunk` returns a chunk-local contribution, `run_step` folds it per chunk · ☐ property tests · ☐ tuning guide.
- **Acceptance:** ☑ filter drops items and is counted at the `None` arm, not derived by subtraction. **Testing:** property tests on counts. **Docs:** tuning guide.

## Phase 7 — JobRepository ☑  ← **first hard problem**
> **7a ☑** domain types (`JobParameters`/`JobInstance`/`JobExecution`/`BatchStatus`/newtype ids), module split, `Step::name`.
> **7b ☑** `JobRepository` trait + `InMemoryJobRepository`; identity dedup working; ADR-007 decides TX ownership (opt-in `TransactionalWriter`, **implemented in Phase 11** — an InMemory fake cannot validate a transaction abstraction).
> **7c-1 ☑** `JobLauncher<R: JobRepository>`: resolves the instance, enforces FR-4.4, opens the execution, persists every status transition. `Job` gained a name. `BatchError::JobInstanceAlreadyComplete` is a *struct* variant — domain errors exist for callers who branch (a scheduler must tell "already ran today, skip" from "DB down, page someone").
> **7c-2 ☑** `StepContribution` + `StepExecution` identity, and the engine wiring that needs them. `Job::run<R>(&mut self, JobExecutionId, &R)` drives the per-step lifecycle.

- **Goals:** `JobRepository` trait + InMemory impl; **transaction ownership** design.
- **Learning:** where the `tx` lives across writer + repository update in async Rust; JobInstance identity from params.
- **Tasks:** ☑ trait design · ☑ InMemory impl (ADR-007 amended: it lives in `batchflow-core`, not a separate `batchflow-memory` crate — zero deps, and core's own tests need it) · ☑ instance/execution/step persistence · ☑ atomic update contract (update replaces in place, errors on unknown id).
- **Acceptance:** ☑ metadata persisted; ☑ identity dedup works; ☑ FR-4.4 enforced. **Testing:** unit (property tests deferred to Phase 6). **Docs:** ADR-007.

**Design decisions worth carrying forward:**
- **A step cannot produce a `StepExecution`.** Its id is minted by the repository and a step has no repository, so `Step::run` takes `&mut StepContribution` and returns `Result<(), BatchError>`. That is a type-level consequence, not a style preference.
- **`JobExecution` does not nest its step executions.** They are a flat collection joined by `job_execution_id` and read via `JobRepository::step_executions`, ordered by insertion. Persisting the same fact twice lets the copies drift; the real schema is two tables + FK, so the InMemory repo is built the shape Phase 11's SQL one must have.
- **The rule "a `Job` never sees the repository" expired in 7c-2.** Spring's `AbstractJob` holds one too — something must mint step ids. What survives: the launcher decides *whether* a job may run, the `Job` drives steps, and a **`Step`** never touches the repository.
- **`?` is the bug in any lifecycle method.** `job.run().await?` skips the status write and strands the execution at `Started`; Rust has no `finally` and no async `Drop`. Both the launcher and `Job::run` bind the `Result`, persist the outcome, then propagate last.

## Phase 8 — Execution Context ☐
- **Goals:** serializable bookmark bag per Job/Step execution.
- **Learning:** serde design; typed access; no untrusted deserialization (S-4).
- **Tasks:** `ExecutionContext` type; reader `update`/`open` (ItemStream-like) hooks.
- **Acceptance:** reader persists + restores position. **Testing:** round-trip serde. **Docs:** rustdoc.

## Phase 9 — Restart Support ☐
- **Goals:** resume a failed JobExecution; skip completed steps; reader seeks from bookmark.
- **Learning:** why atomicity (Phase 7) makes restart safe; no duplicate items.
- **Tasks:** restart path in engine (skip steps whose previous `StepExecution` is `Completed`, read via `JobRepository::step_executions`); status checks; reader open-from-context; **`abandon_execution` on `JobRepository` + the `Starting`/`Started` gate that depends on it** (see debt (1) below — the guard and its escape hatch ship together or a crash becomes unrecoverable).
- **Acceptance:** kill at row 4000 → restart resumes, no dupes. **Testing:** failure-injection + restart tests. **Docs:** restart guide.

## Phase 10 — Retry ☐
- **Goals:** retry classified transient errors w/ backoff (backon); optional chunk-scanning.
- **Learning:** classifier trait; item vs chunk retry; poison-item isolation (FR-6.4).
- **Tasks:** `Classifier`; retry wrapper around process/write; backon integration.
- **Acceptance:** transient errors retried, fatal fail fast. **Testing:** injected transient failures. **Docs:** policy guide.

## Phase 11 — Transactions ☐
- **Goals:** real chunk-oriented transactions (Postgres via sqlx); data+metadata atomic commit.
- **Learning:** sqlx transactions; rollback on `?`; commit interval as TX boundary in practice.
- **Tasks:** `batchflow-postgres`; wire step TX; SQL writer in same tx.
- **Acceptance:** crash mid-chunk rolls back both. **Testing:** testcontainers Postgres. **Docs:** ADR.

## Phase 12 — Metrics ☐
- **Goals:** `metrics` facade + Prometheus; counters/histograms per FR-8.1.
- **Tasks:** `batchflow-metrics`; instrument engine. **Acceptance:** scrape shows throughput/retries/skips. **Testing:** metric assertions. **Docs:** dashboards note.

## Phase 13 — Tracing ☐
- **Goals:** `tracing` spans per job/step/chunk; correlation IDs; OTel export.
- **Tasks:** `batchflow-tracing`; span instrumentation. **Acceptance:** nested spans visible. **Testing:** span capture. **Docs:** OTel setup.

## Phase 14 — Scheduling ☐ (`JobLauncher` already exists — this phase is the adapters only)
- **Goals:** launch API + adapters (tokio-cron-scheduler / cron / k8s). No home-grown engine (ADR-006).
- **Tasks:** `batchflow-scheduler` adapters. `JobLauncher` ☑ landed in 7c-1; a scheduler consumes it and branches on `BatchError::JobInstanceAlreadyComplete` to skip a run it has already done. **Acceptance:** external trigger runs a job. **Testing:** launcher unit. **Docs:** integration guide.

## Phase 15 — Storage Backends ☐
- **Goals:** Redis backend; harden Postgres; backend conformance suite.
- **Tasks:** `batchflow-redis`; shared trait test-suite run against every backend. **Acceptance:** all backends pass same suite. **Testing:** testcontainers. **Docs:** backend matrix.

## Phase 16 — Examples ☐
- **Goals:** runnable end-to-end examples (CSV→Postgres, restart demo, retry/skip demo).
- **Acceptance:** `cargo run --example` works; examples compile in CI. **Docs:** examples README.

## Phase 17 — Performance Optimization ☐
- **Goals:** criterion benches; remove needless allocs/clones; validate P-1..P-5.
- **Acceptance:** benchmarked numbers (no guessing before this). **Testing:** criterion. **Docs:** perf report.

## Phase 18 — Documentation ☐
- **Goals:** complete rustdoc, book-style guide, API-guidelines pass, SemVer/CHANGELOG.
- **Acceptance:** docs.rs clean; guide covers concepts+recipes. **Docs:** everything.

---

## Cross-cutting quality gate (every phase)
`cargo fmt` · `cargo clippy -- -D warnings` · `cargo test` (+ doctests) · examples compile · no dead code · no needless clone/alloc.

## Current position (2026-07-28)
Phase 0 ◐ docs (tech-eval open items) · 1 ☑ workspace · 2 ☑ traits · 3 ☑ Job · 4 ◐ step model (tasklet trait pending) · 5 ☑ engine · 6 ◐ chunk processing (property tests/tuning guide pending) · **7 ☑ JobRepository — complete**.
`batchflow-core` modules: `chunk`/`error`/`execution`/`item`/`job`/`launcher`/`memory`/`repository`/`step` + `#[cfg(test)] testing`, with `lib.rs` as a pure re-export surface (private `mod` + flat `pub use`, so module layout stays refactorable). **42 tests green**, clippy `-D warnings` clean, `cargo fmt` clean.

A job now runs end to end through metadata: `JobLauncher::run` resolves a `JobInstance` from `(job_name, JobParameters)`, refuses a completed one (FR-4.4), opens a `JobExecution`, and `Job::run` persists a counted `StepExecution` per step — all reloadable from the repository alone.

**API hardening (2026-07-27):** `BatchError::Process` added (processors could not previously report failure); `chunk_size` is `NonZeroUsize` everywhere, so the silent `chunk_size == 0` no-op — a job reporting success having processed nothing — is unrepresentable; **ADR-002a**: the framework is `Send` end-to-end (`Step: Send` supertrait + RPITIT `+ Send` on the three core traits), so `tokio::spawn(job.run())` compiles. `job_run_future_is_send` and `launcher_run_future_is_send` lock that in.

**Debt closed in Phase 7:** ~~`filter_count` derived as `read - written`~~ — now counted at the processor's `None` arm, so the underflow panic is gone. ~~`Step` has no name/identity~~ — `Step::name()` plus a persisted `StepExecution` per step.

**Known debt, deliberately deferred:**
1. `Starting`/`Started` still passes the FR-4.4 gate. Rejecting it needs `JobRepository::abandon_execution` **in the same change** — a guard with no escape hatch turns a recoverable crash into a permanently unlaunchable instance. Both in Phase 9.
2. `JobLauncher::run` resolves the instance and reads its last execution under **separate lock acquisitions**, so two processes can both pass the gate. Closed by a real transaction in Phase 11.
3. A failing `update_execution` masks the job's original error (`launcher.rs`). Real systems log the cause before propagating — Phase 13.
4. Library-craft gap: no `#![warn(missing_docs)]`, zero doctests, `Job`/`ChunkStep` lack `Debug` (API guideline C-DEBUG).
5. `read_chunk`/`process_chunk`/`run_step` are `pub` without a deliberate SemVer decision — and `run_step`'s signature changed in 7c-2, which is exactly the kind of break that decision governs. Settle it before 0.1.0.

**Next milestone: Phase 8 — `ExecutionContext`.** Phase 7 gives restart its identity and its per-step status; Phase 8 gives it the bookmark, and Phase 9 joins them. Phase 10 (retry/skip via a `Classifier`) is the alternative branch, but restart is the deeper capability and everything for it is now in place.
