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

## Phase 3 — Job Model ☑ (DAG/conditional flow out of scope for 0.1)
- **Goals:** `Job`, `Step` definitions + builders; linear step ordering.
- **Learning:** builder pattern in Rust; typestate for build-time validation; DAG deferred.
- **Tasks:** ☑ `Job`/`Step` types · ☑ heterogeneous step list (trait objects, ADR-002) · ☑ `JobBuilder` with typestate · ☐ `StepBuilder` (`ChunkStep::new` takes five arguments and is not yet painful; revisit if it grows).
- **Acceptance:** ☑ define a 2-step job in code. **Testing:** ☑ builder unit tests + a `compile_fail` doctest. **Docs:** ☑ rustdoc; ☐ runnable example (Phase 16).

**`JobBuilder` typestate (2026-07-28).** `Job::builder("nightly").step(a).step(b).build()`. `build` is defined only on `JobBuilder<HasSteps>`, so an empty job — one that runs, reports success and processes nothing — is a *compile* error, the same class of silent no-op `NonZeroUsize` rules out for chunk sizes. Consequence: `build` returns `Job`, not `Result<Job, _>`, because the only failure it could report is unrepresentable. `step` takes `S: Step + 'static` by value and boxes internally, so callers never write `Box::new`.
**Cost, documented not hidden:** a typestate builder changes type on the first `.step(..)`, so it cannot be driven from a loop. `Job::new(name, Vec<Box<dyn Step>>)` stays public as the dynamic escape hatch.

## Phase 4 — Step Model ◐ (`StepContribution` + `StepExecution` done in 7c-2; dedicated tasklet trait pending)
- **Goals:** Chunk-step vs tasklet-step; `StepContribution`; `StepExecution` counters.
- **Learning:** pending-delta pattern; step status lifecycle.
- **Tasks:** ☑ `StepExecution` (id/job_execution_id/step_name/status/counters) · ☑ `StepContribution` (private fields, `increment_*` only, `apply` folds) · ☑ status lifecycle (`Starting`→`Started`→`Completed`/`Failed`) · ☐ tasklet trait (today a tasklet is a hand-written `Step` impl that ignores its contribution).
- **Acceptance:** ☑ counters fold in correctly. Rollback-discards-deltas is structural today (an errored chunk yields no contribution to fold) and becomes a real transaction in Phase 11. **Testing:** unit. **Docs:** rustdoc.

## Phase 5 — Execution Engine ☑ (chunk loop + per-step persistence; **real transactions** in Phase 11)
- **Goals:** Drive a Job through its Steps; the chunk loop.
- **Learning:** ownership of reader/processor/writer during a run; where `?` triggers failure — and where it must *not* be used, because it skips the status write that follows (see Phase 7).
- **Tasks:** ☑ step executor running `read_chunk` → process → write · ☑ counters via `StepContribution`, folded per chunk · ☑ `Job::run` persisting a `StepExecution` per step (added in 7c-2; this phase's original "no persistence yet" caveat no longer applies).
- **Acceptance:** ☑ end-to-end job runs to completion and is fully reconstructible from the repository. **Testing:** integration w/ fakes. **Docs:** ☐ flow diagram.

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

## Phase 8 — Execution Context ☑
- **Goals:** serializable bookmark bag per Job/Step execution.
- **Learning:** serde design; typed access; no untrusted deserialization (S-4).
- **Tasks:** ☑ `ExecutionContext` + `ContextValue` · ☑ reader `open`/`update` hooks · ☑ threaded through `run_step`/`ChunkStep`/`Step::run` · ☑ `execution_context` persisted on `StepExecution` and `JobExecution`.
- **Acceptance:** ☑ reader persists + restores position. **Testing:** ☑ round-trip serde + resume + partial-failure. **Docs:** ☑ rustdoc.

**Design decisions:**
- **S-4 shapes the type.** `ContextValue` is a *closed* enum (`String`/`Long`/`Bool`). Spring stores serialized Java objects in its metadata tables, which is where its deserialization-gadget CVEs came from; restricting the wire format to three primitives removes the vulnerability class structurally. Never add a variant holding arbitrary data.
- **Absent ≠ malformed.** `get_long` returns `Result<Option<i64>, _>`: `Ok(None)` for a missing key, `Err` for a wrong type. Collapsing those would let a garbled bookmark silently restart a half-finished job from zero and re-write every committed item — the one outcome restart exists to prevent.
- **Spring's `ItemStream` is merged into `ItemReader`** as `open`/`update` with *default bodies*. A separate opt-in trait would hit ADR-007's blanket-impl wall and force an `Unmanaged<R>` newtype at every call site; default bodies give a non-restartable reader Spring's exact semantics with zero boilerplate. `open` is async and fallible (it may seek); `update` is sync and infallible (writing a position into a map cannot fail).
- **`ContextValue` derives `Eq`**, which is what lets `StepExecution` keep its own. Adding `Double(f64)` later would therefore be **breaking**, not the free change `#[non_exhaustive]` usually allows — `NaN != NaN` kills the derive. That cost being visible is the point.
- **The bookmark is written on failure too.** `Job::run` stores the context regardless of outcome: a step that died at item 900 has a bookmark at 900, and that is the only reason Phase 9 can skip those 900.

**Left for Phase 9:** `Job::run` still passes each step a *fresh* context. Substituting the previous attempt's context is the change that turns a re-run into a resume. Nothing populates `JobExecution::execution_context` yet — the field exists for cross-step data.

## Phase 9 — Restart Support ☑
> **9a ☑** the running-execution gate + its escape hatch. `JobRepository::abandon_execution`; `BatchError::JobExecutionAlreadyRunning` / `CannotAbandon`; the launcher gate is now an exhaustive `match` over `BatchStatus`.
> **9b ☑** the resume. `JobRepository::last_step_execution(instance_id, step_name)`; `Job::run` takes `&JobExecution` and, per step, skips a previously `Completed` one (FR-5.1) or seeds its context from the last attempt's bookmark (FR-5.2, FR-5.3).

- **Goals:** ☑ resume a failed JobExecution; ☑ skip completed steps; ☑ reader seeks from bookmark.
- **Learning:** why atomicity (Phase 7) makes restart safe; no duplicate items.
- **Tasks:** ☑ restart path in engine · ☑ reader open-from-context · ☑ **`abandon_execution` + the `Starting`/`Started` gate**, shipped together.
- **Acceptance:** ☑ fail mid-step → restart resumes, no dupes (`a_restart_resumes_from_the_bookmark_without_duplicating`). **Testing:** ☑ failure-injection + restart tests. **Docs:** ☐ restart guide (Phase 18).

**9b design decisions (2026-07-29):**
- **The lookup is keyed on the `JobInstance`, not the `JobExecution`** — `last_step_execution(instance_id, step_name)`. That is the question restart asks ("has this step ever succeeded for this unit of work?"), and the attempt that succeeded is by definition a *different* execution from the one asking. Phase 11 renders it as a two-table join with `ORDER BY id DESC LIMIT 1`. Mutation-tested: dropping the instance predicate fails `a_bookmark_does_not_leak_across_instances`.
- **`Job::run` widened from `JobExecutionId` to `&JobExecution`.** 7c-2 chose the narrow parameter on the grounds that "a parameter is easier to widen than to narrow" — this is that bill coming due, and it was cheap exactly as predicted. The execution is what knows its instance.
- **A skipped step gets no `StepExecution` on the new attempt.** The record of that work belongs to the attempt that did it; a second `Completed` row for work that did not happen would make counters lie about where reads and writes occurred. Documented consequence: `step_executions(execution_id)` on a restart lists only the steps that ran, so "what did this job do overall?" is a question about the *instance*.
- **The lookup must precede `create_step_execution`.** Mint first and the query returns this attempt's own record — `Starting`, empty context — so nothing is ever skipped and every reader silently restarts from zero, while the job still reports success. Mutation-tested: reordering fails both restart tests. The requirement is stated in `last_step_execution`'s rustdoc rather than enforced by types; a stronger design would be a lookup that excludes the current execution id (worth revisiting in Phase 11).
- **Restart is not a mode.** A fresh run is the same code path with every lookup returning `None`. There is no `if restarting` branch anywhere, which is why `a_first_run_starts_every_step_from_an_empty_context` matters.
- **`SharedSink` test double.** Restart tests must observe writes across *both* attempts, and each attempt builds a fresh `ChunkStep` (as a restarted process does), so a step-owned writer takes its evidence with it — two runs each writing `[4, 8]` into private buffers look identical to one correct run. Duplicates are only visible in a destination that outlives the step.

**9a design decisions (2026-07-29):**
- **`Completed` cannot be abandoned; `Starting`/`Started` can.** `abandon_execution` writes to exactly the field the FR-4.4 gate reads, so permitting it on a `Completed` execution would make an already-run instance relaunchable in two calls. **This diverges from Spring deliberately in the other direction:** Spring's `SimpleJobOperator.abandon` refuses anything "less than STOPPING", which includes `STARTED` — so a `SIGKILL`ed Spring job is stranded and the documented remedy is hand-editing `BATCH_JOB_EXECUTION` with SQL. Allowing it here is the whole point of the escape hatch.
- **The cost of that divergence is honesty in the rustdoc.** With no timestamps and no heartbeat (Phase 12), the repository cannot tell "crashed" from "still running", so `abandon_execution` is documented as an *operator assertion* that the process is dead. A framework that guessed here would run two copies of a billing job.
- **`abandon_execution` takes an id, not a `&JobExecution`.** Given the struct, it would decide a safety question from a `status` the caller may have read minutes ago. The id forces it to read the current one — and read, check and write all happen under one `lock()`, or the check is not a check.
- **The gate is an exhaustive `match`, never `_ => {}`.** `BatchStatus` is `#[non_exhaustive]` for *other* crates, but inside the defining crate a new variant now stops the build at that line and forces a decision. Verified: deleting one arm yields `E0004`.
- **Narrow error variant over a general one.** `CannotAbandon { execution_id, status }` rather than `IllegalStatusTransition { from, to }` — there is exactly one status rule today, and generalising from one example guesses at what Phase 10 needs. Merge when a second real case appears.
- **The ordering requirement became self-enforcing.** Create-then-check used to merely litter the store; with the `Starting` arm in place the launcher now rejects *its own* freshly-created execution (it is the most recent, and it is `Starting`). Mutation-tested: reordering fails 9 tests, not 1.

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
`cargo fmt --check` · `cargo clippy --workspace --all-targets -- -D warnings` · `cargo test --workspace` · examples compile · no dead code · no needless clone/alloc.
**Read the `Doc-tests batchflow_core` count, do not just read the unit count** — see debt (6). A doctest can disappear without anything going red.

## Current position (2026-07-29)
Phase 0 ◐ docs (tech-eval open items) · 1 ☑ workspace · 2 ☑ traits · 3 ☑ Job + typestate `JobBuilder` · 4 ◐ step model (tasklet trait pending) · 5 ☑ engine · 6 ◐ chunk processing (property tests/tuning guide pending) · 7 ☑ JobRepository · 8 ☑ ExecutionContext · **9 ☑ restart**.
`batchflow-core` modules: `chunk`/`context`/`error`/`execution`/`item`/`job`/`launcher`/`memory`/`repository`/`step` + `#[cfg(test)] testing`, with `lib.rs` as a pure re-export surface (private `mod` + flat `pub use`, so module layout stays refactorable). **70 unit tests + 2 doctests green**, clippy `--all-targets -D warnings` clean, `cargo fmt --check` clean.

**US-2 now holds end to end:** a job that dies mid-step is relaunched against the same `JobInstance`, skips the steps that completed, opens its reader at the last committed chunk, and writes no item twice.

A job now runs end to end through metadata: `JobLauncher::run` resolves a `JobInstance` from `(job_name, JobParameters)`, refuses a completed one (FR-4.4), opens a `JobExecution`, and `Job::run` persists a counted `StepExecution` per step — all reloadable from the repository alone.

**API hardening (2026-07-27):** `BatchError::Process` added (processors could not previously report failure); `chunk_size` is `NonZeroUsize` everywhere, so the silent `chunk_size == 0` no-op — a job reporting success having processed nothing — is unrepresentable; **ADR-002a**: the framework is `Send` end-to-end (`Step: Send` supertrait + RPITIT `+ Send` on the three core traits), so `tokio::spawn(job.run())` compiles. `job_run_future_is_send` and `launcher_run_future_is_send` lock that in.

**Debt closed in Phase 7:** ~~`filter_count` derived as `read - written`~~ — now counted at the processor's `None` arm, so the underflow panic is gone. ~~`Step` has no name/identity~~ — `Step::name()` plus a persisted `StepExecution` per step.

**Known debt, deliberately deferred:**
1. ~~`Starting`/`Started` still passes the FR-4.4 gate.~~ **Closed in 9a** — rejected, with `abandon_execution` shipped in the same change.
2. `JobLauncher::run` resolves the instance and reads its last execution under **separate lock acquisitions**, so two processes can both pass the gate. Closed by a real transaction in Phase 11. *(9a narrowed the window but did not close it: the gate now rejects a concurrent `Starting`, so the loser of the race is far likelier to be caught — but two callers can still both read "no execution" and both create one.)*
3. A failing `update_execution` masks the job's original error (`launcher.rs`). Real systems log the cause before propagating — Phase 13.
4. Library-craft gap: no `#![warn(missing_docs)]`, `Job`/`ChunkStep` lack `Debug` (API guideline C-DEBUG).
   ~~Implementing `Step` requires the caller to depend on `async-trait` directly.~~ **Closed in 9a** by `pub use async_trait::async_trait;` in `lib.rs`. Worth recording *how* it was found: the `Job::builder` doctest could not be written without it. Doctests compile as an external crate — they see the crate plus its **dev-dependencies**, not its normal dependencies — so they are the only tests that stand where a user stands. Unit tests structurally cannot feel this class of bug.
5. `read_chunk`/`process_chunk`/`run_step` are `pub` without a deliberate SemVer decision — and `run_step`'s signature changed in 7c-2, which is exactly the kind of break that decision governs. Settle it before 0.1.0.
6. **Doc-comment tests have no build-time protection against deletion.** The `Job::builder` `compile_fail` block has now been stripped along with its surrounding rustdoc **twice** (most recently by commit `9659a37`, which also deleted `Job::run`'s docs and left `Plan.md` claiming doctests existed when the count was 0). Nothing fails when it vanishes. Mitigation for now: the block says in its own text that it is a test, and the `Doc-tests` count in `cargo test` output is part of the quality gate below.

**Next milestone: Phase 10 — retry/skip via a `Classifier`.** FR-5 is closed, so the remaining fault-tolerance gap is *within* a step rather than between attempts: today any error fails the whole step, and a single poison row costs a full restart cycle. Phase 11 (real transactions) is the alternative branch and is what makes the chunk-loop guarantees real rather than structural — ADR-007's `TransactionalWriter` has been waiting on it since Phase 7, and note that everything restart currently promises is validated only against an in-memory fake.
