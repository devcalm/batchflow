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
- **Acceptance:** ☑ *step returns an error* mid-run → restart resumes, no dupes (`a_restart_resumes_from_the_bookmark_without_duplicating`). ☑ *process killed* mid-step → resumes: **this was NOT met by Phase 9** and was closed in 11a; see there. **Testing:** ☑ failure-injection + restart tests. **Docs:** ☐ restart guide (Phase 18).

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

## Phase 11 — Transactions ☑
> **11a ☑** the commit point exists. `StepCommit` trait; `run_step` commits per chunk; `Job::run` supplies a `RepositoryCommit` owning the `StepExecution`.
> **11b ☑** `type Tx` + `begin`/`commit`/`rollback`/`update_step_execution_in` on `JobRepository`; `Tx` threaded through `StepCommit<Tx>`, `Step<Tx>`, `Job<Tx>`, `JobBuilder<State, Tx>` (all defaulting to `()`).
> **11c ☑** `TransactionalWriter<Tx>` + `Unmanaged<W>`.
> **11d ☑** `batchflow-postgres`: 3-table schema, embedded migrations, `PostgresJobRepository` with `Tx = sqlx::Transaction<'static, Postgres>`.
> **11e ☑** testcontainers integration tests — 7 against a real Postgres.

- **Goals:** real chunk-oriented transactions (Postgres via sqlx); data+metadata atomic commit.
- **Learning:** sqlx transactions; rollback on `?`; commit interval as TX boundary in practice.
- **Acceptance:** crash mid-chunk rolls back both. **Testing:** testcontainers Postgres. **Docs:** ADR.

**11a — the commit point (2026-07-29).** Persisting per chunk means the chunk loop must reach the repository, which collides with 7c-2's rule that a `Step` never touches one — and it cannot: `R` as a generic parameter kills `Box<dyn Step>`, and `JobRepository` uses RPITIT so `&dyn JobRepository` does not exist. Resolved with an object-safe seam, `StepCommit`, which a step commits *through*. `Job::run` supplies `RepositoryCommit`, which owns the `StepExecution` and the repository; in 11b that same type owns the `Tx` and `commit` becomes a literal `COMMIT`.
- **`Step::run` signature changed** from `(&mut StepContribution, &mut ExecutionContext)` to `(&mut ExecutionContext, &mut dyn StepCommit)`. The running total moved out of `Job::run` entirely — the committer folds it into the `StepExecution` as it goes. Breaking, and free pre-0.1.0.
- **New observable: the commit count.** `RecordingCommit` counts commit points, so a test can assert the transaction *boundary* rather than only the totals — `chunk_size` 2 over 6 items commits three times, not once.
- **Ordering:** `reader.update(context)` precedes `commit.commit(..)`. Reversed, the bookmark lags the data by one chunk and a restart re-processes it. Mutation-tested: fails 4 tests.

**11b/11c — Shape 1, `Tx` as a type parameter `[DECIDED 2026-07-29]`.** The chunk loop must begin → write → update metadata → commit, but it is reached through `dyn Step`, and a `dyn` method can be neither generic over `Tx` nor able to name `R::Tx`. So `Tx` either becomes a type parameter on `Step`/`Job` or must be erased before crossing the `dyn` boundary (`&mut dyn Any` + a downcast in each backend writer — viable, since `Pool::begin` yields `Transaction<'static, DB>` and `Any` needs `'static`). **Chosen: the type parameter.** A backend mismatch — a Postgres writer wired to a Redis metadata store — is then a compile error rather than a runtime failure on the first chunk, and this framework's whole claim is a guarantee. Cost: `Job<Tx>` is viral, mitigated by `Tx = ()` defaults throughout, so non-enlisting jobs are still spelled `Job`. Pinned by a `compile_fail` doctest on `JobLauncher::run` (a `Job<String>` against `InMemoryJobRepository`, whose `Tx = ()`).
- **`Unmanaged<W>` is now load-bearing at every call site**, which is the intended ergonomics: writing outside the transaction should be visible, not inferred. ADR-007's blanket-impl wall is why it is a newtype.
- **`ChunkStep` gained an inherent `name()`.** With `Step<Tx>` generic, `step.name()` no longer infers which impl to use; an inherent method takes precedence and resolves it.
- **`RepositoryCommit::commit` folds into a *copy*** and swaps it in only after `commit(tx)` succeeds, so in-memory counters can never claim work that rolled back.

**The blind spot ADR-007 predicted, observed directly.** Reversing those last two lines — assigning the in-memory state *before* committing — initially failed **zero** tests, because `InMemoryJobRepository::commit` cannot fail and so satisfies any ordering. Closed with a `CommitFails` fake whose `commit` always errors; the ordering is our own logic, not the backend's, so a fake is a legitimate test for it. What still cannot be tested here is anything requiring a real rollback — that waits for 11d.

**11d/11e — the Postgres backend (2026-07-29).** New member crate `batchflow-postgres` (sqlx 0.9, opt-in per Architecture §4 — the heavy dep stays out of core and out of the facade).
- **Schema:** three tables joined by FK, the shape `InMemoryJobRepository` was deliberately built to. `UNIQUE (job_name, parameters)` on `job_instance` with `parameters JSONB` — FR-4.2's identity enforced by the *database*, which is what finally closes debt (2): two schedulers can no longer both win the check-then-act race, because `find_or_create_instance` is a single `INSERT .. ON CONFLICT .. DO UPDATE .. RETURNING`. `JobParameter`/`JobParameters` gained `Serialize`/`Deserialize` (newtype over `BTreeMap` serializes transparently and in key order, so the JSON *is* a stable identity key).
- **`abandon_execution` is one statement** with `FOR UPDATE` in a CTE, returning both the locked status and the updated id — so "unknown id" and "was Completed" are distinguishable without a second round trip to race against.
- **Counters restore through a `StepContribution`**, since `StepExecution`'s are private and fold-only. Slightly indirect, but it needed no new core API; revisit if a second backend finds it awkward.
- **Embedded migrations + `query!` macros (chosen 2026-07-29).** Consequence: `.sqlx/` is a committed build artifact, regenerated with `cargo sqlx prepare --workspace` whenever a query changes. Verified that with `.sqlx` present and no `DATABASE_URL`, sqlx uses the cache automatically — contributors need neither Docker nor `SQLX_OFFLINE` to build. `sqlx::query!` only covers *our* schema; a user's own writer uses unchecked `sqlx::query`.

**FR-2.4 is finally a guarantee rather than a claim.** `a_failed_chunk_rolls_back_its_rows_and_its_counters` inserts a chunk's rows *and then* fails inside the same transaction, and asserts the rows, the counters and the bookmark all vanish together; `a_restart_inserts_each_row_exactly_once` then restarts and asserts every row lands exactly once. Mutation: committing the failed chunk instead of rolling back fails both.

**Honest gap: the explicit rollback arm is not what the Postgres tests prove.** Deleting `commit.rollback(tx)` entirely leaves all 7 integration tests passing, because sqlx's `Transaction::drop` queues a rollback. The explicit call is still correct — `Tx` is generic, so no `Drop` behaviour is guaranteed for other backends, and sqlx's is deferred to the connection's next use and swallows its own errors — but that reasoning is *not* verified here. What is verified: core asserts `rollback` is called (`RecordingCommit.rollbacks`), and Postgres asserts the failed chunk leaves nothing behind. An earlier version of that code comment claimed dropping the tx would "leak an open transaction"; that was false and has been corrected.

**Bug 11a fixed, found while scoping this phase.** Counters and bookmark were persisted only *after* `step.run` returned, so a process `SIGKILL`ed mid-step lost everything it had committed. Probing the store during a step gave `(read_count: 0, bookmark: None)` after a chunk had fully committed. Phase 9's restart therefore only ever worked for a step that **returned an error** — not for a crash, which is what US-2 actually describes. All five Phase 9 restart tests injected a writer error and none of them noticed. Pinned by `committed_work_is_durable_before_the_step_finishes`, which asserts against the repository *from inside* the second chunk's write.

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
Phase 0 ◐ docs (tech-eval open items) · 1 ☑ workspace · 2 ☑ traits · 3 ☑ Job + typestate `JobBuilder` · 4 ◐ step model (tasklet trait pending) · 5 ☑ engine · 6 ◐ chunk processing (property tests/tuning guide pending) · 7 ☑ JobRepository · 8 ☑ ExecutionContext · 9 ☑ restart · 10 ☐ retry · **11 ☑ transactions**.
`batchflow-core` modules: `chunk`/`context`/`error`/`execution`/`item`/`job`/`launcher`/`memory`/`repository`/`step` + `#[cfg(test)] testing`, with `lib.rs` as a pure re-export surface (private `mod` + flat `pub use`, so module layout stays refactorable). **74 core unit tests + 3 doctests + 7 Postgres integration tests green**, clippy `--all-targets -D warnings` clean, `cargo fmt --check` clean.

**US-2 now holds end to end:** a job that dies mid-step is relaunched against the same `JobInstance`, skips the steps that completed, opens its reader at the last committed chunk, and writes no item twice.

A job now runs end to end through metadata: `JobLauncher::run` resolves a `JobInstance` from `(job_name, JobParameters)`, refuses a completed one (FR-4.4), opens a `JobExecution`, and `Job::run` persists a counted `StepExecution` per step — all reloadable from the repository alone.

**API hardening (2026-07-27):** `BatchError::Process` added (processors could not previously report failure); `chunk_size` is `NonZeroUsize` everywhere, so the silent `chunk_size == 0` no-op — a job reporting success having processed nothing — is unrepresentable; **ADR-002a**: the framework is `Send` end-to-end (`Step: Send` supertrait + RPITIT `+ Send` on the three core traits), so `tokio::spawn(job.run())` compiles. `job_run_future_is_send` and `launcher_run_future_is_send` lock that in.

**Debt closed in Phase 7:** ~~`filter_count` derived as `read - written`~~ — now counted at the processor's `None` arm, so the underflow panic is gone. ~~`Step` has no name/identity~~ — `Step::name()` plus a persisted `StepExecution` per step.

**Known debt, deliberately deferred:**
1. ~~`Starting`/`Started` still passes the FR-4.4 gate.~~ **Closed in 9a** — rejected, with `abandon_execution` shipped in the same change.
2. ~~`JobLauncher::run` resolves the instance and reads its last execution under separate lock acquisitions.~~ **Closed for Postgres in 11d** — `UNIQUE (job_name, parameters)` plus a single `INSERT .. ON CONFLICT` makes instance identity the database's job. The launcher's *gate* (read last execution, then create) is still two statements outside a transaction; two processes racing an instance with no prior execution can still both launch. Narrow, and it needs a `SELECT .. FOR UPDATE` around the gate to close — deferred, and recorded here rather than claimed fixed.
3. A failing `update_execution` masks the job's original error (`launcher.rs`). Real systems log the cause before propagating — Phase 13.
4. Library-craft gap: no `#![warn(missing_docs)]`, `Job`/`ChunkStep` lack `Debug` (API guideline C-DEBUG).
   ~~Implementing `Step` requires the caller to depend on `async-trait` directly.~~ **Closed in 9a** by `pub use async_trait::async_trait;` in `lib.rs`. Worth recording *how* it was found: the `Job::builder` doctest could not be written without it. Doctests compile as an external crate — they see the crate plus its **dev-dependencies**, not its normal dependencies — so they are the only tests that stand where a user stands. Unit tests structurally cannot feel this class of bug.
5. `read_chunk`/`process_chunk`/`run_step` are `pub` without a deliberate SemVer decision — and `run_step`'s signature changed in 7c-2, which is exactly the kind of break that decision governs. Settle it before 0.1.0.
6. **Doc-comment tests have no build-time protection against deletion.** The `Job::builder` `compile_fail` block has now been stripped along with its surrounding rustdoc **twice** (most recently by commit `9659a37`, which also deleted `Job::run`'s docs and left `Plan.md` claiming doctests existed when the count was 0). Nothing fails when it vanishes. Mitigation for now: the block says in its own text that it is a test, and the `Doc-tests` count in `cargo test` output is part of the quality gate below.

**Next milestone: Phase 10 — retry/skip via a `Classifier`.** FR-2.4 and FR-5 are now guarantees enforced by a real database, so the remaining fault-tolerance gap is *within* a chunk: today any error fails the whole step, and one poison row costs a full restart cycle. Phase 10 adds a `Classifier` mapping errors to {Retryable, Skippable, Fatal}, retry via `backon`, and chunk-scanning to isolate the poison item (FR-6.4). Note the new interaction Phase 11 creates: a retry must re-open the transaction, not reuse the rolled-back one.
