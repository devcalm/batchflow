# BatchFlow — Implementation Plan

> Status: **Living roadmap.** Phased. Each phase: goals · learning objectives · tasks · rationale · acceptance · testing · docs.
> Mentoring cadence per phase: **concept → Rust lens → minimal sketch → you implement → I review → improve.**
> Last updated: 2026-07-30

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

## Phase 10 — Retry & Skip ☑ (10d closed 2026-08-05)
> **10a ☑** `Classifier` / `ErrorAction{Retry,Skip,Fail}` / `FailFast`; `BatchError`'s wrapping variants carry a `Cause` instead of a `String`.
> **10b ☑** retry. Processing split from writing; the retry loop wraps `begin → write → commit`; `backon` supplies the backoff schedule; `ChunkStep::with_fault_tolerance` exposes it.
> **10c ☑** skip. `skip_limit` + `ItemDisposition`; `skip_count` on `StepContribution`/`StepExecution`, persisted by migration `0002_skip_count.sql`.
> **10e ☑** `PostgresClassifier` (SQLSTATE) + whole jobs through retry and skip against a real Postgres.
> **10d ☑** chunk-scanning to isolate a poison item on write failure (FR-6.4). Decided *for*, opt-in, off by default. See below.

- **Goals:** retry classified transient errors with backoff; tolerate classified bad items up to a limit.
- **Acceptance:** transient errors retried, bad items skipped and counted, fatal errors fail fast. **Testing:** injected failures in core; real SQLSTATEs and whole jobs in `batchflow-postgres`.

**10a — a classifier is only as good as what the error carries.** FR-6.3 replaces Spring's exception-class
hierarchy (`.retry(DeadlockLoserDataAccessException.class)`) with a function: `Classifier::classify(&self, &BatchError) -> ErrorAction`.
Scoping it exposed a blocker of our own making — `batchflow-postgres` mapped every `sqlx::Error` through
`BatchError::Repository(error.to_string())`, so a deadlock (retryable), a `NOT NULL` violation (skippable) and a
refused connection (fatal) all arrived as indistinguishable prose. **A `String` error payload is a tombstone: it
proves an error happened and destroys what you need to decide about it.** The four wrapping variants now hold
`pub type Cause = Box<dyn Error + Send + Sync + 'static>` with `#[source]`.
- **Constructors, not raw variants.** `BatchError::read/write/process/repository(impl Into<Cause>)` — because Rust
  never applies `Into` implicitly at an argument position, so making the variants take a box would otherwise have
  broken every `format!(..)` call site. The `impl Into` constructor is also what lets `db()` pass the live
  `sqlx::Error` through rather than stringifying it.
- **`PoisonError<MutexGuard<'_, T>>` cannot be a `Cause`** — it *contains* the guard, so it is neither `Send` nor
  `'static`. `InMemoryJobRepository` keeps `.to_string()` there, and that is now correct rather than lazy: nothing
  about "a thread panicked holding this mutex" is classifiable. The `Send + Sync + 'static` bound rejected the
  category at compile time without anyone reasoning about it.
- **`classify` takes `&self`.** A classifier whose verdict depends on call history is untestable and unshareable.
- **The default is `Fail`.** Fault tolerance is opt-in per step, as in Spring before `faultTolerant()`.

**10b — ownership decides where the retry boundary goes.** Read the two signatures:
`process(&mut self, item: Self::In)` **consumes**; `write(&mut self, tx, items: &[Self::Item])` **borrows**.
So a chunk can be re-written as often as you like and cannot be re-processed at all — the inputs were moved into
`process` on the first attempt. Retrying the processor would need `P::In: Clone` threaded virally through
`ChunkStep`, `Step` and `Job` to buy retryability for a case that barely exists; every transient error we care
about (deadlock, serialization failure, connection blip) is on the write side. **Spring re-runs the processor on
retry and therefore documents "your processor must be idempotent" as a user obligation. Ours does not need that
sentence, because ownership made the alternative unwritable.**
- Consequence: `process_chunk` lost its writer and its `tx` (three type parameters down to one) and now runs
  *outside* the transaction. Independent win: a processor doing a 200 ms enrichment call per item no longer holds
  row locks for 200 seconds a chunk.
- **The transaction rule Phase 11 created is enforced by the type system.** `StepCommit::rollback(tx)` and
  `commit(tx)` take `Tx` **by value**, so hoisting `begin` out of the retry loop is `E0382: borrow of moved value`.
  A retry *cannot* reuse a rolled-back transaction. (In Java `tx.rollback()` leaves the variable in scope and still
  callable, which is why Spring documents what Rust rejects.) Without this, Postgres would answer every statement
  on the aborted transaction with `25P02` and the retry would report a failure about transaction state rather than
  the original deadlock.
- **What is *not* type-enforced: rollback before backing off.** Dropping the transaction instead compiles fine.
  Sleeping on an open transaction holds its row locks and its pooled connection for the whole delay — backoff
  amplifying the contention it exists to relieve. Caught only by asserting the *sequence* `Begin, Rollback, Begin,
  Commit`; counters cannot distinguish "rolled back then re-opened" from "opened twice".
- **A failed commit retries too, with nothing to roll back.** `commit` consumed the `tx`. Postgres raises `40001`
  *at* `COMMIT`, so this path is real, and the signature already said what the shape had to be.
- **`backon` supplies the schedule, not the retry.** Its combinator wants a re-callable future factory; our loop
  body needs `&mut` writer, `&mut` committer and a freshly *owned* `Tx` per attempt. We take
  `BackoffBuilder → Iterator<Item = Duration>` and keep the control flow.
- **The schedule is deliberately unbounded** (`with_max_times(usize::MAX)`), because `should_retry` is the single
  authority on how many attempts there are. Both could encode "3 attempts", but they fail *differently*: exceeding
  `should_retry` stops the retry (loud, correct); exhausting the iterator skips the *sleep* and keeps retrying
  (silent hot loop). Two encodings of one rule, where disagreement degrades quietly, is worse than one.
- **`tokio` (feature `time`) is now a real dependency of core.** There is no runtime-agnostic async sleep, and
  `std::thread::sleep` in an `async fn` blocks the whole worker thread — stalling every unrelated task on it,
  including other steps of a spawned job. NFR-4's concern is tokio in core *traits*; a sleep is not a trait.

**10c — skip granularity is decided by trait signatures too.** `read` and `process` are per-item, so the failing
item is known and can be dropped. `write(&[Item])` names a *chunk*: a write error says N items failed, not which.
Skipping there would discard 1000 rows because one was bad — that is not skip, it is data loss. So FR-6.2 covers
read and process, and write-level skip is exactly what FR-6.4 is for.
- **`disposition(error: BatchError, skipped) -> ItemDisposition` takes the error by value.** It is either dropped
  with its item or handed back inside `Fail`. There is no third path where a caller consults the classifier and
  forgets the limit — which a `should_skip(&error) -> bool` would have made a one-line oversight. (`should_retry`
  is the older, weaker shape: two `&&` conditions a caller could get half-right.)
- **`SkipLimitExceeded { limit, #[source] cause }`** rather than the bare item error: one bad row is a
  data-quality nit, five hundred means the input is wrong and re-running will not help. This is the *second*
  domain error the debt list said to generalise from — and it argues the opposite. A status-transition rule
  (`CannotAbandon`) and a tolerance breach share no fields and no handling. They stay separate.
- **A skipped read must not consume a chunk slot** (`while chunk.len() < chunk_size`, not `for _ in 0..`), or
  dirty input silently shrinks the transaction the commit interval promised.
- **The limit is step-wide**, not per chunk: one bad row in each of a thousand chunks is a broken input file, and
  a per-chunk counter would call it healthy. It is *not* rolled back with a failed chunk — the items really were
  seen — and a restart is a new step execution with a fresh count.
- **The step-wide limit is also what bounds the read loop.** A reader that errors without advancing past the bad
  item would be handed it forever; each spin costs a skip, so the limit turns an infinite loop into a step failure.
  Readers still owe the advance, and the test double documents why.

**10e — the classifier that makes it usable, and the first end-to-end proof.** `PostgresClassifier` lives in
`batchflow-postgres`; core never learns a SQLSTATE.
- **Retry is enumerated** (`40001`, `40P01`, `55P03`), *not* matched on class `40`, because that class also holds
  `40003 statement_completion_unknown` — the database saying it does not know whether the statement took effect.
  Retrying that may double-write. A tidier prefix match would have silently made it retryable.
- **Skip is matched by class** (`22` data exception, `23` integrity constraint violation). The SQL standard drew
  that boundary deliberately and it is exactly the one we need: both classes say *this row is wrong*, never *the
  system is unwell*. Enumerating codes would be an incomplete copy of the standard's own grouping.
- **Everything else fails**, including connection failures (class 08) — US-3 asks for fast failure on
  infrastructure errors, and retrying a dead database only spends the budget before failing anyway.
- **`sqlstate()` walks the whole `Error::source()` chain** rather than matching `BatchError` variants. Matching
  sees only the outermost error, so `SkipLimitExceeded → Write → sqlx::Error` would classify as `Fail`, as would
  a user's writer wrapping `sqlx::Error` in its own type. It also sidesteps needing a wildcard arm for
  `#[non_exhaustive]`.
- **Classifier tests must provoke real SQLSTATEs.** A hand-built `sqlx::Error` proves the match arms exist, not
  that the codes are the ones Postgres sends. `SELECT ... FOR UPDATE NOWAIT` against a row another connection
  holds gives a deterministic `55P03`; a genuine `40P01` would need two transactions racing in opposite orders,
  i.e. a race inside the test suite.

**Mutation testing rewrote one of the end-to-end tests, and that is the transferable lesson.** The retry test's
writer originally probed the contended lock *before* inserting, so the doomed transaction held no rows and there
was nothing for a rollback to undo — the test looked right, asserted the right thing, and was structurally
incapable of observing it. Reordering to insert-then-fail makes `items == [1,2,3,4]` load-bearing: a retry that
kept the failed transaction's work yields `[1,1,2,2,3,4]`. Verified by mutating `run_step` to *commit* the failed
chunk before retrying. **The order of operations inside a test double decides whether a test is real or
decorative, and only mutation testing tells you which one you wrote.**

**10d closed 2026-08-05: chunk-scanning inherited, opt-in, off by default.** `FaultTolerance::scan_on_write_failure(true)`. On a write failure the classifier calls skippable, the chunk is re-written one item at a time to find the poison row, and the survivors are committed.

**The two-pass shape is forced by `ItemReader`, not chosen.** Each item is written alone in a throwaway transaction that is **always rolled back**, and only then are the survivors written once through the ordinary commit point. Committing item by item would be cheaper and wrong: `ItemReader::update` reports the reader's position, which after a chunk is read sits *past the whole chunk*, so there is no way to express "bookmark after item three". A crash midway through per-item commits would leave the bookmark at the previous chunk boundary with some items already durable, and the restart would write them again — breaking the exactly-once property the framework exists for. Paying `N + 1` transactions on the failure path is the cheaper side of that trade. Same lesson as 10b-1 and 10c-1: **read the trait signature to find where the policy boundary must go.**

**The non-idempotent-writer question, answered rather than dodged.** For a `TransactionalWriter` the identifying pass rolls back and is invisible. For an `Unmanaged` writer it really delivers: a 1000-item chunk with one bad row sends roughly 2000 items. **No promise is broken** — `Unmanaged` is already an explicit acceptance of at-least-once — but the amplification is severe enough that scanning is opt-in and the rustdoc says so where the person switching it on will read it. That is why the default is `false`.

**Scanning triggers on a failed *write*, never a failed *commit*.** A commit error names the transaction, not a row; there is nothing in it to isolate. `should_scan` is deliberately not consulted on that path.

**A `scanned` flag bounds the loop.** The survivors each wrote cleanly alone, so if the batch still fails the failure is real (a unique constraint spanning two rows in one chunk behaves exactly this way) and rescanning would find nothing to skip. **Mutation-verified, and the signature is worth remembering: removing the guard makes the test *hang*, not fail** — it compiles clean, prints `running 1 test`, and dies to SIGALRM. A missing loop bound does not announce itself as a failure. (`timeout` does not exist on macOS; `perl -e 'alarm shift; exec @ARGV'` is the portable substitute, and an earlier run that appeared to confirm the hang had in fact exited 127, command-not-found — absence of output is not evidence.)

Two other mutations, both caught: committing instead of rolling back in the identifying pass fails 3 tests including `the_identifying_pass_commits_nothing`, and ignoring the opt-in flag fails `scanning_is_off_by_default`. New metric `batchflow_chunk_scans_total`, and skips from this phase are labelled `phase="write"` alongside the existing `read` and `process`.

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
- **Embedded migrations + `query!` macros (chosen 2026-07-29).** Consequence: `.sqlx/` is a committed build artifact, regenerated with `cargo sqlx prepare` **run from inside `crates/batchflow-postgres`, not `--workspace` from the root** — see the Phase 18 note on why the location is load-bearing. Verified that with `.sqlx` present and no `DATABASE_URL`, sqlx uses the cache automatically — contributors need neither Docker nor `SQLX_OFFLINE` to build. `sqlx::query!` only covers *our* schema; a user's own writer uses unchecked `sqlx::query`.

**FR-2.4 is finally a guarantee rather than a claim.** `a_failed_chunk_rolls_back_its_rows_and_its_counters` inserts a chunk's rows *and then* fails inside the same transaction, and asserts the rows, the counters and the bookmark all vanish together; `a_restart_inserts_each_row_exactly_once` then restarts and asserts every row lands exactly once. Mutation: committing the failed chunk instead of rolling back fails both.

**Honest gap: the explicit rollback arm is not what the Postgres tests prove.** Deleting `commit.rollback(tx)` entirely leaves all 7 integration tests passing, because sqlx's `Transaction::drop` queues a rollback. The explicit call is still correct — `Tx` is generic, so no `Drop` behaviour is guaranteed for other backends, and sqlx's is deferred to the connection's next use and swallows its own errors — but that reasoning is *not* verified here. What is verified: core asserts `rollback` is called (`RecordingCommit.rollbacks`), and Postgres asserts the failed chunk leaves nothing behind. An earlier version of that code comment claimed dropping the tx would "leak an open transaction"; that was false and has been corrected.

**Bug 11a fixed, found while scoping this phase.** Counters and bookmark were persisted only *after* `step.run` returned, so a process `SIGKILL`ed mid-step lost everything it had committed. Probing the store during a step gave `(read_count: 0, bookmark: None)` after a chunk had fully committed. Phase 9's restart therefore only ever worked for a step that **returned an error** — not for a crash, which is what US-2 actually describes. All five Phase 9 restart tests injected a writer error and none of them noticed. Pinned by `committed_work_is_durable_before_the_step_finishes`, which asserts against the repository *from inside* the second chunk's write.

## Phase 12 — Metrics ☑
> **12a ☑** the vocabulary. `batchflow_core::metrics`: metric-name and label-key constants plus `describe()`.
> **12b ☑** the chunk loop. `ChunkMetrics` handles hoisted per step; item counters emitted after the commit, retries as they happen.
> **12c ☑** the lifecycle. `jobs_started/finished`, `steps_started/finished`, `step_duration` in `launcher.rs`/`job.rs`.
> **12d ☑** `batchflow-metrics`: Prometheus exporter, bucketed histograms, `install()`.

- **Goals:** `metrics` facade + Prometheus; counters/histograms per FR-8.1.
- **Acceptance:** ☑ a scrape shows items, retries, skips-by-phase and durations. **Testing:** ☑ metric assertions against a `DebuggingRecorder` and a rendered Prometheus scrape. **Docs:** ☐ dashboards note.

**The `metrics` facade is a dependency of `batchflow-core`, not of a side crate.** The emit points are inside the private chunk loop, so no external crate can reach them; a listener trait would have invented a public callback API whose only consumer is metrics. `cargo tree` settled the NFR-3 question with evidence: `metrics` costs core exactly one runtime dependency (`rapidhash`) — lighter than `thiserror`. `batchflow-metrics` still earns its place, holding the exporter.

**Two rules decided the whole design.** (1) *Labels are bounded dimensions; ids are not labels.* `job`/`step`/`status`/`phase` are author-written and finite; a `JobExecutionId` label would mint one time series per run, written once and kept forever. Correlating a specific run is tracing's job (Phase 13), where high cardinality is the point. (2) *Emit after the commit.* A counter incremented before a rollback can never be decremented — counters are monotonic. The chunk loop's control flow enforces this for free: the retry loop leaves only by `break` (committed) or `return Err`, so everything after it describes committed work.

**Retries are the deliberate exception to rule 2**, counted as they happen rather than post-commit. Rule 2 governs counts that must reconcile with committed data; a retry is an event, not a row. Counting them only on chunks that eventually commit would report zero for exactly the chunk an operator is paged about.

**The reconciliation property:** `sum(batchflow_items_written_total)` equals `SELECT sum(write_count)` for the same runs, because both fold at the same commit point. A metric that disagrees with the metadata store is worse than no metric.

**Hoisting the metric handles turned out to be a correctness fix, not just a performance one.** Label values must be owned (`&str` gives `E0521`), so resolving handles per chunk would allocate per commit interval. Building them once per step also *registers* every series at step start — so a counter that has not happened yet reads `0` instead of being absent. That closes the trap where a job which has never failed has no `..._failed_total` series at all, and a success-rate panel silently shows nothing for the healthiest jobs. Found by a test asserting `None` and getting `Some(0)`.

**`jobs_finished_total{status}` is one metric with a label, not two metric names.** The rule: *use a label when summing across its values is meaningful.* `completed + failed = total runs` — meaningful, so label. `read + written` counts the same item twice — meaningless, so separate names. It also means a new `BatchStatus` variant needs no code change, only a new label value.

**Placement is load-bearing in two places, and both are mutation-tested.** `steps_started` sits *below* the restart `continue`, so a step skipped on restart is not counted as started. `jobs_started` sits *after* the FR-4.4 gate, so a rejected relaunch does not inflate `started - finished` — the in-flight count — by one forever.

**12d: no HTTP server.** `metrics-exporter-prometheus` with `default-features = false` drops `http-listener` and `push-gateway`, and with them hyper, rustls and tokio-net: **168 → 60** dependency-tree entries. `render()` returns a `String`; routing it is the application's job. Note `default-features = false` cannot be set in a member crate when the dependency comes from `[workspace.dependencies]` — it must be declared at the root, as `sqlx` already is.

**Explicit histogram buckets, not the default summaries.** Without `set_buckets_for_metric` the exporter renders a histogram as a summary, whose quantiles are computed per process and cannot be aggregated — "p99 chunk latency across four workers" becomes unanswerable. Buckets are summable.

**`describe()` runs after `install_recorder()`.** Descriptions are stored *in* the recorder, so describing first writes them nowhere and the scrape ships with no `# HELP`. Both this and the bucket decision are pinned by mutation.

**Writing the operator-facing help text first was a design step, not documentation.** `describe()`'s string for `steps_started_total` said "a step skipped on restart is not counted" in 12a, before any emit existed; 12c's job was to make the code true. Writing `items_read_total`'s help forced reading `chunk.rs` closely enough to notice that `read` is measured after the skip loop but before processing — something no reader could infer from the name.

**Recorders are global, so tests never install one.** `metrics::with_local_recorder` scopes a recorder to a closure via a thread-local; a test that called `set_global_recorder` would make every other test in the binary order-dependent, and the second one would simply fail. Same discipline at the exporter level: `builder().build_recorder()`, never `install()`.

## Phase 12.5 — CI ☑ (unplanned; added 2026-08-03)
- **Goals:** make the cross-cutting quality gate below executable by a machine that is not the author's laptop.
- **Tasks:** ☑ `.github/workflows/ci.yml` — `test` / `lint` / `msrv` (matrix) / `latest-deps` (scheduled).
- **Acceptance:** ☑ every promise in the table below is checked by a job. **Testing:** the workflow itself; two jobs went red on their first run and both were real.

**A gate you run by hand is a gate you run with the wrong flags.** Three of the four promises this repo makes were unchecked, and running them properly found two live bugs at HEAD.

- **`cargo doc` was never a gate.** `rustdoc::broken_intra_doc_links` is warn-by-default, so `cargo doc` exits **0** on exactly the failure the gate was written to catch. `step.rs:7` had an unresolved `[`StepExecution`]` link at HEAD — `step.rs` never brings the type into scope, and docs.rs would have shipped `StepContribution`'s first line as literal `[StepExecution]` text. Fixed with a reference-style path link (`[`StepExecution`]: crate::StepExecution`), which keeps the prose reading naturally; a `use` for docs alone would trip `unused_imports`. **The gate is `RUSTDOCFLAGS: -D warnings`, not `cargo doc`.**
- **`rust-version = "1.85"` was false in two independent ways.** (1) `sqlx 0.9` requires 1.94, so `batchflow-postgres` could never have built there. (2) `job.rs:180` used a **let-chain**, stable only since 1.88 — so even `batchflow-core` was lying. Nothing detected either, because the claim had no test.

**MSRV belongs to a crate, not to a repository.** `[workspace.package]` is right for `version`/`license`/`authors` — facts about the project — and wrong for `rust-version`, which is a fact about *one crate's dependency graph*. Users take a crate, not a workspace: someone on 1.85 can have `batchflow` + the in-memory store perfectly well, and inheriting 1.94 across the board would deny them that for a Postgres backend they never asked for. So `batchflow-postgres` overrides to `rust-version = "1.94"` and the `msrv` job is a two-lane matrix, each lane checking exactly the crates whose version it names.
- **The let-chain was rewritten, not the MSRV bumped.** `previous.as_ref().is_some_and(|p| p.status() == Completed)` is stable since 1.70 and is *better* code — a predicate instead of a binding nobody uses. When sugar costs three minor versions of compatibility and buys nothing, drop the sugar. Had it cost readability, raising `rust-version` to 1.88 would have been the honest answer instead: **an MSRV describes reality, it is not an aspiration to contort code toward.**
- **The MSRV job is `check`, not `test`, and has no `--all-targets`.** `rust-version` promises the *library* compiles. `testcontainers` and `tokio`'s `test-util` carry their own MSRVs this project promises nothing about, and letting a dev-dependency's bump turn the MSRV job red would train everyone to ignore it.

**The Postgres suite needs no CI configuration, and that is the payoff of 11e.** The tests call `PostgresImage::default().start()` themselves and GitHub's runners ship Docker, so there is no `services:` block. More importantly `DATABASE_URL` is left **unset on purpose**: were it set, `sqlx::query!` would validate against the live database instead of the committed `.sqlx/` cache, and a stale cache would pass CI while breaking every contributor who builds without Docker. Here the thing that makes the build reproducible is the *absence* of a variable.

**`--locked` everywhere, plus a scheduled job that deliberately drops it.** A library's `Cargo.lock` is ignored by everyone downstream; it pins only our own CI. With `--locked`, red always means *someone changed this repository* — but CI can then never tell us an upstream release broke us. `latest-deps` buys that signal back on a weekly cron with `continue-on-error`, where it cannot block an unrelated pull request. Two failure modes, separated, instead of one channel carrying both ambiguously.

**`RUSTFLAGS: -D warnings` at workflow level is not redundant with `clippy -- -D warnings`.** Cargo compiles registry dependencies with `--cap-lints allow`, so the env var denies warnings in *our* crates only — including in the `test` and `msrv` jobs, which never invoke clippy. The clippy flag denies clippy's own lints on top of that.

## Phase 13 — Tracing ☑
> **13a ☑** the identity seam. `StepCommit::job_name()` → `identity() -> StepIdentity<'_>` (job name, step name, `JobExecutionId`, `StepExecutionId`).
> **13b ☑** spans and events. `tracing` in core; `job`/`step` spans via `Instrument`; events for skips, retries and failed cleanup; capture harness + 7 tests, all mutation-verified.
> **13c ☑** the vocabulary — `batchflow_core::tracing`, and **ADR-010: no `batchflow-tracing` crate**.

- **Goals:** `tracing` spans per job/step; correlation IDs; OTel export.
- **Acceptance:** ☑ nested spans visible and asserted. **Testing:** ☑ `CaptureLayer` + 16 mutations, all caught. **Docs:** ☑ OTel setup in the facade rustdoc (ADR-010: no exporter crate).

**Metrics gave the aggregate, the metadata store gives the final state, and neither gives the sequence.** At 02:14 a `skip_count` of 400 cannot say whether the skips were one contiguous block (a corrupt input segment) or scattered evenly (a systematic parse bug), and those two have opposite remedies. Every question that page raises is a question about *ordering in time*, which is the one thing a counter structurally cannot hold. That is why Phase 12 deliberately kept the execution ids **out** of metric labels — one label value per run mints one time series per run — and why they belong here, where high cardinality is the point.

**13a — the identity seam, and a correction to why it exists.** `StepIdentity` replaces `job_name()` rather than joining it with three more getters: the four facts share one owner, one lifetime and one consumer set. `RepositoryCommit` already held every one of them, so the widening cost no plumbing — usually a sign the seam was cut in the right place.
- **The original justification was overstated and is corrected here.** "The chunk loop cannot learn this on its own" is false: `Job::run` holds all four directly, and once it wraps `step.run` in a span the chunk loop inherits them through the span tree. What `StepIdentity` actually buys is that the chunk loop is **self-describing rather than dependent on its caller having wrapped it** — and, more concretely, that a *third-party* `Step` implementation, which receives only `&mut ExecutionContext` and `&mut dyn StepCommit`, has some way to know which execution it is in. That is a public-API argument, not an engine one.
- **`ChunkConfig::new` deliberately still takes two `&str`.** Handing it the whole `StepIdentity` would walk `JobExecutionId` into the function that builds **metric labels**, one plausible-looking line away from unbounded time-series growth that shows up in the Prometheus bill weeks later, not in CI. This cuts against 10b-4's "when two parameters are one decision, pass the decision" — correctly, because that rule is about *coupled* parameters, not about handing a callee fields it must not touch. Same instinct as 9a's `abandon_execution` taking an id rather than a `&JobExecution`.

**13b — the async footgun that the type system does *not* catch.** Holding a `span.enter()` guard across an `.await` compiles. It is wrong: the guard pushes onto a *thread-local* stack, so at a yield the next task on that thread inherits the span, and on resume elsewhere the span is not entered at all. Verified by probe, and the result was the opposite of what we assumed going in:

| held across `.await` | compiles? |
|---|---|
| `std::sync::MutexGuard` | **no** — `future cannot be sent between threads safely` |
| `tracing::span::Entered` | **yes** — `Entered` is `Send` |

The `+ Send` discipline that mechanically forbids the mutex bug is silent here. `.instrument(span)` is the only correct form across yields. **The same trap has a second door in the tests:** `tracing::subscriber::with_default` takes a *synchronous* closure, so an async body stops seeing the subscriber after a yield — and that failure is silent, since a test capturing nothing still passes every assertion about absence. `WithSubscriber::with_subscriber` is the async form. Sync/async pairs: `enter`/`instrument`, `with_default`/`with_subscriber`.

**Chunk-level is events, not spans — this phase revises the plan's own promise.** 10M items at `chunk_size` 100 is 100,000 spans per run, and the question a chunk span mostly answers is already answered by `batchflow_chunk_duration` as an *aggregatable* histogram. The rule: **a span is for something you want to time and nest; an event is for something that happened.** A chunk that commits normally is not interesting; one that retried three times is. Volume is self-bounding — `skip_limit` is a hard cap on skip events, a property inherited free from 10c.

**Observability reached back and changed an API from 10c.** `disposition` takes the error **by value** and `ItemDisposition::Skip` carried nothing, so a skipped row's error was already dropped before anything could log it — the counter was the only survivor. `Skip(BatchError)` returns it while preserving the property 10c chose by-value for: there is still no path where a caller consults the classifier and forgets the limit. Expect this direction of pressure; it is why 12a's "write the operator-facing text first" works.

**The commit-retry path had no event, and the test is what found it.** Both `should_retry` sites look alike, but `Plan.md`'s own Phase 10 note records that **Postgres raises `40001` at `COMMIT`** — so the commit path, not the write path, is where a real serialization failure surfaces. It was the one site missed, which made the most common production retry the invisible one. Two distinct messages now, and `a_retried_commit_emits_its_own_event` asserts the *other* message is absent, so an operator grepping for one cannot silently match the other.

**Debt 3's remaining half is closed.** All three `with_cleanup` sites now emit at `ERROR` before wrapping. ADR-009 already put both failures in the returned value, so nothing was *lost* without a subscriber; what the event adds is the moment — by the time the error reaches a caller, "the metadata store is now stale" has stopped being actionable at the site that knew it. Note the earlier claim that this *needed* a logger stays wrong: the return value is the primary channel, the event is the timestamp.

**Every emit point is mutation-verified — 10 mutations, 10 caught**, nine failing exactly one test. The tenth (renaming the `job` span) fails two, which is genuine coupling: both tests assert on the span tree. Without this pass the whole phase would have been decorative, exactly as `Plan.md` warned after the 10e retry-writer episode. `CaptureLayer` is the tracing counterpart of `DebuggingRecorder`, and it lives under the same rule: **scoped per test, never `set_global_default`** — one per process makes every test order-dependent.
- **`Visit` needs all three `record_*` arms.** tracing dispatches by *type*: `phase = "read"` lands in `record_str`, `skipped = 1usize` in `record_u64`, `error = %error` in `record_debug`. A missing arm drops that field **silently** rather than failing to compile — so the harness itself needed the same skepticism as the code.

**Dependency cost, measured not asserted:** core's runtime tree is **24 crates**; `tracing` added 3 (`tracing`, `tracing-core`, and build-time-only `tracing-attributes`), and `Cargo.lock` gained **zero** new package entries because sqlx and testcontainers already pulled them. `tracing-subscriber` is dev-only with `default-features = false` — and per the note the root file already carries, that flag must be declared in `[workspace.dependencies]`, never in the member.

**13c — the planned crate was not built, and the reasoning is ADR-010.** `Plan.md`'s Phase 0 promise of a
`batchflow-tracing` crate did not survive contact with the built system. Measured rather than estimated: the layer
alone is **39 crates**, adding `opentelemetry-otlp` takes it to **99** (hyper, prost, tower, the http stack) against
core's own runtime tree of 24. 12d's answer applies unchanged — the application owns the pipeline. But once the
exporter is gone there is no content left: `batchflow-metrics` earns its place by holding bucket boundaries and
`describe()` ordering, whereas sampling — the only candidate here — was made moot by 13b rejecting chunk spans, so a
ten-step job emits eleven spans per run. And shipping it would *harm* users: `tracing-opentelemetry` 0.33 is
hard-paired to `opentelemetry` 0.32, so a user on 0.31 would get two semver-incompatible copies in one binary, hence
two global tracer providers, hence spans silently going nowhere while everything compiles. **Not every planned crate
should be built; the plan is a hypothesis, and this one was falsified.**

**What shipped instead is 12a's counterpart: the vocabulary.** `batchflow_core::tracing` holds `SPAN_JOB`/`SPAN_STEP`
and the nine `FIELD_*` keys, each with operator-facing rustdoc, plus the fifteen-line OTLP recipe in the *facade's*
docs — the crate whose dependency graph matches a user's.

**Span names can be constants; field names cannot, and the failure is silent.** Verified by probe in both directions:

| written | recorded |
|---|---|
| `info_span!(SPAN_STEP)` | span named `step` — the constant **is** evaluated ✅ |
| `warn!("phase" = "read")` | field `phase` ✅ |
| `warn!(FIELD_PHASE = "read")` | field **`FIELD_PHASE`** — compiles, constant never read ❌ |

`tracing` takes a field name as an *identifier*. So the engine uses `SPAN_*` at its emit sites and keeps field names
as literals, and the only thing binding those literals to the module is the tests — which is why 13b's assertions
were rewritten to go through the constants.

**Mutation testing found a hole that had been open since Phase 12.** Renaming a `SPAN_*` constant initially survived,
because the emit site and the assertion both use it and move together. Probing further: renaming `ITEMS_READ` — a
*published Prometheus metric name* — left the entire workspace suite green while every Grafana panel would have gone
blank. Closed in both modules with a test that pins the literal **values**. That looks tautological and is not: these
strings are a published contract, exactly like a wire format, and the test's job is to make changing one a deliberate
act with a reviewable diff. `the_phase_key_matches_its_metric_label` additionally pins `FIELD_PHASE == LABEL_PHASE`,
so an event and a counter cannot come to partition skips differently.

**Phase 13's own forward-looking notes are now spent** — the execution ids went into spans,
`StepCommit::job_name` generalised into `StepIdentity`, and debt 3's remaining half closed with the `ERROR` events
at all three `with_cleanup` sites. Nothing from the original Phase 13 sketch is outstanding.

## Phase 14 — Scheduling ☐ (`JobLauncher` already exists — this phase is the adapters only)
- **Goals:** launch API + adapters (tokio-cron-scheduler / cron / k8s). No home-grown engine (ADR-006).
- **Tasks:** `batchflow-scheduler` adapters. `JobLauncher` ☑ landed in 7c-1; a scheduler consumes it and branches on `BatchError::JobInstanceAlreadyComplete` to skip a run it has already done. **Acceptance:** external trigger runs a job. **Testing:** launcher unit. **Docs:** integration guide.

## Phase 15 — Storage Backends ☑ (2026-08-05)
- **Goals:** Redis backend; harden Postgres; backend conformance suite.
- **15a ☑ the conformance suite.** `batchflow-core::conformance` behind a `conformance` feature — **the crate's first feature**, which is also what made the Phase 18 decision to skip `[package.metadata.docs.rs]` expire exactly as predicted; `all-features = true` is now on `batchflow-core`, because a backend author is precisely who needs the suite documented. 32 cases, invoked by `job_repository_conformance!(setup())`, generating one `#[tokio::test]` each. Both backends run the identical list: `memory.rs`'s `conformance_suite` module and `batchflow-postgres/tests/conformance.rs`. **194 tests workspace-wide.**
- **15b ☑ `batchflow-redis`.** `RedisJobRepository`, MSRV 1.88 (redis 1.5), added to the CI MSRV matrix as a third lane and verified locally against a real 1.88 toolchain. **35 tests: the 32 shared cases plus 3 of its own.** `Tx = redis::Pipeline` in `MULTI`/`EXEC` mode — commands buffer client-side and are sent only at commit, so **a rolled-back chunk was never sent at all**, which is a stronger property than Postgres offers and a much narrower one (no read-your-writes inside the transaction, no conflict detection). Every check-then-act operation is a Lua script, because Redis has no unique constraint to lean on the way `find_or_create_instance` leans on one in Postgres; `concurrent_resolution_yields_exactly_one_instance` fires 16 concurrent resolutions at it.

**The conformance suite paid for itself immediately: all 32 cases passed on the first run against a backend written after them.** That is the argument for building the contract before the implementation — the target was executable, so there was no interpretation step between "what the trait promises" and "what this backend does". Mutation-verified here too: `LINDEX -1` → `LINDEX 0` in `last_execution` fails exactly 2 cases.

**A durability concern that had to be written into the crate docs rather than resolved.** The metadata store *is* the exactly-once guarantee — restart is emergent from what was recorded — so losing the last seconds of writes means a restarted job re-does or skips committed work. Redis's default RDB persistence can do exactly that. The crate therefore documents `appendonly yes` + `appendfsync always` as a **requirement, not a tuning note**, and says plainly that PostgreSQL is the recommendation. A backend whose correctness depends on operator configuration must say so where the operator will read it; there is no API change that can fix this.

**The drift was real, asymmetric, and worse than "the suites differ".** In-memory checked 21 contract properties; PostgreSQL checked 7. Fifteen properties — parameter-order identity, `update_execution` replacing rather than appending, every "rejects an unknown id" path — had **never run against a real database**. In the other direction `abandon_execution`, the safety-critical FR-4.4 escape hatch, had **no in-memory test at all**. Neither gap was visible from either file; only putting the two lists side by side showed it.

**Writing the suite is what forces the contract to be written down, and the interesting part is what had to be left out.** `Tx = ()` means `InMemoryJobRepository` cannot discard anything (ADR-007), so **rollback is not part of the `JobRepository` contract** — asserting it would assert a promise the trait does not make, and it stays a PostgreSQL-only test. Likewise **no case asserts an id value**: in-memory draws from one shared counter while PostgreSQL uses a sequence per table, so identical correct behaviour yields different numbers. The Phase 16 note predicting exactly this is now enforced by the suite rather than by memory.

**Mutation-verified, and the mutation was chosen to prove the suite adds coverage rather than merely runs.** Making PostgreSQL's `update_execution` ignore its `rows_affected` check fails **exactly one** case — `update_execution_rejects_an_unknown_execution` — which is one of the fifteen PostgreSQL had never tested. All 32 passed against PostgreSQL on the first run, so the pre-existing gap was in *testing*, not in behaviour; that is the good outcome, and worth stating plainly rather than implying the suite found bugs.

**Duplicates were deleted, not left alongside.** 21 tests removed from `memory.rs` and 7 from `batchflow-postgres/tests/repository.rs`, because two copies of a contract is the very thing that drifted. What stays in `repository.rs` is what is genuinely *not* the trait's promise: rollback, migration idempotency, the `skip_count` CHECK constraint, and the end-to-end restart. `memory.rs` keeps only `repository_is_send_and_sync` — auto-traits are SemVer-visible and lost silently, so they are asserted, not assumed.

**Structural note:** the suite could not live in `mod testing`, which is `#[cfg(test)]` and therefore invisible outside the crate — the same wall Phase 17's benches hit from the other side. A feature-gated `pub mod` is the way to ship test support to downstream crates; the module is `#[cfg(any(test, feature = "conformance"))]` so core's own tests need no feature flag.

## Phase 16 — Examples ☑ (2026-08-04)
- **Goals:** runnable end-to-end examples (CSV→Postgres, restart demo, retry/skip demo).
- **Delivered:** `hello_batch`, `restart_demo`, `retry_skip_demo` in `crates/batchflow/examples/`; `csv_to_postgres` (+ `people.csv`) in `crates/batchflow-postgres/examples/`; `crates/batchflow/tests/restart.rs`; `docs/Examples.md`. **149 tests green** across all four crates with Docker up — the first full verification since 12.5.

**Examples belong in the facade, not in `batchflow-core`.** Same structural argument as the doctest in `crates/batchflow/src/lib.rs`: rustdoc and cargo both pass `--extern` for a crate's own dependencies, so a core example can compile code no user can. The rule that falls out for facade dev-dependencies: **legitimate iff a real user would also declare it.** `tokio` qualifies (nothing runs an async framework without a runtime); `async-trait` does not, and that it does not is the entire point of re-exporting it. `csv_to_postgres` lives in `batchflow-postgres/examples/` with `batchflow` added as a *dev*-dependency there, so it imports `batchflow::batchflow_core::*` + `batchflow_postgres::*` — exactly an application's pair — without dragging sqlx, testcontainers and MSRV 1.94 into the facade's `--all-targets` build. No cycle: `batchflow` depends on `batchflow-core`, not on `batchflow-postgres`.

**CI covers examples, but only by compiling them.** Verified rather than assumed, by planting a `useless_format`: `cargo clippy --workspace --all-targets` fails on it, and the MSRV lane's `check` (no `--all-targets`, line 59 of `ci.yml`) does not. So no workflow change was needed — but "examples compile in CI" buys compilation, not execution, and the acceptance criterion as originally written read as coverage. **Where a property must be enforced rather than illustrated it belongs in `tests/`**, which is why `restart.rs` exists: the demo's `assert!` never runs in CI. Corollary found the same way: a facade-level integration test is *not* redundant with core's unit tests, because its dependency graph is one crate deep and it is therefore the only place a missing `pub use` goes red.

**Assert the exact output, not merely the absence of duplicates.** A reader resuming at the *wrong* offset produces a duplicate-free but incomplete sink; only the full expected vector catches it. Mutation-verified throughout: disabling `ItemReader::open` fails exactly 2 of `restart.rs`'s 3 tests (the status test correctly survives — a broken bookmark does not change what status was recorded), and moving `self.next += 1` after the corrupt-row check in `retry_skip_demo` makes row 4 be skipped five times, exhaust the limit and fail the step with `read=0`. **The skip limit is the only thing between a non-advancing reader and an infinite loop, which is a bad thing to be relying on** — a skipping reader must make progress, and the engine cannot enforce it because `read` is opaque.

**A retry/skip demo did not need Docker, contrary to the phase sketch.** `fault_tolerance.rs` already *proves* retry and skip against real SQLSTATEs; a demo's job is the wiring, which is backend-independent. Building `retry_skip_demo` in-memory around its own `FeedError` made it strictly more useful, since most users classify their own errors rather than Postgres's — and it demonstrates that **classifiers compose by delegation** (`LoaderClassifier` handles `MalformedRow` and defers everything else to `PostgresClassifier`, neither knowing about the other).

**API findings from standing where a user stands** — the real yield of this phase:
1. `InMemoryJobRepository` derives `Default` but has no `new()`, while `JobLauncher::new` sits beside it in the same expression.
2. `JobRepository` exposes only `last_execution(instance_id)`; **there is no way to list all executions of an instance**, so a failed attempt's row becomes unreachable once the next attempt exists (`restart.rs` has to snapshot between runs) and "what did this instance do across all attempts?" is unanswerable. Needs a paging decision before it can be added — flagged for 15/18.
3. In-memory ids come from one shared counter (instance 1, execution 2) while Postgres uses per-table sequences. Harmless — ids are opaque newtypes — but **Phase 15's conformance suite must not assert on id values.**
4. The typestate `JobBuilder` **does** work with a non-`()` `Tx` when the target type is known (`fn build_job(..) -> Job<PgTx>` is enough). Every existing Postgres call site uses `Job::<PgTx>::new` instead, which now looks like habit rather than necessity — worth revisiting.
5. `RetryPolicy` is `Copy`; clippy's `clone_on_copy` caught the `.clone()` calls.

**Two decisions still unstated, deliberately left open:** whether examples and `tests/` should be MSRV-checked (they currently are not, and it is defensible — neither constrains a downstream user's MSRV), and whether CI should execute examples as well as compile them.

## Phase 17 — Performance Optimization ☑ (2026-08-05)
- **Goals:** criterion benches; remove needless allocs/clones; validate P-1..P-5.
- **Delivered:** `crates/batchflow-core/benches/chunk_loop.rs` (criterion 0.7, `harness = false`), `crates/batchflow-core/tests/allocations.rs` (core's first integration test), `docs/Performance.md`. **Measured: ~3.9 ns/item + ~102 ns/chunk**; allocation count is a function of the chunk count only (66 vs 66 across a tenfold item difference). P-1, P-2, P-4 hold; P-3 is structural (the `write(&[Item])` signature, not a measurement); P-5 is the report.

**Sequenced before 18 deliberately: 17 is the last phase that can force a breaking API change.** It immediately did the opposite of what was expected. Reading `chunk.rs` first, the two per-chunk `Vec::with_capacity` calls looked like an obvious win — hoist and reuse the buffers, which means `process_chunk` stops consuming its input by value, i.e. a signature change made days before the API freeze. Measured, those allocations are **0.1 ns/item at `chunk_size` 1,000**: ~2.6% of a framework overhead that is itself under 2% of a real job. **The optimisation was a breaking change worth nothing, and only the benchmark could say so.** There is no outstanding optimisation work in the engine, and 17c is closed by saying that rather than inventing some.

**A benchmark only goes red if a human reads it** — the same weakness as `restart_demo`'s decorative `assert_eq!` (Phase 16). So P-4 is a *test*: a counting `#[global_allocator]` over two runs with the same chunk count and a tenfold item difference. Three traps, two of which make it pass while measuring nothing. (1) A global `AtomicUsize` counts the whole process, since the harness runs `#[test]`s on several threads and allocates for output capture; use `thread_local!` with `const { Cell::new(0) }`, which is `Drop`-free and therefore safe to touch *inside* the allocator. (2) **A thread-local counter plus a multi-threaded runtime measures ~0**, because the future runs on a worker thread with a different cell — the assertion then compares 0 to 0 forever. `Builder::new_current_thread()` is load-bearing, and `the_counter_observes_per_item_allocation` is the positive control that catches it, exactly as the `compile_fail` doctest needs one. (3) Never assert an absolute budget; `allocs < 100` is a magic number that drifts with every feature. Hold the chunk count fixed and multiply the items.

**Structural notes.** Benches and integration tests cannot use core's `testing` fakes — `mod testing` is `#[cfg(test)]` and both targets link the *normal* library build — so they duplicate them, the Phase 16 crate-root lesson arriving by a third door. The bench is only possible because the newtype ids expose `new(i64)` (there for third-party backends), which is what lets a `StepCommit` exist outside the crate. `#[global_allocator]` is per-binary and an integration test is its own binary, so the counter cannot perturb the other 118 tests, and `#![forbid(unsafe_code)]` at `lib.rs:4` still covers everything a user compiles. `cargo add criterion` **auto-downgraded 0.8.2 → 0.7.0 to respect `rust-version = "1.85"`** (0.8 needs 1.86) — the right outcome, since a dev-dependency must not cost users a compiler version; the MSRV lane never compiles benches anyway (`check` without `--all-targets`).

**Known gap, stated in the report rather than hidden:** the benchmark installs neither a metrics recorder nor a tracing subscriber, so the Phase 12/13 observability that sits *inside* the measured loop is configured away. These are a lower bound. Measuring the loop under a live recorder and subscriber is the obvious next benchmark and is not written.

## Phase 18 — Documentation ☑ (2026-08-05) — publishing itself is the only thing left
- **Goals:** complete rustdoc, book-style guide, API-guidelines pass, SemVer/CHANGELOG.
- **18a ◐ the API-guidelines pass** — the breaking-change window, which closes at 0.1.0.
  - ☑ `JobRepository::executions(instance_id) -> Vec<JobExecution>`, oldest first, closing the gap Phase 16 found. **Unpaged, deliberately:** an instance's executions are its retry attempts, which the domain bounds at a handful, and `step_executions` already returns an unbounded `Vec`; the query that would need paging is "all *instances* of a job", which this is not. Adding a method to a public trait is breaking for implementors and there is no honest default body for a storage query, so this had to land now. Cost demonstrated on ourselves: two test fakes stopped compiling.
  - ☑ `#[must_use]` on **constructors and consuming builders only** (24 items). Deliberately *not* on the 25 `&self` getters clippy's `must_use_candidate` also flags: dropping a getter result is obviously pointless and never a subtle bug, and marking them dilutes the signal on the ones where dropping silently discards configuration. **The lint is a starting point, not the authority — it missed `ChunkStep::with_fault_tolerance`, `JobBuilder::step` and `FaultTolerance::classifier`, the three where `x.with_fault_tolerance(f);` as a statement compiles and throws the config away.**
  - ☑ `InMemoryJobRepository::new()` (C-CTOR — it had `Default` but no `new`, while `JobLauncher::new` sits beside it in the same expression).
  - ☑ **`StepBuilder`: decided against, and the Phase 3 item is closed rather than deferred again.** `ChunkStep::new` takes five arguments and all five are *required*; a builder earns its keep when parameters are optional, and the one optional thing here already has `with_fault_tolerance`. A builder would add API surface, a second way to do the same thing, and a runtime failure mode (`build()` on an incomplete builder) in place of a compile-time one. Revisit only if a *required* sixth parameter appears.
  - ☑ **`[package.metadata.docs.rs]`: deliberately not added.** No crate has any features, so `all-features = true` would be a no-op. Revisit the moment a feature is introduced — that is when it starts mattering, and adding it now would read as though it were already doing something.
- **18b ☑ release metadata.** `repository` added to `[workspace.package]` (without it crates.io shows no source link and docs.rs cannot offer "go to source"); `CHANGELOG.md` in Keep a Changelog form, with a **Known limitations** section naming chunk scanning, the launcher gate race, and the absence of parallel steps; README rewritten from "reserves the crate name, not yet usable" to a 0.1.0 status with a crate table and MSRVs; the facade's description no longer says "name reservation release". **Version bumped to 0.1.0 in the manifests — not published.** Bumping a file is reversible; publishing is not.
- **18c ☑ `docs/Guide.md`** — concepts then recipes: the model, first job, chunk loop, restart, transactions, retry/skip, PostgreSQL, observability, and a "mistakes worth avoiding" section drawn from the failure modes this project actually hit.

**A release blocker found by packaging, not by testing.** `.sqlx/` sat at the **workspace root**, and `cargo package -p batchflow-postgres` therefore shipped 13 files with the migrations but *without the query cache*. The published crate would have been unbuildable by anyone — `sqlx::query!` would demand `DATABASE_URL`, and the docs.rs build (no network, no database) would fail outright. Every local check passed the whole time, because locally the root cache is found by the walk up from the crate directory. Fixed by regenerating with `cargo sqlx prepare` **from inside the crate**, which writes `crates/batchflow-postgres/.sqlx/`; sqlx checks `CARGO_MANIFEST_DIR/.sqlx` before the workspace root, so one crate-local copy serves both the workspace build and the published crate, with no duplication to drift. Verified three ways: `env -u DATABASE_URL cargo check -p batchflow-postgres` builds with the root copy deleted, `cargo package --list` now shows 11 cache files inside the package, and `cargo package -p batchflow-core` verifies at 0.1.0. **The general lesson: `cargo test` never opens the envelope you actually mail. `cargo package --list` is a distinct gate from the build.**

**Publish order is core-first and is enforced by cargo, not by discipline.** `cargo package -p batchflow-postgres` currently fails because it wants `batchflow-core = 0.1.0` and `batchflow = 0.1.0` from the registry, which do not exist yet. That is the expected sequence — `batchflow-core`, wait for the index, then `batchflow`, then `batchflow-postgres`/`batchflow-metrics` — not a defect. It also means the packaged Postgres crate has **not** been verified end to end and cannot be until core is on the registry; the evidence for it is the three checks above, not a successful verify.

**A 2.3% flake in the tracing tests, found and fixed 2026-08-05.** CI failed `a_chunk_event_nests_inside_the_step_and_job_spans` with `["step"]` where `["step", "job"]` was expected. It passed 25× solo, 72× across every `--test-threads` value, and 5× on Linux — so it was not an environment difference. Running the whole binary 300 times reproduced it **7 times, across three different tests**, and the real signature was worse than the reported one: `captured()` sometimes returned an **empty** event vector, and sometimes a partial one.

**Cause: `tracing` caches a callsite's `Interest` globally, the first time that callsite is reached.** The ~113 tests here that install no subscriber reach the framework's `warn!`/`info_span!` callsites too, and with no default dispatcher in place those get cached as `Interest::never()` — after which every later `captured()` test sees nothing from *that callsite*, whatever subscriber it installs. Per-callsite poisoning is why the failure looked like one missing span rather than a dead harness. Fixed with `AlwaysAsk`, a process-wide default subscriber that enables nothing but returns `Interest::sometimes()`, forcing `tracing` to consult the current dispatcher per event; installing it also rebuilds the interest cache, undoing any callsite already poisoned. **Verified by measurement, not inspection: 7/300 before, 0/400 after** (p ≈ 8e-5 under the observed rate).

**This is not a user-facing bug** — an application installs one subscriber at startup, before any callsite is reached. It is specific to a parallel test binary using per-task subscribers, and it is the third instance of the standing lesson that a test which captures nothing passes every assertion about absence. Note what did *not* find it: `--test-threads=1`, a Linux container, and 97 targeted runs all stayed green. **Only running the full binary many times reproduced it, because the interference is between tests, not within one.**

**Phase 18 acceptance.** Guide covers concepts and recipes ☑; `#![warn(missing_docs)]` reports zero across all four crates and `RUSTDOCFLAGS="-D warnings" cargo doc` is clean ☑; CHANGELOG and SemVer metadata ☑. **Not met: "docs.rs clean" cannot be verified before publishing** — the crate-local `.sqlx` fix is what makes it plausible, and it stays a claim until a real docs.rs build runs.

**Still open for a release:** publish itself (core-first), a `v0.1.0` tag, and release automation. Phase 14 (scheduling adapters) is the only untouched phase and is additive, so it does not block 0.1.0.

---

## Cross-cutting quality gate (every phase)
**Executed by `.github/workflows/ci.yml` since 12.5 — this list is the specification, that file is the implementation. They must not drift.**
`cargo fmt --all --check` · `cargo clippy --workspace --all-targets --locked -- -D warnings` · `cargo test --workspace --locked` · **`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`** · `cargo +<msrv> check` per lane · examples compile · no dead code · no needless clone/alloc.
**`cargo doc` is a gate in its own right — but only with `RUSTDOCFLAGS="-D warnings"`.** `fmt`, `clippy` and `test` were all green in 12a while a broken intra-doc link sat in `lib.rs`, and 12.5 found the *same class of bug still live* at `step.rs:7`, because bare `cargo doc` exits 0 on broken links. Broken links are what docs.rs ships. `#![warn(missing_docs)]` is on in all four crates, so a new public item without rustdoc now fails clippy too.
**Read the `Doc-tests` counts, do not just read the unit count** — see debt (6). A doctest can disappear without anything going red. Note there are now doctests in four crates (`batchflow_core`, `batchflow`, `batchflow_metrics`, `batchflow_postgres`); the facade's is the one that guards user-facing re-exports.
Postgres integration tests need Docker running. They are part of the gate, not an optional extra — `cargo test --workspace` silently covers less without it.

## Current position (2026-08-03)
**Version bumped to 0.1.0 in the manifests on 2026-08-05, and nothing is published.** Publish order is `batchflow-core` → wait for the index → `batchflow` → `batchflow-postgres` / `batchflow-redis` / `batchflow-metrics`.

Phase 0 ◐ docs (tech-eval open items) · 1 ☑ workspace · 2 ☑ traits · 3 ☑ Job + typestate `JobBuilder` · 4 ◐ step model (tasklet trait pending) · 5 ☑ engine · 6 ◐ chunk processing (property tests pending; **tuning guide closed by `docs/Performance.md`**) · 7 ☑ JobRepository · 8 ☑ ExecutionContext · 9 ☑ restart · **10 ☑ retry & skip** · 11 ☑ transactions · 12 ☑ metrics · 12.5 ☑ CI · 13 ☑ tracing · 14 ☐ scheduling · **15 ☑ storage backends** · **16 ☑ examples** · **17 ☑ performance** · **18 ☑ documentation**.

**The roadmap was completed out of order, and that is worth stating rather than hiding behind a row of ticks.** 16, 17 and 18 were promoted past 14 (scheduling adapters) and 15 (Redis + backend conformance suite) because those two are *additive* — a new crate and a new `JobRepository` impl break nothing — while 17 and 18 had to land before the API froze at 0.1.0. So the release path is complete and two numbered phases behind it are untouched. Remaining work, none of it blocking a release: publish · 14 scheduling adapters · Phase 4's tasklet trait · Phase 6's property tests · Phase 0's tech-eval items.

**151 tests green across the whole workspace, Docker up, 2026-08-05** — 116 `batchflow-core` unit · **2 `batchflow-core` integration (`tests/allocations.rs`, new in 17)** · 3 `batchflow` integration · 3 `batchflow-metrics` unit · 2 `batchflow-postgres` unit · 18 Postgres integration (6 classifier · 3 fault-tolerance · 9 repository) · 7 doctests across five crates. Exactly the previous 149 plus the two new allocation tests, which confirms Phase 17 left the Postgres path untouched — worth checking rather than assuming, since core's `Cargo.toml` gained a dev-dependency and a `[[bench]]` target. `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings` and `RUSTDOCFLAGS="-D warnings" cargo doc` all clean.

**149 tests green across the whole workspace, Docker up, re-verified 2026-08-04** — 116 `batchflow-core` unit · 3 `batchflow` integration (`tests/restart.rs`, new in 16) · 3 `batchflow-metrics` unit · 2 `batchflow-postgres` unit · 18 Postgres integration (6 classifier · 3 fault-tolerance · 9 repository) · 7 doctests across four crates. This is the first full run since 12.5; the previous carried-forward figure of 141 is superseded. `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings` and `RUSTDOCFLAGS="-D warnings" cargo doc` all clean, and all four examples run.

**MSRV is now per-crate and verified:** 1.85 for `batchflow-core`/`batchflow`/`batchflow-metrics`, 1.94 for `batchflow-postgres` (sqlx 0.9). Both lanes checked locally against real toolchains before the workflow was committed, and both are jobs in CI.
`batchflow-core` modules: `chunk`/`classifier`/`context`/`error`/`execution`/`fault`/`item`/`job`/`launcher`/`memory`/`repository`/`step` + `#[cfg(test)] testing`, with `lib.rs` as a pure re-export surface (private `mod` + flat `pub use`, so module layout stays refactorable). **134 tests green: 105 core unit · 3 `batchflow-metrics` unit · 6 doctests (core, facade, metrics) · 20 Postgres integration across `repository`/`classifier`/`fault_tolerance`.** clippy `--all-targets -D warnings` clean, `cargo fmt --check` clean, `cargo doc` clean, `#![warn(missing_docs)]` on everywhere and silent.

**US-3 and US-4 now hold end to end.** A malformed staging row raises a real `22P02`, is skipped, and the job completes with `skip_count` durable in `step_execution`. A writer that loses a lock race raises a real `55P03`, the chunk is rolled back and re-attempted in a *fresh* transaction, and every row lands exactly once.

**Core's dependency set grew in 10b:** `backon` (schedule only, `default-features = false`) and `tokio` (feature `time`, for the backoff sleep). Both are argued in the Phase 10 notes; `tokio` in particular is a deliberate reading of NFR-4.

**US-2 now holds end to end:** a job that dies mid-step is relaunched against the same `JobInstance`, skips the steps that completed, opens its reader at the last committed chunk, and writes no item twice.

A job now runs end to end through metadata: `JobLauncher::run` resolves a `JobInstance` from `(job_name, JobParameters)`, refuses a completed one (FR-4.4), opens a `JobExecution`, and `Job::run` persists a counted `StepExecution` per step — all reloadable from the repository alone.

**API hardening (2026-07-27):** `BatchError::Process` added (processors could not previously report failure); `chunk_size` is `NonZeroUsize` everywhere, so the silent `chunk_size == 0` no-op — a job reporting success having processed nothing — is unrepresentable; **ADR-002a**: the framework is `Send` end-to-end (`Step: Send` supertrait + RPITIT `+ Send` on the three core traits), so `tokio::spawn(job.run())` compiles. `job_run_future_is_send` and `launcher_run_future_is_send` lock that in.

**Debt closed in Phase 7:** ~~`filter_count` derived as `read - written`~~ — now counted at the processor's `None` arm, so the underflow panic is gone. ~~`Step` has no name/identity~~ — `Step::name()` plus a persisted `StepExecution` per step.

**Known debt, deliberately deferred:**
1. ~~`Starting`/`Started` still passes the FR-4.4 gate.~~ **Closed in 9a** — rejected, with `abandon_execution` shipped in the same change.
2. ~~`JobLauncher::run` resolves the instance and reads its last execution under separate lock acquisitions.~~ **Closed for Postgres in 11d** — `UNIQUE (job_name, parameters)` plus a single `INSERT .. ON CONFLICT` makes instance identity the database's job. The launcher's *gate* (read last execution, then create) is still two statements outside a transaction; two processes racing an instance with no prior execution can still both launch. Narrow, and it needs a `SELECT .. FOR UPDATE` around the gate to close — deferred, and recorded here rather than claimed fixed.
3. ~~A failing `update_execution` masks the job's original error (`launcher.rs`).~~ **Closed 2026-07-31, see ADR-009.** It was three sites, not one — the launcher's terminal `update_execution`, `Job::run`'s terminal `update_step_execution`, and `run_step`'s `commit.rollback` — all the same shape: `cleanup().await?` before propagating an outcome, which returns the *cleanup* error and drops the failure being cleaned up after. Fixed with `BatchError::CleanupFailed { cause, during_cleanup }` + `with_cleanup`, keeping the cause as the `#[source]` so classification still sees the real failure. **The earlier note here said this needed a logger (Phase 13); that was wrong** — putting both failures in the returned value is better than logging one, because it does not make recovery depend on whether a subscriber was configured. All three fixed sites are mutation-verified, each failing exactly one test.
4. ~~Library-craft gap: no `#![warn(missing_docs)]`, `Job`/`ChunkStep` lack `Debug`.~~ **Closed 2026-07-31.** `#![warn(missing_docs)]` is on in all four crates and the workspace reports zero — 127 items documented in `batchflow-core`, 3 in `batchflow-postgres`. C-DEBUG satisfied: derives on `FailFast`/`Unmanaged`/`JobLauncher`/`PostgresJobRepository`, hand-written impls where a derive is impossible. `FaultTolerance` prints its policy and `<dyn Classifier>` exactly as ADR-008 specified — a `Debug` supertrait on `Classifier` would tax every user's impl for a diagnostic. `ChunkStep` and `Job` are hand-written too, so no reader, processor, writer or step needs a `Debug` bound of its own: `Job { name: "nightly", steps: ["log"] }`. **`cargo doc` is now a fourth gate** — `fmt`, `clippy` and `test` were all green while a broken intra-doc link sat in `lib.rs`, and broken links are what docs.rs ships.
   ~~Implementing `Step` requires the caller to depend on `async-trait` directly.~~ **Closed in 9a** by `pub use async_trait::async_trait;` in `lib.rs`.
   **Correction (2026-07-30): the reasoning recorded here was wrong.** This entry claimed doctests "see the crate plus its dev-dependencies, not its normal dependencies", and therefore stand where a user stands. Both halves are false, verified by probe: a doctest in `batchflow-core` compiles `use serde::Serialize;` even though `serde` is a normal dependency that core does not re-export. rustdoc passes `--extern` for *all* direct dependencies. **A doctest is therefore more permissive than a real user's crate and can give a false green** — it will compile code a downstream crate cannot. The re-export is still correct and necessary; only the diagnosis was.
   The structural fix is the **facade crate**: `batchflow` depends on exactly one thing, `batchflow-core`, which is the graph a user actually has. A doctest there implementing `Step` via `batchflow::batchflow_core::async_trait` now guards the re-export, and deleting it fails that doctest. Core's own `Job::builder` doctest happens to catch this particular deletion because it imports *through* the re-export — but a core doctest written `use async_trait::async_trait;` would pass while every user broke. In the facade that mistake is unavailable. **User-facing API examples belong in `crates/batchflow/src/lib.rs`.**
5. ~~`read_chunk`/`process_chunk`/`run_step` are `pub` without a deliberate SemVer decision.~~ **Closed in 12b.** All four (including `ProcessedChunk`) are `pub(crate)`; `ChunkStep` is the supported way in. Grep had already shown nothing outside `chunk.rs` used three of them. This is what made 12b's new `ChunkConfig`/`ChunkMetrics` free — they never became API. Net effect of adding observability across the whole chunk loop: the public surface got *smaller*, four items retired against one method added (`StepCommit::job_name`).
6. **Doc-comment tests have no build-time protection against deletion.** The `Job::builder` `compile_fail` block has now been stripped along with its surrounding rustdoc **twice** (most recently by commit `9659a37`, which also deleted `Job::run`'s docs and left `Plan.md` claiming doctests existed when the count was 0). Nothing fails when it vanishes. Mitigation for now: the block says in its own text that it is a test, and the `Doc-tests` count in `cargo test` output is part of the quality gate below.

**Next milestone: Phase 13 — tracing.** (Historical note, kept because the reasoning still holds:) FR-6.1/6.2/6.3 are closed and proven against a real database, so the remaining fault-tolerance gap is narrow and specific: a *write* failure names a chunk, not an item, so `ErrorAction::Skip` cannot apply there. Closing it means chunk-scanning (FR-6.4) — a second one-at-a-time pass, N transactions instead of one, and an unresolved question about non-idempotent writers. That is a design decision to take deliberately, not the obvious next increment.

Phase 12 is the better default next step: retry and skip now happen *silently*. A job that skipped 400 rows and retried 30 chunks reports exactly the same thing as one that sailed through, and `skip_count` in the metadata store is the only evidence. Fault tolerance without observability converts loud failures into quiet data loss.

**One thing Phase 10 did not close:** the explicit `commit.rollback(tx)` before a retry remains **unverified, and is now known to be unverifiable through sqlx** — `Transaction::drop` queues its own rollback, so removing our call changes nothing observable. The call stays right (`Tx` is generic; no other backend guarantees a `Drop`; sqlx's is deferred to the connection's next use and swallows its own errors) but only a backend without drop-rollback could prove it. Recorded here rather than claimed as covered.
