# BatchFlow — Implementation Plan

> Status: **Living roadmap.** Phased. Each phase: goals · learning objectives · tasks · rationale · acceptance · testing · docs.
> Mentoring cadence per phase: **concept → Rust lens → minimal sketch → you implement → I review → improve.**
> Last updated: 2026-07-24

**Legend:** ☐ not started · ◐ in progress · ☑ done

---

## Phase 0 — Research & Architecture ◐
- **Goals:** Understand Spring Batch as a running machine; pick the Rust ecosystem; write these docs.
- **Learning objectives:** JobInstance vs JobExecution; the chunk loop; TX boundary = commit interval; StepContribution; atomic bookmark = restart.
- **Tasks:** ☑ Spring Batch execution-model deep dive · ☑ Requirements.md · ☑ Architecture.md · ☑ Plan.md · ☐ finalize tech eval open items.
- **Acceptance:** Docs exist and capture the model + crate choices + ADRs.
- **Testing:** n/a. **Docs:** the three files.

## Phase 1 — Workspace ☐
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

## Phase 4 — Step Model ☐
- **Goals:** Chunk-step vs tasklet-step; `StepContribution`; `StepExecution` counters.
- **Learning:** pending-delta pattern; step status lifecycle.
- **Tasks:** `StepExecution`, `StepContribution`, status enums; tasklet trait.
- **Acceptance:** counters fold in correctly; rollback discards deltas. **Testing:** unit. **Docs:** rustdoc.

## Phase 5 — Execution Engine ☑ (basic: `run_step` chunk loop + `Step`/`ChunkStep`; no TX/persistence yet)
- **Goals:** Drive a Job through its Steps; the chunk loop (no persistence/TX yet).
- **Learning:** ownership of reader/processor/writer during a run; where `?` triggers failure.
- **Tasks:** step executor running `read_chunk` → process → write with in-memory counters.
- **Acceptance:** end-to-end in-memory job runs to completion. **Testing:** integration w/ fakes. **Docs:** flow diagram.

## Phase 6 — Chunk Processing (full) ☐
- **Goals:** commit interval, filtering, empty-chunk termination, chunk-oriented writer semantics.
- **Learning:** memory vs throughput tradeoff of N; batched writes.
- **Tasks:** finalize chunk semantics; StepContribution integration.
- **Acceptance:** filter drops items; big-N vs small-N both correct. **Testing:** property tests on counts. **Docs:** tuning guide.

## Phase 7 — JobRepository ☐  ← **first hard problem**
- **Goals:** `JobRepository` trait + InMemory impl; **transaction ownership** design.
- **Learning:** where the `tx` lives across writer + repository update in async Rust; JobInstance identity from params.
- **Tasks:** trait design; `batchflow-memory`; instance/execution/step persistence; atomic update contract.
- **Acceptance:** metadata persisted; identity dedup works. **Testing:** unit + property. **Docs:** ADR on TX ownership.

## Phase 8 — Execution Context ☐
- **Goals:** serializable bookmark bag per Job/Step execution.
- **Learning:** serde design; typed access; no untrusted deserialization (S-4).
- **Tasks:** `ExecutionContext` type; reader `update`/`open` (ItemStream-like) hooks.
- **Acceptance:** reader persists + restores position. **Testing:** round-trip serde. **Docs:** rustdoc.

## Phase 9 — Restart Support ☐
- **Goals:** resume a failed JobExecution; skip completed steps; reader seeks from bookmark.
- **Learning:** why atomicity (Phase 7) makes restart safe; no duplicate items.
- **Tasks:** restart path in engine; status checks; reader open-from-context.
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

## Phase 14 — Scheduling ☐
- **Goals:** launch API + adapters (tokio-cron-scheduler / cron / k8s). No home-grown engine.
- **Tasks:** `batchflow-scheduler` adapters; `JobLauncher`. **Acceptance:** external trigger runs a job. **Testing:** launcher unit. **Docs:** integration guide.

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

## Current position
Phase 0 ☑ docs · Phase 1 ☑ workspace · Phase 2 ☑ traits · Phase 3 ☑ Job (basic) · Phase 4 ◐ StepExecution counters (StepContribution/tasklet trait pending) · Phase 5 ☑ engine (basic) · Phase 6 ◐ chunk processing (filtering + counts done; property tests/tuning-guide pending).
All in `batchflow-core/src/lib.rs`, 8 tests green, clippy `-D warnings` clean, `cargo fmt` applied.

**API hardening pass done (2026-07-27):** `BatchError::Process` variant added (processors could not previously report failure); `chunk_size` is `NonZeroUsize` everywhere, so the silent `chunk_size == 0` no-op — a job reporting success having processed nothing — is now unrepresentable; **ADR-002a**: the framework is `Send` end-to-end (`Step: Send` supertrait + RPITIT `+ Send` on the three core traits), so `tokio::spawn(job.run())` compiles — it did not before. `job_run_future_is_send` locks that in.

**Known debt, deliberately deferred:** (1) `filter_count` is computed as `read - written` (`run_step`) — correct today, but derived-by-subtraction breaks the moment skip exists, and underflow-panics if the invariant does; fix with `StepContribution` (Phase 4). (2) `Step` has no name/identity — blocks per-step persistence and "skip completed steps" on restart; resolve in Phase 7. (3) Library-craft gap: no `#![warn(missing_docs)]`, zero doctests, `Job`/`ChunkStep` lack `Debug` (API guideline C-DEBUG), and `read_chunk`/`process_chunk` are `pub` without a deliberate decision to support them under SemVer.

**Next candidate milestones:** (a) persistence — `JobRepository` trait + InMemory, giving Job/Step identity + `JobExecution` (Phase 7), the path to restart; (b) fault tolerance — skip/retry via an error `Classifier` (Phase 10). Phase 7 subsumes debt item (2), so it is the recommended next move.
