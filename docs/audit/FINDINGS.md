# All findings

Every finding from the fifteen passes, in one table. Sorted by severity, then
by pass order. Effort: XS < 1h · S ≈ 1d · M ≈ 2–3d · L > 1w.

> **Status is tracked separately.** This file is the audit as taken at `46f276e`
> and is left unedited so it stays a fixed reference point.
> [PROGRESS.md](PROGRESS.md) records which of these have since been resolved —
> 21 at the last update, including both Criticals.

## Critical

| ID | Title | File(s) | Effort | Worth doing? |
|---|---|---|---|---|
| [PROD-1](06-Production-and-OSS.md#prod-1) / [ASYNC-1](03-Async-and-Concurrency.md#async-1) | No graceful stop; a dropped future leaves the instance `STARTED` forever | `chunk.rs`, `job.rs`, `launcher.rs`, `tasklet.rs` | M | **Yes — blocks deployment.** Restart machinery already exists, so it is ~80 lines. Also gives `BatchStatus::Stopped` a meaning. |
| [SEC-1](04-Errors-and-Security.md#sec-1) | A panic in user code permanently wedges the instance | `job.rs:232`, `launcher.rs:108` | S | **Yes.** One `unwrap()` in a user processor becomes an incident requiring manual DB intervention. |

## High

| ID | Title | File(s) | Effort | Worth doing? |
|---|---|---|---|---|
| [API-1](01-Architecture-and-API.md#api-1) | `ItemReader`/`ItemWriter` have no `close()`; buffered writers lose their tail | `item.rs`, `chunk.rs` | S | **Yes.** Silent data loss on the *happy* path. Provided methods, so no break. |
| [CONC-1](03-Async-and-Concurrency.md#conc-1) | The launcher's FR-4.4 gate is a check-then-act race | `launcher.rs:70-91` | M | **Yes.** Known and documented; the fix is a partial unique index + conditional insert. |
| [PROD-2](06-Production-and-OSS.md#prod-2) | Metadata schema records no time, no heartbeat, no failure reason | `migrations/0001`, `execution.rs` | M | **Yes.** One migration unlocks retention, a reaper, durations and "why did it fail". |
| [ERR-1](04-Errors-and-Security.md#err-1) | The failure that ends a job is never persisted | `launcher.rs`, `job.rs` | M | **Yes.** The store's answer to "why did it fail" is currently `FAILED`. |
| [OSS-1](05-Docs-and-Testing.md#oss-1) | The facade's crates.io README says the crate is unusable | `crates/batchflow/README.md` | XS | **Yes — today.** Highest impact-to-effort ratio in the audit. |
| [API-2](01-Architecture-and-API.md#api-2) | Users must write `batchflow::batchflow_core::Job` | `crates/batchflow/src/lib.rs` | XS | **Yes.** `pub use batchflow_core::*;` — one line plus example updates. |
| [PROD-3](06-Production-and-OSS.md#prod-3) | No retention; one dead tuple per chunk, forever | `repository.rs`, migrations | S | **Yes** (after PROD-2). The "do not delete `Failed`" subtlety is why the framework should own this. |
| [SEC-2](04-Errors-and-Security.md#sec-2) | Redis: eviction is silent metadata loss; Cluster is impossible | `redis/src/lib.rs` | M | **Yes.** Missing-field defaults turn an evicted bookmark into "start from scratch". |
| [PERF-3](02-Rust-Performance-Memory.md#perf-3) | Every chunk commit rewrites a full row incl. JSONB | `postgres/src/lib.rs:170`, `job.rs:42` | M | **Yes.** `FILLFACTOR` alone is one line and the largest win. |
| [TEST-1](05-Docs-and-Testing.md#test-1) | No concurrency tests, including for the known race | `conformance.rs` | M | **Yes.** The central claim is exactly-once under concurrency; concurrency is untested. |
| [OSS-2](06-Production-and-OSS.md#oss-2) | No CONTRIBUTING / SECURITY / CoC / templates | repo root | S | **Yes.** CONTRIBUTING especially — the `DATABASE_URL` and Docker prerequisites are written nowhere a contributor looks. |

## Medium

| ID | Title | File(s) | Effort | Worth doing? |
|---|---|---|---|---|
| [ARCH-1](01-Architecture-and-API.md#arch-1) | `run_step` concentrates four concerns in 200 lines with five loop-carried variables | `chunk.rs:225-420` | M | Yes — before parallel steps land. Makes "scanned at most once" structural rather than a boolean. |
| [ARCH-2](01-Architecture-and-API.md#arch-2) | Skip accounting is six order-dependent subtractions | `chunk.rs` | S | Yes. A `Skips { read, process, write }` struct removes all of them. |
| [API-3](01-Architecture-and-API.md#api-3) | No batch read; every DB-backed reader reimplements buffering | `item.rs:17` | M | Yes — **now**, as a provided method. Breaking to add later. |
| [API-4](01-Architecture-and-API.md#api-4) | Test doubles are `#[cfg(test)]`-private; users implementing `Step` start from nothing | `testing.rs` | S | Yes. `StepCommit` has no public implementation at all. |
| [ASYNC-2](03-Async-and-Concurrency.md#async-2) | No timeouts; a hung writer hangs the job forever | `chunk.rs`, `fault.rs` | S | Yes. `RetryPolicy` bounds attempts but never time. |
| [ASYNC-3](03-Async-and-Concurrency.md#async-3) | No `spawn_blocking` guidance for CPU-bound processors | `item.rs`, `Guide.md` | XS | Yes. One paragraph prevents the most common misuse. |
| [ASYNC-4](03-Async-and-Concurrency.md#async-4) | The cron adapter swallows every runtime failure with no hook | `scheduler/src/cron.rs` | S | Yes. A 10-line `on_outcome` callback. |
| [ASYNC-5](03-Async-and-Concurrency.md#async-5) | Redis `commit` does not verify the `EXEC` reply | `redis/src/lib.rs:318` | S | Yes. A failed `EXEC` reading as success is silent duplicate delivery. |
| [CONC-2](03-Async-and-Concurrency.md#conc-2) | No fencing token; `abandon_execution` can resurrect a live writer | `repository.rs`, `postgres` | M | Yes. A join in the chunk `UPDATE` bounds the damage to one chunk. |
| [ERR-2](04-Errors-and-Security.md#err-2) | `BatchError::Repository` covers I/O, serde, engine bugs and corruption | `error.rs:35` | S | Yes. Classifiers cannot distinguish transient from corrupt. |
| [ERR-5](04-Errors-and-Security.md#err-5) | `RepeatStatus::Continuable` is unbounded with a doc comment for a guard | `tasklet.rs` | S | Yes. The chunk loop's equivalent hazard has a bound; this one does not. |
| [SEC-3](04-Errors-and-Security.md#sec-3) | No validation on job/step names or parameter size | `job.rs`, `step.rs`, `execution.rs` | S | Yes. Names are metric labels — a data-derived name is a cardinality outage. |
| [SEC-5](04-Errors-and-Security.md#sec-5) | No `cargo-deny` / `cargo-audit` in CI | `ci.yml` | XS | Yes. Cheapest item here and the one adopters ask about. |
| [PERF-1](02-Rust-Performance-Memory.md#perf-1) | Chunk buffers re-allocated every iteration | `chunk.rs:99,132,194` | S | Yes — mainly so the allocation test can assert a stronger property. |
| [PERF-2](02-Rust-Performance-Memory.md#perf-2) / [DEBT-2](07-Technical-Debt.md#debt-2) | `TODO(you)` prompts shipped; `throughput` never declared | `benches/chunk_loop.rs` | XS | Yes — today. |
| [PERF-4](02-Rust-Performance-Memory.md#perf-4) | No partition identity on `StepExecution`, foreclosing partitioned steps | `execution.rs`, migrations | S | Yes — add the nullable column *now*, while the table is empty. |
| [MEM-1](02-Rust-Performance-Memory.md#mem-1) | Chunk memory is unbounded and undocumented | `chunk.rs:99` | XS | Yes (docs). Peak memory is the first sizing question and is answered nowhere. |
| [RUST-1](02-Rust-Performance-Memory.md#rust-1) | Full `StepExecution` clone (×2 contexts) on every chunk commit | `job.rs:42-62` | S | Yes — at minimum kill the redundant second clone. |
| [PROD-4](06-Production-and-OSS.md#prod-4) | No `ping`, no `running_executions`, no operator surface | `repository.rs` | M | Yes. Two single-query methods cover the 80% case. |
| [PROD-6](06-Production-and-OSS.md#prod-6) | No release automation for a six-crate lockstep workspace | `.github/workflows` | S | Yes. Highest-risk manual operation in the project. |
| [DOC-1](05-Docs-and-Testing.md#doc-1) | Internal `Plan.md` / `Requirements.md` published as user docs | `docs/` | S | Yes. Move to `docs/internal/`; keep the FR-numbers. |
| [DOC-2](05-Docs-and-Testing.md#doc-2) | No architecture diagram anywhere | `docs/Architecture.md` | S | Yes. Three Mermaid diagrams; the transaction-boundary one especially. |
| [DOC-4](05-Docs-and-Testing.md#doc-4) | No production/operations checklist | `docs/` | S | Yes. Mostly assembly of facts that already exist. |
| [TEST-2](05-Docs-and-Testing.md#test-2) | No coverage measurement | `ci.yml` | XS | Yes — report it, do not gate on it. |
| [TEST-3](05-Docs-and-Testing.md#test-3) | Backend tests fail confusingly without Docker | backend `tests/` | S | Yes. A contributor's first `cargo test` should be green. |
| [OSS-3](06-Production-and-OSS.md#oss-3) | No Dependabot/Renovate | `.github/` | XS | Yes. The MSRV matrix is already the guard rail that makes it safe. |
| [DEBT-1](07-Technical-Debt.md#debt-1) | Comments written for an audience of one; private phase cross-references | throughout `core` | M | Yes — with ARCH-1. Relocate rather than delete. |

## Low

| ID | Title | File(s) | Effort | Worth doing? |
|---|---|---|---|---|
| [ARCH-3](01-Architecture-and-API.md#arch-3) | `conformance` is a feature-gated public module — a unification and semver trap | `core/src/lib.rs:29` | S | Yes — prefer a separate `batchflow-conformance` crate. |
| [ARCH-4](01-Architecture-and-API.md#arch-4) | `JobExecution::execution_context` is public API that nothing writes | `execution.rs:206` | XS–M | Yes — wire it or remove it before 1.0. |
| [ARCH-5](01-Architecture-and-API.md#arch-5) | `batchflow-scheduler` is very thin | — | — | **No.** Recorded so it is not re-litigated. |
| [API-5](01-Architecture-and-API.md#api-5) | `Arc<R>` is not a `JobRepository` | `repository.rs` | XS | Yes. 16 delegating lines. |
| [API-6](01-Architecture-and-API.md#api-6) | `PostgresClassifier` derives nothing | `postgres/src/classifier.rs:32` | XS | Yes, plus `#![warn(missing_debug_implementations)]`. |
| [API-7](01-Architecture-and-API.md#api-7) | `ExecutionContext` has no `remove`, `len` or `iter` | `context.rs` | XS | Yes. Three `BTreeMap` delegations. |
| [API-8](01-Architecture-and-API.md#api-8) | Tasklets silently have no fault tolerance | `tasklet.rs` | XS | Yes — a `#[deprecated]` shim turns an absence into a message. |
| [RUST-2](02-Rust-Performance-Memory.md#rust-2) | `PoisonError` stringified 12 times; violates the crate's own rule | `memory.rs` | XS | Yes. One helper. |
| [RUST-3](02-Rust-Performance-Memory.md#rust-3) | `Script::new` re-hashes on every Redis call | `redis/src/lib.rs` | XS | Yes. `LazyLock` — MSRV already allows it. |
| [RUST-4](02-Rust-Performance-Memory.md#rust-4) | `usize as i64` on the write path while the read path uses `try_from` | `postgres/src/lib.rs` | XS | Yes — keeps the `CHECK` constraint's comment true. |
| [RUST-5](02-Rust-Performance-Memory.md#rust-5) | 20 `String` clones in `ChunkMetrics::new` | `chunk.rs:37` | — | **No.** Once per step; hoisting the handles is the win. |
| [MEM-2](02-Rust-Performance-Memory.md#mem-2) | No cycles, no leaks, no `.await` under a lock | — | — | Verified. Negative result. |
| [ASYNC-6](03-Async-and-Concurrency.md#async-6) | No `spawn`, no task leaks, no deadlock surface | — | — | Verified. Negative result. |
| [CONC-3](03-Async-and-Concurrency.md#conc-3) | Single-method races are closed; cross-method ones are not expressible | `repository.rs` | — | Design note for the CONC-1 fix. |
| [CONC-4](03-Async-and-Concurrency.md#conc-4) | `BatchStatus::Stopped` is written by nothing | `execution.rs:20` | — | Moot once PROD-1 lands. |
| [ERR-3](04-Errors-and-Security.md#err-3) | `expect` in `batchflow-metrics::builder` | `metrics/src/lib.rs:58` | — | **No.** Correct use of `expect`. |
| [ERR-4](04-Errors-and-Security.md#err-4) | Log messages are an interface; only field names are pinned | `tracing.rs` + emit sites | XS | Yes — the tests already depend on the literals. |
| [SEC-4](04-Errors-and-Security.md#sec-4) | `chunk_size`/`skip_limit`/`Continuable` are unbounded | `chunk.rs`, `fault.rs` | XS | Yes (docs + `debug_assert!`). `usize::MAX` chunk size panics rather than errors. |
| [DOC-3](05-Docs-and-Testing.md#doc-3) | Crate docs and READMEs drift independently | crate roots | XS | Yes — `include_str!` where the content should match. |
| [TEST-4](05-Docs-and-Testing.md#test-4) | No fuzzing of the deserialization path | `context.rs` | S | Optional. The dangerous class is already closed by design. |
| [TEST-5](05-Docs-and-Testing.md#test-5) | 1,200 lines of tests inline in `chunk.rs` | `chunk.rs` | — | Follow ARCH-1's decomposition; do not abandon the idiom. |
| [PROD-5](06-Production-and-OSS.md#prod-5) | Semver/MSRV policy is practised but not written down | `docs/` | XS | Yes. Half a page. |
| [DEBT-3](07-Technical-Debt.md#debt-3) | Test doubles duplicated across four crate roots | benches/tests/examples | S | Falls out of API-4. |
| [DEBT-4](07-Technical-Debt.md#debt-4) | `status_name`/`status_from` copied per backend | both backends | S | Yes — moving it to core makes the match exhaustive. |
| [DEBT-5](07-Technical-Debt.md#debt-5) | `UPDATE step_execution` written twice | `postgres/src/lib.rs` | XS | Yes. The in-memory backend already delegates. |
| [DEBT-6](07-Technical-Debt.md#debt-6) | Three copy-pasted typed getters | `context.rs` | — | **No.** Correct as written; the docs pre-empt it. |
| [DEBT-7](07-Technical-Debt.md#debt-7) | Unchecked `+=` on counters that a `CHECK` constraint guards | `step.rs`, `execution.rs` | XS | Yes — consistency with the `try_from` read path. |
| [DEBT-8](07-Technical-Debt.md#debt-8) | Over-engineering | — | — | None found. Every trait has ≥ 2 implementors. |

---

## Counts

| Severity | Count |
|---|---:|
| Critical | 2 |
| High | 11 |
| Medium | 27 |
| Low | 28 (of which 7 are "no change recommended" or verified negatives) |

## The five things to do first

1. **[OSS-1]** Fix `crates/batchflow/README.md` — 30 minutes, and it is
   currently telling every prospective user not to use the crate.
2. **[API-2]** `pub use batchflow_core::*;` — 1 hour, and it improves every
   line of example code in the repository.
3. **[SEC-1]** Panic boundary around `step.run` — 1 day, and it removes the
   most likely path from an application bug to an operational incident.
4. **[API-1]** `close()` on `ItemReader`/`ItemWriter` — 1 day, and it closes a
   silent-data-loss path on the success path.
5. **[PROD-1]** Graceful stop — 2–3 days, and it is the difference between a
   framework that can be deployed and one that cannot.
