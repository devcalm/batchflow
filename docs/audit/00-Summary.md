# BatchFlow — Engineering Audit

**Date:** 2026-08-06 · **Commit:** `46f276e` · **Scope:** whole workspace, 6 crates, ~16k lines

Reviewed as: Principal Rust Engineer · Library Maintainer · Performance Engineer ·
API Designer · Security Reviewer · Production Readiness Reviewer · OSS Maintainer.

The premise of this audit is that BatchFlow becomes a widely used open-source
crate maintained for years. Every finding is judged against that, not against
"does it compile and pass tests" — which it does: `cargo clippy --workspace
--all-targets --all-features -- -D warnings` is clean.

---

## Verdict in one paragraph

The **core engine is the strongest part of this project and is close to
publishable as-is.** The chunk loop's transaction discipline, the
retry/rollback/scan interaction, the counter-reconciliation invariant, the
typestate builder and the `Unmanaged<W>` newtype are all correct and, more
unusually, *justified in place*. The design work is genuinely done.

What is missing is almost entirely **the operational layer around that
engine**, and the gap is wider than the CHANGELOG's "known limitations"
section admits. A job cannot be stopped. A panic in user code permanently
wedges an instance. The metadata store records *that* a job failed but never
*when it ran*, *how long it took*, or *why it failed*. There is no retention
story, so the tables grow forever. Writers have no `close()`, so a buffered
non-transactional writer silently drops its last chunk. These are not polish
items; they are the things an on-call engineer needs at 03:00, and they are
the reason this reads as an excellent 0.1.0 rather than a 1.0 candidate.

The second theme is **packaging**. `crates/batchflow/README.md` — the page
crates.io renders for the flagship crate — still says *"This `0.0.0` release
reserves the crate name… It is not yet usable."* Users reach the API through
`batchflow::batchflow_core::Job`. There is no CONTRIBUTING, no SECURITY.md, no
release automation, no supply-chain check. The engineering is 8/10 and the
distribution is 5/10.

---

## Scores (1–10)

| Dimension | Score | One-line justification |
|---|---:|---|
| Architecture | **9** | Crate boundaries, MSRV split and the `StepCommit` indirection are all earned. Nothing is abstract without a second implementation. |
| API Design | **7** | Ruined slightly by `batchflow::batchflow_core::*`; missing `close()`, batch read, and any cancellation surface. |
| Maintainability | **7** | Very high comment density with internal phase/ADR/"Debt 3" references that mean nothing to a new contributor. |
| Readability | **8** | Naming is excellent and consistent. `run_step` is a 200-line function with three nested loops. |
| Performance | **7** | Honestly measured (3.9 ns/item, 102 ns/chunk). Per-item `read()` forces every reader to buffer; per-chunk `StepExecution` clone + JSONB rewrite. |
| Memory Efficiency | **8** | Allocation is provably per-chunk not per-item, and CI asserts it. Chunk buffers are re-allocated rather than reused. |
| Concurrency | **5** | The launcher gate is racy (known). No parallel steps, no partitioning, no fencing token. One job = one task. |
| Async Design | **7** | `Send` end to end, correct `tokio::time::sleep` in backoff. No cancellation, no timeouts, no `spawn_blocking` guidance. |
| Testing | **8** | Conformance suite, property tests, allocation regression test, `compile_fail` doctests. No race test for the known race; no fuzz, coverage or soak. |
| Documentation | **8** | Rationale-first docs are exemplary. Facade README is factually wrong; internal `Plan.md` is shipped publicly. |
| Security | **7** | `forbid(unsafe_code)`, closed `ContextValue` enum (deliberate anti-gadget design). No input validation, no panic boundary, Redis eviction hazard undocumented. |
| Production Readiness | **5** | No timestamps, no failure reason, no retention, no stop, no heartbeat, no health surface. |
| OSS Readiness | **5** | Licences and CI are good. Everything else a contributor or a downstream security team looks for is absent. |
| Rust Idiomatic Style | **9** | `NonZeroUsize`, newtype ids, by-value `Tx`, `#[non_exhaustive]`, typestate. Idioms are chosen for effect, not for their own sake. |
| Long-term Maintainability | **7** | Strong invariants, but they live in comments rather than in types or tests in several places. |
| **Overall Engineering Quality** | **7.5** | An unusually well-reasoned core engine inside an under-operationalised, under-packaged distribution. |

---

## Top 10 highest-impact improvements

Ranked by engineering impact — correctness and operability first, then reach.

| # | Finding | Severity | Effort | Why it ranks here |
|---|---|---|---|---|
| 1 | [PROD-1 — No graceful stop; `BatchStatus::Stopped` is dead](06-Production-and-OSS.md#prod-1) | **Critical** | M (2–3 d) | The only way to end a running job is `SIGKILL`, which then needs a manual `abandon_execution`. Blocks every deployment that rolls pods. |
| 2 | [SEC-1 — A panic in user code permanently wedges the instance](04-Errors-and-Security.md#sec-1) | **Critical** | S (1 d) | One `unwrap()` in a user's processor leaves `job_execution` at `STARTED` forever. Self-inflicted outage with no automatic recovery. |
| 3 | [API-1 — `ItemReader`/`ItemWriter` have no `close()`](01-Architecture-and-API.md#api-1) | **High** | S (1 d) | A buffered CSV/S3 writer has nowhere to flush. Silent data loss on the happy path. |
| 4 | [CONC-1 — The launcher gate is racy](03-Async-and-Concurrency.md#conc-1) | **High** | M (2 d) | Two replicas can both launch one instance. The framework's headline promise is exactly-once. |
| 5 | [PROD-2 — The metadata schema has no time and no failure reason](06-Production-and-OSS.md#prod-2) | **High** | M (2–3 d) | Cannot answer "when did it run / how long / why did it fail" from the store. Also blocks any future heartbeat or stale-execution reaper. |
| 6 | [OSS-1 — The facade's crates.io README says the crate is unusable](05-Docs-and-Testing.md#oss-1) | **High** | XS (30 min) | The first thing every prospective user reads, and it is wrong. |
| 7 | [API-2 — Users must write `batchflow::batchflow_core::Job`](01-Architecture-and-API.md#api-2) | **High** | XS (1 h) | Every example, test and doc line carries the wart. Cheapest ergonomics win in the repository. |
| 8 | [PROD-3 — No retention or pruning API](06-Production-and-OSS.md#prod-3) | **High** | S (1 d) | `step_execution` gains a dead tuple per *chunk*. An hourly job at chunk size 100 over 10M rows writes 100k row versions per run, forever. |
| 9 | [SEC-2 — Redis backend is Cluster-incompatible and eviction-unsafe](04-Errors-and-Security.md#sec-2) | **High** | M (2 d) | Silent metadata loss under `allkeys-lru`; hard failure under Cluster. Neither is documented. |
| 10 | [API-3 — No batch read; every reader must buffer internally](01-Architecture-and-API.md#api-3) | **Medium** | M (2 d) | Shapes every third-party reader that will ever be written. Cheap now, breaking later. |

---

## Roadmap

### Phase 1 — Critical (before any further feature work)

Correctness and "the framework cannot be operated" issues. Nothing here is
optional for a 0.2.0.

1. **PROD-1** — graceful stop. Introduce a stop signal checked at the chunk
   commit boundary and at each tasklet pass; persist `BatchStatus::Stopped`;
   make `Stopped` a real terminal state rather than a name in an enum.
2. **SEC-1** — panic boundary. Wrap `step.run` in `catch_unwind` (or document
   `panic = "abort"` as a requirement) so a user panic becomes
   `BatchError::Process` and the terminal status is still recorded.
3. **API-1** — `close()` on `ItemReader` and `ItemWriter`, called on both the
   success and failure paths, with a test that a buffered writer flushes.
4. **CONC-1** — close the launcher race with a conditional insert
   (`INSERT … WHERE NOT EXISTS` + a partial unique index on live executions in
   Postgres; one Lua script in Redis). Add a two-launcher race test.
5. **OSS-1** — fix `crates/batchflow/README.md`. Add a CI check that it does
   not drift from the root README again.

### Phase 2 — High value (the 0.2.0 release)

Everything that turns a correct engine into an operable product.

6. **PROD-2** — schema migration `0003`: `created_at`, `start_time`,
   `end_time`, `last_updated`, `exit_code`, `exit_message` on both execution
   tables. Surface them on `JobExecution` / `StepExecution`.
7. **PROD-3** — retention: `JobRepository::purge_before(cutoff)` plus a
   documented `FILLFACTOR`/autovacuum note for `step_execution`.
8. **SEC-2** — Redis: hash-tag every key (`{batchflow}`), declare all keys in
   `KEYS[]`, document the `noeviction` requirement, and assert it on connect.
9. **API-2** — `pub use batchflow_core::*;` in the facade, keeping the
   `batchflow_core` path as a deprecated alias for one release.
10. **PROD-4** — timeouts: a per-chunk deadline on `RetryPolicy`, and a
    documented `spawn_blocking` rule for CPU-bound processors.
11. **OSS-2** — CONTRIBUTING.md, CODE_OF_CONDUCT.md, SECURITY.md, issue/PR
    templates, `cargo-deny` in CI, a release workflow.
12. **TEST-1** — a concurrency test suite: two launchers racing one instance,
    a killed process mid-chunk, a Redis `EXEC` partial failure.

### Phase 3 — Nice to have

13. **API-3** — `read_batch` as a provided method on `ItemReader`.
14. **PERF-1** — reuse chunk buffers across iterations; stop cloning the whole
    `StepExecution` per commit.
15. **API-4** — expose the test doubles behind a `testing` feature so
    downstream `Step`/`Classifier` authors are not writing `VecReader` again.
16. **API-5** — blanket `impl JobRepository for Arc<R>`.
17. **DEBT-1** — thin the comment layer; move phase/ADR archaeology into
    `docs/`, per the project's own stated convention.
18. **DOC-1** — move `docs/Plan.md` and `docs/Requirements.md` out of the
    published tree, or reframe them as design history.

### Phase 4 — Future ideas

19. Partitioned and parallel steps (the reader is `&mut self`, so this is
    partitioning, not sharing — the design already anticipates it).
20. A stale-execution reaper built on the heartbeat that PROD-2 makes possible,
    replacing manual `abandon_execution`.
21. Built-in CSV/JSON/SQL readers and writers as a separate `batchflow-io`
    crate — the traits are small, but everyone reimplements them today.
22. A `JobOperator` surface (list running, stop, restart) as the thing a CLI
    or admin HTTP endpoint is built on.
23. Optional `#[cfg(loom)]` model for the launcher gate once it is fenced.

---

## Where the passes live

> **Remediation is under way.** 25 of these findings — including both Criticals,
> CONC-1 (the launcher race) and PROD-2 (time and failure reason in the store) —
> have been fixed, and one was reassessed and dropped. See [PROGRESS.md](PROGRESS.md) for what was done, what it
> replaced, and what is left. The scores and roadmap below describe the codebase
> **as audited** at `46f276e` and are deliberately not restated.

| File | Passes |
|---|---|
| [01-Architecture-and-API.md](01-Architecture-and-API.md) | 1 — Architecture · 2 — Public API |
| [02-Rust-Performance-Memory.md](02-Rust-Performance-Memory.md) | 3 — Rust idioms · 4 — Performance · 6 — Memory |
| [03-Async-and-Concurrency.md](03-Async-and-Concurrency.md) | 5 — Async · 8 — Concurrency |
| [04-Errors-and-Security.md](04-Errors-and-Security.md) | 7 — Error handling · 11 — Security |
| [05-Docs-and-Testing.md](05-Docs-and-Testing.md) | 9 — Documentation · 10 — Testing |
| [06-Production-and-OSS.md](06-Production-and-OSS.md) | 12 — Production readiness · 13 — OSS readiness |
| [07-Technical-Debt.md](07-Technical-Debt.md) | 14 — Technical debt |
| [FINDINGS.md](FINDINGS.md) | Every finding in one table |
