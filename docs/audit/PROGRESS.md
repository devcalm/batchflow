# Audit remediation — progress

Running record of what has been acted on from [FINDINGS.md](FINDINGS.md).
Findings keep their audit IDs, so a row here and a row there are the same item.

**Last updated:** 2026-08-06 · **Baseline:** `46f276e`

| | |
|---|---:|
| Findings raised | 68 |
| **Resolved** | **21** |
| Closed as "no change recommended" (verified at audit time) | 7 |
| Outstanding | 40 |

Verification for everything below: `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --all-features -- -D warnings`, and
`cargo test` over the four crates that need no Docker — 14 test binaries, all
green. `batchflow-postgres` and `batchflow-redis` compile and lint clean but
their integration suites were **not** run in this session (no Docker daemon
available); see [Not verified here](#not-verified-here).

---

## Round 1 — Phase 1 (Critical) and the cheap high-value items

### PROD-1 / ASYNC-1 — Graceful stop ✅

**Was:** the only way to end a running job was to drop its future, which skipped
the terminal status write and left `job_execution.status = 'STARTED'`
permanently. Every later launch of that instance was refused with
`JobExecutionAlreadyRunning` naming a process that had already exited.
`BatchStatus::Stopped` existed in the enum, was stored by both backends, had a
metric label with a test pinning the string — and nothing ever wrote it.

**Now:** `StopSignal` (`crates/batchflow-core/src/stop.rs`), an `Arc<AtomicBool>`
with `request()` / `is_requested()`. `JobLauncher::with_stop_signal` hands one
in; `StepCommit::stop_requested` is how it reaches a step.

The design decision worth recording: **the check went on `StepCommit`, not on
`Step::run`.** The only place a stop may be honoured *is* a commit boundary, and
`StepCommit` is the commit boundary — so no SPI signature changed and a
third-party `Step` gets the hook for free as a provided method returning
`false`.

Checked in two places, both between units of durable work:

- `chunk_loop`, at the top of the outer loop — before the read rather than after
  the commit, so a job told to stop before it begins does no work at all.
- `TaskletStep::run`, after a pass commits. `Finished` wins over a stop: a
  tasklet that has completed its unit of work has nothing to resume, and
  reporting it stopped would make a restart run it again.

`BatchError::Stopped` propagates; `terminal_status` maps it to
`BatchStatus::Stopped` at both the step and job layers. That status was
*already* in the launcher's restartable arm, so the resume path is the restart
path unchanged — which is why the whole feature is small.

**Tests:** stop mid-job → `Stopped` recorded, one chunk durable, relaunch
finishes without re-writing (the full round trip); stop raised before launch →
nothing written; **unraised signal → job completes** (the control, without which
a check that always fired would still pass); `jobs_finished{status="stopped"}`
distinct from `failed`.

`crates/batchflow-core/src/{stop.rs,chunk.rs,tasklet.rs,job.rs,launcher.rs}`

---

### SEC-1 — Panic boundary ✅

**Was:** a panic in a user's reader, processor, writer or tasklet unwound
through `Job::run` and `JobLauncher::run`, so neither reached its terminal
status write. One `unwrap()` on a bad row wedged the instance until a human ran
`abandon_execution`.

**Now:** `crates/batchflow-core/src/panic.rs`. A panic becomes
`BatchError::Panic { detail }`, the execution records `Failed`, and the instance
restarts normally.

Two implementation notes:

- **No `futures` dependency and no `unsafe`.** `catch_unwind` needs a pinned
  poll; the wrapper is bounded on `F: Unpin` instead of pin-projecting, which
  works because every call site wraps an `#[async_trait]` method whose return
  type is already `Pin<Box<dyn Future>>`. `#![forbid(unsafe_code)]` holds.
- **`AssertUnwindSafe` is discharged by what happens next**, not by inspecting
  the future: a caught panic always fails the step, so any half-updated state is
  dropped rather than observed, and metadata consistency rests on the
  transaction that rolled back.

Applied at both layers — `Job::run` guards each step, `JobLauncher::run` guards
the repository and the job's own bookkeeping.

**Tests:** message survives for both `&'static str` and formatted `String`
payloads (they box differently — handling only one loses exactly the messages
carrying runtime detail); **ordinary errors pass through unchanged** (the
control); a panicking step records `Failed` at both layers; **the instance
relaunches with no `abandon_execution` in between**, which is the operational
point.

Documented limitation: inert under `panic = "abort"`.

---

### API-1 — `close()` on `ItemReader` / `ItemWriter` ✅

**Was:** no termination hook. The obvious buffered writer (`BufWriter`,
`csv::Writer`, a batching client) still held part of the last chunk when `write`
returned and flushed on `Drop`, where the `io::Error` is unobservable — a full
disk produced a job that reported success and wrote a truncated file.

**Now:** provided methods on `ItemReader`, `ItemWriter` and
`TransactionalWriter`, so nothing existing breaks. `Unmanaged<W>` delegates.
`run_step` was split into `run_step` (open → loop → close) and `chunk_loop`.

Semantics, all tested:

- Called on **both** paths. A writer that gave up mid-step still holds a buffer.
- **A failed close fails the step** — a flush that failed is data the step
  reported written and did not write.
- Both collaborators are closed even if the first fails, in reverse acquisition
  order.
- Paired with `open`: a reader whose `open` failed is never closed. The test
  double's `close` *panics*, so a regression here fails loudly.
- Composes with the existing `with_cleanup`, so a close failure during a failed
  step reads as `CleanupFailed { cause, during_cleanup }` — the same precedence
  ADR-009 gives a failed rollback.

`TransactionalWriter::close` takes no transaction, deliberately: the last
chunk's transaction has already resolved, and opening another would write
outside every commit interval.

---

### OSS-1 — Facade README ✅

**Was:** `crates/batchflow/README.md` — what crates.io renders for the flagship
crate — said *"This `0.0.0` release reserves the crate name… It is not yet
usable."*

**Now:** accurate status, a runnable quickstart, the crate table, absolute
GitHub links (relative ones break on crates.io).

Two things stop it recurring: the README is now the facade's crate docs via
`#![doc = include_str!("../README.md")]`, **so its example is a compiled
doctest**; and a CI step fails the build on `0.0.0` / "not yet usable" /
"reserves the crate name" in any crate README. Nothing else would have caught
it — a README is not compiled, and it is the one file no test reads.

---

### API-2 — Facade re-export ✅

**Was:** `use batchflow::batchflow_core::{…}` in every example, test and doc.

**Now:** `pub use batchflow_core::*;`, so `use batchflow::{Job, JobLauncher};`.
The old path is kept `#[doc(hidden)]` and still resolves. The glob is safe
because core's root is a curated `pub use` list.

The two structural doctests the facade carried moved to
`crates/batchflow/tests/facade.rs`, where they *run* rather than merely compile.
The guarantee they exist for is unchanged: this crate's dependency graph is one
crate deep, so it cannot name `async-trait` even by accident.

---

### SEC-2 (part) — Redis eviction ✅ / Cluster documented ✅

**Was:** `HGETALL` on an evicted key returns an empty hash, and every field
lookup had a default — so an evicted step execution read back as a pristine
`STARTING` with an empty bookmark, **indistinguishable from a step that had
never run**. The restart re-read the input from the beginning and re-wrote every
committed item. Silently.

**Now:** an empty hash for a record the engine believes exists is rejected, with
a message naming `maxmemory-policy noeviction` and `appendfsync always`. The run
fails — which is the right trade, because eviction of batch metadata has no safe
interpretation.

Crate docs now state that **Redis Cluster is not supported** structurally rather
than merely untested: the scripts declare cross-slot keys and construct further
keys from `ARGV`.

Still outstanding from SEC-2: hash-tagging keys, declaring every key in `KEYS[]`,
the `CONFIG GET maxmemory-policy` check on connect, and delimiter escaping in
key construction.

---

### The quick-win batch ✅

| ID | Change |
|---|---|
| **API-5** | `impl JobRepository for Arc<R>` — `InMemoryJobRepository` is not `Clone`, so this was the only way to share it, and it did not compile. Tested. |
| **API-6** | `PostgresClassifier` derives `Debug`/`Clone`/`Copy`/`Default`; `#![warn(missing_debug_implementations)]` at all five crate roots (which then required a hand-written `Debug` for `JobBuilder`). |
| **API-7** | `ExecutionContext::{len, remove, iter}`. `len` matters more than it looks: this map is serialized on *every* chunk commit. |
| **RUST-2** | One `lock()` helper in `InMemoryJobRepository` instead of twelve copies, and no more `PoisonError::to_string()` — which rendered the poisoning, not the panic that caused it, in violation of the crate's own "never stringify a cause" rule. |
| **RUST-3** | Redis Lua scripts are `LazyLock<Script>`, hashed once instead of per call. |
| **RUST-4** | `i64::try_from` on the counter write path, mirroring the read path. `as` would reinterpret a large `usize` as negative, which the non-negative `CHECK` would reject with a message claiming corruption. |
| **DEBT-5** | The two identical `UPDATE step_execution` statements are one function generic over `sqlx::Executor`. **The query literal is byte-identical to the `.sqlx/` cache entry** — `sqlx` hashes the string, so it could not be reflowed without a live database. There is now a comment saying so. |
| **PERF-2 / DEBT-2** | `Throughput::Elements` declared, so criterion prints the ns/item figure `docs/Performance.md` quotes rather than it being hand arithmetic. Both `TODO(you)` prompts resolved. |

---

### OSS-2 / OSS-3 / SEC-5 / DOC-4 ✅

- **`CONTRIBUTING.md`** — the highest-value of these, because it records two
  prerequisites that were written down nowhere: the backend suites need Docker,
  and `DATABASE_URL` must stay **unset** so `sqlx` validates against the
  committed cache. It also documents that reindenting a query invalidates that
  cache, which is not obvious and which this very session hit.
- **`SECURITY.md`** — private reporting, supported versions, and an explicit
  statement of the security properties claimed (no `unsafe`, closed
  `ContextValue`, no SQL/Lua injection surface, contained panics) plus what is
  out of scope. A project making a security argument needs a disclosure channel.
- **`CODE_OF_CONDUCT.md`**, issue templates (the bug template asks *"does it
  reproduce against `InMemoryJobRepository`?"*, which splits engine bugs from
  backend bugs in one question) and a PR template with conditional checklists.
- **`deny.toml` + a `cargo-deny` CI lane** on PRs and weekly. `multiple-versions`
  is `warn` rather than `deny`, aimed at ADR-010's specific worry.
- **`.github/dependabot.yml`**, made safe to automate by the existing MSRV
  matrix — a bump that raises a tier fails its lane on the PR.
- **`publish --dry-run` / `cargo package --workspace`** lane: six crates publish
  in lockstep and in dependency order, so packaging problems should surface
  before a release rather than during one.
- **`docs/Operations.md`** — the page an SRE reads. Choosing a backend; the
  commit interval as a *memory* and *blast-radius* decision, not only a
  throughput one; what the per-chunk metadata `UPDATE` costs in row versions and
  the `FILLFACTOR` fix; stopping a job; recovering a wedged instance; a table of
  what to alert on; and an honest "known gaps" section that tells operators to
  run one replica per instance until CONC-1 is closed.

---

## Not verified here

- **`batchflow-postgres` and `batchflow-redis` integration suites.** No Docker
  daemon in this environment. Both crates build and lint clean under
  `--all-features`, and the Postgres SQL change was made byte-identical to the
  committed `.sqlx/` cache precisely so it needs no re-preparation — but the
  conformance suites have not been executed against a live server since these
  changes. **Run `cargo test --workspace --all-features` on a machine with
  Docker before releasing.**
- **The Redis eviction path** is a code change with no test behind it; a test
  needs a container that can be made to evict. Worth adding with the rest of
  SEC-2.

---

## Outstanding, in the order the roadmap wants them

### Phase 1 remainder

- **CONC-1** — the launcher's check-then-act race. The largest correctness item
  left. Needs a partial unique index on live executions plus a conditional
  insert, and the decision has to move from `JobLauncher` into the repository
  (only the store can make it atomically). `docs/Operations.md` currently tells
  operators to run one replica per instance because of this.
- **TEST-1** — the conformance case for concurrent `create_execution`. Write it
  `#[ignore]`d now: it fails against all three backends, which is the point, and
  it becomes the acceptance test for CONC-1.

### Phase 2

- **PROD-2** — timestamps, heartbeat and `exit_message` in the schema. Migration
  `0003`. One migration unlocks retention, an automatic reaper, durations and
  "why did it fail" — four capabilities blocked on one missing column.
- **ERR-1** — persist the failure reason (falls out of PROD-2).
- **PROD-3** — retention. Blocked on PROD-2's timestamps. The "do not delete a
  `Completed` execution while keeping its instance" rule is why the framework
  should own this rather than leaving it to users' `DELETE` statements.
- **PERF-3** — `FILLFACTOR` migration and skipping the JSONB rewrite when the
  context has not changed. Documented in `docs/Operations.md` as manual DDL in
  the meantime.
- **SEC-2 remainder** — key hash-tags, `KEYS[]` declarations, the connect-time
  `maxmemory-policy` check, delimiter escaping.
- **ASYNC-2** — `RetryPolicy::attempt_timeout`. A hung writer still hangs a job.
- **ASYNC-5** — verify the Redis `EXEC` reply rather than typing it `()`.
- **PROD-4** — `JobRepository::ping` and `running_executions`.
- **PROD-6** — release automation (the dry-run lane is in; the publish lane is
  not).
- **ASYNC-3** — the `spawn_blocking` paragraph. Partly covered by
  `docs/Operations.md` §7; still wanted on the `ItemProcessor` rustdoc.
- **SEC-3** — validate job/step names and parameter size. Names are metric label
  values, so a data-derived name is a cardinality outage.

### Phase 3 and beyond

ARCH-1 (decompose `run_step`), ARCH-2 (`Skips` struct), ARCH-3
(`batchflow-conformance` crate), ARCH-4 (wire or remove
`JobExecution::execution_context`), API-3 (`read_batch`), API-4 (public test
doubles — which also resolves DEBT-3), API-8, CONC-2 (fencing token), ERR-2,
ERR-4, ERR-5, PERF-1, PERF-4 (partition column while the table is empty), MEM-1,
RUST-1, TEST-2/3/4, DOC-1/2/3, DEBT-1 (the comment-audience pass, best done with
ARCH-1), DEBT-4, DEBT-7.

---

## Notes for the next session

1. **Get Docker running first** and run the full suite. Nothing below should be
   started until the backend suites are known green.
2. **CONC-1 is the next real piece of work.** It is the only remaining item that
   breaks the framework's headline promise, and `docs/Operations.md` has an
   operational workaround in it that should not be permanent.
3. **PROD-2 before PROD-3 and ERR-1** — both depend on its columns.
4. **Do PERF-4's partition column with PROD-2's migration.** Adding a nullable
   column to an empty table is free; adding it to three years of production
   metadata while changing the restart lookup key is not.
5. **Reindenting a `sqlx::query!` invalidates the offline cache.** It cost time
   in this session. `CONTRIBUTING.md` now says so.
