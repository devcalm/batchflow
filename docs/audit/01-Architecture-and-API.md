# Pass 1 — Architecture Review

## What is right, stated once so the rest of this document is not read as a verdict on the whole

These are load-bearing decisions that a reviewer should *not* try to improve:

- **The `StepCommit<Tx>` indirection.** `JobRepository` is not object-safe
  (RPITIT), `Step` must be `dyn`. Introducing an object-safe commit port rather
  than boxing the repository is the correct resolution, and it has the
  side-effect of keeping persistence out of step code entirely.
- **`Unmanaged<W>` as an explicit newtype** rather than a blanket impl. The
  blanket impl genuinely cannot be written (coherence), and making
  at-least-once *visible at the call site* is better than the alternative even
  if it could.
- **Per-crate MSRV** (1.85 / 1.88 / 1.94) with a matrix CI lane per tier. Most
  workspaces get this wrong by taking the max; this one is correct and the
  reasoning is recorded in the manifests.
- **The closed `ContextValue` enum.** Removing the deserialization-gadget
  class structurally rather than defending against it is the right call, and
  the doc comment correctly identifies that adding `Double` would be breaking
  for a non-obvious reason (`Eq`).
- **Counter reconciliation** — metrics are published only after the commit
  that persisted the same numbers, with retries as the one documented
  exception. `sum(items_written_total) == sum(write_count)` is an invariant a
  surprising number of frameworks do not hold.

The findings below are what is left.

---

<a id="arch-1"></a>
### ARCH-1 — `run_step` concentrates four independent concerns in one 200-line function

**Severity:** Medium · **Files:** `crates/batchflow-core/src/chunk.rs:225-420`

`run_step` is a `loop` containing a nested `loop` containing a `match` with a
`continue` in three arms, and it simultaneously owns: chunk iteration, the
retry state machine, the scan escalation, and metric publication. The three
mutable counters threaded through it (`skipped`, `skipped_before`,
`skipped_after_process`, `attempt`, `scanned`) are the tell — five pieces of
loop-carried state means five invariants held by convention.

**Why it matters.** The correctness of this function is the correctness of the
framework, and it is currently guaranteed by 40 tests plus a wall of comments
rather than by structure. The specific hazard is the `continue` in the scan
arm: it re-enters the retry loop with `attempt` unchanged and `items` replaced.
That interaction is subtle enough that the codebase has a dedicated test whose
doc comment says *"removing the `scanned` guard makes this **hang** rather than
fail"* — an honest admission that the loop bound is not structural.

**Tradeoff.** Splitting it costs indirection in the hottest function in the
crate. The measured budget is 102 ns/chunk, so a couple of extra non-inlined
calls per chunk are free; there is no performance argument for keeping it whole.

**Recommendation.** Extract the inner retry/scan machine into a small state
type, so the loop bound is a type-level fact rather than a boolean:

```rust
/// The write side of one chunk: at most `max_attempts` tries, at most one scan.
struct WriteAttempt<'a> {
    fault: &'a FaultTolerance,
    backoff: Box<dyn Iterator<Item = Duration> + Send + 'a>,
    attempt: u32,
    scanned: bool,
}

enum Next {
    Retry,             // back off, new tx, same items
    Scan,              // isolate, then retry with survivors
    Fail(BatchError),
}

impl WriteAttempt<'_> {
    fn on_error(&mut self, error: BatchError) -> Next { /* the three branches, alone */ }
}
```

`run_step` then reads as `read → process → drive(WriteAttempt) → publish`, and
"a chunk is scanned at most once" becomes a property of `WriteAttempt` that can
be unit-tested without a reader, a writer or a runtime.

**Effort:** M (1–2 days including test migration). **Benefit:** the framework's
single riskiest function becomes independently testable. Worth doing before
parallel steps land, because that work will touch this loop.

---

<a id="arch-2"></a>
### ARCH-2 — Skip accounting is duplicated in four places with hand-rolled arithmetic

**Severity:** Medium · **Files:** `chunk.rs:255,271,285,292-293,372,410-416`

The step-wide `skipped` counter is read and differenced at six sites:

```rust
let skipped_before = skipped;
// ...
let trailing = skipped - skipped_before;                  // empty-chunk path
let skipped_reading = skipped - skipped_before;
let skipped_processing = skipped - skipped_before - skipped_reading;
let skipped_after_process = skipped;
// ...
contribution.increment_skip(skipped - skipped_before);
metrics.skipped_writing.increment((skipped - skipped_after_process) as u64);
```

Every one of these is a subtraction that is correct only if the phases run in
exactly the order they currently do. Reordering `process_chunk` above
`read_chunk` — or inserting a phase — silently produces wrong per-phase skip
attribution, and the metric tests would still pass for the phases that happen
to remain adjacent.

**Recommendation.** Make the phase the key rather than the ordering:

```rust
#[derive(Default, Clone, Copy)]
struct Skips { read: usize, process: usize, write: usize }

impl Skips {
    fn total(&self) -> usize { self.read + self.process + self.write }
    fn record(&mut self, phase: Phase) { /* ... */ }
}
```

`read_chunk`, `process_chunk` and `scan_chunk` each take `&mut Skips` and touch
only their own field. The skip limit reads `total()`. Every subtraction in
`run_step` disappears, and the per-phase metric increments become field reads.

**Effort:** S (half a day). **Benefit:** removes six order-dependent
arithmetic expressions from the framework's most-tested function. Worth doing.

---

<a id="arch-3"></a>
### ARCH-3 — The `conformance` feature is a public module gated on a feature, which is a semver trap

**Severity:** Low · **Files:** `crates/batchflow-core/src/lib.rs:29-30`

```rust
#[cfg(any(test, feature = "conformance"))]
pub mod conformance;
```

The suite is excellent and its existence is the right call. The packaging is
the risk: `conformance` is enabled as a **dev-dependency** by backends, but
Cargo feature unification means that if *any* crate in a user's graph enables
`batchflow-core/conformance` for a non-dev reason, the test suite and its
macro ship into that user's release build. It also means every function in the
suite is a public API item subject to semver — adding a case to
`__conformance_cases!` is technically a breaking change for anyone who wrote
their own invocation list.

**Recommendation.** Two options, in preference order:

1. Move the suite to its own crate, `batchflow-conformance`, depending on
   `batchflow-core`. It is then unambiguously a dev-dependency, it can carry
   its own version and its own semver policy, and adding a case is a minor
   bump there rather than a breaking change in core.
2. If it stays: document explicitly that the *set* of generated cases is not
   covered by semver, and add `#[doc(hidden)]` to `__conformance_cases!`.

**Effort:** S (option 1: half a day; it is a file move). **Benefit:** removes a
real feature-unification hazard and unblocks growing the suite freely.

---

<a id="arch-4"></a>
### ARCH-4 — `JobExecution::execution_context` is public API that nothing writes

**Severity:** Low · **Files:** `execution.rs:206-216`

The doc comment is honest: *"Nothing in the engine writes it yet."* It is
persisted by both backends, round-tripped by the conformance suite, and read by
nobody. A job-scoped context is a real feature (step A hands a value to step B)
but it is currently a serialized empty map written on every execution update.

**Recommendation.** Pre-1.0, pick one:
- **Wire it.** Give `Step::run` access to it, define the write point (job
  scope means it must commit somewhere — probably with the step's transaction),
  and add a conformance case.
- **Remove it.** Keep the column (dropping it later is a migration) but drop
  the accessor pair from the public API until it does something.

Shipping an accessor that is documented as inert is the one option that costs
something in every release afterwards.

**Effort:** XS to remove, M to wire. **Benefit:** smaller published surface, or
a feature. Either beats the status quo.

---

<a id="arch-5"></a>
### ARCH-5 — `batchflow-scheduler` depends on `batchflow-core` only to name `JobLauncher`

**Severity:** Low · **Files:** `crates/batchflow-scheduler/src/trigger.rs`

`trigger` is 40 lines that match two `BatchError` variants. The crate is
justified in ADR-006 and the reasoning holds — but the current split means the
`cron` feature's 29 extra dependencies live in a crate that a user must add
*in addition* to the facade, for a function that arguably belongs on
`JobLauncher` itself.

**Assessment: no change recommended.** This is called out only so the next
reviewer does not re-litigate it. The `Outcome` type is the real content, and
keeping `tokio-cron-scheduler` out of core's graph is worth one extra crate in
a `Cargo.toml`. Revisit only if a second scheduler adapter appears and the
crate stays this thin.

---

# Pass 2 — Public API Review

Read as a library user who has just run `cargo add batchflow`.

<a id="api-2"></a>
### API-2 — Every import goes through `batchflow::batchflow_core::…`

**Severity:** High · **Effort:** XS (1 hour) · **Files:** `crates/batchflow/src/lib.rs:146-147`

```rust
#[doc(inline)]
pub use batchflow_core;
```

That is the entire facade. So every example, every integration test and every
line a user will ever write looks like:

```rust
use batchflow::batchflow_core::{
    BatchError, ChunkStep, InMemoryJobRepository, ItemProcessor, ItemReader,
    ItemWriter, Job, JobLauncher, JobParameter, JobParameters, JobRepository, Unmanaged,
};
```

**Why it matters.** This is the first line of code a user writes and it reads
like a mistake. It also makes the facade's stated purpose — being the crate
people depend on — self-defeating: a user who sees `batchflow::batchflow_core`
will reasonably conclude they should just depend on `batchflow-core` directly,
at which point the doctest guarantee the facade exists to provide is lost.

**Recommendation.**

```rust
//! ...
#[doc(inline)]
pub use batchflow_core::*;

/// The core crate, for callers that want to name it explicitly.
///
/// Prefer the re-exports at the root: `batchflow::Job`, not
/// `batchflow::batchflow_core::Job`.
#[doc(hidden)]
pub use batchflow_core;
```

Then `use batchflow::{Job, JobLauncher, ChunkStep};`. The glob is safe here
because `batchflow-core`'s root exports are an explicit, curated `pub use`
list — there is no accidental surface to leak. Keep the `batchflow_core` path
for one release so nothing breaks, then `#[deprecated]` it.

Update all five examples and `tests/restart.rs` in the same commit; they are
the documentation.

**Benefit:** the single cheapest ergonomics improvement available, and it
touches every user's first impression.

---

<a id="api-1"></a>
### API-1 — `ItemReader` and `ItemWriter` have no `close()`, so buffered writers lose their tail

**Severity:** High · **Effort:** S (1 day) · **Files:** `item.rs:12-73`, `chunk.rs:246,417`

The reader lifecycle is `open → read* → update*`. The writer lifecycle is
`write*`. Neither has a termination hook.

**The failure.** A user writes the obvious CSV writer:

```rust
struct CsvWriter { out: BufWriter<File> }

impl ItemWriter for CsvWriter {
    type Item = Person;
    async fn write(&mut self, items: &[Person]) -> Result<(), BatchError> {
        for p in items { writeln!(self.out, "{},{}", p.name, p.age).map_err(BatchError::write)?; }
        Ok(())
    }
}
```

`BufWriter` holds up to 8 KiB. The step completes, `run_step` returns `Ok(())`,
`Job::run` records `Completed`, `JobLauncher` records `Completed` — and the
last partial buffer is flushed only when `ChunkStep` is dropped, where the
`io::Error` from a failing flush is *unobservable*. A full disk produces a job
that reports success and wrote a truncated file.

This is not hypothetical framing: it is the default behaviour of every buffered
writer in `std`, and `Unmanaged<W>` explicitly invites exactly these writers.

The reader side is milder but real — a reader holding a file handle, a database
cursor or an HTTP connection has no place to release it, and `open` is async
precisely because acquisition is expensive.

**Recommendation.** Provided methods, so no existing implementation breaks:

```rust
pub trait ItemWriter {
    type Item;
    fn write(&mut self, items: &[Self::Item])
        -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Release resources and flush anything buffered.
    ///
    /// Called once when the step ends, on both the success and failure paths.
    /// An error here **fails the step**: a flush that failed is data that was
    /// reported written and is not.
    ///
    /// Default: nothing, which is correct for a writer that buffers nothing.
    fn close(&mut self) -> impl Future<Output = Result<(), BatchError>> + Send {
        async { Ok(()) }
    }
}
```

Call sites in `run_step`:

```rust
let outcome = chunk_loop(reader, processor, writer, config, context, commit).await;

// Always, in reverse acquisition order. A close error on the failure path is a
// cleanup failure, which ADR-009 already has a shape for.
let closed = writer.close().await.and(reader.close().await);

match (outcome, closed) {
    (Ok(()),  Ok(()))      => Ok(()),
    (Ok(()),  Err(e))      => Err(e),          // flush failure fails the step
    (Err(e),  Ok(()))      => Err(e),
    (Err(e),  Err(cleanup)) => Err(e.with_cleanup(Err(cleanup))),
}
```

Note this composes with the existing `with_cleanup` machinery rather than
needing anything new — the shape is already in the codebase.

**Test that must exist:** a writer whose `close` sets a flag; assert the flag
after a successful step *and* after a step that failed mid-chunk.

**Benefit:** closes a silent-data-loss path on the happy path. This is the
highest-value API change in the crate.

---

<a id="api-3"></a>
### API-3 — `read()` is per-item, so every non-trivial reader must implement its own buffering

**Severity:** Medium · **Effort:** M (2 days) · **Files:** `item.rs:17`, `chunk.rs:89-116`

```rust
fn read(&mut self) -> impl Future<Output = Result<Option<Self::Item>, BatchError>> + Send;
```

`read_chunk` calls this `chunk_size` times. For an in-memory or file reader
that is fine. For the reader everyone actually wants — "SELECT the next 1000
rows" — the trait offers no way to say so, so every such reader ends up
with the same private `VecDeque` + refill logic, reimplemented and retested per
integration. The framework already knows the chunk size; the reader is the only
party that does not.

There is also a smaller cost the benchmark makes visible: at chunk size 1 the
loop is 105.7 ns/item versus 3.7 ns at 10,000, i.e. the per-`read()` future
poll is a measurable fraction of the per-item budget.

**Why fix it now.** This is the shape of every third-party reader that will
ever be written against BatchFlow. Adding it after an ecosystem exists is a
breaking change to the most-implemented trait in the crate; adding it now as a
provided method is free.

**Recommendation.**

```rust
pub trait ItemReader {
    type Item;

    fn read(&mut self) -> impl Future<Output = Result<Option<Self::Item>, BatchError>> + Send;

    /// Read up to `max` items into `out`, returning how many were appended.
    ///
    /// Fewer than `max` — including zero — means end of input. Implement this
    /// when the source is naturally batched (one query per chunk rather than
    /// one per row); the default calls [`read`](Self::read) in a loop, which is
    /// what a source with no batching wants.
    ///
    /// `out` is supplied by the engine and reused across chunks, so an
    /// implementation must append rather than clear.
    fn read_batch(
        &mut self,
        out: &mut Vec<Self::Item>,
        max: usize,
    ) -> impl Future<Output = Result<usize, BatchError>> + Send {
        async move {
            let start = out.len();
            while out.len() - start < max {
                match self.read().await? {
                    Some(item) => out.push(item),
                    None => break,
                }
            }
            Ok(out.len() - start)
        }
    }
}
```

**Interaction with skip that must be got right:** the current `read_chunk`
classifies a read error per item and keeps going. A batched `read_batch` that
returns `Err` cannot say *which* item failed, so the skip semantics degrade to
"the whole batch failed". Document this: a reader that implements `read_batch`
opts out of per-item read skipping unless it handles bad rows itself and simply
does not append them. That is the honest trade and it should be in the rustdoc,
not discovered.

**Benefit:** removes duplicated buffering from every database-backed reader and
makes the chunk size actually reach the source.

---

<a id="api-4"></a>
### API-4 — The test doubles are `#[cfg(test)]`-private, so users writing a `Step` start from nothing

**Severity:** Medium · **Effort:** S (1 day) · **Files:** `crates/batchflow-core/src/testing.rs` (1,079 lines)

`testing.rs` contains `VecReader`, `CollectingWriter`, `FlakyWriter`,
`TransientWriter`, `PoisonWriter`, `RecordingCommit`, `SkipAll`, `RetryAll`,
`SharedSink` and a metric-capture harness — 1,079 lines of exactly what a user
needs to test their own `Step`, `Classifier` or `TransactionalWriter`. It is
`mod testing;` under `#[cfg(test)]`, so none of it escapes the crate.

The `conformance` module already proves the team knows this pattern (its own
doc comment names the trap: *"`mod testing` is `#[cfg(test)]` and therefore
invisible outside this crate — which is exactly the trap this module must not
fall into"*). The same reasoning applies one level up.

**Recommendation.** A `testing` feature exposing a curated subset —
specifically `RecordingCommit` (a user implementing `Step` has *no* way to
drive it otherwise: `StepCommit` is a trait with four methods and no provided
implementation), plus `VecReader` / `CollectingWriter` / a controllable
failing writer.

```toml
[features]
# Test doubles for users implementing Step, Classifier or TransactionalWriter.
# Off by default; a dev-dependency, like `conformance`.
testing = []
```

Note the same feature-unification caveat as ARCH-3 applies — prefer a separate
`batchflow-testing` crate if ARCH-3 is taken.

**Benefit:** the difference between "implementing `Step` is a documented
extension point" and "implementing `Step` means writing 200 lines of harness
first".

---

<a id="api-5"></a>
### API-5 — `Arc<R>` is not a `JobRepository`, so sharing a non-`Clone` store is awkward

**Severity:** Low · **Effort:** XS (1 hour) · **Files:** `repository.rs:14`

`JobLauncher::new` takes `R` by value. `PostgresJobRepository` is `Clone` (the
pool is an `Arc` inside), so that case is fine. `InMemoryJobRepository` is not
`Clone`, and `Arc<InMemoryJobRepository>` does not implement `JobRepository` —
so the test in `job.rs:555` has to write `job.run(&execution, &*repository)`,
and a user wanting one in-memory store behind two launchers cannot have one.

**Recommendation.**

```rust
impl<R: JobRepository> JobRepository for std::sync::Arc<R> {
    type Tx = R::Tx;
    fn begin(&self) -> impl Future<Output = Result<Self::Tx, BatchError>> + Send {
        (**self).begin()
    }
    // ... one delegating line per method
}
```

Sixteen delegating methods, mechanical, no design content. It removes a papercut
that every user of the in-memory store hits.

**Benefit:** small but free.

---

<a id="api-6"></a>
### API-6 — `PostgresClassifier` derives nothing

**Severity:** Low · **Effort:** XS · **Files:** `crates/batchflow-postgres/src/classifier.rs:32`

```rust
pub struct PostgresClassifier;
```

No `Debug`, `Clone`, `Copy` or `Default`. `FailFast` next door has
`#[derive(Debug, Clone, Copy, Default)]`. Rust API Guidelines C-COMMON-TRAITS;
the practical consequence is that a user struct holding one cannot derive
`Debug`.

Also worth adding at the crate roots, to catch the next one automatically:

```rust
#![warn(missing_debug_implementations)]
```

**Benefit:** trivial, but this is the kind of thing that gets noticed in a
crates.io review and never gets fixed afterwards.

---

<a id="api-7"></a>
### API-7 — `ExecutionContext` has no `remove`, `len` or iteration

**Severity:** Low · **Effort:** XS · **Files:** `context.rs:64-139`

The surface is `new`, `put`, `get`, `is_empty`, `get_long`, `get_string`,
`get_bool`. A reader that has finished cannot clear its bookmark; a diagnostic
tool cannot enumerate what a step recorded; nothing can ask how large a context
has grown (which matters — it is serialized into a JSONB column on **every
chunk commit**, see [PERF-3](02-Rust-Performance-Memory.md#perf-3)).

**Recommendation.** Add `remove(&mut self, key: &str) -> Option<ContextValue>`,
`len(&self) -> usize`, and `iter(&self) -> impl Iterator<Item = (&str, &ContextValue)>`.
All three are one-line `BTreeMap` delegations and none of them widen the
security surface the closed enum exists to protect.

**Benefit:** makes the type usable for the diagnostics it will inevitably be
asked for.

---

<a id="api-8"></a>
### API-8 — Fault tolerance is a `ChunkStep`-only concept with no compile-time signal

**Severity:** Low · **Files:** `tasklet.rs:139-145`

`TaskletStep` deliberately has no `with_fault_tolerance`, and the reasoning in
the doc comment is sound (retry on a tasklet that has already mutated state
would impose an idempotency obligation the framework avoids elsewhere).

The gap is discoverability: a user who configures `FaultTolerance` and then
switches a step from `ChunkStep` to `TaskletStep` gets no warning that their
retry policy silently stopped existing — the method simply is not there, which
reads as "not implemented yet" rather than "deliberately excluded".

**Recommendation.** Keep the behaviour; make the refusal explicit in the type's
own docs *and* add a deprecated-on-arrival shim that fails to compile with a
message rather than with "no method named":

```rust
impl<T> TaskletStep<T> {
    #[deprecated(note = "Tasklets have no retry or skip by design — a retry would \
                         re-run a tasklet that has already mutated state. Handle \
                         retry inside `execute`, where you can see what you did.")]
    pub fn with_fault_tolerance(self, _: FaultTolerance) -> Self { self }
}
```

**Benefit:** converts a silent absence into a message. Low cost, and the
alternative is a bug report.
