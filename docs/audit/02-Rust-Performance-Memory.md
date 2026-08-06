# Pass 3 — Rust Idioms Review

Reviewed: ownership, borrowing, lifetimes, `Arc`/`Mutex`/`RwLock`, `Box`,
`Cow`, `PhantomData`, cloning, allocation, `Send`/`Sync`, dispatch.

## Baseline

`cargo clippy --workspace --all-targets --all-features -- -D warnings` is
clean. `#![forbid(unsafe_code)]` on all five library crates; the only `unsafe`
in the repository is a `GlobalAlloc` in `tests/allocations.rs`, which is a test
target and therefore never compiled into anything a user links. `PhantomData`
appears exactly twice and both uses are typestate, not variance games. `Cow` is
used once, correctly, in `sqlstate()` where the borrow is genuinely conditional.

Idiom findings below are real but small; this pass is where the codebase is
strongest.

---

<a id="rust-1"></a>
### RUST-1 — `RepositoryCommit::commit` clones the entire `StepExecution` on every chunk

**Severity:** Medium · **Effort:** S · **Files:** `crates/batchflow-core/src/job.rs:42-62`

```rust
async fn commit(&mut self, mut tx: R::Tx, contribution: &StepContribution, context: &ExecutionContext)
    -> Result<(), BatchError>
{
    let mut candidate = self.step_execution.clone();   // String + BTreeMap deep clone
    candidate.apply(contribution);
    candidate.set_execution_context(context.clone());  // second BTreeMap clone

    self.repository.update_step_execution_in(&mut tx, &candidate).await?;
    self.repository.commit(tx).await?;

    *self.step_execution = candidate;
    Ok(())
}
```

Per chunk this is: one `String` allocation (`step_name`), one full `BTreeMap`
clone of the previous context, and one full `BTreeMap` clone of the new one —
the second of which immediately overwrites the first. Every node of a `BTreeMap`
clone is an allocation, and the context holds the reader's bookmark plus
whatever else the reader records.

**Why the clone exists** is correct and should be preserved: the in-memory
counters must not advance until the transaction has actually committed. There
is a dedicated test for it (`a_failed_commit_does_not_advance_the_counters`).
The problem is only the *cost* of achieving it.

**Recommendation.** Two independent fixes, both keeping the invariant:

1. **Kill the redundant clone.** `candidate.clone()` copies the old context and
   then `set_execution_context(context.clone())` throws it away. Build the
   candidate without the context, or add an internal constructor. Immediate
   ~50% cut with no design change.

2. **Stop cloning the identity fields.** The mutable state is four `usize`s and
   a context; `id`, `job_execution_id` and `step_name` are immutable for the
   lifetime of the commit port. Split the "pending" state out:

```rust
struct RepositoryCommit<'a, R> {
    repository: &'a R,
    step_execution: &'a mut StepExecution,
    job_name: &'a str,
}

// In commit(): build the row to persist from immutable identity + candidate
// counters, without materialising a whole StepExecution.
let counters = self.step_execution.counters().plus(contribution); // Copy, 4 usizes
self.repository
    .update_step_execution_in(&mut tx, self.step_execution.id(), &counters, context)
    .await?;
self.repository.commit(tx).await?;
self.step_execution.apply(contribution);
self.step_execution.set_execution_context(context.clone()); // the only clone left
```

This does change `JobRepository::update_step_execution_in`'s signature, which
is a breaking change — worth taking pre-1.0, and it also fixes
[PERF-3](#perf-3) below.

**Measured impact:** against the 102 ns/chunk framework budget this is
significant in *relative* terms and negligible against a real `COMMIT`
(100 µs–1 ms). Take fix 1 unconditionally; take fix 2 only if
`update_step_execution_in` is being changed for PERF-3 anyway.

---

<a id="rust-2"></a>
### RUST-2 — `InMemoryJobRepository` maps `PoisonError` to a string, losing the panic

**Severity:** Low · **Files:** `memory.rs` — 12 occurrences of the same line

```rust
let mut inner = self.inner.lock().map_err(|e| BatchError::repository(e.to_string()))?;
```

Twelve copies. Two problems:

1. `.to_string()` on a `PoisonError` yields `"poisoned lock: another task
   failed while holding the lock"` — the original panic is gone, which is
   exactly the information the operator needs. The codebase's own
   `BatchError::repository` doc says *"Prefer passing the error itself — a
   `to_string()` here is the information a `Classifier` later needs and cannot
   recover."* This is the one place in the repository that violates its own rule.
2. Twelve copies of an error-mapping expression is where the next one gets
   subtly different.

**Recommendation.**

```rust
impl InMemoryJobRepository {
    fn lock(&self) -> Result<MutexGuard<'_, Inner>, BatchError> {
        // A poisoned lock means a previous caller panicked while holding it.
        // The store's contents may be half-updated, so this is not recoverable.
        self.inner
            .lock()
            .map_err(|_| BatchError::repository("in-memory repository lock is poisoned"))
    }
}
```

and replace all twelve. Alternatively use `parking_lot::Mutex` (no poisoning,
no `Result`) — but that is a new dependency in a crate whose in-memory store is
explicitly justified as "zero extra dependencies", so the helper is the better
trade.

**Effort:** XS. **Benefit:** one call site instead of twelve; honest message.

---

<a id="rust-3"></a>
### RUST-3 — `Script::new(...)` is constructed on every Redis call

**Severity:** Low · **Effort:** XS · **Files:** `crates/batchflow-redis/src/lib.rs` — 6 sites

```rust
let id: i64 = Script::new(FIND_OR_CREATE_INSTANCE)
    .key(...)
    .invoke_async(&mut self.conn())
    .await
```

`Script::new` computes the script's SHA-1 eagerly. Doing it per call re-hashes
a constant string on every repository operation — for `create_step_execution`
and `update_step_execution` that is once per step, but `update_execution` runs
per launch and the pattern will spread as the backend grows.

**Recommendation.**

```rust
use std::sync::LazyLock;

static FIND_OR_CREATE_INSTANCE: LazyLock<Script> =
    LazyLock::new(|| Script::new(r"
        local id = redis.call('GET', KEYS[1])
        ...
    "));
```

`LazyLock` is stable since 1.80 and this crate's MSRV is 1.88, so no new
dependency. Six mechanical edits.

**Benefit:** removes a per-call hash. Small, but it is free and it makes the
scripts read as the constants they are.

---

<a id="rust-4"></a>
### RUST-4 — `usize as i64` casts in the Postgres backend are unchecked in one direction

**Severity:** Low · **Files:** `crates/batchflow-postgres/src/lib.rs:182-185, 408-411`

```rust
step_execution.read_count() as i64,
```

The read path is careful (`count()` uses `usize::try_from` and errors on a
negative), but the write path uses `as`, which on a 64-bit target silently
reinterprets any `usize > i64::MAX` as negative — where it would then be
rejected by the `CHECK (read_count >= 0)` constraint with a confusing message.

Unreachable in practice (that many items is not a thing), but it is an
asymmetry with the read path that already has the right shape, and the
`CHECK` constraint's own comment says a negative value *"means corruption"* —
which this would make untrue.

**Recommendation.** Mirror the read path:

```rust
fn stored(value: usize) -> Result<i64, BatchError> {
    i64::try_from(value)
        .map_err(|_| BatchError::repository(format!("counter {value} exceeds i64")))
}
```

**Effort:** XS. **Benefit:** removes the only lossy cast in the crate and
keeps the `CHECK` constraint's meaning honest.

---

<a id="rust-5"></a>
### RUST-5 — `ChunkMetrics::new` clones a `String` twenty times per step

**Severity:** Low — **no change recommended** · **Files:** `chunk.rs:37-52`

Ten counters × two labels, each `job.clone()` / `step.clone()`. Twenty
`String` allocations at step start.

**Assessment:** correct as written. The alternative (`Arc<str>`) does not help
because the `metrics` crate's `Label` takes `SharedString`, and hoisting the
handles out of the chunk loop — which this code does — is worth far more than
twenty allocations *once per step*. The comment above it already states the
reasoning. Listed here only so it is not "found" again.

---

# Pass 4 — Performance Review

## What has been measured, and what has not

The project's `docs/Performance.md` is unusually honest — it states the
machine, the toolchain, the sample count, fits a two-parameter model
(`3.9 ns/item + 102 ns/chunk`), and reports the residuals rather than hiding
them. `tests/allocations.rs` turns the per-chunk allocation claim into a CI
assertion with a positive control. This is better performance practice than
most 1.0 crates have.

**What is not measured, and matters more than what is:**

| Not measured | Why it matters |
|---|---|
| Throughput against a real Postgres | The only number a user can act on. The framework's 102 ns/chunk is ~0.1% of a `COMMIT`; the *interesting* question is how much of the remaining 99.9% BatchFlow adds through the metadata `UPDATE` it does inside every chunk transaction. |
| Retry/skip path cost | `scan_on_write_failure` is documented as `N+1` transactions but never benchmarked. An operator deciding whether to enable it has prose, not a number. |
| Metadata table growth | See [PERF-3](#perf-3) — one dead tuple per chunk. |
| Memory high-water mark | `Vec::with_capacity(chunk_size)` × item size. A chunk of 10,000 fat items is unbounded from the framework's point of view. |

---

<a id="perf-3"></a>
### PERF-3 — Every chunk commit rewrites the full `step_execution` row, including a JSONB column

**Severity:** High · **Effort:** M (2 days) · **Files:** `postgres/src/lib.rs:170-200`, `job.rs:42-62`

```sql
UPDATE step_execution
   SET status = $2, read_count = $3, write_count = $4,
       filter_count = $5, skip_count = $6, execution_context = $7
 WHERE id = $1
```

This runs **inside every chunk transaction**. Three separate costs:

1. **MVCC bloat.** Postgres implements `UPDATE` as insert-new-version +
   mark-old-dead. One dead tuple per chunk. A 10M-row job at chunk size 100 =
   **100,000 row versions for a single `step_execution` row**, all of which
   autovacuum must collect. On a nightly job that is 100k/night on a table that
   never gets big enough for autovacuum's default scale factor
   (`autovacuum_vacuum_scale_factor = 0.2` of a tiny table) to trigger promptly.
2. **WAL amplification.** `execution_context` is JSONB. Because the row is
   updated in full, the whole JSONB value is re-logged every chunk even when
   the bookmark moved by one integer. TOAST does not help — small contexts are
   inline, so it is a full-row rewrite.
3. **Serialization cost.** `json(step_execution.execution_context())?` builds a
   `serde_json::Value` (a `Map` allocation plus one per key) per chunk, on the
   critical path inside the transaction.

**Why it matters.** This is the difference between "BatchFlow costs 102 ns per
chunk" (true, and what the docs say) and what a user will actually observe,
which includes an extra full-row UPDATE with JSONB in every commit. The docs
currently only account for the former.

**Recommendation.** Four steps, in order of value:

1. **Set `FILLFACTOR` so the updates are HOT.** A HOT update reuses the same
   page and does not touch indexes:

   ```sql
   ALTER TABLE step_execution SET (fillfactor = 70);
   ALTER TABLE step_execution SET (autovacuum_vacuum_scale_factor = 0.0,
                                   autovacuum_vacuum_threshold = 1000);
   ```

   HOT requires that no indexed column changes — true here, since only
   counters, status and context change. This is a one-line migration and the
   single biggest win.

2. **Do not rewrite the context when it did not change.** Readers that record
   nothing (the default `update` body is empty — see `item.rs:38`) currently
   pay a JSONB rewrite per chunk for an unchanging `{}`. Compare and skip:

   ```rust
   // In RepositoryCommit::commit
   let context_changed = context != self.step_execution.execution_context();
   ```

   and emit a two-statement path, or use `execution_context = COALESCE($7, execution_context)`
   with a `NULL` when unchanged.

3. **Benchmark it.** Add a Postgres-backed benchmark (testcontainers, `#[ignore]`d
   by default) reporting chunks/sec at several chunk sizes with and without a
   bookmarking reader. `docs/Performance.md` currently has a denominator and no
   numerator.

4. **Document the operational consequence** in `docs/Performance.md`: "the
   metadata store takes one row update per chunk; size your commit interval and
   your autovacuum settings accordingly."

**Benefit:** the difference between a framework that scales to 10M-row jobs and
one whose metadata table becomes the bottleneck. This is the highest-value
performance item in the audit.

---

<a id="perf-1"></a>
### PERF-1 — Chunk buffers are re-allocated every iteration instead of reused

**Severity:** Medium · **Effort:** S (half a day) · **Files:** `chunk.rs:99, 132, 194`

Three `Vec` allocations per chunk:

```rust
let mut chunk: Vec<R::Item> = Vec::with_capacity(chunk_size.get());  // read_chunk
let mut outputs: Vec<P::Out> = Vec::with_capacity(items.len());      // process_chunk
let mut survivors = Vec::with_capacity(items.len());                 // scan_chunk (failure path only)
```

The allocation test correctly asserts these are per-chunk, not per-item — but
"per chunk" is not the floor. Steady state should be **zero**: the loop knows
the capacity, and both buffers are dead by the end of the iteration.

**Why it is not zero today.** `read_chunk` returns `Vec<R::Item>` and
`process_chunk` consumes it by value, so ownership moves out of the loop body
on every iteration.

**Recommendation.** Hoist and pass `&mut Vec`, which also composes with
[API-3](01-Architecture-and-API.md#api-3)'s `read_batch(&mut Vec, max)`:

```rust
// Before the loop.
let mut inputs:  Vec<R::Item> = Vec::with_capacity(chunk_size.get());
let mut outputs: Vec<P::Out>  = Vec::with_capacity(chunk_size.get());

loop {
    inputs.clear();
    outputs.clear();

    read_chunk(reader, &mut inputs, chunk_size, fault, &mut skips).await?;
    if inputs.is_empty() { /* trailing-skip path */ break; }

    // `drain` so `inputs` keeps its capacity for the next chunk.
    let filtered = process_chunk(processor, inputs.drain(..), &mut outputs, fault, &mut skips).await?;
    // ...
}
```

**Caveat worth stating:** the scan path replaces `items` with survivors, so it
needs a third buffer or an in-place `retain`-style pass. In-place is possible
(`Vec::retain` cannot be async, but a manual index-based compaction can) and
would take the failure path to zero allocations too — but that is extra
complexity on a path that already costs `N+1` transactions, so the third buffer
is the right trade there.

**Measured expectation:** 3 allocations per chunk → 0 in steady state. Against
102 ns/chunk this is a few percent, not a transformation. Do it because the
allocation test can then assert the stronger, more useful property —
*allocation is a function of neither the item count nor the chunk count* —
which is a much better regression guard than the current one.

---

<a id="perf-2"></a>
### PERF-2 — The benchmark file ships with unfinished `TODO(you)` scaffolding

**Severity:** Medium · **Effort:** XS · **Files:** `crates/batchflow-core/benches/chunk_loop.rs:116-126`

```rust
// TODO(you) #1 — the sweep. Which chunk sizes actually answer the tuning
// question, and how many items do you hold fixed across them? Consider: ...

// TODO(you) #2 — declare throughput, or don't. `group.throughput(..)` makes
// criterion print items/sec alongside the time. Decide what the element is ...
```

These read as tutorial prompts left in a shipped file. Two consequences: the
crate publishes an unfinished-looking benchmark, and `group.throughput` is
genuinely not declared — so criterion prints times rather than items/sec, and
the ns/item figures in `docs/Performance.md` were computed by hand outside the
harness rather than reported by it.

**Recommendation.** Resolve both:

```rust
group.throughput(Throughput::Elements(ITEMS));
```

with the element defined as an item (matching the ns/item figure the docs
quote), and delete the prompts. If the intent is to keep the reasoning, move it
to a doc comment stating the decision rather than asking the question.

**Benefit:** criterion then reports the number the docs quote, so the two
cannot drift; and the published crate stops shipping a `TODO(you)`.

---

<a id="perf-4"></a>
### PERF-4 — No parallelism anywhere: one job is one task, one step at a time

**Severity:** Medium (design, not defect) · **Files:** `job.rs:181`, `item.rs:5-7`

`Job::run` is a sequential `for step in &mut self.steps`. `ItemReader::read`
takes `&mut self`, so a reader cannot be shared. This is documented and
deliberate — the CHANGELOG lists parallel/partitioned steps as future work, and
`item.rs` states *"parallelism comes from partitioning rather than from sharing
one reader"*, which is the right architecture.

**What is missing is the seam.** There is currently no type a partitioning
implementation would plug into: no `Partitioner`, no `StepExecution` fan-out
shape, and `StepExecution` has no partition identity, so N partitions of one
step would collide on `last_step_execution(instance_id, step_name)`.

**Recommendation for the *current* release** — not to build it, but to stop
foreclosing it:

- Add a partition discriminator to `StepExecution` now, even if always `None`.
  Adding it later is a schema migration *and* a change to the restart lookup
  key, i.e. the two riskiest things to change post-1.0.
- Or, explicitly document that partitioned steps will require a schema change,
  so nobody is surprised.

**Benefit:** the cost of adding a nullable `partition` column to a table with
no rows is zero. The cost of adding it to a table with three years of
production metadata, while changing the semantics of the restart lookup, is
not.

---

# Pass 6 — Memory Review

## Findings

<a id="mem-1"></a>
### MEM-1 — Chunk memory is unbounded from the framework's point of view

**Severity:** Medium · **Files:** `chunk.rs:99`

`Vec::with_capacity(chunk_size.get())` where `chunk_size: NonZeroUsize` is
whatever the user passed. Peak memory is `chunk_size × (size_of::<R::Item>() +
size_of::<P::Out>())` plus whatever the items own. A user who reads
`chunk_size = 100_000` rows of a wide table holds all of them, plus all of the
processed outputs, plus (on the scan path) a third copy.

Two specific consequences:

1. **No back-pressure signal.** The framework's only tuning knob for memory is
   the same knob that controls the transaction boundary. A user who wants small
   transactions and large reads, or vice versa, cannot express it.
2. **`NonZeroUsize::MAX` panics rather than errors.**
   `Vec::with_capacity(usize::MAX)` aborts with "capacity overflow" — a panic
   in a crate that otherwise routes every failure through `BatchError`.

**Recommendation.**

- Document the memory model in `docs/Performance.md`: peak = chunk_size ×
  (input + output size), and that the commit interval is therefore a memory
  decision as well as a durability one. This is currently nowhere.
- Consider a `ChunkStep::max_chunk_bytes` or a debug assertion for
  implausible chunk sizes. Low priority — but the panic path should at least be
  documented as a precondition on `ChunkStep::new`.

**Effort:** XS (docs) to S (a guard). **Benefit:** the peak-memory question is
the first one a user sizing a job asks, and the docs currently answer only the
latency half.

---

<a id="mem-2"></a>
### MEM-2 — No reference cycles, no `Arc` explosion, no leaks — verified

**Severity:** none — recorded as a negative result.

- `Arc` appears in `src/` exactly once, in `ScheduledJob` (holding the
  launcher), plus `Arc<InMemoryJobRepository>` in tests. No cycles are
  constructible: `Job` owns `Vec<Box<dyn Step>>`, steps own their
  collaborators, nothing points back up.
- No `Rc`, no `RefCell`, no `Weak`, no `MaybeUninit`.
- `Box` is used for `dyn Step`, `dyn Classifier`, `Cause` and the two
  `CleanupFailed` fields — all genuinely required by erasure or recursion.
- The one `Mutex` (`InMemoryJobRepository::inner`) is held only across
  synchronous work; no `.await` inside a guard anywhere in the crate. Verified
  by inspection of all twelve lock sites.

This is the pass with the fewest findings, and that is a real result rather
than an absence of looking.
