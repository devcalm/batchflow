# Pass 14 — Technical Debt

Reviewed: temporary solutions, duplicated code, over- and under-engineering,
TODOs, FIXMEs, hacky code, poor naming, confusing abstractions, future
maintenance risk.

## Method

`grep -rn 'TODO\|FIXME\|XXX\|HACK\|unimplemented!\|todo!' crates/*/src crates/*/benches crates/*/tests`
returns **two** hits, both in `benches/chunk_loop.rs`. There is no
`#[allow(dead_code)]`, no commented-out code, and no `#[allow]` suppressing a
clippy lint anywhere in `src/`.

By the usual measures this codebase has almost no debt. The findings below are
therefore about a different and less common kind: **debt that takes the form of
information rather than code.**

---

<a id="debt-1"></a>
### DEBT-1 — The source is written for an audience of one, and that audience is the author

**Severity:** Medium · **Effort:** M (a deliberate pass, not a refactor)

This is the most consequential maintainability finding in the audit, and it is
easy to mistake for a strength — because the individual comments are *good*.
The problem is aggregate and it is about audience.

**The evidence.**

*Comment density.* `chunk.rs` implementation is ~420 lines, of which roughly
150 are comments. `run_step` alone has 14 comment blocks, several of them five
or six lines. The project's own convention — recorded in the maintainer's
working notes — is that code comments stay minimal and rationale lives in
`docs/`. The code does the opposite.

*Private vocabulary as cross-reference.* The source refers to a numbering
scheme that exists only in `docs/Plan.md`:

```rust
// Phase 11b makes this the owner of the step's transaction ...           job.rs:20
// Debt 3. A store that rejects the terminal status write must not ...    launcher.rs:808
// It emitted nothing until 13b-3.                                        chunk.rs:1379
// Found by mutation testing in 13c ...                                   tracing.rs:94
// the exact blind spot ADR-007 warned about.                             job.rs:587
// Phase 12's deliberate choice                                           tasklet.rs:362
```

For the author these are precise. For a contributor reading `job.rs` in
isolation they are dangling references — "Phase 11b" is a section heading in a
688-line planning document that the reader has not been told exists, and
"Debt 3" appears in no file in the repository at all.

*Historical framing in present-tense code.* Comments describe what changed
rather than what is:

> "Phase 11 moves the call inside the chunk loop, where a rollback drops the
> contribution unapplied" — `execution.rs:320`

The reader wants to know what the invariant *is*. The migration that produced
it is git history.

**Why this is debt rather than style.** Three concrete costs, all of which
appear only once someone else touches the code:

1. **Comments that describe history go stale invisibly.** "Phase 11 moves the
   call inside the chunk loop" stops being true the moment the call moves
   again, and nothing fails.
2. **Signal dilution.** `chunk.rs` contains genuinely load-bearing warnings —
   *"a rollback that itself failed leaves the transaction in an unknown state,
   so this does not retry"* — sitting at the same visual weight as archaeology.
   A reader skimming for the dangerous parts cannot find them by density.
3. **It raises the cost of every future refactor**, because
   [ARCH-1](01-Architecture-and-API.md#arch-1)'s decomposition means deciding,
   for 150 lines of comment, which fragment each one now belongs to.

**Recommendation.** Not "write fewer comments" — the *content* is valuable and
should not be lost. Sort it by audience:

| Comment kind | Where it belongs | Example |
|---|---|---|
| Invariant a future editor could break | Stays inline, ideally as a `debug_assert!` or a named test | *"rolled back whether or not the write succeeded"* |
| Rejected alternative and why | `docs/Architecture.md`, referenced by ADR number only | *"Committing item by item instead would be cheaper and wrong: …"* (16 lines in `chunk.rs`) |
| Historical record of a change | Git history / CHANGELOG | *"Phase 11 moves the call inside the chunk loop"* |
| Cross-reference to a phase | Delete, or make the target reachable | *"Debt 3"*, *"13b-3"*, *"Phase 12's deliberate choice"* |

The FR-numbers (`FR-2.4`, `FR-6.4`) are the exception and should **stay** —
they index a requirements register that is a stable, legitimate document. Fix
[DOC-1](05-Docs-and-Testing.md#doc-1) so a reader is told where that register
is.

A concrete target: `chunk.rs`'s implementation half from ~35% comment lines to
~15%, with the removed content relocated rather than deleted. That is a
mechanical, reviewable change, and it is best done at the same time as ARCH-1.

**Benefit:** the file that most needs to be readable by a second person becomes
readable by a second person.

---

<a id="debt-2"></a>
### DEBT-2 — `TODO(you)` prompts shipped in the benchmark

**Severity:** Medium · **Effort:** XS · **Files:** `crates/batchflow-core/benches/chunk_loop.rs:116-126`

The repository's only two TODOs, and they are second-person prompts:

```rust
// TODO(you) #1 — the sweep. Which chunk sizes actually answer the tuning
// question, and how many items do you hold fixed across them? Consider: ...

// TODO(you) #2 — declare throughput, or don't. `group.throughput(..)` makes
// criterion print items/sec alongside the time. Decide what the element is
// here ...
```

Covered as [PERF-2](02-Rust-Performance-Memory.md#perf-2); listed here because
it is the clearest instance of DEBT-1's pattern — a file written as a
conversation with a future self, published as a library artefact.

The functional half matters too: `group.throughput` is genuinely not declared,
so criterion reports times and the ns/item figures quoted in
`docs/Performance.md` were computed by hand. Declaring it makes the harness
report the number the docs cite, which is how the two stop drifting.

---

<a id="debt-3"></a>
### DEBT-3 — Fake collaborators are duplicated across four crate roots

**Severity:** Low · **Effort:** S (resolved by [API-4](01-Architecture-and-API.md#api-4))

`BenchReader`, `Passthrough`, `NullWriter` and `NoOpCommit` exist in near-identical
form in:

- `crates/batchflow-core/benches/chunk_loop.rs`
- `crates/batchflow-core/tests/allocations.rs`
- `crates/batchflow/tests/restart.rs` (as `Counter` / `Double`)
- `crates/batchflow/examples/restart_demo.rs`

`allocations.rs:53-56` correctly identifies why:

> "Unavoidable: every integration test and every bench is its own crate root,
> and core's shared `testing` module is `#[cfg(test)]`, which a `tests/` target
> does not see."

The diagnosis is right; the conclusion ("unavoidable") is not. Exposing the
doubles behind a `testing` feature — [API-4](01-Architecture-and-API.md#api-4)
— makes them reachable from benches, integration tests, examples *and* users.
The duplication is a symptom of a missing public surface rather than an
inherent constraint.

**Recommendation.** Fix API-4; the duplication resolves as a consequence. Until
then, the comment should say "avoidable, but the doubles are not yet public"
rather than "unavoidable", so the next reader does not accept it as settled.

---

<a id="debt-4"></a>
### DEBT-4 — `status_name` / `status_from` are copied verbatim between backends

**Severity:** Low · **Effort:** S

`crates/batchflow-postgres/src/lib.rs:67-95` and
`crates/batchflow-redis/src/lib.rs:125-153` are the same 30 lines twice: six
`const &str`s, a `BatchStatus → &'static str` match with an identical
`#[non_exhaustive]` catch-all arm, and the inverse.

Any third backend copies them a third time, and the failure mode of a
divergence is silent and severe: if one backend writes `"COMPLETE"` where
another expects `"COMPLETED"`, an execution round-trips as an error at read
time — or worse, a typo in the catch-all arm makes a status silently
unrepresentable.

**Recommendation.** The strings are a **storage format**, which makes them
core's business, not each backend's. Put them where the enum is:

```rust
// batchflow-core/src/execution.rs
impl BatchStatus {
    /// The canonical stored representation. Backends persist this string; it
    /// is a wire format and changing one is a migration, not a rename.
    ///
    /// Exhaustive by design: `BatchStatus` is `#[non_exhaustive]` only for
    /// other crates, so a new variant stops the build *here* and forces a
    /// decision about what every backend stores — rather than each backend
    /// discovering it independently at runtime.
    pub fn as_stored(self) -> &'static str { /* ... */ }

    /// Parses [`as_stored`](Self::as_stored). An unknown string is corrupt
    /// metadata, not a new status.
    pub fn from_stored(name: &str) -> Result<Self, BatchError> { /* ... */ }
}
```

This is strictly better than the current arrangement for a reason beyond
deduplication: in core the match can be **exhaustive**, so a new `BatchStatus`
variant is a compile error at the one place that must decide its stored form.
Today each backend has a runtime catch-all (*"a new variant compiles here and
has to be caught at runtime"* — both files say so), which is a check that fires
in production instead of in CI.

Add a conformance case asserting every status round-trips.

**Benefit:** removes 60 duplicated lines, converts two runtime checks into one
compile-time one, and makes a third backend cheaper to write.

---

<a id="debt-5"></a>
### DEBT-5 — `update_step_execution` and `update_step_execution_in` are the same SQL twice

**Severity:** Low · **Effort:** XS · **Files:** `crates/batchflow-postgres/src/lib.rs:170-200, 397-426`

Two methods, byte-identical `UPDATE` statements and byte-identical
`rows_affected == 0` handling, differing only in `.execute(&mut **tx)` vs.
`.execute(&self.pool)`. The `.sqlx/` cache therefore holds two entries for the
same query.

sqlx's `Executor` trait makes this deduplicable:

```rust
async fn update_step<'e, E>(executor: E, step: &StepExecution) -> Result<(), BatchError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{ /* the single copy */ }
```

Note the in-memory backend already does exactly this
(`update_step_execution_in` delegates to `update_step_execution`), so the
pattern is established — Postgres is the outlier.

**Benefit:** one query to keep correct instead of two. Small, but this is the
query that runs on every chunk commit, so a divergence between the transactional
and non-transactional paths would be a genuinely confusing bug.

---

<a id="debt-6"></a>
### DEBT-6 — `ExecutionContext`'s three typed getters are copy-pasted, and the docs say so

**Severity:** Low — **no change recommended** · **Files:** `context.rs:102-138`

`get_long`, `get_string` and `get_bool` are the same six-line match three
times. The doc comment pre-empts the finding:

> "`get_string` and `get_bool` repeat this shape rather than sharing a generic
> or a macro: three copies of six lines stay readable and each keeps its own
> rustdoc."

**Assessment: correct as written.** A macro would cost the per-method rustdoc,
which is where the important "absent vs. wrong type" distinction is explained.
A generic would need a `TryFrom<&ContextValue>` bound that buys nothing at
three implementations. Recorded so it is not "found" again — and as an example
of the codebase pre-empting a reviewer, which is the pattern DEBT-1 is a
degenerate case of.

---

<a id="debt-7"></a>
### DEBT-7 — Under-engineering: the counters are `usize` with unchecked addition

**Severity:** Low · **Files:** `step.rs:35-52`, `execution.rs:322-327`

```rust
pub fn increment_read(&mut self, count: usize) { self.read_count += count; }
```

and

```rust
pub fn apply(&mut self, contribution: &StepContribution) {
    self.read_count += contribution.read_count();
    // ...
}
```

Plain `+=`. In release builds this wraps silently on overflow; in debug it
panics. Overflowing a `usize` counter requires 2⁶⁴ items and is not reachable —
**but** the Postgres backend stores these as `BIGINT` with a
`CHECK (read_count >= 0)` constraint whose comment states that a negative value
*"means corruption rather than a legitimate value"*. A wrapped `usize` written
through `as i64` would produce exactly that, and the comment would be wrong
about what it meant.

The interesting asymmetry: this codebase reaches for `NonZeroUsize` and
`NonZeroU32` to make invalid states unrepresentable, and uses `try_from` on
every read path — but the write path uses `+=` and `as`.

**Recommendation.** `saturating_add`, which is the honest semantics (a counter
that has saturated is wrong, but it is *monotonically* wrong and does not turn
into corruption), plus the `i64::try_from` from
[RUST-4](02-Rust-Performance-Memory.md#rust-4). One-line changes; take them for
consistency with the read path rather than because the overflow is reachable.

---

<a id="debt-8"></a>
### DEBT-8 — Over-engineering: none found

**Severity:** none — recorded as a negative result.

This pass specifically looked for abstraction that has not earned its keep, and
did not find any:

- **Every trait has ≥ 2 implementations or a documented external
  implementor.** `JobRepository` has 3, `Classifier` has 2 shipped plus the
  user's, `Step` has 2, `ItemReader`/`Writer`/`Processor` are the SPI.
  `StepCommit` has 1 in `src/` (`RepositoryCommit`) plus test and bench
  implementations — and it exists to solve a concrete `dyn`-compatibility
  problem, not to abstract.
- **No generic parameter is unused.** `Job<Tx>`, `ChunkStep<R, P, W>` and
  `JobBuilder<State, Tx>` all carry meaning; the `PhantomData` uses are
  typestate.
- **No builder without a decision.** `JobBuilder` prevents an empty job at
  compile time; `RetryPolicy` and `FaultTolerance` are configuration with real
  defaults.
- **`Unmanaged<W>`** looks like ceremony until you learn the blanket impl is
  impossible under coherence — which the doc comment explains.

The one place where the abstraction/simplicity trade is arguably wrong is
[ARCH-3](01-Architecture-and-API.md#arch-3) (the conformance suite as a
feature-gated public module), and that is a packaging call rather than
over-abstraction.

---

## Debt summary

| Item | Kind | Severity | Effort | Take it? |
|---|---|---|---|---|
| [DEBT-1](#debt-1) Author-audience comments and private cross-references | Information | Medium | M | **Yes** — with ARCH-1 |
| [DEBT-2](#debt-2) `TODO(you)` in the shipped benchmark | Unfinished | Medium | XS | **Yes** — now |
| [DEBT-3](#debt-3) Duplicated test doubles | Duplication | Low | S | **Yes** — falls out of API-4 |
| [DEBT-4](#debt-4) `status_name`/`status_from` copied per backend | Duplication | Low | S | **Yes** — also removes a runtime check |
| [DEBT-5](#debt-5) Duplicated `UPDATE step_execution` | Duplication | Low | XS | Yes |
| [DEBT-6](#debt-6) Three typed getters | Duplication | Low | — | **No** — correct as written |
| [DEBT-7](#debt-7) Unchecked counter arithmetic | Under-engineering | Low | XS | Yes — consistency |
| [DEBT-8](#debt-8) Over-engineering | — | none | — | Nothing found |
