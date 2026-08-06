# Pass 9 — Documentation Review

Reviewed: README, crate docs, module docs, public API docs, examples,
tutorials, rustdoc examples, internal architecture docs, diagrams, onboarding.

## Baseline

The documentation is the project's second-best asset after the engine, and the
reason is a discipline most projects do not have: **every non-obvious decision
is documented with its alternative and the reason the alternative was
rejected.** Examples, chosen from many —

- `item.rs:8-11` explains why the traits spell out
  `impl Future<Output = …> + Send` instead of `async fn` (an `async fn` in a
  trait produces an unnameable type no bound can constrain).
- `context.rs:19-28` explains why there is no `Double(f64)` *and* why adding
  one would be breaking for a second-order reason (`StepExecution: Eq`).
- `tracing.rs:21-37` documents a genuine trap — `tracing::warn!(FIELD_PHASE = …)`
  compiles and emits a field literally named `FIELD_PHASE` — and explains why
  the constants therefore cannot be single-source-of-truth.
- `chunk.rs:169-182` explains why chunk scanning is two passes rather than
  per-item commits, in terms of what `ItemReader::update` can and cannot
  express.

`docs/Performance.md` states its machine, toolchain, sample count, fitted model
*and residuals*, and says outright that unmeasured things are unmeasured. That
is better than most 1.0 crates.

The findings below are about **packaging and audience**, not quality.

---

<a id="oss-1"></a>
### OSS-1 — The facade's crates.io README says the crate is not usable

**Severity:** High · **Effort:** XS (30 minutes) · **Files:** `crates/batchflow/README.md`

```markdown
## ⚠️ Status: under active development

This `0.0.0` release **reserves the crate name** while the framework is being
designed and built in the open. **It is not yet usable.** The API is unstable
and will change.
```

`crates/batchflow/Cargo.toml` sets `readme = "README.md"`, so **this** is what
crates.io renders for the flagship crate — the one the root README tells
everyone to depend on. It contradicts every other document in the repository,
which describe a `0.1.0` that runs jobs end to end.

It also lists "Planned capabilities" that are all shipped, and links
`LICENSE-APACHE` / `LICENSE-MIT` relatively — which resolve inside the package
(both files are duplicated into `crates/batchflow/`), so those are fine.

**Recommendation.** Replace with a trimmed version of the root README: status,
install snippet, one runnable example, the crate table, links to
`docs/Guide.md` (absolute GitHub URLs, since relative links break on
crates.io), and the licence block.

**Then prevent the recurrence**, because this is a drift class, not a one-off:

```yaml
- name: crate READMEs must not claim 0.0.0
  run: |
    ! grep -rn "0\.0\.0\|not yet usable" crates/*/README.md
```

Better still, generate the facade README from the crate docs with
`cargo-readme`, or make it a symlink to the root README if the content should
be identical.

**Benefit:** the first thing every prospective user reads currently tells them
not to use the crate. Highest ratio of impact to effort in the audit.

---

<a id="doc-1"></a>
### DOC-1 — Internal planning documents are published as user documentation

**Severity:** Medium · **Effort:** S · **Files:** `docs/Plan.md` (688 lines), `docs/Requirements.md` (160 lines)

`docs/Plan.md` is a private build log: eighteen phases with checkboxes, dates
(*"Phase 10d closed 2026-08-05"*), a "Current position" section, and
sub-phase identifiers (`13b-3`) that appear as cross-references in the source
code. `docs/Requirements.md` is an FR/US/NFR register.

Neither is *bad*; both are genuinely useful artefacts. But they are shipped in
a public repository as peers of `Guide.md` and `Examples.md`, and they answer
questions a user never asks. The concrete costs:

1. A first-time reader who opens `docs/` sees six files and cannot tell which
   two are for them.
2. The source code references `Plan.md`'s vocabulary (*"Debt 3"*, *"Phase
   11b"*, *"13b-3"*, *"FR-6.4"*) as if it were shared context. For the author
   it is; for a new contributor it is a private index. See
   [DEBT-1](07-Technical-Debt.md#debt-1).
3. `Plan.md` will go stale the moment development stops following it, and a
   stale roadmap in a public repo is worse than no roadmap.

**Recommendation.** Move both to `docs/internal/` with a one-line header
explaining what they are and that they are historical, and keep `docs/` for
Guide, Examples, Performance and Architecture. Keep the FR-numbers — they are
genuinely useful as stable identifiers in code comments — but put the register
they index somewhere a reader is told is a register.

Alternatively, convert `Requirements.md` into a `docs/Design.md` written for a
reader rather than for the author, and retire `Plan.md` to the git history
once 1.0 ships.

**Benefit:** `docs/` becomes navigable, and the code's cross-references point
at something a contributor is told how to read.

---

<a id="doc-2"></a>
### DOC-2 — No architecture diagram anywhere

**Severity:** Medium · **Effort:** S · **Files:** `docs/Architecture.md`

`docs/Architecture.md` is 490 lines of prose and tables describing a system
with a genuinely non-obvious shape: two families of domain objects
(`JobInstance`/`JobExecution`/`StepExecution` vs. `Job`/`Step`/`ItemReader`),
a `StepCommit` port that exists because of `dyn`-compatibility, a transaction
whose boundary is the commit interval, and a restart mechanism that is emergent
rather than implemented.

Every one of those is a picture. There is not one in the repository.

**Recommendation.** Three diagrams, in a format that renders on GitHub and
docs.rs (Mermaid renders on GitHub; embed the same content as ASCII in
rustdoc):

1. **The chunk loop's transaction boundary** — the single most important
   concept in the framework, and the one that distinguishes it from a `for`
   loop:

   ```
   ┌─ transaction ────────────────────────────────────┐
   read × N ─▶ process ─▶ │ write ─▶ update bookmark ─▶ update counters │ ─▶ COMMIT
        (outside)         └──────────────────────────────────────────────┘
   ```

2. **The metadata object graph** — `JobInstance 1─* JobExecution 1─*
   StepExecution`, with the identity key `(job_name, parameters)` marked, and
   the arrow showing that restart reads `last_step_execution(instance, name)`
   *across* executions. That last edge is the whole of restart and it is the
   thing prose explains least well.

3. **Crate dependency graph** — six crates, which crate depends on which, and
   where the MSRV boundaries fall.

**Benefit:** `docs/Architecture.md` currently requires a careful linear read to
extract a shape that a diagram conveys in ten seconds. For an OSS project, the
diagram is what determines whether someone evaluates the project at all.

---

<a id="doc-3"></a>
### DOC-3 — No `#![doc = include_str!("../README.md")]`, so crate docs and README drift independently

**Severity:** Low · **Effort:** XS · **Files:** all crate roots

Each crate's `//!` docs and its README are maintained separately.
[OSS-1](#oss-1) is what that drift looks like when it goes unnoticed for a
release.

**Recommendation.** Where the content should be identical, make it identical:

```rust
#![doc = include_str!("../README.md")]
```

Where it should not (the facade's crate docs contain doctests that a README
cannot), keep them separate but add the CI grep from OSS-1.

---

<a id="doc-4"></a>
### DOC-4 — The Guide has no "production checklist"

**Severity:** Medium · **Effort:** S · **Files:** `docs/Guide.md`

`docs/Guide.md` (713 lines) covers concepts and recipes well. What a user
deploying this for the first time needs, and cannot currently assemble from one
place:

- Which backend, and why (`appendfsync always` + `noeviction` for Redis; see
  [SEC-2](04-Errors-and-Security.md#sec-2)).
- How to size a chunk: the latency curve *and* the memory implication
  ([MEM-1](02-Rust-Performance-Memory.md#mem-1)) *and* the metadata-write
  implication ([PERF-3](02-Rust-Performance-Memory.md#perf-3)).
- What to do when a process dies: `abandon_execution`, and the fact that
  nothing does it automatically.
- What to alert on: `batchflow_chunk_scans_total > 0`,
  `jobs_started - jobs_finished` as in-flight,
  `items_skipped_total` rate, `chunk_retries_total` rate. The metric docs
  describe each metric individually; nothing says which ones page you.
- The `spawn_blocking` rule ([ASYNC-3](03-Async-and-Concurrency.md#async-3)).
- Retention: nothing prunes the metadata tables
  ([PROD-3](06-Production-and-OSS.md#prod-3)).

**Recommendation.** A `docs/Operations.md`, ~2 pages, that is the page an SRE
reads. It is mostly assembly of facts that already exist in scattered rustdoc.

**Benefit:** the gap between "well documented for a reader" and "well
documented for an operator" is the gap between this being a 0.1.0 and a 1.0.

---

# Pass 10 — Testing Review

## Baseline

The test suite is genuinely strong and several parts of it are better than the
norm for a 1.0 crate:

| Technique | Where | Assessment |
|---|---|---|
| **Conformance suite** | `conformance.rs`, 775 lines, 35 cases | Excellent. Its own doc comment records *why* it exists: the two backends' test files had drifted (21 properties vs. 7) and `abandon_execution` was tested against only one. Solving contract drift with executable contracts is the right answer. |
| **Property tests** | `properties.rs`, 301 lines, proptest | Found a real bug (trailing skips dropped) that no example-based test had. The regression seeds are committed *and* `exclude`d from the published package, with the reason recorded. |
| **Allocation regression test** | `tests/allocations.rs` | Turns a performance claim into a CI failure, and — critically — has a **positive control** (`the_counter_observes_per_item_allocation`) so the measurement cannot silently degrade to comparing 0 with 0. |
| **`compile_fail` doctests** | `job.rs:131`, `launcher.rs:38` | The typestate builder and the `Tx`-mismatch guarantee have no other check. Both have positive controls so a typo does not pass as a success. |
| **Negative controls throughout** | e.g. `the_default_policy_does_not_skip`, `a_step_that_does_not_fail_does_not_wait` | Consistently present. This is the discipline most suites lack. |
| **Mutation-testing derived tests** | `metrics.rs:173`, `tracing.rs:96` | Renaming `ITEMS_READ` left the whole suite green while every dashboard would have gone blank; the response was a value-pinning test. Correct diagnosis and correct fix. |

**Facade-level tests** (`crates/batchflow/tests/restart.rs`) exist specifically
because a doctest in `batchflow-core` can name that crate's own dependencies
and therefore compiles code a user cannot. That reasoning is right and rare.

---

<a id="test-1"></a>
### TEST-1 — No concurrency tests, including for the race the project already knows about

**Severity:** High · **Effort:** M · **Files:** `conformance.rs`, all backend test files

Every test in the repository is single-threaded and sequential. There is no
test that:

- **races two launchers on one instance** — the known
  [CONC-1](03-Async-and-Concurrency.md#conc-1) defect. The CHANGELOG documents
  it in prose; nothing asserts it, so nothing will tell you when it is fixed or
  if it regresses.
- **races two `find_or_create_instance` calls** — the one race that *is* closed
  (by `ON CONFLICT`), and therefore the one most worth pinning, because the fix
  is a single SQL clause someone could refactor away.
- **kills a process mid-chunk and restarts.** `restart.rs` simulates this by
  building a fresh `Job` against the same store, which is the right *shape* —
  but it never exercises an actually-interrupted transaction. The Postgres
  suite has the infrastructure (testcontainers) to do the real thing.
- **exercises `abandon_execution` while a job is running** — the zombie-writer
  scenario in [CONC-2](03-Async-and-Concurrency.md#conc-2).

**Recommendation.** Three additions, in order of value:

1. A conformance case for concurrent `create_execution` (see CONC-1 for the
   code). It will fail today — that is the point; mark it `#[ignore]` with the
   defect referenced, so it becomes the acceptance test for the fix.
2. A Postgres integration test that opens a transaction, writes a chunk, and
   drops the connection without committing — asserting the bookmark did not
   move and a restart re-reads exactly that chunk.
3. A `tokio::join!`-based conformance case for concurrent
   `find_or_create_instance`, pinning the property that is currently correct.

**Benefit:** the framework's central claim is exactly-once under concurrency,
and concurrency is the one thing the suite does not exercise.

---

<a id="test-2"></a>
### TEST-2 — No coverage measurement, so gaps are found by reading

**Severity:** Medium · **Effort:** XS · **Files:** `.github/workflows/ci.yml`

No `cargo-llvm-cov`, no `tarpaulin`, no coverage reporting. The suite is
clearly thorough, but "clearly thorough" is an impression; the specific
question — *which branches of `run_step`'s retry/scan state machine are never
taken in tests* — cannot currently be answered.

Given [ARCH-1](01-Architecture-and-API.md#arch-1) (a 200-line function with
three nested control-flow levels and five loop-carried variables), that is
exactly the function where a coverage number is worth having.

**Recommendation.**

```yaml
  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: taiki-e/install-action@cargo-llvm-cov
      - run: cargo llvm-cov --workspace --all-features --lcov --output-path lcov.info
      - uses: codecov/codecov-action@v4
```

Do **not** add a coverage gate — a percentage threshold produces tests written
for the number. Report it, and look at the uncovered lines in `chunk.rs`
specifically.

**Benefit:** converts "the suite looks thorough" into a list of specific
uncovered branches.

---

<a id="test-3"></a>
### TEST-3 — Backend tests are silently skipped when Docker is absent

**Severity:** Medium · **Effort:** S · **Files:** `crates/batchflow-postgres/tests/*`, `crates/batchflow-redis/tests/*`

The Postgres and Redis suites use `testcontainers`. On a machine without
Docker, they fail — but a contributor running `cargo test --workspace` locally
gets a wall of container errors indistinguishable from real failures, and the
docs say nothing about the prerequisite.

Conversely, the CI relies on Docker being preinstalled on GitHub runners (the
workflow comment says so). That is true today and is an undeclared dependency
on the runner image.

**Recommendation.**

1. Document the prerequisite in CONTRIBUTING ([OSS-2](06-Production-and-OSS.md#oss-2)):
   "backend tests need Docker; `cargo test -p batchflow-core -p batchflow` runs
   everything that does not."
2. Consider gating them behind a `#[cfg_attr(not(feature = "docker-tests"), ignore)]`
   so the default local run is green and the CI lane opts in. The tradeoff is
   that an opt-in test is a test that gets forgotten — so if this is done, the
   CI lane must enable it explicitly and loudly.

**Benefit:** a new contributor's first `cargo test` succeeds, which is a
surprisingly large factor in whether they submit a second patch.

---

<a id="test-4"></a>
### TEST-4 — No fuzzing of the one place untrusted bytes enter the engine

**Severity:** Low · **Effort:** S · **Files:** `context.rs`, `execution.rs`

The `ExecutionContext` / `JobParameters` deserialization path is the only place
the engine parses data it did not produce — and it parses it *from the metadata
store*, which the security design explicitly treats as potentially hostile
(that is the stated reason `ContextValue` is a closed enum).

The closed enum makes gadget attacks structurally impossible, which is the
important half. The remaining question is robustness: what does
`from_json::<ExecutionContext>(value)` do with deeply nested, enormous, or
malformed JSON? `serde_json` has a recursion limit, so the answer is probably
"errors cleanly" — but "probably" is what fuzzing is for, and the payoff is a
one-time investment.

**Recommendation.** A `cargo-fuzz` target:

```rust
fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<batchflow_core::ExecutionContext>(data);
    let _ = serde_json::from_slice::<batchflow_core::JobParameters>(data);
});
```

Run it once for an hour locally and commit any corpus findings; a nightly CI
lane is optional. This is low severity precisely because the design already
removed the dangerous class — this is checking the remaining boring one.

---

<a id="test-5"></a>
### TEST-5 — Tests live inline with the code they test, which is correct but has grown expensive

**Severity:** Low · **Files:** `chunk.rs` (1,638 lines, ~1,200 of them tests)

`chunk.rs` is 1,638 lines, of which the implementation is roughly 420. The rest
is two `#[cfg(test)]` modules. Same pattern in `launcher.rs` (930 lines, ~780
tests) and `job.rs` (641, ~300).

Inline unit tests are idiomatic Rust and give access to private items, which
several of these tests genuinely need (`ChunkConfig` is `pub(crate)`). **No
change is recommended to the pattern.** But the ratio means the primary source
file for the framework's most important function requires scrolling past 1,200
lines of tests to read its neighbours, and a reviewer diffing `chunk.rs` sees a
mixed diff.

**Recommendation.** If [ARCH-1](01-Architecture-and-API.md#arch-1) is taken and
`run_step` is decomposed, let the test modules follow the decomposition —
`chunk/read.rs`, `chunk/write.rs`, `chunk/scan.rs`, each with its own inline
tests. That gets the benefit without abandoning the idiom.
