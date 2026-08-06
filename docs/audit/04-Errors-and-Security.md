# Pass 7 — Error Handling Review

Reviewed: error hierarchy, propagation, context, `thiserror` usage, messages,
panics, `unwrap`/`expect`, recoverability, retry logic, diagnostics, logging.

## Baseline

This is the second-strongest pass in the audit. Specifically:

- **`BatchError::with_cleanup` (ADR-009)** is the right answer to a problem
  most Rust codebases get wrong. Rust has no `finally`, so the naïve
  `cleanup?;` in a lifecycle method discards the error it was cleaning up
  after. `with_cleanup` keeps both, makes the *original* the `source()` so
  classifier chains still work, and renders both in one `Display`. There are
  four independent tests for it across three modules.
- **`Cause` is boxed and erased but never stringified**, so
  `PostgresClassifier` can downcast back to `sqlx::Error` and read the
  SQLSTATE. The doc comment on `BatchError::read` et al. explicitly warns
  against `to_string()`. This is the decision that makes the whole classifier
  design work.
- **`SkipLimitExceeded` is a distinct variant** carrying the triggering item
  error as `source`, because "one bad row" and "this file is garbage" need
  different operator responses. Correct, and tested.
- **`ExecutionContextType` errors rather than returning `None`** on a type
  mismatch, because a garbled bookmark silently restarting from zero would
  duplicate every committed item. This is the load-bearing decision of
  `context.rs` and it has the load-bearing test.
- **`#[non_exhaustive]` on `BatchError`**, with the cost (wildcard arms)
  accepted explicitly.

---

<a id="err-1"></a>
### ERR-1 — The failure that ends a job is logged and returned but never persisted

**Severity:** High · **Effort:** M · **Files:** `launcher.rs:110-143`, `job.rs:235-261`, `migrations/`

```rust
let status = if outcome.is_ok() { BatchStatus::Completed } else { BatchStatus::Failed };
execution.set_status(status);
self.repository.update_execution(&execution).await;
```

The error is dropped on the floor as far as the metadata store is concerned.
`job_execution` has columns `id, instance_id, status, execution_context` —
nowhere to put it. Same for `step_execution`.

**Why it matters.** The metadata store is the framework's operator interface;
the whole restart design rests on "restart is emergent from what was
recorded". But the first question anyone asks a failed batch job is *why did it
fail*, and the store's answer is `FAILED`. The actual cause exists only in the
process's log stream, which:

- is retained for a different (usually shorter) period than the metadata,
- is not joinable to `execution_id` unless a tracing subscriber was installed,
- and is gone entirely for anyone querying the store from a different service
  (a dashboard, a CLI, an admin endpoint).

Spring Batch — the stated model — persists `exit_code` and `exit_message` for
exactly this reason.

**Recommendation.** Migration `0003`:

```sql
ALTER TABLE job_execution  ADD COLUMN exit_message TEXT;
ALTER TABLE step_execution ADD COLUMN exit_message TEXT;
```

and in the launcher/job:

```rust
let status = if outcome.is_ok() { BatchStatus::Completed } else { BatchStatus::Failed };
execution.set_status(status);
if let Err(ref error) = outcome {
    // The full chain, not just the top: the classifier's decision was made on
    // a nested cause and the operator needs to see the same thing it saw.
    execution.set_exit_message(render_chain(error));
}
```

with a bounded renderer — this is untrusted-ish text going into a database
column, and an error chain from a user's writer can be arbitrarily long:

```rust
/// The error and its causes, one per line, truncated to `MAX` bytes at a char
/// boundary. Bounded because the chain includes a user error whose `Display`
/// this crate does not control.
fn render_chain(error: &BatchError) -> String { /* ... */ }
```

**Effort:** M (2 days, including the Redis side and a conformance case).
**Benefit:** the store can answer the first question an operator asks. This is
the highest-value error-handling change available.

---

<a id="err-2"></a>
### ERR-2 — `BatchError::Repository` is a single variant covering four unrelated conditions

**Severity:** Medium · **Effort:** S · **Files:** `error.rs:35-37`, backends throughout

`BatchError::repository(...)` is constructed for:

- a genuine backend I/O failure (`sqlx::Error`, `redis::RedisError`),
- a serialization failure (`serde_json::Error` in `json()` / `from_json()`),
- **a programming error** — `"unknown execution {id:?}"`, `"unknown step
  execution {id:?}"`, `"unknown instance"` — which is an engine bug or a
  corrupted store, not a database problem,
- **data corruption** — `"negative counter {value} in step_execution"`,
  `"unknown status '{other}'"`.

A `Classifier` therefore cannot tell "the database is down, retry" from "this
row is corrupt, do not retry" — and `PostgresClassifier` correctly gives up on
both, because it downcasts to `sqlx::Error` and the corruption cases carry a
`String`. Which means corrupt-metadata failures get the same treatment as a
disk full, and neither can be distinguished by a user writing their own
classifier.

**Recommendation.** Split the non-I/O cases out, which is cheap because
`BatchError` is `#[non_exhaustive]`:

```rust
/// The metadata store returned something the engine cannot interpret: an
/// unknown status string, a negative counter, a row that should exist.
///
/// Distinct from [`Repository`](BatchError::Repository) because the response
/// differs: a backend failure may be transient and retryable, corrupt metadata
/// never is.
#[error("metadata store is inconsistent: {detail}")]
MetadataCorrupt { detail: String },
```

Reserve `Repository` for errors that carry a real backend `Cause` — which also
makes the doc comment on `Cause` ("preserved rather than stringified") true
across the board, where today ~15 `format!`-based `BatchError::repository`
calls violate it.

**Benefit:** classifiers become able to make the retry/fail decision the type
exists to enable, and the "never stringify a cause" rule stops having
exceptions.

---

<a id="err-3"></a>
### ERR-3 — `metrics::describe()` and `Prometheus` builder `expect` on library paths

**Severity:** Low · **Files:** `crates/batchflow-metrics/src/lib.rs:58-62`

```rust
PrometheusBuilder::new()
    .set_buckets_for_metric(full(CHUNK_DURATION), CHUNK_BUCKETS)
    .expect("chunk buckets are a non-empty constant")
```

The comment justifies it correctly — the only failure is an empty bucket list
and both lists are literal constants, so this is an assertion about this file,
not about caller input. **No change recommended.** Recorded so it is not
re-flagged; this is the correct use of `expect` in a library.

The one nearby item worth a second look is `install()`, which returns
`Result<_, BuildError>` — correct, since "a recorder is already installed" is a
real caller error.

---

<a id="err-4"></a>
### ERR-4 — Two log messages are the API, and nothing pins them

**Severity:** Low · **Files:** `chunk.rs:330, 357`, `job.rs:253`, `launcher.rs:134`

`crates/batchflow-core/src/tracing.rs` carefully publishes span names and field
keys as constants *and tests their values*, because they are a contract an
operator writes queries against. The **event messages** are not:

```rust
tracing::error!(error = %cleanup, "failed to record the terminal job status; the metadata store is now stale");
```

Meanwhile the tests match on those exact strings
(`events_named(&events, Level::ERROR, "rollback failed; the transaction is in an unknown state")`),
and the chunk module even has a test asserting two retry messages are
*distinct* so an operator grepping for one does not match the other — which is
an explicit acknowledgement that the strings are an interface.

**Recommendation.** Apply the same treatment the field names got: constants in
`tracing.rs` with a value-pinning test.

```rust
/// Emitted when a rollback fails, leaving the transaction in an unknown state.
pub const EVENT_ROLLBACK_FAILED: &str = "rollback failed; the transaction is in an unknown state";
```

Note the module already documents why field *names* must stay literals
(`tracing` takes them as identifiers) — that constraint does not apply to the
message, which is an ordinary expression. So this one can genuinely be a single
source of truth, unlike the fields.

**Effort:** XS. **Benefit:** consistency with the stated "the vocabulary is a
published contract" policy, and the tests stop carrying duplicate literals.

---

<a id="err-5"></a>
### ERR-5 — `RepeatStatus::Continuable` is an unbounded loop with a doc comment for a guard

**Severity:** Medium · **Effort:** S · **Files:** `tasklet.rs:22-28, 194-229`

```rust
/// The tasklet owes progress. Nothing bounds this loop — there is no
/// item count to bound it *with* ... so a tasklet that always returns
/// `Continuable` runs until the process is killed.
Continuable,
```

The disclosure is honest. The consequence is that a buggy tasklet — one whose
bookmark write is wrong, or whose termination condition has an off-by-one —
produces an infinite loop of `BEGIN … COMMIT` against the metadata store, at
whatever rate the tasklet's work allows. With `Unmanaged`, that is an infinite
loop of real side effects.

The chunk loop faced the identical problem (a reader that errors without
advancing) and solved it — the skip limit bounds it. The tasklet path has no
equivalent.

**Recommendation.** An optional pass budget, defaulting to unbounded so nothing
breaks:

```rust
impl<T> TaskletStep<T> {
    /// Fail the step after `max` `Continuable` passes.
    ///
    /// Unbounded by default, because a legitimate tasklet may genuinely have
    /// an unknown number of passes. Set it when the count *is* bounded: a
    /// tasklet that overruns its own bound is looping, and an infinite loop of
    /// committed passes is worse than a failed step.
    #[must_use]
    pub fn max_passes(mut self, max: NonZeroU32) -> Self { /* ... */ }
}
```

with a new `BatchError::TaskletPassLimitExceeded { limit }`, mirroring
`SkipLimitExceeded`.

This also composes with [ASYNC-1](03-Async-and-Concurrency.md#async-1): the
stop signal is checked between passes, so an operator gains a second way out.

**Benefit:** removes the only unbounded loop in the engine that has no
structural bound.

---

# Pass 11 — Security Review

Reviewed: panic safety, `unsafe`, FFI, DoS vectors, resource exhaustion,
unbounded queues, integer overflow, input validation, path traversal,
deserialization, configuration validation.

## Baseline

- **`#![forbid(unsafe_code)]` on all five library crates.** The only `unsafe`
  is a counting `GlobalAlloc` in `tests/allocations.rs`, which is a test target
  and never linked into user code. The file says so.
- **No FFI, no `libc`, no C dependencies** beyond what `sqlx`/`redis` pull.
- **No path handling at all** in the framework — no `Path`, no file I/O, no
  `include_str!` outside an example. Path traversal is structurally
  inapplicable.
- **`ContextValue` is a closed three-variant enum**, explicitly to remove the
  deserialization-gadget class that produced Spring Batch's CVEs. The doc
  comment names the attack and instructs future maintainers not to add a
  variant that can hold arbitrary data. This is the best security decision in
  the codebase and it is a *design* decision, not a mitigation.
- **No SQL injection surface**: every query is `sqlx::query!` with bind
  parameters, validated at compile time against the committed `.sqlx/` cache.
  Table and column names are literals.
- **Lua injection**: all six Redis scripts are `const &str`; user data reaches
  them only through `ARGV`/`KEYS`. Correct.

---

<a id="sec-1"></a>
### SEC-1 — A panic in user code leaves the instance permanently unlaunchable

**Severity:** Critical · **Effort:** S (1 day) · **Files:** `job.rs:232`, `launcher.rs:108`

```rust
let outcome = step.run(&mut context, &mut commit).instrument(span).await;
```

If a user's `ItemProcessor`, `ItemReader`, `ItemWriter` or `Tasklet` panics —
an `unwrap()` on a `None`, an index out of bounds, an arithmetic overflow in a
debug build — the panic unwinds straight through `Job::run` and
`JobLauncher::run`. Neither `update_step_execution(Failed)` nor
`update_execution(Failed)` runs.

The store is left with `job_execution.status = 'STARTED'`. The launcher's gate
at `launcher.rs:78-83` then refuses every subsequent launch of that instance
with `JobExecutionAlreadyRunning` — **forever**, until a human calls
`abandon_execution`.

**Why this is the highest-severity finding alongside ASYNC-1.** It converts an
ordinary, recoverable application bug (a panic on one bad row) into an
operational incident requiring manual database intervention, and it does so
*silently*: the panic message goes to stderr, the store shows a job apparently
still running, and the next scheduled tick is refused with a message about a
process that no longer exists.

It is also the most likely bug to actually occur: `unwrap()` in a data
transform is the single most common panic in Rust application code.

**Recommendation.** A panic boundary around user code, converting to a
`BatchError` so the existing terminal-status machinery runs:

```rust
use futures::FutureExt;   // or a hand-rolled AssertUnwindSafe wrapper

let outcome = AssertUnwindSafe(step.run(&mut context, &mut commit))
    .catch_unwind()
    .instrument(span)
    .await
    .unwrap_or_else(|panic| {
        let detail = panic_message(&panic);   // downcast &str / String
        tracing::error!(step = %name, panic = %detail, "step panicked");
        Err(BatchError::Process(format!("step panicked: {detail}").into()))
    });
```

Three things must be said about this in the rustdoc, because
`catch_unwind` is not free of nuance:

1. **`AssertUnwindSafe` is a real assertion here**, and it is justified: a
   panicking step's `&mut` state is discarded (the step is dropped, the job
   fails), and the metadata store's consistency is guaranteed by the
   transaction that rolled back, not by the step's in-memory state.
2. **It does not work under `panic = "abort"`.** Document that a user who sets
   `panic = "abort"` in their release profile loses this protection and must
   rely on external supervision plus `abandon_execution`.
3. **A panic is still a bug.** The boundary exists to keep one bad row from
   wedging an instance, not to make panicking an acceptable error channel. Say
   so, and keep the `ERROR`-level event.

**Alternative if `catch_unwind` is judged too invasive:** a stale-execution
reaper built on the heartbeat from
[PROD-2](06-Production-and-OSS.md#prod-2) — an execution whose `last_updated`
is older than a threshold is automatically abandonable. That is strictly more
work and it leaves a window; take the panic boundary as well, not instead.

**Benefit:** removes the most likely path from "an application bug" to "a
production incident requiring a DBA".

---

<a id="sec-2"></a>
### SEC-2 — The Redis backend loses metadata under eviction and cannot run on Redis Cluster

**Severity:** High · **Effort:** M (2 days) · **Files:** `crates/batchflow-redis/src/lib.rs`

Two independent problems in one backend.

**(a) Eviction is silent metadata loss.** The module docs are emphatic about
durability — *"Run Redis with `appendonly yes` and `appendfsync always`"* —
and are right to be. But they say nothing about `maxmemory-policy`. Under the
common `allkeys-lru` or `allkeys-random`, Redis will evict *any* key under
memory pressure, including `batchflow:step:*` — which holds the bookmark.

The failure is worse than losing a write: `HGETALL` on an evicted key returns
an empty hash, and `step_from` handles missing fields with defaults:

```rust
execution.set_status(status_from(fields.get("status").map_or(STARTING, String::as_str))?);
execution.set_execution_context(decode_context(fields.get("context").map_or("{}", String::as_str))?);
```

So an evicted step execution reads back as `STARTING` with an **empty
bookmark** — indistinguishable from a step that has never run. A restart then
re-reads the input from the beginning and re-writes every already-committed
item. That is precisely the duplicate-delivery failure the framework exists to
prevent, and it is silent.

The `map_or` defaults are the aggravating factor: they turn "this key is gone"
into "this key says start from scratch".

**(b) Redis Cluster is impossible, not merely untested.** Two reasons:

1. `FIND_OR_CREATE_INSTANCE` declares `KEYS[1]` (the lookup key) and `KEYS[2]`
   (`batchflow:seq`). Those hash to different slots, so Cluster rejects the
   script with `CROSSSLOT`.
2. Every script constructs additional keys *inside* the script from `ARGV`:
   ```lua
   redis.call('HSET', ARGV[3] .. ':instance:' .. id, ...)
   redis.call('RPUSH', ARGV[4] .. ':instance:' .. instance_id .. ':step:' .. ARGV[2], id)
   ```
   Undeclared keys are rejected in Cluster mode.

**(c) Key construction is ambiguous under a colon in a name.**

```rust
fn instance_lookup_key(job_name: &str, parameters: &str) -> String {
    format!("{NS}:lookup:{job_name}:{parameters}")
}
```

A job named `nightly:eu` with parameters `X` produces the same key as a job
named `nightly` with parameters `eu:X`. Contrived, but the same pattern applies
to `instance_step_key(instance_id, step_name)` — and *there* a collision means
two different steps share a bookmark index, so a restart resumes the wrong
step at the wrong position. Not attacker-controlled in a typical deployment,
but it is a correctness property that depends on user-supplied strings not
containing a delimiter, which is exactly the class of assumption that fails.

**Recommendation.**

1. **Document `maxmemory-policy noeviction` with the same emphasis as
   `appendfsync always`**, and verify it on connect:

   ```rust
   /// Fails if the server is configured to evict keys.
   ///
   /// The metadata *is* the exactly-once guarantee, so an evicted bookmark is
   /// silent duplicate delivery on the next restart — not a cache miss.
   pub async fn verify_configuration(&self) -> Result<(), BatchError> {
       let policy: HashMap<String, String> =
           redis::cmd("CONFIG").arg("GET").arg("maxmemory-policy")
               .query_async(&mut self.conn()).await.map_err(re)?;
       match policy.get("maxmemory-policy").map(String::as_str) {
           Some("noeviction") | None => Ok(()),
           Some(other) => Err(BatchError::repository(format!(
               "redis maxmemory-policy is '{other}'; batchflow metadata must not be \
                evictable — set 'noeviction' or use batchflow-postgres"))),
       }
   }
   ```

   Call it from `connect()`. `CONFIG GET` is unavailable on some managed
   offerings, so treat an error as a warning rather than a hard failure — but
   *log* it, do not swallow it.

2. **Stop defaulting missing fields.** A `HGETALL` that returns an empty map
   for a key the engine believes exists is corruption, not a fresh record:

   ```rust
   if fields.is_empty() {
       return Err(BatchError::repository(format!(
           "step execution {id} is missing from redis; it was evicted or the \
            store was flushed — restart safety cannot be guaranteed")));
   }
   ```

   This converts silent duplicate delivery into a loud failure, which is
   strictly the right trade for this framework.

3. **Hash-tag every key** so a future Cluster deployment is at least
   single-slot: `{batchflow}:instance:{id}`. Declare every key in `KEYS[]`.
   Until then, **state in the module docs that Redis Cluster is not supported**
   — it currently reads as untested rather than impossible.

4. **Escape or reject delimiters** in `job_name` and `step_name`, or hash the
   composite rather than concatenating it.

**Benefit:** removes a silent-data-loss path from a shipped backend and
replaces an unstated incompatibility with a documented one.

---

<a id="sec-3"></a>
### SEC-3 — No input validation on job names, step names or parameter size

**Severity:** Medium · **Effort:** S · **Files:** `job.rs:151`, `step.rs:177`, `execution.rs:60`

`Job::new(name: impl Into<String>)`, `ChunkStep::new(name: impl Into<String>)`
and `JobParameters::with(key, value)` accept anything. Three concrete
consequences:

1. **Postgres btree index limit.** `UNIQUE (job_name, parameters)` on a JSONB
   column: a btree index entry cannot exceed roughly 2704 bytes (⅓ of an 8 KB
   page). A job whose parameters carry a large value — a list of file paths, a
   query, a serialized filter — fails at `INSERT` with
   `index row size N exceeds btree version 4 maximum 2704`, surfaced to the
   user as an opaque `BatchError::Repository`. There is no guidance anywhere
   that parameters are size-constrained.
2. **Redis key length.** No bound; the serialized parameters go straight into
   the key. Long keys are memory-resident and inflate every keyspace scan.
3. **Metric cardinality.** `job` and `step` are metric label values.
   `metrics.rs` is careful to state that labels must be *"bounded,
   author-written values"* and that ids must never be labels — but nothing
   enforces that a job name is bounded, and a user generating names from data
   (`format!("import-{customer_id}")`) blows up the Prometheus registry with
   one series per customer, permanently. That is a classic metrics-cardinality
   outage and the framework currently makes it easy.

**Recommendation.** Validate at construction, where the error is attributable:

```rust
/// Names are used as metric label values and as metadata-store keys, so they
/// must be bounded and stable. Rejecting here is far cheaper than discovering
/// it as a Prometheus cardinality incident or an opaque index-size error.
const MAX_NAME: usize = 128;
const MAX_PARAMETERS_BYTES: usize = 2048;   // stays inside Postgres's btree limit
```

`Job::new` / `ChunkStep::new` / `TaskletStep::new` cannot return `Result`
without a breaking change, so either:

- take the break now, pre-1.0 (preferred — `Job::try_new` plus a
  `debug_assert!` is a half-measure), or
- validate in `JobLauncher::run` and `find_or_create_instance`, which is where
  the value first has to be durable anyway.

At minimum, **document the limits** in the `JobParameters` and `Job::builder`
rustdoc, and state explicitly that a job name must not be derived from data.

**Benefit:** turns three opaque, late, environment-specific failures into one
early, attributable one.

---

<a id="sec-4"></a>
### SEC-4 — Unbounded resource commitments driven by configuration

**Severity:** Low · **Files:** `chunk.rs:99`, `fault.rs:162`, `tasklet.rs`

Three configuration values with no upper bound and no validation:

| Value | Unbounded consequence |
|---|---|
| `chunk_size: NonZeroUsize` | `Vec::with_capacity` — `usize::MAX` **panics** ("capacity overflow") rather than erroring. Large-but-plausible values are an unbounded memory commitment; see [MEM-1](02-Rust-Performance-Memory.md#mem-1). |
| `skip_limit: usize` | `usize::MAX` disables the only bound on `read_chunk`'s loop, so a reader that errors without advancing spins forever. The doc comment for `read_chunk` names the skip limit as exactly this bound. |
| `RepeatStatus::Continuable` | No pass bound at all — see [ERR-5](#err-5). |

None of these is attacker-controlled in a normal deployment (they are
author-written constants), which is why this is Low rather than High. But they
are the three places where a configuration mistake becomes a hang or an abort
rather than an error, and a framework that is otherwise this careful about
making invalid states unrepresentable should close them.

**Recommendation.** Document each as a precondition, and add
`debug_assert!`s. A hard cap is probably wrong — the right chunk size is
workload-dependent — but "this panics rather than erroring" belongs in the
rustdoc for `ChunkStep::new`.

---

<a id="sec-5"></a>
### SEC-5 — No supply-chain checking in CI

**Severity:** Medium · **Effort:** XS · **Files:** `.github/workflows/ci.yml`

The CI is genuinely good — `--all-features` everywhere, an MSRV matrix per
crate tier, `-D warnings` in `RUSTFLAGS` with an explanation of why that only
affects first-party crates, a scheduled `latest-deps` lane that buys back the
signal `--locked` costs. That is more thought than most projects give CI.

What is absent: **no `cargo audit`, no `cargo deny`, no dependency review.**
The workspace pulls `sqlx`, `redis`, `tokio`, `serde`, `metrics`,
`tokio-cron-scheduler` and their trees — 66 crates with the `cron` feature, per
the CHANGELOG. A published RUSTSEC advisory in any of them is currently
invisible to this project until a human notices.

For a crate that will be a *dependency* of other people's batch pipelines, a
downstream security team will look for this and its absence is a real adoption
blocker.

**Recommendation.**

```yaml
  deny:
    name: cargo-deny
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: EmbarkStudios/cargo-deny-action@v2
        with:
          command: check advisories bans licenses sources
```

plus a `deny.toml` that pins the dual MIT/Apache-2.0 licence policy the project
already states, and bans duplicate major versions (which will catch the
`opentelemetry`-style split ADR-010 is explicitly worried about).

Run it on the same weekly schedule as `latest-deps`, plus on PRs — advisories
appear between releases, so a push-only trigger misses them.

**Benefit:** the cheapest item on this list, and the one most likely to be
asked about by an adopter.
