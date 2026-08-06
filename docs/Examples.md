# BatchFlow examples

Five runnable programs, ordered so each one adds a single idea. They import
through the facade (`batchflow`) rather than `batchflow-core` wherever they can,
so they compile against the same dependency graph a user has — an example that
could name the framework's own dependencies would happily compile code nobody
else can. The two that live outside the facade do so because they need a crate
the facade does not depend on, and each imports exactly the pair an application
would.

| Example | Run | Needs Docker | Adds |
|---|---|---|---|
| `hello_batch` | `cargo run -p batchflow --example hello_batch` | no | the whole path: launcher → job → chunk step → metadata |
| `restart_demo` | `cargo run -p batchflow --example restart_demo` | no | `ItemReader::open`/`update` — resuming at the last committed chunk |
| `retry_skip_demo` | `cargo run -p batchflow --example retry_skip_demo` | no | a `Classifier` over your own error type; retry, skip, skip limit |
| `external_trigger` | `cargo run -p batchflow-scheduler --example external_trigger` | no | what a cron entry actually does — a run key per tick, and the two refusals that are not errors |
| `csv_to_postgres` | `cargo run -p batchflow-postgres --example csv_to_postgres` | **yes** | a real `TransactionalWriter` — rows, counters and bookmark commit together |

`csv_to_postgres` starts its own throwaway Postgres via testcontainers, so it
needs nothing but a running Docker daemon. The five lines that do so are
marked; an application replaces them with one `PgPool::connect`.

## What each one is actually showing

**`hello_batch`** — reads `1..=10`, doubles, filters multiples of three, prints
each chunk, then reads the counters back out of the repository. The number to
look at is the last chunk: it reads two items and writes one. `chunk_size` is a
**read** interval, so a filtered item shrinks the write; a *skipped* item does
not, because a skip must not quietly shrink the transaction the commit interval
promised.

**`restart_demo`** — the writer fails once, the job is launched twice with the
same `JobParameters`, and the second launch resumes. Watch the persisted
counters: attempt 1 read eight items and is credited with four, because
uncommitted work is uncounted. The two attempts sum to exactly one clean pass.

**`retry_skip_demo`** — defines a `FeedError` and a `FeedClassifier` mapping it
onto `Retry` / `Skip` / `Fail`. Three scenarios: a transient write failure
retried in a fresh transaction, malformed rows skipped, and a skip limit
exceeded. Note that retry re-attempts the **whole chunk** — `write(&[Item])`
names N items, so there is nothing to single out — while skip is per-item
because `read` and `process` are.

**`external_trigger`** — the same job fired four times: the scheduled tick, a
re-fire after the node was replaced, a second replica firing the same tick, and
tomorrow's tick. Only the first and last do any work, and **none of the four is
an error**. This is the point of the example: `JobLauncher::run` reports the two
refusals as `BatchError`s, which is right for a caller that asked for a run and
wrong for a schedule, and `trigger` is what re-classifies them. Watch the last
two lines — each date recorded exactly one execution, so the refusals created no
metadata at all, which is why `batchflow_triggers_total` has to exist separately.

**`csv_to_postgres`** — loads `people.csv` into a real table. One row is
malformed and is skipped during processing; one chunk inserts its rows and
*then* fails, so the rollback has real work to do. After the failure the table
holds only the committed chunk, the counters agree with it, and the bookmark
points at the same place — one transaction held all three. The second launch
resumes and finishes the file, and no person is loaded twice.

## Examples are not tests

CI **compiles** examples (`cargo clippy --workspace --all-targets` covers them —
verified by planting a lint and watching it fail) but never **runs** them. An
example is documentation that happens to be type-checked.

Where a property has to be enforced rather than demonstrated, it lives in a
test instead: `crates/batchflow/tests/restart.rs` asserts the restart semantics
that `restart_demo` illustrates, and it runs under `cargo test --workspace`. It
sits in the facade crate for the same reason the examples do — that is the only
place a missing `pub use` shows up as a red test.

The `assert!`s inside `restart_demo` and `csv_to_postgres` are there for a human
running them by hand. They are not a substitute for the suite.
