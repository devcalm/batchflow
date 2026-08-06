# BatchFlow

A production-grade **batch processing framework for Rust**, inspired by
[Spring Batch](https://spring.io/projects/spring-batch) but designed around
idiomatic Rust: traits, ownership, `Result`-based error handling, and an
async-first execution engine.

This is the **facade crate** — the one to depend on. It re-exports
`batchflow-core`; storage backends, the Prometheus exporter and the scheduler
adapters are separate crates you add alongside it.

## Status

`0.1.0` — the first usable release. A job runs end to end, commits per chunk,
restarts from a durable bookmark, and retries or skips by classified error,
against either the in-memory store or PostgreSQL.

Pre-1.0, so the API may still change; breaking changes are a minor version bump
and are recorded in the
[CHANGELOG](https://github.com/devcalm/batchflow/blob/main/CHANGELOG.md),
which also lists the known limitations.

```toml
[dependencies]
batchflow = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## What it does

- Chunk-oriented processing (`read → process → write`) where the commit
  interval **is** the transaction boundary: a chunk's rows, its counters and
  its reader bookmark become durable together or not at all
- Trait-based `ItemReader` / `ItemProcessor` / `ItemWriter`
- Tasklets for steps that are one unit of work rather than a loop, with a
  commit point per pass so they restart like any other step
- Durable job metadata behind a `JobRepository` trait (in-memory, PostgreSQL,
  Redis)
- Restart: skip completed steps, reopen the reader at the last committed chunk
- Retry and skip driven by a `Classifier` over your own error type, including
  optional chunk scanning to isolate a poison row on write failure
- Graceful stop at a committed chunk boundary, so a rolling deploy leaves a
  restartable job rather than a wedged one
- Metrics (`metrics`/Prometheus) and tracing (`tracing`/OpenTelemetry)
- Adapters for external schedulers (cron, Kubernetes CronJobs) rather than a
  bespoke scheduler

Not yet: parallel or partitioned steps, built-in CSV/JSON/SQL readers.

## Example

```rust
use batchflow::{
    BatchError, ChunkStep, InMemoryJobRepository, ItemProcessor, ItemReader,
    ItemWriter, Job, JobLauncher, JobParameter, JobParameters, Unmanaged,
};
use std::num::NonZeroUsize;

struct Counter { next: u32, last: u32 }

impl ItemReader for Counter {
    type Item = u32;
    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        if self.next > self.last { return Ok(None); }
        let item = self.next;
        self.next += 1;
        Ok(Some(item))
    }
}

struct Double;

impl ItemProcessor for Double {
    type In = u32;
    type Out = u32;
    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        Ok(Some(item * 2))
    }
}

struct Print;

impl ItemWriter for Print {
    type Item = u32;
    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        println!("{items:?}");
        Ok(())
    }
}

# async fn run() -> Result<(), BatchError> {
let launcher = JobLauncher::new(InMemoryJobRepository::new());

let mut job = Job::builder("nightly")
    .step(ChunkStep::new(
        "double",
        Counter { next: 1, last: 10 },
        Double,
        Unmanaged(Print),
        NonZeroUsize::new(3).unwrap(),
    ))
    .build();

let parameters = JobParameters::new()
    .with("date", JobParameter::String("2026-08-06".into()));

launcher.run(&mut job, &parameters).await?;
# Ok(())
# }
```

Five runnable programs, from this ten-item job to a CSV loaded into Postgres
through a real transaction:

```sh
cargo run -p batchflow --example hello_batch
```

## Companion crates

| Crate | What it is | MSRV |
|---|---|---|
| [`batchflow`](https://crates.io/crates/batchflow) | The facade — depend on this | 1.85 |
| [`batchflow-core`](https://crates.io/crates/batchflow-core) | Traits and execution engine | 1.85 |
| [`batchflow-postgres`](https://crates.io/crates/batchflow-postgres) | PostgreSQL metadata store | 1.94 |
| [`batchflow-redis`](https://crates.io/crates/batchflow-redis) | Redis metadata store (needs `appendfsync always` and `noeviction`) | 1.88 |
| [`batchflow-metrics`](https://crates.io/crates/batchflow-metrics) | Prometheus exporter | 1.85 |
| [`batchflow-scheduler`](https://crates.io/crates/batchflow-scheduler) | Trigger semantics; `cron` feature for in-process cron | 1.85 |

## Documentation

- [Guide](https://github.com/devcalm/batchflow/blob/main/docs/Guide.md) — concepts and recipes
- [Operations](https://github.com/devcalm/batchflow/blob/main/docs/Operations.md) — deploying, sizing and alerting
- [Examples](https://github.com/devcalm/batchflow/blob/main/docs/Examples.md) — runnable programs
- [Performance](https://github.com/devcalm/batchflow/blob/main/docs/Performance.md) — measured overhead and how to pick a chunk size
- [Architecture](https://github.com/devcalm/batchflow/blob/main/docs/Architecture.md)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
