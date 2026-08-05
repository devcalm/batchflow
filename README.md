# BatchFlow

A production-grade **batch processing framework for Rust**, inspired by
[Spring Batch](https://spring.io/projects/spring-batch) but designed around
idiomatic Rust: traits, ownership, `Result`-based error handling, and an
async-first execution engine.

BatchFlow owns the **orchestration layer** — execution engine, chunk
processing, fault tolerance, restartability, and metadata persistence — while
integrating mature crates from the Rust ecosystem for everything else
(async runtime, database access, serialization, tracing, metrics).

## Status

`0.1.0` — the first usable release. A job runs end to end, commits per chunk,
restarts from a durable bookmark, and retries or skips by classified error,
against either the in-memory store or PostgreSQL.

Pre-1.0, so the API may still change; breaking changes will be a minor version
bump and are recorded in [CHANGELOG.md](CHANGELOG.md), which also lists the
known limitations — chunk scanning, the launcher's gate race, and the absence
of parallel steps.

```toml
[dependencies]
batchflow = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Examples

Four runnable programs, from a ten-item in-memory job to a CSV loaded into
Postgres through a real transaction — see [docs/Examples.md](docs/Examples.md).

```sh
cargo run -p batchflow --example hello_batch
```

## What it does

- Chunk-oriented processing (`read → process → write`) where the commit
  interval **is** the transaction boundary: a chunk's rows, its counters and
  its reader bookmark become durable together or not at all
- Trait-based `ItemReader` / `ItemProcessor` / `ItemWriter`
- Durable job metadata behind a `JobRepository` trait (in-memory, PostgreSQL)
- Restart: skip completed steps, reopen the reader at the last committed chunk
- Retry and skip driven by a `Classifier` over your own error type
- Metrics (`metrics`/Prometheus) and tracing (`tracing`/OpenTelemetry)
- Integration with external schedulers (cron, Kubernetes CronJobs) rather than
  a bespoke scheduler

Not yet: parallel or partitioned steps, chunk scanning, a Redis backend,
scheduling adapters.

## Crates

| Crate | What it is | MSRV |
|---|---|---|
| [`batchflow`](crates/batchflow) | The facade — depend on this | 1.85 |
| [`batchflow-core`](crates/batchflow-core) | Traits and execution engine | 1.85 |
| [`batchflow-postgres`](crates/batchflow-postgres) | PostgreSQL metadata store | 1.94 |
| [`batchflow-metrics`](crates/batchflow-metrics) | Prometheus exporter | 1.85 |

## Documentation

- [Guide](docs/Guide.md) — concepts and recipes
- [Examples](docs/Examples.md) — four runnable programs
- [Performance](docs/Performance.md) — measured overhead and how to pick a chunk size
- [Architecture](docs/Architecture.md) · [Requirements](docs/Requirements.md)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
