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
known limitations — the launcher's gate race and the absence of parallel
steps.

```toml
[dependencies]
batchflow = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Examples

Five runnable programs, from a ten-item in-memory job to a CSV loaded into
Postgres through a real transaction — see [docs/Examples.md](docs/Examples.md).

```sh
cargo run -p batchflow --example hello_batch
```

## What it does

- Chunk-oriented processing (`read → process → write`) where the commit
  interval **is** the transaction boundary: a chunk's rows, its counters and
  its reader bookmark become durable together or not at all
- Trait-based `ItemReader` / `ItemProcessor` / `ItemWriter`
- Tasklets for steps that are one unit of work rather than a loop, with an
  optional commit point per pass so they restart like any other step
- Durable job metadata behind a `JobRepository` trait (in-memory, PostgreSQL, Redis)
- Restart: skip completed steps, reopen the reader at the last committed chunk
- Retry and skip driven by a `Classifier` over your own error type, including
  optional chunk scanning to isolate a poison row on write failure
- Graceful stop at a committed chunk boundary, so a rolling deploy leaves a
  restartable job rather than a wedged one
- Metrics (`metrics`/Prometheus) and tracing (`tracing`/OpenTelemetry)
- Adapters for external schedulers (cron, Kubernetes CronJobs) rather than a
  bespoke scheduler — the framework classifies what a firing produced and lets
  something else decide when to fire

Not yet: parallel or partitioned steps, built-in CSV/JSON/SQL readers.

## Crates

| Crate | What it is | MSRV |
|---|---|---|
| [`batchflow`](crates/batchflow) | The facade — depend on this | 1.85 |
| [`batchflow-core`](crates/batchflow-core) | Traits and execution engine | 1.85 |
| [`batchflow-postgres`](crates/batchflow-postgres) | PostgreSQL metadata store | 1.94 |
| [`batchflow-redis`](crates/batchflow-redis) | Redis metadata store (needs `appendfsync always`, `noeviction`) | 1.88 |
| [`batchflow-metrics`](crates/batchflow-metrics) | Prometheus exporter | 1.85 |
| [`batchflow-scheduler`](crates/batchflow-scheduler) | Trigger semantics; `cron` feature for in-process cron | 1.85 |

## Documentation

- [Guide](docs/Guide.md) — concepts and recipes
- [Operations](docs/Operations.md) — deploying, sizing, stopping, and what to alert on
- [Examples](docs/Examples.md) — five runnable programs
- [Performance](docs/Performance.md) — measured overhead and how to pick a chunk size
- [Architecture](docs/Architecture.md) · [Requirements](docs/Requirements.md)
- [Audit](docs/audit/00-Summary.md) — engineering review and what has been acted on

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers the two non-obvious prerequisites:
the backend test suites need Docker, and `DATABASE_URL` must stay unset so that
`sqlx` validates against the committed offline cache. Participation is governed
by the [Code of Conduct](CODE_OF_CONDUCT.md); vulnerabilities go through
[SECURITY.md](SECURITY.md), not the issue tracker.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
