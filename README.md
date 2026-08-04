# BatchFlow

A production-grade **batch processing framework for Rust**, inspired by
[Spring Batch](https://spring.io/projects/spring-batch) but designed around
idiomatic Rust: traits, ownership, `Result`-based error handling, and an
async-first execution engine.

BatchFlow owns the **orchestration layer** — execution engine, chunk
processing, fault tolerance, restartability, and metadata persistence — while
integrating mature crates from the Rust ecosystem for everything else
(async runtime, database access, serialization, tracing, metrics).

## ⚠️ Status: under active development

This `0.0.0` release **reserves the crate name** while the framework is being
designed and built in the open. **It is not yet usable.** The API is unstable
and will change.

## Examples

Four runnable programs, from a ten-item in-memory job to a CSV loaded into
Postgres through a real transaction — see [docs/Examples.md](docs/Examples.md).

```sh
cargo run -p batchflow --example hello_batch
```

## Planned capabilities

- Chunk-oriented processing (`read → process → write`) with configurable commit intervals
- Trait-based `ItemReader` / `ItemProcessor` / `ItemWriter` abstractions
- Durable job metadata with pluggable storage backends (in-memory, PostgreSQL, Redis)
- Restartability with checkpoint/bookmark semantics
- Retry and skip policies with error classification
- Metrics (`metrics`/Prometheus) and tracing (`tracing`/OpenTelemetry)
- Integration with external schedulers (cron, Kubernetes CronJobs) rather than a bespoke scheduler

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
