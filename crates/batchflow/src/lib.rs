//! # BatchFlow
//!
//! A production-grade batch processing framework for Rust, inspired by
//! [Spring Batch](https://spring.io/projects/spring-batch) but designed around
//! idiomatic Rust: traits, ownership, `Result`-based error handling, and an
//! async-first execution engine.
//!
//! This is the **facade crate**: the one users depend on. It re-exports
//! [`batchflow_core`] and, over time, will feature-gate the storage backends,
//! observability, and I/O adapters so a single dependency pulls in a coherent
//! framework.
//!
//! ## Status
//!
//! **Under active development.** This `0.0.0` release reserves the crate name
//! while the framework is built in the open. It is **not yet usable**.
//!
//! ## This crate is where user-facing API tests belong
//!
//! A doctest in `batchflow-core` can name every one of that crate's own
//! dependencies, because rustdoc passes `--extern` for all of them. So it will
//! happily compile code a real user cannot — `async-trait` is a dependency of
//! `batchflow-core`, not of anyone who depends on it.
//!
//! A doctest in `batchflow-core` catches a missing re-export only if it happens
//! to import through the re-export path; write `use async_trait::async_trait;`
//! there instead and it passes while every user breaks. This crate depends on
//! exactly one thing, `batchflow-core`, so that mistake is not available —
//! `async-trait` is not in its dependency graph at all. The guarantee here is
//! structural rather than a matter of how the example was written.
//!
//! The check below is that `#[async_trait]` — required to implement [`Step`] —
//! is reachable without taking a direct dependency on the `async-trait` crate.
//!
//! [`Step`]: batchflow_core::Step
//!
//! ```
//! use batchflow::batchflow_core::{
//!     BatchError, ExecutionContext, Step, StepCommit, async_trait,
//! };
//!
//! struct Cleanup;
//!
//! #[async_trait]
//! impl Step for Cleanup {
//!     fn name(&self) -> &str {
//!         "cleanup"
//!     }
//!
//!     async fn run(
//!         &mut self,
//!         _context: &mut ExecutionContext,
//!         _commit: &mut dyn StepCommit,
//!     ) -> Result<(), BatchError> {
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## Observability
//!
//! The engine emits through the [`metrics`] and [`tracing`] facades and records
//! nothing until the application installs a recorder and a subscriber. Both are
//! process-global, so neither is installed on the application's behalf.
//!
//! Prometheus has a wiring crate, `batchflow-metrics`, because there are real
//! decisions to hold: histogram buckets, and describing metrics at the right
//! moment. **Tracing deliberately has no such crate** (ADR-010) — an exporter
//! would treble the dependency tree and pin an `opentelemetry` major version
//! into this API, for glue the application can write in fifteen lines:
//!
//! ```ignore
//! use tracing_subscriber::prelude::*;
//!
//! // Your choice of exporter, your choice of `opentelemetry` version.
//! let tracer = my_otlp_tracer()?;
//!
//! tracing_subscriber::registry()
//!     .with(tracing_opentelemetry::layer().with_tracer(tracer))
//!     .with(tracing_subscriber::fmt::layer())
//!     .init();
//! ```
//!
//! What the engine does provide is a stable vocabulary to query against —
//! [`batchflow_core::tracing`] for span names and field keys,
//! [`batchflow_core::metrics`] for metric names and label keys. The split
//! between them is deliberate: metrics carry no execution ids (one label value
//! per run is one time series per run, kept forever), spans carry them all
//! (correlating one run is the question spans exist to answer).
//!
//! [`metrics`]: https://docs.rs/metrics
//! [`tracing`]: https://docs.rs/tracing
#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[doc(inline)]
pub use batchflow_core;
