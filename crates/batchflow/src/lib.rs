//! # BatchFlow
//!
//! A production-grade batch processing framework for Rust, inspired by
//! [Spring Batch](https://spring.io/projects/spring-batch) but designed around
//! idiomatic Rust: traits, ownership, `Result`-based error handling, and an
//! async-first execution engine.
//!
//! This is the **facade crate**: the one users depend on. It re-exports
//! [`batchflow_core`]; the storage backends (`batchflow-postgres`,
//! `batchflow-redis`), the Prometheus wiring (`batchflow-metrics`) and the
//! scheduler adapters (`batchflow-scheduler`) are separate crates you add
//! alongside it, because each carries a dependency tree — and in two cases an
//! MSRV — that nobody should pay for a backend they do not use.
//!
//! ## Status
//!
//! `0.1.0` — the first usable release. A job runs end to end, commits per chunk,
//! restarts from a durable bookmark, and retries or skips by classified error.
//! Pre-1.0, so the API may still change; see the CHANGELOG for the known
//! limitations.
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
//! A [`Tasklet`] needs no macro at all — it is a plain trait with a native
//! `async fn`, since nothing about it has to be `dyn`. The check below is that
//! the four names it takes are all re-exported, and that [`Unmanaged`] adapts a
//! tasklet to a job's transaction type exactly as it does a writer:
//!
//! [`Tasklet`]: batchflow_core::Tasklet
//! [`Unmanaged`]: batchflow_core::Unmanaged
//!
//! ```
//! use batchflow::batchflow_core::{
//!     BatchError, ContextValue, ExecutionContext, Job, RepeatStatus,
//!     StepContribution, Tasklet, TaskletStep, Unmanaged,
//! };
//!
//! /// Archives one file per pass, so each pass commits and a restart resumes.
//! struct Archive { total: i64 }
//!
//! impl Tasklet for Archive {
//!     async fn execute(
//!         &mut self,
//!         context: &mut ExecutionContext,
//!         contribution: &mut StepContribution,
//!     ) -> Result<RepeatStatus, BatchError> {
//!         let done = context.get_long("archived")?.unwrap_or(0) + 1;
//!         context.put("archived", ContextValue::Long(done));
//!         contribution.increment_write(1);
//!
//!         Ok(if done >= self.total {
//!             RepeatStatus::Finished
//!         } else {
//!             RepeatStatus::Continuable
//!         })
//!     }
//! }
//!
//! // The annotation is load-bearing, and not only here: anything built from
//! // `Unmanaged` implements its step trait for *every* `Tx`, so nothing in the
//! // expression pins one. Naming the job's type is what chooses it — `Job` is
//! // `Job<()>`, and a Postgres job is `Job<PgTx>` by the same one word.
//! let job: Job = Job::builder("nightly")
//!     .step(TaskletStep::new("archive", Unmanaged(Archive { total: 3 })))
//!     .build();
//!
//! assert_eq!(job.name(), "nightly");
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
