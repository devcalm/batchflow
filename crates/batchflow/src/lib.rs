#![doc = include_str!("../README.md")]
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
#![warn(missing_debug_implementations)]

// User-facing API tests live in this crate's `tests/`, not in `batchflow-core`.
//
// A doctest in `batchflow-core` can name every one of that crate's own
// dependencies, because rustdoc passes `--extern` for all of them. So it will
// happily compile code a real user cannot: `async-trait` is a dependency of
// `batchflow-core`, not of anyone who depends on it. Write
// `use async_trait::async_trait;` in a core doctest and it passes while every
// user breaks.
//
// This crate depends on exactly one thing, so that mistake is not available
// here — the guarantee is structural rather than a matter of how the example
// was written. See `tests/facade.rs`.

/// Everything from [`batchflow_core`], re-exported so callers write
/// `batchflow::Job` rather than `batchflow::batchflow_core::Job`.
///
/// The core crate's root is an explicit, curated `pub use` list, so this glob
/// has no accidental surface to leak.
pub use batchflow_core::*;

/// The core crate under its own name.
///
/// Prefer the re-exports at this crate's root; this path exists so that code
/// written against `batchflow::batchflow_core::…` keeps compiling.
#[doc(hidden)]
pub use batchflow_core;
