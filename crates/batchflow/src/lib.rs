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
#![forbid(unsafe_code)]

#[doc(inline)]
pub use batchflow_core;
