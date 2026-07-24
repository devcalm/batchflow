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
#![forbid(unsafe_code)]

#[doc(inline)]
pub use batchflow_core;
