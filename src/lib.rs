//! # BatchFlow
//!
//! A production-grade batch processing framework for Rust, inspired by
//! [Spring Batch](https://spring.io/projects/spring-batch) but designed around
//! idiomatic Rust: traits, ownership, `Result`-based error handling, and an
//! async-first execution engine.
//!
//! BatchFlow owns the **orchestration layer** — the execution engine, chunk
//! processing, fault tolerance, restartability, and metadata — while
//! integrating mature crates from the Rust ecosystem for everything else.
//!
//! ## Status
//!
//! **Under active development.** This `0.0.0` release reserves the crate name
//! while the design and API are being built out in the open. It is **not yet
//! usable** as a batch framework. Follow along as the phases land.
//!
//! See the project documentation for the architecture, requirements, and the
//! phased implementation roadmap.
#![forbid(unsafe_code)]

// Intentionally empty for the reservation release. Core traits and the
// execution engine land in subsequent versions.
