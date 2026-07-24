//! # BatchFlow Core
//!
//! Core traits and the execution engine for [BatchFlow](https://crates.io/crates/batchflow),
//! a production-grade batch processing framework for Rust, inspired by
//! [Spring Batch](https://spring.io/projects/spring-batch) but designed around
//! idiomatic Rust: traits, ownership, `Result`-based error handling, and an
//! async-first execution engine.
//!
//! This crate holds the framework's heart — the `ItemReader` / `ItemProcessor`
//! / `ItemWriter` abstractions, the chunk-oriented execution engine, and the
//! metadata model. Storage backends, observability, and I/O adapters live in
//! sibling crates.
//!
//! ## Status
//!
//! **Under active development.** The API is unstable and being built out phase
//! by phase. Not yet usable as a batch framework.
#![forbid(unsafe_code)]

// Phase 2 lands here: `BatchError`, the `ItemReader` / `ItemProcessor` /
// `ItemWriter` traits (associated types — see ADR-003), and `read_chunk`.
