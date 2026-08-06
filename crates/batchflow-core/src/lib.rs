//! # BatchFlow Core
//!
//! Core traits and execution engine for BatchFlow.
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod chunk;
mod error;
mod execution;
mod fault;
mod item;
mod job;
mod launcher;
mod panic;
mod step;
mod stop;
mod tasklet;

mod classifier;
mod context;
mod memory;
#[cfg(test)]
mod properties;
mod repository;
#[cfg(test)]
mod testing;

// Available to core's own tests without the feature, and to backend crates
// with it. `mod testing` is `#[cfg(test)]` and therefore invisible outside this
// crate - which is exactly the trap this module must not fall into.
#[cfg(any(test, feature = "conformance"))]
pub mod conformance;

pub mod metrics;
pub mod tracing;

/// Re-exported so implementing [`Step`] needs no direct `async-trait` dependency.
pub use async_trait::async_trait;
pub use classifier::{Classifier, ErrorAction, FailFast};
pub use context::{ContextValue, ExecutionContext};
pub use error::{BatchError, Cause};
pub use execution::{
    BatchStatus, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobParameter,
    JobParameters, StepExecution, StepExecutionId,
};
pub use fault::{FaultTolerance, ItemDisposition, RetryPolicy};
pub use item::{ItemProcessor, ItemReader, ItemWriter, TransactionalWriter, Unmanaged};
pub use job::{HasSteps, Job, JobBuilder, NoSteps};
pub use launcher::JobLauncher;
pub use memory::InMemoryJobRepository;
pub use repository::JobRepository;
pub use step::{ChunkStep, Step, StepCommit, StepContribution, StepIdentity};
pub use stop::StopSignal;
pub use tasklet::{RepeatStatus, Tasklet, TaskletStep, TransactionalTasklet};
