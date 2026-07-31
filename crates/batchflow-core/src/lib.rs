//! # BatchFlow Core
//!
//! Core traits and execution engine for BatchFlow.
#![forbid(unsafe_code)]

mod chunk;
mod error;
mod execution;
mod fault;
mod item;
mod job;
mod launcher;
mod step;

mod classifier;
mod context;
mod memory;
mod repository;
#[cfg(test)]
mod testing;

pub mod metrics;

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
pub use step::{ChunkStep, Step, StepCommit, StepContribution};
