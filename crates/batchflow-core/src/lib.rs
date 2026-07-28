//! # BatchFlow Core
//!
//! Core traits and execution engine for BatchFlow.
#![forbid(unsafe_code)]

mod chunk;
mod error;
mod execution;
mod item;
mod job;
mod launcher;
mod step;

mod memory;
mod repository;
#[cfg(test)]
mod testing;

pub use chunk::{process_chunk, read_chunk, run_step};
pub use error::BatchError;
pub use execution::{
    BatchStatus, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobParameter,
    JobParameters, StepExecution, StepExecutionId,
};
pub use item::{ItemProcessor, ItemReader, ItemWriter};
pub use job::{HasSteps, Job, JobBuilder, NoSteps};
pub use launcher::JobLauncher;
pub use memory::InMemoryJobRepository;
pub use repository::JobRepository;
pub use step::{ChunkStep, Step, StepContribution};
