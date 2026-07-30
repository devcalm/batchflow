use crate::{BatchStatus, JobExecutionId, JobInstanceId};
use thiserror::Error;

/// The underlying cause carried by the wrapping variants.
///
/// Boxed and erased so core stays free of backend error types, but preserved
/// rather than stringified: a [`Classifier`](crate::Classifier) decides retry
/// vs. skip vs. fail by downcasting this back to the concrete error.
pub type Cause = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BatchError {
    #[error("Read failed: {0}")]
    Read(#[source] Cause),

    #[error("Write failed: {0}")]
    Write(#[source] Cause),

    #[error("Process failed: {0}")]
    Process(#[source] Cause),

    #[error("Repository failed: {0}")]
    Repository(#[source] Cause),

    #[error("job instance '{job_name}' ({instance_id:?}) is already complete")]
    JobInstanceAlreadyComplete {
        job_name: String,
        instance_id: JobInstanceId,
    },

    #[error(
        "job '{job_name}' already has a running execution ({execution_id:?}); \
         if that process is dead, abandon it to unblock this instance"
    )]
    JobExecutionAlreadyRunning {
        job_name: String,
        execution_id: JobExecutionId,
    },

    #[error("cannot abandon execution {execution_id:?}: it is {status:?}")]
    CannotAbandon {
        execution_id: JobExecutionId,
        status: BatchStatus,
    },

    /// The step gave up because too many items were skipped.
    ///
    /// Distinct from the item error it wraps, because the operational response
    /// differs: one bad row is a data-quality nit, five hundred means the input
    /// is wrong and re-running will not help. The offending error is the
    /// `source`, so the detail is not lost.
    #[error("skip limit of {limit} exceeded")]
    SkipLimitExceeded {
        limit: usize,
        #[source]
        cause: Cause,
    },

    #[error("execution context key '{key}' holds a {actual}, expected {expected}")]
    ExecutionContextType {
        key: String,
        expected: &'static str,
        actual: &'static str,
    },
}

/// Constructors for the wrapping variants.
///
/// `impl Into<Cause>` accepts a `&str`, a `String` or any concrete error, so
/// callers never spell the box. Prefer passing the error itself — a
/// `to_string()` here is the information a `Classifier` later needs and cannot
/// recover.
impl BatchError {
    pub fn read(cause: impl Into<Cause>) -> Self {
        Self::Read(cause.into())
    }

    pub fn write(cause: impl Into<Cause>) -> Self {
        Self::Write(cause.into())
    }

    pub fn process(cause: impl Into<Cause>) -> Self {
        Self::Process(cause.into())
    }

    pub fn repository(cause: impl Into<Cause>) -> Self {
        Self::Repository(cause.into())
    }
}
