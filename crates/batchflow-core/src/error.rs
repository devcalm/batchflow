use crate::{BatchStatus, JobExecutionId, JobInstanceId};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BatchError {
    #[error("Read failed: {0}")]
    Read(String),

    #[error("Write failed: {0}")]
    Write(String),

    #[error("Process failed: {0}")]
    Process(String),

    #[error("Repository failed: {0}")]
    Repository(String),

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

    #[error("execution context key '{key}' holds a {actual}, expected {expected}")]
    ExecutionContextType {
        key: String,
        expected: &'static str,
        actual: &'static str,
    },
}
