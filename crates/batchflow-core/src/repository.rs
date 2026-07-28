use crate::{
    BatchError, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobParameters,
    StepExecution,
};
use std::future::Future;

pub trait JobRepository: Send + Sync {
    fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<JobInstance, BatchError>> + Send;

    fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<Option<JobInstance>, BatchError>> + Send;

    fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<JobExecution, BatchError>> + Send;

    fn update_execution(
        &self,
        execution: &JobExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<Option<JobExecution>, BatchError>> + Send;

    /// Mint a [`StepExecution`] for `step_name` under `job_execution_id`.
    ///
    /// Only the repository can assign a `StepExecutionId`, which is why a
    /// `Step` reports a `StepContribution` instead of building one of these.
    fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> impl Future<Output = Result<StepExecution, BatchError>> + Send;

    fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Every step execution under `job_execution_id`, in the order the steps
    /// ran. Phase 9's restart reads this to decide which steps to skip, so the
    /// ordering is part of the contract, not an accident of storage.
    fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> impl Future<Output = Result<Vec<StepExecution>, BatchError>> + Send;
}
