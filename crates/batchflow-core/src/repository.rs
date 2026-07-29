use crate::{
    BatchError, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobParameters,
    StepExecution,
};
use std::future::Future;

pub trait JobRepository: Send + Sync {
    /// The backend's transaction. `()` for stores that have none — those
    /// degrade to at-least-once, which is honest rather than hidden.
    type Tx: Send;

    fn begin(&self) -> impl Future<Output = Result<Self::Tx, BatchError>> + Send;

    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), BatchError>> + Send;

    fn rollback(&self, tx: Self::Tx) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Update inside `tx`, so the counters and bookmark become durable with the
    /// chunk's data rather than after it.
    fn update_step_execution_in(
        &self,
        tx: &mut Self::Tx,
        step_execution: &StepExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

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

    /// Mark `execution_id` as [`BatchStatus::Abandoned`](crate::BatchStatus),
    /// releasing its `JobInstance` so it can be launched again.
    ///
    /// An operator action: it asserts the process is dead, which the repository
    /// cannot verify.
    ///
    /// # Errors
    ///
    /// - [`BatchError::CannotAbandon`] if the execution is `Completed`.
    /// - [`BatchError::Repository`] if `execution_id` is unknown.
    fn abandon_execution(
        &self,
        execution_id: JobExecutionId,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Mint a [`StepExecution`] for `step_name` under `job_execution_id`.
    fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> impl Future<Output = Result<StepExecution, BatchError>> + Send;

    fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// The most recent execution of `step_name` under any attempt at
    /// `instance_id`, or `None` if this step has never run for that instance.
    ///
    /// Callers must resolve this **before** minting the current attempt's
    /// record with [`create_step_execution`](Self::create_step_execution), or
    /// the answer is that record: `Starting`, with an empty context.
    fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> impl Future<Output = Result<Option<StepExecution>, BatchError>> + Send;

    /// Every step execution under `job_execution_id`, in the order the steps
    /// ran. A restart lists only the steps it did not skip.
    fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> impl Future<Output = Result<Vec<StepExecution>, BatchError>> + Send;
}
