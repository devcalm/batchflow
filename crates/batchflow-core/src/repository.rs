use crate::{
    BatchError, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobParameters,
    StepExecution,
};
use std::future::Future;

/// The metadata store: instances, executions, counters and bookmarks.
///
/// Everything restart depends on lives here, which is why restart is emergent
/// rather than a feature - the engine only has to read what was recorded.
///
/// Methods take `&self` and rely on interior mutability, never `&mut self`, so
/// one repository can be shared behind an `Arc` by a launcher and every step.
pub trait JobRepository: Send + Sync {
    /// The backend's transaction. `()` for stores that have none — those
    /// degrade to at-least-once, which is honest rather than hidden.
    type Tx: Send;

    /// Opens a transaction.
    fn begin(&self) -> impl Future<Output = Result<Self::Tx, BatchError>> + Send;

    /// Commits one. Takes `tx` by value, so a committed transaction cannot be
    /// reused.
    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Rolls one back. By value for the same reason - which is what makes
    /// "reuse a rolled-back transaction on retry" fail to compile.
    fn rollback(&self, tx: Self::Tx) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Update inside `tx`, so the counters and bookmark become durable with the
    /// chunk's data rather than after it.
    fn update_step_execution_in(
        &self,
        tx: &mut Self::Tx,
        step_execution: &StepExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Resolves `(job_name, parameters)` to an instance, creating it if new.
    ///
    /// One method rather than `exists` plus `create`: check-then-act is a TOCTOU
    /// race, and two schedulers firing at once would both win it.
    fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<JobInstance, BatchError>> + Send;

    /// Looks up an instance without creating one.
    fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<Option<JobInstance>, BatchError>> + Send;

    /// Opens a new attempt at `instance_id`.
    fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<JobExecution, BatchError>> + Send;

    /// Persists a changed execution, replacing it in place. Errors if the id is
    /// unknown, rather than silently inserting.
    fn update_execution(
        &self,
        execution: &JobExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// The most recent attempt at `instance_id`, which is what the FR-4.4 gate
    /// reads.
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

    /// Persists a changed step execution outside any transaction.
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
