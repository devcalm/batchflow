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

    /// Every attempt at `instance_id`, oldest first — so the last element is
    /// what [`last_execution`](Self::last_execution) returns.
    ///
    /// Answers "what did this instance do across all of its attempts?", which
    /// `last_execution` alone cannot: once a second attempt exists, the first
    /// one's record is otherwise unreachable.
    ///
    /// Unpaged deliberately. An instance's executions are its retry attempts,
    /// which the domain bounds at a handful; it is listing a *job's instances*
    /// that would need paging, and that is a different query.
    fn executions(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<Vec<JobExecution>, BatchError>> + Send;

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

/// So a store that is not [`Clone`] can still be shared.
///
/// [`JobLauncher`](crate::JobLauncher) takes its repository by value.
/// [`PostgresJobRepository`] is `Clone` because a `PgPool` is an `Arc` inside,
/// but [`InMemoryJobRepository`](crate::InMemoryJobRepository) is not — and
/// without this impl, wrapping it in an `Arc` to share it produces something
/// that is no longer a `JobRepository`.
///
/// [`PostgresJobRepository`]: https://docs.rs/batchflow-postgres
impl<R: JobRepository> JobRepository for std::sync::Arc<R> {
    type Tx = R::Tx;

    fn begin(&self) -> impl Future<Output = Result<Self::Tx, BatchError>> + Send {
        (**self).begin()
    }

    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), BatchError>> + Send {
        (**self).commit(tx)
    }

    fn rollback(&self, tx: Self::Tx) -> impl Future<Output = Result<(), BatchError>> + Send {
        (**self).rollback(tx)
    }

    fn update_step_execution_in(
        &self,
        tx: &mut Self::Tx,
        step_execution: &StepExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send {
        (**self).update_step_execution_in(tx, step_execution)
    }

    fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<JobInstance, BatchError>> + Send {
        (**self).find_or_create_instance(job_name, parameters)
    }

    fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<Option<JobInstance>, BatchError>> + Send {
        (**self).find_instance(job_name, parameters)
    }

    fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<JobExecution, BatchError>> + Send {
        (**self).create_execution(instance_id)
    }

    fn update_execution(
        &self,
        execution: &JobExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send {
        (**self).update_execution(execution)
    }

    fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<Option<JobExecution>, BatchError>> + Send {
        (**self).last_execution(instance_id)
    }

    fn executions(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<Vec<JobExecution>, BatchError>> + Send {
        (**self).executions(instance_id)
    }

    fn abandon_execution(
        &self,
        execution_id: JobExecutionId,
    ) -> impl Future<Output = Result<(), BatchError>> + Send {
        (**self).abandon_execution(execution_id)
    }

    fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> impl Future<Output = Result<StepExecution, BatchError>> + Send {
        (**self).create_step_execution(job_execution_id, step_name)
    }

    fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send {
        (**self).update_step_execution(step_execution)
    }

    fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> impl Future<Output = Result<Option<StepExecution>, BatchError>> + Send {
        (**self).last_step_execution(instance_id, step_name)
    }

    fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> impl Future<Output = Result<Vec<StepExecution>, BatchError>> + Send {
        (**self).step_executions(job_execution_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::{InMemoryJobRepository, Job, JobLauncher, JobParameters};
    use std::sync::Arc;

    /// `InMemoryJobRepository` is not `Clone`, so before the blanket impl this
    /// was the one way to share it — and it did not compile.
    #[tokio::test]
    async fn a_launcher_can_own_a_shared_repository() {
        let repository = Arc::new(InMemoryJobRepository::new());
        let launcher = JobLauncher::new(Arc::clone(&repository));

        let mut job: Job = Job::new("nightly", vec![]);
        let execution = launcher.run(&mut job, &JobParameters::new()).await.unwrap();

        // The other handle observes what the launcher recorded.
        use crate::JobRepository;
        assert!(
            repository
                .last_execution(execution.instance_id())
                .await
                .unwrap()
                .is_some()
        );
    }
}
