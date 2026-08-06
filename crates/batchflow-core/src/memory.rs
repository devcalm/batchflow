use crate::repository::JobRepository;
use crate::{
    BatchError, BatchStatus, JobExecution, JobExecutionId, JobInstance, JobInstanceId,
    JobParameters, StepExecution, StepExecutionId,
};
use std::collections::HashMap;
use std::sync::Mutex;

/// A [`JobRepository`] held in memory, for tests and for trying things out.
///
/// Has no transactions, so `Tx = ()` and every writer is effectively
/// unmanaged - which is why a transaction abstraction must never be validated
/// against it. Not durable: everything is lost when the process exits, so a
/// restart across processes needs a real backend.
#[derive(Debug, Default)]
pub struct InMemoryJobRepository {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    next_id: i64,
    instances: HashMap<(String, JobParameters), JobInstance>,
    executions: Vec<JobExecution>,
    /// Kept flat and joined by `job_execution_id` rather than nested inside
    /// `JobExecution` — the same shape the SQL backend will have.
    step_executions: Vec<StepExecution>,
}

impl InMemoryJobRepository {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The guard, or a repository error.
    ///
    /// One helper rather than twelve copies of the same `map_err`. The message
    /// is written out instead of `PoisonError::to_string()` because that
    /// renders the poisoning, not the panic that caused it — and the crate's
    /// own rule is that a cause is preserved, never stringified. There is
    /// nothing here to preserve: the panic is long gone, and what the caller
    /// needs to know is that this store's contents may be half-updated.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Inner>, BatchError> {
        self.inner.lock().map_err(|_| {
            BatchError::repository(
                "in-memory repository lock is poisoned: a previous caller panicked while \
                 holding it, so the store may be half-updated",
            )
        })
    }
}

impl JobRepository for InMemoryJobRepository {
    /// No transactions: this store cannot roll back, so a writer enlisted here
    /// gets at-least-once. Per ADR-007 the abstraction is validated against
    /// Postgres, never against this.
    type Tx = ();

    async fn begin(&self) -> Result<(), BatchError> {
        Ok(())
    }

    async fn commit(&self, _tx: ()) -> Result<(), BatchError> {
        Ok(())
    }

    async fn rollback(&self, _tx: ()) -> Result<(), BatchError> {
        Ok(())
    }

    async fn update_step_execution_in(
        &self,
        _tx: &mut (),
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        self.update_step_execution(step_execution).await
    }

    async fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<JobInstance, BatchError> {
        let mut inner = self.lock()?;
        let key = (job_name.to_string(), parameters.clone());

        if let Some(existing) = inner.instances.get(&key) {
            return Ok(existing.clone());
        }
        inner.next_id += 1;
        let job = JobInstance::new(
            JobInstanceId::new(inner.next_id),
            job_name,
            parameters.clone(),
        );
        inner.instances.insert(key, job.clone());
        Ok(job)
    }

    async fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<Option<JobInstance>, BatchError> {
        let inner = self.lock()?;

        let key = (job_name.to_string(), parameters.clone());

        if let Some(existing) = inner.instances.get(&key) {
            return Ok(Some(existing.clone()));
        }

        Ok(None)
    }

    async fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<JobExecution, BatchError> {
        let mut inner = self.lock()?;

        if !inner.instances.values().any(|i| i.id() == instance_id) {
            return Err(BatchError::repository(format!(
                "Instance {:?} not found",
                instance_id
            )));
        }

        inner.next_id += 1;
        let execution = JobExecution::new(JobExecutionId::new(inner.next_id), instance_id);
        inner.executions.push(execution.clone());
        Ok(execution)
    }

    async fn update_execution(&self, execution: &JobExecution) -> Result<(), BatchError> {
        let mut inner = self.lock()?;

        match inner
            .executions
            .iter_mut()
            .find(|e| e.id() == execution.id())
        {
            Some(slot) => {
                *slot = execution.clone();
                Ok(())
            }
            None => Err(BatchError::repository(format!(
                "unknown execution {:?}",
                execution.id()
            ))),
        }
    }

    async fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Option<JobExecution>, BatchError> {
        let inner = self.lock()?;

        Ok(inner
            .executions
            .iter()
            .rev()
            .find(|e| e.instance_id() == instance_id)
            .cloned())
    }

    async fn executions(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Vec<JobExecution>, BatchError> {
        let inner = self.lock()?;

        Ok(inner
            .executions
            .iter()
            .filter(|e| e.instance_id() == instance_id)
            .cloned()
            .collect())
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        let mut inner = self.lock()?;

        match inner.executions.iter_mut().find(|e| e.id() == execution_id) {
            Some(slot) => {
                if slot.status() == BatchStatus::Completed {
                    return Err(BatchError::CannotAbandon {
                        execution_id,
                        status: slot.status(),
                    });
                }

                slot.set_status(BatchStatus::Abandoned);
                Ok(())
            }
            None => Err(BatchError::repository(format!(
                "unknown execution {execution_id:?}"
            ))),
        }
    }

    async fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> Result<StepExecution, BatchError> {
        let mut inner = self.lock()?;

        if !inner.executions.iter().any(|e| e.id() == job_execution_id) {
            return Err(BatchError::repository(format!(
                "unknown job execution {job_execution_id:?}"
            )));
        }

        inner.next_id += 1;
        let step_execution = StepExecution::new(
            StepExecutionId::new(inner.next_id),
            job_execution_id,
            step_name,
        );
        inner.step_executions.push(step_execution.clone());
        Ok(step_execution)
    }

    async fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        let mut inner = self.lock()?;

        match inner
            .step_executions
            .iter_mut()
            .find(|s| s.id() == step_execution.id())
        {
            Some(slot) => {
                *slot = step_execution.clone();
                Ok(())
            }
            None => Err(BatchError::repository(format!(
                "unknown step execution {:?}",
                step_execution.id()
            ))),
        }
    }

    async fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> Result<Option<StepExecution>, BatchError> {
        let inner = self.lock()?;

        // `.rev()`: step executions are ordered by insertion, so the last match
        // is the most recent attempt.
        Ok(inner
            .step_executions
            .iter()
            .rev()
            .find(|step| {
                step.step_name() == step_name
                    && inner.executions.iter().any(|execution| {
                        execution.id() == step.job_execution_id()
                            && execution.instance_id() == instance_id
                    })
            })
            .cloned())
    }

    async fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Vec<StepExecution>, BatchError> {
        let inner = self.lock()?;

        Ok(inner
            .step_executions
            .iter()
            .filter(|s| s.job_execution_id() == job_execution_id)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Auto-traits are SemVer-visible and can be lost silently, so they are
    /// asserted rather than assumed. Everything else about this type is checked
    /// by the shared conformance suite below.
    #[test]
    fn repository_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryJobRepository>();
    }
}

/// The in-memory store's half of the shared contract. The identical list runs
/// against PostgreSQL in `batchflow-postgres/tests/conformance.rs`.
#[cfg(test)]
mod conformance_suite {
    use super::InMemoryJobRepository;

    async fn setup() -> ((), InMemoryJobRepository) {
        ((), InMemoryJobRepository::new())
    }

    crate::job_repository_conformance!(setup());
}
