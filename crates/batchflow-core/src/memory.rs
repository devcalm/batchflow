use crate::repository::JobRepository;
use crate::{
    BatchError, BatchStatus, JobExecution, JobExecutionId, JobInstance, JobInstanceId,
    JobParameters, StepExecution, StepExecutionId,
};
use std::collections::HashMap;
use std::sync::Mutex;

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
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;
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
        let inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
        let inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

        Ok(inner
            .executions
            .iter()
            .rev()
            .find(|e| e.instance_id() == instance_id)
            .cloned())
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
        let inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
        let inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::repository(e.to_string()))?;

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
    use crate::{BatchStatus, JobParameter, JobParameters, StepContribution};

    fn params(pairs: &[(&str, &str)]) -> JobParameters {
        pairs.iter().fold(JobParameters::new(), |acc, (k, v)| {
            acc.with(*k, JobParameter::String((*v).into()))
        })
    }

    /// Step executions are joined to a job execution by foreign key, so the
    /// parent row has to exist before any of them can be created.
    async fn open_execution(repo: &InMemoryJobRepository) -> JobExecution {
        let instance = repo
            .find_or_create_instance("nightly", &params(&[("date", "d")]))
            .await
            .unwrap();

        repo.create_execution(instance.id()).await.unwrap()
    }

    #[tokio::test]
    async fn identical_parameters_resolve_to_the_same_instance() {
        let repo = InMemoryJobRepository::default();
        let p = params(&[("date", "2026-07-27")]);

        let a = repo.find_or_create_instance("nightly", &p).await.unwrap();
        let b = repo.find_or_create_instance("nightly", &p).await.unwrap();

        assert_eq!(a.id(), b.id()); // one instance, not two
    }

    #[tokio::test]
    async fn different_parameters_resolve_to_different_instances() {
        let repo = InMemoryJobRepository::default();

        let a = repo
            .find_or_create_instance("nightly", &params(&[("date", "2026-07-27")]))
            .await
            .unwrap();
        let b = repo
            .find_or_create_instance("nightly", &params(&[("date", "2026-07-28")]))
            .await
            .unwrap();

        assert_ne!(a.id(), b.id());
    }

    #[tokio::test]
    async fn parameter_insertion_order_does_not_affect_identity() {
        let repo = InMemoryJobRepository::default();

        let a = repo
            .find_or_create_instance("nightly", &params(&[("date", "d"), ("region", "eu")]))
            .await
            .unwrap();
        let b = repo
            .find_or_create_instance("nightly", &params(&[("region", "eu"), ("date", "d")]))
            .await
            .unwrap();

        assert_eq!(a.id(), b.id());
    }

    #[tokio::test]
    async fn same_parameters_under_different_job_names_are_different_instances() {
        let repo = InMemoryJobRepository::default();
        let p = params(&[("date", "2026-07-27")]);

        let a = repo.find_or_create_instance("nightly", &p).await.unwrap();
        let b = repo.find_or_create_instance("hourly", &p).await.unwrap();

        assert_ne!(a.id(), b.id());
    }

    #[tokio::test]
    async fn find_instance_returns_none_when_never_created() {
        let repo = InMemoryJobRepository::default();

        let found = repo
            .find_instance("nightly", &params(&[("date", "d")]))
            .await
            .unwrap();

        assert!(found.is_none());
    }

    // ---- executions ----

    /// The restart shape: one instance, several attempts.
    #[tokio::test]
    async fn an_instance_can_have_several_distinct_executions() {
        let repo = InMemoryJobRepository::default();
        let instance = repo
            .find_or_create_instance("nightly", &params(&[("date", "d")]))
            .await
            .unwrap();

        let first = repo.create_execution(instance.id()).await.unwrap();
        let second = repo.create_execution(instance.id()).await.unwrap();

        assert_ne!(
            first.id(),
            second.id(),
            "each attempt needs its own identity"
        );
        assert_eq!(first.instance_id(), instance.id());
        assert_eq!(second.instance_id(), instance.id());
    }

    #[tokio::test]
    async fn create_execution_rejects_an_unknown_instance() {
        let repo = InMemoryJobRepository::default();

        let result = repo.create_execution(JobInstanceId::new(999)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn update_execution_persists_a_status_change() {
        let repo = InMemoryJobRepository::default();
        let instance = repo
            .find_or_create_instance("nightly", &params(&[("date", "d")]))
            .await
            .unwrap();

        let mut execution = repo.create_execution(instance.id()).await.unwrap();
        assert_eq!(execution.status(), BatchStatus::Starting);

        execution.set_status(BatchStatus::Completed);
        repo.update_execution(&execution).await.unwrap();

        let reloaded = repo.last_execution(instance.id()).await.unwrap().unwrap();
        assert_eq!(reloaded.status(), BatchStatus::Completed);
    }

    /// Regression: `update` must replace in place, not append. If it appended, the
    /// updated first execution would become the most recent one and this would fail.
    #[tokio::test]
    async fn update_execution_replaces_rather_than_appending() {
        let repo = InMemoryJobRepository::default();
        let instance = repo
            .find_or_create_instance("nightly", &params(&[("date", "d")]))
            .await
            .unwrap();

        let mut first = repo.create_execution(instance.id()).await.unwrap();
        let second = repo.create_execution(instance.id()).await.unwrap();

        first.set_status(BatchStatus::Failed);
        repo.update_execution(&first).await.unwrap();

        let last = repo.last_execution(instance.id()).await.unwrap().unwrap();
        assert_eq!(last.id(), second.id(), "updating must not reorder history");
    }

    #[tokio::test]
    async fn update_execution_rejects_an_unknown_execution() {
        let repo = InMemoryJobRepository::default();
        let orphan = JobExecution::new(JobExecutionId::new(999), JobInstanceId::new(999));

        assert!(repo.update_execution(&orphan).await.is_err());
    }

    /// Regression: `last_execution` must filter by instance. With two instances
    /// interleaved, the global last belongs to the *other* instance.
    #[tokio::test]
    async fn last_execution_is_scoped_to_its_instance() {
        let repo = InMemoryJobRepository::default();
        let nightly = repo
            .find_or_create_instance("nightly", &params(&[("date", "d")]))
            .await
            .unwrap();
        let hourly = repo
            .find_or_create_instance("hourly", &params(&[("date", "d")]))
            .await
            .unwrap();

        let nightly_exec = repo.create_execution(nightly.id()).await.unwrap();
        let hourly_exec = repo.create_execution(hourly.id()).await.unwrap();

        // `hourly_exec` was created last overall, so an unscoped impl returns it here.
        let found = repo.last_execution(nightly.id()).await.unwrap().unwrap();

        assert_eq!(found.id(), nightly_exec.id());
        assert_ne!(found.id(), hourly_exec.id());
    }

    #[tokio::test]
    async fn last_execution_returns_none_for_an_instance_with_no_attempts() {
        let repo = InMemoryJobRepository::default();
        let instance = repo
            .find_or_create_instance("nightly", &params(&[("date", "d")]))
            .await
            .unwrap();

        assert!(repo.last_execution(instance.id()).await.unwrap().is_none());
    }

    // ---- step executions ----

    #[tokio::test]
    async fn update_step_execution_persists_counters_and_status() {
        let repo = InMemoryJobRepository::default();
        let job_execution = open_execution(&repo).await;

        let mut step = repo
            .create_step_execution(job_execution.id(), "load")
            .await
            .unwrap();
        assert_eq!(step.status(), BatchStatus::Starting);

        let mut contribution = StepContribution::new();
        contribution.increment_read(10);
        contribution.increment_write(7);
        contribution.increment_filter(3);

        step.apply(&contribution);
        step.set_status(BatchStatus::Completed);
        repo.update_step_execution(&step).await.unwrap();

        let reloaded = repo.step_executions(job_execution.id()).await.unwrap();

        // Length pins replace-in-place; an appending impl leaves two rows.
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded[0].read_count(), 10);
        assert_eq!(reloaded[0].write_count(), 7);
        assert_eq!(reloaded[0].filter_count(), 3);
        assert_eq!(reloaded[0].status(), BatchStatus::Completed);
    }

    /// Two attempts at one instance must not see each other's steps — Phase 9
    /// decides what to skip from exactly this query.
    #[tokio::test]
    async fn step_executions_are_scoped_to_their_job_execution() {
        let repo = InMemoryJobRepository::default();
        let first = open_execution(&repo).await;
        let second = repo.create_execution(first.instance_id()).await.unwrap();

        repo.create_step_execution(first.id(), "load")
            .await
            .unwrap();
        repo.create_step_execution(second.id(), "load")
            .await
            .unwrap();

        let steps = repo.step_executions(first.id()).await.unwrap();

        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].job_execution_id(), first.id());
    }

    /// Order is part of the contract, not an accident of storage.
    #[tokio::test]
    async fn step_executions_come_back_in_the_order_they_ran() {
        let repo = InMemoryJobRepository::default();
        let execution = open_execution(&repo).await;

        for name in ["extract", "transform", "load"] {
            repo.create_step_execution(execution.id(), name)
                .await
                .unwrap();
        }

        let names: Vec<String> = repo
            .step_executions(execution.id())
            .await
            .unwrap()
            .iter()
            .map(|s| s.step_name().to_string())
            .collect();

        assert_eq!(names, ["extract", "transform", "load"]);
    }

    #[tokio::test]
    async fn create_step_execution_rejects_an_unknown_job_execution() {
        let repo = InMemoryJobRepository::default();

        let result = repo
            .create_step_execution(JobExecutionId::new(999), "load")
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn update_step_execution_rejects_an_unknown_step_execution() {
        let repo = InMemoryJobRepository::default();
        let orphan =
            StepExecution::new(StepExecutionId::new(999), JobExecutionId::new(999), "ghost");

        assert!(repo.update_step_execution(&orphan).await.is_err());
    }

    #[test]
    fn repository_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryJobRepository>();
    }
}
