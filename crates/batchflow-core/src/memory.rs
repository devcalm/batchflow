use crate::repository::JobRepository;
use crate::{BatchError, JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobParameters};
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
}

impl JobRepository for InMemoryJobRepository {
    async fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<JobInstance, BatchError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BatchError::Repository(e.to_string()))?;
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
            .map_err(|e| BatchError::Repository(e.to_string()))?;

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
            .map_err(|e| BatchError::Repository(e.to_string()))?;

        if !inner.instances.values().any(|i| i.id() == instance_id) {
            return Err(BatchError::Repository(format!(
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
            .map_err(|e| BatchError::Repository(e.to_string()))?;

        match inner
            .executions
            .iter_mut()
            .find(|e| e.id() == execution.id())
        {
            Some(slot) => {
                *slot = execution.clone();
                Ok(())
            }
            None => Err(BatchError::Repository(format!(
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
            .map_err(|e| BatchError::Repository(e.to_string()))?;

        Ok(inner
            .executions
            .iter()
            .rev()
            .find(|e| e.instance_id() == instance_id)
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchStatus, JobParameter, JobParameters};

    fn params(pairs: &[(&str, &str)]) -> JobParameters {
        pairs.iter().fold(JobParameters::new(), |acc, (k, v)| {
            acc.with(*k, JobParameter::String((*v).into()))
        })
    }

    // ---- instance identity (FR-4.2) ----

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

    #[test]
    fn repository_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryJobRepository>();
    }
}
