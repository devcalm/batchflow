use crate::{BatchError, BatchStatus, Job, JobExecution, JobParameters, JobRepository};

/// Owns the `JobExecution` lifecycle: resolves identity, enforces FR-4.4, and
/// records what happened.
///
/// The launcher decides *whether* a job may run; the [`Job`] drives its steps.
/// Both reach the repository — a step never does.
pub struct JobLauncher<R> {
    repository: R,
}

impl<R: JobRepository> JobLauncher<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub fn repository(&self) -> &R {
        &self.repository
    }

    /// # Errors
    ///
    /// [`BatchError::JobInstanceAlreadyComplete`] if the gate rejects the
    /// launch; otherwise the step's own error if the job fails — the execution
    /// is persisted as `Failed` *before* that error propagates.
    pub async fn run(
        &self,
        job: &mut Job,
        parameters: &JobParameters,
    ) -> Result<JobExecution, BatchError> {
        let instance = self
            .repository
            .find_or_create_instance(job.name(), parameters)
            .await?;

        if let Some(last) = self.repository.last_execution(instance.id()).await?
            && last.status() == BatchStatus::Completed
        {
            return Err(BatchError::JobInstanceAlreadyComplete {
                job_name: job.name().into(),
                instance_id: instance.id(),
            });
        }

        let mut execution = self.repository.create_execution(instance.id()).await?;
        execution.set_status(BatchStatus::Started);
        self.repository.update_execution(&execution).await?;

        let outcome = job.run(execution.id(), &self.repository).await;

        let status = if outcome.is_ok() {
            BatchStatus::Completed
        } else {
            BatchStatus::Failed
        };

        execution.set_status(status);
        self.repository.update_execution(&execution).await?;

        outcome?;
        Ok(execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{CollectingWriter, EvenDoubler, FailingStep, LogStep, VecReader, nz};
    use crate::{ChunkStep, InMemoryJobRepository, JobParameter};

    fn params(date: &str) -> JobParameters {
        JobParameters::new().with("date", JobParameter::String(date.into()))
    }

    fn ok_job() -> Job {
        Job::new("nightly", vec![Box::new(LogStep)])
    }

    fn failing_job() -> Job {
        Job::new("nightly", vec![Box::new(FailingStep)])
    }

    /// Reload the last execution straight from the repository. Asserting on the
    /// value `run` returned would pass even if nothing were ever persisted.
    async fn reload_last(
        launcher: &JobLauncher<InMemoryJobRepository>,
        date: &str,
    ) -> JobExecution {
        let instance = launcher
            .repository()
            .find_instance("nightly", &params(date))
            .await
            .unwrap()
            .expect("a launch must create its instance");

        launcher
            .repository()
            .last_execution(instance.id())
            .await
            .unwrap()
            .expect("a launch must create its execution")
    }

    #[tokio::test]
    async fn a_successful_run_is_persisted_as_completed() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let mut job = ok_job();

        let execution = launcher.run(&mut job, &params("2026-07-28")).await.unwrap();

        assert_eq!(execution.status(), BatchStatus::Completed);

        // The returned value is convenience; the repository is the record.
        // Asserting on the reload is what proves it was actually persisted.
        let instance = launcher
            .repository()
            .find_instance("nightly", &params("2026-07-28"))
            .await
            .unwrap()
            .expect("launching must create the instance");
        let reloaded = launcher
            .repository()
            .last_execution(instance.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(reloaded.status(), BatchStatus::Completed);
    }

    #[tokio::test]
    async fn a_failing_job_is_persisted_as_failed() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let mut job = failing_job();

        let result = launcher.run(&mut job, &params("2026-07-28")).await;

        // 1. The step's own error reaches the caller unwrapped. Pinning the
        //    variant guards against a launcher that swallows the cause and
        //    substitutes one of its own.
        assert!(matches!(result, Err(BatchError::Process(_))));

        // 2. ...and, the actual point, it was RECORDED before propagating.
        assert_eq!(
            reload_last(&launcher, "2026-07-28").await.status(),
            BatchStatus::Failed
        );
    }

    /// The whole of Phase 7 in one test: launching resolves an instance, opens
    /// an execution, and leaves a counted, completed step execution joined to
    /// it — all reachable from the repository alone.
    #[tokio::test]
    async fn step_executions_are_recorded_under_the_job_execution() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let chunk_step = ChunkStep::new(
            "double-evens",
            VecReader::new(vec![1, 2, 3, 4]),
            EvenDoubler,
            CollectingWriter::new(),
            nz(2),
        );
        let mut job = Job::new("nightly", vec![Box::new(chunk_step)]);

        let execution = launcher.run(&mut job, &params("2026-07-28")).await.unwrap();

        let steps = launcher
            .repository()
            .step_executions(execution.id())
            .await
            .unwrap();

        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].step_name(), "double-evens");
        assert_eq!(steps[0].job_execution_id(), execution.id());
        assert_eq!(steps[0].read_count(), 4);
        assert_eq!(steps[0].write_count(), 2);
        assert_eq!(steps[0].filter_count(), 2);
        assert_eq!(steps[0].status(), BatchStatus::Completed);
    }

    #[tokio::test]
    async fn rerunning_a_completed_instance_is_rejected() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let mut first = ok_job();

        launcher
            .run(&mut first, &params("2026-07-28"))
            .await
            .unwrap();
        let completed_id = reload_last(&launcher, "2026-07-28").await.id();

        // Same name, same parameters => same instance, and it is already done.
        let mut second = ok_job();
        let result = launcher.run(&mut second, &params("2026-07-28")).await;

        assert!(matches!(
            result,
            Err(BatchError::JobInstanceAlreadyComplete { .. })
        ));

        // The gate must reject *before* opening an execution. Without this,
        // an impl that creates first and checks second would still pass.
        assert_eq!(
            reload_last(&launcher, "2026-07-28").await.id(),
            completed_id,
            "a rejected launch must not leave a new execution behind"
        );
    }

    #[tokio::test]
    async fn rerunning_a_failed_instance_is_allowed() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let mut failing = failing_job();

        assert!(
            launcher
                .run(&mut failing, &params("2026-07-28"))
                .await
                .is_err()
        );
        let failed_id = reload_last(&launcher, "2026-07-28").await.id();

        // Same instance, second attempt — the door Phase 9 walks through.
        let mut retry = ok_job();
        launcher
            .run(&mut retry, &params("2026-07-28"))
            .await
            .unwrap();

        let retried = reload_last(&launcher, "2026-07-28").await;
        assert_ne!(retried.id(), failed_id, "a retry needs its own execution");
        assert_eq!(retried.status(), BatchStatus::Completed);
    }

    /// FR-4.4 blocks a completed *instance*, not a job name. Otherwise a
    /// nightly job could never run twice.
    #[tokio::test]
    async fn a_completed_instance_does_not_block_other_parameters() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());

        let mut monday = ok_job();
        launcher
            .run(&mut monday, &params("2026-07-27"))
            .await
            .unwrap();

        let mut tuesday = ok_job();
        let execution = launcher
            .run(&mut tuesday, &params("2026-07-28"))
            .await
            .unwrap();

        assert_eq!(execution.status(), BatchStatus::Completed);
    }

    #[test]
    fn launcher_run_future_is_send() {
        fn assert_send<T: Send>(_: T) {}

        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let mut job = ok_job();
        let parameters = params("2026-07-28");

        assert_send(launcher.run(&mut job, &parameters));
    }
}
