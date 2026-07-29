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
    /// - [`BatchError::JobInstanceAlreadyComplete`] if this instance has already
    ///   succeeded (FR-4.4).
    /// - [`BatchError::JobExecutionAlreadyRunning`] if its last execution has
    ///   not reached a terminal status. If that process is in fact dead, clear
    ///   it with
    ///   [`JobRepository::abandon_execution`](crate::JobRepository::abandon_execution).
    /// - otherwise the step's own error if the job fails — the execution is
    ///   persisted as `Failed` *before* that error propagates.
    pub async fn run(
        &self,
        job: &mut Job,
        parameters: &JobParameters,
    ) -> Result<JobExecution, BatchError> {
        let instance = self
            .repository
            .find_or_create_instance(job.name(), parameters)
            .await?;

        // Runs before `create_execution`, or a refused launch leaves an orphan
        // row. Every arm is spelled out so a new `BatchStatus` breaks the build
        // here rather than defaulting to "launch over it".
        if let Some(last) = self.repository.last_execution(instance.id()).await? {
            match last.status() {
                BatchStatus::Completed => {
                    return Err(BatchError::JobInstanceAlreadyComplete {
                        job_name: job.name().into(),
                        instance_id: instance.id(),
                    });
                }
                BatchStatus::Starting | BatchStatus::Started => {
                    return Err(BatchError::JobExecutionAlreadyRunning {
                        job_name: job.name().into(),
                        execution_id: last.id(),
                    });
                }
                // Terminal but unsuccessful — the restart door.
                BatchStatus::Failed | BatchStatus::Stopped | BatchStatus::Abandoned => {}
            }
        }

        let mut execution = self.repository.create_execution(instance.id()).await?;
        execution.set_status(BatchStatus::Started);
        self.repository.update_execution(&execution).await?;

        let outcome = job.run(&execution, &self.repository).await;

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
    use crate::testing::{
        BookmarkReader, CollectingWriter, EvenDoubler, FailingStep, LogStep, SharedSink, VecReader,
        nz,
    };
    use crate::{ChunkStep, InMemoryJobRepository, JobExecutionId, JobParameter};

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

    // ---- the running-execution gate and its escape hatch ----

    /// Simulates a `SIGKILL`ed process: an execution opened, marked `Started`,
    /// and never resolved. No running job can leave this state behind, so it is
    /// built through the repository.
    async fn crashed_execution(
        launcher: &JobLauncher<InMemoryJobRepository>,
        date: &str,
    ) -> JobExecutionId {
        let repository = launcher.repository();

        let instance = repository
            .find_or_create_instance("nightly", &params(date))
            .await
            .unwrap();

        let mut execution = repository.create_execution(instance.id()).await.unwrap();
        execution.set_status(BatchStatus::Started);
        repository.update_execution(&execution).await.unwrap();

        execution.id()
    }

    #[tokio::test]
    async fn launching_over_a_running_execution_is_rejected() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let running = crashed_execution(&launcher, "2026-07-29").await;

        let mut job = ok_job();
        let result = launcher.run(&mut job, &params("2026-07-29")).await;

        assert!(matches!(
            result,
            Err(BatchError::JobExecutionAlreadyRunning { .. })
        ));

        // Catches a gate that checks after creating.
        assert_eq!(
            reload_last(&launcher, "2026-07-29").await.id(),
            running,
            "a rejected launch must not leave a new execution behind"
        );
    }

    /// `Starting` and `Started` are separate variants; guarding only one is an
    /// easy miss.
    #[tokio::test]
    async fn launching_over_a_starting_execution_is_rejected() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let instance = launcher
            .repository()
            .find_or_create_instance("nightly", &params("2026-07-29"))
            .await
            .unwrap();
        let starting = launcher
            .repository()
            .create_execution(instance.id())
            .await
            .unwrap();

        assert_eq!(starting.status(), BatchStatus::Starting);

        let mut job = ok_job();
        let result = launcher.run(&mut job, &params("2026-07-29")).await;

        assert!(matches!(
            result,
            Err(BatchError::JobExecutionAlreadyRunning { .. })
        ));
    }

    /// Without this, the gate above makes a crashed instance unlaunchable
    /// forever.
    #[tokio::test]
    async fn abandoning_a_crashed_execution_unblocks_the_instance() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let crashed = crashed_execution(&launcher, "2026-07-29").await;

        launcher
            .repository()
            .abandon_execution(crashed)
            .await
            .unwrap();

        let mut job = ok_job();
        let execution = launcher.run(&mut job, &params("2026-07-29")).await.unwrap();

        assert_ne!(execution.id(), crashed, "the retry needs its own execution");
        assert_eq!(execution.status(), BatchStatus::Completed);
    }

    /// `Completed` is the whole of FR-4.4's enforcement, and this writes the
    /// field the gate reads.
    #[tokio::test]
    async fn a_completed_execution_cannot_be_abandoned() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let mut job = ok_job();
        launcher.run(&mut job, &params("2026-07-29")).await.unwrap();

        let completed = reload_last(&launcher, "2026-07-29").await.id();
        let result = launcher.repository().abandon_execution(completed).await;

        assert!(matches!(result, Err(BatchError::CannotAbandon { .. })));

        // Catches an impl that mutates and then reports the error.
        assert_eq!(
            reload_last(&launcher, "2026-07-29").await.status(),
            BatchStatus::Completed,
            "a refused abandon must not have mutated anything"
        );
    }

    #[tokio::test]
    async fn abandoning_an_unknown_execution_is_an_error() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());

        let result = launcher
            .repository()
            .abandon_execution(JobExecutionId::new(999))
            .await;

        assert!(matches!(result, Err(BatchError::Repository(_))));
    }

    // ---- restart (FR-5) ----

    /// One chunk step over `[2, 4, 6, 8]`, tolerating `ok_writes` chunk writes
    /// before failing. Built fresh per attempt, as a restarted process would:
    /// nothing may carry over in memory.
    fn bookmarked_job(sink: &SharedSink, ok_writes: usize) -> Job {
        Job::new(
            "nightly",
            vec![Box::new(ChunkStep::new(
                "load",
                BookmarkReader::new(vec![2, 4, 6, 8]),
                EvenDoubler,
                sink.writer(ok_writes),
                nz(2),
            ))],
        )
    }

    /// US-2 / FR-5.3: a restart must not re-write what already committed.
    #[tokio::test]
    async fn a_restart_resumes_from_the_bookmark_without_duplicating() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let sink = SharedSink::new();

        // Attempt 1: the first chunk commits, the second chunk's write fails.
        let mut first = bookmarked_job(&sink, 1);
        assert!(
            launcher
                .run(&mut first, &params("2026-07-29"))
                .await
                .is_err()
        );
        assert_eq!(sink.written(), vec![4, 8]);

        // Attempt 2: a new process. Fresh reader, fresh writer, same store.
        let mut second = bookmarked_job(&sink, usize::MAX);
        launcher
            .run(&mut second, &params("2026-07-29"))
            .await
            .unwrap();

        // Ignoring the bookmark would leave [4, 8, 4, 8, 12, 16].
        assert_eq!(sink.written(), vec![4, 8, 12, 16]);

        let attempt = reload_last(&launcher, "2026-07-29").await;
        let steps = launcher
            .repository()
            .step_executions(attempt.id())
            .await
            .unwrap();

        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].read_count(), 2);
        assert_eq!(steps[0].status(), BatchStatus::Completed);
    }

    /// FR-5.1: the restart must run only the step that did not complete.
    #[tokio::test]
    async fn a_restart_skips_a_step_that_already_completed() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let sink = SharedSink::new();

        let loader = |sink: &SharedSink| {
            ChunkStep::new(
                "load",
                VecReader::new(vec![2, 4]),
                EvenDoubler,
                sink.writer(usize::MAX),
                nz(2),
            )
        };

        let mut first = Job::new(
            "nightly",
            vec![Box::new(loader(&sink)), Box::new(FailingStep)],
        );
        assert!(
            launcher
                .run(&mut first, &params("2026-07-29"))
                .await
                .is_err()
        );
        assert_eq!(sink.written(), vec![4, 8]);

        let mut second = Job::new(
            "nightly",
            vec![Box::new(loader(&sink)), Box::new(FailingStep)],
        );
        assert!(
            launcher
                .run(&mut second, &params("2026-07-29"))
                .await
                .is_err()
        );

        let attempt = reload_last(&launcher, "2026-07-29").await;
        let steps = launcher
            .repository()
            .step_executions(attempt.id())
            .await
            .unwrap();

        // A skipped step gets no record on this attempt...
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].step_name(), "failing");

        // ...and did no work either. Records alone would not catch a re-run.
        assert_eq!(sink.written(), vec![4, 8]);
    }

    /// The skip is gated on `Completed`, not on a previous record existing.
    #[tokio::test]
    async fn a_step_that_failed_before_runs_again() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let sink = SharedSink::new();

        for _ in 0..2 {
            let mut job = Job::new("nightly", vec![Box::new(FailingStep)]);
            assert!(launcher.run(&mut job, &params("2026-07-29")).await.is_err());
        }

        let attempt = reload_last(&launcher, "2026-07-29").await;
        let steps = launcher
            .repository()
            .step_executions(attempt.id())
            .await
            .unwrap();

        assert_eq!(steps.len(), 1, "a failed step is not skipped");
        assert_eq!(steps[0].status(), BatchStatus::Failed);
        assert!(sink.written().is_empty());
    }

    /// FR-4.2: a different `date` is a different instance, so it starts fresh.
    #[tokio::test]
    async fn a_bookmark_does_not_leak_across_instances() {
        let launcher = JobLauncher::new(InMemoryJobRepository::default());
        let monday = SharedSink::new();

        let mut first = bookmarked_job(&monday, 1);
        assert!(
            launcher
                .run(&mut first, &params("2026-07-27"))
                .await
                .is_err()
        );
        assert_eq!(monday.written(), vec![4, 8]);

        let tuesday = SharedSink::new();
        let mut other = bookmarked_job(&tuesday, usize::MAX);
        launcher
            .run(&mut other, &params("2026-07-28"))
            .await
            .unwrap();

        // A lookup keyed on step name alone would produce [12, 16].
        assert_eq!(tuesday.written(), vec![4, 8, 12, 16]);
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
