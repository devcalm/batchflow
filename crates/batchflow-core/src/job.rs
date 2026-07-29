use crate::BatchError;
use crate::{BatchStatus, JobExecution, JobRepository, Step, StepContribution};
use std::marker::PhantomData;

pub struct Job {
    name: String,
    steps: Vec<Box<dyn Step>>,
}

impl Job {
    /// Start building a [`Job`] called `name`. Steps are boxed internally, so
    /// callers never write `Box::new`.
    ///
    /// ```
    /// use batchflow_core::async_trait;
    /// use batchflow_core::{BatchError, ExecutionContext, Job, Step, StepContribution};
    ///
    /// struct Cleanup;
    ///
    /// #[async_trait]
    /// impl Step for Cleanup {
    ///     fn name(&self) -> &str {
    ///         "cleanup"
    ///     }
    ///
    ///     async fn run(
    ///         &mut self,
    ///         _contribution: &mut StepContribution,
    ///         _context: &mut ExecutionContext,
    ///     ) -> Result<(), BatchError> {
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let job = Job::builder("nightly").step(Cleanup).build();
    /// assert_eq!(job.name(), "nightly");
    /// ```
    ///
    /// [`build`](JobBuilder::build) is defined only on `JobBuilder<HasSteps>`,
    /// so a job with no steps is a compile error. The builder changes type on
    /// the first `.step(..)` and so cannot be driven from a loop; [`Job::new`]
    /// is the dynamic escape hatch.
    ///
    /// **The block below is a test** — the only check on the typestate.
    ///
    /// ```compile_fail
    /// use batchflow_core::Job;
    ///
    /// let job = Job::builder("nightly").build();
    /// ```
    pub fn builder(name: impl Into<String>) -> JobBuilder<NoSteps> {
        JobBuilder {
            name: name.into(),
            steps: Vec::new(),
            _state: PhantomData,
        }
    }

    pub fn new(name: impl Into<String>, steps: Vec<Box<dyn Step>>) -> Self {
        Self {
            name: name.into(),
            steps,
        }
    }

    /// Run every step in order under `job_execution`, persisting a
    /// [`StepExecution`](crate::StepExecution) for each one that runs.
    ///
    /// Restart (FR-5): each step looks up the previous attempt at this
    /// *instance* by step name. A `Completed` one is skipped and gets no record
    /// on this attempt; otherwise this attempt's
    /// [`ExecutionContext`](crate::ExecutionContext) is seeded from the
    /// bookmark it left, so the reader resumes at the last committed chunk. On
    /// a fresh run every lookup returns `None`.
    ///
    /// The first failing step stops the job, after its own record is persisted
    /// as `Failed`.
    pub async fn run<R: JobRepository>(
        &mut self,
        job_execution: &JobExecution,
        repository: &R,
    ) -> Result<(), BatchError> {
        for step in &mut self.steps {
            // Must precede `create_step_execution`: mint first and this returns
            // *this* attempt's own record, so nothing is ever skipped and every
            // reader restarts from zero.
            let previous = repository
                .last_step_execution(job_execution.instance_id(), step.name())
                .await?;

            if let Some(previous) = &previous
                && previous.status() == BatchStatus::Completed
            {
                continue;
            }

            let mut context = previous
                .map(|previous| previous.execution_context().clone())
                .unwrap_or_default();

            let mut step_execution = repository
                .create_step_execution(job_execution.id(), step.name())
                .await?;
            step_execution.set_status(BatchStatus::Started);
            repository.update_step_execution(&step_execution).await?;

            let mut contribution = StepContribution::new();
            let outcome = step.run(&mut contribution, &mut context).await;

            step_execution.apply(&contribution);
            step_execution.set_execution_context(context);
            step_execution.set_status(if outcome.is_ok() {
                BatchStatus::Completed
            } else {
                BatchStatus::Failed
            });
            repository.update_step_execution(&step_execution).await?;

            outcome?;
        }

        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Typestate marker: no step has been added yet, so there is nothing to build.
#[derive(Debug)]
pub struct NoSteps;

/// Typestate marker: at least one step has been added.
#[derive(Debug)]
pub struct HasSteps;

pub struct JobBuilder<State = NoSteps> {
    name: String,
    steps: Vec<Box<dyn Step>>,
    _state: PhantomData<State>,
}

impl<State> JobBuilder<State> {
    pub fn step<S: Step + 'static>(mut self, step: S) -> JobBuilder<HasSteps> {
        self.steps.push(Box::new(step));

        JobBuilder {
            name: self.name,
            steps: self.steps,
            _state: PhantomData,
        }
    }
}

impl JobBuilder<HasSteps> {
    pub fn build(self) -> Job {
        Job {
            name: self.name,
            steps: self.steps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        BookmarkReader, CollectingWriter, EvenDoubler, FailingStep, FlakyWriter, LogStep, POSITION,
        VecReader, nz,
    };
    use crate::{
        ChunkStep, InMemoryJobRepository, JobExecutionId, JobInstanceId, JobParameters,
        JobRepository,
    };

    /// A job execution to hang step executions off. Steps are stored by
    /// foreign key, so the parent row has to exist first.
    async fn open_execution(repository: &InMemoryJobRepository) -> JobExecution {
        let instance = repository
            .find_or_create_instance("test-name", &JobParameters::new())
            .await
            .unwrap();

        repository.create_execution(instance.id()).await.unwrap()
    }

    #[tokio::test]
    async fn job_runs_heterogeneous_steps_in_order() {
        let repository = InMemoryJobRepository::default();
        let execution = open_execution(&repository).await;

        let chunk_step = ChunkStep::new(
            "double-evens",
            VecReader::new(vec![1, 2, 3, 4]),
            EvenDoubler,
            CollectingWriter::new(),
            nz(2),
        );
        let mut job = Job::new("test-name", vec![Box::new(chunk_step), Box::new(LogStep)]);

        job.run(&execution, &repository).await.unwrap();

        let steps = repository.step_executions(execution.id()).await.unwrap();

        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].step_name(), "double-evens");
        assert_eq!(steps[0].read_count(), 4); // read 1,2,3,4
        assert_eq!(steps[0].write_count(), 2); // evens 2,4 → written 4,8
        assert_eq!(steps[0].filter_count(), 2); // odds 1,3 filtered
        assert_eq!(steps[0].status(), BatchStatus::Completed);

        // Each step gets its own record — the log step did no I/O, and that
        // must not be polluted by the chunk step that ran before it.
        assert_eq!(steps[1].step_name(), "log");
        assert_eq!(steps[1].read_count(), 0);
        assert_eq!(steps[1].write_count(), 0);
    }

    #[tokio::test]
    async fn a_failing_step_stops_the_job_and_records_its_own_failure() {
        let repository = InMemoryJobRepository::default();
        let execution = open_execution(&repository).await;

        let mut job = Job::new("test-name", vec![Box::new(FailingStep), Box::new(LogStep)]);

        assert!(job.run(&execution, &repository).await.is_err());

        let steps = repository.step_executions(execution.id()).await.unwrap();

        assert_eq!(steps.len(), 1, "the step after a failure must not run");
        assert_eq!(steps[0].step_name(), "failing");
        assert_eq!(steps[0].status(), BatchStatus::Failed);
    }

    #[tokio::test]
    async fn a_step_bookmark_is_persisted_on_its_step_execution() {
        let repository = InMemoryJobRepository::default();
        let execution = open_execution(&repository).await;

        let chunk_step = ChunkStep::new(
            "bookmarked",
            BookmarkReader::new(vec![2, 4, 6, 8]),
            EvenDoubler,
            CollectingWriter::new(),
            nz(2),
        );
        let mut job = Job::new("test-name", vec![Box::new(chunk_step)]);

        job.run(&execution, &repository).await.unwrap();

        let steps = repository.step_executions(execution.id()).await.unwrap();

        assert_eq!(
            steps[0].execution_context().get_long(POSITION).unwrap(),
            Some(4)
        );
    }

    /// The bookmark matters more on failure: it is what a restart resumes from.
    #[tokio::test]
    async fn a_failing_step_still_persists_its_bookmark() {
        let repository = InMemoryJobRepository::default();
        let execution = open_execution(&repository).await;

        let chunk_step = ChunkStep::new(
            "bookmarked",
            BookmarkReader::new(vec![2, 4, 6, 8]),
            EvenDoubler,
            FlakyWriter::new(1), // first chunk commits, second does not
            nz(2),
        );
        let mut job = Job::new("test-name", vec![Box::new(chunk_step)]);

        assert!(job.run(&execution, &repository).await.is_err());

        let steps = repository.step_executions(execution.id()).await.unwrap();

        assert_eq!(steps[0].status(), BatchStatus::Failed);
        assert_eq!(steps[0].read_count(), 2);
        assert_eq!(
            steps[0].execution_context().get_long(POSITION).unwrap(),
            Some(2)
        );
    }

    /// The restart machinery must not change the fresh-run path.
    #[tokio::test]
    async fn a_first_run_starts_every_step_from_an_empty_context() {
        let repository = InMemoryJobRepository::default();
        let execution = open_execution(&repository).await;

        let chunk_step = ChunkStep::new(
            "bookmarked",
            BookmarkReader::new(vec![2, 4]),
            EvenDoubler,
            CollectingWriter::new(),
            nz(2),
        );
        let mut job = Job::new("test-name", vec![Box::new(chunk_step)]);

        job.run(&execution, &repository).await.unwrap();

        let steps = repository.step_executions(execution.id()).await.unwrap();
        assert_eq!(
            steps[0].read_count(),
            2,
            "nothing may be skipped on a first run"
        );
    }

    #[tokio::test]
    async fn builder_produces_a_runnable_job_preserving_step_order() {
        let repository = InMemoryJobRepository::default();
        let execution = open_execution(&repository).await;

        let chunk_step = ChunkStep::new(
            "double-evens",
            VecReader::new(vec![1, 2, 3, 4]),
            EvenDoubler,
            CollectingWriter::new(),
            nz(2),
        );

        let mut job = Job::builder("test-name")
            .step(chunk_step)
            .step(LogStep)
            .build();

        assert_eq!(job.name(), "test-name");

        job.run(&execution, &repository).await.unwrap();

        let steps = repository.step_executions(execution.id()).await.unwrap();
        let names: Vec<&str> = steps.iter().map(|s| s.step_name()).collect();

        assert_eq!(names, ["double-evens", "log"]);
        assert_eq!(steps[0].read_count(), 4);
    }

    /// Positive control for the `compile_fail` doctest on [`Job::builder`],
    /// which would otherwise pass on any compile error, including a typo.
    #[test]
    fn build_is_reachable_once_a_step_has_been_added() {
        let job = Job::builder("test-name").step(LogStep).build();

        assert_eq!(job.name(), "test-name");
    }

    #[test]
    fn job_run_future_is_send() {
        fn assert_send<T: Send>(_: T) {}

        let repository = InMemoryJobRepository::default();
        let mut job = Job::new("test-name", vec![]);
        let execution = JobExecution::new(JobExecutionId::new(1), JobInstanceId::new(1));

        assert_send(job.run(&execution, &repository));
    }
}
