use crate::BatchError;
use crate::{BatchStatus, ExecutionContext, JobExecutionId, JobRepository, Step, StepContribution};
use std::marker::PhantomData;

pub struct Job {
    name: String,
    steps: Vec<Box<dyn Step>>,
}

impl Job {

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

    pub async fn run<R: JobRepository>(
        &mut self,
        job_execution_id: JobExecutionId,
        repository: &R,
    ) -> Result<(), BatchError> {
        for step in &mut self.steps {
            let mut step_execution = repository
                .create_step_execution(job_execution_id, step.name())
                .await?;
            step_execution.set_status(BatchStatus::Started);
            repository.update_step_execution(&step_execution).await?;

            let mut contribution = StepContribution::new();
            
            let mut context = ExecutionContext::new();
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
    use crate::{ChunkStep, InMemoryJobRepository, JobExecution, JobParameters};

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

        job.run(execution.id(), &repository).await.unwrap();

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

        assert!(job.run(execution.id(), &repository).await.is_err());

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

        job.run(execution.id(), &repository).await.unwrap();

        let steps = repository.step_executions(execution.id()).await.unwrap();

        assert_eq!(
            steps[0].execution_context().get_long(POSITION).unwrap(),
            Some(4)
        );
    }

    /// The bookmark matters *more* on failure than on success: it is the only
    /// reason a restart can skip the chunks that did commit. A step recorded as
    /// `Failed` with an empty context would force a full re-run.
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

        assert!(job.run(execution.id(), &repository).await.is_err());

        let steps = repository.step_executions(execution.id()).await.unwrap();

        assert_eq!(steps[0].status(), BatchStatus::Failed);
        assert_eq!(steps[0].read_count(), 2);
        assert_eq!(
            steps[0].execution_context().get_long(POSITION).unwrap(),
            Some(2)
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

        // No `Box::new` at the call site — that is the ergonomic win over
        // `Job::new`, and it is why `step` takes `S: Step + 'static` by value.
        let mut job = Job::builder("test-name")
            .step(chunk_step)
            .step(LogStep)
            .build();

        assert_eq!(job.name(), "test-name");

        job.run(execution.id(), &repository).await.unwrap();

        let steps = repository.step_executions(execution.id()).await.unwrap();
        let names: Vec<&str> = steps.iter().map(|s| s.step_name()).collect();

        assert_eq!(names, ["double-evens", "log"]);
        assert_eq!(steps[0].read_count(), 4);
    }

    /// The positive control for the `compile_fail` doctest on [`Job::builder`].
    ///
    /// A `compile_fail` doctest passes when its snippet fails to compile for
    /// *any* reason — a typo would satisfy it just as well as the typestate. So
    /// this asserts the same chain compiles once a step is added; the doctest
    /// then only has one remaining explanation for failing.
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

        assert_send(job.run(JobExecutionId::new(1), &repository));
    }
}
