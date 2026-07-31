use crate::BatchError;
use crate::metrics::{
    LABEL_JOB, LABEL_STATUS, LABEL_STEP, STEP_DURATION, STEPS_FINISHED, STEPS_STARTED, status_label,
};
use crate::{
    BatchStatus, ExecutionContext, JobExecution, JobRepository, Step, StepCommit, StepContribution,
    StepExecution,
};
use ::metrics::{counter, histogram};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::time::Instant;

/// Persists a step's committed work: folds the counters into its
/// [`StepExecution`] and stores the bookmark, then writes the row.
///
/// Phase 11b makes this the owner of the step's transaction, so the items, the
/// counters and the bookmark commit as one.
struct RepositoryCommit<'a, R> {
    repository: &'a R,
    step_execution: &'a mut StepExecution,
    job_name: &'a str,
}

#[async_trait]
impl<R: JobRepository> StepCommit<R::Tx> for RepositoryCommit<'_, R> {
    fn job_name(&self) -> &str {
        self.job_name
    }

    async fn begin(&mut self) -> Result<R::Tx, BatchError> {
        self.repository.begin().await
    }

    async fn commit(
        &mut self,
        mut tx: R::Tx,
        contribution: &StepContribution,
        context: &ExecutionContext,
    ) -> Result<(), BatchError> {
        // Folded into a copy, and only swapped in once the transaction has
        // actually committed — so the in-memory counters can never claim work
        // that rolled back.
        let mut candidate = self.step_execution.clone();
        candidate.apply(contribution);
        candidate.set_execution_context(context.clone());

        self.repository
            .update_step_execution_in(&mut tx, &candidate)
            .await?;
        self.repository.commit(tx).await?;

        *self.step_execution = candidate;
        Ok(())
    }

    async fn rollback(&mut self, tx: R::Tx) -> Result<(), BatchError> {
        self.repository.rollback(tx).await
    }
}

/// `Tx` is the transaction type of the repository this job will run against,
/// defaulting to `()` for jobs whose writers do not enlist.
pub struct Job<Tx = ()> {
    name: String,
    steps: Vec<Box<dyn Step<Tx>>>,
}

impl<Tx> Job<Tx> {
    /// Start building a [`Job`] called `name`. Steps are boxed internally, so
    /// callers never write `Box::new`.
    ///
    /// ```
    /// use batchflow_core::async_trait;
    /// use batchflow_core::{BatchError, ExecutionContext, Job, Step, StepCommit};
    ///
    /// struct Cleanup;
    ///
    /// #[async_trait]
    /// impl Step for Cleanup {
    ///     fn name(&self) -> &str {
    ///         "cleanup"
    ///     }
    ///
    ///     // A tasklet has no chunks, so nothing to commit.
    ///     async fn run(
    ///         &mut self,
    ///         _context: &mut ExecutionContext,
    ///         _commit: &mut dyn StepCommit,
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
    pub fn builder(name: impl Into<String>) -> JobBuilder<NoSteps, Tx> {
        JobBuilder {
            name: name.into(),
            steps: Vec::new(),
            _state: PhantomData,
        }
    }

    pub fn new(name: impl Into<String>, steps: Vec<Box<dyn Step<Tx>>>) -> Self {
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
    pub async fn run<R>(
        &mut self,
        job_execution: &JobExecution,
        repository: &R,
    ) -> Result<(), BatchError>
    where
        R: JobRepository<Tx = Tx>,
        Tx: Send,
    {
        let job_name = self.name.as_str();

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

            // Below the `continue`, so a step skipped on restart is not counted
            // as started. It did not run, and a `steps_started` that included it
            // would make a restart look like a full execution.
            let started = Instant::now();
            counter!(
                STEPS_STARTED,
                LABEL_JOB => job_name.to_owned(),
                LABEL_STEP => step.name().to_owned(),
            )
            .increment(1);

            let mut context = previous
                .map(|previous| previous.execution_context().clone())
                .unwrap_or_default();

            let mut step_execution = repository
                .create_step_execution(job_execution.id(), step.name())
                .await?;
            step_execution.set_status(BatchStatus::Started);
            repository.update_step_execution(&step_execution).await?;

            // Counters and bookmark are now persisted by the step at each of
            // its commit points, not here — so a crash mid-step leaves the work
            // that did commit recorded.
            let outcome = {
                let mut commit = RepositoryCommit {
                    repository,
                    step_execution: &mut step_execution,
                    job_name,
                };
                step.run(&mut context, &mut commit).await
            };

            let status = if outcome.is_ok() {
                BatchStatus::Completed
            } else {
                BatchStatus::Failed
            };
            step_execution.set_status(status);
            repository.update_step_execution(&step_execution).await?;

            // After the write, so the metric cannot claim a terminal status the
            // repository never recorded.
            histogram!(
                STEP_DURATION,
                LABEL_JOB => job_name.to_owned(),
                LABEL_STEP => step_execution.step_name().to_owned(),
            )
            .record(started.elapsed().as_secs_f64());
            counter!(
                STEPS_FINISHED,
                LABEL_JOB => job_name.to_owned(),
                LABEL_STEP => step_execution.step_name().to_owned(),
                LABEL_STATUS => status_label(status),
            )
            .increment(1);

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

pub struct JobBuilder<State = NoSteps, Tx = ()> {
    name: String,
    steps: Vec<Box<dyn Step<Tx>>>,
    _state: PhantomData<State>,
}

impl<State, Tx> JobBuilder<State, Tx> {
    pub fn step<S: Step<Tx> + 'static>(mut self, step: S) -> JobBuilder<HasSteps, Tx> {
        self.steps.push(Box::new(step));

        JobBuilder {
            name: self.name,
            steps: self.steps,
            _state: PhantomData,
        }
    }
}

impl<Tx> JobBuilder<HasSteps, Tx> {
    pub fn build(self) -> Job<Tx> {
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
        BookmarkReader, CollectingWriter, CommitFails, EvenDoubler, FailingStep, FlakyWriter,
        LogStep, POSITION, VecReader, nz,
    };
    use crate::{
        ChunkStep, InMemoryJobRepository, JobExecutionId, JobInstanceId, JobParameters,
        JobRepository, Unmanaged,
    };
    use std::sync::{Arc, Mutex};

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
            Unmanaged(CollectingWriter::new()),
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
            Unmanaged(CollectingWriter::new()),
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
            Unmanaged(FlakyWriter::new(1)), // first chunk commits, second does not
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
            Unmanaged(CollectingWriter::new()),
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
            Unmanaged(CollectingWriter::new()),
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

    /// `(read_count, bookmark)` as persisted at some instant during a step.
    type Snapshot = Option<(usize, Option<i64>)>;

    /// Reports what the repository held while the second chunk was being
    /// written — i.e. after the first chunk had fully committed, and before the
    /// step returned.
    struct ProbingWriter {
        repository: Arc<InMemoryJobRepository>,
        execution_id: JobExecutionId,
        writes: usize,
        seen: Arc<Mutex<Snapshot>>,
    }

    impl crate::ItemWriter for ProbingWriter {
        type Item = u32;

        async fn write(&mut self, _items: &[u32]) -> Result<(), BatchError> {
            self.writes += 1;
            if self.writes == 2 {
                let steps = self.repository.step_executions(self.execution_id).await?;
                *self.seen.lock().expect("probe poisoned") = steps.first().map(|step| {
                    (
                        step.read_count(),
                        step.execution_context().get_long(POSITION).unwrap(),
                    )
                });
            }
            Ok(())
        }
    }

    /// The crash case, and the reason persistence moved into the chunk loop:
    /// a process killed mid-step never returns from `step.run`, so anything
    /// persisted only afterwards is lost and the restart re-writes every item.
    #[tokio::test]
    async fn committed_work_is_durable_before_the_step_finishes() {
        let repository = Arc::new(InMemoryJobRepository::default());
        let instance = repository
            .find_or_create_instance("test-name", &JobParameters::new())
            .await
            .unwrap();
        let execution = repository.create_execution(instance.id()).await.unwrap();

        let seen = Arc::new(Mutex::new(None));
        let step = ChunkStep::new(
            "load",
            BookmarkReader::new(vec![2, 4, 6, 8]),
            EvenDoubler,
            Unmanaged(ProbingWriter {
                repository: Arc::clone(&repository),
                execution_id: execution.id(),
                writes: 0,
                seen: Arc::clone(&seen),
            }),
            nz(2),
        );
        let mut job = Job::new("test-name", vec![Box::new(step)]);

        job.run(&execution, &*repository).await.unwrap();

        // Two items read and a bookmark at 2 — the first chunk, already durable.
        // Before this change the snapshot was `(0, None)`.
        assert_eq!(*seen.lock().unwrap(), Some((2, Some(2))));
    }

    /// Counters and bookmark advance only once the transaction has committed.
    /// Reversing those two lines in `RepositoryCommit::commit` is invisible to
    /// every other test, because `InMemoryJobRepository` cannot fail a commit —
    /// exactly the blind spot ADR-007 warned about.
    #[tokio::test]
    async fn a_failed_commit_does_not_advance_the_counters() {
        let repository = CommitFails::new();
        let instance = repository
            .find_or_create_instance("test-name", &JobParameters::new())
            .await
            .unwrap();
        let execution = repository.create_execution(instance.id()).await.unwrap();

        let step = ChunkStep::new(
            "load",
            BookmarkReader::new(vec![2, 4]),
            EvenDoubler,
            Unmanaged(CollectingWriter::new()),
            nz(2),
        );
        let mut job = Job::new("test-name", vec![Box::new(step)]);

        assert!(job.run(&execution, &repository).await.is_err());

        let steps = repository.step_executions(execution.id()).await.unwrap();

        assert_eq!(steps[0].status(), BatchStatus::Failed);
        assert_eq!(
            steps[0].read_count(),
            0,
            "work whose commit failed must not be counted"
        );
        assert_eq!(
            steps[0].execution_context().get_long(POSITION).unwrap(),
            None
        );
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
