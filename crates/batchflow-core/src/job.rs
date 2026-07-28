use crate::BatchError;
use crate::{BatchStatus, JobExecutionId, JobRepository, Step, StepContribution};

pub struct Job {
    name: String,
    steps: Vec<Box<dyn Step>>,
}

impl Job {
    pub fn new(name: impl Into<String>, steps: Vec<Box<dyn Step>>) -> Self {
        Self {
            name: name.into(),
            steps,
        }
    }

    /// Run every step in order under `job_execution_id`, persisting a
    /// [`StepExecution`](crate::StepExecution) for each.
    ///
    /// Takes the id rather than the whole `JobExecution`: this is all the step
    /// loop needs, and a parameter is easier to widen later than to narrow.
    ///
    /// The first failing step stops the job — but only *after* its own record
    /// is persisted as `Failed`, so the metadata never claims a step is still
    /// running. Same shape as the launcher one level up, for the same reason.
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
            let outcome = step.run(&mut contribution).await;

            // Fold whatever the step managed to report, success or not: a step
            // that failed at item 900 of 1000 really did process 900.
            step_execution.apply(&contribution);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{CollectingWriter, EvenDoubler, FailingStep, LogStep, VecReader, nz};
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

    #[test]
    fn job_run_future_is_send() {
        fn assert_send<T: Send>(_: T) {}

        let repository = InMemoryJobRepository::default();
        let mut job = Job::new("test-name", vec![]);

        assert_send(job.run(JobExecutionId::new(1), &repository));
    }
}
