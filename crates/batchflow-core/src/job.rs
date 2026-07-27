use crate::BatchError;
use crate::{Step, StepExecution};

pub struct Job {
    steps: Vec<Box<dyn Step>>,
}

impl Job {
    pub fn new(steps: Vec<Box<dyn Step>>) -> Self {
        Self { steps }
    }

    pub async fn run(&mut self) -> Result<Vec<StepExecution>, BatchError> {
        let mut executions = Vec::with_capacity(self.steps.len());

        for step in &mut self.steps {
            executions.push(step.run().await?);
        }

        Ok(executions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChunkStep;
    use crate::testing::{CollectingWriter, EvenDoubler, LogStep, VecReader, nz};

    #[tokio::test]
    async fn job_runs_heterogeneous_steps_in_order() {
        let chunk_step = ChunkStep::new(
            "double-evens",
            VecReader::new(vec![1, 2, 3, 4]),
            EvenDoubler,
            CollectingWriter::new(),
            nz(2),
        );

        let mut job = Job::new(vec![Box::new(chunk_step), Box::new(LogStep)]);
        let execs = job.run().await.unwrap();

        assert_eq!(execs.len(), 2);
        assert_eq!(execs[0].read_count, 4); // chunk step read 1,2,3,4
        assert_eq!(execs[0].write_count, 2); // evens 2,4 → written 4,8
        assert_eq!(execs[1], StepExecution::default()); // log step did no I/O
    }

    #[test]
    fn job_run_future_is_send() {
        fn assert_send<T: Send>(_: T) {}

        let mut job = Job::new(vec![]);
        assert_send(job.run());
    }
}
