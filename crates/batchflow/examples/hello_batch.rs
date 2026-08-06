//! The smallest job that exercises the whole engine: launcher, job, chunk step,
//! reader, processor, writer, and a metadata store you can read counters back
//! out of afterwards. No database, no Docker.
//!
//! Run with: `cargo run -p batchflow --example hello_batch`

use batchflow::{
    BatchError, ChunkStep, InMemoryJobRepository, ItemProcessor, ItemReader, ItemWriter, Job,
    JobLauncher, JobParameter, JobParameters, JobRepository, Unmanaged,
};
use std::num::NonZeroUsize;

/// Yields `next..=last`, then end of input.
struct Counter {
    next: u32,
    last: u32,
}

impl ItemReader for Counter {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        if self.next > self.last {
            return Ok(None);
        }

        let item = self.next;
        self.next += 1;
        Ok(Some(item))
    }

    // `open` and `update` keep their default bodies, which means this reader
    // records no position and is therefore not restartable. That is a real
    // limitation, not an omission - see the note at the bottom of main.
}

/// Doubles each item, dropping the ones divisible by three.
struct Double;

impl ItemProcessor for Double {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        if item % 3 == 0 {
            // A filter, not a skip: nothing failed, we decided this item is not
            // wanted. The two land in different counters.
            return Ok(None);
        }

        Ok(Some(item * 2))
    }
}

/// Prints one line per chunk.
struct Stdout;

impl ItemWriter for Stdout {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        println!("chunk: {items:?}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BatchError> {
    let launcher = JobLauncher::new(InMemoryJobRepository::default());

    let step = ChunkStep::new(
        "double",
        Counter { next: 1, last: 10 },
        Double,
        // stdout cannot join a transaction, so it is adapted explicitly. The
        // wrapper is the place where at-least-once delivery gets accepted for
        // this step: a chunk that fails after printing has already printed.
        Unmanaged(Stdout),
        NonZeroUsize::new(4).expect("4 is not zero"),
    );

    let mut job = Job::builder("hello").step(step).build();

    // Parameters are what identify a JobInstance. Change any value here and the
    // next launch is a different instance, free to run from the start.
    let parameters = JobParameters::new().with("run", JobParameter::String("demo".into()));

    let execution = launcher.run(&mut job, &parameters).await?;

    // The point of the metadata store: these counters are reloadable from the
    // repository alone. The step object is not consulted, and after a crash it
    // would not exist.
    println!(
        "\njob {:?} finished as {:?}",
        execution.id(),
        execution.status()
    );

    for step_execution in launcher
        .repository()
        .step_executions(execution.id())
        .await?
    {
        println!(
            "step {:?}: read={} written={} filtered={} skipped={}",
            step_execution.step_name(),
            step_execution.read_count(),
            step_execution.write_count(),
            step_execution.filter_count(),
            step_execution.skip_count(),
        );
    }

    Ok(())
}
