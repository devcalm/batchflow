//! A job that dies mid-step, is relaunched against the same `JobInstance`, and
//! resumes at the last committed chunk without writing any item twice (US-2).
//!
//! Run with: `cargo run -p batchflow --example restart_demo`
//!
//! This is `hello_batch` with three changes: the reader records its position,
//! the writer fails once, and the job is launched twice.

use batchflow::{
    BatchError, ChunkStep, ContextValue, ExecutionContext, InMemoryJobRepository, ItemProcessor,
    ItemReader, ItemWriter, Job, JobLauncher, JobParameter, JobParameters, JobRepository,
    Unmanaged,
};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Where this reader records how far it got.
///
/// Namespaced by step: a context is one bag shared by everything in the step,
/// and two collaborators picking the same bare key would silently corrupt each
/// other's bookmark.
const POSITION: &str = "double.next";

/// Yields `next..=last`, and remembers where it got to.
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

    /// Restores the position recorded by the last committed chunk.
    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        // `get_long` keeps three outcomes apart, and all three matter here:
        // absent means a genuinely fresh run, so leave `next` alone; a wrong
        // type is a corrupt bookmark and propagates. Collapsing them would let
        // a garbled context restart a half-done job from the top.
        let Some(recorded) = context.get_long(POSITION)? else {
            return Ok(());
        };

        // `as` would be the bug here, not the conversion. It never fails, it
        // wraps: a negative bookmark becomes a huge positive one, the reader
        // reads past the end, and the step reports success having processed
        // nothing. `try_from` turns that into a loud failure instead.
        self.next = u32::try_from(recorded).map_err(BatchError::read)?;
        Ok(())
    }

    /// Records the position. Called at the commit point, after a successful
    /// write, so the bookmark and the data it describes commit together.
    fn update(&self, context: &mut ExecutionContext) {
        context.put(POSITION, ContextValue::Long(i64::from(self.next)));
    }
}

/// Doubles each item, dropping the ones divisible by three.
struct Double;

impl ItemProcessor for Double {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        if item % 3 == 0 {
            return Ok(None);
        }

        Ok(Some(item * 2))
    }
}

/// Appends to a destination that outlives the step, and fails the first time it
/// is handed the chunk containing `14`.
///
/// Both fields are shared handles on purpose. The sink has to outlive the step
/// because a restart builds a *fresh* `ChunkStep`: a step-owned buffer would
/// take its evidence with it, and two runs writing into two private buffers
/// look exactly like one correct run. Duplicates are only visible in a
/// destination that survives both attempts.
struct FlakyWriter {
    sink: Arc<Mutex<Vec<u32>>>,
    armed: Arc<AtomicBool>,
}

impl ItemWriter for FlakyWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if items.contains(&14) && self.armed.swap(false, Ordering::SeqCst) {
            println!("  write FAILED for {items:?}");
            return Err(BatchError::write("the database went away"));
        }

        // Locked and released without an await in between. Holding this guard
        // across one would not merely be bad practice - `ItemWriter::write`
        // requires `+ Send`, and a `MutexGuard` is not, so it would not compile.
        self.sink
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(items);

        println!("  wrote {items:?}");
        Ok(())
    }
}

/// Builds a job from scratch, as a restarted *process* would.
///
/// Reusing one `Job` across both launches would defeat the demo: its reader is
/// owned by value and has already advanced in memory, so it would resume
/// correctly whether or not the bookmark works at all.
fn build_job(sink: &Arc<Mutex<Vec<u32>>>, armed: &Arc<AtomicBool>) -> Job {
    let step = ChunkStep::new(
        "double",
        Counter { next: 1, last: 10 },
        Double,
        // Not transactional: this writer's effects do not roll back with the
        // chunk. It stays clean here only because it checks for failure before
        // touching the sink - a file or an HTTP POST gets no such choice, which
        // is exactly what `Unmanaged` signs for.
        Unmanaged(FlakyWriter {
            sink: Arc::clone(sink),
            armed: Arc::clone(armed),
        }),
        NonZeroUsize::new(4).expect("4 is not zero"),
    );

    Job::builder("restartable").step(step).build()
}

#[tokio::main]
async fn main() -> Result<(), BatchError> {
    let launcher = JobLauncher::new(InMemoryJobRepository::default());
    let sink = Arc::new(Mutex::new(Vec::new()));
    let armed = Arc::new(AtomicBool::new(true));

    // The same parameters both times. That is what makes the second launch a
    // restart of the same JobInstance rather than a fresh run: change any value
    // and the engine has nothing to resume from.
    let parameters = JobParameters::new().with("run", JobParameter::String("demo".into()));

    println!("--- attempt 1 ---");
    let mut job = build_job(&sink, &armed);
    match launcher.run(&mut job, &parameters).await {
        Ok(execution) => println!("unexpectedly finished as {:?}", execution.status()),
        Err(error) => println!("  job failed: {error}"),
    }
    report(&launcher, &parameters).await?;

    println!("--- attempt 2 ---");
    let mut job = build_job(&sink, &armed);
    let execution = launcher.run(&mut job, &parameters).await?;
    println!("  job finished as {:?}", execution.status());
    report(&launcher, &parameters).await?;

    let written = sink.lock().expect("sink poisoned").clone();
    println!("\nsink across both attempts: {written:?}");

    let mut sorted = written.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), written.len(), "an item was written twice");
    println!("no item written twice - US-2 holds");

    Ok(())
}

/// Prints the counters the *repository* holds for the most recent attempt.
///
/// Read back through the store rather than off the step, because that is the
/// only copy a restarted process would have.
async fn report(
    launcher: &JobLauncher<InMemoryJobRepository>,
    parameters: &JobParameters,
) -> Result<(), BatchError> {
    let repository = launcher.repository();

    let instance = repository
        .find_instance("restartable", parameters)
        .await?
        .expect("the launch created an instance");

    let execution = repository
        .last_execution(instance.id())
        .await?
        .expect("the launch created an execution");

    for step in repository.step_executions(execution.id()).await? {
        println!(
            "  persisted: status={:?} read={} written={} filtered={} bookmark={:?}",
            step.status(),
            step.read_count(),
            step.write_count(),
            step.filter_count(),
            step.execution_context().get(POSITION),
        );
    }

    Ok(())
}
