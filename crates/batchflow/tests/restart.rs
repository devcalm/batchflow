//! Restart semantics (US-2), asserted through the facade only.
//!
//! Core already has unit tests for this. They are not a substitute: they see
//! `batchflow-core`'s internals and its own dependencies, so they cannot fail on
//! a type that was never re-exported. This file's dependency graph is one crate
//! deep - the same one a user has - which is the only place a missing `pub use`
//! shows up as a red test.
//!
//! The collaborators below are duplicated from `examples/restart_demo.rs`
//! rather than shared. Every example and every integration-test file is its own
//! crate root, so neither can `use` the other's items; sharing would mean a
//! `tests/common/mod.rs` that the example still could not reach.

use batchflow::{
    BatchError, BatchStatus, ChunkStep, ContextValue, ExecutionContext, InMemoryJobRepository,
    ItemProcessor, ItemReader, ItemWriter, Job, JobLauncher, JobParameter, JobParameters,
    JobRepository, Unmanaged,
};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const JOB: &str = "restartable";
const POSITION: &str = "double.next";

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

    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        let Some(recorded) = context.get_long(POSITION)? else {
            return Ok(());
        };

        self.next = u32::try_from(recorded).map_err(BatchError::read)?;
        Ok(())
    }

    fn update(&self, context: &mut ExecutionContext) {
        context.put(POSITION, ContextValue::Long(i64::from(self.next)));
    }
}

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

/// Fails once, on the chunk containing `14`, then behaves.
struct FlakyWriter {
    sink: Arc<Mutex<Vec<u32>>>,
    armed: Arc<AtomicBool>,
}

impl ItemWriter for FlakyWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if items.contains(&14) && self.armed.swap(false, Ordering::SeqCst) {
            return Err(BatchError::write("the database went away"));
        }

        self.sink
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(items);
        Ok(())
    }
}

/// Built fresh per attempt, as a restarted process would. Reusing one `Job`
/// would leave the reader already advanced in memory, and the test would pass
/// with `open` deleted.
fn build_job(sink: &Arc<Mutex<Vec<u32>>>, armed: &Arc<AtomicBool>) -> Job {
    let step = ChunkStep::new(
        "double",
        Counter { next: 1, last: 10 },
        Double,
        Unmanaged(FlakyWriter {
            sink: Arc::clone(sink),
            armed: Arc::clone(armed),
        }),
        NonZeroUsize::new(4).expect("4 is not zero"),
    );

    Job::builder(JOB).step(step).build()
}

/// What both tests need: everything observable after two attempts.
struct Outcome {
    /// Items that reached the destination, in the order they arrived.
    written: Vec<u32>,
    /// `read_count` persisted by each attempt, oldest first.
    read_counts: Vec<usize>,
    /// Status persisted by each attempt, oldest first.
    statuses: Vec<BatchStatus>,
}

/// Runs the job twice against one repository: the first attempt fails
/// mid-step, the second resumes.
async fn run_twice() -> Outcome {
    let launcher = JobLauncher::new(InMemoryJobRepository::default());
    let sink = Arc::new(Mutex::new(Vec::new()));
    let armed = Arc::new(AtomicBool::new(true));

    // Identical both times: same parameters means same JobInstance, which is
    // what makes the second launch a restart rather than a fresh run.
    let parameters = JobParameters::new().with("run", JobParameter::String("test".into()));

    let mut first = build_job(&sink, &armed);
    let error = launcher
        .run(&mut first, &parameters)
        .await
        .expect_err("the first attempt must fail, or nothing is being restarted");
    assert!(
        matches!(error, BatchError::Write(_)),
        "expected the writer's failure, got {error:?}"
    );

    // Snapshotted before the second attempt: `last_execution` only ever returns
    // the most recent, so attempt 1's row is unreachable once attempt 2 exists.
    let (mut read_counts, mut statuses) = (Vec::new(), Vec::new());
    let (read, status) = last_attempt(&launcher, &parameters).await;
    read_counts.push(read);
    statuses.push(status);

    let mut second = build_job(&sink, &armed);
    launcher
        .run(&mut second, &parameters)
        .await
        .expect("the second attempt must succeed");

    let (read, status) = last_attempt(&launcher, &parameters).await;
    read_counts.push(read);
    statuses.push(status);

    let written = sink.lock().expect("sink poisoned").clone();

    Outcome {
        written,
        read_counts,
        statuses,
    }
}

/// `(read_count, status)` of the single step in the most recent attempt, read
/// back through the repository - the only copy a restarted process would have.
async fn last_attempt(
    launcher: &JobLauncher<InMemoryJobRepository>,
    parameters: &JobParameters,
) -> (usize, BatchStatus) {
    let repository = launcher.repository();

    let instance = repository
        .find_instance(JOB, parameters)
        .await
        .expect("lookup failed")
        .expect("the launch created an instance");

    let execution = repository
        .last_execution(instance.id())
        .await
        .expect("lookup failed")
        .expect("the launch created an execution");

    let steps = repository
        .step_executions(execution.id())
        .await
        .expect("lookup failed");

    let [step] = steps.as_slice() else {
        panic!("expected exactly one step execution, got {}", steps.len());
    };

    (step.read_count(), step.status())
}

/// The exact sequence matters, not just the absence of duplicates. A reader
/// that resumed from the wrong offset can still produce a duplicate-free sink
/// by skipping items - only the full expected vector catches that.
#[tokio::test]
async fn a_restart_writes_every_item_exactly_once() {
    let outcome = run_twice().await;

    assert_eq!(outcome.written, vec![2, 4, 8, 10, 14, 16, 20]);
}

/// Uncommitted work must be uncounted. The first attempt read eight items and
/// may only claim the four its committed chunk covered, so the two attempts sum
/// to exactly one clean pass over the input.
#[tokio::test]
async fn counters_across_both_attempts_sum_to_one_clean_run() {
    let outcome = run_twice().await;

    assert_eq!(outcome.read_counts, vec![4, 6]);
    assert_eq!(
        outcome.read_counts.iter().sum::<usize>(),
        10,
        "the input has ten items and a restart must re-read only what did not commit"
    );
}

/// The failed attempt has to be *recorded* as failed. If it were left `Started`
/// the launcher would refuse the restart outright.
#[tokio::test]
async fn each_attempt_persists_its_own_outcome() {
    let outcome = run_twice().await;

    assert_eq!(
        outcome.statuses,
        vec![BatchStatus::Failed, BatchStatus::Completed]
    );
}
