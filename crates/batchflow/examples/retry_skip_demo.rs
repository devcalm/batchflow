//! Fault tolerance: retrying a transient failure, skipping a bad row, and
//! failing once too many rows are bad (US-3, US-4).
//!
//! Run with: `cargo run -p batchflow --example retry_skip_demo`
//!
//! No database. The engine never learns what a `FeedError` is any more than it
//! learns what a SQLSTATE is - the mapping from "an error my source produced"
//! to "what the engine should do about it" is the one thing a [`Classifier`]
//! exists to hold, and writing one for your own error type is what most users
//! need. `batchflow-postgres` ships the same shape for real SQLSTATEs.

use batchflow::batchflow_core::{
    BatchError, ChunkStep, Classifier, ErrorAction, FaultTolerance, InMemoryJobRepository,
    ItemProcessor, ItemReader, ItemWriter, Job, JobLauncher, JobParameter, JobParameters,
    JobRepository, RetryPolicy, Unmanaged,
};
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What this example's imaginary data source can do wrong.
#[derive(Debug)]
enum FeedError {
    /// One row is malformed. The rest of the feed is fine.
    CorruptRow(u32),
    /// The destination is briefly unreachable.
    Unavailable,
}

impl fmt::Display for FeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CorruptRow(row) => write!(f, "row {row} is malformed"),
            Self::Unavailable => write!(f, "destination unavailable"),
        }
    }
}

impl Error for FeedError {}

/// Maps this application's errors onto the three things the engine can do.
struct FeedClassifier;

impl Classifier for FeedClassifier {
    fn classify(&self, error: &BatchError) -> ErrorAction {
        match feed_error(error) {
            // One bad row: drop it and keep going.
            Some(FeedError::CorruptRow(_)) => ErrorAction::Skip,
            // The system is briefly unwell: the same write may work next time.
            Some(FeedError::Unavailable) => ErrorAction::Retry,
            // Anything we did not put here ourselves is not understood, and an
            // error you do not understand is not one you should retry.
            None => ErrorAction::Fail,
        }
    }
}

/// Finds a [`FeedError`] anywhere in the chain, not just at the top.
///
/// Two reasons this walks rather than matching `BatchError` variants. The
/// engine wraps causes as it goes - a skip-limit failure is
/// `SkipLimitExceeded` -> `Read` -> `FeedError`, three deep - so a top-level
/// match would miss it. And `BatchError` is `#[non_exhaustive]`, so such a
/// match needs a wildcard arm anyway, which is exactly the arm that silently
/// swallows a variant added later.
fn feed_error(error: &BatchError) -> Option<&FeedError> {
    let mut current: Option<&(dyn Error + 'static)> = Some(error);

    while let Some(error) = current {
        if let Some(feed) = error.downcast_ref::<FeedError>() {
            return Some(feed);
        }
        current = error.source();
    }

    None
}

/// Yields `1..=last`, failing on the rows listed in `corrupt`.
struct Feed {
    next: u32,
    last: u32,
    corrupt: &'static [u32],
}

impl ItemReader for Feed {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        if self.next > self.last {
            return Ok(None);
        }

        let item = self.next;

        // Advance *before* deciding to fail. A reader that errors without
        // moving past the offending row hands the engine the same bad item
        // forever; the only thing standing between that and an infinite loop is
        // the skip limit, which turns a hang into a step failure. Do not rely
        // on it - a skipping reader must make progress.
        self.next += 1;

        if self.corrupt.contains(&item) {
            return Err(BatchError::read(FeedError::CorruptRow(item)));
        }

        Ok(Some(item))
    }
}

struct Double;

impl ItemProcessor for Double {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        Ok(Some(item * 2))
    }
}

/// Reports `Unavailable` for its first `failures` calls, then works.
struct FlakySink {
    sink: Arc<Mutex<Vec<u32>>>,
    failures: Arc<AtomicU32>,
}

impl ItemWriter for FlakySink {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            println!("    write failed for {items:?}");
            return Err(BatchError::write(FeedError::Unavailable));
        }

        self.sink
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(items);
        println!("    wrote {items:?}");
        Ok(())
    }
}

/// One scenario: build a job, run it, report what the store believes.
async fn scenario(
    title: &str,
    corrupt: &'static [u32],
    write_failures: u32,
    fault: FaultTolerance,
) -> Result<(), BatchError> {
    println!("--- {title} ---");

    let launcher = JobLauncher::new(InMemoryJobRepository::default());
    let sink = Arc::new(Mutex::new(Vec::new()));

    let step = ChunkStep::new(
        "load",
        Feed {
            next: 1,
            last: 10,
            corrupt,
        },
        Double,
        Unmanaged(FlakySink {
            sink: Arc::clone(&sink),
            failures: Arc::new(AtomicU32::new(write_failures)),
        }),
        NonZeroUsize::new(4).expect("4 is not zero"),
    )
    // One call, not `.retry(..).classifier(..)` as two: the policy and the
    // classifier are a single decision, and splitting them would let the second
    // setter quietly discard the first.
    .with_fault_tolerance(fault);

    let mut job = Job::builder(title).step(step).build();
    let parameters = JobParameters::new().with("run", JobParameter::String("demo".into()));

    let outcome = launcher.run(&mut job, &parameters).await;

    match &outcome {
        Ok(execution) => println!("  job {:?}", execution.status()),
        Err(error) => {
            println!("  job failed: {error}");
            // The chain is why `feed_error` walks instead of matching: the
            // original bad row is still in here, under the limit error.
            let mut source = error.source();
            while let Some(cause) = source {
                println!("    caused by: {cause}");
                source = cause.source();
            }
        }
    }

    let repository = launcher.repository();
    let instance = repository
        .find_instance(title, &parameters)
        .await?
        .expect("the launch created an instance");
    let execution = repository
        .last_execution(instance.id())
        .await?
        .expect("the launch created an execution");

    for step in repository.step_executions(execution.id()).await? {
        println!(
            "  persisted: status={:?} read={} written={} skipped={}",
            step.status(),
            step.read_count(),
            step.write_count(),
            step.skip_count(),
        );
    }

    println!("  sink: {:?}\n", sink.lock().expect("sink poisoned"));

    // The job's own outcome, reported last. Returning `Ok(())` here would have
    // meant a failed job and a healthy scenario were indistinguishable to the
    // caller - the same shape as the `?`-past-the-status-write bug the launcher
    // is written to avoid.
    outcome.map(|_| ())
}

#[tokio::main]
async fn main() -> Result<(), BatchError> {
    // Shortened so the demo does not spend a second asleep. Production defaults
    // are 100ms to 30s; a *test* neither shortens nor sleeps, it uses
    // `#[tokio::test(start_paused = true)]` and asserts on elapsed time.
    let quick = RetryPolicy::attempts(NonZeroU32::new(3).expect("3 is not zero"))
        .min_delay(Duration::from_millis(10))
        .max_delay(Duration::from_millis(50));

    // The whole chunk is re-attempted in a *fresh* transaction, up to three
    // times. Two failures is inside the budget.
    scenario(
        "retry",
        &[],
        2,
        FaultTolerance::new()
            .classifier(FeedClassifier)
            .retry(quick),
    )
    .await?;

    // Rows 4 and 7 are malformed. Note that a skipped read does not consume a
    // slot: chunks stay the size the commit interval promised, so dirty input
    // cannot quietly shrink the transaction.
    scenario(
        "skip",
        &[4, 7],
        0,
        FaultTolerance::new()
            .classifier(FeedClassifier)
            .retry(quick)
            .skip_limit(5),
    )
    .await?;

    // Same feed, a budget of one. The second bad row stops being "one bad row"
    // and becomes "this feed is garbage", which is a different page at 3am.
    scenario(
        "skip limit exceeded",
        &[4, 7],
        0,
        FaultTolerance::new()
            .classifier(FeedClassifier)
            .retry(quick)
            .skip_limit(1),
    )
    .await
    .expect_err("a skip limit of one cannot absorb two bad rows");

    Ok(())
}
