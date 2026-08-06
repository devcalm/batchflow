//! Integration tests, so they stand exactly where a user stands: one crate
//! deep, importing only what is actually re-exported. A missing `pub use` goes
//! red here and nowhere else (the Phase 16 lesson).

use batchflow_core::{
    BatchError, ExecutionContext, InMemoryJobRepository, Job, JobLauncher, JobParameter,
    JobParameters, JobRepository, Step, StepCommit, async_trait,
};
use batchflow_scheduler::metrics::{
    LABEL_OUTCOME, OUTCOME_ALREADY_COMPLETE, OUTCOME_ALREADY_RUNNING, OUTCOME_FAILED, OUTCOME_RAN,
    TRIGGERS,
};
use batchflow_scheduler::{Due, Outcome, ScheduledJob, trigger};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Noop;

#[async_trait]
impl Step for Noop {
    fn name(&self) -> &str {
        "noop"
    }

    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit,
    ) -> Result<(), BatchError> {
        Ok(())
    }
}

struct Boom;

#[async_trait]
impl Step for Boom {
    fn name(&self) -> &str {
        "boom"
    }

    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit,
    ) -> Result<(), BatchError> {
        Err(BatchError::write("the disk is on fire"))
    }
}

fn nightly() -> Job {
    Job::builder("nightly").step(Noop).build()
}

fn on(date: &str) -> JobParameters {
    JobParameters::new().with("date", JobParameter::String(date.into()))
}

#[tokio::test]
async fn a_first_firing_runs_the_job() {
    let launcher = JobLauncher::new(InMemoryJobRepository::new());

    let outcome = trigger(&launcher, &mut nightly(), &on("2026-08-06"))
        .await
        .unwrap();

    assert!(outcome.ran());
}

/// The property every scheduler depends on: firing the same tick twice is a
/// no-op, not a duplicate run and not an error. Missed-tick retries, a second
/// replica and an operator re-running by hand all land here.
#[tokio::test]
async fn re_firing_the_same_tick_is_refused_without_an_error() {
    let launcher = JobLauncher::new(InMemoryJobRepository::new());
    let today = on("2026-08-06");

    trigger(&launcher, &mut nightly(), &today).await.unwrap();
    let outcome = trigger(&launcher, &mut nightly(), &today).await.unwrap();

    assert!(matches!(outcome, Outcome::AlreadyComplete { .. }));
    assert!(!outcome.ran());
}

/// The positive control for the test above: without it, a `trigger` that
/// refused *everything* after the first call would look correct.
#[tokio::test]
async fn the_next_tick_is_a_new_instance_and_runs() {
    let launcher = JobLauncher::new(InMemoryJobRepository::new());

    trigger(&launcher, &mut nightly(), &on("2026-08-06"))
        .await
        .unwrap();
    let outcome = trigger(&launcher, &mut nightly(), &on("2026-08-07"))
        .await
        .unwrap();

    assert!(outcome.ran(), "a different run key is a different instance");
}

/// An unfinished execution holds its instance, so the schedule skips rather
/// than starting a second copy. The refusal comes from the metadata store, so
/// it holds across processes too.
#[tokio::test]
async fn a_tick_that_overlaps_a_running_execution_is_refused() {
    let launcher = JobLauncher::new(InMemoryJobRepository::new());
    let today = on("2026-08-06");

    // Stand in for a run still in flight: an instance with a non-terminal
    // execution against it.
    let instance = launcher
        .repository()
        .find_or_create_instance("nightly", &today)
        .await
        .unwrap();
    launcher
        .repository()
        .create_execution(instance.id())
        .await
        .unwrap();

    let outcome = trigger(&launcher, &mut nightly(), &today).await.unwrap();

    assert!(matches!(outcome, Outcome::AlreadyRunning { .. }));
}

/// Refusals are absorbed; real failures are not. A scheduler that swallowed
/// this would report a healthy nightly run that never happened.
#[tokio::test]
async fn a_failing_job_still_propagates_its_error() {
    let launcher = JobLauncher::new(InMemoryJobRepository::new());
    let mut job = Job::builder("nightly").step(Boom).build();

    let error = trigger(&launcher, &mut job, &on("2026-08-06"))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("disk is on fire"));
}

/// A tick builds a *fresh* job. Reusing one across firings would hand the
/// second tick a reader already at end of input.
#[tokio::test]
async fn each_tick_builds_a_new_job() {
    let builds = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&builds);

    let scheduled = ScheduledJob::new(
        Arc::new(JobLauncher::new(InMemoryJobRepository::new())),
        move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Due {
                job: nightly(),
                parameters: on("2026-08-06"),
            }
        },
    );

    assert!(scheduled.run_due().await.unwrap().ran());
    // Same run key, so this one is refused — but the job was still built,
    // which is what proves the closure runs per tick rather than once.
    assert!(!scheduled.run_due().await.unwrap().ran());

    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

/// Every firing is counted, including the two that ran nothing — which is the
/// entire reason this metric exists. A nightly job refused for a week emits no
/// `jobs_started` at all, so without this the silence is indistinguishable from
/// a healthy deployment.
#[test]
fn every_outcome_is_counted_under_its_own_label() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    // Current-thread: `with_local_recorder` scopes the recorder to *this*
    // thread, and a multi-thread runtime would emit from a worker that cannot
    // see it — measuring zero and passing every assertion about absence.
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let launcher = JobLauncher::new(InMemoryJobRepository::new());
                let today = on("2026-08-06");

                trigger(&launcher, &mut nightly(), &today).await.unwrap();
                trigger(&launcher, &mut nightly(), &today).await.unwrap();

                let mut boom = Job::builder("doomed").step(Boom).build();
                assert!(trigger(&launcher, &mut boom, &on("x")).await.is_err());

                let overlapping = on("2026-08-07");
                let instance = launcher
                    .repository()
                    .find_or_create_instance("nightly", &overlapping)
                    .await
                    .unwrap();
                launcher
                    .repository()
                    .create_execution(instance.id())
                    .await
                    .unwrap();
                trigger(&launcher, &mut nightly(), &overlapping)
                    .await
                    .unwrap();
            });
    });

    let snapshot = snapshotter.snapshot().into_vec();

    assert_eq!(count(&snapshot, OUTCOME_RAN), Some(1));
    assert_eq!(count(&snapshot, OUTCOME_ALREADY_COMPLETE), Some(1));
    assert_eq!(count(&snapshot, OUTCOME_ALREADY_RUNNING), Some(1));
    assert_eq!(count(&snapshot, OUTCOME_FAILED), Some(1));
}

/// The `TRIGGERS` counter whose `outcome` label is `outcome`.
fn count(
    snapshot: &[(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )],
    outcome: &str,
) -> Option<u64> {
    snapshot
        .iter()
        .find_map(|(composite, _unit, _help, value)| match value {
            DebugValue::Counter(n)
                if composite.key().name() == TRIGGERS
                    && composite
                        .key()
                        .labels()
                        .any(|label| label.key() == LABEL_OUTCOME && label.value() == outcome) =>
            {
                Some(*n)
            }
            _ => None,
        })
}
