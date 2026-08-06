//! The `cron` adapter. Compiled only with the feature that provides it.
#![cfg(feature = "cron")]

use batchflow_core::{
    BatchError, ExecutionContext, InMemoryJobRepository, Job, JobLauncher, JobParameter,
    JobParameters, Step, StepCommit, async_trait,
};
use batchflow_scheduler::{Due, ScheduledJob};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_cron_scheduler::JobScheduler;

struct Counting(Arc<AtomicUsize>);

#[async_trait]
impl Step for Counting {
    fn name(&self) -> &str {
        "counting"
    }

    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit,
    ) -> Result<(), BatchError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn scheduled(
    runs: Arc<AtomicUsize>,
    tick: Arc<AtomicUsize>,
) -> ScheduledJob<InMemoryJobRepository, impl Fn() -> Due + Send + Sync + 'static> {
    let launcher = Arc::new(JobLauncher::new(InMemoryJobRepository::new()));

    ScheduledJob::new(launcher, move || {
        // A distinct run key per firing, standing in for the tick's date. With
        // a constant one the second firing would be refused by FR-4.4 and this
        // test could not tell "the schedule fired twice" from "it fired once".
        let n = tick.fetch_add(1, Ordering::SeqCst);

        Due {
            job: Job::builder("nightly")
                .step(Counting(Arc::clone(&runs)))
                .build(),
            parameters: JobParameters::new().with("tick", JobParameter::Long(n as i64)),
        }
    })
}

#[tokio::test]
async fn a_cron_schedule_fires_the_job() {
    let runs = Arc::new(AtomicUsize::new(0));
    let job = scheduled(Arc::clone(&runs), Arc::new(AtomicUsize::new(0)));

    let mut scheduler = JobScheduler::new().await.unwrap();
    // Every second, so the test costs a couple of seconds rather than a minute.
    scheduler
        .add(job.into_cron_job("* * * * * * *").unwrap())
        .await
        .unwrap();
    scheduler.start().await.unwrap();

    // Real sleeping, not `start_paused`: the scheduler runs its own timer task
    // and a paused clock would auto-advance past every tick at once.
    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
    scheduler.shutdown().await.unwrap();

    let fired = runs.load(Ordering::SeqCst);
    assert!(fired >= 1, "the schedule never fired");
    // Loose upper bound: this asserts the adapter is driven by the schedule and
    // not by a hot loop, without pinning an exact count a busy CI box cannot
    // guarantee.
    assert!(fired <= 5, "fired {fired} times in 2.5 seconds");
}

/// A malformed expression is caught at wiring time, not at 02:00 three weeks
/// later. This is the only failure `into_cron_job` can return.
#[test]
fn an_invalid_expression_is_rejected_when_the_job_is_built() {
    let job = scheduled(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));

    assert!(job.into_cron_job("not a cron expression").is_err());
}
