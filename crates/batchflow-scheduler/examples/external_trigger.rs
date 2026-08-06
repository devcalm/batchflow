//! What a Kubernetes `CronJob`, a systemd timer or a shell cron entry does:
//! build the run key from the clock, fire once, exit.
//!
//! Run with `cargo run -p batchflow-scheduler --example external_trigger`.
//!
//! The four firings below are the four cases a schedule actually meets in
//! production, in the order it meets them — and only the first does any work.

use batchflow_core::{
    BatchError, ChunkStep, InMemoryJobRepository, ItemProcessor, ItemReader, ItemWriter, Job,
    JobLauncher, JobParameter, JobParameters, JobRepository, Unmanaged,
};
use batchflow_scheduler::{Outcome, trigger};
use std::num::NonZeroUsize;

struct Rows(std::vec::IntoIter<u32>);

impl ItemReader for Rows {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        Ok(self.0.next())
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

struct Print;

impl ItemWriter for Print {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        println!("    wrote chunk {items:?}");
        Ok(())
    }
}

/// A fresh job per tick. A step owns its reader, so handing a second firing the
/// same job would hand it a reader already at end of input.
fn nightly() -> Job {
    Job::builder("nightly")
        .step(ChunkStep::new(
            "double",
            Rows(vec![1, 2, 3, 4, 5].into_iter()),
            Double,
            Unmanaged(Print),
            NonZeroUsize::new(2).unwrap(),
        ))
        .build()
}

/// The run key. In a real deployment this is the tick's date — `date +%F` in the
/// CronJob's command, or `chrono::Utc::now().date_naive()` in-process.
fn run_key(date: &str) -> JobParameters {
    JobParameters::new().with("date", JobParameter::String(date.into()))
}

/// Every [`Outcome`] is a success, refusals included: the work is either done
/// or in hand, and a container that exits non-zero here is restarted straight
/// back into the same refusal.
///
/// The non-zero exits come from the `?`s below — a step that failed, or a
/// metadata store that could not be reached. Those are the only two things a
/// schedule should be alerted about.
#[tokio::main]
async fn main() -> Result<(), BatchError> {
    let launcher = JobLauncher::new(InMemoryJobRepository::new());

    println!("06:00 — the scheduled tick");
    let outcome = trigger(&launcher, &mut nightly(), &run_key("2026-08-06")).await?;
    println!("  {outcome:?}\n");

    println!("06:05 — the node was replaced and the tick re-fired");
    let outcome = trigger(&launcher, &mut nightly(), &run_key("2026-08-06")).await?;
    assert!(
        matches!(outcome, Outcome::AlreadyComplete { .. }),
        "the same run key must not run twice"
    );
    println!("  {outcome:?}\n");

    println!("06:05 — a second replica fired the same tick");
    // Same instance, still complete: replicas deduplicate through the metadata
    // store, not through anything the schedulers agree between themselves.
    let outcome = trigger(&launcher, &mut nightly(), &run_key("2026-08-06")).await?;
    assert!(!outcome.ran());
    println!("  {outcome:?}\n");

    println!("tomorrow 06:00 — a new run key");
    let outcome = trigger(&launcher, &mut nightly(), &run_key("2026-08-07")).await?;
    assert!(outcome.ran(), "a new day is a new instance");
    println!("  {outcome:?}\n");

    // Every attempt is on the record, including the ones that ran nothing:
    // the two refusals created no execution at all, which is exactly why
    // `batchflow_triggers_total` exists.
    for date in ["2026-08-06", "2026-08-07"] {
        let instance = launcher
            .repository()
            .find_instance("nightly", &run_key(date))
            .await?
            .expect("every trigger resolves an instance");

        let executions = launcher.repository().executions(instance.id()).await?;
        println!("{date}: {} execution(s) recorded", executions.len());
    }

    Ok(())
}
