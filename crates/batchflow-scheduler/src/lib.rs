//! # BatchFlow Scheduler
//!
//! Adapters that let an external scheduler trigger a BatchFlow job (FR-9.1).
//!
//! **There is no cron engine here, deliberately (ADR-006).** Every production
//! deployment already has something that knows how to fire things on a
//! schedule, survive a reboot and alert when a tick is missed — Kubernetes,
//! systemd timers, Airflow, a queue. Writing a fourth one inside a batch
//! framework means reimplementing leader election and missed-tick policy in the
//! wrong place. What is missing is smaller and specific: **the three-way
//! classification of what a firing produced.**
//!
//! [`JobLauncher::run`](batchflow_core::JobLauncher::run) reports "this
//! instance already completed" and "another execution is still running" as
//! errors, which is right for a caller that asked for a run. For a *schedule*
//! they are the system working correctly, and [`trigger`] is the ten lines that
//! say so:
//!
//! ```
//! use batchflow_core::{InMemoryJobRepository, Job, JobLauncher, JobParameter, JobParameters};
//! use batchflow_scheduler::{Outcome, trigger};
//! # use batchflow_core::{async_trait, BatchError, ExecutionContext, Step, StepCommit};
//! # struct Noop;
//! # #[async_trait]
//! # impl Step for Noop {
//! #     fn name(&self) -> &str { "noop" }
//! #     async fn run(&mut self, _c: &mut ExecutionContext, _k: &mut dyn StepCommit)
//! #         -> Result<(), BatchError> { Ok(()) }
//! # }
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let launcher = JobLauncher::new(InMemoryJobRepository::new());
//! let today = JobParameters::new().with("date", JobParameter::String("2026-08-06".into()));
//!
//! let mut job = Job::builder("nightly").step(Noop).build();
//! assert!(trigger(&launcher, &mut job, &today).await?.ran());
//!
//! // The same tick fired again — a missed-tick retry, a second replica, an
//! // operator re-running by hand. Not an error, and not a second run.
//! let mut job = Job::builder("nightly").step(Noop).build();
//! let outcome = trigger(&launcher, &mut job, &today).await?;
//! assert!(matches!(outcome, Outcome::AlreadyComplete { .. }));
//! # Ok::<_, BatchError>(())
//! # }).unwrap();
//! ```
//!
//! ## The run key is the whole design
//!
//! `JobParameters` is what makes a schedule idempotent. Deriving them from the
//! tick — `date=2026-08-06` for a nightly job, `hour=2026-08-06T14` for an
//! hourly one — means a re-fire resolves to the same
//! [`JobInstance`](batchflow_core::JobInstance) and is refused by FR-4.4, while
//! the *next* tick resolves to a new one and runs. Constant parameters give a
//! job that runs exactly once ever; parameters containing a timestamp give one
//! with no deduplication at all and no restart, since every attempt is a new
//! instance. Neither is usually what a schedule wants.
//!
//! ## Kubernetes, systemd, or anything else that runs a process
//!
//! No feature flag needed: build the parameters from the clock, call
//! [`trigger`], and map the outcome onto an exit code. A refusal is exit 0 —
//! the work is done or in hand, and a `CronJob` that exits non-zero here will
//! be restarted by the controller into the same refusal.
//!
//! ## In-process cron
//!
//! Enable the `cron` feature for the [`tokio-cron-scheduler`] adapter, which is
//! appropriate when the batch process is a long-lived service rather than a
//! container per run. See [`ScheduledJob::into_cron_job`].
//!
//! [`tokio-cron-scheduler`]: https://docs.rs/tokio-cron-scheduler
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

#[cfg(feature = "cron")]
mod cron;
pub mod metrics;
mod trigger;

pub use trigger::{Due, Outcome, ScheduledJob, trigger};
