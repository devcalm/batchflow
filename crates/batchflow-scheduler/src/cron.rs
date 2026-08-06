use crate::{Due, Outcome, ScheduledJob};
use batchflow_core::JobRepository;
use std::sync::Arc;
use tokio_cron_scheduler::{Job as CronJob, JobSchedulerError};

impl<R, F> ScheduledJob<R, F>
where
    R: JobRepository + 'static,
    R::Tx: 'static,
    F: Fn() -> Due<R::Tx> + Send + Sync + 'static,
{
    /// Wraps this schedule into a [`tokio_cron_scheduler::Job`], ready to add to
    /// a `JobScheduler`.
    ///
    /// `schedule` is a seven-field cron expression in UTC — seconds first, so
    /// `"0 0 2 * * * *"` is 02:00 daily, not every second of the second minute.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use batchflow_core::{InMemoryJobRepository, Job, JobLauncher, JobParameter, JobParameters};
    /// # use batchflow_scheduler::{Due, ScheduledJob};
    /// # use tokio_cron_scheduler::JobScheduler;
    /// # async fn wiring(build: impl Fn() -> Job + Send + Sync + 'static) -> Result<(), Box<dyn std::error::Error>> {
    /// let launcher = Arc::new(JobLauncher::new(InMemoryJobRepository::new()));
    ///
    /// let nightly = ScheduledJob::new(launcher, move || Due {
    ///     job: build(),
    ///     // The run key: same day, same instance, so a re-fire is a no-op.
    ///     parameters: JobParameters::new()
    ///         .with("date", JobParameter::String(today())),
    /// });
    ///
    /// let scheduler = JobScheduler::new().await?;
    /// scheduler.add(nightly.into_cron_job("0 0 2 * * * *")?).await?;
    /// scheduler.start().await?;
    /// # Ok(())
    /// # }
    /// # fn today() -> String { unimplemented!() }
    /// ```
    ///
    /// # A tick has nobody to return an error to
    ///
    /// The callback's return type is `()`, so this adapter is the end of the
    /// line for both outcomes and failures: it logs them and emits
    /// [`TRIGGERS`](crate::metrics::TRIGGERS), and there is nowhere else for
    /// them to go. That makes the Phase 12/13 vocabulary the *only* channel
    /// here, not a supplement to one — an unmonitored in-process schedule can
    /// fail every night in complete silence. Deployments that would rather see
    /// a failing exit code should run [`trigger`](crate::trigger) from `main`
    /// under an external scheduler instead.
    ///
    /// # Overlapping ticks
    ///
    /// A tick that fires while the previous run is still going is refused by
    /// the launcher and logged as [`Outcome::AlreadyRunning`]. The refusal is
    /// the metadata store's, not the scheduler's, so it also holds across
    /// replicas — two processes on the same store cannot both run one instance.
    ///
    /// # Errors
    ///
    /// [`JobSchedulerError`] if `schedule` is not a valid cron expression.
    /// Every *runtime* failure is logged rather than returned, per above.
    pub fn into_cron_job(self, schedule: &str) -> Result<CronJob, JobSchedulerError> {
        // `Arc`, because `new_async` wants an `FnMut` it can call on every tick
        // while the future from the previous one may still be alive.
        let scheduled = Arc::new(self);

        CronJob::new_async(schedule, move |_uuid, _lock| {
            let scheduled = Arc::clone(&scheduled);

            Box::pin(async move {
                match scheduled.run_due().await {
                    // The refusals already logged themselves, with the detail
                    // (instance id, execution id) this site does not have.
                    Ok(Outcome::Ran(execution)) => tracing::info!(
                        execution_id = execution.id().get(),
                        status = ?execution.status(),
                        "scheduled job ran"
                    ),
                    Ok(_) => {}
                    Err(error) => tracing::error!(
                        error = %error,
                        "scheduled job failed; no caller will see this error"
                    ),
                }
            })
        })
    }
}
