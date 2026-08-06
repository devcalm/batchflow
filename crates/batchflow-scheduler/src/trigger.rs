use crate::metrics::{
    LABEL_JOB, LABEL_OUTCOME, OUTCOME_ALREADY_COMPLETE, OUTCOME_ALREADY_RUNNING, OUTCOME_FAILED,
    OUTCOME_RAN, TRIGGERS,
};
use batchflow_core::{
    BatchError, Job, JobExecution, JobExecutionId, JobInstanceId, JobLauncher, JobParameters,
    JobRepository,
};
use std::sync::Arc;

/// What one firing of a schedule produced.
///
/// The point of this type is that **two of the three arms are not errors.**
/// [`JobLauncher::run`] reports "this instance already completed" and "another
/// execution is still running" as [`BatchError`]s, because to a caller that
/// asked for a run they are failures. To a *schedule* they are the system
/// working: a nightly job whose 03:00 run is still going at 04:00 must not be
/// started twice, and a re-fire after a completed run is a no-op by design
/// (FR-4.4). A scheduler that treated them as errors would page someone every
/// night.
///
/// `#[non_exhaustive]`, so a future refusal reason is an additive change. The
/// cost — callers need a wildcard arm — is the same trade [`BatchError`] makes.
#[derive(Debug)]
#[non_exhaustive]
pub enum Outcome {
    /// The launch was accepted. The execution carries its own terminal status;
    /// `Ran` does not mean the job succeeded, only that it was allowed to try.
    Ran(JobExecution),

    /// Refused: this `(job_name, parameters)` has already completed. Firing the
    /// same schedule twice for the same run key is how a scheduler retries a
    /// missed tick, so this is the expected steady state, not a fault.
    AlreadyComplete {
        /// The instance the parameters resolved to.
        instance_id: JobInstanceId,
    },

    /// Refused: a previous execution is `Starting` or `Started`.
    ///
    /// If that process is in fact dead, an operator clears it with
    /// [`JobRepository::abandon_execution`] — the framework cannot tell "still
    /// running" from "crashed" without a heartbeat, and guessing would run two
    /// copies of a billing job.
    AlreadyRunning {
        /// The execution holding the instance.
        execution_id: JobExecutionId,
    },
}

impl Outcome {
    /// Whether a job actually ran. `false` for both refusals.
    #[must_use]
    pub fn ran(&self) -> bool {
        matches!(self, Outcome::Ran(_))
    }

    /// The label value this outcome is counted under.
    fn label(&self) -> &'static str {
        match self {
            Outcome::Ran(_) => OUTCOME_RAN,
            Outcome::AlreadyComplete { .. } => OUTCOME_ALREADY_COMPLETE,
            Outcome::AlreadyRunning { .. } => OUTCOME_ALREADY_RUNNING,
        }
    }
}

/// Fires `job` once, turning the launcher's two *refusal* errors into
/// [`Outcome`]s and leaving every other error alone.
///
/// This is the whole of BatchFlow's scheduling logic, and deliberately so
/// (ADR-006): the framework exposes a launch API and something else decides
/// when to call it. Cron, a Kubernetes `CronJob`, a queue consumer and a
/// hand-run CLI all need this same three-way classification and none of them
/// need an engine from us.
///
/// # Errors
///
/// Anything that is genuinely wrong: the step's own failure, or the metadata
/// store being unreachable. Those still propagate, because a scheduler that
/// swallowed them would report a healthy nightly run that never happened.
pub async fn trigger<R>(
    launcher: &JobLauncher<R>,
    job: &mut Job<R::Tx>,
    parameters: &JobParameters,
) -> Result<Outcome, BatchError>
where
    R: JobRepository,
{
    // Owned before the call: `run` takes `job` mutably, so the name cannot be
    // borrowed across it, and a metric label value has to be owned anyway.
    let job_name = job.name().to_owned();

    let outcome = match launcher.run(job, parameters).await {
        Ok(execution) => Outcome::Ran(execution),

        Err(BatchError::JobInstanceAlreadyComplete { instance_id, .. }) => {
            tracing::info!(
                job = %job_name,
                instance_id = instance_id.get(),
                "schedule fired for an instance that has already completed; nothing to do"
            );
            Outcome::AlreadyComplete { instance_id }
        }

        Err(BatchError::JobExecutionAlreadyRunning { execution_id, .. }) => {
            // WARN, not INFO: one overlap is a slow night, a run of them means
            // the job no longer fits its schedule — which is an operator
            // decision (widen the interval, or partition the input), and it is
            // invisible from the job's own metrics because nothing ran.
            tracing::warn!(
                job = %job_name,
                execution_id = execution_id.get(),
                "schedule fired while a previous execution is still running; skipping"
            );
            Outcome::AlreadyRunning { execution_id }
        }

        Err(error) => {
            ::metrics::counter!(
                TRIGGERS,
                LABEL_JOB => job_name,
                LABEL_OUTCOME => OUTCOME_FAILED,
            )
            .increment(1);
            return Err(error);
        }
    };

    ::metrics::counter!(
        TRIGGERS,
        LABEL_JOB => job_name,
        LABEL_OUTCOME => outcome.label(),
    )
    .increment(1);

    Ok(outcome)
}

/// The job and the run key a schedule is due to launch.
///
/// One value rather than two parameters because they are one decision: the run
/// key *is* what makes this tick distinct from the last one, and a `Due` whose
/// parameters came from a different tick than its job is not a thing that
/// should be constructible by accident.
pub struct Due<Tx = ()> {
    /// A freshly built job. See [`ScheduledJob`] for why it must be fresh.
    pub job: Job<Tx>,
    /// The parameters identifying this run — typically the tick's date or hour.
    /// Equal parameters mean the same [`JobInstance`](batchflow_core::JobInstance),
    /// which is what makes a re-fire a no-op rather than a duplicate run.
    pub parameters: JobParameters,
}

impl<Tx> std::fmt::Debug for Due<Tx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Due")
            .field("job", &self.job)
            .field("parameters", &self.parameters)
            .finish()
    }
}

/// A [`Job`] bound to a launcher and a way to describe the run that is due.
///
/// The closure returns a **freshly built** [`Job`] on every tick, and that is
/// not an implementation detail. A step owns its reader, processor and writer,
/// and [`Job::run`] takes `&mut self`; reusing one job across firings would
/// hand the second tick a reader already positioned at end of input. It is also
/// what a restarted *process* does — a resumed job is always a new object
/// seeded from the metadata store — so building fresh keeps the scheduled path
/// and the restart path identical, with no second code path to get wrong.
pub struct ScheduledJob<R, F> {
    launcher: Arc<JobLauncher<R>>,
    due: F,
}

impl<R, F> ScheduledJob<R, F>
where
    R: JobRepository,
    F: Fn() -> Due<R::Tx>,
{
    /// Binds `due` to `launcher`.
    ///
    /// The launcher is shared rather than owned so several schedules can sit on
    /// one metadata store — the usual arrangement, since instance identity is
    /// only meaningful within one.
    #[must_use]
    pub fn new(launcher: Arc<JobLauncher<R>>, due: F) -> Self {
        Self { launcher, due }
    }

    /// The launcher, for callers that want to query metadata directly.
    pub fn launcher(&self) -> &JobLauncher<R> {
        &self.launcher
    }

    /// Builds this tick's job and fires it.
    ///
    /// # Errors
    ///
    /// As [`trigger`]: refusals become [`Outcome`]s, everything else propagates.
    pub async fn run_due(&self) -> Result<Outcome, BatchError> {
        let Due {
            mut job,
            parameters,
        } = (self.due)();

        trigger(&self.launcher, &mut job, &parameters).await
    }
}

impl<R, F> std::fmt::Debug for ScheduledJob<R, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledJob").finish_non_exhaustive()
    }
}
