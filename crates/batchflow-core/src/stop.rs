use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A cooperative request to stop at the next commit boundary.
///
/// # Why a signal rather than dropping the future
///
/// Dropping a running job's future does stop it — and leaves
/// `job_execution.status` at `Started`, because the unwind skips the write that
/// records a terminal status. Every later launch of that instance is then
/// refused with
/// [`JobExecutionAlreadyRunning`](crate::BatchError::JobExecutionAlreadyRunning)
/// naming a process that has already exited, recoverable only by an operator
/// calling [`abandon_execution`](crate::JobRepository::abandon_execution). A
/// rolling deploy that lands mid-job does exactly this.
///
/// A stop signal instead ends the step *between* chunks, records
/// [`BatchStatus::Stopped`](crate::BatchStatus) — which the launcher's gate
/// already treats as restartable — and leaves the bookmark at the last
/// committed chunk. The next launch resumes from there. Nothing new is needed
/// to make that work: it is the restart path, unchanged.
///
/// # Checked only at commit boundaries
///
/// Never mid-chunk. A chunk that has been written but not committed must roll
/// back cleanly, and interrupting a user's `write` partway would make
/// cancellation safety that user's problem — which is the obligation this
/// framework exists to avoid imposing.
///
/// The practical consequence: a stop takes effect within one commit interval.
/// A step whose chunk takes ten minutes stops in up to ten minutes, which is
/// another reason the commit interval is an operational decision and not only
/// a throughput one.
///
/// # Usage
///
/// ```no_run
/// # use batchflow_core::{InMemoryJobRepository, Job, JobLauncher, JobParameters, StopSignal};
/// # async fn wiring(mut job: Job) -> Result<(), Box<dyn std::error::Error>> {
/// let stop = StopSignal::new();
/// let launcher = JobLauncher::new(InMemoryJobRepository::new()).with_stop_signal(stop.clone());
///
/// // Hand the other half to whatever decides to stop: a `tokio::signal`
/// // handler for the SIGTERM of a rolling deploy, an admin endpoint, a deadline.
/// tokio::spawn({
///     let stop = stop.clone();
///     async move {
///         wait_for_sigterm().await;
///         stop.request();
///     }
/// });
///
/// // Finishes the chunk in flight, commits it, and returns `BatchError::Stopped`.
/// launcher.run(&mut job, &JobParameters::new()).await?;
/// # Ok(())
/// # }
/// # async fn wait_for_sigterm() {}
/// ```
///
/// Cheap to clone — one `Arc` — and safe to share across tasks and threads.
#[derive(Debug, Clone, Default)]
pub struct StopSignal(Arc<AtomicBool>);

impl StopSignal {
    /// A signal that has not been raised.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask the running job to stop at its next commit boundary.
    ///
    /// Idempotent, and safe to call from any thread or task — including a
    /// signal handler. There is no un-request: a job that has been told to stop
    /// stops, and starting again is a new launch, which is what makes the
    /// decision auditable in the metadata store.
    pub fn request(&self) {
        // `Release`, paired with the `Acquire` in `is_requested`. Nothing is
        // published alongside the flag today, but a future stop *reason* would
        // be, and a `Relaxed` store here would be the bug that hides.
        self.0.store(true, Ordering::Release);
    }

    /// Whether a stop has been requested.
    #[must_use]
    pub fn is_requested(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_signal_is_not_raised() {
        assert!(!StopSignal::new().is_requested());
    }

    /// Clones share one flag — otherwise handing a clone to a signal handler
    /// would raise a flag nothing reads.
    #[test]
    fn a_clone_observes_a_request_made_through_the_original() {
        let signal = StopSignal::new();
        let handle = signal.clone();

        signal.request();

        assert!(handle.is_requested());
    }

    #[test]
    fn requesting_twice_is_harmless() {
        let signal = StopSignal::new();

        signal.request();
        signal.request();

        assert!(signal.is_requested());
    }
}
