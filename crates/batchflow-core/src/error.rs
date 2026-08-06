use crate::{BatchStatus, JobExecutionId, JobInstanceId};
use thiserror::Error;

/// The underlying cause carried by the wrapping variants.
///
/// Boxed and erased so core stays free of backend error types, but preserved
/// rather than stringified: a [`Classifier`](crate::Classifier) decides retry
/// vs. skip vs. fail by downcasting this back to the concrete error.
pub type Cause = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Everything the engine can fail with.
///
/// The four wrapping variants carry a [`Cause`] for a
/// [`Classifier`](crate::Classifier) to inspect; the rest are domain errors,
/// which exist because callers branch on them — a scheduler must tell "already
/// ran today, skip" from "the database is down, page someone".
///
/// `#[non_exhaustive]`: matching on it needs a wildcard arm, so new variants are
/// not a breaking change.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BatchError {
    /// An [`ItemReader`](crate::ItemReader) failed.
    #[error("Read failed: {0}")]
    Read(#[source] Cause),

    /// An [`ItemWriter`](crate::ItemWriter) failed.
    #[error("Write failed: {0}")]
    Write(#[source] Cause),

    /// An [`ItemProcessor`](crate::ItemProcessor) failed.
    #[error("Process failed: {0}")]
    Process(#[source] Cause),

    /// The metadata store failed.
    #[error("Repository failed: {0}")]
    Repository(#[source] Cause),

    /// FR-4.4: this instance already succeeded, so re-running it would redo
    /// billed work. A scheduler should treat this as "nothing to do".
    #[error("job instance '{job_name}' ({instance_id:?}) is already complete")]
    JobInstanceAlreadyComplete {
        /// Name of the job that was launched.
        job_name: String,
        /// The instance the parameters resolved to.
        instance_id: JobInstanceId,
    },

    /// Another execution of this instance is `Starting` or `Started`.
    ///
    /// With no heartbeat (Phase 12) the repository cannot tell "running" from
    /// "crashed", so clearing this is an operator decision:
    /// [`abandon_execution`](crate::JobRepository::abandon_execution).
    #[error(
        "job '{job_name}' already has a running execution ({execution_id:?}); \
         if that process is dead, abandon it to unblock this instance"
    )]
    JobExecutionAlreadyRunning {
        /// Name of the job that was launched.
        job_name: String,
        /// The execution already holding the instance.
        execution_id: JobExecutionId,
    },

    /// Abandoning was refused because the execution is not in an abandonable
    /// status — notably `Completed`, which would make a finished instance
    /// relaunchable in two calls.
    #[error("cannot abandon execution {execution_id:?}: it is {status:?}")]
    CannotAbandon {
        /// The execution that was asked to be abandoned.
        execution_id: JobExecutionId,
        /// The status it was actually in.
        status: BatchStatus,
    },

    /// The step gave up because too many items were skipped.
    ///
    /// Distinct from the item error it wraps, because the operational response
    /// differs: one bad row is a data-quality nit, five hundred means the input
    /// is wrong and re-running will not help. The offending error is the
    /// `source`, so the detail is not lost.
    #[error("skip limit of {limit} exceeded")]
    SkipLimitExceeded {
        /// The configured limit that was passed.
        limit: usize,
        /// The item error that tipped it over.
        #[source]
        cause: Cause,
    },

    /// Something failed while the engine was handling an earlier failure.
    ///
    /// Both are kept because they call for different responses. `cause` is what
    /// actually went wrong. `during_cleanup` means the engine could not finish
    /// tidying up — typically that a terminal status never reached the store, so
    /// the metadata still shows `Started` and the next launch will be refused
    /// with [`JobExecutionAlreadyRunning`](BatchError::JobExecutionAlreadyRunning)
    /// until an operator calls
    /// [`abandon_execution`](crate::JobRepository::abandon_execution).
    ///
    /// `cause` is the [`Error::source`](std::error::Error::source), so a
    /// [`Classifier`](crate::Classifier) walking the chain still reaches the
    /// original backend error and classifies on the real failure.
    ///
    /// Note the chain node is a `Box<BatchError>`, not a `BatchError`: walking
    /// with `source()` and downcasting to a concrete backend error works, but a
    /// mid-chain `downcast_ref::<BatchError>()` would need the boxed type.
    #[error("{cause}; while handling it, cleanup also failed: {during_cleanup}")]
    CleanupFailed {
        /// What originally went wrong.
        #[source]
        cause: Box<BatchError>,
        /// What failed while responding to it.
        during_cleanup: Box<BatchError>,
    },

    /// A [`StopSignal`](crate::StopSignal) was raised and the step ended at a
    /// commit boundary.
    ///
    /// Not a failure: the work that committed is durable and the bookmark sits
    /// just past it, so relaunching the same instance resumes rather than
    /// repeats. It is an `Err` because it is *not success* — a step that
    /// stopped did not finish, and returning `Ok` would let
    /// [`Job::run`](crate::Job::run) mark it `Completed` and skip it on the
    /// restart that is supposed to finish it.
    ///
    /// The execution is persisted as
    /// [`BatchStatus::Stopped`](crate::BatchStatus::Stopped), which the
    /// launcher's gate already accepts as restartable.
    #[error("stopped at a commit boundary on request")]
    Stopped,

    /// User code panicked and the engine caught it at its boundary.
    ///
    /// A panic is a bug, never a signal — this variant exists so that one bad
    /// row cannot wedge an instance. Without the boundary the unwind skips the
    /// write that records a terminal status, and the metadata store is left
    /// showing `Started` for a process that has died, which no later launch can
    /// get past without an operator calling
    /// [`abandon_execution`](crate::JobRepository::abandon_execution).
    ///
    /// Carries the panic message rather than the payload: a `Box<dyn Any>` is
    /// neither `Sync` nor `Error`, so it cannot be a [`Cause`].
    ///
    /// Inert under `panic = "abort"`, where there is no unwinding to catch.
    #[error("panicked: {detail}")]
    Panic {
        /// What `panic!` was called with, if it was a string.
        detail: String,
    },

    /// A bookmark held the wrong type — a garbled context, not a missing one.
    ///
    /// Kept distinct from "key absent" so a corrupt bookmark aborts the run
    /// instead of silently restarting from zero and rewriting every committed
    /// item.
    #[error("execution context key '{key}' holds a {actual}, expected {expected}")]
    ExecutionContextType {
        /// The key that was read.
        key: String,
        /// The type the caller asked for.
        expected: &'static str,
        /// The type actually stored.
        actual: &'static str,
    },
}

/// Constructors for the wrapping variants.
///
/// `impl Into<Cause>` accepts a `&str`, a `String` or any concrete error, so
/// callers never spell the box. Prefer passing the error itself — a
/// `to_string()` here is the information a `Classifier` later needs and cannot
/// recover.
impl BatchError {
    /// Wraps a reader failure.
    #[must_use]
    pub fn read(cause: impl Into<Cause>) -> Self {
        Self::Read(cause.into())
    }

    /// Wraps a writer failure.
    #[must_use]
    pub fn write(cause: impl Into<Cause>) -> Self {
        Self::Write(cause.into())
    }

    /// Wraps a processor failure.
    #[must_use]
    pub fn process(cause: impl Into<Cause>) -> Self {
        Self::Process(cause.into())
    }

    /// Wraps a metadata-store failure.
    #[must_use]
    pub fn repository(cause: impl Into<Cause>) -> Self {
        Self::Repository(cause.into())
    }

    /// Keeps `self` as the reported failure, preserving `cleanup` if it failed
    /// too.
    ///
    /// Rust has no `finally`, so a lifecycle method that writes `cleanup?`
    /// silently discards the error it was cleaning up after — the caller is
    /// told the rollback failed and never learns why there was one. Bind the
    /// cleanup result and pass it here instead:
    ///
    /// ```
    /// # use batchflow_core::BatchError;
    /// let outcome = BatchError::write("deadlock");
    /// let cleanup = Err(BatchError::repository("connection reset"));
    ///
    /// let reported = outcome.with_cleanup(cleanup);
    /// assert!(matches!(reported, BatchError::CleanupFailed { .. }));
    /// ```
    ///
    /// An `Ok` cleanup returns `self` untouched, so call sites read the same
    /// either way.
    #[must_use]
    pub fn with_cleanup(self, cleanup: Result<(), BatchError>) -> Self {
        match cleanup {
            Ok(()) => self,
            Err(during_cleanup) => Self::CleanupFailed {
                cause: Box::new(self),
                during_cleanup: Box::new(during_cleanup),
            },
        }
    }
}
