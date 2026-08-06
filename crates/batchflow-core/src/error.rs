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
    /// The store records a heartbeat
    /// ([`Timestamps::last_updated`](crate::Timestamps::last_updated)), so a
    /// stale execution is *findable* — but it cannot prove a process is dead,
    /// so clearing this is still an operator decision:
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

/// The longest `exit_message` the engine will store.
///
/// Bounded because the chain includes a user error whose `Display` this crate
/// does not control, and the value goes into a database column. Truncation is
/// marked, so a reader can tell a short message from a clipped one.
const MAX_EXIT_MESSAGE: usize = 2000;

/// Renders an error and its causes for the metadata store.
///
/// The whole chain rather than just the top: the classifier's decision was made
/// on a nested cause, and an operator reading the store needs to see what it
/// saw. A `BatchError::Write` whose message is "Write failed" and nothing else
/// is the situation this exists to end.
#[must_use]
pub fn exit_message(error: &BatchError) -> String {
    use std::error::Error as _;
    use std::fmt::Write as _;

    let mut rendered = error.to_string();
    let mut source = error.source();

    while let Some(cause) = source {
        let text = cause.to_string();

        // Appended only if it is not already there. The wrapping variants
        // interpolate their cause into their own `Display` *and* expose it as
        // `source` — `Write(e)` renders as "Write failed: {e}" — so a naive
        // walk prints the innermost error twice. Others, notably
        // `SkipLimitExceeded`, do not interpolate and genuinely need the
        // append. Testing the rendered text is what covers both without
        // hard-coding which variants behave which way.
        if !rendered.contains(&text) {
            // `write!` to a String is infallible; the `let _` is the honest way
            // to say so without an `unwrap` that implies it might not be.
            let _ = write!(rendered, ": {text}");
        }

        source = cause.source();
    }

    if rendered.len() > MAX_EXIT_MESSAGE {
        // On a char boundary, or this panics on multi-byte input -- which is
        // exactly the sort of message a data error tends to carry.
        let cut = (0..=MAX_EXIT_MESSAGE)
            .rev()
            .find(|at| rendered.is_char_boundary(*at))
            .unwrap_or(0);
        rendered.truncate(cut);
        rendered.push_str(" [truncated]");
    }

    rendered
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

#[cfg(test)]
mod exit_message_tests {
    use super::*;

    /// The whole point: the classifier's verdict was reached on a *nested*
    /// cause, so an operator reading the store has to see the same thing it
    /// saw. Rendering only the top gives "Write failed" and nothing else.
    #[test]
    fn the_whole_cause_chain_is_rendered() {
        let error = BatchError::write(std::io::Error::other("deadlock detected: 40P01"));

        let rendered = exit_message(&error);

        assert!(rendered.contains("Write failed"), "{rendered}");
        assert!(rendered.contains("deadlock detected: 40P01"), "{rendered}");
    }

    /// `CleanupFailed` nests a `BatchError` inside a `BatchError`, which is the
    /// deepest chain the engine builds. Both halves have to survive.
    #[test]
    fn a_nested_batch_error_survives() {
        let error = BatchError::write("boom").with_cleanup(Err(BatchError::repository("reset")));

        let rendered = exit_message(&error);

        assert!(rendered.contains("boom"), "{rendered}");
        assert!(rendered.contains("reset"), "{rendered}");
    }

    /// Bounded, because the chain includes a user error whose `Display` this
    /// crate does not control and the value goes into a database column.
    #[test]
    fn an_enormous_message_is_truncated_and_says_so() {
        let error = BatchError::write("x".repeat(10_000));

        let rendered = exit_message(&error);

        assert!(rendered.len() <= MAX_EXIT_MESSAGE + " [truncated]".len());
        assert!(
            rendered.ends_with(" [truncated]"),
            "truncation must be visible"
        );
    }

    /// Truncating by byte index panics mid-character otherwise — and a message
    /// carrying non-ASCII is exactly what a data-quality error looks like.
    #[test]
    fn truncation_lands_on_a_character_boundary() {
        // 3 bytes per char, so the cut lands mid-character unless it is nudged.
        let error = BatchError::write("日".repeat(5_000));

        let rendered = exit_message(&error);

        assert!(rendered.ends_with(" [truncated]"));
        // Reaching here at all is the assertion: `String::truncate` panics on a
        // non-boundary index.
        assert!(rendered.len() <= MAX_EXIT_MESSAGE + " [truncated]".len());
    }

    /// The control: a short message is passed through untouched, so nothing
    /// carries a truncation marker it did not earn.
    ///
    /// It also pins the no-duplication rule. `BatchError::Process` renders as
    /// `"Process failed: {cause}"` *and* exposes that cause as `source`, so a
    /// naive chain walk yields "Process failed: bad row 7: bad row 7" — which
    /// this test caught.
    #[test]
    fn a_short_message_is_left_alone() {
        let rendered = exit_message(&BatchError::process("bad row 7"));

        assert_eq!(rendered, "Process failed: bad row 7");
        assert!(!rendered.contains("truncated"));
    }

    /// The other half of that rule: a variant whose `Display` does *not*
    /// interpolate its cause must still get it appended, or the detail is lost.
    /// `SkipLimitExceeded` renders only "skip limit of N exceeded".
    #[test]
    fn a_cause_the_display_omits_is_still_appended() {
        let error = BatchError::SkipLimitExceeded {
            limit: 3,
            cause: "bad row 7".into(),
        };

        let rendered = exit_message(&error);

        assert!(rendered.contains("skip limit of 3 exceeded"), "{rendered}");
        assert!(
            rendered.contains("bad row 7"),
            "the tipping error must survive: {rendered}"
        );
    }
}
