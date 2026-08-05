use crate::{BatchError, Classifier, ErrorAction, FailFast};
use backon::{BackoffBuilder, ExponentialBuilder};
use std::num::NonZeroU32;
use std::time::Duration;

/// How many times a transient failure may be re-attempted, and how long to wait
/// between attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    min_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    /// Try once and give up.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: NonZeroU32::MIN,
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        }
    }

    /// `max_attempts` counts *total* attempts, not retries after the first:
    /// `attempts(3)` writes at most three times.
    ///
    /// `NonZeroU32` for the same reason `chunk_size` is `NonZeroUsize` — zero
    /// attempts would skip the write and report the chunk done.
    #[must_use]
    pub fn attempts(max_attempts: NonZeroU32) -> Self {
        Self {
            max_attempts,
            ..Self::none()
        }
    }

    /// The first wait, doubled on each subsequent attempt up to `max_delay`.
    #[must_use]
    pub fn min_delay(mut self, min_delay: Duration) -> Self {
        self.min_delay = min_delay;
        self
    }

    /// The ceiling on any single wait. Without one, exponential growth turns a
    /// long-lived outage into a job that appears hung.
    #[must_use]
    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Total attempts allowed, including the first.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts.get()
    }

    /// The wait schedule for one chunk's attempts.
    ///
    /// Deliberately unbounded: [`FaultTolerance::should_retry`] is the single
    /// authority on *how many* attempts there are. A schedule that also enforced
    /// the limit would be a second copy of that rule, free to disagree with the
    /// first — and the failure mode is silent (an exhausted iterator skips the
    /// sleep rather than stopping the retry).
    ///
    /// Jittered, so that a hundred workers deadlocked against each other do not
    /// all wake at the same instant and deadlock again.
    pub(crate) fn backoff(&self) -> impl Iterator<Item = Duration> {
        ExponentialBuilder::default()
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(usize::MAX)
            .with_jitter()
            .build()
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::none()
    }
}

/// What to do about an error raised by a single item.
///
/// Returned by [`FaultTolerance::disposition`], which takes the error by value:
/// either it is discarded along with the item, or it comes back out in `Fail`.
/// There is no third path where a caller forgets to check the limit.
#[derive(Debug)]
pub enum ItemDisposition {
    /// Drop this item, count it, carry on with the chunk.
    Skip(BatchError),
    /// Give up on the step, with this error.
    Fail(BatchError),
}

/// A step's fault-tolerance configuration: how errors are classified, how often
/// a transient one may be re-attempted, and how many bad items are tolerated.
pub struct FaultTolerance {
    classifier: Box<dyn Classifier>,
    retry: RetryPolicy,
    skip_limit: usize,
}

impl Default for FaultTolerance {
    /// Fail on the first error — the same starting point as Spring Batch
    /// before `faultTolerant()` is called.
    fn default() -> Self {
        Self {
            classifier: Box::new(FailFast),
            retry: RetryPolicy::none(),
            skip_limit: 0,
        }
    }
}

/// Hand-written because [`Classifier`] is a trait object, and per ADR-008 a
/// `Debug` supertrait would tax every user's impl for the sake of a diagnostic.
/// The classifier prints as a placeholder; the settings that get tuned print in
/// full.
impl std::fmt::Debug for FaultTolerance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultTolerance")
            .field("classifier", &format_args!("<dyn Classifier>"))
            .field("retry", &self.retry)
            .field("skip_limit", &self.skip_limit)
            .finish()
    }
}

impl FaultTolerance {
    /// A fail-fast policy: no retries, no skips.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the classifier that decides retry vs. skip vs. fail.
    #[must_use]
    pub fn classifier(mut self, classifier: impl Classifier + 'static) -> Self {
        self.classifier = Box::new(classifier);
        self
    }

    /// Sets how many attempts a retryable chunk gets, and how long it waits.
    #[must_use]
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// How many skippable item failures a step tolerates before failing.
    ///
    /// Counted across the whole step, not per chunk — one bad row in each of a
    /// thousand chunks is a broken input file, and a per-chunk limit would call
    /// it healthy.
    #[must_use]
    pub fn skip_limit(mut self, skip_limit: usize) -> Self {
        self.skip_limit = skip_limit;
        self
    }

    /// Whether `error`, seen on 1-based `attempt`, gets another try.
    ///
    /// [`ErrorAction::Skip`] answers `false` here. A write error names a whole
    /// chunk, not an item, so there is nothing this can single out to skip —
    /// isolating the poison item needs chunk-scanning (FR-6.4, Phase 10d).
    pub fn should_retry(&self, error: &BatchError, attempt: u32) -> bool {
        self.classifier.classify(error) == ErrorAction::Retry && attempt < self.retry.max_attempts()
    }

    /// What to do about `error` from one item, given how many have already been
    /// skipped in this step.
    ///
    /// Takes `error` by value so the two outcomes are exhaustive: the error is
    /// either dropped with its item or handed back inside
    /// [`ItemDisposition::Fail`]. Exceeding the limit wraps it in
    /// [`BatchError::SkipLimitExceeded`] rather than reporting the bare item
    /// error, so "one bad row" and "this file is garbage" are distinguishable
    /// at the call site that has to page someone.
    pub fn disposition(&self, error: BatchError, skipped: usize) -> ItemDisposition {
        if self.classifier.classify(&error) != ErrorAction::Skip {
            return ItemDisposition::Fail(error);
        }

        if skipped < self.skip_limit {
            ItemDisposition::Skip(error)
        } else {
            ItemDisposition::Fail(BatchError::SkipLimitExceeded {
                limit: self.skip_limit,
                cause: error.into(),
            })
        }
    }

    pub(crate) fn backoff(&self) -> impl Iterator<Item = Duration> {
        self.retry.backoff()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{RetryAll, SkipAll, nz32};

    #[test]
    fn the_default_retries_nothing() {
        let fault = FaultTolerance::default();

        assert!(!fault.should_retry(&BatchError::write("boom"), 1));
    }

    /// `attempts(n)` is a budget of `n` tries, so the `n`th failure is final.
    #[test]
    fn attempts_are_a_total_not_a_remainder() {
        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)));

        assert!(fault.should_retry(&BatchError::write("boom"), 1));
        assert!(fault.should_retry(&BatchError::write("boom"), 2));
        assert!(!fault.should_retry(&BatchError::write("boom"), 3));
    }

    /// A generous retry budget is not permission to retry a fatal error.
    #[test]
    fn the_classifier_decides_before_the_budget_does() {
        let fault = FaultTolerance::new().retry(RetryPolicy::attempts(nz32(10)));

        assert!(!fault.should_retry(&BatchError::write("boom"), 1));
    }

    /// The default skips nothing, so an unconfigured step still fails fast.
    #[test]
    fn the_default_skips_nothing() {
        let fault = FaultTolerance::default();

        assert!(matches!(
            fault.disposition(BatchError::process("bad"), 0),
            ItemDisposition::Fail(BatchError::Process(_))
        ));
    }

    /// `skip_limit(n)` tolerates exactly `n` items: the `n`th skip is allowed,
    /// the one after it is not.
    #[test]
    fn the_limit_is_a_count_of_items_tolerated() {
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(2);

        assert!(matches!(
            fault.disposition(BatchError::process("bad"), 1),
            ItemDisposition::Skip(_)
        ));
        assert!(matches!(
            fault.disposition(BatchError::process("bad"), 2),
            ItemDisposition::Fail(BatchError::SkipLimitExceeded { .. })
        ));
    }

    /// Over the limit, the item error becomes the *source* of a
    /// `SkipLimitExceeded` rather than being reported bare or discarded.
    #[test]
    fn the_item_error_survives_as_the_cause() {
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(0);

        let ItemDisposition::Fail(error) = fault.disposition(BatchError::process("bad row 7"), 0)
        else {
            panic!("expected a failure past the limit");
        };

        assert!(
            std::error::Error::source(&error)
                .is_some_and(|cause| { cause.to_string().contains("bad row 7") })
        );
    }

    /// A skippable error is never retried and a retryable one is never skipped —
    /// the classifier's verdict routes it to exactly one of the two policies.
    #[test]
    fn the_two_policies_do_not_overlap() {
        let skipping = FaultTolerance::new()
            .classifier(SkipAll)
            .skip_limit(10)
            .retry(RetryPolicy::attempts(nz32(10)));

        assert!(!skipping.should_retry(&BatchError::write("boom"), 1));

        let retrying = FaultTolerance::new()
            .classifier(RetryAll)
            .skip_limit(10)
            .retry(RetryPolicy::attempts(nz32(10)));

        assert!(matches!(
            retrying.disposition(BatchError::process("boom"), 0),
            ItemDisposition::Fail(_)
        ));
    }
}
