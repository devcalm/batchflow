use crate::BatchError;

/// What the engine should do about one error.
///
/// A closed enum: the engine must handle every case, so a new one would be a
/// breaking change — which is correct, because a policy the engine cannot act
/// on is not a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorAction {
    /// Transient — the same operation may succeed on another attempt.
    Retry,
    /// This item is bad; the rest of the chunk is fine.
    Skip,
    /// Nothing here is recoverable. Fail the step.
    Fail,
}

/// Treats every error as fatal — the default, and the only safe one for a
/// framework that cannot know which of a backend's failures are transient.
#[derive(Debug, Clone, Copy, Default)]
pub struct FailFast;

/// Decides whether an error is worth retrying, worth skipping, or fatal.
///
/// This is where backend knowledge lives: `batchflow-core` never learns what a
/// SQLSTATE is, and a Postgres classifier maps `40001` to
/// [`Retry`](ErrorAction::Retry) by downcasting a [`Cause`](crate::Cause).
///
/// `&self`, not `&mut self`: a verdict that depended on call history would be
/// neither shareable across steps nor testable in isolation.
pub trait Classifier: Send + Sync {
    /// Classifies one error.
    fn classify(&self, error: &BatchError) -> ErrorAction;
}

impl Classifier for FailFast {
    fn classify(&self, _error: &BatchError) -> ErrorAction {
        ErrorAction::Fail
    }
}
