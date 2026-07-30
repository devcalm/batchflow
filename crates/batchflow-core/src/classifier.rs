use crate::BatchError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorAction {
    /// Transient — the same operation may succeed on another attempt.
    Retry,
    /// This item is bad; the rest of the chunk is fine.
    Skip,
    /// Nothing here is recoverable. Fail the step.
    Fail,
}

pub struct FailFast;

pub trait Classifier: Send + Sync {
    fn classify(&self, error: &BatchError) -> ErrorAction;
}

impl Classifier for FailFast {
    fn classify(&self, _error: &BatchError) -> ErrorAction {
        ErrorAction::Fail
    }
}
