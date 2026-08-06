use batchflow_core::{BatchError, Classifier, ErrorAction};
use std::borrow::Cow;
use std::error::Error;

/// Classifies Postgres errors by SQLSTATE (FR-6.3).
///
/// This is the Rust replacement for Spring Batch's
/// `.retry(DeadlockLoserDataAccessException.class)` — a function instead of a
/// list of exception classes, and the reason [`BatchError`]'s wrapping variants
/// carry the original error rather than its message.
///
/// Opt in per step; the framework default remains `FailFast`:
///
/// ```
/// use batchflow_core::{FaultTolerance, RetryPolicy};
/// use batchflow_postgres::PostgresClassifier;
/// use std::num::NonZeroU32;
///
/// let fault = FaultTolerance::new()
///     .classifier(PostgresClassifier)
///     .retry(RetryPolicy::attempts(NonZeroU32::new(3).unwrap()))
///     .skip_limit(10);
///
/// // Hand this to `ChunkStep::with_fault_tolerance`.
/// # let _ = fault;
/// ```
///
/// Anything that is not a Postgres error, or whose SQLSTATE is not listed
/// below, is [`ErrorAction::Fail`]. A classifier that guesses is worse than one
/// that gives up: guessing `Retry` re-runs work of unknown status, and guessing
/// `Skip` silently discards rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostgresClassifier;

impl Classifier for PostgresClassifier {
    fn classify(&self, error: &BatchError) -> ErrorAction {
        let Some(code) = sqlstate(error) else {
            return ErrorAction::Fail;
        };

        match &*code {
            // Transaction rollback the database chose for us. The statement
            // definitely did not take effect, so re-running it is safe.
            //
            // Enumerated rather than matched on class "40": that class also
            // holds 40003 `statement_completion_unknown`, where the outcome is
            // by definition unknown and a retry may double-write.
            "40001" // serialization_failure
            | "40P01" // deadlock_detected
            | "55P03" // lock_not_available
            => ErrorAction::Retry,

            // Classes 22 (data exception) and 23 (integrity constraint
            // violation). Matched by *class*, because that is the unit the SQL
            // standard defines: both say "this row is wrong", never "the system
            // is unwell". A bad date, an overlong string, a missing foreign key
            // and a duplicate key are all one item's problem.
            _ if code.starts_with("22") || code.starts_with("23") => ErrorAction::Skip,

            // Everything else — including connection failures (class 08) and
            // insufficient resources (class 53). Retrying a dead database just
            // spends the retry budget before failing anyway, and US-3 asks for
            // fast failure on infrastructure errors.
            _ => ErrorAction::Fail,
        }
    }
}

/// Finds the SQLSTATE of the first Postgres error in `error`'s source chain.
///
/// Walks the chain rather than matching on [`BatchError`]'s variants, so it
/// still works when the error is nested — a user's writer wrapping a
/// `sqlx::Error` in its own type, or the engine wrapping an item error in
/// [`BatchError::SkipLimitExceeded`]. Matching variants would also need a
/// wildcard arm, since `BatchError` is `#[non_exhaustive]` outside its crate.
fn sqlstate(error: &BatchError) -> Option<Cow<'_, str>> {
    let mut current: Option<&(dyn Error + 'static)> = Some(error);

    while let Some(source) = current {
        if let Some(database_error) = source
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
        {
            return database_error.code();
        }
        current = source.source();
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An error that never came from a database is not something to guess at.
    #[test]
    fn a_non_database_error_fails() {
        assert_eq!(
            PostgresClassifier.classify(&BatchError::write("disk full")),
            ErrorAction::Fail
        );
    }

    /// A `sqlx` error that carries no SQLSTATE — a pool timeout, a decode
    /// failure — is still not classifiable.
    #[test]
    fn a_sqlx_error_without_a_sqlstate_fails() {
        assert_eq!(
            PostgresClassifier.classify(&BatchError::repository(sqlx::Error::PoolTimedOut)),
            ErrorAction::Fail
        );
    }
}
