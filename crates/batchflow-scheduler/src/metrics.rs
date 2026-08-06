//! Metric names and label values emitted when a schedule fires.
//!
//! Separate from [`batchflow_core::metrics`] because a trigger is not a run: a
//! schedule that fires and is *refused* produces no `JobExecution` at all, so
//! there is nothing in the core vocabulary that could describe it. That is
//! precisely the case an operator needs to see — a nightly job whose instance
//! was already complete has been silently doing nothing since the day it was
//! deployed.
//!
//! Same two rules as Phase 12. Label values are bounded and author-written; ids
//! never become labels. And a label is used where summing across its values is
//! meaningful: `ran + already_complete + already_running + failed` is the number
//! of times the schedule fired, which is a number worth graphing.

/// Schedule firings, by what came of them.
/// Labels: [`LABEL_JOB`], [`LABEL_OUTCOME`].
pub const TRIGGERS: &str = "batchflow_triggers_total";

/// The job's name, matching [`batchflow_core::metrics::LABEL_JOB`] so the two
/// vocabularies join.
pub const LABEL_JOB: &str = "job";

/// What the trigger produced: one of the four `OUTCOME_*` values.
pub const LABEL_OUTCOME: &str = "outcome";

/// The launch was accepted and the job ran. Says nothing about whether it
/// succeeded — `batchflow_jobs_finished_total{status}` answers that.
pub const OUTCOME_RAN: &str = "ran";
/// Refused: this instance has already completed (FR-4.4).
pub const OUTCOME_ALREADY_COMPLETE: &str = "already_complete";
/// Refused: a previous execution of this instance is still running.
pub const OUTCOME_ALREADY_RUNNING: &str = "already_running";
/// The launch was accepted and the job, or the metadata store, failed.
pub const OUTCOME_FAILED: &str = "failed";

/// Registers help text and units for this crate's metrics.
///
/// Call after installing a recorder, and alongside
/// [`batchflow_core::metrics::describe`] — describing before a recorder exists
/// writes the text nowhere.
pub fn describe() {
    use ::metrics::{Unit, describe_counter};

    describe_counter!(
        TRIGGERS,
        Unit::Count,
        "Schedule firings, by outcome. `already_complete` and `already_running` \
         are refusals, not errors: the schedule fired and no job ran."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the *values*. These are a published contract exactly like a wire
    /// format — 13c found that renaming a metric constant leaves the whole
    /// suite green while every dashboard goes blank, because the emit site and
    /// the assertion move together.
    #[test]
    fn the_vocabulary_is_a_published_contract() {
        assert_eq!(TRIGGERS, "batchflow_triggers_total");
        assert_eq!(LABEL_JOB, "job");
        assert_eq!(LABEL_OUTCOME, "outcome");

        assert_eq!(OUTCOME_RAN, "ran");
        assert_eq!(OUTCOME_ALREADY_COMPLETE, "already_complete");
        assert_eq!(OUTCOME_ALREADY_RUNNING, "already_running");
        assert_eq!(OUTCOME_FAILED, "failed");
    }

    /// The join with core's vocabulary is the whole reason these two metric
    /// families are usable together; a divergence would partition the same job
    /// under two different label keys.
    #[test]
    fn the_job_key_matches_the_core_label() {
        assert_eq!(LABEL_JOB, batchflow_core::metrics::LABEL_JOB);
    }
}
