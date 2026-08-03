//! Span names and field keys emitted by the engine.
//!
//! Nothing is recorded until the application installs a subscriber; until then
//! every emit site is a check against a global. This module is the stable
//! vocabulary an operator writes queries against, and the counterpart to
//! [`metrics`](crate::metrics) — which deliberately carries *no* execution ids,
//! because one label value per run mints one time series per run. Spans are
//! where those ids belong: high cardinality is the point, and correlating a
//! single run is the question they exist to answer.
//!
//! # Spans
//!
//! Two, nested: [`SPAN_JOB`] over one `JobExecution`, and [`SPAN_STEP`] over
//! each step's `run` inside it. There is deliberately **no chunk span** — a job
//! of 10M items at a commit interval of 100 would emit 100,000 of them per run,
//! and the question one would answer is already answered, aggregatably, by
//! `batchflow_chunk_duration_seconds`. Chunk-level detail is events instead:
//! a chunk that commits normally is not interesting, and a chunk that retried
//! three times is.
//!
//! # Field names are literals in the engine, deliberately
//!
//! The `SPAN_*` constants are used at their emit sites, so they are a single
//! source of truth. The `FIELD_*` constants **cannot be**, and the reason is a
//! trap worth stating plainly: `tracing`'s macros take a field name as an
//! *identifier*, so
//!
//! ```text
//! tracing::warn!(FIELD_PHASE = "read", "item skipped");
//! ```
//!
//! compiles cleanly and emits a field literally named `FIELD_PHASE`. The
//! constant is never read. Field names therefore stay string literals in the
//! engine, and these constants exist for queries, for downstream
//! [`Step`](crate::Step) implementations that want to match the engine's
//! vocabulary, and for the tests that pin the two together — which are the only
//! thing that makes a rename on either side visible.

/// Span covering one `JobExecution`.
///
/// Opened by `JobLauncher::run` *after* the FR-4.4 gate, so a relaunch that was
/// refused opens no span. Fields: [`FIELD_JOB`], [`FIELD_INSTANCE_ID`],
/// [`FIELD_EXECUTION_ID`].
pub const SPAN_JOB: &str = "job";

/// Span covering one step's `run`, nested inside [`SPAN_JOB`].
///
/// Not opened for a step skipped on restart — that step did not run. Covers
/// `Step::run` only, so the terminal status write that follows it belongs to
/// the enclosing job span. Fields: [`FIELD_STEP`], [`FIELD_STEP_EXECUTION_ID`].
pub const SPAN_STEP: &str = "step";

/// The job's name, as given to [`Job::builder`](crate::Job::builder).
pub const FIELD_JOB: &str = "job";
/// The [`JobInstanceId`](crate::JobInstanceId) — the unit of work, stable
/// across restarts of the same parameters.
pub const FIELD_INSTANCE_ID: &str = "instance_id";
/// The [`JobExecutionId`](crate::JobExecutionId) — this attempt. A restart is a
/// new execution against the same instance.
pub const FIELD_EXECUTION_ID: &str = "execution_id";
/// The step's name, as reported by [`Step::name`](crate::Step::name).
pub const FIELD_STEP: &str = "step";
/// The [`StepExecutionId`](crate::StepExecutionId) for this attempt at a step.
pub const FIELD_STEP_EXECUTION_ID: &str = "step_execution_id";

/// Where an item was dropped: `read` or `process`. Matches
/// [`LABEL_PHASE`](crate::metrics::LABEL_PHASE), so an event and a counter
/// describe the same partition.
pub const FIELD_PHASE: &str = "phase";
/// Which attempt a chunk is on, 1-based. An `attempt` of 1 on a retry event
/// means the *first* try failed.
pub const FIELD_ATTEMPT: &str = "attempt";
/// The step-wide running ordinal of a skipped item, not a per-chunk count —
/// so consecutive values across nearby events are what a contiguous run of bad
/// input looks like, as against a scattered systematic fault.
pub const FIELD_SKIPPED: &str = "skipped";
/// The tolerated or propagated error, rendered with `Display`.
pub const FIELD_ERROR: &str = "error";

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the *values*, which no other test can.
    ///
    /// Every other assertion goes through these constants — as does every emit
    /// site for the span names — so a rename moves both sides together and the
    /// suite stays green while every dashboard and saved query breaks. That is
    /// not a tautology any more than snapshotting a wire format is: these
    /// strings are a published contract, and the point is to make changing one
    /// a deliberate act with a diff attached.
    ///
    /// Found by mutation testing in 13c, which also showed
    /// [`metrics`](crate::metrics) had the same hole.
    #[test]
    fn the_vocabulary_is_a_published_contract() {
        assert_eq!(SPAN_JOB, "job");
        assert_eq!(SPAN_STEP, "step");

        assert_eq!(FIELD_JOB, "job");
        assert_eq!(FIELD_INSTANCE_ID, "instance_id");
        assert_eq!(FIELD_EXECUTION_ID, "execution_id");
        assert_eq!(FIELD_STEP, "step");
        assert_eq!(FIELD_STEP_EXECUTION_ID, "step_execution_id");
        assert_eq!(FIELD_PHASE, "phase");
        assert_eq!(FIELD_ATTEMPT, "attempt");
        assert_eq!(FIELD_SKIPPED, "skipped");
        assert_eq!(FIELD_ERROR, "error");
    }

    /// An event and a counter must partition skips the same way, or
    /// `items_skipped_total{phase="read"}` and the events beneath it disagree.
    #[test]
    fn the_phase_key_matches_its_metric_label() {
        assert_eq!(FIELD_PHASE, crate::metrics::LABEL_PHASE);
    }
}
