//! Metric names and label keys emitted by the engine.
//!
//! Nothing is recorded until the application installs a recorder; until then
//! every emit site is a null check on a global. Call [`describe`] once at
//! startup, after installing one, to attach help text and units.
//!
//! Every metric is keyed by bounded, author-written values — a job name, a step
//! name, a status — and never by a [`JobExecutionId`](crate::JobExecutionId) or
//! any other repository-minted id. One label value per run would mint one time
//! series per run, each written to once and then kept forever. Correlating a
//! specific execution is a job for tracing, where high cardinality is the point.
//!
//! Counters carry a `_total` suffix and histograms are in seconds, per the
//! Prometheus conventions the exporters expect.

/// Job executions started. Labels: [`LABEL_JOB`].
pub const JOBS_STARTED: &str = "batchflow_jobs_started_total";
/// Job executions that reached a terminal status.
/// Labels: [`LABEL_JOB`], [`LABEL_STATUS`].
pub const JOBS_FINISHED: &str = "batchflow_jobs_finished_total";
/// Step executions started. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const STEPS_STARTED: &str = "batchflow_steps_started_total";
/// Step executions that reached a terminal status.
/// Labels: [`LABEL_JOB`], [`LABEL_STEP`], [`LABEL_STATUS`].
pub const STEPS_FINISHED: &str = "batchflow_steps_finished_total";

/// Items read from the reader. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const ITEMS_READ: &str = "batchflow_items_read_total";
/// Items written by a chunk that committed. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const ITEMS_WRITTEN: &str = "batchflow_items_written_total";
/// Items the processor dropped by returning `None`. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const ITEMS_FILTERED: &str = "batchflow_items_filtered_total";
/// Items dropped after a skippable error.
/// Labels: [`LABEL_JOB`], [`LABEL_STEP`], [`LABEL_PHASE`].
pub const ITEMS_SKIPPED: &str = "batchflow_items_skipped_total";

/// Chunk transactions committed. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const CHUNKS_COMMITTED: &str = "batchflow_chunks_committed_total";
/// Retry attempts after a failed write or commit. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const CHUNK_RETRIES: &str = "batchflow_chunk_retries_total";
/// Wall time for one chunk. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const CHUNK_DURATION: &str = "batchflow_chunk_duration_seconds";
/// Wall time for one step execution. Labels: [`LABEL_JOB`], [`LABEL_STEP`].
pub const STEP_DURATION: &str = "batchflow_step_duration_seconds";

/// The job's name, as given to [`Job::builder`](crate::Job::builder).
pub const LABEL_JOB: &str = "job";
/// The step's name, as reported by [`Step::name`](crate::Step::name).
pub const LABEL_STEP: &str = "step";
/// The terminal [`BatchStatus`](crate::BatchStatus), lowercased.
pub const LABEL_STATUS: &str = "status";
/// Where an item was dropped: `read`, `process`, `write` (chunk scanning), or
/// `tasklet`. Summing across the values gives every skip in the step.
pub const LABEL_PHASE: &str = "phase";

/// Counter: chunks re-written one item at a time to isolate a poison row.
///
/// Labels: [`LABEL_JOB`], [`LABEL_STEP`]. A non-zero rate means chunks are
/// failing on a single bad item and every good item in them is being written
/// twice — worth an alert, not just a graph.
pub const CHUNK_SCANS: &str = "batchflow_chunk_scans_total";

/// The label value for a status.
///
/// Spelled out per variant rather than with a wildcard: `BatchStatus` is
/// `#[non_exhaustive]` only for other crates, so inside this one an exhaustive
/// match makes a new variant stop the build here and force a decision about
/// what operators should see. A `_ => "unknown"` arm would answer that question
/// wrongly and silently, forever.
pub(crate) fn status_label(status: crate::BatchStatus) -> &'static str {
    use crate::BatchStatus::*;

    match status {
        Starting => "starting",
        Started => "started",
        Completed => "completed",
        Failed => "failed",
        Stopped => "stopped",
        Abandoned => "abandoned",
    }
}

/// Registers help text and units for every metric in this module.
///
/// Optional — metrics record correctly without it — but a scrape endpoint that
/// has never been described exposes bare numbers whose semantics an operator
/// has to guess. Call it once, after installing a recorder; calling it before
/// one is installed silently does nothing.
pub fn describe() {
    use ::metrics::{Unit, describe_counter, describe_histogram};

    describe_counter!(JOBS_STARTED, Unit::Count, "Job executions started.");
    describe_counter!(
        JOBS_FINISHED,
        Unit::Count,
        "Job executions that reached a terminal status, by status."
    );
    describe_counter!(
        STEPS_STARTED,
        Unit::Count,
        "Step executions started. A step skipped on restart is not counted."
    );
    describe_counter!(
        STEPS_FINISHED,
        Unit::Count,
        "Step executions that reached a terminal status, by status."
    );

    describe_counter!(
        ITEMS_READ,
        Unit::Count,
        "Items successfully read. Excludes reads that errored and were skipped; \
         includes items later filtered or skipped while processing."
    );
    describe_counter!(
        ITEMS_WRITTEN,
        Unit::Count,
        "Items written by a chunk that committed. Rolled-back chunks are not counted."
    );
    describe_counter!(
        ITEMS_FILTERED,
        Unit::Count,
        "Items the processor deliberately dropped by returning None. Not an error."
    );
    describe_counter!(
        ITEMS_SKIPPED,
        Unit::Count,
        "Items dropped after an error the classifier deemed skippable, by phase."
    );

    describe_counter!(
        CHUNKS_COMMITTED,
        Unit::Count,
        "Chunk transactions committed."
    );
    describe_counter!(
        CHUNK_RETRIES,
        Unit::Count,
        "Retry attempts after a failed write or commit. First attempts are not counted."
    );
    describe_counter!(
        CHUNK_SCANS,
        Unit::Count,
        "Chunks re-written one item at a time to isolate a poison row. A non-zero \
         rate means every good item in those chunks was written twice."
    );

    describe_histogram!(
        CHUNK_DURATION,
        Unit::Seconds,
        "Wall time for one chunk: read, process, write and commit."
    );
    describe_histogram!(
        STEP_DURATION,
        Unit::Seconds,
        "Wall time for one step execution."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the *values*, which no other test does.
    ///
    /// Every emit site and every assertion goes through these constants, so a
    /// rename moves both sides together: mutation testing in 13c renamed
    /// `ITEMS_READ` to `batchflow_ITEMS_RENAMED_total` and the whole workspace
    /// suite stayed green, while every Grafana panel would have gone blank.
    /// A metric name is a published contract; changing one should require a
    /// diff here that a reviewer can see.
    #[test]
    fn the_metric_vocabulary_is_a_published_contract() {
        assert_eq!(JOBS_STARTED, "batchflow_jobs_started_total");
        assert_eq!(JOBS_FINISHED, "batchflow_jobs_finished_total");
        assert_eq!(STEPS_STARTED, "batchflow_steps_started_total");
        assert_eq!(STEPS_FINISHED, "batchflow_steps_finished_total");

        assert_eq!(ITEMS_READ, "batchflow_items_read_total");
        assert_eq!(ITEMS_WRITTEN, "batchflow_items_written_total");
        assert_eq!(ITEMS_FILTERED, "batchflow_items_filtered_total");
        assert_eq!(ITEMS_SKIPPED, "batchflow_items_skipped_total");

        assert_eq!(CHUNKS_COMMITTED, "batchflow_chunks_committed_total");
        assert_eq!(CHUNK_RETRIES, "batchflow_chunk_retries_total");
        assert_eq!(CHUNK_SCANS, "batchflow_chunk_scans_total");
        assert_eq!(CHUNK_DURATION, "batchflow_chunk_duration_seconds");
        assert_eq!(STEP_DURATION, "batchflow_step_duration_seconds");

        assert_eq!(LABEL_JOB, "job");
        assert_eq!(LABEL_STEP, "step");
        assert_eq!(LABEL_STATUS, "status");
        assert_eq!(LABEL_PHASE, "phase");
    }

    /// The status label values are equally published, and an exhaustive match
    /// means a new `BatchStatus` variant lands here as a compile error.
    #[test]
    fn every_status_renders_a_stable_label() {
        use crate::BatchStatus::*;

        assert_eq!(status_label(Starting), "starting");
        assert_eq!(status_label(Started), "started");
        assert_eq!(status_label(Completed), "completed");
        assert_eq!(status_label(Failed), "failed");
        assert_eq!(status_label(Stopped), "stopped");
        assert_eq!(status_label(Abandoned), "abandoned");
    }
}
