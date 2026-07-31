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
/// Where an item was dropped: `read` or `process`.
pub const LABEL_PHASE: &str = "phase";

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
