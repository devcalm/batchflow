use crate::FaultTolerance;
use crate::chunk::{ChunkConfig, run_step};
use crate::{BatchError, ExecutionContext, ItemProcessor, ItemReader, TransactionalWriter};
use crate::{JobExecutionId, StepExecutionId};
use async_trait::async_trait;
use std::num::NonZeroUsize;

/// A chunk's pending counter deltas, folded into a [`StepExecution`] only at
/// the commit point.
///
/// [`StepExecution`]: crate::StepExecution
///
/// Separate from `StepExecution` because the owners differ: a step reports
/// deltas, the repository owns the persisted totals and the id. A step cannot
/// return a `StepExecution` at all - it has no repository to mint one.
///
/// Fields are private with increment-only setters, so a step can add work it
/// did but never assert a total.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StepContribution {
    read_count: usize,
    write_count: usize,
    filter_count: usize,
    skip_count: usize,
}

impl StepContribution {
    /// A contribution with every counter at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `count` items read.
    pub fn increment_read(&mut self, count: usize) {
        self.read_count += count;
    }

    /// Records `count` items written.
    pub fn increment_write(&mut self, count: usize) {
        self.write_count += count;
    }

    /// Records `count` items filtered out by the processor.
    pub fn increment_filter(&mut self, count: usize) {
        self.filter_count += count;
    }

    /// Records `count` items skipped after a tolerated failure.
    pub fn increment_skip(&mut self, count: usize) {
        self.skip_count += count;
    }

    /// Items read.
    pub fn read_count(&self) -> usize {
        self.read_count
    }

    /// Items written.
    pub fn write_count(&self) -> usize {
        self.write_count
    }

    /// Items filtered out.
    pub fn filter_count(&self) -> usize {
        self.filter_count
    }

    /// Items dropped because they failed and the classifier called it
    /// skippable. Distinct from `filter_count`: a filtered item is one the
    /// processor deliberately declined, a skipped one is a failure tolerated.
    pub fn skip_count(&self) -> usize {
        self.skip_count
    }

    /// Fold another contribution into this one — the commit point.
    ///
    /// Adds rather than replaces: an unapplied contribution is always work not
    /// yet counted, never a correction of what already was.
    pub fn apply(&mut self, other: &StepContribution) {
        self.read_count += other.read_count;
        self.write_count += other.write_count;
        self.filter_count += other.filter_count;
        self.skip_count += other.skip_count;
    }
}

/// Identifies the step a [`StepCommit`] belongs to.
///
/// The chunk loop is reached through `dyn Step`, so it cannot learn any of
/// this on its own; the engine is the only thing that knows all four facts.
#[derive(Debug, Clone, Copy)]
pub struct StepIdentity<'a> {
    /// The name of the job this step is running under.
    pub job_name: &'a str,
    /// The step's own name.
    pub step_name: &'a str,
    /// The job execution this step belongs to.
    pub job_execution_id: JobExecutionId,
    /// This step execution.
    pub step_execution_id: StepExecutionId,
}

/// The step's transaction boundary. `commit` folds `contribution` into the
/// persisted counters, stores `context` as the bookmark, and commits `tx` —
/// so the chunk's data and its metadata become durable together.
///
/// A [`Step`] holds one of these instead of a repository: it is object-safe,
/// which `JobRepository` is not, and it keeps persistence out of step code.
#[async_trait]
pub trait StepCommit<Tx = ()>: Send {
    /// Identifies the step this commit belongs to.
    ///
    /// No default body: one returning placeholder values would silently
    /// mislabel every span and metric for the lifetime of the process.
    fn identity(&self) -> StepIdentity<'_>;

    /// Opens the transaction for one chunk.
    async fn begin(&mut self) -> Result<Tx, BatchError>;

    /// Folds `contribution`, stores `context` as the bookmark, and commits.
    async fn commit(
        &mut self,
        tx: Tx,
        contribution: &StepContribution,
        context: &ExecutionContext,
    ) -> Result<(), BatchError>;

    /// Discards the chunk's work. Taking `tx` by value is what makes reusing a
    /// rolled-back transaction fail to compile.
    async fn rollback(&mut self, tx: Tx) -> Result<(), BatchError>;
}

/// One unit of work in a [`Job`](crate::Job).
///
/// `Send`, and boxed as `Box<dyn Step<Tx>>` so a job can hold steps of
/// different concrete types - which is why this uses `#[async_trait]` rather
/// than the native async-fn-in-trait the item traits use: RPITIT is not
/// `dyn`-compatible.
#[async_trait]
pub trait Step<Tx = ()>: Send {
    /// The step's name. Stable across restarts, since it is the key a resume
    /// looks the previous attempt up by.
    fn name(&self) -> &str;

    /// Commit once per unit of work that has become durable. Anything not
    /// committed before the process dies is lost, so a step that commits only
    /// at the end is not restartable from a crash.
    async fn run(
        &mut self,
        context: &mut ExecutionContext,
        commit: &mut dyn StepCommit<Tx>,
    ) -> Result<(), BatchError>;
}

/// The chunk-oriented [`Step`]: read, process, write, commit, repeat.
///
/// Owns its reader, processor and writer, and drives them in commit-interval
/// sized chunks. Fault tolerance is off by default - see
/// [`with_fault_tolerance`](ChunkStep::with_fault_tolerance).
pub struct ChunkStep<R, P, W> {
    name: String,
    reader: R,
    processor: P,
    writer: W,
    chunk_size: NonZeroUsize,
    fault: FaultTolerance,
}

impl<R, P, W> ChunkStep<R, P, W> {
    /// Wires a pipeline together with `chunk_size` as its commit interval.
    ///
    /// `NonZeroUsize` because a zero interval would write nothing and report
    /// success - a failure mode worth making unrepresentable rather than
    /// checking for.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        reader: R,
        processor: P,
        writer: W,
        chunk_size: NonZeroUsize,
    ) -> Self {
        Self {
            name: name.into(),
            reader,
            processor,
            writer,
            chunk_size,
            // Fail on the first error until the caller asks for otherwise.
            fault: FaultTolerance::default(),
        }
    }

    /// Opt this step into retry (and, from Phase 10c, skip).
    ///
    /// Takes the whole [`FaultTolerance`] rather than a policy and a classifier
    /// separately: the two are one decision, and 10c widens the struct without
    /// touching this signature.
    #[must_use]
    pub fn with_fault_tolerance(mut self, fault: FaultTolerance) -> Self {
        self.fault = fault;
        self
    }

    /// Inherent, so `step.name()` resolves without inferring `Tx` — the trait
    /// method alone is ambiguous now that `Step` is generic.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Hand-written so the reader, processor and writer need no `Debug` bound of
/// their own - a step is identified by its wiring, not by its collaborators'
/// internals.
impl<R, P, W> std::fmt::Debug for ChunkStep<R, P, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkStep")
            .field("name", &self.name)
            .field("chunk_size", &self.chunk_size)
            .field("fault", &self.fault)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<R, P, W, Tx> Step<Tx> for ChunkStep<R, P, W>
where
    R: ItemReader + Send,
    P: ItemProcessor<In = R::Item> + Send,
    W: TransactionalWriter<Tx, Item = P::Out> + Send,
    R::Item: Send,
    P::Out: Send,
    Tx: Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(
        &mut self,
        context: &mut ExecutionContext,
        commit: &mut dyn StepCommit<Tx>,
    ) -> Result<(), BatchError> {
        let config = ChunkConfig::new(
            self.chunk_size,
            &self.fault,
            commit.identity().job_name,
            &self.name,
        );

        run_step(
            &mut self.reader,
            &mut self.processor,
            &mut self.writer,
            &config,
            context,
            commit,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        CollectingWriter, EvenDoubler, RecordingCommit, RetryAll, TransientWriter, VecReader, nz,
        nz32,
    };
    use crate::{RetryPolicy, Unmanaged};

    #[test]
    fn increments_accumulate() {
        let mut contribution = StepContribution::new();

        contribution.increment_read(2);
        contribution.increment_read(3);
        contribution.increment_write(4);
        contribution.increment_filter(1);

        assert_eq!(contribution.read_count(), 5);
        assert_eq!(contribution.write_count(), 4);
        assert_eq!(contribution.filter_count(), 1);
    }

    /// `apply` must add, not overwrite. The receiver starts non-zero on
    /// purpose: against a zeroed receiver an assigning impl looks identical.
    #[test]
    fn apply_sums_rather_than_replaces() {
        let mut total = StepContribution::new();
        total.increment_read(5);
        total.increment_write(3);
        total.increment_filter(2);

        let mut chunk = StepContribution::new();
        chunk.increment_read(3);
        chunk.increment_write(1);
        chunk.increment_filter(2);

        total.apply(&chunk);

        assert_eq!(total.read_count(), 8);
        assert_eq!(total.write_count(), 4);
        assert_eq!(total.filter_count(), 4);
    }

    #[tokio::test]
    async fn chunk_step_owns_and_runs_its_pipeline() {
        let mut step = ChunkStep::new(
            "double-evens",
            VecReader::new(vec![1, 2, 3, 4, 5, 6]),
            EvenDoubler,
            Unmanaged(CollectingWriter::new()),
            nz(2),
        );

        assert_eq!(step.name(), "double-evens");

        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::new();
        step.run(&mut context, &mut commit).await.unwrap();

        assert_eq!(commit.total.read_count(), 6);
        assert_eq!(commit.total.write_count(), 3);
        assert_eq!(commit.total.filter_count(), 3);

        // chunk_size 2 over 6 items: the step commits three times, not once.
        assert_eq!(commit.commits, 3);
    }

    /// The configured policy must actually reach the chunk loop.
    #[tokio::test(start_paused = true)]
    async fn a_configured_retry_policy_reaches_the_chunk_loop() {
        let mut step = ChunkStep::new(
            "flaky",
            VecReader::new(vec![2, 4]),
            EvenDoubler,
            Unmanaged(TransientWriter::new(1)), // one deadlock, then fine
            nz(2),
        )
        .with_fault_tolerance(
            FaultTolerance::new()
                .classifier(RetryAll)
                .retry(RetryPolicy::attempts(nz32(3))),
        );

        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::new();

        step.run(&mut context, &mut commit).await.unwrap();

        assert_eq!(commit.begins, 2, "the retry opened a second transaction");
        assert_eq!(commit.rollbacks, 1);
        assert_eq!(commit.total.write_count(), 2);
    }

    /// Positive control for the test above: the *same* step without
    /// `with_fault_tolerance` fails. Without this, a `TransientWriter` that
    /// simply never failed would make that test pass for the wrong reason.
    #[tokio::test]
    async fn the_same_step_without_a_policy_fails_on_the_first_error() {
        let mut step = ChunkStep::new(
            "flaky",
            VecReader::new(vec![2, 4]),
            EvenDoubler,
            Unmanaged(TransientWriter::new(1)),
            nz(2),
        );

        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::new();

        assert!(step.run(&mut context, &mut commit).await.is_err());
        assert_eq!(commit.begins, 1);
        assert_eq!(commit.commits, 0);
    }
}
