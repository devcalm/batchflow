use crate::run_step;
use crate::{BatchError, ExecutionContext, ItemProcessor, ItemReader, TransactionalWriter};
use async_trait::async_trait;
use std::num::NonZeroUsize;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StepContribution {
    read_count: usize,
    write_count: usize,
    filter_count: usize,
}

impl StepContribution {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment_read(&mut self, count: usize) {
        self.read_count += count;
    }

    pub fn increment_write(&mut self, count: usize) {
        self.write_count += count;
    }

    pub fn increment_filter(&mut self, count: usize) {
        self.filter_count += count;
    }

    pub fn read_count(&self) -> usize {
        self.read_count
    }

    pub fn write_count(&self) -> usize {
        self.write_count
    }

    pub fn filter_count(&self) -> usize {
        self.filter_count
    }

    /// Fold another contribution into this one — the commit point.
    ///
    /// Adds rather than replaces: an unapplied contribution is always work not
    /// yet counted, never a correction of what already was.
    pub fn apply(&mut self, other: &StepContribution) {
        self.read_count += other.read_count;
        self.write_count += other.write_count;
        self.filter_count += other.filter_count;
    }
}

/// The step's transaction boundary. `commit` folds `contribution` into the
/// persisted counters, stores `context` as the bookmark, and commits `tx` —
/// so the chunk's data and its metadata become durable together.
///
/// A [`Step`] holds one of these instead of a repository: it is object-safe,
/// which `JobRepository` is not, and it keeps persistence out of step code.
#[async_trait]
pub trait StepCommit<Tx = ()>: Send {
    async fn begin(&mut self) -> Result<Tx, BatchError>;

    async fn commit(
        &mut self,
        tx: Tx,
        contribution: &StepContribution,
        context: &ExecutionContext,
    ) -> Result<(), BatchError>;

    async fn rollback(&mut self, tx: Tx) -> Result<(), BatchError>;
}

#[async_trait]
pub trait Step<Tx = ()>: Send {
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

pub struct ChunkStep<R, P, W> {
    name: String,
    reader: R,
    processor: P,
    writer: W,
    chunk_size: NonZeroUsize,
}

impl<R, P, W> ChunkStep<R, P, W> {
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
        }
    }

    /// Inherent, so `step.name()` resolves without inferring `Tx` — the trait
    /// method alone is ambiguous now that `Step` is generic.
    pub fn name(&self) -> &str {
        &self.name
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
        run_step(
            &mut self.reader,
            &mut self.processor,
            &mut self.writer,
            self.chunk_size,
            context,
            commit,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Unmanaged;
    use crate::testing::{CollectingWriter, EvenDoubler, RecordingCommit, VecReader, nz};

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
}
