use crate::run_step;
use crate::{BatchError, ItemProcessor, ItemReader, ItemWriter};
use async_trait::async_trait;
use std::num::NonZeroUsize;

/// Counter deltas a step reports while it runs.
///
/// A step may only *accumulate* into one — it cannot assign a count, and cannot
/// reach identity or status. Those live on [`StepExecution`](crate::StepExecution),
/// which the engine owns. Phase 11 makes this the rollback unit: a chunk's contribution is
/// folded in only once its transaction commits, so a rollback drops it.
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

#[async_trait]
pub trait Step: Send {
    fn name(&self) -> &str;

    /// Run the step, reporting counter deltas into `contribution`.
    ///
    /// A step deliberately cannot return a [`StepExecution`](crate::StepExecution):
    /// that record's id is minted by the `JobRepository`, and a step has no
    /// repository — nor should it, since that is the separation the engine and
    /// launcher exist to enforce.
    async fn run(&mut self, contribution: &mut StepContribution) -> Result<(), BatchError>;
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
}

#[async_trait]
impl<R, P, W> Step for ChunkStep<R, P, W>
where
    R: ItemReader + Send,
    P: ItemProcessor<In = R::Item> + Send,
    W: ItemWriter<Item = P::Out> + Send,
    R::Item: Send,
    P::Out: Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&mut self, contribution: &mut StepContribution) -> Result<(), BatchError> {
        run_step(
            &mut self.reader,
            &mut self.processor,
            &mut self.writer,
            self.chunk_size,
            contribution,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{CollectingWriter, EvenDoubler, VecReader, nz};

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
            CollectingWriter::new(),
            nz(2),
        );

        assert_eq!(step.name(), "double-evens");

        let mut contribution = StepContribution::new();
        step.run(&mut contribution).await.unwrap();

        assert_eq!(contribution.read_count(), 6);
        assert_eq!(contribution.write_count(), 3);
        assert_eq!(contribution.filter_count(), 3);
    }
}
