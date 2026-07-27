use crate::run_step;
use crate::{BatchError, ItemProcessor, ItemReader, ItemWriter};
use async_trait::async_trait;
use std::num::NonZeroUsize;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StepExecution {
    /// Items pulled from the reader.
    pub read_count: usize,
    /// Items written (i.e. survived the processor's filter).
    pub write_count: usize,
    /// Items dropped by the processor (it returned `None`).
    pub filter_count: usize,
}

#[async_trait]
pub trait Step: Send {
    fn name(&self) -> &str;
    async fn run(&mut self) -> Result<StepExecution, BatchError>;
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

    async fn run(&mut self) -> Result<StepExecution, BatchError> {
        run_step(
            &mut self.reader,
            &mut self.processor,
            &mut self.writer,
            self.chunk_size,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{CollectingWriter, EvenDoubler, VecReader, nz};

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

        let exec = step.run().await.unwrap();

        assert_eq!(exec.read_count, 6);
        assert_eq!(exec.write_count, 3);
        assert_eq!(exec.filter_count, 3);
    }
}
