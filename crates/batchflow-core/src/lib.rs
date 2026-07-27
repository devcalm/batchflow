//! # BatchFlow Core
//!
//! Core traits and execution engine for BatchFlow.
#![forbid(unsafe_code)]

use async_trait::async_trait;
use std::future::Future;
use std::num::NonZeroUsize;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BatchError {
    #[error("Read failed: {0}")]
    Read(String),

    #[error("Write failed: {0}")]
    Write(String),

    #[error("Process failed: {0}")]
    Process(String),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StepExecution {
    /// Items pulled from the reader.
    pub read_count: usize,
    /// Items written (i.e. survived the processor's filter).
    pub write_count: usize,
    /// Items dropped by the processor (it returned `None`).
    pub filter_count: usize,
}

pub trait ItemReader {
    type Item;

    fn read(&mut self) -> impl Future<Output = Result<Option<Self::Item>, BatchError>> + Send;
}

pub trait ItemWriter {
    type Item;

    fn write(
        &mut self,
        items: &[Self::Item],
    ) -> impl Future<Output = Result<(), BatchError>> + Send;
}

pub trait ItemProcessor {
    type In;
    type Out;

    fn process(
        &mut self,
        item: Self::In,
    ) -> impl Future<Output = Result<Option<Self::Out>, BatchError>> + Send;
}

pub async fn read_chunk<R>(
    reader: &mut R,
    chunk_size: NonZeroUsize,
) -> Result<Vec<R::Item>, BatchError>
where
    R: ItemReader,
{
    let non_zero_chunk_size: usize = chunk_size.get();
    let mut chunk: Vec<R::Item> = Vec::with_capacity(non_zero_chunk_size);

    for _ in 0..non_zero_chunk_size {
        if let Some(item) = reader.read().await? {
            chunk.push(item);
        } else {
            break;
        }
    }

    Ok(chunk)
}

pub async fn process_chunk<P, W>(
    processor: &mut P,
    writer: &mut W,
    items: Vec<P::In>,
) -> Result<usize, BatchError>
where
    P: ItemProcessor,
    W: ItemWriter<Item = P::Out>,
{
    let mut outputs: Vec<P::Out> = Vec::with_capacity(items.len());

    for item in items {
        if let Some(out) = processor.process(item).await? {
            outputs.push(out);
        }
    }

    writer.write(&outputs).await?;

    Ok(outputs.len())
}

pub async fn run_step<R, P, W>(
    reader: &mut R,
    processor: &mut P,
    writer: &mut W,
    chunk_size: NonZeroUsize,
) -> Result<StepExecution, BatchError>
where
    R: ItemReader,
    P: ItemProcessor<In = R::Item>, // processor consumes what the reader produces
    W: ItemWriter<Item = P::Out>,   // writer consumes what the processor produces
{
    let mut step = StepExecution::default();
    loop {
        let chunk = read_chunk(reader, chunk_size).await?;
        if chunk.is_empty() {
            break;
        }
        let read = chunk.len();
        let written = process_chunk(processor, writer, chunk).await?;

        step.read_count += read;
        step.write_count += written;
        step.filter_count += read - written;
    }

    Ok(step)
}

#[async_trait]
pub trait Step: Send {
    async fn run(&mut self) -> Result<StepExecution, BatchError>;
}

pub struct ChunkStep<R, P, W> {
    reader: R,
    processor: P,
    writer: W,
    chunk_size: NonZeroUsize,
}

impl<R, P, W> ChunkStep<R, P, W> {
    pub fn new(reader: R, processor: P, writer: W, chunk_size: NonZeroUsize) -> Self {
        Self {
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

pub struct Job {
    steps: Vec<Box<dyn Step>>,
}

impl Job {
    pub fn new(steps: Vec<Box<dyn Step>>) -> Self {
        Self { steps }
    }

    pub async fn run(&mut self) -> Result<Vec<StepExecution>, BatchError> {
        let mut executions = Vec::with_capacity(self.steps.len());

        for step in &mut self.steps {
            executions.push(step.run().await?);
        }

        Ok(executions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only shorthand for a chunk size literal. A zero here is a bug in the
    /// test itself, so panicking is the right response.
    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test chunk size must be non-zero")
    }

    struct VecReader {
        items: Vec<u32>,
        pos: usize,
    }

    impl ItemReader for VecReader {
        type Item = u32;

        async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
            let item = self.items.get(self.pos).copied();
            if item.is_some() {
                self.pos += 1;
            }
            Ok(item)
        }
    }

    /// A test reader that yields `remaining_ok` items, then errors.
    struct FailingReader {
        remaining_ok: usize,
    }

    impl ItemReader for FailingReader {
        type Item = u32;
        async fn read(&mut self) -> Result<Option<u32>, BatchError> {
            if self.remaining_ok == 0 {
                return Err(BatchError::Read("boom".into()));
            }
            self.remaining_ok -= 1;
            Ok(Some(7))
        }
    }

    #[tokio::test]
    async fn reads_a_full_chunk() {
        let mut reader = VecReader {
            items: vec![1, 2, 3, 4, 5],
            pos: 0,
        };
        let chunk = read_chunk(&mut reader, nz(3)).await.unwrap();

        assert_eq!(chunk, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn reads_partial_chunk_at_eof() {
        let mut reader = VecReader {
            items: vec![1, 2],
            pos: 0,
        };
        let chunk = read_chunk(&mut reader, nz(5)).await.unwrap();

        assert_eq!(chunk, vec![1, 2]);
    }

    #[tokio::test]
    async fn error_short_circuits() {
        let mut reader = FailingReader { remaining_ok: 2 };
        let result = read_chunk(&mut reader, nz(5)).await;

        assert!(result.is_err());
    }

    struct EvenDoubler;

    impl ItemProcessor for EvenDoubler {
        type In = u32;
        type Out = u32;
        async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
            if item % 2 == 0 {
                Ok(Some(item * 2))
            } else {
                Ok(None) // odd → filtered out
            }
        }
    }
    struct CollectingWriter {
        written: Vec<u32>,
    }

    impl ItemWriter for CollectingWriter {
        type Item = u32;
        async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
            self.written.extend_from_slice(items);
            Ok(())
        }
    }

    #[tokio::test]
    async fn processes_filters_and_writes() {
        let mut processor = EvenDoubler;
        let mut writer = CollectingWriter {
            written: Vec::new(),
        };

        process_chunk(&mut processor, &mut writer, vec![1, 2, 3, 4])
            .await
            .unwrap();

        assert_eq!(writer.written, vec![4, 8]);
    }

    #[tokio::test]
    async fn run_step_reads_processes_writes_end_to_end() {
        let mut reader = VecReader {
            items: vec![1, 2, 3, 4, 5, 6],
            pos: 0,
        };
        let mut processor = EvenDoubler;
        let mut writer = CollectingWriter {
            written: Vec::new(),
        };

        // chunk_size = 2 -> the loop runs several times, proving it iterates.
        let step = run_step(&mut reader, &mut processor, &mut writer, nz(2))
            .await
            .unwrap();

        // odds filtered out; evens doubled: 2->4, 4->8, 6->12
        assert_eq!(writer.written, vec![4, 8, 12]);
        assert_eq!(step.read_count, 6); // read all six
        assert_eq!(step.write_count, 3); // three evens written
        assert_eq!(step.filter_count, 3); // three odds filtered
    }

    #[tokio::test]
    async fn chunk_step_owns_and_runs_its_pipeline() {
        let reader = VecReader {
            items: vec![1, 2, 3, 4, 5, 6],
            pos: 0,
        };
        let processor = EvenDoubler;
        let writer = CollectingWriter {
            written: Vec::new(),
        };

        let mut step = ChunkStep::new(reader, processor, writer, nz(2));
        let exec = step.run().await.unwrap();

        assert_eq!(exec.read_count, 6);
        assert_eq!(exec.write_count, 3);
        assert_eq!(exec.filter_count, 3);
    }

    struct LogStep;

    #[async_trait]
    impl Step for LogStep {
        async fn run(&mut self) -> Result<StepExecution, BatchError> {
            // A real tasklet would do work here (delete temp files, send a report).
            Ok(StepExecution::default())
        }
    }

    #[tokio::test]
    async fn job_runs_heterogeneous_steps_in_order() {
        let chunk_step = ChunkStep::new(
            VecReader {
                items: vec![1, 2, 3, 4],
                pos: 0,
            },
            EvenDoubler,
            CollectingWriter {
                written: Vec::new(),
            },
            nz(2),
        );
        let log_step = LogStep;

        let mut job = Job::new(vec![Box::new(chunk_step), Box::new(log_step)]);
        let execs = job.run().await.unwrap();

        assert_eq!(execs.len(), 2);
        assert_eq!(execs[0].read_count, 4); // chunk step read 1,2,3,4
        assert_eq!(execs[0].write_count, 2); // evens 2,4 → written 4,8
        assert_eq!(execs[1], StepExecution::default()); // log step did no I/O
    }

    /// Auto-traits are part of the public API under SemVer, but nothing warns you
    /// when you lose one. Assert it, so a future change to `Step` can't silently
    /// make jobs un-`tokio::spawn`able again.
    #[test]
    fn job_run_future_is_send() {
        fn assert_send<T: Send>(_: T) {}

        let mut job = Job::new(vec![]);
        assert_send(job.run());
    }
}
