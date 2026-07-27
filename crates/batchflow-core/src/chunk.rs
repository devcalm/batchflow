use crate::BatchError;
use crate::StepExecution;
use crate::{ItemProcessor, ItemReader, ItemWriter};
use std::num::NonZeroUsize;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{CollectingWriter, EvenDoubler, FailingReader, VecReader, nz};

    #[tokio::test]
    async fn reads_a_full_chunk() {
        let mut reader = VecReader::new(vec![1, 2, 3, 4, 5]);
        let chunk = read_chunk(&mut reader, nz(3)).await.unwrap();

        assert_eq!(chunk, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn reads_partial_chunk_at_eof() {
        let mut reader = VecReader::new(vec![1, 2]);
        let chunk = read_chunk(&mut reader, nz(5)).await.unwrap();

        assert_eq!(chunk, vec![1, 2]);
    }

    #[tokio::test]
    async fn error_short_circuits() {
        let mut reader = FailingReader { remaining_ok: 2 };
        let result = read_chunk(&mut reader, nz(5)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn processes_filters_and_writes() {
        let mut processor = EvenDoubler;
        let mut writer = CollectingWriter::new();

        process_chunk(&mut processor, &mut writer, vec![1, 2, 3, 4])
            .await
            .unwrap();

        assert_eq!(writer.written, vec![4, 8]);
    }

    #[tokio::test]
    async fn run_step_reads_processes_writes_end_to_end() {
        let mut reader = VecReader::new(vec![1, 2, 3, 4, 5, 6]);
        let mut processor = EvenDoubler;
        let mut writer = CollectingWriter::new();

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
}
