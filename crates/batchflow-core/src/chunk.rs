use crate::{BatchError, ExecutionContext, StepContribution};
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
) -> Result<StepContribution, BatchError>
where
    P: ItemProcessor,
    W: ItemWriter<Item = P::Out>,
{
    let mut outputs: Vec<P::Out> = Vec::with_capacity(items.len());
    let mut filtered = 0usize;

    for item in items {
        match processor.process(item).await? {
            Some(out) => outputs.push(out),
            None => filtered += 1,
        }
    }

    writer.write(&outputs).await?;

    // Counters are produced only on the success path. A failed write leaves via
    // the `?` above, so there is no contribution for the caller to fold in —
    // that is what makes uncommitted work uncounted, not the statement order.
    let mut contribution = StepContribution::new();
    contribution.increment_write(outputs.len());
    contribution.increment_filter(filtered);

    Ok(contribution)
}

pub async fn run_step<R, P, W>(
    reader: &mut R,
    processor: &mut P,
    writer: &mut W,
    chunk_size: NonZeroUsize,
    contribution: &mut StepContribution,
    context: &mut ExecutionContext,
) -> Result<(), BatchError>
where
    R: ItemReader,
    P: ItemProcessor<In = R::Item>, // processor consumes what the reader produces
    W: ItemWriter<Item = P::Out>,   // writer consumes what the processor produces
{
    reader.open(context).await?;

    loop {
        let chunk = read_chunk(reader, chunk_size).await?;
        if chunk.is_empty() {
            break;
        }
        let read = chunk.len();
        
        let mut chunk_contribution = process_chunk(processor, writer, chunk).await?;
        chunk_contribution.increment_read(read);
        
        contribution.apply(&chunk_contribution);
        reader.update(context);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContextValue;
    use crate::testing::{
        BookmarkReader, CollectingWriter, EvenDoubler, FailingReader, FailingWriter, FlakyWriter,
        POSITION, VecReader, nz,
    };

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

        let contribution = process_chunk(&mut processor, &mut writer, vec![1, 2, 3, 4])
            .await
            .unwrap();

        assert_eq!(writer.written, vec![4, 8]);

        // Filters are observed at the `None` arm, not derived as read - written.
        assert_eq!(contribution.write_count(), 2);
        assert_eq!(contribution.filter_count(), 2);

        // `process_chunk` never sees the reader, so reads are the caller's to add.
        assert_eq!(contribution.read_count(), 0);
    }

    #[tokio::test]
    async fn run_step_reads_processes_writes_end_to_end() {
        let mut reader = VecReader::new(vec![1, 2, 3, 4, 5, 6]);
        let mut processor = EvenDoubler;
        let mut writer = CollectingWriter::new();
        let mut contribution = StepContribution::new();
        let mut context = ExecutionContext::new();

        // chunk_size = 2 -> the loop runs several times, proving it iterates
        // and that each chunk's contribution folds in rather than overwriting.
        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut contribution,
            &mut context,
        )
        .await
        .unwrap();

        // odds filtered out; evens doubled: 2->4, 4->8, 6->12
        assert_eq!(writer.written, vec![4, 8, 12]);
        assert_eq!(contribution.read_count(), 6); // read all six
        assert_eq!(contribution.write_count(), 3); // three evens written
        assert_eq!(contribution.filter_count(), 3); // three odds filtered
    }

    /// A chunk whose write fails contributes nothing: `process_chunk` hands its
    /// counters back by value, so the error path yields none at all and
    /// `run_step` has nothing to fold. Phase 11's rollback leans on this.
    #[tokio::test]
    async fn a_failed_write_contributes_nothing() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = FailingWriter;
        let mut contribution = StepContribution::new();
        let mut context = ExecutionContext::new();

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut contribution,
            &mut context,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(contribution, StepContribution::default());
    }

    // ---- the atomic bookmark ----

    #[tokio::test]
    async fn run_step_records_the_readers_position() {
        let mut reader = BookmarkReader::new(vec![2, 4, 6]);
        let mut processor = EvenDoubler;
        let mut writer = CollectingWriter::new();
        let mut contribution = StepContribution::new();
        let mut context = ExecutionContext::new();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut contribution,
            &mut context,
        )
        .await
        .unwrap();

        assert_eq!(context.get_long(POSITION).unwrap(), Some(3));
    }

    #[tokio::test]
    async fn run_step_resumes_from_a_recorded_position() {
        let mut context = ExecutionContext::new();
        context.put(POSITION, ContextValue::Long(2));

        let mut reader = BookmarkReader::new(vec![2, 4, 6, 8]);
        let mut processor = EvenDoubler;
        let mut writer = CollectingWriter::new();
        let mut contribution = StepContribution::new();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut contribution,
            &mut context,
        )
        .await
        .unwrap();

        // Items 2 and 4 were committed by the previous run. Re-reading them
        // here would double-write 4 and 8 — the exact failure restart exists
        // to prevent.
        assert_eq!(writer.written, vec![12, 16]);
        assert_eq!(contribution.read_count(), 2);
        assert_eq!(context.get_long(POSITION).unwrap(), Some(4));
    }

    /// Counters and bookmark must always describe the *same* committed work.
    /// The second chunk's write fails, so neither its items nor its position
    /// may appear.
    #[tokio::test]
    async fn a_failed_chunk_leaves_the_bookmark_at_the_last_committed_chunk() {
        let mut reader = BookmarkReader::new(vec![2, 4, 6, 8]);
        let mut processor = EvenDoubler;
        let mut writer = FlakyWriter::new(1); // first chunk commits, second does not
        let mut contribution = StepContribution::new();
        let mut context = ExecutionContext::new();

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut contribution,
            &mut context,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(writer.written, vec![4, 8]);
        assert_eq!(contribution.read_count(), 2);
        assert_eq!(context.get_long(POSITION).unwrap(), Some(2));
    }
}
