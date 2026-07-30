use crate::{BatchError, ExecutionContext, FaultTolerance, ItemDisposition};
use crate::{ItemProcessor, ItemReader, TransactionalWriter};
use crate::{StepCommit, StepContribution};
use std::num::NonZeroUsize;
use std::time::Duration;

/// What a processed chunk yields: the items to write, and how many the
/// processor filtered out.
pub struct ProcessedChunk<O> {
    pub items: Vec<O>,
    pub filtered: usize,
}

/// Reads up to `chunk_size` items, skipping ones the classifier tolerates.
///
/// A skipped read does *not* use up a slot in the chunk — chunks stay the size
/// the commit interval promised, so the transaction boundary does not quietly
/// shrink because the input is dirty.
///
/// `skipped` is the step-wide running total, which is what bounds this loop: a
/// reader that errors without advancing past the offending item would otherwise
/// spin forever. The skip limit turns that into a step failure instead.
pub async fn read_chunk<R>(
    reader: &mut R,
    chunk_size: NonZeroUsize,
    fault: &FaultTolerance,
    skipped: &mut usize,
) -> Result<Vec<R::Item>, BatchError>
where
    R: ItemReader,
{
    let non_zero_chunk_size: usize = chunk_size.get();
    let mut chunk: Vec<R::Item> = Vec::with_capacity(non_zero_chunk_size);

    while chunk.len() < non_zero_chunk_size {
        match reader.read().await {
            Ok(Some(item)) => chunk.push(item),
            Ok(None) => break,
            Err(error) => match fault.disposition(error, *skipped) {
                ItemDisposition::Skip => *skipped += 1,
                ItemDisposition::Fail(error) => return Err(error),
            },
        }
    }

    Ok(chunk)
}

/// Runs the processor over a chunk, skipping items the classifier tolerates.
///
/// Called exactly once per chunk, outside any transaction. `process` consumes
/// its input, so a second attempt would have nothing to re-process — which is
/// why the retry boundary in [`run_step`] sits around the write, not here.
pub async fn process_chunk<P>(
    processor: &mut P,
    items: Vec<P::In>,
    fault: &FaultTolerance,
    skipped: &mut usize,
) -> Result<ProcessedChunk<P::Out>, BatchError>
where
    P: ItemProcessor,
{
    let mut outputs: Vec<P::Out> = Vec::with_capacity(items.len());
    let mut filtered = 0usize;

    for item in items {
        match processor.process(item).await {
            Ok(Some(out)) => outputs.push(out),
            Ok(None) => filtered += 1,
            Err(error) => match fault.disposition(error, *skipped) {
                ItemDisposition::Skip => *skipped += 1,
                ItemDisposition::Fail(error) => return Err(error),
            },
        }
    }

    Ok(ProcessedChunk {
        items: outputs,
        filtered,
    })
}

/// Waits out one step of the backoff schedule.
///
/// `tokio::time::sleep`, never `std::thread::sleep`: this runs on a shared
/// worker thread, and blocking it would stall every unrelated task scheduled
/// there — including the other steps of a job driven by `tokio::spawn`.
///
/// The schedule is unbounded, so `None` is unreachable; not sleeping is the
/// safe reading of it either way, since the retry limit lives elsewhere.
async fn back_off(backoff: &mut impl Iterator<Item = Duration>) {
    if let Some(delay) = backoff.next() {
        tokio::time::sleep(delay).await;
    }
}

pub async fn run_step<R, P, W, Tx>(
    reader: &mut R,
    processor: &mut P,
    writer: &mut W,
    chunk_size: NonZeroUsize,
    context: &mut ExecutionContext,
    commit: &mut dyn StepCommit<Tx>,
    fault: &FaultTolerance,
) -> Result<(), BatchError>
where
    R: ItemReader,
    P: ItemProcessor<In = R::Item>, // processor consumes what the reader produces
    W: TransactionalWriter<Tx, Item = P::Out>, // writer consumes what the processor produces
    Tx: Send,
{
    reader.open(context).await?;

    // Step-wide, because the skip limit is step-wide: one bad row in each of a
    // thousand chunks is a broken input, and a per-chunk counter would never
    // notice. It is *not* rolled back with a failed chunk — the items really
    // were seen, and a restart starts a new step execution with a fresh count.
    let mut skipped = 0usize;

    loop {
        let skipped_before = skipped;

        let chunk = read_chunk(reader, chunk_size, fault, &mut skipped).await?;
        if chunk.is_empty() {
            break;
        }
        let read = chunk.len();

        // Outside the transaction: processing may be slow, and holding locks
        // across it is how a batch job becomes a production incident. It also
        // runs exactly once — `process` consumes its item, so the retry below
        // has only the outputs to work with, never the inputs.
        let processed = process_chunk(processor, chunk, fault, &mut skipped).await?;

        let mut chunk_contribution = StepContribution::new();
        chunk_contribution.increment_read(read);
        chunk_contribution.increment_write(processed.items.len());
        chunk_contribution.increment_filter(processed.filtered);
        chunk_contribution.increment_skip(skipped - skipped_before);

        // 1-based, counting total attempts rather than retries after the first.
        let mut attempt = 1u32;
        // Fresh per chunk: each chunk's backoff starts from `min_delay` again.
        let mut backoff = fault.backoff();

        loop {
            // The commit interval is the transaction boundary: the items
            // written, the counters describing them and the reader's position
            // all become durable together, or none of them do.
            //
            // Inside the retry loop, because a retry needs a *new*
            // transaction. The previous one was rolled back or consumed by a
            // failed commit, and a backend that aborts a transaction on error
            // rejects every further statement on it (Postgres: 25P02).
            let mut tx = commit.begin().await?;

            if let Err(error) = writer.write(&mut tx, &processed.items).await {
                // Roll back *before* backing off. Sleeping on an open
                // transaction holds its row locks and its pooled connection for
                // the whole delay, which is how one deadlock becomes a pile-up
                // of them — backoff amplifying the contention it exists to
                // relieve.
                commit.rollback(tx).await?;

                if fault.should_retry(&error, attempt) {
                    back_off(&mut backoff).await;
                    attempt += 1;
                    continue;
                }
                return Err(error);
            }

            reader.update(context);

            match commit.commit(tx, &chunk_contribution, context).await {
                Ok(()) => break,
                Err(error) => {
                    // No rollback: `commit` took `tx` by value, so there is
                    // nothing left to roll back. A commit can still fail
                    // transiently (Postgres raises 40001 at COMMIT), so this
                    // path retries too — with its own fresh transaction.
                    if fault.should_retry(&error, attempt) {
                        back_off(&mut backoff).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        BookmarkReader, CollectingWriter, CommitEvent, EvenDoubler, FailingReader, FailingWriter,
        FlakyWriter, POSITION, PoisonProcessor, PoisonReader, RecordingCommit, RetryAll, SkipAll,
        TransientWriter, VecReader, nz, nz32,
    };
    use crate::{ContextValue, RetryPolicy, Unmanaged};
    use tokio::time::Instant;

    #[tokio::test]
    async fn reads_a_full_chunk() {
        let mut reader = VecReader::new(vec![1, 2, 3, 4, 5]);
        let chunk = read_chunk(&mut reader, nz(3), &FaultTolerance::default(), &mut 0)
            .await
            .unwrap();

        assert_eq!(chunk, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn reads_partial_chunk_at_eof() {
        let mut reader = VecReader::new(vec![1, 2]);
        let chunk = read_chunk(&mut reader, nz(5), &FaultTolerance::default(), &mut 0)
            .await
            .unwrap();

        assert_eq!(chunk, vec![1, 2]);
    }

    #[tokio::test]
    async fn error_short_circuits() {
        let mut reader = FailingReader { remaining_ok: 2 };
        let result = read_chunk(&mut reader, nz(5), &FaultTolerance::default(), &mut 0).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn processes_and_filters() {
        let mut processor = EvenDoubler;

        let processed = process_chunk(
            &mut processor,
            vec![1, 2, 3, 4],
            &FaultTolerance::default(),
            &mut 0,
        )
        .await
        .unwrap();

        // Filters are observed at the `None` arm, not derived as read - written.
        assert_eq!(processed.items, vec![4, 8]);
        assert_eq!(processed.filtered, 2);
    }

    #[tokio::test]
    async fn writes_what_it_is_given() {
        let mut writer = Unmanaged(CollectingWriter::new());

        writer.write(&mut (), &[4, 8]).await.unwrap();

        assert_eq!(writer.0.written, vec![4, 8]);
    }

    #[tokio::test]
    async fn run_step_reads_processes_writes_end_to_end() {
        let mut reader = VecReader::new(vec![1, 2, 3, 4, 5, 6]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        // chunk_size = 2 -> the loop runs several times, proving it iterates
        // and that each chunk's contribution folds in rather than overwriting.
        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
        )
        .await
        .unwrap();

        // odds filtered out; evens doubled: 2->4, 4->8, 6->12
        assert_eq!(writer.0.written, vec![4, 8, 12]);
        assert_eq!(commit.total.read_count(), 6); // read all six
        assert_eq!(commit.total.write_count(), 3); // three evens written
        assert_eq!(commit.total.filter_count(), 3); // three odds filtered
    }

    /// A chunk whose write fails contributes nothing: `process_chunk` hands its
    /// counters back by value, so the error path yields none at all and
    /// `run_step` has nothing to fold. Phase 11's rollback leans on this.
    #[tokio::test]
    async fn a_failed_write_contributes_nothing() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(FailingWriter);
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(commit.total, StepContribution::default());
        assert_eq!(
            commit.commits, 0,
            "a failed chunk must not reach the commit point"
        );

        // The transaction was opened and must not be left open. Without the
        // rollback arm in `run_step`, `tx` is dropped here instead — which for
        // a real backend leaks a connection holding locks.
        assert_eq!(commit.begins, 1);
        assert_eq!(commit.rollbacks, 1);
    }

    /// The commit interval *is* the transaction boundary (FR-2.4): one
    /// transaction per chunk, not one per step and not one per item.
    #[tokio::test]
    async fn every_chunk_gets_its_own_transaction() {
        let mut reader = VecReader::new(vec![1, 2, 3, 4, 5, 6]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
        )
        .await
        .unwrap();

        assert_eq!(commit.begins, 3);
        assert_eq!(commit.commits, 3);
        assert_eq!(commit.rollbacks, 0);
    }

    /// A chunk that commits stays committed when a *later* chunk fails — the
    /// point of a per-chunk boundary rather than a per-step one.
    #[tokio::test]
    async fn an_earlier_chunk_stays_committed_when_a_later_one_fails() {
        let mut reader = VecReader::new(vec![2, 4, 6, 8]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(FlakyWriter::new(1));
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(commit.begins, 2);
        assert_eq!(commit.commits, 1);
        assert_eq!(commit.rollbacks, 1);
        assert_eq!(commit.total.write_count(), 2);
    }

    // ---- retry (FR-6.1) ----

    /// The rule Phase 11 created: a retry may not reuse the transaction that
    /// just failed. The event sequence is the assertion — `Begin, Rollback,
    /// Begin, Commit` says the second attempt opened its own.
    #[tokio::test(start_paused = true)]
    async fn a_retryable_write_error_is_retried_in_a_fresh_transaction() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(TransientWriter::new(1)); // one deadlock, then fine
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)));

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &fault,
        )
        .await
        .unwrap();

        assert_eq!(
            commit.events,
            vec![
                CommitEvent::Begin,
                CommitEvent::Rollback,
                CommitEvent::Begin,
                CommitEvent::Commit
            ],
        );

        // The chunk landed once, not twice: the retry re-wrote the same
        // borrowed outputs rather than re-processing.
        assert_eq!(writer.0.written, vec![4, 8]);
        assert_eq!(commit.total.write_count(), 2);
        assert_eq!(commit.total.read_count(), 2);
    }

    /// A retry must wait, not spin: an immediate re-attempt against a saturated
    /// pool or a rate-limited endpoint is a hot loop that deepens the outage.
    ///
    /// `start_paused` makes tokio auto-advance its clock whenever every task is
    /// idle, so asserting on a one-second wait costs no real time — the sleep is
    /// observed, not endured.
    #[tokio::test(start_paused = true)]
    async fn a_retry_waits_before_trying_again() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(TransientWriter::new(1));
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)).min_delay(Duration::from_secs(1)));

        let started = Instant::now();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &fault,
        )
        .await
        .unwrap();

        assert_eq!(commit.begins, 2, "the retry happened at all");
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "expected a backoff of at least min_delay, waited {:?}",
            started.elapsed()
        );
    }

    /// The control for the test above: a step that never fails never waits, so
    /// the elapsed time there is the backoff and not some other stall.
    #[tokio::test(start_paused = true)]
    async fn a_step_that_does_not_fail_does_not_wait() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)).min_delay(Duration::from_secs(1)));

        let started = Instant::now();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &fault,
        )
        .await
        .unwrap();

        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    /// `attempts(2)` means two tries in total, then the step fails with the
    /// error from the *last* attempt — not an exhaustion error of our own.
    #[tokio::test(start_paused = true)]
    async fn retries_are_bounded_by_the_policy() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(TransientWriter::new(99)); // never recovers
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(2)));

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &fault,
        )
        .await;

        assert!(matches!(result, Err(BatchError::Write(_))));
        assert_eq!(commit.begins, 2);
        assert_eq!(commit.rollbacks, 2);
        assert_eq!(commit.commits, 0);
        assert_eq!(commit.total, StepContribution::default());
    }

    /// A budget of ten attempts is not permission to retry a fatal error: the
    /// classifier is consulted first, so `FailFast` still stops at one.
    #[tokio::test]
    async fn a_fatal_error_is_not_retried_however_large_the_budget() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(FailingWriter);
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new().retry(RetryPolicy::attempts(nz32(10)));

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &fault,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(commit.begins, 1);
    }

    /// A commit can fail transiently too (Postgres raises 40001 at `COMMIT`).
    /// `commit` consumed the transaction, so there is nothing to roll back —
    /// the absence of a `Rollback` between the two `Commit`s is the point.
    #[tokio::test(start_paused = true)]
    async fn a_failed_commit_is_retried_without_a_rollback() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::failing_commits(1);
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)));

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &fault,
        )
        .await
        .unwrap();

        assert_eq!(
            commit.events,
            vec![
                CommitEvent::Begin,
                CommitEvent::Commit, // failed
                CommitEvent::Begin,
                CommitEvent::Commit, // succeeded
            ],
        );
        assert_eq!(commit.rollbacks, 0);
        assert_eq!(commit.commits, 1, "only the successful commit counts");
    }

    // ---- skip (FR-6.2) ----

    /// One poison item is dropped; the rest of the chunk still commits. Without
    /// skip, item 4 would fail the step and item 6 would never be written.
    #[tokio::test]
    async fn a_skippable_item_is_dropped_and_the_chunk_survives() {
        let mut reader = VecReader::new(vec![2, 4, 6]);
        let mut processor = PoisonProcessor { poison: 4 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(1);

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(3),
            &mut context,
            &mut commit,
            &fault,
        )
        .await
        .unwrap();

        assert_eq!(writer.0.written, vec![4, 12]);
        assert_eq!(commit.total.read_count(), 3, "the bad item was still read");
        assert_eq!(commit.total.write_count(), 2);
        assert_eq!(commit.total.skip_count(), 1);

        // A skip is not a filter: the processor never declined this item, it
        // failed on it. Collapsing the two would hide bad data as intent.
        assert_eq!(commit.total.filter_count(), 0);
    }

    /// The default policy skips nothing, so the same step fails. Positive
    /// control for the test above.
    #[tokio::test]
    async fn the_default_policy_does_not_skip() {
        let mut reader = VecReader::new(vec![2, 4, 6]);
        let mut processor = PoisonProcessor { poison: 4 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(3),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
        )
        .await;

        assert!(matches!(result, Err(BatchError::Process(_))));
        assert_eq!(commit.commits, 0);
    }

    /// Past the limit the step fails with [`BatchError::SkipLimitExceeded`],
    /// not the bare item error — "this file is garbage" is a different page
    /// from "one row was odd".
    #[tokio::test]
    async fn exceeding_the_skip_limit_fails_with_a_distinct_error() {
        let mut reader = VecReader::new(vec![2, 4, 4, 6]);
        let mut processor = PoisonProcessor { poison: 4 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(1);

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(4),
            &mut context,
            &mut commit,
            &fault,
        )
        .await;

        let Err(BatchError::SkipLimitExceeded { limit, cause }) = result else {
            panic!("expected SkipLimitExceeded");
        };
        assert_eq!(limit, 1);

        // The item error is preserved as the source, not swallowed.
        assert!(cause.to_string().contains("bad item 4"));
    }

    /// A skipped *read* must not shrink the chunk: the commit interval is a
    /// promise about transaction size, and dirty input should not quietly
    /// shorten it.
    #[tokio::test]
    async fn a_skipped_read_does_not_shrink_the_chunk() {
        let mut reader = PoisonReader::new(vec![2, 99, 4, 6], 99);
        let mut processor = PoisonProcessor { poison: 0 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(1);

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(3),
            &mut context,
            &mut commit,
            &fault,
        )
        .await
        .unwrap();

        // One chunk of three good items, not a short chunk of two.
        assert_eq!(writer.0.written, vec![4, 8, 12]);
        assert_eq!(commit.commits, 1);
        assert_eq!(commit.total.read_count(), 3);
        assert_eq!(commit.total.skip_count(), 1);
    }

    /// The limit is step-wide, not per chunk: one bad row in each of several
    /// chunks is a broken input, and a per-chunk counter would call it healthy.
    #[tokio::test]
    async fn the_skip_limit_spans_chunks() {
        let mut reader = VecReader::new(vec![4, 2, 4, 6]);
        let mut processor = PoisonProcessor { poison: 4 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        // chunk_size 2 -> the two poison items land in different chunks.
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(1);

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &fault,
        )
        .await;

        assert!(matches!(result, Err(BatchError::SkipLimitExceeded { .. })));
        assert_eq!(commit.commits, 1, "the first chunk still committed");
    }

    // ---- the atomic bookmark ----

    #[tokio::test]
    async fn run_step_records_the_readers_position() {
        let mut reader = BookmarkReader::new(vec![2, 4, 6]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
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
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
        )
        .await
        .unwrap();

        // Items 2 and 4 were committed by the previous run. Re-reading them
        // here would double-write 4 and 8 — the exact failure restart exists
        // to prevent.
        assert_eq!(writer.0.written, vec![12, 16]);
        assert_eq!(commit.total.read_count(), 2);
        assert_eq!(context.get_long(POSITION).unwrap(), Some(4));
    }

    /// Counters and bookmark must always describe the *same* committed work.
    /// The second chunk's write fails, so neither its items nor its position
    /// may appear.
    #[tokio::test]
    async fn a_failed_chunk_leaves_the_bookmark_at_the_last_committed_chunk() {
        let mut reader = BookmarkReader::new(vec![2, 4, 6, 8]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(FlakyWriter::new(1)); // first chunk commits, second does not
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            nz(2),
            &mut context,
            &mut commit,
            &FaultTolerance::default(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(writer.0.written, vec![4, 8]);
        assert_eq!(commit.total.read_count(), 2);
        assert_eq!(context.get_long(POSITION).unwrap(), Some(2));
    }
}
