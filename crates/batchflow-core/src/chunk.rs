use crate::metrics::{
    CHUNK_DURATION, CHUNK_RETRIES, CHUNK_SCANS, CHUNKS_COMMITTED, ITEMS_FILTERED, ITEMS_READ,
    ITEMS_SKIPPED, ITEMS_WRITTEN, LABEL_JOB, LABEL_PHASE, LABEL_STEP,
};
use crate::{BatchError, ExecutionContext, FaultTolerance, ItemDisposition};
use crate::{ItemProcessor, ItemReader, TransactionalWriter};
use crate::{StepCommit, StepContribution};
use ::metrics::{Counter, Histogram, counter, histogram};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

/// What a processed chunk yields: the items to write, and how many the
/// processor filtered out.
pub(crate) struct ProcessedChunk<O> {
    pub items: Vec<O>,
    pub filtered: usize,
}

/// Metric handles for one step execution, resolved once.
///
/// Label values must be owned, so building these per chunk would allocate two
/// `String`s and hit the registry eight times for every commit interval.
pub(crate) struct ChunkMetrics {
    items_read: Counter,
    items_written: Counter,
    items_filtered: Counter,
    skipped_reading: Counter,
    skipped_processing: Counter,
    skipped_writing: Counter,
    chunks_committed: Counter,
    retries: Counter,
    scans: Counter,
    chunk_duration: Histogram,
}

impl ChunkMetrics {
    pub(crate) fn new(job: &str, step: &str) -> Self {
        let (job, step) = (job.to_owned(), step.to_owned());

        Self {
            items_read: counter!(ITEMS_READ, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            items_written: counter!(ITEMS_WRITTEN, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            items_filtered: counter!(ITEMS_FILTERED, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            skipped_reading: counter!(ITEMS_SKIPPED, LABEL_JOB => job.clone(), LABEL_STEP => step.clone(), LABEL_PHASE => "read"),
            skipped_processing: counter!(ITEMS_SKIPPED, LABEL_JOB => job.clone(), LABEL_STEP => step.clone(), LABEL_PHASE => "process"),
            skipped_writing: counter!(ITEMS_SKIPPED, LABEL_JOB => job.clone(), LABEL_STEP => step.clone(), LABEL_PHASE => "write"),
            scans: counter!(CHUNK_SCANS, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            chunks_committed: counter!(CHUNKS_COMMITTED, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            retries: counter!(CHUNK_RETRIES, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            chunk_duration: histogram!(CHUNK_DURATION, LABEL_JOB => job, LABEL_STEP => step),
        }
    }
}

/// What a step was configured with, as opposed to the collaborators it drives.
///
/// Bundled because `run_step` was already at clippy's argument limit: growing
/// under an `#[allow]` would have hidden the next addition too.
pub(crate) struct ChunkConfig<'a> {
    chunk_size: NonZeroUsize,
    fault: &'a FaultTolerance,
    metrics: ChunkMetrics,
}

impl<'a> ChunkConfig<'a> {
    pub(crate) fn new(
        chunk_size: NonZeroUsize,
        fault: &'a FaultTolerance,
        job: &str,
        step: &str,
    ) -> Self {
        Self {
            chunk_size,
            fault,
            metrics: ChunkMetrics::new(job, step),
        }
    }
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
pub(crate) async fn read_chunk<R>(
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
                ItemDisposition::Skip(error) => {
                    *skipped += 1;
                    tracing::warn!(phase = "read", skipped = *skipped, error = %error, "item skipped");
                }
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
pub(crate) async fn process_chunk<P>(
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
                ItemDisposition::Skip(error) => {
                    tracing::warn!(phase = "process", skipped = *skipped, error = %error, "item skipped");
                    *skipped += 1
                }
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

/// Finds the poison items in a failed chunk, returning the ones that survive.
///
/// Two passes, and the split is forced rather than chosen. Each item is written
/// **alone in a throwaway transaction that is always rolled back**, so this pass
/// commits nothing and a crash inside it is invisible. The survivors are then
/// written once, for real, through the ordinary commit point.
///
/// Committing item by item instead would be cheaper and wrong:
/// [`ItemReader::update`] reports the reader's position, which after a chunk is
/// read sits *past the whole chunk*. There is no way to express "bookmark after
/// item three", so a crash midway would leave the bookmark at the previous
/// chunk boundary with some items already durable — and the restart would write
/// them again. Exactly-once is the framework's reason to exist; paying `N + 1`
/// transactions on the failure path is the cheaper side of that trade.
async fn scan_chunk<W, Tx>(
    writer: &mut W,
    commit: &mut dyn StepCommit<Tx>,
    items: Vec<W::Item>,
    fault: &FaultTolerance,
    skipped: &mut usize,
) -> Result<Vec<W::Item>, BatchError>
where
    W: TransactionalWriter<Tx>,
    Tx: Send,
{
    let mut survivors = Vec::with_capacity(items.len());

    for item in items {
        let mut tx = commit.begin().await?;
        let outcome = writer.write(&mut tx, std::slice::from_ref(&item)).await;

        // Rolled back whether or not the write succeeded: this pass exists to
        // ask a question, never to answer it durably.
        if let Err(rollback_error) = commit.rollback(tx).await {
            tracing::error!(
                error = %rollback_error,
                "rollback failed during chunk scan; the transaction is in an unknown state"
            );
            return Err(rollback_error);
        }

        match outcome {
            Ok(()) => survivors.push(item),
            Err(error) => match fault.disposition(error, *skipped) {
                ItemDisposition::Skip(error) => {
                    *skipped += 1;
                    tracing::warn!(phase = "write", skipped = *skipped, error = %error, "item skipped");
                }
                ItemDisposition::Fail(error) => return Err(error),
            },
        }
    }

    Ok(survivors)
}

pub(crate) async fn run_step<R, P, W, Tx>(
    reader: &mut R,
    processor: &mut P,
    writer: &mut W,
    config: &ChunkConfig<'_>,
    context: &mut ExecutionContext,
    commit: &mut dyn StepCommit<Tx>,
) -> Result<(), BatchError>
where
    R: ItemReader,
    P: ItemProcessor<In = R::Item>, // processor consumes what the reader produces
    W: TransactionalWriter<Tx, Item = P::Out>, // writer consumes what the processor produces
    Tx: Send,
{
    let ChunkConfig {
        chunk_size,
        fault,
        metrics,
    } = config;
    let chunk_size = *chunk_size;

    reader.open(context).await?;

    // Step-wide, because the skip limit is step-wide: one bad row in each of a
    // thousand chunks is a broken input, and a per-chunk counter would never
    // notice. It is *not* rolled back with a failed chunk — the items really
    // were seen, and a restart starts a new step execution with a fresh count.
    let mut skipped = 0usize;

    loop {
        let skipped_before = skipped;
        let chunk_start = Instant::now();

        let chunk = read_chunk(reader, chunk_size, fault, &mut skipped).await?;
        if chunk.is_empty() {
            // An empty chunk is not necessarily an idle one: a file whose last
            // rows are all malformed skips its way to exhaustion, and those
            // skips are real work. Breaking straight out would drop them from
            // `skip_count` and from the metric — the step would report zero
            // skips for exactly the input segment that was unreadable — and
            // would leave the bookmark short of them, so a restart re-reads
            // rows already known to be poison.
            //
            // Found by `skips_and_reads_partition_the_input`, which no
            // example-based test had covered: it only bites when the *tail* of
            // the input is skippable.
            let trailing = skipped - skipped_before;
            if trailing > 0 {
                let tx = commit.begin().await?;
                reader.update(context);

                let mut contribution = StepContribution::new();
                contribution.increment_skip(trailing);
                commit.commit(tx, &contribution, context).await?;

                metrics.skipped_reading.increment(trailing as u64);
            }
            break;
        }
        let read = chunk.len();
        let skipped_reading = skipped - skipped_before;

        // Outside the transaction: processing may be slow, and holding locks
        // across it is how a batch job becomes a production incident. It also
        // runs exactly once — `process` consumes its item, so the retry below
        // has only the outputs to work with, never the inputs.
        let processed = process_chunk(processor, chunk, fault, &mut skipped).await?;
        let skipped_processing = skipped - skipped_before - skipped_reading;
        let skipped_after_process = skipped;

        // Owned rather than borrowed: a chunk scan replaces this list with the
        // items that survived it.
        let mut items = processed.items;

        // 1-based, counting total attempts rather than retries after the first.
        let mut attempt = 1u32;
        // Fresh per chunk: each chunk's backoff starts from `min_delay` again.
        let mut backoff = fault.backoff();
        // A scan yields items that each wrote cleanly on their own. If writing
        // them together still fails, that is a genuine failure of the batch -
        // rescanning would find nothing and loop forever.
        let mut scanned = false;

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

            if let Err(error) = writer.write(&mut tx, &items).await {
                // Roll back *before* backing off. Sleeping on an open
                // transaction holds its row locks and its pooled connection for
                // the whole delay, which is how one deadlock becomes a pile-up
                // of them — backoff amplifying the contention it exists to
                // relieve.
                // A rollback that itself failed leaves the transaction in an
                // unknown state, so this does not retry — and it reports the
                // write error, not the rollback error, because the write is why
                // there was a rollback at all.
                if let Err(rollback_error) = commit.rollback(tx).await {
                    tracing::error!(
                        error = %rollback_error,
                        "rollback failed; the transaction is in an unknown state"
                    );
                    return Err(error.with_cleanup(Err(rollback_error)));
                }

                if fault.should_retry(&error, attempt) {
                    tracing::warn!(attempt, error = %error, "chunk failed, retrying");
                    metrics.retries.increment(1);
                    back_off(&mut backoff).await;
                    attempt += 1;
                    continue;
                }

                // A retry re-writes the same chunk, so it cannot help when one
                // item in it is bad. Scanning is what turns "this chunk failed"
                // into "this item failed" (FR-6.4).
                if !scanned && fault.should_scan(&error) {
                    items = scan_chunk(writer, commit, items, fault, &mut skipped).await?;
                    scanned = true;
                    metrics.scans.increment(1);
                    if items.is_empty() {
                        // Every item was poison. There is nothing left to write,
                        // but the skips still have to be counted and the
                        // bookmark still has to advance past them - otherwise a
                        // restart re-reads the same doomed chunk forever.
                        tracing::warn!("every item in the chunk was skipped");
                    }
                    continue;
                }
                return Err(error);
            }

            reader.update(context);

            // Built here rather than before the loop: a scan changes both the
            // number of items written and the number skipped.
            let mut chunk_contribution = StepContribution::new();
            chunk_contribution.increment_read(read);
            chunk_contribution.increment_write(items.len());
            chunk_contribution.increment_filter(processed.filtered);
            chunk_contribution.increment_skip(skipped - skipped_before);

            match commit.commit(tx, &chunk_contribution, context).await {
                Ok(()) => break,
                Err(error) => {
                    // No rollback: `commit` took `tx` by value, so there is
                    // nothing left to roll back. A commit can still fail
                    // transiently (Postgres raises 40001 at COMMIT), so this
                    // path retries too — with its own fresh transaction.
                    if fault.should_retry(&error, attempt) {
                        tracing::warn!(attempt, error = %error, "chunk commit failed, retrying");
                        metrics.retries.increment(1);
                        back_off(&mut backoff).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(error);
                }
            }
        }

        // Reached only by a chunk that committed: the loop above leaves either
        // by `break` on a successful commit or by returning the error. So the
        // counters published here can never describe work that rolled back,
        // and they agree with what the repository persisted in the same
        // transaction — `sum(items_written_total)` reconciles with
        // `sum(write_count)`.
        //
        // Retries are the deliberate exception, counted above as they happen:
        // a chunk that retried five times and then failed must still report
        // them, or the metric hides exactly the incident it exists for.
        metrics
            .chunk_duration
            .record(chunk_start.elapsed().as_secs_f64());
        metrics.chunks_committed.increment(1);
        metrics.items_read.increment(read as u64);
        metrics.items_written.increment(items.len() as u64);
        metrics.items_filtered.increment(processed.filtered as u64);
        metrics.skipped_reading.increment(skipped_reading as u64);
        metrics
            .skipped_processing
            .increment(skipped_processing as u64);
        metrics
            .skipped_writing
            .increment((skipped - skipped_after_process) as u64);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        BookmarkReader, CollectingWriter, CommitEvent, EvenDoubler, FailingReader, FailingWriter,
        FlakyWriter, POSITION, PoisonProcessor, PoisonReader, RecordingCommit, RetryAll, SkipAll,
        TransientWriter, VecReader, block_on, captured, counter, events_named, labels_of, nz, nz32,
        recorded,
    };
    use crate::tracing::{FIELD_ATTEMPT, FIELD_ERROR, FIELD_PHASE, FIELD_SKIPPED};
    use crate::{ContextValue, RetryPolicy, Unmanaged};
    use std::error::Error;
    use tokio::time::Instant;
    use tracing::Level;

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
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(3), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(3), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(4), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(3), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
        )
        .await
        .unwrap();

        // One chunk of three good items, not a short chunk of two.
        assert_eq!(writer.0.written, vec![4, 8, 12]);
        assert_eq!(commit.commits, 1);
        assert_eq!(commit.total.read_count(), 3);
        assert_eq!(commit.total.skip_count(), 1);
    }

    /// Skips in the *tail* of the input are counted, even though the chunk they
    /// happened in is empty and never becomes a write.
    ///
    /// Found by the property test `skips_and_reads_partition_the_input`; no
    /// example test had put a poison row last, and breaking straight out of the
    /// loop on an empty chunk discarded those skips silently. A step reporting
    /// zero skips for an unreadable trailing segment is exactly the quiet data
    /// loss Phase 12 exists to prevent.
    #[tokio::test]
    async fn skips_at_the_end_of_the_input_are_still_counted() {
        let mut reader = PoisonReader::new(vec![2, 4, 99], 99);
        let mut processor = PoisonProcessor { poison: 0 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        // chunk_size 2, so the poison row is alone in a third read that yields
        // an empty chunk.
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(1);

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
        )
        .await
        .unwrap();

        assert_eq!(writer.0.written, vec![4, 8]);
        assert_eq!(commit.total.read_count(), 2);
        assert_eq!(commit.total.skip_count(), 1, "the trailing skip is counted");
        // The extra commit is metadata only: no items, but the skip and the
        // bookmark past it have to become durable somewhere.
        assert_eq!(commit.commits, 2);
    }

    /// The other half of the rule: an ordinary exhausted read commits nothing.
    /// Without this, "always commit on an empty chunk" would pass the test
    /// above while adding a pointless transaction to every step in the system.
    #[tokio::test]
    async fn an_exhausted_reader_with_no_skips_commits_nothing_extra() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = PoisonProcessor { poison: 0 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
        )
        .await
        .unwrap();

        assert_eq!(commit.commits, 1);
        assert_eq!(commit.begins, 1);
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
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
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
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(writer.0.written, vec![4, 8]);
        assert_eq!(commit.total.read_count(), 2);
        assert_eq!(context.get_long(POSITION).unwrap(), Some(2));
    }

    /// The reconciliation property: every published item counter equals what
    /// the same commit persisted. A metric that disagrees with the metadata
    /// store is worse than no metric.
    #[test]
    fn a_committed_chunk_publishes_counters_matching_what_it_persisted() {
        let mut commit = RecordingCommit::new();

        let snapshot = recorded(|| {
            block_on(async {
                let mut reader = VecReader::new(vec![1, 2, 3, 4, 5, 6]);
                let mut processor = EvenDoubler;
                let mut writer = Unmanaged(CollectingWriter::new());
                let mut context = ExecutionContext::new();

                run_step(
                    &mut reader,
                    &mut processor,
                    &mut writer,
                    &ChunkConfig::new(nz(2), &FaultTolerance::default(), "nightly", "load"),
                    &mut context,
                    &mut commit,
                )
                .await
                .unwrap();
            });
        });

        assert_eq!(
            counter(&snapshot, ITEMS_READ, None),
            Some(commit.total.read_count() as u64)
        );
        assert_eq!(
            counter(&snapshot, ITEMS_WRITTEN, None),
            Some(commit.total.write_count() as u64)
        );
        assert_eq!(
            counter(&snapshot, ITEMS_FILTERED, None),
            Some(commit.total.filter_count() as u64)
        );
        // chunk_size 2 over 6 items: the transaction boundary, not the totals.
        assert_eq!(counter(&snapshot, CHUNKS_COMMITTED, None), Some(3));

        assert_eq!(
            labels_of(&snapshot, ITEMS_READ),
            vec![
                (LABEL_JOB.to_owned(), "nightly".to_owned()),
                (LABEL_STEP.to_owned(), "load".to_owned()),
            ]
        );
    }

    /// Rule 2, and its one exception, in a single run: a chunk that retried and
    /// then failed publishes its *retries* but none of its item counters.
    ///
    /// Counting retries only on chunks that eventually commit would report zero
    /// for exactly the chunk an operator is paged about.
    #[test]
    fn a_chunk_that_retried_and_failed_publishes_retries_but_no_items() {
        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)));

        let snapshot = recorded(|| {
            block_on(async {
                let mut reader = VecReader::new(vec![2, 4]);
                let mut processor = EvenDoubler;
                let mut writer = Unmanaged(FailingWriter);
                let mut commit = RecordingCommit::new();
                let mut context = ExecutionContext::new();

                let result = run_step(
                    &mut reader,
                    &mut processor,
                    &mut writer,
                    &ChunkConfig::new(nz(2), &fault, "nightly", "load"),
                    &mut context,
                    &mut commit,
                )
                .await;

                assert!(result.is_err());
            });
        });

        // Three attempts means two retries.
        assert_eq!(counter(&snapshot, CHUNK_RETRIES, None), Some(2));

        // `Some(0)`, not `None`: hoisting the handles registers every series
        // when the step starts, so a metric that has not happened yet reads as
        // zero rather than being absent. That is what makes a rate query over a
        // job that has never failed return 0 instead of nothing at all.
        assert_eq!(counter(&snapshot, ITEMS_READ, None), Some(0));
        assert_eq!(counter(&snapshot, ITEMS_WRITTEN, None), Some(0));
        assert_eq!(counter(&snapshot, CHUNKS_COMMITTED, None), Some(0));
    }

    /// Skips are attributed to the phase that dropped the item: a bad input row
    /// and a bad transform are different incidents.
    #[test]
    fn skips_are_published_under_the_phase_that_dropped_the_item() {
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(10);

        let snapshot = recorded(|| {
            block_on(async {
                let mut reader = PoisonReader::new(vec![2, 3, 4], 3);
                let mut processor = EvenDoubler;
                let mut writer = Unmanaged(CollectingWriter::new());
                let mut commit = RecordingCommit::new();
                let mut context = ExecutionContext::new();

                run_step(
                    &mut reader,
                    &mut processor,
                    &mut writer,
                    &ChunkConfig::new(nz(4), &fault, "nightly", "load"),
                    &mut context,
                    &mut commit,
                )
                .await
                .unwrap();
            });
        });

        assert_eq!(counter(&snapshot, ITEMS_SKIPPED, Some("read")), Some(1));
        assert_eq!(counter(&snapshot, ITEMS_SKIPPED, Some("process")), Some(0));
    }

    /// A rollback that fails must not become the error the caller sees: the
    /// write is *why* there was a rollback, and reporting only the rollback
    /// leaves nobody able to say what the step was actually doing wrong.
    #[tokio::test]
    async fn a_failed_rollback_does_not_hide_the_write_error() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(FailingWriter);
        let mut commit = RecordingCommit::failing_rollback();
        let mut context = ExecutionContext::new();

        let error = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "nightly", "load"),
            &mut context,
            &mut commit,
        )
        .await
        .unwrap_err();

        let BatchError::CleanupFailed {
            cause,
            during_cleanup,
        } = &error
        else {
            panic!("expected both failures to survive, got {error:?}");
        };
        assert!(matches!(**cause, BatchError::Write(_)));
        assert!(matches!(**during_cleanup, BatchError::Repository(_)));

        // A `Classifier` walks `source()` looking for a concrete backend error
        // (`PostgresClassifier` downcasts to `sqlx::Error`). Wrapping must not
        // break that walk, so assert the innermost cause is still reachable.
        let mut node: Option<&(dyn Error + 'static)> = error.source();
        let mut depth = 0;
        while let Some(current) = node {
            depth += 1;
            node = current.source();
        }
        assert_eq!(depth, 2, "chain: Box<BatchError::Write> -> its Cause");
    }

    // ---- tracing (Phase 13b) ----

    /// A skipped row is a data-quality incident. The counter says *how many*;
    /// only the event says *where*, which is what separates a corrupt segment
    /// from a systematic parse bug.
    #[tokio::test]
    async fn a_skipped_read_emits_a_warning_naming_the_phase() {
        let mut reader = PoisonReader::new(vec![2, 99, 4, 6], 99);
        let mut processor = PoisonProcessor { poison: 0 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(1);

        let (result, events) = captured(run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(3), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
        ))
        .await;
        result.unwrap();

        let skips = events_named(&events, Level::WARN, "item skipped");
        assert_eq!(skips.len(), 1, "{events:#?}");
        assert_eq!(skips[0].field(FIELD_PHASE), Some("read"));
        // The running ordinal, not a flag: consecutive ordinals across nearby
        // reads are what "contiguous" looks like in a log.
        assert_eq!(skips[0].field(FIELD_SKIPPED), Some("1"));
        assert!(
            skips[0]
                .field("error")
                .unwrap()
                .contains("malformed row 99")
        );
    }

    #[tokio::test]
    async fn a_skipped_process_item_emits_a_warning_naming_the_phase() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = PoisonProcessor { poison: 4 };
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(1);

        let (result, events) = captured(run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
        ))
        .await;
        result.unwrap();

        let skips = events_named(&events, Level::WARN, "item skipped");
        assert_eq!(skips.len(), 1, "{events:#?}");
        assert_eq!(skips[0].field(FIELD_PHASE), Some("process"));
        assert!(skips[0].field(FIELD_ERROR).unwrap().contains("bad item 4"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_retried_write_emits_an_event_carrying_its_attempt_number() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(TransientWriter::new(1));
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)));

        let (result, events) = captured(run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
        ))
        .await;
        result.unwrap();

        let retries = events_named(&events, Level::WARN, "chunk failed, retrying");
        assert_eq!(retries.len(), 1, "{events:#?}");
        assert_eq!(retries[0].field(FIELD_ATTEMPT), Some("1"));
        assert!(retries[0].field(FIELD_ERROR).unwrap().contains("deadlock"));
    }

    /// The retry an operator actually sees in production: Postgres raises
    /// `40001` *at* `COMMIT`, so this path — not the write path — is where a
    /// serialization failure surfaces. It emitted nothing until 13b-3.
    #[tokio::test(start_paused = true)]
    async fn a_retried_commit_emits_its_own_event() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(CollectingWriter::new());
        let mut commit = RecordingCommit::failing_commits(1);
        let mut context = ExecutionContext::new();

        let fault = FaultTolerance::new()
            .classifier(RetryAll)
            .retry(RetryPolicy::attempts(nz32(3)));

        let (result, events) = captured(run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(2), &fault, "test-job", "test-step"),
            &mut context,
            &mut commit,
        ))
        .await;
        result.unwrap();

        let retries = events_named(&events, Level::WARN, "chunk commit failed, retrying");
        assert_eq!(retries.len(), 1, "{events:#?}");
        assert_eq!(retries[0].field(FIELD_ATTEMPT), Some("1"));

        // Distinct message from the write-path retry: an operator grepping for
        // one must not silently match the other.
        assert!(events_named(&events, Level::WARN, "chunk failed, retrying").is_empty());
    }

    /// A rollback that fails leaves the transaction in an unknown state, and by
    /// the time the error reaches a caller that fact has stopped being
    /// actionable at the site that knew it. ADR-009 keeps both failures in the
    /// returned error; this is the event at the moment it happens.
    #[tokio::test]
    async fn a_failed_rollback_emits_an_error_event() {
        let mut reader = VecReader::new(vec![2, 4]);
        let mut processor = EvenDoubler;
        let mut writer = Unmanaged(FailingWriter);
        let mut commit = RecordingCommit::failing_rollback();
        let mut context = ExecutionContext::new();

        let (result, events) = captured(run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &ChunkConfig::new(nz(2), &FaultTolerance::default(), "test-job", "test-step"),
            &mut context,
            &mut commit,
        ))
        .await;
        result.unwrap_err();

        let failures = events_named(
            &events,
            Level::ERROR,
            "rollback failed; the transaction is in an unknown state",
        );
        assert_eq!(failures.len(), 1, "{events:#?}");
    }
}

#[cfg(test)]
mod scan_tests {
    use super::*;
    use crate::metrics::{CHUNK_SCANS, ITEMS_SKIPPED};
    use crate::testing::{
        BatchHostileWriter, CommitEvent, PoisonWriter, RecordingCommit, SkipAll, VecReader,
        counter, nz, recorded,
    };
    use crate::{ExecutionContext, Unmanaged};

    struct Identity;

    impl ItemProcessor for Identity {
        type In = u32;
        type Out = u32;

        async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
            Ok(Some(item))
        }
    }

    fn scanning(skip_limit: usize) -> FaultTolerance {
        FaultTolerance::new()
            .classifier(SkipAll)
            .skip_limit(skip_limit)
            .scan_on_write_failure(true)
    }

    async fn run(
        items: Vec<u32>,
        poison: u32,
        fault: FaultTolerance,
        chunk_size: usize,
    ) -> (Result<(), BatchError>, RecordingCommit, Vec<Vec<u32>>) {
        let mut reader = VecReader::new(items);
        let mut processor = Identity;
        let mut writer = Unmanaged(PoisonWriter::new(poison));
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();
        let config = ChunkConfig {
            chunk_size: nz(chunk_size),
            fault: &fault,
            metrics: ChunkMetrics::new("nightly", "load"),
        };

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &config,
            &mut context,
            &mut commit,
        )
        .await;

        (result, commit, writer.0.attempts)
    }

    /// FR-6.4: one bad row no longer costs the whole chunk.
    #[tokio::test]
    async fn a_poison_item_is_isolated_and_the_rest_of_the_chunk_commits() {
        let (result, commit, _) = run(vec![1, 2, 3], 2, scanning(5), 3).await;

        result.unwrap();
        assert_eq!(commit.total.write_count(), 2);
        assert_eq!(commit.total.skip_count(), 1);
        assert_eq!(commit.total.read_count(), 3);
    }

    /// The crash-safety property, and the reason the scan is two passes rather
    /// than a stream of per-item commits: every transaction the identifying
    /// pass opens is rolled back, and exactly one commit happens at the end.
    #[tokio::test]
    async fn the_identifying_pass_commits_nothing() {
        let (result, commit, _) = run(vec![1, 2, 3], 2, scanning(5), 3).await;
        result.unwrap();

        // batch attempt + 3 single-item probes + the real write.
        assert_eq!(commit.begins, 5);
        assert_eq!(commit.rollbacks, 4);
        assert_eq!(commit.commits, 1);
        assert_eq!(*commit.events.last().unwrap(), CommitEvent::Commit);
        assert_eq!(
            commit
                .events
                .iter()
                .filter(|e| **e == CommitEvent::Commit)
                .count(),
            1
        );
    }

    /// The cost, asserted rather than described: every good item is written
    /// twice — once to ask whether it is the poison one, once for real.
    #[tokio::test]
    async fn a_scan_writes_every_good_item_twice() {
        let (result, _, attempts) = run(vec![1, 2, 3], 2, scanning(5), 3).await;
        result.unwrap();

        assert_eq!(
            attempts,
            vec![
                vec![1, 2, 3], // the batch that failed
                vec![1],       // identifying pass
                vec![2],       // the poison item
                vec![3],
                vec![1, 3], // the survivors, for real
            ]
        );
    }

    /// Off unless asked for. Scanning costs a second pass and, with an
    /// `Unmanaged` writer, real duplicate delivery.
    #[tokio::test]
    async fn scanning_is_off_by_default() {
        let fault = FaultTolerance::new().classifier(SkipAll).skip_limit(5);
        let (result, commit, _) = run(vec![1, 2, 3], 2, fault, 3).await;

        result.unwrap_err();
        assert_eq!(commit.commits, 0);
    }

    #[tokio::test]
    async fn a_scan_that_exceeds_the_skip_limit_fails_the_step() {
        let (result, _, _) = run(vec![1, 2, 3], 2, scanning(0), 3).await;

        let error = result.unwrap_err();
        assert!(
            matches!(error, BatchError::SkipLimitExceeded { limit: 0, .. }),
            "expected SkipLimitExceeded, got {error:?}"
        );
    }

    /// A scanned chunk's survivors each wrote cleanly alone, so if the batch
    /// still fails the failure is real — rescanning would find nothing to skip
    /// and spin forever.
    ///
    /// Removing the `scanned` guard makes this **hang** rather than fail, which
    /// is the honest signature of a missing loop bound.
    #[tokio::test]
    async fn a_chunk_is_scanned_at_most_once() {
        let fault = scanning(5);
        let mut reader = VecReader::new(vec![1, 2, 3]);
        let mut processor = Identity;
        let mut writer = Unmanaged(BatchHostileWriter);
        let mut commit = RecordingCommit::new();
        let mut context = ExecutionContext::new();
        let config = ChunkConfig {
            chunk_size: nz(3),
            fault: &fault,
            metrics: ChunkMetrics::new("nightly", "load"),
        };

        let result = run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &config,
            &mut context,
            &mut commit,
        )
        .await;

        result.unwrap_err();
        assert_eq!(commit.commits, 0);
        // One batch attempt, three probes, one retry of the survivors: five.
        assert_eq!(commit.begins, 5, "a second scan would push this higher");
    }

    #[tokio::test]
    async fn a_fully_poisoned_chunk_skips_everything_and_still_advances() {
        let (result, commit, _) = run(vec![2, 2], 2, scanning(5), 2).await;

        result.unwrap();
        assert_eq!(commit.total.write_count(), 0);
        assert_eq!(commit.total.skip_count(), 2);
        assert_eq!(commit.commits, 1, "the bookmark must still advance");
    }

    /// Sync, not `#[tokio::test]`: `recorded` scopes a thread-local recorder
    /// and `block_on` builds its own runtime, so an outer one would nest.
    #[test]
    fn a_write_skip_is_counted_under_its_own_phase() {
        let snapshot = recorded(|| {
            crate::testing::block_on(async {
                run(vec![1, 2, 3], 2, scanning(5), 3).await.0.unwrap();
            });
        });

        assert_eq!(counter(&snapshot, ITEMS_SKIPPED, Some("write")), Some(1));
        assert_eq!(counter(&snapshot, ITEMS_SKIPPED, Some("read")), Some(0));
        assert_eq!(counter(&snapshot, ITEMS_SKIPPED, Some("process")), Some(0));
        assert_eq!(counter(&snapshot, CHUNK_SCANS, None), Some(1));
    }
}
