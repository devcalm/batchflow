use crate::{BatchError, ExecutionContext};
use std::future::Future;

/// Produces items one at a time, and remembers where it got to.
///
/// `&mut self` means a reader cannot be shared, so parallelism comes from
/// partitioning rather than from sharing one reader.
///
/// The return type is written out rather than declared `async fn` so it can
/// carry `+ Send`: an `async fn` in a trait produces an unnameable future type
/// that no bound can constrain. Implementors still write a plain `async fn`.
pub trait ItemReader {
    /// What this reader produces.
    type Item;

    /// Reads the next item, or `Ok(None)` at end of input.
    fn read(&mut self) -> impl Future<Output = Result<Option<Self::Item>, BatchError>> + Send;

    /// Restore position from a previous run.
    ///
    /// Default: do nothing, i.e. start from the beginning. A reader that does
    /// not override this is simply not restartable.
    fn open(
        &mut self,
        _context: &ExecutionContext,
    ) -> impl Future<Output = Result<(), BatchError>> + Send {
        async { Ok(()) }
    }

    /// Record the current position into `context`.
    ///
    /// Sync and infallible on purpose: writing your own position into a map
    /// cannot fail, and an `async fn` that never awaits misrepresents what the
    /// function does. `open` is async because it may seek a file or reposition
    /// a database cursor.
    ///
    /// Default: record nothing, matching the default `open`.
    fn update(&self, _context: &mut ExecutionContext) {}

    /// Release whatever [`open`](Self::open) acquired.
    ///
    /// Called once when the step ends, on both the success and the failure
    /// path, and paired with `open`: a reader whose `open` failed is never
    /// closed, because it never opened.
    ///
    /// An error here fails the step. A reader that cannot release a file handle
    /// or a cursor is reporting that something is wrong with the resource, and
    /// swallowing it would hide it — the bookmark is already durable by this
    /// point, so a restart resumes correctly either way.
    ///
    /// Default: nothing, which is correct for a reader that holds nothing.
    fn close(&mut self) -> impl Future<Output = Result<(), BatchError>> + Send {
        async { Ok(()) }
    }
}

/// Writes a whole chunk at once.
///
/// Chunk-oriented rather than per-item so a backend can batch its I/O: one
/// `COPY` or one multi-row `INSERT` per commit interval, not one round trip per
/// row.
pub trait ItemWriter {
    /// What this writer accepts.
    type Item;

    /// Writes one chunk. Called once per commit interval.
    fn write(
        &mut self,
        items: &[Self::Item],
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Flush anything buffered and release resources.
    ///
    /// Called once when the step ends, on both the success and the failure
    /// path. **An error here fails the step**, and that is the point: a writer
    /// that buffers — `BufWriter`, a batching HTTP client, a `csv::Writer` —
    /// still holds part of the last chunk when `write` returns, and flushing
    /// only on `Drop` makes the resulting `io::Error` unobservable. A full disk
    /// would then produce a job that reported success and wrote a truncated
    /// file.
    ///
    /// Default: nothing, which is correct for a writer that buffers nothing.
    ///
    /// ```
    /// # use batchflow_core::{BatchError, ItemWriter};
    /// # use std::io::Write;
    /// struct CsvWriter<W: Write + Send> {
    ///     out: std::io::BufWriter<W>,
    /// }
    ///
    /// impl<W: Write + Send> ItemWriter for CsvWriter<W> {
    ///     type Item = String;
    ///
    ///     async fn write(&mut self, items: &[String]) -> Result<(), BatchError> {
    ///         for line in items {
    ///             writeln!(self.out, "{line}").map_err(BatchError::write)?;
    ///         }
    ///         Ok(())
    ///     }
    ///
    ///     // Without this, the last partial buffer is flushed by `Drop`, where
    ///     // a failure has nowhere to go.
    ///     async fn close(&mut self) -> Result<(), BatchError> {
    ///         self.out.flush().map_err(BatchError::write)
    ///     }
    /// }
    /// ```
    fn close(&mut self) -> impl Future<Output = Result<(), BatchError>> + Send {
        async { Ok(()) }
    }
}

/// An [`ItemWriter`] that enlists in the step's transaction, so its writes
/// commit with the counters and the bookmark (FR-2.4).
///
/// Writers that cannot enlist — CSV, S3, stdout — keep implementing plain
/// [`ItemWriter`] and are adapted with [`Unmanaged`], which is an explicit
/// acceptance of at-least-once for that step.
pub trait TransactionalWriter<Tx>: Send {
    /// What this writer accepts.
    type Item;

    /// Writes one chunk inside the step's transaction.
    fn write(
        &mut self,
        tx: &mut Tx,
        items: &[Self::Item],
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    /// Release resources at the end of the step.
    ///
    /// No transaction is passed, and that is deliberate: the last chunk's
    /// transaction has already committed or rolled back by the time this runs,
    /// and opening another one here would write outside every commit interval —
    /// data with no counters and no bookmark. A transactional writer normally
    /// has nothing to do here, since its durability is the transaction's.
    ///
    /// Default: nothing. See [`ItemWriter::close`] for the buffered case this
    /// exists for.
    fn close(&mut self) -> impl Future<Output = Result<(), BatchError>> + Send {
        async { Ok(()) }
    }
}

/// Adapts a plain [`ItemWriter`] — or a plain [`Tasklet`](crate::Tasklet) — to
/// any transaction by ignoring it.
///
/// A blanket `impl<W: ItemWriter, Tx> TransactionalWriter<Tx> for W` is not
/// possible: it would overlap every direct impl, and Rust cannot prove a type
/// is *not* an `ItemWriter`. A distinct `Self` type is the way out, and making
/// it explicit at the call site is the point — non-transactional writing should
/// be visible, not inferred.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unmanaged<W>(pub W);

impl<W, Tx> TransactionalWriter<Tx> for Unmanaged<W>
where
    W: ItemWriter + Send,
    W::Item: Sync,
    Tx: Send,
{
    type Item = W::Item;

    fn write(
        &mut self,
        _tx: &mut Tx,
        items: &[Self::Item],
    ) -> impl Future<Output = Result<(), BatchError>> + Send {
        self.0.write(items)
    }

    /// Delegates to the wrapped writer, which is where the buffer is.
    fn close(&mut self) -> impl Future<Output = Result<(), BatchError>> + Send {
        self.0.close()
    }
}

/// Transforms one item, or drops it.
pub trait ItemProcessor {
    /// What this processor consumes.
    type In;
    /// What it produces.
    type Out;

    /// Transforms one item. `Ok(None)` filters it out, which is counted
    /// separately from a skip: a filter is a deliberate decision, a skip is a
    /// tolerated failure.
    fn process(
        &mut self,
        item: Self::In,
    ) -> impl Future<Output = Result<Option<Self::Out>, BatchError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContextValue;
    use crate::testing::{BookmarkReader, POSITION, VecReader};

    /// Pins the default bodies: a reader that overrides neither method is
    /// simply not restartable, and must not pretend otherwise.
    #[tokio::test]
    async fn a_plain_reader_records_nothing() {
        let mut reader = VecReader::new(vec![1, 2, 3]);
        let mut context = ExecutionContext::new();

        reader.open(&context).await.unwrap();
        reader.read().await.unwrap();
        reader.update(&mut context);

        assert!(context.is_empty());
    }

    #[tokio::test]
    async fn a_bookmark_reader_records_its_position() {
        let mut reader = BookmarkReader::new(vec![1, 2, 3, 4, 5]);
        let mut context = ExecutionContext::new();

        reader.read().await.unwrap();
        reader.read().await.unwrap();
        reader.update(&mut context);

        assert_eq!(context.get_long(POSITION).unwrap(), Some(2));
    }

    #[tokio::test]
    async fn open_resumes_from_a_recorded_position() {
        let mut context = ExecutionContext::new();
        context.put(POSITION, ContextValue::Long(3));

        let mut reader = BookmarkReader::new(vec![1, 2, 3, 4, 5]);
        reader.open(&context).await.unwrap();

        // Items 1..=3 were already committed by the previous run. Resuming at
        // the start instead would re-write all three.
        assert_eq!(reader.read().await.unwrap(), Some(4));
    }

    #[tokio::test]
    async fn open_rejects_a_corrupt_bookmark() {
        let mut context = ExecutionContext::new();
        context.put(POSITION, ContextValue::String("halfway".into()));

        let mut reader = BookmarkReader::new(vec![1, 2, 3]);

        assert!(reader.open(&context).await.is_err());
    }

    /// `as` would wrap a negative into a huge `usize`; `try_from` refuses it.
    #[tokio::test]
    async fn open_rejects_a_negative_bookmark() {
        let mut context = ExecutionContext::new();
        context.put(POSITION, ContextValue::Long(-1));

        let mut reader = BookmarkReader::new(vec![1, 2, 3]);

        assert!(reader.open(&context).await.is_err());
    }
}
