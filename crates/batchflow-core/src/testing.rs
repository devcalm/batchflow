//! Shared test doubles.
//!
//! Compiled only under `cfg(test)`, so nothing here reaches the public API or a
//! release build. Lives in its own module because several modules' tests need the
//! same fakes, and duplicating them is how test suites drift apart.

use crate::metrics::LABEL_PHASE;
use crate::{
    BatchError, BatchStatus, Classifier, ContextValue, ErrorAction, ExecutionContext,
    InMemoryJobRepository, ItemProcessor, ItemReader, ItemWriter, JobExecution, JobExecutionId,
    JobInstance, JobInstanceId, JobParameters, JobRepository, RepeatStatus, Step, StepCommit,
    StepContribution, StepExecution, StepExecutionId, StepIdentity, Tasklet,
};
use ::metrics::{Key, SharedString, Unit};
use async_trait::async_trait;
use metrics_util::CompositeKey;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::collections::BTreeMap;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex, Once};
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber, span};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// Shorthand for a chunk-size literal. A zero here is a bug in the test itself,
/// so panicking is the right response.
pub(crate) fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("test chunk size must be non-zero")
}

/// Shorthand for a retry-attempt literal.
pub(crate) fn nz32(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("test attempt count must be non-zero")
}

/// Classifies every error as transient.
///
/// No real policy looks like this — it exists so a test can drive the retry
/// path without needing a backend whose errors carry SQLSTATE codes.
pub(crate) struct RetryAll;

impl Classifier for RetryAll {
    fn classify(&self, _error: &BatchError) -> ErrorAction {
        ErrorAction::Retry
    }
}

/// Classifies every error as a bad item.
pub(crate) struct SkipAll;

impl Classifier for SkipAll {
    fn classify(&self, _error: &BatchError) -> ErrorAction {
        ErrorAction::Skip
    }
}

/// Errors on any item equal to `poison`, *having already advanced past it*.
///
/// The advance is what makes a read error skippable at all: a reader that
/// errors without moving on would be handed the same item forever.
pub(crate) struct PoisonReader {
    pub items: Vec<u32>,
    pub pos: usize,
    pub poison: u32,
}

impl PoisonReader {
    pub(crate) fn new(items: Vec<u32>, poison: u32) -> Self {
        Self {
            items,
            pos: 0,
            poison,
        }
    }
}

impl ItemReader for PoisonReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
        let Some(item) = self.items.get(self.pos).copied() else {
            return Ok(None);
        };
        self.pos += 1;

        if item == self.poison {
            return Err(BatchError::read(format!("malformed row {item}")));
        }
        Ok(Some(item))
    }
}

/// Doubles every item except `poison`, which fails.
///
/// Never returns `None`, so a test can tell a skip from a filter without the
/// two counters shadowing each other.
pub(crate) struct PoisonProcessor {
    pub poison: u32,
}

impl ItemProcessor for PoisonProcessor {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        if item == self.poison {
            return Err(BatchError::process(format!("bad item {item}")));
        }
        Ok(Some(item * 2))
    }
}

/// Reads a fixed list of items, then reports exhaustion.
pub(crate) struct VecReader {
    pub items: Vec<u32>,
    pub pos: usize,
}

impl VecReader {
    pub(crate) fn new(items: Vec<u32>) -> Self {
        Self { items, pos: 0 }
    }
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

/// Key `BookmarkReader` stores its position under.
pub(crate) const POSITION: &str = "position";

/// Like [`VecReader`], but restartable: it records its position and seeks back
/// to it on `open`.
///
/// `VecReader` stays plain on purpose, so the *default* (non-restartable)
/// `open`/`update` bodies keep getting exercised too.
pub(crate) struct BookmarkReader {
    pub items: Vec<u32>,
    pub pos: usize,
}

impl BookmarkReader {
    pub(crate) fn new(items: Vec<u32>) -> Self {
        Self { items, pos: 0 }
    }
}

impl ItemReader for BookmarkReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
        let item = self.items.get(self.pos).copied();
        if item.is_some() {
            self.pos += 1;
        }
        Ok(item)
    }

    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        // The `?` is what makes a corrupt bookmark abort the run instead of
        // silently restarting it from zero.
        if let Some(position) = context.get_long(POSITION)? {
            // `try_from`, not `as`: a negative position is corrupt data, and
            // `as` would wrap it into a huge number instead of complaining.
            self.pos = usize::try_from(position)
                .map_err(|_| BatchError::read(format!("negative bookmark {position}")))?;
        }

        Ok(())
    }

    fn update(&self, context: &mut ExecutionContext) {
        // usize -> i64 only loses data above 2^63 items, which is not a batch.
        context.put(POSITION, ContextValue::Long(self.pos as i64));
    }
}

/// Yields `remaining_ok` items, then errors.
pub(crate) struct FailingReader {
    pub remaining_ok: usize,
}

impl ItemReader for FailingReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        if self.remaining_ok == 0 {
            return Err(BatchError::read("boom"));
        }
        self.remaining_ok -= 1;
        Ok(Some(7))
    }
}

/// Doubles even items; filters odd ones out by returning `None`.
pub(crate) struct EvenDoubler;

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

/// Accumulates everything written, so tests can assert on the exact output.
pub(crate) struct CollectingWriter {
    pub written: Vec<u32>,
}

impl CollectingWriter {
    pub(crate) fn new() -> Self {
        Self {
            written: Vec::new(),
        }
    }
}

impl ItemWriter for CollectingWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        self.written.extend_from_slice(items);
        Ok(())
    }
}

/// Fails any batch containing `poison`, and records every batch it was asked
/// to write.
///
/// Writing a *batch* is what fails, so a chunk containing the poison item
/// cannot succeed however often it is retried — only isolating the item can.
/// `attempts` exposes the two passes a scan makes, which is otherwise invisible.
pub(crate) struct PoisonWriter {
    pub poison: u32,
    pub attempts: Vec<Vec<u32>>,
}

impl PoisonWriter {
    pub(crate) fn new(poison: u32) -> Self {
        Self {
            poison,
            attempts: Vec::new(),
        }
    }
}

impl ItemWriter for PoisonWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        self.attempts.push(items.to_vec());
        if items.contains(&self.poison) {
            return Err(BatchError::write(format!(
                "row {} is malformed",
                self.poison
            )));
        }
        Ok(())
    }
}

/// Succeeds on a single item and fails on any batch — so every item survives
/// the identifying pass and the survivors still cannot be written together.
///
/// A real shape, not a contrived one: a unique constraint spanning two rows in
/// the same chunk behaves exactly like this.
pub(crate) struct BatchHostileWriter;

impl ItemWriter for BatchHostileWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if items.len() > 1 {
            return Err(BatchError::write("this batch cannot be written together"));
        }
        Ok(())
    }
}

/// Fails every write — pins the rule that counters are recorded only once the
/// write has succeeded.
pub(crate) struct FailingWriter;

impl ItemWriter for FailingWriter {
    type Item = u32;

    async fn write(&mut self, _items: &[u32]) -> Result<(), BatchError> {
        Err(BatchError::write("boom"))
    }
}

/// Succeeds for `ok_writes` chunks, then fails — lets tests observe the state a
/// partially-completed step leaves behind.
pub(crate) struct FlakyWriter {
    pub written: Vec<u32>,
    pub ok_writes: usize,
}

impl FlakyWriter {
    pub(crate) fn new(ok_writes: usize) -> Self {
        Self {
            written: Vec::new(),
            ok_writes,
        }
    }
}

impl ItemWriter for FlakyWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if self.ok_writes == 0 {
            return Err(BatchError::write("boom"));
        }
        self.ok_writes -= 1;
        self.written.extend_from_slice(items);
        Ok(())
    }
}

/// Fails its first `failures` writes, then succeeds.
///
/// The inverse of [`FlakyWriter`], which succeeds first: this one exercises
/// recovery, that one exercises partial progress.
pub(crate) struct TransientWriter {
    pub written: Vec<u32>,
    pub failures: usize,
}

impl TransientWriter {
    pub(crate) fn new(failures: usize) -> Self {
        Self {
            written: Vec::new(),
            failures,
        }
    }
}

impl ItemWriter for TransientWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if self.failures > 0 {
            self.failures -= 1;
            return Err(BatchError::write("deadlock"));
        }
        self.written.extend_from_slice(items);
        Ok(())
    }
}

/// A write destination that outlives the step writing to it, so a restart test
/// can see everything written across both attempts. A step-owned writer would
/// drop its evidence, making a duplicated item invisible.
#[derive(Clone, Default)]
pub(crate) struct SharedSink(Arc<Mutex<Vec<u32>>>);

impl SharedSink {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A writer onto this sink that succeeds `ok_writes` times, then fails.
    pub(crate) fn writer(&self, ok_writes: usize) -> SharedWriter {
        SharedWriter {
            sink: Arc::clone(&self.0),
            ok_writes,
        }
    }

    pub(crate) fn written(&self) -> Vec<u32> {
        self.0.lock().expect("sink poisoned").clone()
    }

    /// Appends directly, for writers built by a test rather than by
    /// [`writer`](Self::writer).
    pub(crate) fn record(&self, items: &[u32]) {
        self.0
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(items);
    }
}

pub(crate) struct SharedWriter {
    sink: Arc<Mutex<Vec<u32>>>,
    ok_writes: usize,
}

impl ItemWriter for SharedWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if self.ok_writes == 0 {
            return Err(BatchError::write("boom"));
        }
        self.ok_writes -= 1;

        // Guard created and dropped in one statement: holding it across an
        // `.await` would make the future `!Send`.
        self.sink
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(items);

        Ok(())
    }
}

/// An [`InMemoryJobRepository`] whose `commit` always fails.
///
/// It cannot roll anything back — no fake can — but it does let a test observe
/// what the engine does *around* a failed commit, which is our logic rather
/// than the backend's.
pub(crate) struct CommitFails(InMemoryJobRepository);

impl CommitFails {
    pub(crate) fn new() -> Self {
        Self(InMemoryJobRepository::default())
    }
}

impl JobRepository for CommitFails {
    type Tx = ();

    async fn begin(&self) -> Result<(), BatchError> {
        Ok(())
    }

    async fn commit(&self, _tx: ()) -> Result<(), BatchError> {
        Err(BatchError::repository("commit failed"))
    }

    async fn rollback(&self, tx: ()) -> Result<(), BatchError> {
        self.0.rollback(tx).await
    }

    async fn update_step_execution_in(
        &self,
        tx: &mut (),
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        self.0.update_step_execution_in(tx, step_execution).await
    }

    async fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<JobInstance, BatchError> {
        self.0.find_or_create_instance(job_name, parameters).await
    }

    async fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<Option<JobInstance>, BatchError> {
        self.0.find_instance(job_name, parameters).await
    }

    async fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<JobExecution, BatchError> {
        self.0.create_execution(instance_id).await
    }

    async fn update_execution(&self, execution: &JobExecution) -> Result<(), BatchError> {
        self.0.update_execution(execution).await
    }

    async fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Option<JobExecution>, BatchError> {
        self.0.last_execution(instance_id).await
    }

    async fn executions(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Vec<JobExecution>, BatchError> {
        self.0.executions(instance_id).await
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        self.0.abandon_execution(execution_id).await
    }

    async fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> Result<StepExecution, BatchError> {
        self.0
            .create_step_execution(job_execution_id, step_name)
            .await
    }

    async fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        self.0.update_step_execution(step_execution).await
    }

    async fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> Result<Option<StepExecution>, BatchError> {
        self.0.last_step_execution(instance_id, step_name).await
    }

    async fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Vec<StepExecution>, BatchError> {
        self.0.step_executions(job_execution_id).await
    }
}

/// A tasklet-style step that does no item I/O — stands in for "delete temp files,
/// send a report". Proves a `Job` can hold heterogeneous step types.
pub(crate) struct LogStep;

#[async_trait]
impl Step for LogStep {
    fn name(&self) -> &str {
        "log"
    }

    // A tasklet reads and writes nothing, so it has nothing to commit.
    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit<()>,
    ) -> Result<(), BatchError> {
        Ok(())
    }
}

/// A tasklet that does `total` passes, one per `execute` call, and records how
/// many are done in its context.
///
/// Counting passes in the *context* rather than in a field is what makes it a
/// restart fake: handed a context that already says `2`, it does one pass, not
/// three — exactly as a real archiver resuming after a crash would.
pub(crate) struct CountingTasklet {
    total: i64,
}

impl CountingTasklet {
    /// The bookmark key. Public to the tests, which assert on what committed.
    pub(crate) const DONE: &'static str = "passes.done";

    pub(crate) fn new(total: i64) -> Self {
        Self { total }
    }
}

impl Tasklet for CountingTasklet {
    async fn execute(
        &mut self,
        context: &mut ExecutionContext,
        contribution: &mut StepContribution,
    ) -> Result<RepeatStatus, BatchError> {
        let done = context.get_long(Self::DONE)?.unwrap_or(0) + 1;
        context.put(Self::DONE, ContextValue::Long(done));
        contribution.increment_write(1);

        Ok(if done >= self.total {
            RepeatStatus::Finished
        } else {
            RepeatStatus::Continuable
        })
    }
}

/// A tasklet that increments its counters and *then* fails, so a test can see
/// that the deltas went with the rolled-back transaction rather than being
/// folded. Failing before incrementing would make the assertion vacuous — the
/// same trap 10e's retry writer fell into.
pub(crate) struct FailingTasklet;

impl Tasklet for FailingTasklet {
    async fn execute(
        &mut self,
        _context: &mut ExecutionContext,
        contribution: &mut StepContribution,
    ) -> Result<RepeatStatus, BatchError> {
        contribution.increment_write(7);
        Err(BatchError::write("tasklet failed"))
    }
}

/// A step that always fails — lets tests drive the launcher's failure path and
/// assert that the step's *own* error reaches the caller unwrapped.
pub(crate) struct FailingStep;

#[async_trait]
impl Step for FailingStep {
    fn name(&self) -> &str {
        "failing"
    }

    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit<()>,
    ) -> Result<(), BatchError> {
        Err(BatchError::process("boom"))
    }
}

/// A writer that buffers, so the last chunk only lands on `close`.
///
/// The whole reason `close` exists: without it the tail sits in `pending`
/// forever and the step reports success having written a truncated result.
#[derive(Default)]
pub(crate) struct BufferingWriter {
    /// Holds items until a flush; a real one would be a `BufWriter`.
    pub pending: Vec<u32>,
    pub flushed: Arc<Mutex<Vec<u32>>>,
    pub closes: Arc<Mutex<usize>>,
    /// Fails the flush, as a full disk would.
    pub fail_on_close: bool,
}

impl BufferingWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn failing() -> Self {
        Self {
            fail_on_close: true,
            ..Self::default()
        }
    }

    pub fn flushed(&self) -> Vec<u32> {
        self.flushed.lock().expect("flushed poisoned").clone()
    }

    pub fn closes(&self) -> usize {
        *self.closes.lock().expect("closes poisoned")
    }
}

impl ItemWriter for BufferingWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        self.pending.extend_from_slice(items);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), BatchError> {
        *self.closes.lock().expect("closes poisoned") += 1;

        if self.fail_on_close {
            return Err(BatchError::write("flush failed: no space left on device"));
        }

        self.flushed
            .lock()
            .expect("flushed poisoned")
            .append(&mut self.pending);
        Ok(())
    }
}

/// Records whether `close` was called, so the failure path can assert it too.
pub(crate) struct ClosingReader {
    pub inner: VecReader,
    pub closes: Arc<Mutex<usize>>,
}

impl ClosingReader {
    pub fn new(items: Vec<u32>) -> Self {
        Self {
            inner: VecReader::new(items),
            closes: Arc::new(Mutex::new(0)),
        }
    }

    pub fn closes(&self) -> usize {
        *self.closes.lock().expect("closes poisoned")
    }
}

impl ItemReader for ClosingReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        self.inner.read().await
    }

    async fn close(&mut self) -> Result<(), BatchError> {
        *self.closes.lock().expect("closes poisoned") += 1;
        Ok(())
    }
}

/// Fails `open`, to pin that `close` is paired with it rather than run
/// unconditionally.
pub(crate) struct UnopenableReader;

impl ItemReader for UnopenableReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        Ok(None)
    }

    async fn open(&mut self, _context: &ExecutionContext) -> Result<(), BatchError> {
        Err(BatchError::read("cannot open input"))
    }

    async fn close(&mut self) -> Result<(), BatchError> {
        panic!("close must not run for a reader whose open failed");
    }
}

/// Panics rather than returning an error — an `unwrap()` in user code.
///
/// The panic boundary in `Job::run` is what stops this from unwinding past the
/// terminal status write and leaving the instance blocked forever.
pub(crate) struct PanickingStep;

#[async_trait]
impl Step for PanickingStep {
    fn name(&self) -> &str {
        "panicking"
    }

    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit<()>,
    ) -> Result<(), BatchError> {
        panic!("called `Option::unwrap()` on a `None` value");
    }
}

/// Runs `body` with the panic hook suppressed.
///
/// Without it the harness prints a backtrace for every deliberately caught
/// panic, which makes a passing run look like a failing one.
pub(crate) fn without_panic_output<T>(body: impl FnOnce() -> T) -> T {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = body();
    std::panic::set_hook(previous);
    outcome
}

/// What happened at a transaction boundary, in order.
///
/// Counters alone cannot tell "rolled back, then opened a fresh transaction"
/// from "opened two transactions and rolled one back at the end" — only the
/// sequence can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitEvent {
    Begin,
    Commit,
    Rollback,
}

/// Accumulates what a step commits, so tests can assert on totals and on the
/// *number* of commit points without needing a repository.
#[derive(Default)]
pub(crate) struct RecordingCommit {
    pub total: StepContribution,
    pub begins: usize,
    pub commits: usize,
    pub rollbacks: usize,
    pub context: ExecutionContext,
    pub events: Vec<CommitEvent>,
    /// Fail this many `commit` calls before the first success.
    pub commit_failures: usize,
    /// Fail every `rollback`.
    pub rollback_fails: bool,
}

impl RecordingCommit {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn failing_commits(commit_failures: usize) -> Self {
        Self {
            commit_failures,
            ..Self::default()
        }
    }

    pub(crate) fn failing_rollback() -> Self {
        Self {
            rollback_fails: true,
            ..Self::default()
        }
    }
}

#[async_trait]
impl StepCommit<()> for RecordingCommit {
    fn identity(&self) -> StepIdentity<'_> {
        StepIdentity {
            job_name: "test-job",
            step_name: "test-step",
            job_execution_id: JobExecutionId::new(1),
            step_execution_id: StepExecutionId::new(1),
        }
    }

    async fn begin(&mut self) -> Result<(), BatchError> {
        self.begins += 1;
        self.events.push(CommitEvent::Begin);
        Ok(())
    }

    async fn commit(
        &mut self,
        _tx: (),
        contribution: &StepContribution,
        context: &ExecutionContext,
    ) -> Result<(), BatchError> {
        // Recorded on entry, so a *failed* commit still shows in the sequence —
        // otherwise a test cannot see that no rollback followed it.
        self.events.push(CommitEvent::Commit);

        if self.commit_failures > 0 {
            self.commit_failures -= 1;
            return Err(BatchError::repository("serialization failure at commit"));
        }

        self.total.apply(contribution);
        self.commits += 1;
        self.context = context.clone();
        Ok(())
    }

    async fn rollback(&mut self, _tx: ()) -> Result<(), BatchError> {
        self.rollbacks += 1;
        self.events.push(CommitEvent::Rollback);

        if self.rollback_fails {
            return Err(BatchError::repository("rollback failed"));
        }
        Ok(())
    }
}

/// Accepts everything except a *terminal failure* status write, which it
/// rejects.
///
/// Narrow on purpose: failing every write would stop a job before it ever
/// produced an outcome to mask, so the test could not reach the case it exists
/// for.
pub(crate) struct StatusWriteFails(InMemoryJobRepository);

impl StatusWriteFails {
    pub(crate) fn new() -> Self {
        Self(InMemoryJobRepository::default())
    }
}

impl JobRepository for StatusWriteFails {
    type Tx = ();

    async fn begin(&self) -> Result<(), BatchError> {
        Ok(())
    }

    async fn commit(&self, tx: ()) -> Result<(), BatchError> {
        self.0.commit(tx).await
    }

    async fn rollback(&self, tx: ()) -> Result<(), BatchError> {
        self.0.rollback(tx).await
    }

    async fn update_step_execution_in(
        &self,
        tx: &mut (),
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        self.0.update_step_execution_in(tx, step_execution).await
    }

    async fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<JobInstance, BatchError> {
        self.0.find_or_create_instance(job_name, parameters).await
    }

    async fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<Option<JobInstance>, BatchError> {
        self.0.find_instance(job_name, parameters).await
    }

    async fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<JobExecution, BatchError> {
        self.0.create_execution(instance_id).await
    }

    async fn update_execution(&self, execution: &JobExecution) -> Result<(), BatchError> {
        // The record still lands, so the store is not left inconsistent - only
        // the caller's report of it fails, which is the case under test.
        self.0.update_execution(execution).await?;

        if execution.status() == BatchStatus::Failed {
            return Err(BatchError::repository("status write failed"));
        }
        Ok(())
    }

    async fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Option<JobExecution>, BatchError> {
        self.0.last_execution(instance_id).await
    }

    async fn executions(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Vec<JobExecution>, BatchError> {
        self.0.executions(instance_id).await
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        self.0.abandon_execution(execution_id).await
    }

    async fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> Result<StepExecution, BatchError> {
        self.0
            .create_step_execution(job_execution_id, step_name)
            .await
    }

    async fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        self.0.update_step_execution(step_execution).await?;

        if step_execution.status() == BatchStatus::Failed {
            return Err(BatchError::repository("status write failed"));
        }
        Ok(())
    }

    async fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> Result<Option<StepExecution>, BatchError> {
        self.0.last_step_execution(instance_id, step_name).await
    }

    async fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Vec<StepExecution>, BatchError> {
        self.0.step_executions(job_execution_id).await
    }
}

// ---------------------------------------------------------------------------
// Metrics test support
// ---------------------------------------------------------------------------

/// Runs `body` with a recorder scoped to this thread and returns what it
/// emitted.
///
/// `with_local_recorder` rather than `metrics::set_global_recorder`: a
/// global can be installed only once per process, so tests using one are
/// order-dependent and cannot run in parallel.
pub(crate) type Recorded = Vec<(CompositeKey, Option<Unit>, Option<SharedString>, DebugValue)>;

pub(crate) fn recorded(body: impl FnOnce()) -> Recorded {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, body);
    snapshotter.snapshot().into_vec()
}

/// A current-thread runtime, so emissions land on the thread the recorder
/// is scoped to.
pub(crate) fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// The counter named `name` whose `phase` label matches, or `None` if no
/// such series was ever emitted — which is distinct from a series sitting
/// at zero, and is what the rollback test asserts.
pub(crate) fn counter(snapshot: &Recorded, name: &str, phase: Option<&str>) -> Option<u64> {
    snapshot
        .iter()
        .find_map(|(composite, _unit, _help, value)| match value {
            DebugValue::Counter(n)
                if composite.key().name() == name
                    && phase_of(composite.key()).as_deref() == phase =>
            {
                Some(*n)
            }
            _ => None,
        })
}

fn phase_of(key: &Key) -> Option<String> {
    key.labels()
        .find(|label| label.key() == LABEL_PHASE)
        .map(|label| label.value().to_owned())
}

pub(crate) fn labels_of(snapshot: &Recorded, name: &str) -> Vec<(String, String)> {
    snapshot
        .iter()
        .find(|(composite, ..)| composite.key().name() == name)
        .map(|(composite, ..)| {
            composite
                .key()
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tracing test support
// ---------------------------------------------------------------------------

/// One captured event, flattened for assertions.
#[derive(Debug, Clone)]
pub(crate) struct Captured {
    pub level: Level,
    pub message: String,
    pub fields: BTreeMap<String, String>,
    /// Span names enclosing the event, innermost first.
    pub spans: Vec<String>,
}

impl Captured {
    /// The value of `name`, or `None` if the event never carried that field.
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

/// Flattens an event's fields to strings.
///
/// All three `record_*` arms are needed: `tracing` dispatches by *type*, so
/// `phase = "read"` lands in `record_str`, `skipped = 1usize` in `record_u64`
/// and `error = %error` in `record_debug`. A missing arm drops that field
/// silently rather than failing to compile.
#[derive(Default)]
struct FieldCollector {
    message: String,
    fields: BTreeMap<String, String>,
}

impl FieldCollector {
    fn put(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = value;
        } else {
            self.fields.insert(field.name().to_owned(), value);
        }
    }
}

impl Visit for FieldCollector {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.to_owned());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.to_string());
    }
}

/// A process-wide default that enables nothing but claims interest in
/// everything.
///
/// `tracing` caches a callsite's `Interest` **globally**, the first time that
/// callsite is reached. The tests here that install no subscriber reach the
/// framework's `warn!` and `info_span!` callsites too, and with no default in
/// place those get cached as `Interest::never()` — after which every later
/// [`captured`] test sees nothing from that callsite, whatever subscriber it
/// installs. It presents as a missing event, not as a broken harness.
///
/// `sometimes` forces `tracing` to ask the *current* dispatcher per event
/// rather than trust the cache, which is what makes a per-task subscriber work.
/// Applications never hit this: they install one subscriber at startup, before
/// any callsite is reached.
struct AlwaysAsk;

impl Subscriber for AlwaysAsk {
    fn register_callsite(&self, _: &Metadata<'_>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, _: &Event<'_>) {}
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

/// Collects every event into a shared buffer. The tracing counterpart of
/// [`DebuggingRecorder`].
#[derive(Clone, Default)]
pub(crate) struct CaptureLayer(Arc<Mutex<Vec<Captured>>>);

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut collector = FieldCollector::default();
        event.record(&mut collector);

        let spans = ctx
            .event_scope(event)
            .map(|scope| scope.map(|span| span.name().to_owned()).collect())
            .unwrap_or_default();

        self.0.lock().unwrap().push(Captured {
            level: *event.metadata().level(),
            message: collector.message,
            fields: collector.fields,
            spans,
        });
    }
}

/// Runs `future` with a subscriber scoped to it, returning its output and
/// everything it emitted.
///
/// `WithSubscriber` rather than `tracing::subscriber::with_default`: the latter
/// takes a *synchronous* closure and installs a thread-local, which an async
/// body stops seeing the moment it is polled on another thread. That failure is
/// silent — a test capturing nothing passes every assertion about absence. It
/// is the same trap as `Span::enter` versus `Instrument`, and the compiler
/// catches neither.
///
/// Never `set_global_default`, for the reason [`recorded`] gives about
/// recorders: one per process makes every test order-dependent.
pub(crate) async fn captured<F>(future: F) -> (F::Output, Vec<Captured>)
where
    F: Future,
{
    // Setting a global default also rebuilds the interest cache, so this undoes
    // any callsite a subscriber-less test poisoned before the first capture.
    static ASK_EVERY_TIME: Once = Once::new();
    ASK_EVERY_TIME.call_once(|| {
        let _ = ::tracing::subscriber::set_global_default(AlwaysAsk);
    });

    let layer = CaptureLayer::default();
    let events = Arc::clone(&layer.0);
    let subscriber = tracing_subscriber::registry().with(layer);

    let output = future.with_subscriber(subscriber).await;

    let events = events.lock().unwrap().clone();
    (output, events)
}

/// Every captured event at `level` whose message matches.
pub(crate) fn events_named<'a>(
    events: &'a [Captured],
    level: Level,
    message: &str,
) -> Vec<&'a Captured> {
    events
        .iter()
        .filter(|event| event.level == level && event.message == message)
        .collect()
}
