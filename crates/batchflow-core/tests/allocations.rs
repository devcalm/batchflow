//! P-4 as an assertion rather than an illustration.
//!
//! A benchmark only goes red if a human reads it. This file turns "the chunk
//! loop allocates per chunk, never per item" into something CI can fail on.
//!
//! The measurement is scale-invariant on purpose: it holds the *chunk* count
//! fixed and multiplies the *item* count by ten. An absolute budget
//! (`assert!(allocs < 100)`) would be a magic number that drifts every time
//! the engine gains a feature; "ten times the items, same allocations" stays
//! true no matter what the baseline is.

use batchflow_core::{
    BatchError, ChunkStep, ExecutionContext, ItemProcessor, ItemReader, ItemWriter, JobExecutionId,
    Step, StepCommit, StepContribution, StepExecutionId, StepIdentity, Unmanaged, async_trait,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

thread_local! {
    /// Thread-local, not a global `AtomicUsize`: the test harness runs `#[test]`
    /// functions on several threads at once and allocates for output capture,
    /// so a process-wide counter would measure unrelated noise.
    ///
    /// `const`-initialised and `Drop`-free, which is what makes it safe to touch
    /// from inside the allocator — a TLS slot with a destructor can be accessed
    /// during thread teardown, and allocating there would recurse.
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

/// Delegates everything to the system allocator and counts the calls.
///
/// The only `unsafe` in the repository, and it never ships: this is a `tests/`
/// target, so `batchflow-core`'s own `#![forbid(unsafe_code)]` still holds for
/// everything a user compiles.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.with(|n| n.set(n.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

// The fakes below are near-duplicates of `benches/chunk_loop.rs`. Unavoidable:
// every integration test and every bench is its own crate root, and core's
// shared `testing` module is `#[cfg(test)]`, which a `tests/` target does not
// see — it links the normal library build.

struct BenchReader {
    next: u64,
    count: u64,
}

impl ItemReader for BenchReader {
    type Item = u64;

    async fn read(&mut self) -> Result<Option<u64>, BatchError> {
        if self.next == self.count {
            return Ok(None);
        }
        self.next += 1;
        Ok(Some(self.next))
    }
}

struct Passthrough;

impl ItemProcessor for Passthrough {
    type In = u64;
    type Out = u64;

    async fn process(&mut self, item: u64) -> Result<Option<u64>, BatchError> {
        Ok(Some(item))
    }
}

/// The positive control's processor: allocates once per item, deliberately.
struct BoxEveryItem;

impl ItemProcessor for BoxEveryItem {
    type In = u64;
    type Out = Box<u64>;

    async fn process(&mut self, item: u64) -> Result<Option<Box<u64>>, BatchError> {
        Ok(Some(Box::new(item)))
    }
}

struct NullWriter<T>(PhantomData<T>);

impl<T: Send + Sync> ItemWriter for NullWriter<T> {
    type Item = T;

    async fn write(&mut self, items: &[T]) -> Result<(), BatchError> {
        std::hint::black_box(items);
        Ok(())
    }
}

struct NoOpCommit;

#[async_trait]
impl StepCommit<()> for NoOpCommit {
    fn identity(&self) -> StepIdentity<'_> {
        StepIdentity {
            job_name: "alloc",
            step_name: "chunk_loop",
            job_execution_id: JobExecutionId::new(1),
            step_execution_id: StepExecutionId::new(1),
        }
    }

    async fn begin(&mut self) -> Result<(), BatchError> {
        Ok(())
    }

    async fn commit(
        &mut self,
        _tx: (),
        _contribution: &StepContribution,
        _context: &ExecutionContext,
    ) -> Result<(), BatchError> {
        Ok(())
    }

    async fn rollback(&mut self, _tx: ()) -> Result<(), BatchError> {
        Ok(())
    }
}

/// Runs one step to completion and returns the allocations it made.
///
/// `new_current_thread` is load-bearing, not a preference. A multi-threaded
/// runtime drives the future on a worker thread, whose `ALLOCS` is a different
/// cell — the count would come back ~0 and every assertion below would pass
/// while measuring nothing at all. That is what the positive control exists to
/// catch.
fn allocations<P>(processor: P, items: u64, chunk_size: usize) -> usize
where
    P: ItemProcessor<In = u64> + Send,
    P::Out: Send + Sync,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");

    runtime.block_on(async {
        let mut step = ChunkStep::new(
            "chunk_loop",
            BenchReader {
                next: 0,
                count: items,
            },
            processor,
            Unmanaged(NullWriter(PhantomData)),
            NonZeroUsize::new(chunk_size).expect("chunk size must be non-zero"),
        );
        let mut context = ExecutionContext::new();
        let mut commit = NoOpCommit;

        // Snapshot inside `block_on`, so runtime construction is excluded.
        let before = ALLOCS.with(Cell::get);
        Step::<()>::run(&mut step, &mut context, &mut commit)
            .await
            .expect("step failed");
        ALLOCS.with(Cell::get) - before
    })
}

/// P-4: allocation is a function of the chunk count, not the item count.
///
/// Both runs commit ten chunks. The second reads ten times as many items, so
/// any per-item allocation shows up as a ~9,000 difference.
#[test]
fn the_chunk_loop_does_not_allocate_per_item() {
    let few = allocations(Passthrough, 1_000, 100);
    let many = allocations(Passthrough, 10_000, 1_000);

    assert_eq!(
        few, many,
        "ten times the items over the same ten chunks must not allocate more"
    );
}

/// The control. Without it, trap 2 (a multi-threaded runtime, or a counter
/// that never fires) makes the test above compare 0 to 0 and pass forever.
///
/// Same two configurations, but a processor that boxes every item. Here the
/// count *must* scale with items, which proves the measurement can see into
/// the chunk loop at all.
#[test]
fn the_counter_observes_per_item_allocation() {
    let few = allocations(BoxEveryItem, 1_000, 100);
    let many = allocations(BoxEveryItem, 10_000, 1_000);

    assert!(
        many >= few + 8_000,
        "per-item allocation should be visible: {few} then {many}"
    );
}
