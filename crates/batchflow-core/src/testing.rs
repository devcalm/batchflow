//! Shared test doubles.
//!
//! Compiled only under `cfg(test)`, so nothing here reaches the public API or a
//! release build. Lives in its own module because several modules' tests need the
//! same fakes, and duplicating them is how test suites drift apart.

use crate::{BatchError, ItemProcessor, ItemReader, ItemWriter, Step, StepContribution};
use async_trait::async_trait;
use std::num::NonZeroUsize;

/// Shorthand for a chunk-size literal. A zero here is a bug in the test itself,
/// so panicking is the right response.
pub(crate) fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("test chunk size must be non-zero")
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

/// Yields `remaining_ok` items, then errors.
pub(crate) struct FailingReader {
    pub remaining_ok: usize,
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

/// Fails every write — pins the rule that counters are recorded only once the
/// write has succeeded.
pub(crate) struct FailingWriter;

impl ItemWriter for FailingWriter {
    type Item = u32;

    async fn write(&mut self, _items: &[u32]) -> Result<(), BatchError> {
        Err(BatchError::Write("boom".into()))
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

    // A tasklet reads and writes nothing, so it has no counters to report.
    async fn run(&mut self, _contribution: &mut StepContribution) -> Result<(), BatchError> {
        Ok(())
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

    async fn run(&mut self, _contribution: &mut StepContribution) -> Result<(), BatchError> {
        Err(BatchError::Process("boom".into()))
    }
}
