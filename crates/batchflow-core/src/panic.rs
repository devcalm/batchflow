//! A panic boundary around user code.
//!
//! Without one, an `unwrap()` in a user's processor unwinds straight through
//! [`Job::run`](crate::Job::run) and [`JobLauncher::run`](crate::JobLauncher::run),
//! so neither layer reaches the write that records its terminal status. The
//! metadata store is left showing `Started`, and every later launch of that
//! instance is refused with
//! [`JobExecutionAlreadyRunning`](crate::BatchError::JobExecutionAlreadyRunning)
//! naming a process that no longer exists — recoverable only by an operator
//! calling [`abandon_execution`](crate::JobRepository::abandon_execution).
//!
//! Catching turns that into an ordinary failed execution, which restarts.
//!
//! # This does not make panicking an error channel
//!
//! A panic is still a bug, and it is still reported at `ERROR`. The boundary
//! exists so that one bad row cannot wedge an instance — not so that user code
//! can signal through `panic!`.
//!
//! # `panic = "abort"`
//!
//! Under `[profile.release] panic = "abort"` there is no unwinding to catch, so
//! this is inert and a panicking step aborts the process. That leaves the same
//! stale `Started` row a `SIGKILL` would; see
//! [`abandon_execution`](crate::JobRepository::abandon_execution).

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Polls `F` inside [`catch_unwind`], yielding the panic instead of unwinding.
///
/// Bounded on `F: Unpin` rather than pin-projecting, because that keeps the
/// whole thing inside `#![forbid(unsafe_code)]`. Every call site wraps a
/// `#[async_trait]` method, whose return type is `Pin<Box<dyn Future>>` and
/// therefore already `Unpin`.
struct CatchUnwind<F>(F);

impl<F: Future + Unpin> Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // `AssertUnwindSafe` is a real assertion, and it is discharged by what
        // happens next rather than by the future's contents: a caught panic
        // always fails the step, so the half-updated state it may have left
        // behind is dropped rather than observed. Metadata consistency does not
        // rest on it either — that is the transaction's job, and a panic
        // mid-chunk rolls back like any other failure.
        let polled = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut self.get_mut().0).poll(context)
        }));

        match polled {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}

/// What `panic!` was called with, if it was called with something printable.
///
/// The standard library boxes the payload as `&str` for a literal and `String`
/// for a formatted message; anything else is opaque and there is nothing
/// useful to render.
fn describe(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panicked with a non-string payload".to_owned()
    }
}

/// Runs `future`, converting a panic into `Err(on_panic(detail))`.
///
/// `on_panic` builds the error rather than this function choosing one, because
/// the right variant depends on the layer: a panicking step is a step failure,
/// a panicking repository call is not.
pub(crate) async fn guarded<F, T, E>(future: F, on_panic: impl FnOnce(String) -> E) -> Result<T, E>
where
    F: Future<Output = Result<T, E>> + Unpin,
{
    match CatchUnwind(future).await {
        Ok(outcome) => outcome,
        Err(payload) => Err(on_panic(describe(&*payload))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BatchError;

    /// The harness prints a backtrace for every caught panic otherwise, which
    /// makes a passing run look like a failing one.
    fn silently<T>(body: impl FnOnce() -> T) -> T {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = body();
        std::panic::set_hook(previous);
        outcome
    }

    async fn panicking() -> Result<(), BatchError> {
        panic!("bad item 4");
    }

    async fn failing() -> Result<(), BatchError> {
        Err(BatchError::process("ordinary failure"))
    }

    async fn succeeding() -> Result<(), BatchError> {
        Ok(())
    }

    /// Sync, not `#[tokio::test]`: `silently` has to wrap the poll that
    /// panics, so the runtime is built inside it — and building one inside an
    /// existing runtime panics for an unrelated reason.
    #[test]
    fn a_panic_becomes_an_error_carrying_its_message() {
        let error = silently(|| {
            block_on(guarded(Box::pin(panicking()), |detail| BatchError::Panic {
                detail,
            }))
        })
        .unwrap_err();

        let BatchError::Panic { detail } = &error else {
            panic!("expected BatchError::Panic, got {error:?}");
        };
        assert_eq!(detail, "bad item 4");
    }

    /// A formatted `panic!("... {x}")` boxes a `String`, not a `&'static str`.
    /// Handling only one of the two loses exactly the messages that carry
    /// runtime detail.
    #[test]
    fn a_formatted_panic_message_survives() {
        async fn formatted(row: u32) -> Result<(), BatchError> {
            panic!("bad row {row}");
        }

        let error = silently(|| {
            block_on(guarded(Box::pin(formatted(7)), |detail| {
                BatchError::Panic { detail }
            }))
        })
        .unwrap_err();

        assert!(error.to_string().contains("bad row 7"), "{error}");
    }

    /// The control: the boundary must be transparent to everything that is not
    /// a panic, or it would rewrite ordinary errors on the way past.
    #[tokio::test]
    async fn an_ordinary_error_passes_through_unchanged() {
        let error = guarded(Box::pin(failing()), |detail| BatchError::Panic { detail })
            .await
            .unwrap_err();

        assert!(matches!(error, BatchError::Process(_)), "{error:?}");
    }

    #[tokio::test]
    async fn success_passes_through() {
        guarded(Box::pin(succeeding()), |detail| BatchError::Panic {
            detail,
        })
        .await
        .unwrap();
    }

    /// Drives the future to completion inside the hook swap, so the poll that
    /// panics happens while the hook is suppressed. An `.await` would hand
    /// control back to an outer runtime and resume outside it.
    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime")
            .block_on(future)
    }
}
