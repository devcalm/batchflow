//! What a user can reach through the facade alone.
//!
//! These were doctests in `batchflow-core` until the audit's API-2. They belong
//! here for the reason `lib.rs` records: a doctest in `batchflow-core` can name
//! that crate's own dependencies, so it compiles code a user cannot. This
//! crate's dependency graph is one crate deep — the same one a user has — so a
//! missing re-export shows up as a red test rather than as a bug report.
//!
//! Everything below imports from the crate root (`batchflow::Job`), which is
//! also what pins the root re-export in place.

use batchflow::{
    BatchError, ContextValue, ExecutionContext, Job, RepeatStatus, Step, StepCommit,
    StepContribution, Tasklet, TaskletStep, Unmanaged, async_trait,
};

/// `#[async_trait]` is required to implement [`Step`], and must be reachable
/// without a direct dependency on the `async-trait` crate.
mod step_needs_no_direct_async_trait_dependency {
    use super::*;

    struct Cleanup;

    #[async_trait]
    impl Step for Cleanup {
        fn name(&self) -> &str {
            "cleanup"
        }

        async fn run(
            &mut self,
            _context: &mut ExecutionContext,
            _commit: &mut dyn StepCommit,
        ) -> Result<(), BatchError> {
            Ok(())
        }
    }

    #[test]
    fn a_step_can_be_implemented_and_built_into_a_job() {
        let job = Job::builder("nightly").step(Cleanup).build();

        assert_eq!(job.name(), "nightly");
    }
}

/// A [`Tasklet`] needs no macro at all — it is a plain trait with a native
/// `async fn`, since nothing about it has to be `dyn`. What this pins is that
/// all four names it takes are re-exported, and that [`Unmanaged`] adapts a
/// tasklet to a job's transaction type exactly as it does a writer.
mod tasklet_needs_no_macro {
    use super::*;

    /// Archives one file per pass, so each pass commits and a restart resumes.
    struct Archive {
        total: i64,
    }

    impl Tasklet for Archive {
        async fn execute(
            &mut self,
            context: &mut ExecutionContext,
            contribution: &mut StepContribution,
        ) -> Result<RepeatStatus, BatchError> {
            let done = context.get_long("archived")?.unwrap_or(0) + 1;
            context.put("archived", ContextValue::Long(done));
            contribution.increment_write(1);

            Ok(if done >= self.total {
                RepeatStatus::Finished
            } else {
                RepeatStatus::Continuable
            })
        }
    }

    #[test]
    fn a_tasklet_step_builds_against_the_default_transaction() {
        // The annotation is load-bearing, and not only here: anything built
        // from `Unmanaged` implements its step trait for *every* `Tx`, so
        // nothing in the expression pins one. Naming the job's type is what
        // chooses it — `Job` is `Job<()>`, and a Postgres job is `Job<PgTx>` by
        // the same one word.
        let job: Job = Job::builder("nightly")
            .step(TaskletStep::new("archive", Unmanaged(Archive { total: 3 })))
            .build();

        assert_eq!(job.name(), "nightly");
    }
}
